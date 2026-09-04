//! The replay loop — [`BacktestEngine::run`], the normative state machine.
//!
//! `run` is the synchronous, single-threaded replay loop that drives the three
//! seams — a [`DataFeed`], an [`ExecutionModel`], and a [`Strategy`] — through
//! monomorphised generics (no `dyn` dispatch per step). It sequences the three
//! phases of the normative state machine
//! ([docs/02 §3](../../../docs/02-engine-architecture.md#3-the-replay-loop--normative-state-machine)):
//!
//! - **Startup (§3.1).** Obtain the feed's [`crate::data::TapeMeta`] (an empty
//!   or invalid tape already failed at feed construction, before any `run_id`),
//!   seed the sole [`ChaCha8Rng`] from `config.seed`, mint the seeded id
//!   counters, allocate the reusable `cmds` / `fills` / `equity_curve` buffers
//!   **once**, peek `S0`, run `on_start`, and **execute its opening intents
//!   against `S0` before step 0's `on_snapshot`** — so the startup positions are
//!   already open when the step-0 callback runs (no duplicate-decision window).
//!   No equity point is emitted at startup; its cash effect folds into step 0's
//!   single point.
//! - **Per step (§3.2).** For each snapshot in order: advance the [`SimClock`];
//!   `cmds.clear()`, `exits` (closes) **strictly before** `on_snapshot`
//!   (entries) into the one submission queue; `fills.clear()`,
//!   `execution.fill`; correlate fills back to intents to mint ids and update
//!   the position inventory; then the **single** ledger mutation emits the
//!   step's one [`EquityPoint`].
//! - **Termination (§3.3).** The final snapshot (known up front from
//!   `TapeMeta.final_step`) runs `on_end` **inside** that step — closes only; a
//!   `Submit(Open)` from `on_end` is a [`BacktestError::Execution`]. Any leg
//!   still open after `on_end` is marked-to-last at `S_last` mid and reported
//!   `open_at_end = true`; it is **never** force-closed with a synthetic fill.
//!
//! # Determinism
//!
//! The loop reads no wall clock (time is [`crate::domain::SimTime`] from the
//! feed), draws randomness **only** from the seeded [`ChaCha8Rng`], mints
//! lifecycle ids from seeded monotonic counters (never a random source), keys
//! its carried-forward marks in an ordered map, and holds no `.await`. The
//! `optionstratlib` adapter's per-step reprice reads a wall clock internally,
//! but the loop **marks every position from the snapshot mid** and reads no
//! repriced premium/Greek into any decision or result — the reprice is
//! dormant, so `(seed, config, data)` is byte-reproducible
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
//!
//! # Allocation discipline (PB-1)
//!
//! `cmds`, `fills`, and `equity_curve` are sized once at startup and refilled in
//! place; the seams append by `&mut` reference. Contract keys are interned
//! ([`crate::domain::Underlying`] is `Arc<str>`), so cloning one onto a fill or
//! an inventory leg is a refcount bump, not a heap allocation. A warm step body
//! with no strategy activity therefore allocates nothing in engine code
//! ([docs/07 §4](../../../docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop)).

use chrono::{DateTime, Utc};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use rust_decimal::Decimal;

use optionstratlib::backtesting::{BacktestResult, ExitReason};

use crate::config::BacktestConfig;
use crate::data::{DataFeed, DataSourceSpec};
use crate::domain::{
    Cents, ChainSnapshot, EquityPoint, Fill, GreeksAttributionRow, OpenPosition, OrderCommand,
    OrderId, OrderIntent, PendingOrder, PositionAction, PositionId, PriceCents, Quantity,
    TimeInForce, TradeId,
};
use crate::engine::bundle_collector::{BundleCollector, FillRecord, PositionSnapshot};
use crate::engine::clock::SimClock;
use crate::engine::ledger::Ledger;
use crate::engine::strategy::{ChainContext, Strategy};
use crate::engine::substrate::{AttributionCollector, AttributionSubstrate};
use crate::engine::tradelog::{ClosedTrade, TradeLogCollector};
use crate::error::BacktestError;
use crate::execution::{CarryGroup, ExecutionModel, FillGroup};
use std::collections::BTreeMap;

/// Seeded, deterministic monotonic id counters.
///
/// Lifecycle ids are minted from these counters, **never** from the RNG, so a
/// reproducible run assigns identical ids
/// ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility),
/// [docs/01 §7](../../../docs/01-domain-model.md#7-execution-records)). Each
/// counter starts at `1` and advances with checked arithmetic (an exhausted
/// counter is a typed overflow, never a wrap).
#[derive(Debug, Clone, Copy)]
struct IdCounters {
    position: u64,
    order: u64,
    trade: u64,
}

impl IdCounters {
    /// Fresh counters, each anchored at `1`.
    const fn new() -> Self {
        Self {
            position: 1,
            order: 1,
            trade: 1,
        }
    }

    /// Mint the next [`PositionId`] (one per opened leg).
    fn mint_position(&mut self) -> Result<PositionId, BacktestError> {
        let id = self.position;
        self.position = self
            .position
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        Ok(PositionId::new(id))
    }

    /// Mint the next [`OrderId`] (one per submitted order).
    fn mint_order(&mut self) -> Result<OrderId, BacktestError> {
        let id = self.order;
        self.order = self
            .order
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        Ok(OrderId::new(id))
    }

    /// Mint the next [`TradeId`] (one per multi-leg open group in a step).
    fn mint_trade(&mut self) -> Result<TradeId, BacktestError> {
        let id = self.trade;
        self.trade = self
            .trade
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        Ok(TradeId::new(id))
    }
}

/// The output of one replay run.
///
/// `run` returns this small engine result rather than a bare [`BacktestResult`]
/// so it can expose the per-step equity curve as domain [`EquityPoint`]s
/// (analytics #16/#17 consume these to compute attribution, drawdown, and the
/// summary metrics) and the legs left open at feed exhaustion (the
/// `open_at_end = true` rows a positions writer emits at v0.3). The upstream
/// [`BacktestResult`] is populated with what #14 can compute directly —
/// `strategy_name`, the initial/final capital, and the test period — and left
/// otherwise `Default`; the rich metrics (Sharpe, drawdown analysis, trade
/// statistics) are analytics' responsibility (#16/#32), not fabricated here.
///
/// # The attribution hand-off (analytics reads engine output)
///
/// The engine cannot import `analytics` (layering,
/// [CLAUDE.md](../../../CLAUDE.md)), so the Greek decomposition runs **post-run**.
/// The loop collects the per-step, per-leg decomposition inputs into
/// [`Self::attribution_substrate`] (owned, PB-1-safe,
/// [`crate::engine::substrate`]); a caller above both layers (the `run.rs`
/// composition root) then runs the analytics pass and fills
/// [`Self::greeks_attribution`]. The engine leaves that field **empty** — it is
/// analytics' output, not the engine's — exactly as it leaves the rich
/// [`BacktestResult`] metrics to `metrics::populate`.
#[derive(Debug)]
#[must_use = "a BacktestRun carries the run's results and should be consumed"]
pub struct BacktestRun {
    /// The upstream result, minimally populated (see the type docs).
    pub result: BacktestResult,
    /// One [`EquityPoint`] per step, in step order — the mark-to-market curve.
    pub equity_curve: Vec<EquityPoint>,
    /// The legs still open when the feed exhausted (`open_at_end = true`),
    /// marked-to-last at `S_last` mid — never force-closed.
    pub open_at_end: Vec<OpenPosition>,
    /// One [`ClosedTrade`] per realised leg close, in close order — the owned
    /// per-trade log the post-run metrics pass ([`crate::analytics::metrics`],
    /// #32) reads for per-leg realised P&L and [`ExitReason`]. The engine
    /// **collects** it (it cannot import `analytics`); the composition root runs
    /// the metrics pass over it, exactly as for [`Self::attribution_substrate`].
    pub trade_log: Vec<ClosedTrade>,
    /// The per-step, per-leg **attribution substrate** the loop collected — the
    /// owned inputs the post-run P&L-attribution pass consumes
    /// ([`crate::engine::substrate`], #31).
    pub attribution_substrate: AttributionSubstrate,
    /// The per-step P&L attribution rows, one per step
    /// ([`GreeksAttributionRow`]). **Empty** as returned by
    /// [`BacktestEngine::run`] — analytics fills it post-run from
    /// [`Self::attribution_substrate`] (the composition root does this via
    /// [`crate::analytics::attribution::attribute`]).
    pub greeks_attribution: Vec<GreeksAttributionRow>,
    /// The full fill stream — one [`FillRecord`] per executed fill, with its
    /// correlated `trade_id` / `position_id` / `order_id` / `fill_seq`. The
    /// source of `fills.parquet` (#34); the writer stamps `strategy_run_id` and
    /// derives `contract_id`.
    pub fills: Vec<FillRecord>,
    /// The per-step position snapshots — one [`PositionSnapshot`] per open leg
    /// per step, plus one terminal row per leg at close. The source of
    /// `positions.parquet` (#34).
    pub positions: Vec<PositionSnapshot>,
    /// The run's data-source provenance ([`DataFeed::meta`]), embedded verbatim
    /// in the bundle manifest.
    pub data_source: DataSourceSpec,
    /// The pinned materialised-tape identity ([`crate::data::TapeMeta::data_identity`])
    /// — the `run_id` data-identity input the bundle writer hashes.
    pub data_identity: String,
}

/// The deterministic replay engine — the facade over [`BacktestEngine::run`].
#[derive(Debug, Clone, Copy, Default)]
pub struct BacktestEngine;

impl BacktestEngine {
    /// Run one backtest to completion over the feed's materialised tape.
    ///
    /// Drives the normative state machine (startup → per-step → termination,
    /// see the [module docs](self)) with monomorphised generics — no `dyn`
    /// dispatch per step. `config` supplies the seed and initial capital;
    /// `feed`, `execution`, and `strategy` are the three seams; `strategy_name`
    /// labels the [`BacktestResult`].
    ///
    /// # Errors
    ///
    /// - [`BacktestError::Config`] if the initial capital is not a positive
    ///   integer-cents value.
    /// - [`BacktestError::Data`] if the tape is inconsistent with its metadata
    ///   (reports non-empty but yields nothing, or ends before its declared
    ///   final step).
    /// - [`BacktestError::DataOutOfOrder`] from the clock if a snapshot's `ts`
    ///   does not strictly advance.
    /// - [`BacktestError::Execution`] if a `Close` names an unknown leg or an
    ///   oversized quantity, or if `on_end` emits anything other than a close.
    /// - Any [`BacktestError`] the strategy, the execution model, or the ledger
    ///   raises (including [`BacktestError::ArithmeticOverflow`] on the
    ///   integer-cents ledger).
    pub fn run<F, X, S>(
        config: &BacktestConfig,
        mut feed: F,
        execution: X,
        strategy: S,
        strategy_name: &str,
    ) -> Result<BacktestRun, BacktestError>
    where
        F: DataFeed,
        X: ExecutionModel,
        S: Strategy,
    {
        // --- Startup (§3.1) -------------------------------------------------
        let initial_cents = i64::try_from(config.initial_capital).map_err(|_| {
            BacktestError::Config("initial capital exceeds the i64 cents range".to_string())
        })?;
        if initial_cents <= 0 {
            return Err(BacktestError::Config(
                "initial capital must be positive".to_string(),
            ));
        }

        // Obtain the pinned tape facts — an empty/invalid tape already failed
        // at feed construction, before any run_id or writer exists.
        let tape_meta = feed.tape_meta().clone();
        if !tape_meta.non_empty {
            return Err(BacktestError::Data("tape is empty".to_string()));
        }
        let final_step = tape_meta.final_step;
        let first_ts = tape_meta.first_ts;
        // The bundle-manifest provenance + the run_id data-identity input,
        // captured before the loop consumes the feed.
        let data_source = feed.meta();
        let data_identity = tape_meta.data_identity.clone();
        let steps =
            usize::try_from(final_step.value()).map_err(|_| BacktestError::ArithmeticOverflow)?;
        let capacity = steps
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        let mut state = RunState {
            strategy,
            execution,
            clock: SimClock::new(),
            rng: ChaCha8Rng::seed_from_u64(config.seed),
            ids: IdCounters::new(),
            ledger: Ledger::new(Cents::new(initial_cents)),
            inventory: Vec::new(),
            begin_scratch: Vec::with_capacity(16),
            cmds: Vec::with_capacity(16),
            fills: Vec::with_capacity(16),
            equity_curve: Vec::with_capacity(capacity),
            attribution: AttributionCollector::with_capacity(capacity),
            trade_log: TradeLogCollector::with_capacity(16),
            bundle: BundleCollector::with_capacity(16),
            pending_orders: BTreeMap::new(),
            pending_scratch: Vec::new(),
            submit_ids_scratch: Vec::with_capacity(16),
        };

        // Peek S0: the feed has no `peek`, so pull it and hold it as the
        // current snapshot; TapeMeta.non_empty guarantees it is present.
        let mut current = feed.next()?.ok_or_else(|| {
            BacktestError::Data(
                "tape metadata reports a non-empty tape but the feed yielded no first snapshot"
                    .to_string(),
            )
        })?;

        // §3.1 steps 6–7: run on_start and execute its opening intents against
        // S0 BEFORE step 0's on_snapshot — no equity point is emitted here.
        state.on_start(&current)?;

        // Now the opening inventory (hence the steady-state leg count) is known,
        // pre-size the attribution leg buffer so warm steps push without
        // reallocating (PB-1). Reserving here, before the measured (b)–(g) tail,
        // keeps the per-step body allocation-free for a constant-leg run.
        state
            .attribution
            .reserve_legs(capacity, state.inventory.len());
        // Pre-size the beginning-of-step buffer to the opening leg count so a
        // constant-leg warm step refills it without reallocating (PB-1). A run
        // that later opens more legs grows amortised at that transient.
        state.begin_scratch.reserve(state.inventory.len());
        // Each opening leg closes at most once, so the trade log is bounded by
        // the opening leg count for a hold-to-close run; reserving here keeps a
        // close an amortised push (a run that opens more later grows amortised at
        // that transient, never on a warm step). PB-1-safe.
        state.trade_log.reserve(state.inventory.len());
        // The bundle fill stream (opens + closes) and per-leg-per-step position
        // snapshots are reserved from the same opening leg count + step count, so
        // the warm-step position push never reallocates (PB-1); the fill stream
        // is untouched on a warm step (no fills).
        state.bundle.reserve(state.inventory.len(), capacity);

        // --- Per-step loop (§3.2) + termination (§3.3) ----------------------
        // `last_ts` is assigned exactly once, at the terminal-step break — the
        // loop's only non-error exit — so the final snapshot's ts reaches the
        // result without a dead initial assignment.
        let last_ts;
        loop {
            let is_last = current.step == final_step;
            state.step(&current, is_last)?;
            if is_last {
                last_ts = current.ts;
                break;
            }
            current = feed.next()?.ok_or_else(|| {
                BacktestError::Data(
                    "feed exhausted before reaching the declared final step".to_string(),
                )
            })?;
        }

        // --- Result assembly ------------------------------------------------
        let final_equity = state
            .equity_curve
            .last()
            .map_or(initial_cents, |point| point.equity_cents);
        let result = BacktestResult {
            strategy_name: strategy_name.to_string(),
            initial_capital: dollars(initial_cents),
            final_capital: dollars(final_equity),
            test_period_start: DateTime::<Utc>::from_timestamp_nanos(first_ts.value()),
            test_period_end: DateTime::<Utc>::from_timestamp_nanos(last_ts.value()),
            ..BacktestResult::default()
        };

        let RunState {
            equity_curve,
            inventory,
            attribution,
            trade_log,
            bundle,
            ..
        } = state;
        let (fills, positions) = bundle.into_parts();
        Ok(BacktestRun {
            result,
            equity_curve,
            open_at_end: inventory,
            trade_log: trade_log.into_log(),
            attribution_substrate: attribution.into_substrate(),
            // Empty by design — analytics fills it post-run (see the type docs).
            greeks_attribution: Vec::new(),
            fills,
            positions,
            data_source,
            data_identity,
        })
    }
}

/// The mutable run-scoped state, so the per-step borrows stay disjoint.
struct RunState<S: Strategy, X: ExecutionModel> {
    strategy: S,
    execution: X,
    clock: SimClock,
    rng: ChaCha8Rng,
    ids: IdCounters,
    ledger: Ledger,
    /// The authoritative open-leg inventory, in `position_id` (mint) order — the
    /// slice [`ChainContext::open`] borrows. Legs are pushed on open and removed
    /// on full close, so the order is monotone and therefore deterministic.
    inventory: Vec<OpenPosition>,
    /// Reusable buffer holding the **beginning-of-step** inventory (the holdings
    /// as of `S_{n-1}`, before this step's opens/closes), captured before
    /// [`apply_step_fills`] mutates [`Self::inventory`]. Fed to the ledger's
    /// attribution hand-off so a leg that closes this step still contributes its
    /// θ/Δ/V interval instead of inflating the residual (F22). Cleared and
    /// refilled in place each step — PB-1-safe (an `OpenPosition` clone is an
    /// `Arc<str>` refcount bump, not a heap allocation).
    begin_scratch: Vec<OpenPosition>,
    cmds: Vec<OrderCommand>,
    fills: Vec<Fill>,
    equity_curve: Vec<EquityPoint>,
    /// Collects the per-step, per-leg attribution substrate the post-run
    /// analytics pass consumes (#31). Sized at `on_start`, pushed amortised each
    /// step — PB-1-safe, never a per-step allocation on a constant-leg run.
    attribution: AttributionCollector,
    /// Collects one [`ClosedTrade`] per realised leg close the post-run metrics
    /// pass consumes (#32). Reserved at `on_start`; a close is an amortised push
    /// and a warm step touches it not at all — PB-1-safe.
    trade_log: TradeLogCollector,
    /// Collects the bundle's fill stream ([`FillRecord`]) and per-step position
    /// snapshots ([`PositionSnapshot`]) the writer serialises (#34). Reserved at
    /// `on_start`; a fill is a sparse push and each open leg contributes one
    /// alloc-free snapshot per step against the reserved buffer — PB-1-safe.
    bundle: BundleCollector,
    /// Live resting (GTC) orders by their stable [`OrderId`] (#110) — the
    /// registry the carry-fill prefix is applied against, `Cancel`/`Replace` are
    /// validated against, and [`ChainContext::pending`] is rebuilt from.
    /// `BTreeMap` for deterministic iteration; empty on every IOC-only run.
    pending_orders: BTreeMap<OrderId, PendingEntry>,
    /// Reusable buffer for the strategy-facing pending view, rebuilt from the
    /// registry each step (cleared in place; a [`PendingOrder`] clone is `Copy`
    /// fields plus an `Arc<str>` refcount bump — PB-1-safe, and empty on every
    /// IOC-only run).
    pending_scratch: Vec<PendingOrder>,
    /// Reusable buffer of pre-minted order ids — one per `Submit`/`Replace` in
    /// command order, minted BEFORE the execution model runs so a resting order
    /// can record its engine identity (#110). The minting order replicates the
    /// former in-`apply_step_fills` sequence exactly, so IOC-only runs derive
    /// byte-identical ids.
    submit_ids_scratch: Vec<OrderId>,
}

impl<S: Strategy, X: ExecutionModel> RunState<S, X> {
    /// Run `on_start` and execute its opening intents against `S0` (§3.1).
    ///
    /// No equity point is emitted; the cash effect folds into step 0's point.
    fn on_start(&mut self, snapshot: &ChainSnapshot) -> Result<(), BacktestError> {
        let Self {
            strategy,
            execution,
            rng,
            ids,
            ledger,
            inventory,
            cmds,
            fills,
            trade_log,
            bundle,
            pending_orders,
            submit_ids_scratch,
            ..
        } = self;
        cmds.clear();
        {
            let mut ctx = ChainContext {
                snapshot,
                open: inventory.as_slice(),
                pending: &[],
                marks: ledger.marks(),
                rng: &mut *rng,
                step: snapshot.step,
            };
            strategy.on_start(&mut ctx, cmds)?;
        }
        fills.clear();
        mint_submit_ids(cmds.as_slice(), ids, submit_ids_scratch)?;
        execution.fill(
            cmds.as_slice(),
            submit_ids_scratch.as_slice(),
            snapshot,
            fills,
        )?;
        let groups = execution.fill_groups();
        let carry = execution.carry_fills();
        // Startup emits opening intents only, so no close reason is consulted;
        // the ranges make every `Close` (were one ever emitted) a ManualClose.
        let reasons = CloseReasons {
            exits_end: 0,
            on_end_start: cmds.len(),
            policy_reason: ExitReason::ManualClose,
        };
        apply_step_fills(
            cmds.as_slice(),
            fills.as_slice(),
            groups,
            carry,
            submit_ids_scratch.as_slice(),
            snapshot,
            inventory,
            ids,
            ledger,
            trade_log,
            bundle,
            pending_orders,
            &reasons,
        )?;
        Ok(())
    }

    /// Process one snapshot as a step (§3.2), running `on_end` inside the final
    /// step (§3.3). Emits exactly one [`EquityPoint`].
    fn step(&mut self, snapshot: &ChainSnapshot, is_last: bool) -> Result<(), BacktestError> {
        let Self {
            strategy,
            execution,
            clock,
            rng,
            ids,
            ledger,
            inventory,
            begin_scratch,
            cmds,
            fills,
            equity_curve,
            attribution,
            trade_log,
            bundle,
            pending_orders,
            pending_scratch,
            submit_ids_scratch,
        } = self;

        // a. advance the clock (rejects a non-increasing ts as DataOutOfOrder).
        clock.advance_to(snapshot.ts, snapshot.step)?;

        // b. rebuild the strategy-facing pending view from the registry (#110):
        //    each entry's intent shows its REMAINING resting size. Cleared and
        //    refilled in place — empty (and allocation-free) on IOC-only runs.
        rebuild_pending_view(pending_orders, pending_scratch);

        // c. exits (closes) into the shared queue — strictly before entries.
        cmds.clear();
        {
            let ctx = ChainContext {
                snapshot,
                open: inventory.as_slice(),
                pending: pending_scratch.as_slice(),
                marks: ledger.marks(),
                rng: &mut *rng,
                step: snapshot.step,
            };
            strategy.exits(&ctx, cmds)?;
        }
        // Closes in `cmds[0..exits_end)` are the exit-policy phase's — their
        // trade-log ExitReason is the applied policy's (queried once, and ONLY
        // when the phase produced a close, so it is off the warm-step path).
        let exits_end = cmds.len();
        // d. entries/adjustments appended AFTER the exits.
        {
            let mut ctx = ChainContext {
                snapshot,
                open: inventory.as_slice(),
                pending: pending_scratch.as_slice(),
                marks: ledger.marks(),
                rng: &mut *rng,
                step: snapshot.step,
            };
            strategy.on_snapshot(&mut ctx, cmds)?;
        }
        // §3.3: on_end runs INSIDE the final step; it may append closes only.
        // Closes at or after `on_end_start` are the terminal end-of-data closes.
        let on_end_start = cmds.len();
        if is_last {
            {
                let mut ctx = ChainContext {
                    snapshot,
                    open: inventory.as_slice(),
                    pending: pending_scratch.as_slice(),
                    marks: ledger.marks(),
                    rng: &mut *rng,
                    step: snapshot.step,
                };
                strategy.on_end(&mut ctx, cmds)?;
            }
            reject_non_close(cmds.get(on_end_start..).unwrap_or(&[]))?;
        }

        // The exit-policy reason is queried only when the exits phase produced a
        // close (a real policy trigger) — never on a warm step, so it is off the
        // PB-1 step path. A no-close step uses a non-allocating placeholder.
        let policy_reason = if exits_end > 0 {
            strategy.exit_reason()
        } else {
            ExitReason::ManualClose
        };
        let reasons = CloseReasons {
            exits_end,
            on_end_start,
            policy_reason,
        };

        // e. the single execution phase (naive = e2 only; realistic = e1
        //    refresh + e2 commands, with e1 carry fills routed via #110).
        fills.clear();
        mint_submit_ids(cmds.as_slice(), ids, submit_ids_scratch)?;
        execution.fill(
            cmds.as_slice(),
            submit_ids_scratch.as_slice(),
            snapshot,
            fills,
        )?;
        // The fill→order correlation for this call (`None` ⇒ one-per-`Submit`
        // 1:1, naive; `Some` ⇒ one group per filling `Submit`, realistic), plus
        // the carry channel naming which refresh fills belong to which resting
        // order (#110). Borrows `execution` immutably, disjoint from the
        // inventory/ledger/bundle borrows `apply_step_fills` takes.
        let groups = execution.fill_groups();
        let carry = execution.carry_fills();

        // Capture the beginning-of-step holdings (as of S_{n-1}) BEFORE
        // apply_step_fills mutates the inventory, so a leg that closes this step
        // still contributes its attribution interval (F22). Refilled in place —
        // no per-step allocation on a constant-leg run (PB-1).
        begin_scratch.clear();
        begin_scratch.extend_from_slice(inventory.as_slice());

        // Mint ids + update the inventory + move cash (close validation lives in
        // apply_step_fills, which fails an unknown/oversized close as Execution),
        // and record the per-leg trade log on opens and closes.
        apply_step_fills(
            cmds.as_slice(),
            fills.as_slice(),
            groups,
            carry,
            submit_ids_scratch.as_slice(),
            snapshot,
            inventory,
            ids,
            ledger,
            trade_log,
            bundle,
            pending_orders,
            &reasons,
        )?;

        // Build the beginning-of-step attribution hand-off BEFORE settle advances
        // the retained Greek endpoint from S_{n-1} to S_n (F22): the legs open at
        // S_{n-1}, including any that closed this step, each with its S_{n-1}
        // unit Greeks.
        ledger
            .collect_attribution_marks(begin_scratch.as_slice(), snapshot.spec.contract_multiplier);

        // f. the ONE ledger mutation for the step ⇒ the step's single point.
        let point = ledger.settle(snapshot.step, snapshot.ts, inventory.as_slice(), snapshot)?;
        // g. record: the ordered equity curve (#14), the per-step attribution
        // substrate (#31), AND one bundle position snapshot per surviving open
        // leg (#34). All are amortised pushes into buffers sized at startup — no
        // per-step allocation on a constant-leg run (PB-1). The ledger's per-leg
        // hand-off (`position_marks`) plus its `spread_capture` / `fees` /
        // `step_pnl` are live right here, before the next `settle` overwrites
        // them; the collectors copy out what the post-run passes need.
        equity_curve.push(point);
        let marks = ledger.position_marks();
        let spread_capture = ledger.spread_capture();
        let fees = ledger.fees();
        let step_pnl = ledger.step_pnl();
        // Attribution reads the BEGINNING-of-step holdings (incl. legs closed
        // this step), NOT the post-fill survivors `marks`; the bundle position
        // snapshots below still read `marks` (what is held at end of step).
        attribution.collect(
            snapshot,
            ledger.attribution_marks(),
            spread_capture,
            fees,
            step_pnl,
        );
        // `is_last` legs surviving to the final settle are exactly the legs open
        // at feed exhaustion, so their open rows carry `open_at_end = true`.
        bundle.collect_step(snapshot.step.value(), snapshot.ts.value(), marks, is_last)?;
        Ok(())
    }
}

/// Reject any `on_end` command that is not a close.
///
/// `on_end` may emit **only** `Submit`s whose action is `Close`; a `Submit(Open)`
/// — or a `Cancel` / `Replace` — is a
/// [`BacktestError::Execution`] ([docs/02 §3.3](../../../docs/02-engine-architecture.md#33-termination-feed-exhausted)).
fn reject_non_close(on_end_cmds: &[OrderCommand]) -> Result<(), BacktestError> {
    for cmd in on_end_cmds {
        let is_close = matches!(
            cmd,
            OrderCommand::Submit(OrderIntent {
                action: PositionAction::Close(_),
                ..
            })
        );
        if !is_close {
            return Err(BacktestError::Execution(
                "on_end may emit only close commands; a submit(open), cancel, or replace from on_end is rejected"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// The per-step close-phase ranges the loop hands [`apply_step_fills`] so it can
/// attribute each `Close`'s [`ExitReason`] to the phase that emitted it: the
/// exit-policy phase (`cmds[0..exits_end)`), the terminal end-of-data phase
/// (`cmds[on_end_start..)`), or an in-step strategy adjustment (in between).
/// Mint one [`OrderId`] per `Submit`/`Replace` in command order into the reused
/// scratch, BEFORE the execution model runs — the identity bridge (#110). The
/// minting sequence replicates the former in-`apply_step_fills` per-`Submit`
/// minting exactly, so an IOC-only run derives byte-identical ids.
fn mint_submit_ids(
    cmds: &[OrderCommand],
    ids: &mut IdCounters,
    scratch: &mut Vec<OrderId>,
) -> Result<(), BacktestError> {
    scratch.clear();
    for cmd in cmds {
        if matches!(cmd, OrderCommand::Submit(_) | OrderCommand::Replace { .. }) {
            scratch.push(ids.mint_order()?);
        }
    }
    Ok(())
}

/// Rebuild the strategy-facing pending view from the registry (#110): one
/// [`PendingOrder`] per live resting order, in `OrderId` order, each intent
/// showing its REMAINING resting size. Cleared and refilled in place.
fn rebuild_pending_view(
    pending: &BTreeMap<OrderId, PendingEntry>,
    scratch: &mut Vec<PendingOrder>,
) {
    scratch.clear();
    scratch.extend(pending.values().map(|entry| entry.pending.clone()));
}

/// Register a GTC remainder as a live working order (#110). `fills_so_far` is
/// the count of fills the order already produced at its submit step (its
/// `fill_seq` 0..n), so a later carried fill continues the sequence at `n`.
#[allow(
    clippy::too_many_arguments,
    reason = "one argument per registration-time signal; the registry insert centralises the shape"
)]
fn register_pending(
    pending: &mut BTreeMap<OrderId, PendingEntry>,
    order_id: OrderId,
    intent: &OrderIntent,
    remaining: Quantity,
    fills_so_far: u32,
    trade_id: Option<TradeId>,
    position_id: Option<PositionId>,
    reason: Option<ExitReason>,
) {
    let mut resting = intent.clone();
    resting.quantity = remaining;
    pending.insert(
        order_id,
        PendingEntry {
            pending: PendingOrder {
                order_id,
                intent: resting,
            },
            trade_id,
            position_id,
            fills_so_far,
            reason,
        },
    );
}

/// Extend an existing open leg with a carried fill run (#110): quantity grows
/// by the run's total and the entry premium re-averages as the quantity-weighted
/// VWAP of the old leg and the new fills — the same half-to-even policy as
/// [`vwap_cents`]. The old leg's contribution uses its stored (already-rounded)
/// VWAP — the only value the leg retains — so the combined figure is exact given
/// that stored entry; the ledger's cash stays exact regardless (it applies every
/// fill individually).
fn extend_open_leg(
    inventory: &mut [OpenPosition],
    position_id: PositionId,
    order_fills: &[Fill],
    agg: &FillAggregate,
) -> Result<(), BacktestError> {
    let Some(leg) = inventory.iter_mut().find(|l| l.position_id == position_id) else {
        return Err(BacktestError::Execution(format!(
            "carry extension targets position {} which is not open",
            position_id.value()
        )));
    };
    let old_qty = u64::from(leg.quantity.value());
    let old_notional = u128::from(leg.entry_premium.value())
        .checked_mul(u128::from(old_qty))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let mut carry_notional: u128 = 0;
    for fill in order_fills {
        let leg_notional = u128::from(fill.price.value())
            .checked_mul(u128::from(u64::from(fill.quantity.value())))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        carry_notional = carry_notional
            .checked_add(leg_notional)
            .ok_or(BacktestError::ArithmeticOverflow)?;
    }
    let total_qty = old_qty
        .checked_add(u64::from(agg.quantity.value()))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let total_notional = old_notional
        .checked_add(carry_notional)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    leg.quantity =
        Quantity::new(u32::try_from(total_qty).map_err(|_| BacktestError::ArithmeticOverflow)?)?;
    leg.entry_premium = vwap_cents(total_notional, total_qty)?;
    Ok(())
}

/// Apply one carry group — a contiguous run of refresh fills belonging to one
/// prior-step resting order — against the pending registry (#110): an `Open`
/// entry pushes a NEW inventory leg under the order's original trade, a `Close`
/// entry reduces its target leg; `fill_seq` continues from the order's prior
/// fill count; the entry's remaining size shrinks and the entry drops at zero.
#[allow(
    clippy::too_many_arguments,
    reason = "the carry application threads the same per-step state as apply_step_fills"
)]
fn apply_carry_group(
    order_id: OrderId,
    order_fills: &[Fill],
    snapshot: &ChainSnapshot,
    inventory: &mut Vec<OpenPosition>,
    ids: &mut IdCounters,
    ledger: &mut Ledger,
    trade_log: &mut TradeLogCollector,
    bundle: &mut BundleCollector,
    pending: &mut BTreeMap<OrderId, PendingEntry>,
    multiplier: u32,
    ts_ns: i64,
) -> Result<(), BacktestError> {
    let Some(entry) = pending.get_mut(&order_id) else {
        return Err(BacktestError::Execution(format!(
            "carry group names order {} which is not pending",
            order_id.value()
        )));
    };
    let agg = aggregate_fills(order_fills)?;
    let first = order_fills
        .first()
        .ok_or_else(|| BacktestError::Execution("a carry group's fill run is empty".to_string()))?;
    let count = u32::try_from(order_fills.len()).map_err(|_| BacktestError::ArithmeticOverflow)?;
    match entry.pending.intent.action {
        PositionAction::Open => {
            let trade_id = match entry.trade_id {
                Some(existing) => existing,
                None => {
                    let minted = ids.mint_trade()?;
                    entry.trade_id = Some(minted);
                    minted
                }
            };
            // ONE leg per resting order: the first fill (at submit or here)
            // creates the position; every later carried fill EXTENDS it —
            // quantity grows and the entry premium re-averages — so an
            // `order_id` maps to exactly one `PositionId` in the bundle's
            // trade → position → order → fill tree.
            let position_id = match entry.position_id {
                Some(existing) => {
                    extend_open_leg(inventory, existing, order_fills, &agg)?;
                    let Some(leg) = inventory.iter().find(|l| l.position_id == existing) else {
                        return Err(BacktestError::Execution(format!(
                            "carry extension lost leg {}",
                            existing.value()
                        )));
                    };
                    trade_log.record_open_extend(existing, agg.quantity, leg.entry_premium)?;
                    bundle.register_open_leg(existing, trade_id, leg.entry_premium, first.side);
                    existing
                }
                None => {
                    let minted = ids.mint_position()?;
                    entry.position_id = Some(minted);
                    inventory.push(OpenPosition {
                        position_id: minted,
                        contract: first.contract.clone(),
                        side: first.side,
                        quantity: agg.quantity,
                        entry_premium: agg.vwap,
                    });
                    trade_log.record_open(
                        minted,
                        trade_id,
                        first.contract.clone(),
                        first.side,
                        agg.quantity,
                        agg.vwap,
                        ts_ns,
                    );
                    bundle.register_open_leg(minted, trade_id, agg.vwap, first.side);
                    minted
                }
            };
            for (level, fill) in order_fills.iter().enumerate() {
                let level = u32::try_from(level).map_err(|_| BacktestError::ArithmeticOverflow)?;
                let fill_seq = entry
                    .fills_so_far
                    .checked_add(level)
                    .ok_or(BacktestError::ArithmeticOverflow)?;
                bundle.record_open_fill(fill, order_id.value(), trade_id, position_id, fill_seq);
                ledger.apply_fill(fill, multiplier)?;
            }
        }
        PositionAction::Close(position_id) => {
            let full_close = reduce_leg(inventory, position_id, agg.quantity)?;
            let reason = entry.reason.clone().unwrap_or(ExitReason::ManualClose);
            trade_log.record_close(
                position_id,
                agg.vwap,
                agg.fees,
                agg.slippage,
                ts_ns,
                agg.quantity,
                multiplier,
                reason.clone(),
            )?;
            let snapshot_mark = snapshot.quotes.get(&first.contract).map(|quote| quote.mid);
            for (level, fill) in order_fills.iter().enumerate() {
                let level = u32::try_from(level).map_err(|_| BacktestError::ArithmeticOverflow)?;
                let fill_seq = entry
                    .fills_so_far
                    .checked_add(level)
                    .ok_or(BacktestError::ArithmeticOverflow)?;
                bundle.record_close_fill(fill, order_id.value(), position_id, fill_seq)?;
                ledger.apply_fill(fill, multiplier)?;
            }
            if full_close {
                bundle.record_close_terminal(
                    &first.contract,
                    first.step.value(),
                    first.ts.value(),
                    position_id,
                    agg.quantity.value(),
                    snapshot_mark,
                    multiplier,
                    reason,
                )?;
            }
        }
    }
    entry.fills_so_far = entry
        .fills_so_far
        .checked_add(count)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let remaining = entry
        .pending
        .intent
        .quantity
        .value()
        .checked_sub(agg.quantity.value())
        .ok_or_else(|| {
            BacktestError::Execution(format!(
                "carry fills exceed order {}'s resting size",
                order_id.value()
            ))
        })?;
    if remaining == 0 {
        pending.remove(&order_id);
    } else {
        entry.pending.intent.quantity = Quantity::new(remaining)?;
    }
    Ok(())
}

/// One live resting (GTC) order the engine is tracking across steps (#110):
/// the strategy-facing [`PendingOrder`] view plus the correlation state the
/// carry-fill application needs — the order's minted trade (opens), how many
/// fills it has produced so far (the `fill_seq` continuation), and the close
/// reason captured at registration (closes).
#[derive(Debug, Clone)]
struct PendingEntry {
    /// The strategy-facing view: the stable [`OrderId`] handle plus the resting
    /// intent with `quantity` = the REMAINING resting size.
    pending: PendingOrder,
    /// The trade this order's fills belong to. Every pending OPEN carries a
    /// trade: the step trade is reserved at registration even when the order
    /// filled nothing, so a multi-leg entry whose orders all rest still shares
    /// ONE step-wide trade when the legs later fill (the public same-step
    /// `trade_id` rule). `None` for closes (a close joins its position's trade).
    trade_id: Option<TradeId>,
    /// The single leg this open order builds. A partial submit creates it; a
    /// zero-fill open creates it on the FIRST carried fill; every later carried
    /// fill EXTENDS it (quantity + weighted entry) — one `order_id` maps to one
    /// `PositionId` in the bundle's trade → position → order → fill tree.
    /// `None` for closes and for opens that have not filled yet.
    position_id: Option<PositionId>,
    /// Fills this order has produced so far — the next carried fill's
    /// `fill_seq`.
    fills_so_far: u32,
    /// The close reason captured when a `Close` rested (registration step), so a
    /// later carried close records the reason the strategy scheduled it with.
    /// `None` for opens.
    reason: Option<ExitReason>,
}

struct CloseReasons {
    /// One past the last exit-policy close index — closes below it are the
    /// applied [`ExitPolicy`]'s.
    exits_end: usize,
    /// The first `on_end` (terminal) close index — closes at or after it are
    /// end-of-data closes. Equal to `cmds.len()` on a non-terminal step.
    on_end_start: usize,
    /// The reason for the exit-policy closes (the applied policy's), queried
    /// once per step and only when the exits phase produced a close.
    policy_reason: ExitReason,
}

impl CloseReasons {
    /// The [`ExitReason`] for the `Close` at command index `idx`.
    fn reason_for(&self, idx: usize) -> ExitReason {
        if idx < self.exits_end {
            self.policy_reason.clone()
        } else if idx >= self.on_end_start {
            // The feed exhausted and `on_end` flattened the leg — not an
            // options-expiry, not a policy trigger. Recorded honestly as an
            // end-of-data close (no dedicated upstream variant exists for it).
            ExitReason::Other("end_of_data".to_string())
        } else {
            // A close a strategy emitted from `on_snapshot` (an adjustment).
            ExitReason::ManualClose
        }
    }
}

/// The quantity-weighted aggregate of one order's fills — the reference an
/// [`OpenPosition`] / [`ClosedTrade`] carries when a single order fills across
/// several price levels.
///
/// Cash itself moves **exactly**, per fill, through [`Ledger::apply_fill`]; this
/// aggregate is the leg-level reference (its entry/exit premium is the VWAP, its
/// `quantity` the total filled, its `fees` / `slippage` the sums), so a
/// single-shot fill (`n = 1`) degenerates to that one fill's own values.
struct FillAggregate {
    /// Total contracts filled across the order's levels.
    quantity: Quantity,
    /// Quantity-weighted average price (VWAP), integer cents, **half-to-even**
    /// rounded — the repo's single money-rounding policy.
    vwap: PriceCents,
    /// Sum of the levels' fees (each `≥ 0`), integer cents.
    fees: Cents,
    /// Sum of the levels' signed slippage, integer cents.
    slippage: Cents,
}

/// Aggregate an order's contiguous run of level fills into a [`FillAggregate`].
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] on quantity / notional / fee / slippage
/// overflow, or an empty run (`fills` is guaranteed non-empty by the caller).
fn aggregate_fills(fills: &[Fill]) -> Result<FillAggregate, BacktestError> {
    let mut total_qty: u64 = 0;
    let mut notional: u128 = 0;
    let mut total_fees: i64 = 0;
    let mut total_slippage: i64 = 0;
    for fill in fills {
        let q = u64::from(fill.quantity.value());
        total_qty = total_qty
            .checked_add(q)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let leg_notional = u128::from(fill.price.value())
            .checked_mul(u128::from(q))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        notional = notional
            .checked_add(leg_notional)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        total_fees = total_fees
            .checked_add(fill.fees.value())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        total_slippage = total_slippage
            .checked_add(fill.slippage.value())
            .ok_or(BacktestError::ArithmeticOverflow)?;
    }
    let quantity =
        Quantity::new(u32::try_from(total_qty).map_err(|_| BacktestError::ArithmeticOverflow)?)?;
    let vwap = vwap_cents(notional, total_qty)?;
    Ok(FillAggregate {
        quantity,
        vwap,
        fees: Cents::new(total_fees),
        slippage: Cents::new(total_slippage),
    })
}

/// The volume-weighted average price `notional / total_qty` in integer cents,
/// **half-to-even** rounded (the repo's single money-rounding policy). Exact for
/// a single-shot order (`notional = price × qty`, so it recovers `price`).
///
/// # Rounding policy (must agree with `PriceCents::from_decimal_dollars`)
///
/// This is a **second** encoding of the repo's one money-rounding policy:
/// `PriceCents::from_decimal_dollars` rounds `Decimal` amounts half-to-even (and
/// the liquidity seeder does the same via `round_dp_with_strategy(0,
/// MidpointNearestEven)`). Here the division stays in `u128` — no `Decimal`, no
/// `f64` — so the tie rule is implemented directly (`2·remainder` vs the
/// divisor, ties to the even quotient). The two implementations **must produce
/// the same cents for the same mathematical value**; the half-to-even tie
/// branch is locked to the policy by
/// [`tests::test_vwap_cents_half_to_even_ties_match_money_policy`].
///
/// # Errors
///
/// [`BacktestError::Execution`] if `total_qty` is zero (an empty order — never
/// reached, the caller aggregates `≥ 1` fills each of quantity `> 0`);
/// [`BacktestError::ArithmeticOverflow`] on the rounding arithmetic.
#[must_use = "the computed VWAP is the leg's reference entry/exit premium"]
fn vwap_cents(notional: u128, total_qty: u64) -> Result<PriceCents, BacktestError> {
    if total_qty == 0 {
        return Err(BacktestError::Execution(
            "cannot average a zero-quantity order".to_string(),
        ));
    }
    let divisor = u128::from(total_qty);
    let quotient = notional / divisor;
    let remainder = notional % divisor;
    // Half-to-even: compare 2·remainder to the divisor.
    let twice_rem = remainder
        .checked_mul(2)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let rounded = if twice_rem < divisor {
        quotient
    } else if twice_rem > divisor {
        quotient
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?
    } else if quotient.is_multiple_of(2) {
        // Exactly halfway → round to the even neighbour.
        quotient
    } else {
        quotient
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?
    };
    let cents = u64::try_from(rounded).map_err(|_| BacktestError::ArithmeticOverflow)?;
    Ok(PriceCents::new(cents))
}

/// Correlate the step's fills back to its commands to mint lifecycle ids,
/// update the position inventory, move the ledger cash, and record the per-leg
/// trade log on opens and closes.
///
/// # The fill→order grouping
///
/// Each `Submit` mints one [`OrderId`] and owns a contiguous run of one or more
/// fills. `groups` (the [`ExecutionModel::fill_groups`] channel, [`FillGroup`])
/// is the **explicit correlation mode**, so the two contracts are never
/// conflated by a coincidental fill count (F31): `None` is the one-per-`Submit`
/// 1:1 contract (naive mode — exactly one fill per `Submit`, in submission
/// order, and no uncorrelated fills); `Some(groups)` is the grouped contract
/// (realistic mode) mapping each filling `Submit` to its level count (a
/// marketable order walking `n` levels yields `n` fills, one group of
/// `fill_count = n`). `Some(&[])` is a grouped step that produced no command
/// fills — any fill present is then a surplus and a typed error, whereas the old
/// empty-slice sentinel would silently treat it as naive one-per-`Submit`. Either
/// way the run's fills are
/// aggregated to **one** leg: a group of `Open`s in a step shares one freshly
/// minted [`TradeId`]; each `Open` mints one [`PositionId`], pushes one inventory
/// leg whose `quantity` is the total filled and whose `entry_premium` is the VWAP
/// across its levels, and records one trade-log open; each `Close` reduces
/// (partial) or removes (full) its leg by the **total** filled quantity. Every
/// level fill is applied to the ledger **individually** (so cash stays exact) and
/// is written as its own bundle `FillRecord` with an incrementing `fill_seq`
/// (`0..n`), all sharing the order's ids — the `(step, order_id, fill_seq)` unique
/// key ([docs/05 §7](../../../docs/05-analytics-and-reporting.md#7-fillsparquet)).
/// A full close pushes one terminal `positions.parquet` row for the whole close.
///
/// # The carry channel and the pending registry (#110)
///
/// `carry` (the [`ExecutionModel::carry_fills`] channel, [`CarryGroup`]) names
/// which of the refresh-generated fills at the **front** of `fills` belong to
/// which prior-step resting order. The prefix is applied FIRST, group by group,
/// against `pending`: an `Open` entry pushes a NEW inventory leg (its own
/// [`PositionId`], entry = the carried fill's price, the order's original
/// [`TradeId`] — minted on the first carried fill if the submit step filled
/// nothing) and a `Close` entry reduces its target leg; `fill_seq` continues
/// from the order's prior fill count. A carry group naming an unknown order is a
/// typed error. `carry` with `groups == None` (the naive contract) is also a
/// typed error — carrying requires the grouped mode.
///
/// A `Submit`/`Replace` replacement with `tif == Gtc` that fills partially or
/// not at all REGISTERS its remainder in `pending` (a working order) instead of
/// failing; an IOC that produced no fill remains a typed error. `Cancel` and the
/// replaced side of `Replace` drop their registry entry (validated against the
/// step-start registry first — an id never registered is a typed error; an id
/// consumed by THIS step's refresh is a benign no-op).
///
/// `carry` plus `groups` must account for **exactly** the produced fills; any
/// mismatch is a typed [`BacktestError::Execution`].
///
/// # Errors
///
/// - [`BacktestError::Execution`] if a `Close` names an unknown leg or an
///   oversized quantity, if an IOC `Submit` produced no fill, if a
///   `Cancel`/`Replace` names an order that was never pending, if a carry group
///   names an unknown resting order, or if the fills are not fully correlated to
///   the submitted orders.
/// - [`BacktestError::ArithmeticOverflow`] from id minting, the aggregation, the
///   ledger, the trade log's realised-P&L arithmetic, or the bundle collector's
///   unrealised arithmetic.
#[allow(
    clippy::too_many_arguments,
    reason = "the correlation step threads the loop's per-step buffers, fill grouping, id counters, ledger, trade log, bundle collector, and close-phase ranges; splitting it would fragment the single fill-correlation pass"
)]
fn apply_step_fills(
    cmds: &[OrderCommand],
    fills: &[Fill],
    groups: Option<&[FillGroup]>,
    carry: &[CarryGroup],
    submit_ids: &[OrderId],
    snapshot: &ChainSnapshot,
    inventory: &mut Vec<OpenPosition>,
    ids: &mut IdCounters,
    ledger: &mut Ledger,
    trade_log: &mut TradeLogCollector,
    bundle: &mut BundleCollector,
    pending: &mut BTreeMap<OrderId, PendingEntry>,
    reasons: &CloseReasons,
) -> Result<(), BacktestError> {
    let multiplier = snapshot.spec.contract_multiplier;
    let ts_ns = snapshot.ts.value();

    // Pre-validate lifecycle commands against the STEP-START registry (#110):
    // a Cancel/Replace naming an order that is not pending now was never owned
    // by the strategy (assert_owned guards the seam, but a hand-rolled Strategy
    // could bypass it) — fail closed. During the command walk below an absent
    // entry is instead benign: this step's own refresh (e1) may have consumed
    // the order before its cancel applied.
    for cmd in cmds {
        let (OrderCommand::Cancel(order_id) | OrderCommand::Replace { order_id, .. }) = cmd else {
            continue;
        };
        if !pending.contains_key(order_id) {
            return Err(BacktestError::Execution(format!(
                "cancel/replace targets order {} which is not pending",
                order_id.value()
            )));
        }
    }

    // The carry channel requires the grouped correlation mode: under the naive
    // 1:1 contract there is no resting book, so a non-empty carry is a seam
    // violation, not data.
    if groups.is_none() && !carry.is_empty() {
        return Err(BacktestError::Execution(
            "carry fills require the grouped correlation mode".to_string(),
        ));
    }

    // Coverage precheck: the correlation mode must account for EXACTLY the
    // produced fills, so the per-command consumption below never mis-attributes a
    // fill. `None` (naive) is one fill per `Submit`; `Some(groups)` (grouped,
    // realistic) is the sum of the groups' fill counts — `Some(&[])` therefore
    // expects zero fills, so a surplus refresh fill is rejected here rather than
    // silently consumed one-per-`Submit` (F31).
    let mut carry_total: usize = 0;
    for group in carry {
        let count =
            usize::try_from(group.fill_count).map_err(|_| BacktestError::ArithmeticOverflow)?;
        carry_total = carry_total
            .checked_add(count)
            .ok_or(BacktestError::ArithmeticOverflow)?;
    }
    let command_fills = match groups {
        None => cmds
            .iter()
            .filter(|cmd| matches!(cmd, OrderCommand::Submit(_)))
            .count(),
        Some(groups) => {
            let mut total: usize = 0;
            for group in groups {
                let count = usize::try_from(group.fill_count)
                    .map_err(|_| BacktestError::ArithmeticOverflow)?;
                total = total
                    .checked_add(count)
                    .ok_or(BacktestError::ArithmeticOverflow)?;
            }
            total
        }
    };
    let expected_fills = carry_total
        .checked_add(command_fills)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    if expected_fills != fills.len() {
        return Err(BacktestError::Execution(format!(
            "execution fills are not fully correlated to submitted orders \
             ({} fills, {expected_fills} expected from the order grouping; a surplus is an \
             uncorrelated refresh fill and a shortfall is a submit that did not fill)",
            fills.len()
        )));
    }

    let mut cursor: usize = 0;

    // --- The carry prefix (#110): refresh fills of prior-step resting orders,
    // applied FIRST against the pending registry, before any command fill.
    for group in carry {
        let count =
            usize::try_from(group.fill_count).map_err(|_| BacktestError::ArithmeticOverflow)?;
        let end = cursor
            .checked_add(count)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let order_fills = fills.get(cursor..end).ok_or_else(|| {
            BacktestError::Execution("carry correlation ran past the produced fills".to_string())
        })?;
        cursor = end;
        apply_carry_group(
            group.order_id,
            order_fills,
            snapshot,
            inventory,
            ids,
            ledger,
            trade_log,
            bundle,
            pending,
            multiplier,
            ts_ns,
        )?;
    }

    // `None` (naive) has no group iterator; `Some` (grouped) walks the groups in
    // command order, matching each filling `Submit` by its `command_index`.
    let mut groups_iter = groups.map(|g| g.iter().peekable());
    // One trade_id per step's group of Open intents.
    let mut step_trade: Option<TradeId> = None;
    // The pre-minted id ordinal: one id per `Submit`/`Replace`, in command order.
    let mut submit_ordinal: usize = 0;
    for (idx, cmd) in cmds.iter().enumerate() {
        // Cancel drops its registry entry (benign when this step's refresh
        // already consumed the order); Replace additionally routes its
        // replacement below exactly like a `Submit`.
        let intent = match cmd {
            OrderCommand::Submit(intent) => intent,
            OrderCommand::Cancel(order_id) => {
                pending.remove(order_id);
                continue;
            }
            OrderCommand::Replace {
                order_id,
                replacement,
            } => {
                pending.remove(order_id);
                replacement
            }
        };
        {
            {
                let order_id = submit_ids.get(submit_ordinal).copied().ok_or_else(|| {
                    BacktestError::Execution(
                        "pre-minted submit_ids under-cover the step's commands".to_string(),
                    )
                })?;
                submit_ordinal = submit_ordinal
                    .checked_add(1)
                    .ok_or(BacktestError::ArithmeticOverflow)?;
                // How many fills this `Submit` produced: exactly one under the
                // naive one-per-`Submit` contract (`None`), else this command's
                // group (`Some`), or zero when no group targets this index.
                let count = match groups_iter.as_mut() {
                    None => 1,
                    Some(iter) => match iter.peek() {
                        Some(group) if group.command_index == idx => {
                            let count = usize::try_from(group.fill_count)
                                .map_err(|_| BacktestError::ArithmeticOverflow)?;
                            iter.next();
                            count
                        }
                        // No group at this index ⇒ this Submit produced no fill.
                        _ => 0,
                    },
                };
                if count == 0 {
                    if matches!(intent.tif, TimeInForce::Gtc) {
                        // A GTC that crossed nothing rests whole — a working
                        // order the carry channel can fill later (#110). An
                        // OPEN reserves the step-wide trade NOW: opens emitted
                        // together in one step share one trade_id even when
                        // every one of them rests before filling.
                        let (trade, reason) = match intent.action {
                            PositionAction::Open => {
                                let trade = match step_trade {
                                    Some(existing) => existing,
                                    None => {
                                        let minted = ids.mint_trade()?;
                                        step_trade = Some(minted);
                                        minted
                                    }
                                };
                                (Some(trade), None)
                            }
                            PositionAction::Close(_) => (None, Some(reasons.reason_for(idx))),
                        };
                        register_pending(
                            pending,
                            order_id,
                            intent,
                            intent.quantity,
                            0,
                            trade,
                            None,
                            reason,
                        );
                        continue;
                    }
                    return Err(BacktestError::Execution(format!(
                        "submitted order at command index {idx} produced no fill \
                         (a zero-fill order cannot open or close a leg)"
                    )));
                }
                let end = cursor
                    .checked_add(count)
                    .ok_or(BacktestError::ArithmeticOverflow)?;
                let order_fills = fills.get(cursor..end).ok_or_else(|| {
                    BacktestError::Execution(
                        "fill correlation ran past the produced fills".to_string(),
                    )
                })?;
                cursor = end;
                let first = order_fills.first().ok_or_else(|| {
                    BacktestError::Execution("an order's fill run is empty".to_string())
                })?;
                debug_assert!(
                    order_fills.iter().all(|f| f.contract == intent.contract),
                    "a fill must echo its submitted contract"
                );
                let agg = aggregate_fills(order_fills)?;
                // The position this Submit opened (None for closes) — threaded
                // into a GTC remainder's registry entry so later carried fills
                // EXTEND this leg instead of minting a new one per carry.
                let mut opened_position: Option<PositionId> = None;
                match intent.action {
                    PositionAction::Open => {
                        let trade_id = match step_trade {
                            Some(existing) => existing,
                            None => {
                                let minted = ids.mint_trade()?;
                                step_trade = Some(minted);
                                minted
                            }
                        };
                        let position_id = ids.mint_position()?;
                        opened_position = Some(position_id);
                        inventory.push(OpenPosition {
                            position_id,
                            contract: first.contract.clone(),
                            side: first.side,
                            quantity: agg.quantity,
                            entry_premium: agg.vwap,
                        });
                        trade_log.record_open(
                            position_id,
                            trade_id,
                            first.contract.clone(),
                            first.side,
                            agg.quantity,
                            agg.vwap,
                            ts_ns,
                        );
                        bundle.register_open_leg(position_id, trade_id, agg.vwap, first.side);
                        for (level, fill) in order_fills.iter().enumerate() {
                            let fill_seq = u32::try_from(level)
                                .map_err(|_| BacktestError::ArithmeticOverflow)?;
                            bundle.record_open_fill(
                                fill,
                                order_id.value(),
                                trade_id,
                                position_id,
                                fill_seq,
                            );
                            ledger.apply_fill(fill, multiplier)?;
                        }
                    }
                    PositionAction::Close(position_id) => {
                        let full_close = reduce_leg(inventory, position_id, agg.quantity)?;
                        let reason = reasons.reason_for(idx);
                        trade_log.record_close(
                            position_id,
                            agg.vwap,
                            agg.fees,
                            agg.slippage,
                            ts_ns,
                            agg.quantity,
                            multiplier,
                            reason.clone(),
                        )?;
                        // The terminal-row mark is the closing step's snapshot mid
                        // (a fill exists, so the contract is normally quoted); an
                        // absent quote carries forward in the collector.
                        let snapshot_mark =
                            snapshot.quotes.get(&first.contract).map(|quote| quote.mid);
                        for (level, fill) in order_fills.iter().enumerate() {
                            let fill_seq = u32::try_from(level)
                                .map_err(|_| BacktestError::ArithmeticOverflow)?;
                            bundle.record_close_fill(
                                fill,
                                order_id.value(),
                                position_id,
                                fill_seq,
                            )?;
                            ledger.apply_fill(fill, multiplier)?;
                        }
                        if full_close {
                            bundle.record_close_terminal(
                                &first.contract,
                                first.step.value(),
                                first.ts.value(),
                                position_id,
                                agg.quantity.value(),
                                snapshot_mark,
                                multiplier,
                                reason,
                            )?;
                        }
                    }
                }
                // A GTC that filled only part of its size rests the remainder —
                // register it as a working order the carry channel can fill
                // later (#110). IOC remainders were discarded by the model.
                if matches!(intent.tif, TimeInForce::Gtc) {
                    let filled = u64::from(agg.quantity.value());
                    let total = u64::from(intent.quantity.value());
                    if filled < total {
                        let remainder = total
                            .checked_sub(filled)
                            .ok_or(BacktestError::ArithmeticOverflow)?;
                        let remainder = Quantity::new(
                            u32::try_from(remainder)
                                .map_err(|_| BacktestError::ArithmeticOverflow)?,
                        )?;
                        let (trade, reason) = match intent.action {
                            PositionAction::Open => (step_trade, None),
                            PositionAction::Close(_) => (None, Some(reasons.reason_for(idx))),
                        };
                        let produced = u32::try_from(order_fills.len())
                            .map_err(|_| BacktestError::ArithmeticOverflow)?;
                        register_pending(
                            pending,
                            order_id,
                            intent,
                            remainder,
                            produced,
                            trade,
                            opened_position,
                            reason,
                        );
                    }
                }
            }
        }
    }
    // The precheck guarantees full coverage; assert the walk consumed every fill
    // and every group as defence in depth. A `None` (naive) iterator has no
    // groups to leave unconsumed, so it is trivially exhausted.
    let groups_unconsumed = groups_iter.is_some_and(|mut iter| iter.next().is_some());
    if cursor != fills.len() || groups_unconsumed {
        return Err(BacktestError::Execution(
            "execution fills were not fully correlated to submitted orders".to_string(),
        ));
    }
    Ok(())
}

/// Validate and apply a `Close` against the live inventory: a partial close
/// reduces the leg and leaves it open; a full close removes it. Returns `true` on
/// a **full** close (the leg is gone), `false` on a partial one — the flag the
/// bundle collector uses to decide whether to write a terminal `PositionRow`.
///
/// # Errors
///
/// Returns [`BacktestError::Execution`] if `position_id` names no open leg or
/// the close quantity exceeds the leg's open size
/// ([docs/01 §7](../../../docs/01-domain-model.md#7-execution-records)).
fn reduce_leg(
    inventory: &mut Vec<OpenPosition>,
    position_id: PositionId,
    close_qty: Quantity,
) -> Result<bool, BacktestError> {
    let idx = inventory
        .iter()
        .position(|leg| leg.position_id == position_id)
        .ok_or_else(|| {
            BacktestError::Execution(format!(
                "close targets position {} which is not open",
                position_id.value()
            ))
        })?;
    let open_qty = inventory
        .get(idx)
        .ok_or_else(|| BacktestError::Execution("open leg vanished from inventory".to_string()))?
        .quantity
        .value();
    let requested = close_qty.value();
    if requested > open_qty {
        return Err(BacktestError::Execution(format!(
            "close quantity {requested} exceeds open size {open_qty} for position {}",
            position_id.value()
        )));
    }
    if requested == open_qty {
        inventory.remove(idx); // full close
        Ok(true)
    } else {
        // requested < open_qty, so the subtraction is a positive remainder.
        let remaining = open_qty - requested;
        let leg = inventory.get_mut(idx).ok_or_else(|| {
            BacktestError::Execution("open leg vanished from inventory".to_string())
        })?;
        leg.quantity = Quantity::new(remaining)?;
        Ok(false)
    }
}

/// Convert integer cents to a `Decimal` dollar amount for the upstream
/// [`BacktestResult`] money fields (the `optionstratlib` monetary convention is
/// dollars). Lossless: cents are exactly representable at scale 2.
#[must_use]
fn dollars(cents: i64) -> Decimal {
    Decimal::from_i128_with_scale(i128::from(cents), 2)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rand_chacha::ChaCha8Rng;
    use rand_chacha::rand_core::{Rng, SeedableRng};
    use rust_decimal_macros::dec;

    use super::{BacktestEngine, BacktestRun, vwap_cents};
    use crate::config::{BacktestConfig, FeeSchedule, SlippageModel};
    use crate::data::DataSourceSpec;
    use crate::data::feed::InMemoryFeed;
    use crate::domain::{
        Cents, ChainSnapshot, ContractKey, ExecutionMode, Fill, InstrumentSpec, OrderCommand,
        OrderId, OrderIntent, PositionAction, PositionId, PriceCents, Quantity, QuoteView, SimTime,
        StepIndex, Underlying,
    };
    use crate::engine::strategy::{ChainContext, Strategy};
    use crate::error::BacktestError;
    use crate::execution::{ExecutionModel, FillGroup, NaiveFill};

    const TS0: i64 = 1_750_291_200_000_000_000;
    const STRIKE: u64 = 510_000;

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        u
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    fn call_key() -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(TS0)),
            strike: PriceCents::new(STRIKE),
            style: OptionStyle::Call,
        }
    }

    fn quote(mid: u64) -> QuoteView {
        debug_assert!(mid >= 10, "fixtures use a mid of at least 10c");
        QuoteView {
            contract: call_key(),
            bid: PriceCents::new(mid - 10),
            ask: PriceCents::new(mid + 10),
            mid: PriceCents::new(mid),
            bid_size: qty(50),
            ask_size: qty(50),
            implied_volatility: dec!(0.2),
            delta: dec!(0.3),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    fn snapshot(step: u32, mid: u64) -> ChainSnapshot {
        let mut quotes = BTreeMap::new();
        let q = quote(mid);
        quotes.insert(q.contract.clone(), q);
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("valid spec");
        };
        ChainSnapshot {
            ts: SimTime::new(TS0 + i64::from(step) * 1_000),
            step: StepIndex::new(step),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes,
        }
    }

    fn feed_of(mids: &[u64]) -> InMemoryFeed {
        let tape: Vec<ChainSnapshot> = mids
            .iter()
            .enumerate()
            .map(|(step, &mid)| {
                let step = u32::try_from(step).unwrap_or(u32::MAX);
                snapshot(step, mid)
            })
            .collect();
        let source = DataSourceSpec::Parquet {
            path: "test.parquet".to_string(),
            sha256: "test-sha".to_string(),
        };
        let Ok(feed) = InMemoryFeed::new("test-sha".to_string(), tape, source) else {
            panic!("a strictly-ordered non-empty tape builds a feed");
        };
        feed
    }

    fn config() -> BacktestConfig {
        BacktestConfig {
            data_source: DataSourceSpec::Parquet {
                path: "test.parquet".to_string(),
                sha256: "test-sha".to_string(),
            },
            mode: ExecutionMode::Naive,
            seed: 7,
            initial_capital: 10_000_000,
            fees: FeeSchedule {
                per_contract_cents: 0,
                per_order_cents: 0,
            },
            slippage: SlippageModel::None,
            marketable_cap_ticks: 10,
            liquidity_profile: crate::config::LiquidityProfile::default(),
            limits: crate::config::ResourceLimits::default(),
            output_dir: "runs/out".into(),
            overwrite: false,
        }
    }

    fn naive() -> NaiveFill {
        NaiveFill::new(
            SlippageModel::None,
            FeeSchedule {
                per_contract_cents: 0,
                per_order_cents: 0,
            },
        )
    }

    /// Build a `Submit(Open)` of one long call at the snapshot's mid.
    fn open_call(snapshot: &ChainSnapshot) -> OrderCommand {
        let Some(quote) = snapshot.quotes.get(&call_key()) else {
            panic!("the call is quoted in the fixture");
        };
        OrderCommand::Submit(OrderIntent {
            contract: call_key(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty(1),
            limit: None,
            tif: crate::domain::TimeInForce::Ioc,
            decision_mid: quote.mid,
        })
    }

    // --- test strategies ---------------------------------------------------

    /// Opens one long call at `on_start`, records `ctx.open.len()` at each
    /// `on_snapshot`, and never closes — so the leg is `open_at_end`.
    struct HoldOpen {
        opened: bool,
        seen_open_lens: Rc<RefCell<Vec<usize>>>,
    }

    impl Strategy for HoldOpen {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            out.push(open_call(ctx.snapshot));
            self.opened = true;
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.seen_open_lens.borrow_mut().push(ctx.open.len());
            // If startup ordering were broken (ctx.open empty at step 0), a
            // naive strategy would re-open here; a correct loop shows the leg.
            if !self.opened {
                out.push(open_call(ctx.snapshot));
                self.opened = true;
            }
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }
    }

    /// Emits a `Close` naming an unknown `position_id` at the first snapshot.
    struct CloseUnknown;

    impl Strategy for CloseUnknown {
        fn on_start(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            let Some(quote) = ctx.snapshot.quotes.get(&call_key()) else {
                panic!("the call is quoted");
            };
            out.push(OrderCommand::Submit(OrderIntent {
                contract: call_key(),
                action: PositionAction::Close(PositionId::new(999)),
                side: Side::Short,
                quantity: qty(1),
                limit: None,
                tif: crate::domain::TimeInForce::Ioc,
                decision_mid: quote.mid,
            }));
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }
    }

    /// Opens one long call (qty 1) at `on_start`, then at the first snapshot
    /// emits a `Close` of that leg with an oversized quantity (2 > 1).
    struct CloseOversized;

    impl Strategy for CloseOversized {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            out.push(open_call(ctx.snapshot));
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            let Some(leg) = ctx.open.first() else {
                return Ok(());
            };
            let Some(quote) = ctx.snapshot.quotes.get(&call_key()) else {
                panic!("the call is quoted");
            };
            out.push(OrderCommand::Submit(OrderIntent {
                contract: call_key(),
                action: PositionAction::Close(leg.position_id),
                side: Side::Short,
                quantity: qty(2), // oversized: open size is 1
                limit: None,
                tif: crate::domain::TimeInForce::Ioc,
                decision_mid: quote.mid,
            }));
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }
    }

    /// Emits a `Submit(Open)` from `on_end` — which the loop must reject.
    struct OnEndOpen;

    impl Strategy for OnEndOpen {
        fn on_start(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_end(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            out.push(open_call(ctx.snapshot));
            Ok(())
        }
    }

    /// Draws from `ctx.rng` each snapshot and opens once when the draw is even
    /// — exercises the RNG path so determinism must hold across same-seed runs.
    struct RngProbe {
        opened: bool,
    }

    impl Strategy for RngProbe {
        fn on_start(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            let draw = ctx.rng.next_u32();
            if !self.opened && draw.is_multiple_of(2) {
                out.push(open_call(ctx.snapshot));
                self.opened = true;
            }
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }
    }

    /// A strategy that emits no commands — used to isolate the execution seam.
    struct Passive;

    impl Strategy for Passive {
        fn on_start(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }
    }

    /// An execution model that appends one uncorrelated fill (simulating a
    /// realistic refresh-generated fill of a resting order) while reporting the
    /// **grouped** correlation contract with **no** groups (`Some(&[])`). This is
    /// the F31 case the old empty-slice sentinel silently mis-correlated as naive
    /// one-per-`Submit`.
    struct GroupedRefreshOnly {
        groups: Vec<FillGroup>,
    }

    impl ExecutionModel for GroupedRefreshOnly {
        fn fill(
            &mut self,
            _commands: &[OrderCommand],
            _submit_ids: &[OrderId],
            snap: &ChainSnapshot,
            out_fills: &mut Vec<Fill>,
        ) -> Result<(), BacktestError> {
            // A refresh fill with no corresponding Submit command in this step.
            out_fills.push(Fill {
                ts: snap.ts,
                step: snap.step,
                contract: call_key(),
                side: Side::Short,
                quantity: qty(1),
                price: PriceCents::new(100),
                fees: Cents::new(0),
                slippage: Cents::new(0),
                mode: ExecutionMode::Realistic,
            });
            Ok(())
        }

        // Grouped contract, but no groups: any produced fill is a surplus.
        fn fill_groups(&self) -> Option<&[FillGroup]> {
            Some(&self.groups)
        }

        fn mode(&self) -> ExecutionMode {
            ExecutionMode::Realistic
        }
    }

    // --- tests -------------------------------------------------------------

    #[test]
    fn test_grouped_no_groups_surplus_fill_is_typed_error_not_silent() {
        // F31: a grouped-mode step (`Some(&[])`) that produces a fill with NO
        // Submit group must be a typed Execution error — NOT silently consumed
        // one-per-`Submit` as the old empty-slice sentinel would. This holds in
        // release builds (the correlation check is a real guard, not a
        // debug_assert): at on_start the strategy submits nothing, yet the model
        // appends a refresh fill, so `expected_fills = 0 != 1`.
        let feed = feed_of(&[105, 106]);
        let config = config();
        let execution = GroupedRefreshOnly { groups: Vec::new() };
        let result = BacktestEngine::run(&config, feed, execution, Passive, "passive");
        assert!(
            matches!(result, Err(BacktestError::Execution(_))),
            "a surplus refresh fill under the grouped contract is a typed error"
        );
    }

    #[test]
    fn test_on_start_positions_are_visible_in_open_at_step_zero() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let strategy = HoldOpen {
            opened: false,
            seen_open_lens: Rc::clone(&seen),
        };
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[200, 210, 220]),
            naive(),
            strategy,
            "hold",
        );
        let Ok(_run) = run else {
            panic!("the run succeeds");
        };
        let lens = seen.borrow();
        // The on_start leg is open when step 0's on_snapshot runs.
        assert!(
            matches!(lens.first(), Some(1)),
            "step 0 on_snapshot sees the startup leg"
        );
        assert!(
            lens.iter().all(|&n| n == 1),
            "the single leg stays open every step (never re-opened)"
        );
    }

    #[test]
    fn test_run_emits_exactly_one_equity_point_per_step_including_last() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let strategy = HoldOpen {
            opened: false,
            seen_open_lens: Rc::clone(&seen),
        };
        let Ok(run) = BacktestEngine::run(
            &config(),
            feed_of(&[200, 210, 220, 230]),
            naive(),
            strategy,
            "hold",
        ) else {
            panic!("the run succeeds");
        };
        assert_eq!(run.equity_curve.len(), 4, "one equity point per snapshot");
        let steps: Vec<u32> = run.equity_curve.iter().map(|p| p.step).collect();
        assert_eq!(steps, vec![0, 1, 2, 3], "points are in step order");
    }

    #[test]
    fn test_leg_left_open_is_open_at_end_without_synthetic_terminal_fill() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let strategy = HoldOpen {
            opened: false,
            seen_open_lens: Rc::clone(&seen),
        };
        let Ok(run) =
            BacktestEngine::run(&config(), feed_of(&[200, 300]), naive(), strategy, "hold")
        else {
            panic!("the run succeeds");
        };
        // The leg is reported open at end, never force-closed.
        assert_eq!(run.open_at_end.len(), 1);
        let Some(leg) = run.open_at_end.first() else {
            panic!("one leg open at end");
        };
        assert_eq!(leg.position_id, PositionId::new(1));
        // It is marked-to-last at S_last mid (300c): long call value =
        // 300 * 1 * 100 * (+1) = +30_000, folded into the final equity point.
        let Some(last) = run.equity_curve.last() else {
            panic!("a final equity point exists");
        };
        assert_eq!(last.position_value_cents, 30_000);
    }

    #[test]
    fn test_close_unknown_leg_is_execution_error() {
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[200, 210]),
            naive(),
            CloseUnknown,
            "close",
        );
        assert!(matches!(run, Err(BacktestError::Execution(_))));
    }

    #[test]
    fn test_close_oversized_quantity_is_execution_error() {
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[200, 210]),
            naive(),
            CloseOversized,
            "close",
        );
        assert!(matches!(run, Err(BacktestError::Execution(_))));
    }

    #[test]
    fn test_open_from_on_end_is_execution_error() {
        // A single-step tape so on_end fires at step 0 (the terminal step).
        let run = BacktestEngine::run(&config(), feed_of(&[200]), naive(), OnEndOpen, "onend");
        assert!(matches!(run, Err(BacktestError::Execution(_))));
    }

    #[test]
    fn test_same_seed_same_result_with_rng_consuming_strategy() {
        let curves = |seed: u64| -> BacktestRun {
            let mut cfg = config();
            cfg.seed = seed;
            let Ok(run) = BacktestEngine::run(
                &cfg,
                feed_of(&[200, 210, 220, 230, 240]),
                naive(),
                RngProbe { opened: false },
                "rng",
            ) else {
                panic!("the run succeeds");
            };
            run
        };
        let a = curves(42);
        let b = curves(42);
        // Same seed + config + data ⇒ byte-identical equity curve and open tail.
        assert_eq!(a.equity_curve, b.equity_curve);
        assert_eq!(a.open_at_end, b.open_at_end);
        assert_eq!(a.result.final_capital, b.result.final_capital);
    }

    /// Known-answer test for the seeded RNG: the first draws of
    /// `ChaCha8Rng::seed_from_u64` are pinned to literal values so a
    /// `rand_chacha` / `rand_core` bump that silently changes the seed
    /// expander or the stream fails here, instead of only in a strategy that
    /// draws from `ctx.rng` (the same-seed tests above compare a run against
    /// itself and cannot see a stream change). Values captured on
    /// rand_chacha 0.10.0, which the 2026-09-04 refresh probed identical to
    /// 0.3.1 for `seed_from_u64` + `next_u32`.
    #[test]
    fn test_chacha8_seeded_stream_is_pinned() {
        let draws = |seed: u64| -> [u32; 4] {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            [
                rng.next_u32(),
                rng.next_u32(),
                rng.next_u32(),
                rng.next_u32(),
            ]
        };
        assert_eq!(
            draws(42),
            [962_419_617, 2_928_721_845, 628_724_104, 4_081_401_798]
        );
        assert_eq!(
            draws(0),
            [2_811_902_828, 3_045_455_719, 3_134_767_159, 2_001_118_559]
        );
    }

    #[test]
    fn test_initial_capital_zero_is_config_error() {
        let mut cfg = config();
        cfg.initial_capital = 0;
        let strategy = HoldOpen {
            opened: false,
            seen_open_lens: Rc::new(RefCell::new(Vec::new())),
        };
        let run = BacktestEngine::run(&cfg, feed_of(&[200]), naive(), strategy, "hold");
        assert!(matches!(run, Err(BacktestError::Config(_))));
    }

    /// A **realistic marketable order that walks two price levels** produces one
    /// [`Fill`] per level, all correlated to ONE order (`order_id` /
    /// `position_id` / `trade_id`) with an incrementing `fill_seq`, and the
    /// aggregate leg carries the total filled quantity and the VWAP entry premium.
    /// This is the multi-level capability the #36 conformance fixture drives — the
    /// engine's fill→order grouping lifting the old 1:1 constraint.
    ///
    /// Book (thin): a `Flat` touch of 3 plus two uniform deeper levels (`r = 1`)
    /// stepping one tick, seeded from `S0` (bid 190 / ask 210, tick 5). A
    /// marketable buy for 5 sweeps `3 @ 210` then `2 @ 215` — two fills at
    /// progressively worse prices.
    #[cfg(feature = "orderbook")]
    #[test]
    fn test_realistic_marketable_open_walks_two_levels_into_one_correlated_order() {
        use crate::config::{LiquidityProfile, TouchSize};
        use crate::execution::RealisticFill;

        /// Opens one long call for `lots` contracts (marketable IOC) at
        /// `on_start` and holds it to feed exhaustion.
        struct OpenLots {
            lots: u32,
        }

        impl Strategy for OpenLots {
            fn on_start(
                &mut self,
                ctx: &mut ChainContext,
                out: &mut Vec<OrderCommand>,
            ) -> Result<(), BacktestError> {
                let Some(quote) = ctx.snapshot.quotes.get(&call_key()) else {
                    panic!("the call is quoted in the fixture");
                };
                out.push(OrderCommand::Submit(OrderIntent {
                    contract: call_key(),
                    action: PositionAction::Open,
                    side: Side::Long,
                    quantity: qty(self.lots),
                    limit: None,
                    tif: crate::domain::TimeInForce::Ioc,
                    decision_mid: quote.mid,
                }));
                Ok(())
            }

            fn on_snapshot(
                &mut self,
                _ctx: &mut ChainContext,
                _out: &mut Vec<OrderCommand>,
            ) -> Result<(), BacktestError> {
                Ok(())
            }

            fn on_end(
                &mut self,
                _ctx: &mut ChainContext,
                _out: &mut Vec<OrderCommand>,
            ) -> Result<(), BacktestError> {
                Ok(())
            }
        }

        let profile = LiquidityProfile {
            touch_size: TouchSize::Flat { contracts: 3 },
            depth_levels: 2,
            decay: dec!(1),
        };
        let fees = FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        };
        let run_once = || {
            let execution = RealisticFill::with_liquidity_profile(fees, 10, 7, profile);
            let Ok(run) = BacktestEngine::run(
                &config(),
                feed_of(&[200, 200]),
                execution,
                OpenLots { lots: 5 },
                "multilevel",
            ) else {
                panic!("the multi-level realistic run succeeds");
            };
            run
        };
        let run = run_once();

        // One marketable order walked two levels ⇒ two correlated FillRecords.
        assert_eq!(run.fills.len(), 2, "the buy for 5 walks two ask levels");
        let (Some(f0), Some(f1)) = (run.fills.first(), run.fills.get(1)) else {
            panic!("two fill records expected");
        };
        // All fills of the order share ONE order / leg / trade; fill_seq 0,1.
        assert_eq!(f0.order_id, f1.order_id, "one order across both levels");
        assert_eq!(f0.position_id, f1.position_id, "one leg across both levels");
        assert_eq!(f0.trade_id, f1.trade_id, "one trade across both levels");
        assert_eq!(f0.fill_seq, 0);
        assert_eq!(f1.fill_seq, 1);
        // Progressively worse prices for a buy, per-level sizes summing to 5.
        assert_eq!((f0.fill.price.value(), f0.fill.quantity.value()), (210, 3));
        assert_eq!((f1.fill.price.value(), f1.fill.quantity.value()), (215, 2));
        assert!(
            f1.fill.price.value() > f0.fill.price.value(),
            "each deeper level fills worse for a buy"
        );
        assert!(
            run.fills
                .iter()
                .all(|r| r.fill.mode == ExecutionMode::Realistic)
        );
        // The once-per-order fee sits on the first level only.
        assert_eq!(f0.fill.fees.value(), 3 * 65 + 100);
        assert_eq!(f1.fill.fees.value(), 2 * 65);

        // The (step, order_id, fill_seq) bundle key is unique across the order.
        let mut keys: Vec<(u32, u64, u32)> = run
            .fills
            .iter()
            .map(|r| (r.fill.step.value(), r.order_id, r.fill_seq))
            .collect();
        let total = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(
            keys.len(),
            total,
            "the (step, order_id, fill_seq) key must be unique"
        );

        // The aggregate leg: total filled quantity 5, VWAP entry premium.
        assert_eq!(run.open_at_end.len(), 1, "the single leg is held open");
        let Some(leg) = run.open_at_end.first() else {
            panic!("one open leg at end");
        };
        assert_eq!(leg.quantity.value(), 5, "aggregate quantity across levels");
        // VWAP = round_half_even((210·3 + 215·2) / 5) = 1060 / 5 = 212.
        assert_eq!(leg.entry_premium.value(), 212, "VWAP entry premium");
        assert_eq!(leg.side, Side::Long);

        // Cash moved EXACTLY per fill (not via the VWAP): a Long buy debits
        // Σ(price·qty·mult) + Σ fees = (210·3·100 + 295) + (215·2·100 + 130)
        // = 63_295 + 43_130 = 106_425. Final position marks at S_last mid 200:
        // 200·5·100 = 100_000, so equity = 10_000_000 − 106_425 + 100_000.
        let Some(last) = run.equity_curve.last() else {
            panic!("a final equity point exists");
        };
        assert_eq!(last.position_value_cents, 100_000);
        assert_eq!(last.equity_cents, 10_000_000 - 106_425 + 100_000);

        // Determinism: the same (seed, config, data) reproduces the fill stream.
        let again = run_once();
        assert_eq!(run.fills, again.fills, "the fill stream is deterministic");
        assert_eq!(run.equity_curve, again.equity_curve);
    }

    /// [`vwap_cents`] is a second encoding of the repo's half-to-even money
    /// policy, so its **tie** branch (untested by the exact-division multi-level
    /// scenario, where `1060 / 5 = 212`) must agree with
    /// [`crate::domain::PriceCents::from_decimal_dollars`]. This hits a genuine
    /// `.5` tie in **both** directions — rounding down to an even quotient and up
    /// to an even quotient — and cross-checks each against the money policy, so
    /// the raw-`u128` rounding can never silently drift from `from_decimal_dollars`.
    #[test]
    fn test_vwap_cents_half_to_even_ties_match_money_policy() {
        use rust_decimal::Decimal;

        // The money-policy oracle: the same cents value expressed as dollars and
        // rounded by the single `from_decimal_dollars` half-to-even path.
        let oracle = |notional: u128, total_qty: u64| -> u64 {
            let Ok(n) = i64::try_from(notional) else {
                panic!("fixture notional fits i64");
            };
            let dollars = Decimal::from(n) / Decimal::from(total_qty) / Decimal::ONE_HUNDRED;
            let Ok(price) = PriceCents::from_decimal_dollars(dollars) else {
                panic!("the oracle price must convert");
            };
            price.value()
        };

        // Tie DOWN to even: 842 / 4 = 210.5, quotient 210 already even ⇒ 210.
        let Ok(down) = vwap_cents(842, 4) else {
            panic!("vwap must compute");
        };
        assert_eq!(down.value(), 210, "210.5 ties to the even 210, not 211");
        assert_eq!(
            down.value(),
            oracle(842, 4),
            "the tie-down cent must equal the money policy"
        );

        // Tie UP to even: 846 / 4 = 211.5, quotient 211 odd ⇒ rounds up to 212.
        let Ok(up) = vwap_cents(846, 4) else {
            panic!("vwap must compute");
        };
        assert_eq!(up.value(), 212, "211.5 ties to the even 212, not 211");
        assert_eq!(
            up.value(),
            oracle(846, 4),
            "the tie-up cent must equal the money policy"
        );

        // Non-tie sanity, both sides of the midpoint.
        let Ok(below) = vwap_cents(841, 4) else {
            panic!("vwap must compute");
        };
        assert_eq!(below.value(), 210, "210.25 rounds down to 210");
        let Ok(above) = vwap_cents(843, 4) else {
            panic!("vwap must compute");
        };
        assert_eq!(above.value(), 211, "210.75 rounds up to 211");
    }

    /// A **realistic marketable CLOSE that walks two price levels** reduces its
    /// leg by the total swept quantity, writes exactly ONE terminal
    /// `positions.parquet` row for the whole close, and emits one `FillRecord`
    /// per level (`fill_seq = 0, 1`) with per-fill cash exact. This exercises the
    /// close side of the fill→order grouping (aggregate + `record_close_terminal`),
    /// which the open-only scenario left at `n = 1`.
    ///
    /// Book (thin, symmetric `Flat` 3 + two uniform deeper levels): a long call
    /// for 5 opens by sweeping the ask (`3 @ 210`, `2 @ 215`), then a marketable
    /// sell-to-close for 5 sweeps the bid **down** (`3 @ 190`, `2 @ 185`).
    #[cfg(feature = "orderbook")]
    #[test]
    fn test_realistic_marketable_close_walks_two_levels_into_one_terminal_row() {
        use crate::config::{LiquidityProfile, TouchSize};
        use crate::execution::RealisticFill;

        /// Opens one long call for `lots` at `on_start`, then closes the whole leg
        /// (marketable sell-to-close) at the first `on_snapshot`.
        struct OpenThenCloseLots {
            lots: u32,
            closed: bool,
        }

        impl Strategy for OpenThenCloseLots {
            fn on_start(
                &mut self,
                ctx: &mut ChainContext,
                out: &mut Vec<OrderCommand>,
            ) -> Result<(), BacktestError> {
                let Some(quote) = ctx.snapshot.quotes.get(&call_key()) else {
                    panic!("the call is quoted in the fixture");
                };
                out.push(OrderCommand::Submit(OrderIntent {
                    contract: call_key(),
                    action: PositionAction::Open,
                    side: Side::Long,
                    quantity: qty(self.lots),
                    limit: None,
                    tif: crate::domain::TimeInForce::Ioc,
                    decision_mid: quote.mid,
                }));
                Ok(())
            }

            fn on_snapshot(
                &mut self,
                ctx: &mut ChainContext,
                out: &mut Vec<OrderCommand>,
            ) -> Result<(), BacktestError> {
                if self.closed {
                    return Ok(());
                }
                let Some(leg) = ctx.open.first() else {
                    return Ok(());
                };
                let Some(quote) = ctx.snapshot.quotes.get(&call_key()) else {
                    panic!("the call is quoted in the fixture");
                };
                out.push(OrderCommand::Submit(OrderIntent {
                    contract: call_key(),
                    action: PositionAction::Close(leg.position_id),
                    side: Side::Short, // sell-to-close a long leg
                    quantity: leg.quantity,
                    limit: None,
                    tif: crate::domain::TimeInForce::Ioc,
                    decision_mid: quote.mid,
                }));
                self.closed = true;
                Ok(())
            }

            fn on_end(
                &mut self,
                _ctx: &mut ChainContext,
                _out: &mut Vec<OrderCommand>,
            ) -> Result<(), BacktestError> {
                Ok(())
            }
        }

        let profile = LiquidityProfile {
            touch_size: TouchSize::Flat { contracts: 3 },
            depth_levels: 2,
            decay: dec!(1),
        };
        let fees = FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        };
        let execution = RealisticFill::with_liquidity_profile(fees, 10, 7, profile);
        let Ok(run) = BacktestEngine::run(
            &config(),
            feed_of(&[200]),
            execution,
            OpenThenCloseLots {
                lots: 5,
                closed: false,
            },
            "close-walk",
        ) else {
            panic!("the multi-level close run succeeds");
        };

        // The sell-to-close swept two bid levels ⇒ two Short close fills, all
        // correlated to ONE close order with incrementing fill_seq.
        let closes: Vec<_> = run
            .fills
            .iter()
            .filter(|r| r.fill.side == Side::Short)
            .collect();
        assert_eq!(closes.len(), 2, "the close for 5 walks two bid levels");
        let (Some(c0), Some(c1)) = (closes.first(), closes.get(1)) else {
            panic!("two close fill records expected");
        };
        assert_eq!(
            c0.order_id, c1.order_id,
            "one close order across both levels"
        );
        assert_eq!(c0.position_id, 1);
        assert_eq!(c1.position_id, 1);
        assert_eq!(c0.fill_seq, 0);
        assert_eq!(c1.fill_seq, 1);
        // A sell walks the bid DOWN: 190 then 185 (progressively worse), 3 then 2.
        assert_eq!((c0.fill.price.value(), c0.fill.quantity.value()), (190, 3));
        assert_eq!((c1.fill.price.value(), c1.fill.quantity.value()), (185, 2));
        assert!(
            c1.fill.price.value() < c0.fill.price.value(),
            "each deeper level fills worse for a sell"
        );

        // The whole leg (5) closed ⇒ no open-at-end, and EXACTLY ONE terminal row
        // for the multi-level close, carrying the total swept quantity.
        assert!(run.open_at_end.is_empty(), "the whole leg closed");
        let terminals: Vec<_> = run
            .positions
            .iter()
            .filter(|p| p.exit_reason.is_some())
            .collect();
        assert_eq!(
            terminals.len(),
            1,
            "one terminal row for the multi-level close, not one per level"
        );
        let Some(term) = terminals.first() else {
            panic!("one terminal row");
        };
        assert_eq!(term.position_id, 1);
        assert_eq!(
            term.quantity, 5,
            "the terminal row aggregates the total closed quantity"
        );

        // Per-fill cash is exact (not via any VWAP): the open debits 106_425
        // (63_295 + 43_130); the close credits (190·3·100 − 295) + (185·2·100 −
        // 130) = 56_705 + 36_870 = 93_575. The leg is flat at the end, so
        // equity = 10_000_000 − 106_425 + 93_575 and position value is 0.
        let Some(last) = run.equity_curve.last() else {
            panic!("a final equity point exists");
        };
        assert_eq!(last.position_value_cents, 0);
        assert_eq!(last.equity_cents, 10_000_000 - 106_425 + 93_575);
    }

    // --- #110 resting-order lifecycle through the full engine loop ----------

    /// A strategy that submits ONE resting GTC buy at `on_start` and otherwise
    /// holds; optionally cancels it at a given step (via the pending view).
    #[cfg(feature = "orderbook")]
    struct GtcRest {
        limit: u64,
        quantity: u32,
        cancel_at_step: Option<u32>,
    }

    #[cfg(feature = "orderbook")]
    impl Strategy for GtcRest {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            out.push(OrderCommand::Submit(OrderIntent {
                contract: call_key(),
                action: PositionAction::Open,
                side: Side::Long,
                quantity: Quantity::new(self.quantity)?,
                limit: Some(PriceCents::new(self.limit)),
                tif: crate::domain::TimeInForce::Gtc,
                decision_mid: ctx
                    .snapshot
                    .quotes
                    .get(&call_key())
                    .map_or(PriceCents::new(self.limit), |q| q.mid),
            }));
            Ok(())
        }

        fn exits(
            &mut self,
            _ctx: &ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            if let Some(step) = self.cancel_at_step
                && ctx.step.value() == step
                && let Some(pending) = ctx.pending.first()
            {
                out.push(OrderCommand::Cancel(pending.order_id));
            }
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn exit_reason(&self) -> super::ExitReason {
            super::ExitReason::ManualClose
        }
    }

    #[cfg(feature = "orderbook")]
    fn realistic() -> crate::execution::RealisticFill {
        let cfg = config();
        crate::execution::RealisticFill::with_liquidity_profile(
            cfg.fees,
            cfg.marketable_cap_ticks,
            cfg.seed,
            cfg.liquidity_profile,
        )
    }

    /// The carry path end to end: a GTC buy rests at step 0 (ask 610 above the
    /// 500 limit), the step-1 reseed ask at 500 crosses it, and the engine
    /// opens the leg from the carried fill under the ORIGINAL order id.
    #[test]
    #[cfg(feature = "orderbook")]
    fn test_engine_resting_gtc_opens_via_carry_with_original_order_id() {
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[600, 490, 490]),
            realistic(),
            GtcRest {
                limit: 500,
                quantity: 2,
                cancel_at_step: None,
            },
            "gtc-rest",
        );
        let Ok(run) = run else {
            panic!("the carry-path run succeeds: {run:?}");
        };
        assert_eq!(run.fills.len(), 1, "exactly the carried fill");
        let Some(fill) = run.fills.first() else {
            panic!("one fill record");
        };
        assert_eq!(fill.order_id, 1, "the step-0 pre-minted order id");
        assert_eq!(fill.fill.step.value(), 1, "filled by step 1's refresh");
        assert_eq!(fill.fill_seq, 0, "the order's first fill");
        assert_eq!(fill.fill.price.value(), 500);
        assert_eq!(run.open_at_end.len(), 1, "the carried open stays open");
        assert_eq!(run.equity_curve.len(), 3);
    }

    /// Cancelling the resting order before the market crosses means it NEVER
    /// fills: no fill record, no position, a flat run.
    #[test]
    #[cfg(feature = "orderbook")]
    fn test_engine_cancel_before_cross_never_fills() {
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[600, 600, 490]),
            realistic(),
            GtcRest {
                limit: 500,
                quantity: 2,
                cancel_at_step: Some(1),
            },
            "gtc-cancel",
        );
        let Ok(run) = run else {
            panic!("the cancel run succeeds: {run:?}");
        };
        assert!(run.fills.is_empty(), "a cancelled order never fills");
        assert!(run.open_at_end.is_empty());
        assert!(run.trade_log.is_empty());
    }

    /// A pending open still resting when the feed exhausts is dropped cleanly:
    /// no phantom fill, no position, the run ends flat.
    #[test]
    #[cfg(feature = "orderbook")]
    fn test_engine_pending_open_at_end_is_dropped_cleanly() {
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[600, 600]),
            realistic(),
            GtcRest {
                limit: 500,
                quantity: 2,
                cancel_at_step: None,
            },
            "gtc-unfilled",
        );
        let Ok(run) = run else {
            panic!("the unfilled run succeeds: {run:?}");
        };
        assert!(run.fills.is_empty(), "no fill was invented at end of data");
        assert!(run.open_at_end.is_empty());
        assert!(run.trade_log.is_empty());
    }

    /// Review F1: a GTC that fills PARTIALLY at submit and completes on a later
    /// refresh builds ONE position — the carried fill extends the leg (quantity
    /// and blended entry) under the same position/trade/order identity.
    #[cfg(feature = "orderbook")]
    fn sized_feed(specs: &[(u64, u32)]) -> InMemoryFeed {
        let tape: Vec<ChainSnapshot> = specs
            .iter()
            .enumerate()
            .map(|(step, &(mid, size))| {
                let step = u32::try_from(step).unwrap_or(u32::MAX);
                let mut snap = snapshot(step, mid);
                if let Some(q) = snap.quotes.get_mut(&call_key()) {
                    q.ask_size = qty(size);
                    q.bid_size = qty(size);
                }
                snap
            })
            .collect();
        let source = DataSourceSpec::Parquet {
            path: "test.parquet".to_string(),
            sha256: "test-sha".to_string(),
        };
        let Ok(feed) = InMemoryFeed::new("test-sha".to_string(), tape, source) else {
            panic!("a strictly-ordered non-empty tape builds a feed");
        };
        feed
    }

    #[test]
    #[cfg(feature = "orderbook")]
    fn test_engine_partial_fill_across_snapshots_extends_one_position() {
        // Step 0: ask 500 size 2 — the GTC buy 4 @ 500 fills 2, rests 2.
        // Step 1: ask 500 size 2 again — the reseed crosses the remainder.
        let run = BacktestEngine::run(
            &config(),
            sized_feed(&[(490, 2), (490, 2), (490, 2)]),
            realistic(),
            GtcRest {
                limit: 500,
                quantity: 4,
                cancel_at_step: None,
            },
            "gtc-partial",
        );
        let Ok(run) = run else {
            panic!("the partial-fill run succeeds: {run:?}");
        };
        assert_eq!(run.fills.len(), 2, "submit fill + carried fill");
        let (Some(first), Some(second)) = (run.fills.first(), run.fills.get(1)) else {
            panic!("two fill records");
        };
        assert_eq!(first.order_id, second.order_id, "one order");
        assert_eq!(
            first.position_id, second.position_id,
            "ONE position — the carry extends the leg, never mints a second"
        );
        assert_eq!(first.trade_id, second.trade_id, "one trade");
        assert_eq!(
            (first.fill_seq, second.fill_seq),
            (0, 1),
            "fill_seq continues"
        );
        assert_eq!(run.open_at_end.len(), 1);
        let Some(leg) = run.open_at_end.first() else {
            panic!("one leg");
        };
        assert_eq!(leg.quantity.value(), 4, "the leg carries the full size");
    }

    /// Review F2: a multi-order entry whose GTC opens ALL rest at submit still
    /// shares ONE step-wide trade when the legs later fill.
    #[cfg(feature = "orderbook")]
    struct TwoGtcRest;

    #[cfg(feature = "orderbook")]
    impl Strategy for TwoGtcRest {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            let mid = ctx
                .snapshot
                .quotes
                .get(&call_key())
                .map_or(PriceCents::new(500), |q| q.mid);
            for _ in 0..2 {
                out.push(OrderCommand::Submit(OrderIntent {
                    contract: call_key(),
                    action: PositionAction::Open,
                    side: Side::Long,
                    quantity: qty(1),
                    limit: Some(PriceCents::new(500)),
                    tif: crate::domain::TimeInForce::Gtc,
                    decision_mid: mid,
                }));
            }
            Ok(())
        }

        fn exits(
            &mut self,
            _ctx: &ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn exit_reason(&self) -> super::ExitReason {
            super::ExitReason::ManualClose
        }
    }

    #[test]
    #[cfg(feature = "orderbook")]
    fn test_engine_all_resting_multi_leg_entry_shares_one_trade() {
        // Both GTC buys rest at step 0 (ask 610) and fill on step 1's reseed
        // (ask 500) — two orders, two positions, ONE step-wide trade.
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[600, 490, 490]),
            realistic(),
            TwoGtcRest,
            "two-gtc",
        );
        let Ok(run) = run else {
            panic!("the all-resting run succeeds: {run:?}");
        };
        assert_eq!(run.fills.len(), 2, "both resting opens filled");
        let (Some(first), Some(second)) = (run.fills.first(), run.fills.get(1)) else {
            panic!("two fill records");
        };
        assert_ne!(first.order_id, second.order_id, "two orders");
        assert_ne!(first.position_id, second.position_id, "two legs");
        assert_eq!(
            first.trade_id, second.trade_id,
            "opens emitted together share ONE step-wide trade"
        );
    }

    /// The carry path is deterministic: two identical runs produce identical
    /// equity curves and fill identities.
    #[test]
    #[cfg(feature = "orderbook")]
    fn test_engine_carry_path_run_twice_is_identical() {
        let go = || {
            let run = BacktestEngine::run(
                &config(),
                feed_of(&[600, 490, 490]),
                realistic(),
                GtcRest {
                    limit: 500,
                    quantity: 2,
                    cancel_at_step: None,
                },
                "gtc-rest",
            );
            let Ok(run) = run else {
                panic!("the run succeeds");
            };
            let fill_ids: Vec<(u64, u32, u32)> = run
                .fills
                .iter()
                .map(|f| (f.order_id, f.fill.step.value(), f.fill_seq))
                .collect();
            (run.equity_curve, fill_ids)
        };
        let (equity_a, fills_a) = go();
        let (equity_b, fills_b) = go();
        assert_eq!(equity_a, equity_b, "byte-identical equity curves");
        assert_eq!(fills_a, fills_b, "identical fill identities");
    }

    /// #110: a strategy that opens marketable-IOC at start and rests a GTC
    /// close that never crosses — the leg must end honestly `open_at_end` (the
    /// pending close is dropped, never phantom-filled, and the run never
    /// aborts in `reduce_leg`).
    #[cfg(feature = "orderbook")]
    struct OpenThenRestClose {
        rested: bool,
    }

    #[cfg(feature = "orderbook")]
    impl Strategy for OpenThenRestClose {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            out.push(OrderCommand::Submit(OrderIntent {
                contract: call_key(),
                action: PositionAction::Open,
                side: Side::Long,
                quantity: qty(1),
                limit: None,
                tif: crate::domain::TimeInForce::Ioc,
                decision_mid: ctx
                    .snapshot
                    .quotes
                    .get(&call_key())
                    .map_or(PriceCents::new(500), |q| q.mid),
            }));
            Ok(())
        }

        fn exits(
            &mut self,
            _ctx: &ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            if !self.rested
                && let Some(leg) = ctx.open.first()
            {
                self.rested = true;
                out.push(OrderCommand::Submit(OrderIntent {
                    contract: leg.contract.clone(),
                    action: PositionAction::Close(leg.position_id),
                    side: Side::Short,
                    quantity: leg.quantity,
                    // A sell limit far ABOVE the market: rests, never crosses.
                    limit: Some(PriceCents::new(100_000)),
                    tif: crate::domain::TimeInForce::Gtc,
                    decision_mid: PriceCents::new(600),
                }));
            }
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            Ok(())
        }

        fn exit_reason(&self) -> super::ExitReason {
            super::ExitReason::ManualClose
        }
    }

    #[test]
    #[cfg(feature = "orderbook")]
    fn test_engine_resting_close_that_never_fills_leaves_leg_open_at_end() {
        let run = BacktestEngine::run(
            &config(),
            feed_of(&[600, 600, 600]),
            realistic(),
            OpenThenRestClose { rested: false },
            "rest-close",
        );
        let Ok(run) = run else {
            panic!("the resting-close run succeeds: {run:?}");
        };
        // The open filled marketable at start; the resting close never crossed.
        assert_eq!(run.open_at_end.len(), 1, "the leg ends honestly open");
        assert!(
            run.trade_log.is_empty(),
            "no close was realised — the resting close never filled"
        );
        // Exactly the opening fills exist (marketable walk), none from the
        // resting close.
        assert!(
            run.fills.iter().all(|f| f.fill.step.value() == 0),
            "every fill is the step-0 open"
        );
    }
}
