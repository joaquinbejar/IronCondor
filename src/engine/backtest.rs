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
    OrderId, OrderIntent, PositionAction, PositionId, PriceCents, Quantity, TradeId,
};
use crate::engine::bundle_collector::{BundleCollector, FillRecord, PositionSnapshot};
use crate::engine::clock::SimClock;
use crate::engine::ledger::Ledger;
use crate::engine::strategy::{ChainContext, Strategy};
use crate::engine::substrate::{AttributionCollector, AttributionSubstrate};
use crate::engine::tradelog::{ClosedTrade, TradeLogCollector};
use crate::error::BacktestError;
use crate::execution::{ExecutionModel, FillGroup};

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
            cmds: Vec::with_capacity(16),
            fills: Vec::with_capacity(16),
            equity_curve: Vec::with_capacity(capacity),
            attribution: AttributionCollector::with_capacity(capacity),
            trade_log: TradeLogCollector::with_capacity(16),
            bundle: BundleCollector::with_capacity(16),
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
            ..
        } = self;
        cmds.clear();
        {
            let mut ctx = ChainContext {
                snapshot,
                open: inventory.as_slice(),
                pending: &[],
                rng: &mut *rng,
                step: snapshot.step,
            };
            strategy.on_start(&mut ctx, cmds)?;
        }
        fills.clear();
        execution.fill(cmds.as_slice(), snapshot, fills)?;
        let groups = execution.fill_groups();
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
            snapshot,
            inventory,
            ids,
            ledger,
            trade_log,
            bundle,
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
            cmds,
            fills,
            equity_curve,
            attribution,
            trade_log,
            bundle,
        } = self;

        // a. advance the clock (rejects a non-increasing ts as DataOutOfOrder).
        clock.advance_to(snapshot.ts, snapshot.step)?;

        // c. exits (closes) into the shared queue — strictly before entries.
        cmds.clear();
        {
            let ctx = ChainContext {
                snapshot,
                open: inventory.as_slice(),
                pending: &[],
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
                pending: &[],
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
                    pending: &[],
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

        // e. the single execution phase (naive = e2 only; realistic e1 is v0.2).
        fills.clear();
        execution.fill(cmds.as_slice(), snapshot, fills)?;
        // The fill→order grouping for this call (empty ⇒ single-shot 1:1, naive;
        // non-empty ⇒ one group per filling `Submit`, realistic). Borrows
        // `execution` immutably, disjoint from the inventory/ledger/bundle borrows
        // `apply_step_fills` takes.
        let groups = execution.fill_groups();

        // Mint ids + update the inventory + move cash (close validation lives in
        // apply_step_fills, which fails an unknown/oversized close as Execution),
        // and record the per-leg trade log on opens and closes.
        apply_step_fills(
            cmds.as_slice(),
            fills.as_slice(),
            groups,
            snapshot,
            inventory,
            ids,
            ledger,
            trade_log,
            bundle,
            &reasons,
        )?;

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
        attribution.collect(snapshot, marks, spread_capture, fees, step_pnl);
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
/// says how many fills each `Submit` produced: an **empty** `groups` is the
/// single-shot 1:1 contract (naive mode — exactly one fill per `Submit`, in
/// submission order); a **non-empty** `groups` maps each filling `Submit` to its
/// level count (realistic mode — a marketable order walking `n` levels yields
/// `n` fills, one group of `fill_count = n`). Either way the run's fills are
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
/// The `groups` must account for **exactly** the produced fills: any surplus is
/// an uncorrelated fill (a refresh-generated fill of a prior-step resting order —
/// routing those through the engine's inventory is deferred with the resting-order
/// lifecycle), and any shortfall is a `Submit` that produced no fill; both are a
/// typed [`BacktestError::Execution`].
///
/// # Errors
///
/// - [`BacktestError::Execution`] if a `Close` names an unknown leg or an
///   oversized quantity, if a `Submit` produced no fill, or if the fills are not
///   fully correlated to the submitted orders.
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
    groups: &[FillGroup],
    snapshot: &ChainSnapshot,
    inventory: &mut Vec<OpenPosition>,
    ids: &mut IdCounters,
    ledger: &mut Ledger,
    trade_log: &mut TradeLogCollector,
    bundle: &mut BundleCollector,
    reasons: &CloseReasons,
) -> Result<(), BacktestError> {
    let multiplier = snapshot.spec.contract_multiplier;
    let ts_ns = snapshot.ts.value();

    // Coverage precheck: the groups (or the implicit one-fill-per-`Submit` mapping
    // when `groups` is empty) must account for EXACTLY the produced fills, so the
    // per-command consumption below never mis-attributes a fill.
    let expected_fills = if groups.is_empty() {
        cmds.iter()
            .filter(|cmd| matches!(cmd, OrderCommand::Submit(_)))
            .count()
    } else {
        let mut total: usize = 0;
        for group in groups {
            let count =
                usize::try_from(group.fill_count).map_err(|_| BacktestError::ArithmeticOverflow)?;
            total = total
                .checked_add(count)
                .ok_or(BacktestError::ArithmeticOverflow)?;
        }
        total
    };
    if expected_fills != fills.len() {
        return Err(BacktestError::Execution(format!(
            "execution fills are not fully correlated to submitted orders \
             ({} fills, {expected_fills} expected from the order grouping; a surplus is an \
             uncorrelated refresh fill and a shortfall is a submit that did not fill)",
            fills.len()
        )));
    }

    let mut cursor: usize = 0;
    let mut groups_iter = groups.iter().peekable();
    // One trade_id per step's group of Open intents.
    let mut step_trade: Option<TradeId> = None;
    for (idx, cmd) in cmds.iter().enumerate() {
        match cmd {
            OrderCommand::Submit(intent) => {
                let order_id = ids.mint_order()?;
                // How many fills this `Submit` produced: exactly one under the
                // empty-groups single-shot contract, else this command's group.
                let count = if groups.is_empty() {
                    1
                } else {
                    match groups_iter.peek() {
                        Some(group) if group.command_index == idx => {
                            let count = usize::try_from(group.fill_count)
                                .map_err(|_| BacktestError::ArithmeticOverflow)?;
                            groups_iter.next();
                            count
                        }
                        // No group at this index ⇒ this Submit produced no fill.
                        _ => 0,
                    }
                };
                if count == 0 {
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
            }
            // Cancel/replace produce no fill and no inventory/cash effect here;
            // resting-order lifecycle (and its order-id minting) lands with the
            // realistic book (v0.2).
            OrderCommand::Cancel(_) | OrderCommand::Replace { .. } => {}
        }
    }
    // The precheck guarantees full coverage; assert the walk consumed every fill
    // and every group as defence in depth.
    if cursor != fills.len() || groups_iter.next().is_some() {
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
    use rand_chacha::rand_core::RngCore;
    use rust_decimal_macros::dec;

    use super::{BacktestEngine, BacktestRun, vwap_cents};
    use crate::config::{BacktestConfig, FeeSchedule, SlippageModel};
    use crate::data::DataSourceSpec;
    use crate::data::feed::InMemoryFeed;
    use crate::domain::{
        ChainSnapshot, ContractKey, ExecutionMode, InstrumentSpec, OrderCommand, OrderIntent,
        PositionAction, PositionId, PriceCents, Quantity, QuoteView, SimTime, StepIndex,
        Underlying,
    };
    use crate::engine::strategy::{ChainContext, Strategy};
    use crate::error::BacktestError;
    use crate::execution::NaiveFill;

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

    // --- tests -------------------------------------------------------------

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
}
