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

use optionstratlib::backtesting::BacktestResult;

use crate::config::BacktestConfig;
use crate::data::DataFeed;
use crate::domain::{
    Cents, ChainSnapshot, EquityPoint, Fill, OpenPosition, OrderCommand, OrderId, OrderIntent,
    PositionAction, PositionId, Quantity, TradeId,
};
use crate::engine::clock::SimClock;
use crate::engine::ledger::Ledger;
use crate::engine::strategy::{ChainContext, Strategy};
use crate::error::BacktestError;
use crate::execution::ExecutionModel;

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
            ..
        } = state;
        Ok(BacktestRun {
            result,
            equity_curve,
            open_at_end: inventory,
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
        apply_step_fills(
            cmds.as_slice(),
            fills.as_slice(),
            snapshot,
            inventory,
            ids,
            ledger,
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
        if is_last {
            let len_before = cmds.len();
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
            reject_non_close(cmds.get(len_before..).unwrap_or(&[]))?;
        }

        // e. the single execution phase (naive = e2 only; realistic e1 is v0.2).
        fills.clear();
        execution.fill(cmds.as_slice(), snapshot, fills)?;

        // Mint ids + update the inventory + move cash (close validation lives in
        // apply_step_fills, which fails an unknown/oversized close as Execution).
        apply_step_fills(
            cmds.as_slice(),
            fills.as_slice(),
            snapshot,
            inventory,
            ids,
            ledger,
        )?;

        // f. the ONE ledger mutation for the step ⇒ the step's single point.
        let point = ledger.settle(snapshot.step, snapshot.ts, inventory.as_slice(), snapshot)?;
        // g. record (attribution is v0.3; #14 collects the ordered curve).
        equity_curve.push(point);
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

/// Correlate the step's fills back to its commands to mint lifecycle ids,
/// update the position inventory, and move the ledger cash.
///
/// Naive mode fills every `Submit` single-shot and in submission order, and
/// produces no fill for a `Cancel` / `Replace`, so the fills line up with the
/// `Submit`s one-to-one. Each `Submit` mints one [`OrderId`]; a group of `Open`s
/// in one step shares one freshly minted [`TradeId`]; each `Open` mints a
/// [`PositionId`] and pushes an inventory leg; each `Close` reduces (partial) or
/// removes (full) its leg after validating the quantity. Cash moves through the
/// ledger for every fill.
///
/// The `OrderId` / `TradeId` are minted (advancing the seeded counters
/// deterministically) but not surfaced by #14: the bundle rows that carry them
/// (`FillRow` / `PositionRow`) land at v0.3, so the ids are computed in the
/// correct pattern now and become stable outputs then.
///
/// # Errors
///
/// - [`BacktestError::Execution`] if a `Close` names an unknown leg or an
///   oversized quantity, or if the fill count does not match the submit count
///   (multi-level / refresh fills are a v0.2 realistic-mode concern).
/// - [`BacktestError::ArithmeticOverflow`] from id minting or the ledger.
fn apply_step_fills(
    cmds: &[OrderCommand],
    fills: &[Fill],
    snapshot: &ChainSnapshot,
    inventory: &mut Vec<OpenPosition>,
    ids: &mut IdCounters,
    ledger: &mut Ledger,
) -> Result<(), BacktestError> {
    let multiplier = snapshot.spec.contract_multiplier;
    let mut fills_iter = fills.iter();
    // One trade_id per step's group of Open intents.
    let mut step_trade: Option<TradeId> = None;
    for cmd in cmds {
        match cmd {
            OrderCommand::Submit(intent) => {
                let _order_id = ids.mint_order()?; // v0.3 FillRow.order_id
                let fill = fills_iter.next().ok_or_else(|| {
                    BacktestError::Execution(
                        "execution produced fewer fills than submitted orders (naive mode fills each submit single-shot)"
                            .to_string(),
                    )
                })?;
                debug_assert!(
                    fill.contract == intent.contract,
                    "a naive fill must echo the submitted contract"
                );
                match intent.action {
                    PositionAction::Open => {
                        let _trade_id = match step_trade {
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
                            contract: fill.contract.clone(),
                            side: fill.side,
                            quantity: fill.quantity,
                            entry_premium: fill.price,
                        });
                    }
                    PositionAction::Close(position_id) => {
                        reduce_leg(inventory, position_id, fill.quantity)?;
                    }
                }
                ledger.apply_fill(fill, multiplier)?;
            }
            // Naive mode keeps no resting book, so cancel/replace produce no
            // fill and no inventory/cash effect; resting-order lifecycle (and
            // its order-id minting) lands with the realistic book (v0.2).
            OrderCommand::Cancel(_) | OrderCommand::Replace { .. } => {}
        }
    }
    if fills_iter.next().is_some() {
        return Err(BacktestError::Execution(
            "execution produced more fills than submitted orders (multi-level / refresh fills are v0.2)"
                .to_string(),
        ));
    }
    Ok(())
}

/// Validate and apply a `Close` against the live inventory: a partial close
/// reduces the leg and leaves it open; a full close removes it.
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
) -> Result<(), BacktestError> {
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
    } else {
        // requested < open_qty, so the subtraction is a positive remainder.
        let remaining = open_qty - requested;
        let leg = inventory.get_mut(idx).ok_or_else(|| {
            BacktestError::Execution("open leg vanished from inventory".to_string())
        })?;
        leg.quantity = Quantity::new(remaining)?;
    }
    Ok(())
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

    use super::{BacktestEngine, BacktestRun};
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
}
