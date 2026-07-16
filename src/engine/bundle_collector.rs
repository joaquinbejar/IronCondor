//! The engine→bundle **fill stream** and **per-step position snapshots** — the
//! two collections the result-bundle writer ([`crate::bundle::writer`], #34)
//! turns into `fills.parquet` and `positions.parquet`.
//!
//! # Why a post-run collection (layering + look-back)
//!
//! `fills.parquet` needs **one row per executed fill** ([docs/05 §7](../../../docs/05-analytics-and-reporting.md#7-fillsparquet))
//! and `positions.parquet` needs **one row per open leg per step, plus one
//! terminal row at close** ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet)).
//! The loop drops both once a step ends — the `fills` scratch is `clear()`ed
//! each step and the ledger's [`PositionMark`] hand-off is reused scratch valid
//! only until the next `settle`. Since `bundle` **consumes** engine output and
//! the engine must **not** import `bundle` ([CLAUDE.md](../../../CLAUDE.md)
//! module boundaries), the loop **collects** owned records the writer reads
//! post-run — the same pattern as [`crate::engine::tradelog`] and
//! [`crate::engine::substrate`].
//!
//! # Representation (PB-1-safe)
//!
//! Two **flat** `Vec`s, never a `Vec`-of-`Vec`:
//!
//! - [`BundleCollector::fills`] — one [`FillRecord`] per executed fill. Fills are
//!   **sparse** (an open/close event, never a warm revaluation-only step), so a
//!   push happens only on a fill.
//! - [`BundleCollector::positions`] — one [`PositionSnapshot`] per open leg per
//!   step (plus a terminal row per leg on full close). Pushed once per open leg
//!   per step, reserved to `step_count × leg_count` once the opening leg count is
//!   known, exactly like [`crate::engine::substrate`]'s leg buffer.
//!
//! A [`PositionSnapshot`] carries a [`ContractKey`] (its `underlying` is an
//! `Arc<str>`), **not** a `String` `contract_id`: cloning it onto a snapshot is a
//! refcount bump, not a heap allocation — the same alloc-free clone the ledger's
//! [`PositionMark`] push already performs — so a per-leg-per-step push allocates
//! nothing against a reserved `Vec` (PB-1,
//! [docs/07 §4](../../../docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop)).
//! The writer derives the wire `contract_id` at encode time (termination phase,
//! free to allocate).
//!
//! # Terminal-row semantics ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet))
//!
//! A **full** close writes a terminal [`PositionSnapshot`] observed at the
//! **beginning-of-step endpoint** — immediately before the closing fill: its
//! `quantity` is the size open before the close, its `mark_cents` is the closing
//! step's **snapshot mid** (a fill exists, so the contract is quoted that step),
//! its `unrealized_cents` is the leg's P&L at that mid, and its `exit_reason` is
//! the applied reason (the **only** non-null `exit_reason` in the bundle). A
//! **partial** close does not close the leg, so it writes no terminal row — the
//! reduced leg keeps taking ordinary open rows. A leg still open at feed
//! exhaustion carries `open_at_end = true` on its last open row (the final step's
//! open rows), and has no terminal row.
//!
//! # Determinism
//!
//! The collector reads no wall clock and draws no randomness; it records the
//! snapshot timestamps and the loop's deterministic command / inventory order,
//! and keys its open-leg bookkeeping in an ordered [`BTreeMap`]. The same
//! `(seed, config, data)` yields byte-identical collections
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use std::collections::BTreeMap;

use optionstratlib::Side;
use optionstratlib::backtesting::ExitReason;

use crate::domain::execution::sign_convention;
use crate::domain::{ContractKey, Fill, PositionId, PriceCents, TradeId};
use crate::engine::ledger::PositionMark;
use crate::error::BacktestError;

/// One executed fill plus its engine-assigned lifecycle ids — the wire-agnostic
/// source of a `fills.parquet` row ([docs/05 §7](../../../docs/05-analytics-and-reporting.md#7-fillsparquet)).
///
/// The [`Fill`] carries `ts` / `step` / `contract` / `side` / `quantity` /
/// `price` / `fees` / `slippage` / `mode`; the four ids are correlated to it at
/// fill time in the loop (they are not on the `Fill` itself). The writer stamps
/// `strategy_run_id` and derives `contract_id` from the fill's [`ContractKey`] at
/// encode time. `fill_seq` is `0` for a single-shot naive fill; a realistic order
/// walking price levels increments it per level (the `(step, order_id, fill_seq)`
/// unique key).
#[derive(Debug, Clone, PartialEq)]
pub struct FillRecord {
    /// The executed fill (timestamps, contract, side, size, price, frictions).
    pub fill: Fill,
    /// The multi-leg trade this fill belongs to (a `u64` on the wire).
    pub trade_id: u64,
    /// The single leg this fill opens or closes.
    pub position_id: u64,
    /// The submitted order that produced this fill.
    pub order_id: u64,
    /// The 0-based fill index within the order (levels walked); `0` in naive mode.
    pub fill_seq: u32,
}

/// One leg's per-step mark-to-market snapshot — the wire-agnostic source of a
/// `positions.parquet` row ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet)).
///
/// Carries a [`ContractKey`] (its `underlying` an `Arc<str>`) rather than a
/// `String` `contract_id`, so a per-leg-per-step push is heap-allocation-free
/// (PB-1); the writer derives `contract_id` at encode time. `exit_reason` is
/// `None` on an open row and `Some(..)` only on a terminal (full-close) row —
/// the **only** nullable column in the bundle.
#[derive(Debug, Clone, PartialEq)]
pub struct PositionSnapshot {
    /// The 0-based step ordinal.
    pub step: u32,
    /// The step's snapshot timestamp (ns since the Unix epoch, UTC).
    pub ts_ns: i64,
    /// The stable leg id (a `u64` on the wire).
    pub position_id: u64,
    /// The multi-leg trade this leg belongs to.
    pub trade_id: u64,
    /// The leg's contract identity — the writer derives the wire `contract_id`.
    pub contract: ContractKey,
    /// The direction the leg was **opened** in (`Long` bought, `Short` sold) —
    /// never the closing trade's side.
    pub side: Side,
    /// Open contracts (`> 0`); on a terminal row, the contracts closed.
    pub quantity: u32,
    /// Average entry premium, integer cents.
    pub avg_price_cents: u64,
    /// The mark (snapshot mid; last-known carried forward if `stale_mark`).
    pub mark_cents: u64,
    /// The leg's unrealised P&L at the mark, signed integer cents.
    pub unrealized_cents: i64,
    /// `true` if the contract had no quote this step and the mark was carried
    /// forward.
    pub stale_mark: bool,
    /// `None` while open; the applied [`ExitReason`] on the terminal row.
    pub exit_reason: Option<ExitReason>,
    /// `true` if the leg was still open when the feed exhausted.
    pub open_at_end: bool,
}

/// The still-open bookkeeping the collector keeps per live leg so it can attach
/// each fill's / snapshot's `trade_id`, the leg's opening premium, and the leg's
/// **opened** side (a close trades the opposite side, but the row reports the
/// leg's side).
#[derive(Debug, Clone, Copy)]
struct LegMeta {
    trade_id: TradeId,
    entry_premium: PriceCents,
    side: Side,
}

/// The in-loop collector that builds the run's [`FillRecord`] and
/// [`PositionSnapshot`] streams one event at a time (PB-1-safe).
///
/// Construct with [`BundleCollector::with_capacity`] at startup, call
/// [`BundleCollector::reserve`] once the opening leg count is known, then
/// [`BundleCollector::record_open_fill`] / [`BundleCollector::record_close_fill`]
/// as fills correlate and [`BundleCollector::collect_step`] once per step (after
/// the ledger settles), and [`BundleCollector::into_parts`] at the end.
#[derive(Debug)]
pub(crate) struct BundleCollector {
    /// One record per executed fill, in the loop's deterministic fill order.
    fills: Vec<FillRecord>,
    /// One snapshot per open leg per step (plus a terminal row per full close),
    /// pushed in `(step, then per-step event/inventory order)`; the writer sorts
    /// by `(step, position_id)`.
    positions: Vec<PositionSnapshot>,
    /// Live-leg bookkeeping, keyed by the stable leg id — ordered for
    /// determinism, grown only on an open (sparse).
    open_legs: BTreeMap<PositionId, LegMeta>,
}

impl BundleCollector {
    /// A collector whose fill `Vec` starts at `fill_capacity`. The position `Vec`
    /// starts empty; [`Self::reserve`] sizes it once the leg count is known.
    #[must_use]
    pub fn with_capacity(fill_capacity: usize) -> Self {
        Self {
            fills: Vec::with_capacity(fill_capacity),
            positions: Vec::new(),
            open_legs: BTreeMap::new(),
        }
    }

    /// Reserve both buffers once the opening leg count (`open_leg_count`) and the
    /// step count (`step_capacity`) are known, so warm-step pushes never
    /// reallocate (PB-1).
    ///
    /// Fills are reserved for `2 × open_leg_count` (each opening leg closes at
    /// most once for a hold-to-close run); positions for
    /// `step_capacity × open_leg_count + open_leg_count` (one open row per leg per
    /// step, plus one terminal row per leg). The fills buffer never grows on a
    /// warm step (fills are sparse open/close events). The positions buffer is
    /// pushed every step, so its zero-alloc guarantee is a **steady-state /
    /// constant-leg** one: a run that opens *more* legs than `open_leg_count`
    /// mid-run can exhaust this reservation, and the amortised regrowth may then
    /// land on a no-fill step — the in-scope strategies (iron condor / short
    /// strangle) open all legs at `on_start`, so the gate holds. Checked
    /// arithmetic (per `rules/global_rules.md`): an unreachable overflow simply
    /// skips the pre-reservation and lets the `Vec` grow by amortised `push`.
    pub fn reserve(&mut self, open_leg_count: usize, step_capacity: usize) {
        if let Some(want) = open_leg_count.checked_mul(2)
            && want > self.fills.capacity()
        {
            self.fills.reserve(want - self.fills.len());
        }
        if let Some(want) = step_capacity
            .checked_mul(open_leg_count)
            .and_then(|open_rows| open_rows.checked_add(open_leg_count))
            && want > self.positions.capacity()
        {
            self.positions.reserve(want - self.positions.len());
        }
    }

    /// Record a leg opening: push its [`FillRecord`] and start its live-leg
    /// bookkeeping (trade id, entry premium, opened side).
    pub fn record_open_fill(
        &mut self,
        fill: &Fill,
        order_id: u64,
        trade_id: TradeId,
        position_id: PositionId,
    ) {
        self.fills.push(FillRecord {
            fill: fill.clone(),
            trade_id: trade_id.value(),
            position_id: position_id.value(),
            order_id,
            fill_seq: 0,
        });
        self.open_legs.insert(
            position_id,
            LegMeta {
                trade_id,
                entry_premium: fill.price,
                side: fill.side,
            },
        );
    }

    /// Record a leg close: push its [`FillRecord`] and, on a **full** close, its
    /// terminal [`PositionSnapshot`] (observed at the beginning-of-step endpoint)
    /// and drop the live-leg bookkeeping. A **partial** close writes no terminal
    /// row — the reduced leg keeps taking ordinary open rows.
    ///
    /// `snapshot_mark` is the closing step's snapshot mid for the contract (a fill
    /// exists, so it is normally quoted); when absent, the entry premium is
    /// carried forward and the terminal row is flagged `stale_mark`.
    ///
    /// # Errors
    ///
    /// - [`BacktestError::Execution`] if `position_id` names no open leg (an
    ///   internal invariant — the loop validated the close against inventory
    ///   before calling this).
    /// - [`BacktestError::ArithmeticOverflow`] if the unrealised-P&L cents
    ///   arithmetic overflows.
    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per close-time signal (fill, order id, leg id, full-close flag, snapshot mark, multiplier, reason); the collector centralises the fill + terminal-row bookkeeping in one place"
    )]
    pub fn record_close_fill(
        &mut self,
        fill: &Fill,
        order_id: u64,
        position_id: PositionId,
        full_close: bool,
        snapshot_mark: Option<PriceCents>,
        contract_multiplier: u32,
        exit_reason: ExitReason,
    ) -> Result<(), BacktestError> {
        let leg = *self.open_legs.get(&position_id).ok_or_else(|| {
            BacktestError::Execution(format!(
                "bundle close targets position {} which is not open",
                position_id.value()
            ))
        })?;
        self.fills.push(FillRecord {
            fill: fill.clone(),
            trade_id: leg.trade_id.value(),
            position_id: position_id.value(),
            order_id,
            fill_seq: 0,
        });
        if full_close {
            // Terminal row: the beginning-of-step mark is the closing step's
            // snapshot mid (a fill exists ⇒ the contract is quoted). If somehow
            // absent, carry the entry premium forward and flag it stale rather
            // than fabricate a price.
            let (mark, stale) = match snapshot_mark {
                Some(mid) => (mid, false),
                None => (leg.entry_premium, true),
            };
            let unrealized = unrealized_cents(
                mark,
                leg.entry_premium,
                fill.quantity.value(),
                contract_multiplier,
                leg.side,
            )?;
            self.positions.push(PositionSnapshot {
                step: fill.step.value(),
                ts_ns: fill.ts.value(),
                position_id: position_id.value(),
                trade_id: leg.trade_id.value(),
                contract: fill.contract.clone(),
                side: leg.side,
                quantity: fill.quantity.value(),
                avg_price_cents: leg.entry_premium.value(),
                mark_cents: mark.value(),
                unrealized_cents: unrealized,
                stale_mark: stale,
                exit_reason: Some(exit_reason),
                open_at_end: false,
            });
            self.open_legs.remove(&position_id);
        }
        Ok(())
    }

    /// Collect one open [`PositionSnapshot`] per surviving open leg, from the
    /// ledger's per-step [`PositionMark`] hand-off, right after the ledger
    /// settled the step.
    ///
    /// `open_at_end` is `true` **only** at the final step: the surviving
    /// inventory there is exactly the legs left open when the feed exhausted, so
    /// their last open rows carry the flag ([docs/02 §3.3](../../../docs/02-engine-architecture.md#33-termination-feed-exhausted)).
    /// Legs closed this step are **not** in `marks` (the loop removed them before
    /// `settle`); their terminal rows were already pushed by
    /// [`Self::record_close_fill`].
    ///
    /// # Errors
    ///
    /// - [`BacktestError::Execution`] if a marked leg has no live-leg bookkeeping
    ///   (an internal invariant — every open leg was recorded at its open).
    /// - [`BacktestError::ArithmeticOverflow`] on the unrealised-P&L arithmetic.
    pub fn collect_step(
        &mut self,
        step: u32,
        ts_ns: i64,
        marks: &[PositionMark],
        open_at_end: bool,
    ) -> Result<(), BacktestError> {
        for mark in marks {
            let leg = *self.open_legs.get(&mark.position_id).ok_or_else(|| {
                BacktestError::Execution(format!(
                    "position snapshot for leg {} has no recorded open",
                    mark.position_id.value()
                ))
            })?;
            let unrealized = unrealized_cents(
                mark.mark,
                leg.entry_premium,
                mark.quantity.value(),
                mark.contract_multiplier,
                mark.side,
            )?;
            self.positions.push(PositionSnapshot {
                step,
                ts_ns,
                position_id: mark.position_id.value(),
                trade_id: leg.trade_id.value(),
                contract: mark.contract.clone(),
                side: mark.side,
                quantity: mark.quantity.value(),
                avg_price_cents: leg.entry_premium.value(),
                mark_cents: mark.mark.value(),
                unrealized_cents: unrealized,
                stale_mark: mark.stale_mark,
                exit_reason: None,
                open_at_end,
            });
        }
        Ok(())
    }

    /// Consume the collector into the owned `(fills, positions)` streams. Order is
    /// the loop's deterministic push order; the writer sorts each into its wire
    /// key order.
    #[must_use]
    pub fn into_parts(self) -> (Vec<FillRecord>, Vec<PositionSnapshot>) {
        (self.fills, self.positions)
    }
}

/// A leg's unrealised P&L in integer cents: `(mark − entry) × quantity ×
/// contract_multiplier × side_sign` (long `+`, short `−`) — the mark-to-market
/// value the closing fill converts to realised P&L
/// ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet),
/// [docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if any product or the difference
/// exceeds the `i64` cents range.
fn unrealized_cents(
    mark: PriceCents,
    entry: PriceCents,
    quantity: u32,
    contract_multiplier: u32,
    side: Side,
) -> Result<i64, BacktestError> {
    let diff = i128::from(mark.value())
        .checked_sub(i128::from(entry.value()))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let scaled = diff
        .checked_mul(i128::from(quantity))
        .and_then(|v| v.checked_mul(i128::from(contract_multiplier)))
        .and_then(|v| v.checked_mul(i128::from(sign_convention::side_sign(side))))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    i64::try_from(scaled).map_err(|_| BacktestError::ArithmeticOverflow)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::backtesting::ExitReason;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};

    use super::{BundleCollector, unrealized_cents};
    use crate::domain::{
        Cents, ContractKey, ExecutionMode, Fill, PositionId, PriceCents, Quantity, TradeId,
        Underlying,
    };
    use crate::engine::ledger::PositionMark;

    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        u
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is valid");
        };
        q
    }

    fn key(strike: u64) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                TS0 + 30 * NANOS_PER_DAY,
            )),
            strike: PriceCents::new(strike),
            style: OptionStyle::Call,
        }
    }

    fn open_fill(price: u64, step: u32) -> Fill {
        Fill {
            ts: crate::domain::SimTime::new(TS0 + i64::from(step) * NANOS_PER_DAY),
            step: crate::domain::StepIndex::new(step),
            contract: key(510_000),
            side: Side::Short,
            quantity: qty(2),
            price: PriceCents::new(price),
            fees: Cents::new(65),
            slippage: Cents::new(0),
            mode: ExecutionMode::Naive,
        }
    }

    fn close_fill(price: u64, step: u32, quantity: u32) -> Fill {
        Fill {
            ts: crate::domain::SimTime::new(TS0 + i64::from(step) * NANOS_PER_DAY),
            step: crate::domain::StepIndex::new(step),
            contract: key(510_000),
            side: Side::Long, // buy back a short leg
            quantity: qty(quantity),
            price: PriceCents::new(price),
            fees: Cents::new(65),
            slippage: Cents::new(0),
            mode: ExecutionMode::Naive,
        }
    }

    fn mark(mark_cents: u64, quantity: u32, stale: bool) -> PositionMark {
        PositionMark {
            position_id: PositionId::new(1),
            contract: key(510_000),
            side: Side::Short,
            quantity: qty(quantity),
            contract_multiplier: 100,
            mark: PriceCents::new(mark_cents),
            stale_mark: stale,
            prior_greeks: None,
        }
    }

    #[test]
    fn test_unrealized_cents_short_leg_mark_below_entry_is_a_gain() {
        // Short at 2000, marked at 1500, qty 2, mult 100:
        // (1500 − 2000) × 2 × 100 × side_sign(Short=−1) = +100_000.
        let pnl = unrealized_cents(
            PriceCents::new(1_500),
            PriceCents::new(2_000),
            2,
            100,
            Side::Short,
        );
        assert!(matches!(pnl, Ok(100_000)));
    }

    #[test]
    fn test_record_open_pushes_one_fill_and_no_position_row() {
        let mut collector = BundleCollector::with_capacity(4);
        collector.record_open_fill(&open_fill(2_000, 0), 1, TradeId::new(1), PositionId::new(1));
        let (fills, positions) = collector.into_parts();
        assert_eq!(fills.len(), 1);
        assert!(
            positions.is_empty(),
            "an open pushes no position row itself"
        );
        let Some(record) = fills.first() else {
            panic!("one fill record");
        };
        assert_eq!(record.trade_id, 1);
        assert_eq!(record.position_id, 1);
        assert_eq!(record.order_id, 1);
        assert_eq!(record.fill_seq, 0);
    }

    #[test]
    fn test_collect_step_emits_open_row_per_marked_leg() {
        let mut collector = BundleCollector::with_capacity(4);
        collector.record_open_fill(&open_fill(2_000, 0), 1, TradeId::new(7), PositionId::new(1));
        let Ok(()) = collector.collect_step(0, TS0, &[mark(1_900, 2, false)], false) else {
            panic!("collect step");
        };
        let (_fills, positions) = collector.into_parts();
        assert_eq!(positions.len(), 1);
        let Some(row) = positions.first() else {
            panic!("one open row");
        };
        assert_eq!(row.position_id, 1);
        assert_eq!(row.trade_id, 7);
        assert_eq!(row.avg_price_cents, 2_000);
        assert_eq!(row.mark_cents, 1_900);
        // (1900 − 2000) × 2 × 100 × (−1) = +20_000.
        assert_eq!(row.unrealized_cents, 20_000);
        assert!(row.exit_reason.is_none(), "open rows have no exit reason");
        assert!(!row.open_at_end);
    }

    #[test]
    fn test_full_close_writes_terminal_row_with_exit_reason() {
        let mut collector = BundleCollector::with_capacity(4);
        collector.record_open_fill(&open_fill(2_000, 0), 1, TradeId::new(7), PositionId::new(1));
        // Full close of 2 at step 3, snapshot mid 1_800.
        let Ok(()) = collector.record_close_fill(
            &close_fill(1_750, 3, 2),
            2,
            PositionId::new(1),
            true,
            Some(PriceCents::new(1_800)),
            100,
            ExitReason::Other("end_of_data".to_string()),
        ) else {
            panic!("record close");
        };
        // After a full close, a later collect_step over an empty mark set adds
        // nothing (the leg is gone from inventory).
        let Ok(()) = collector.collect_step(3, TS0, &[], true) else {
            panic!("collect step after close");
        };
        let (fills, positions) = collector.into_parts();
        assert_eq!(fills.len(), 2, "one open + one close fill");
        assert_eq!(positions.len(), 1, "exactly one terminal row");
        let Some(term) = positions.first() else {
            panic!("terminal row");
        };
        assert_eq!(term.step, 3);
        assert_eq!(term.quantity, 2, "contracts closed before the close");
        assert_eq!(
            term.mark_cents, 1_800,
            "the snapshot mid, not the fill price"
        );
        assert_eq!(
            term.side,
            Side::Short,
            "the leg's opened side, not the close"
        );
        // (1800 − 2000) × 2 × 100 × (−1) = +40_000.
        assert_eq!(term.unrealized_cents, 40_000);
        assert!(!term.open_at_end);
        assert!(matches!(
            term.exit_reason.as_ref(),
            Some(ExitReason::Other(reason)) if reason == "end_of_data"
        ));
    }

    #[test]
    fn test_partial_close_writes_no_terminal_row_and_leg_stays_open() {
        let mut collector = BundleCollector::with_capacity(4);
        collector.record_open_fill(&open_fill(2_000, 0), 1, TradeId::new(7), PositionId::new(1));
        // Partial close of 1 of 2 (full_close = false).
        let Ok(()) = collector.record_close_fill(
            &close_fill(1_900, 2, 1),
            2,
            PositionId::new(1),
            false,
            Some(PriceCents::new(1_900)),
            100,
            ExitReason::ManualClose,
        ) else {
            panic!("partial close");
        };
        // The reduced leg (qty 1) still takes an ordinary open row.
        let Ok(()) = collector.collect_step(2, TS0, &[mark(1_900, 1, false)], false) else {
            panic!("collect step");
        };
        let (fills, positions) = collector.into_parts();
        assert_eq!(fills.len(), 2);
        assert_eq!(positions.len(), 1, "only the surviving-leg open row");
        let Some(row) = positions.first() else {
            panic!("open row");
        };
        assert!(
            row.exit_reason.is_none(),
            "a partial close writes no terminal row"
        );
        assert_eq!(row.quantity, 1, "the reduced open size");
    }

    #[test]
    fn test_open_at_end_flag_set_on_final_step_open_rows() {
        let mut collector = BundleCollector::with_capacity(4);
        collector.record_open_fill(&open_fill(2_000, 0), 1, TradeId::new(7), PositionId::new(1));
        // Final step with the leg still open ⇒ open_at_end = true on its open row.
        let Ok(()) = collector.collect_step(5, TS0, &[mark(2_100, 2, false)], true) else {
            panic!("collect final step");
        };
        let (_fills, positions) = collector.into_parts();
        let Some(row) = positions.first() else {
            panic!("open row");
        };
        assert!(row.open_at_end, "a leg open at feed exhaustion is flagged");
        assert!(
            row.exit_reason.is_none(),
            "no terminal row for an open_at_end leg"
        );
    }

    #[test]
    fn test_reserve_grows_capacity_without_changing_len() {
        let mut collector = BundleCollector::with_capacity(0);
        collector.reserve(4, 64);
        assert!(collector.positions.capacity() >= 64 * 4);
        assert_eq!(collector.positions.len(), 0);
        assert_eq!(collector.fills.len(), 0);
    }
}
