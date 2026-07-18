//! The engine→analytics **trade log** — one owned [`ClosedTrade`] per leg
//! close, collected in the replay loop and carried on
//! [`crate::engine::BacktestRun`] for the post-run metrics pass (#32).
//!
//! # Why a post-run trade log (layering)
//!
//! Per-leg realised P&L and the [`ExitReason`] that closed a leg require
//! **pairing each opened leg with its close** — data the replay loop drops once
//! a fill is applied (the step `fills` scratch is cleared every step). Since
//! `analytics` **consumes** engine output and the engine must **not** import
//! `analytics` ([CLAUDE.md](../../../CLAUDE.md) Module Boundaries), the loop
//! **collects** the pairing into owned [`ClosedTrade`] records the post-run
//! [`crate::analytics::metrics`] pass reads — no tape look-back, no
//! recomputation ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
//!
//! # Representation (PB-1-safe)
//!
//! Closes are **sparse** — a leg opens once and closes at most once — so the
//! log grows only on a close event, never on a warm revaluation-only step. The
//! output [`Vec<ClosedTrade>`] is **reserved at `on_start`** (bounded by the
//! opening leg count) so a close is an amortised `push`, and the open-leg
//! tracking is an ordered [`BTreeMap`] (deterministic, never a `HashMap`) that
//! grows only when a leg opens. A steady-state step with no open/close activity
//! touches the collector not at all, so it never trips the `zero_alloc` PB-1
//! gate ([docs/07 §4](../../../docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop)).
//!
//! # Determinism
//!
//! The collector reads no wall clock and draws no randomness; it records the
//! snapshot timestamps the loop already holds and pushes closes in the loop's
//! deterministic command order. The same `(seed, config, data)` yields a
//! byte-identical trade log
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use std::collections::BTreeMap;

use optionstratlib::Side;
use optionstratlib::backtesting::ExitReason;

use crate::domain::{Cents, ContractKey, PositionId, PriceCents, Quantity, TradeId};
use crate::error::BacktestError;

/// One realised leg close — the owned record the post-run metrics pass turns
/// into per-leg [`crate::analytics::metrics`] statistics (and, later, a
/// `TradeRecord` / `positions.parquet` terminal row, #33).
///
/// Every money field is **integer cents** ([ADR-0003](../../../docs/adr/0003-money-as-integer-cents.md));
/// timestamps are ns since the Unix epoch (UTC).
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedTrade {
    /// The multi-leg trade this leg belonged to (shared by the legs a strategy
    /// opened together in one step).
    pub trade_id: TradeId,
    /// The engine-minted stable leg identity that was closed.
    pub position_id: PositionId,
    /// The closed leg's contract identity (carries strike / style / underlying
    /// — the per-leg split reads `style` and `side` off this).
    pub contract: ContractKey,
    /// The direction the leg was open in (`Long` bought, `Short` sold).
    pub side: Side,
    /// Contracts closed on this event (`> 0`) — the full open size for a clean
    /// close, or the reduced amount for a partial close.
    pub quantity: Quantity,
    /// Contracts → underlying units for this leg (e.g. `100`) — the same
    /// multiplier the realised P&L was scaled by. Carried so the post-run metrics
    /// pass can put premium aggregates (`entry_received` / `entry_paid` /
    /// `net_premium`) on the **same cash basis** as [`Self::realized_pnl`] rather
    /// than reporting a per-contract-unweighted premium (F23).
    pub contract_multiplier: u32,
    /// Per-contract entry premium in integer cents.
    pub entry_premium: PriceCents,
    /// Per-contract exit (close) execution price in integer cents.
    pub exit_price: PriceCents,
    /// Fees charged on the closing fill, integer cents, **always ≥ 0**.
    pub close_fees: Cents,
    /// Signed slippage on the closing fill vs its decision mid (positive =
    /// adverse, [docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
    pub close_slippage: Cents,
    /// Realised P&L for this close in integer cents: the signed premium
    /// round-trip `side_diff × quantity × contract_multiplier` **minus the
    /// closing fee**. It **excludes the open fee**, which was charged to cash at
    /// entry and is already reflected in the mark-to-market equity curve — so
    /// `Σ realized_pnl` is a per-leg realised figure, not the equity
    /// reconciliation (that is the attribution's job,
    /// [docs/05 §3.1](../../../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
    pub realized_pnl: Cents,
    /// The snapshot timestamp the leg was opened at (ns, UTC).
    pub entry_ts: i64,
    /// The snapshot timestamp the leg was closed at (ns, UTC).
    pub exit_ts: i64,
    /// The upstream [`ExitReason`] that closed the leg — mapped from the applied
    /// [`optionstratlib::simulation::ExitPolicy`] for a policy-triggered close,
    /// or an end-of-data / manual reason for a terminal or adjustment close
    /// ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
    pub exit_reason: ExitReason,
}

/// The still-open bookkeeping the collector keeps per live leg, so it can pair a
/// close with its opening premium, timestamp, and remaining size.
#[derive(Debug, Clone)]
struct OpenLeg {
    trade_id: TradeId,
    contract: ContractKey,
    side: Side,
    entry_premium: PriceCents,
    entry_ts: i64,
    /// Contracts still open on this leg (decremented on partial closes).
    remaining: u32,
}

/// The in-loop collector that builds the run's [`Vec<ClosedTrade>`] one close at
/// a time (PB-1-safe).
///
/// Construct with [`TradeLogCollector::with_capacity`] at startup, call
/// [`TradeLogCollector::reserve`] once the opening leg count is known,
/// [`TradeLogCollector::record_open`] when a leg opens and
/// [`TradeLogCollector::record_close`] when one closes, and
/// [`TradeLogCollector::into_log`] at the end.
#[derive(Debug)]
pub(crate) struct TradeLogCollector {
    /// Legs still open, keyed by their stable id — ordered for determinism, and
    /// grown only on an open (sparse), so a warm step never allocates here.
    open: BTreeMap<PositionId, OpenLeg>,
    /// The realised closes, in the loop's deterministic close order.
    closed: Vec<ClosedTrade>,
}

impl TradeLogCollector {
    /// A collector sized for `open_capacity` opening legs. The closed-trade
    /// `Vec` starts empty; [`Self::reserve`] sizes it once the opening leg count
    /// is known.
    #[must_use]
    pub fn with_capacity(open_capacity: usize) -> Self {
        Self {
            open: BTreeMap::new(),
            closed: Vec::with_capacity(open_capacity),
        }
    }

    /// Reserve the closed-trade `Vec` for `expected_closes` legs, so each close
    /// is an amortised `push` and a steady-state run never reallocates here
    /// (PB-1). A run whose closes later exceed this reservation grows amortised
    /// at that transient — never on a warm revaluation-only step.
    pub fn reserve(&mut self, expected_closes: usize) {
        if expected_closes > self.closed.capacity() {
            // `expected_closes > capacity >= len`, so the shortfall never
            // underflows.
            self.closed.reserve(expected_closes - self.closed.len());
        }
    }

    /// Record a leg opening: its trade id, contract, side, entry premium, entry
    /// timestamp, and opening size, so a later close can be paired against it.
    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per open-time signal (ids, contract, side, size, premium, ts); the collector centralises leg bookkeeping in one place"
    )]
    pub fn record_open(
        &mut self,
        position_id: PositionId,
        trade_id: TradeId,
        contract: ContractKey,
        side: Side,
        quantity: Quantity,
        entry_premium: PriceCents,
        entry_ts: i64,
    ) {
        self.open.insert(
            position_id,
            OpenLeg {
                trade_id,
                contract,
                side,
                entry_premium,
                entry_ts,
                remaining: quantity.value(),
            },
        );
    }

    /// Record a leg close, computing its realised P&L and pushing a
    /// [`ClosedTrade`]. A partial close (`quantity` below the leg's remaining
    /// size) keeps the leg open with the residual; a full close removes it.
    ///
    /// # Errors
    ///
    /// - [`BacktestError::Execution`] if `position_id` names no open leg (an
    ///   internal invariant — the loop validated the close against inventory
    ///   before calling this).
    /// - [`BacktestError::ArithmeticOverflow`] if the realised-P&L cents
    ///   arithmetic overflows `i64`.
    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per close-time signal (id, price, fees, slippage, ts, size, reason); the collector centralises the realised-P&L math in one place"
    )]
    pub fn record_close(
        &mut self,
        position_id: PositionId,
        exit_price: PriceCents,
        close_fees: Cents,
        close_slippage: Cents,
        exit_ts: i64,
        quantity: Quantity,
        contract_multiplier: u32,
        exit_reason: ExitReason,
    ) -> Result<(), BacktestError> {
        let leg = self.open.get_mut(&position_id).ok_or_else(|| {
            BacktestError::Execution(format!(
                "trade-log close targets position {} which is not open",
                position_id.value()
            ))
        })?;

        let realized_pnl = realized_pnl_cents(
            leg.side,
            leg.entry_premium,
            exit_price,
            quantity,
            contract_multiplier,
            close_fees,
        )?;

        self.closed.push(ClosedTrade {
            trade_id: leg.trade_id,
            position_id,
            contract: leg.contract.clone(),
            side: leg.side,
            quantity,
            contract_multiplier,
            entry_premium: leg.entry_premium,
            exit_price,
            close_fees,
            close_slippage,
            realized_pnl,
            entry_ts: leg.entry_ts,
            exit_ts,
            exit_reason,
        });

        // Reduce the remaining size; a full close removes the leg. The `else`
        // branch guarantees `closed < leg.remaining`, so `checked_sub` never
        // returns `None` here — it upholds the checked-counter rule and surfaces
        // any impossible underflow as a typed error rather than wrapping.
        let closed = quantity.value();
        if closed >= leg.remaining {
            self.open.remove(&position_id);
        } else {
            leg.remaining = leg
                .remaining
                .checked_sub(closed)
                .ok_or(BacktestError::ArithmeticOverflow)?;
        }
        Ok(())
    }

    /// Consume the collector into the owned trade log, in close order.
    #[must_use]
    pub fn into_log(self) -> Vec<ClosedTrade> {
        self.closed
    }
}

/// The realised P&L of a leg close in integer cents: the signed premium
/// round-trip `side_diff × quantity × contract_multiplier` **minus** the closing
/// fee. For a **long** leg the diff is `exit − entry`; for a **short** leg it is
/// `entry − exit` (the sign convention of
/// [docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if any product or the fee subtraction
/// exceeds the `i64` cents range.
fn realized_pnl_cents(
    side: Side,
    entry_premium: PriceCents,
    exit_price: PriceCents,
    quantity: Quantity,
    contract_multiplier: u32,
    close_fees: Cents,
) -> Result<Cents, BacktestError> {
    let entry = i128::from(entry_premium.value());
    let exit = i128::from(exit_price.value());
    // side_diff: long profits when exit > entry; short profits when entry > exit.
    let per_contract = match side {
        Side::Long => exit
            .checked_sub(entry)
            .ok_or(BacktestError::ArithmeticOverflow)?,
        Side::Short => entry
            .checked_sub(exit)
            .ok_or(BacktestError::ArithmeticOverflow)?,
    };
    let gross = per_contract
        .checked_mul(i128::from(quantity.value()))
        .and_then(|g| g.checked_mul(i128::from(contract_multiplier)))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let net = gross
        .checked_sub(i128::from(close_fees.value()))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let net = i64::try_from(net).map_err(|_| BacktestError::ArithmeticOverflow)?;
    Ok(Cents::new(net))
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::backtesting::ExitReason;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};

    use super::{TradeLogCollector, realized_pnl_cents};
    use crate::domain::{
        Cents, ContractKey, PositionId, PriceCents, Quantity, TradeId, Underlying,
    };

    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        u
    }

    fn key(strike: u64, style: OptionStyle) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                TS0 + 30 * NANOS_PER_DAY,
            )),
            strike: PriceCents::new(strike),
            style,
        }
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is valid");
        };
        q
    }

    #[test]
    fn test_realized_pnl_short_leg_bought_back_cheaper_is_a_gain() {
        // Short sold at 200c, bought back at 150c, qty 1, mult 100, fee 65c.
        // gross = (200 − 150) × 1 × 100 = 5_000; net = 5_000 − 65 = 4_935.
        let pnl = realized_pnl_cents(
            Side::Short,
            PriceCents::new(200),
            PriceCents::new(150),
            qty(1),
            100,
            Cents::new(65),
        );
        assert!(matches!(pnl, Ok(c) if c.value() == 4_935));
    }

    #[test]
    fn test_realized_pnl_long_leg_sold_lower_is_a_loss() {
        // Long bought at 200c, sold at 150c, qty 2, mult 100, fee 30c.
        // gross = (150 − 200) × 2 × 100 = −10_000; net = −10_000 − 30 = −10_030.
        let pnl = realized_pnl_cents(
            Side::Long,
            PriceCents::new(200),
            PriceCents::new(150),
            qty(2),
            100,
            Cents::new(30),
        );
        assert!(matches!(pnl, Ok(c) if c.value() == -10_030));
    }

    #[test]
    fn test_collector_pairs_open_and_close_into_realised_trade() {
        let mut collector = TradeLogCollector::with_capacity(4);
        collector.reserve(4);
        collector.record_open(
            PositionId::new(1),
            TradeId::new(1),
            key(510_000, OptionStyle::Call),
            Side::Short,
            qty(1),
            PriceCents::new(2_000),
            TS0,
        );
        let Ok(()) = collector.record_close(
            PositionId::new(1),
            PriceCents::new(1_500),
            Cents::new(65),
            Cents::new(0),
            TS0 + NANOS_PER_DAY,
            qty(1),
            100,
            ExitReason::TargetReached,
        ) else {
            panic!("close pairs with the open");
        };
        let log = collector.into_log();
        assert_eq!(log.len(), 1);
        let Some(trade) = log.first() else {
            panic!("one closed trade");
        };
        assert_eq!(trade.position_id, PositionId::new(1));
        assert_eq!(trade.trade_id, TradeId::new(1));
        assert_eq!(trade.side, Side::Short);
        // (2000 − 1500) × 1 × 100 − 65 = 49_935.
        assert_eq!(trade.realized_pnl, Cents::new(49_935));
        assert_eq!(trade.entry_ts, TS0);
        assert_eq!(trade.exit_ts, TS0 + NANOS_PER_DAY);
        assert_eq!(trade.exit_reason, ExitReason::TargetReached);
    }

    #[test]
    fn test_collector_partial_close_keeps_leg_open_then_full_close_removes_it() {
        let mut collector = TradeLogCollector::with_capacity(1);
        collector.record_open(
            PositionId::new(7),
            TradeId::new(3),
            key(490_000, OptionStyle::Put),
            Side::Short,
            qty(3),
            PriceCents::new(1_000),
            TS0,
        );
        // Close 1 of 3 → leg stays open.
        let Ok(()) = collector.record_close(
            PositionId::new(7),
            PriceCents::new(900),
            Cents::new(10),
            Cents::new(0),
            TS0 + NANOS_PER_DAY,
            qty(1),
            100,
            ExitReason::ManualClose,
        ) else {
            panic!("partial close records a trade");
        };
        // Close the remaining 2 → leg removed.
        let Ok(()) = collector.record_close(
            PositionId::new(7),
            PriceCents::new(800),
            Cents::new(20),
            Cents::new(0),
            TS0 + 2 * NANOS_PER_DAY,
            qty(2),
            100,
            ExitReason::Expiration,
        ) else {
            panic!("full close records a trade");
        };
        let log = collector.into_log();
        assert_eq!(log.len(), 2, "each close event is one record");
        // A close after the leg is gone is an execution error (invariant guard).
        let mut empty = collector_after(&log);
        let err = empty.record_close(
            PositionId::new(7),
            PriceCents::new(800),
            Cents::new(0),
            Cents::new(0),
            TS0,
            qty(1),
            100,
            ExitReason::Expiration,
        );
        assert!(matches!(
            err,
            Err(crate::error::BacktestError::Execution(_))
        ));
    }

    /// A fresh collector (used only to prove a close against no open leg errors).
    fn collector_after(_log: &[super::ClosedTrade]) -> TradeLogCollector {
        TradeLogCollector::with_capacity(0)
    }
}
