//! The mark-to-market ledger — minimal for v0.1, enriched by issue #15.
//!
//! [`Ledger`] holds the run's cash and equity peak and produces one
//! [`EquityPoint`] per step ([docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)).
//! The replay loop ([`crate::engine::backtest`]) calls it in the single
//! ledger-mutation phase of each step: it applies the step's fills to cash
//! ([`Ledger::apply_fill`]), then re-marks the open legs at the snapshot's mid
//! and emits the step's one equity point ([`Ledger::settle`]).
//!
//! # Scope (this is the minimal #14 ledger; #15 enriches it in place)
//!
//! This implementation computes `cash`, `position_value`, `equity`, and
//! `drawdown` correctly — cash moves **only** through fills and fees, the
//! valuation identity `equity = cash + position_value` holds each step, and
//! `drawdown` is never clamped. Issue #15 owns the **rigorous** rules layered
//! on top: the cash-conservation and equity-reconciliation property suite,
//! the settlement-mark rejection at/after a held contract's expiry step, and a
//! richer `stale_mark` model. The minimal carry-forward here already keeps a
//! held contract's **last-known mid** across a step where it is unquoted, so
//! the loop is correct-but-minimal, not wrong.
//!
//! # Determinism
//!
//! The ledger reads no wall clock and draws no randomness. It keys its
//! last-known marks in a [`BTreeMap`] (ordered, never a `HashMap`), and every
//! money operation is checked integer-cents arithmetic — a silent wrap in the
//! P&L ledger would be a correctness bug ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use std::collections::BTreeMap;

use crate::domain::execution::sign_convention;
use crate::domain::{
    Cents, ChainSnapshot, ContractKey, EquityPoint, Fill, OpenPosition, PriceCents, SimTime,
    StepIndex,
};
use crate::error::BacktestError;

/// The run's cash-and-mark ledger.
///
/// Construct with [`Ledger::new`] at startup (cash = the run's initial
/// capital); the loop then drives [`Ledger::apply_fill`] for each fill and one
/// [`Ledger::settle`] per step.
#[derive(Debug, Clone)]
pub struct Ledger {
    /// Account cash in integer cents — moves only through fills and fees.
    cash: Cents,
    /// The running equity peak in integer cents, `max(peak, equity)` each step;
    /// initialised to the run's initial capital (`> 0`), so it is never zero.
    running_peak: i64,
    /// Last-known mid per held contract, carried forward for a contract absent
    /// from a later snapshot (`stale_mark`,
    /// [docs/01 §6](../../../docs/01-domain-model.md#6-market-data)). Ordered by
    /// [`ContractKey`] for determinism; grows only when a new contract is first
    /// marked (warmup), so a warm step body does not allocate here.
    marks: BTreeMap<ContractKey, PriceCents>,
}

impl Ledger {
    /// Create a ledger with `initial_capital` cash and the equity peak anchored
    /// at the same value.
    ///
    /// `initial_capital` must be `> 0` (the caller guarantees it, matching
    /// `config.validate`) so the drawdown denominator is never zero.
    #[must_use]
    pub fn new(initial_capital: Cents) -> Self {
        Self {
            cash: initial_capital,
            running_peak: initial_capital.value(),
            marks: BTreeMap::new(),
        }
    }

    /// The current cash balance in integer cents.
    #[must_use]
    pub const fn cash(&self) -> Cents {
        self.cash
    }

    /// Apply one fill's cash flow: a **credit** for a sell (short side), a
    /// **debit** for a buy (long side), always **minus** the fee.
    ///
    /// Cash flow is `−side_sign × price × quantity × contract_multiplier`
    /// (selling a short leg credits `+`, buying a long leg debits `−`,
    /// [docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)),
    /// then the fee (always `≥ 0`) is subtracted. Cash moves **only** here,
    /// never in [`Ledger::settle`].
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] if the scaled cash flow,
    /// the fee subtraction, or the running cash total exceeds the integer-cents
    /// range.
    pub fn apply_fill(
        &mut self,
        fill: &Fill,
        contract_multiplier: u32,
    ) -> Result<(), BacktestError> {
        let gross = i128::from(fill.price.value())
            .checked_mul(i128::from(fill.quantity.value()))
            .and_then(|g| g.checked_mul(i128::from(contract_multiplier)))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        // -side_sign: sell (Short) credits (+), buy (Long) debits (-).
        let signed_flow = gross
            .checked_mul(-i128::from(sign_convention::side_sign(fill.side)))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let net = signed_flow
            .checked_sub(i128::from(fill.fees.value()))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let net = i64::try_from(net).map_err(|_| BacktestError::ArithmeticOverflow)?;
        self.cash = self.cash.checked_add(Cents::new(net))?;
        Ok(())
    }

    /// Re-mark the open legs at `snapshot`'s mid and emit the step's single
    /// [`EquityPoint`].
    ///
    /// A held leg quoted this step marks at its snapshot mid (and refreshes the
    /// carried-forward value); a leg whose contract is **absent** this step
    /// keeps its last-known mid, falling back to its entry premium if it has
    /// never been quoted. `position_value = Σ (mark × quantity ×
    /// contract_multiplier × side_sign)` (long `+`, short `−`), `equity = cash
    /// + position_value`, the peak is raised to `max(peak, equity)`, and
    /// `drawdown = (equity − peak) / peak` (never clamped).
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] if a leg's marked value,
    /// the position-value sum, or `cash + position_value` exceeds range.
    pub fn settle(
        &mut self,
        step: StepIndex,
        ts: SimTime,
        open: &[OpenPosition],
        snapshot: &ChainSnapshot,
    ) -> Result<EquityPoint, BacktestError> {
        let multiplier = i128::from(snapshot.spec.contract_multiplier);
        let mut position_value: i128 = 0;
        for leg in open {
            let mark = self.resolve_mark(leg, snapshot);
            let leg_value = i128::from(mark.value())
                .checked_mul(i128::from(leg.quantity.value()))
                .and_then(|v| v.checked_mul(multiplier))
                .and_then(|v| v.checked_mul(i128::from(sign_convention::side_sign(leg.side))))
                .ok_or(BacktestError::ArithmeticOverflow)?;
            position_value = position_value
                .checked_add(leg_value)
                .ok_or(BacktestError::ArithmeticOverflow)?;
        }
        let position_value_cents =
            i64::try_from(position_value).map_err(|_| BacktestError::ArithmeticOverflow)?;
        let equity_cents = self
            .cash
            .value()
            .checked_add(position_value_cents)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if equity_cents > self.running_peak {
            self.running_peak = equity_cents;
        }
        let drawdown = self.drawdown(equity_cents);
        Ok(EquityPoint::new(
            step.value(),
            ts.value(),
            self.cash.value(),
            position_value_cents,
            equity_cents,
            drawdown,
        ))
    }

    /// Resolve a leg's mark this step: the snapshot mid if the contract is
    /// quoted (refreshing the carried-forward value), else the last-known mid,
    /// else the entry premium.
    fn resolve_mark(&mut self, leg: &OpenPosition, snapshot: &ChainSnapshot) -> PriceCents {
        if let Some(quote) = snapshot.quotes.get(&leg.contract) {
            // Update the carried-forward mid. `get_mut` avoids cloning the key
            // on the warm path; the clone only happens the first time a
            // contract is marked (warmup), keeping the step body allocation-free.
            match self.marks.get_mut(&leg.contract) {
                Some(slot) => *slot = quote.mid,
                None => {
                    self.marks.insert(leg.contract.clone(), quote.mid);
                }
            }
            quote.mid
        } else {
            self.marks
                .get(&leg.contract)
                .copied()
                .unwrap_or(leg.entry_premium)
        }
    }

    /// `(equity − running_peak) / running_peak`, never clamped; `0` at the
    /// degenerate `running_peak ≤ 0` (unreachable for a valid run).
    #[allow(
        clippy::cast_precision_loss,
        reason = "drawdown is the one documented analytic float; f64 is its wire type (docs/01 §9)"
    )]
    fn drawdown(&self, equity_cents: i64) -> f64 {
        if self.running_peak <= 0 {
            return 0.0;
        }
        (equity_cents as f64 - self.running_peak as f64) / self.running_peak as f64
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rust_decimal_macros::dec;

    use super::Ledger;
    use crate::domain::{
        Cents, ChainSnapshot, ContractKey, ExecutionMode, Fill, InstrumentSpec, OpenPosition,
        PositionId, PriceCents, Quantity, QuoteView, SimTime, StepIndex, Underlying,
    };

    const TS0: i64 = 1_750_291_200_000_000_000;

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

    fn key(strike: u64, style: OptionStyle) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(TS0)),
            strike: PriceCents::new(strike),
            style,
        }
    }

    fn quote(strike: u64, style: OptionStyle, mid: u64) -> QuoteView {
        // Fixtures always pass `mid >= 1`, so the bid stays non-negative without
        // `saturating_*` (banned by the rules, even in tests).
        debug_assert!(mid >= 1, "quote fixtures use a mid of at least 1c");
        QuoteView {
            contract: key(strike, style),
            bid: PriceCents::new(mid - 1),
            ask: PriceCents::new(mid + 1),
            mid: PriceCents::new(mid),
            bid_size: qty(10),
            ask_size: qty(10),
            implied_volatility: dec!(0.2),
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    fn snapshot(step: u32, contract_mid: Option<(u64, OptionStyle, u64)>) -> ChainSnapshot {
        let mut quotes = BTreeMap::new();
        if let Some((strike, style, mid)) = contract_mid {
            let q = quote(strike, style, mid);
            quotes.insert(q.contract.clone(), q);
        }
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(1), 100) else {
            panic!("valid spec");
        };
        ChainSnapshot {
            ts: SimTime::new(TS0 + i64::from(step)),
            step: StepIndex::new(step),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes,
        }
    }

    fn short_call(mid_entry: u64) -> OpenPosition {
        OpenPosition {
            position_id: PositionId::new(1),
            contract: key(510_000, OptionStyle::Call),
            side: Side::Short,
            quantity: qty(1),
            entry_premium: PriceCents::new(mid_entry),
        }
    }

    fn sell_fill(price: u64, fees: i64) -> Fill {
        Fill {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            contract: key(510_000, OptionStyle::Call),
            side: Side::Short,
            quantity: qty(1),
            price: PriceCents::new(price),
            fees: Cents::new(fees),
            slippage: Cents::new(0),
            mode: ExecutionMode::Naive,
        }
    }

    #[test]
    fn test_ledger_sell_credits_cash_minus_fees() {
        // Sell a short call at 200c, 1 contract, 100x multiplier, fee 65c.
        // cash delta = +200 * 1 * 100 - 65 = +19_935.
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let result = ledger.apply_fill(&sell_fill(200, 65), 100);
        assert!(matches!(result, Ok(())));
        assert_eq!(ledger.cash().value(), 10_000_000 + 20_000 - 65);
    }

    #[test]
    fn test_ledger_settle_short_leg_marks_as_liability() {
        // Sell short call for +20_000 cash (fee 0), then mark it at 200c mid:
        // position_value = 200 * 1 * 100 * side_sign(Short=-1) = -20_000.
        // equity = (10_000_000 + 20_000) + (-20_000) = 10_000_000. At a fresh
        // sell of a fairly-priced option, equity is unchanged (drawdown 0).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let Ok(()) = ledger.apply_fill(&sell_fill(200, 0), 100) else {
            panic!("apply fill");
        };
        let snap = snapshot(0, Some((510_000, OptionStyle::Call, 200)));
        let point = ledger.settle(StepIndex::new(0), snap.ts, &[short_call(200)], &snap);
        let Ok(point) = point else {
            panic!("settle succeeds");
        };
        assert_eq!(point.cash_cents, 10_000_000 + 20_000);
        assert_eq!(point.position_value_cents, -20_000);
        assert_eq!(point.equity_cents, 10_000_000);
        assert!(
            (point.drawdown - 0.0).abs() < f64::EPSILON,
            "fresh peak → 0 drawdown"
        );
    }

    #[test]
    fn test_ledger_drawdown_is_negative_below_peak_never_clamped() {
        // No fills; peak = 10_000_000. Mark a short leg richer than entry so the
        // liability grows and equity falls below the peak → negative drawdown.
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        // Short leg with no cash applied: position_value = -(300*100) = -30_000.
        let snap = snapshot(1, Some((510_000, OptionStyle::Call, 300)));
        let point = ledger.settle(StepIndex::new(1), snap.ts, &[short_call(200)], &snap);
        let Ok(point) = point else {
            panic!("settle succeeds");
        };
        assert_eq!(point.equity_cents, 10_000_000 - 30_000);
        assert!(
            point.drawdown < 0.0,
            "equity below peak → negative drawdown"
        );
    }

    #[test]
    fn test_ledger_carries_last_known_mark_when_contract_absent() {
        // Step 0 marks the leg at 250c, step 1 has no quote for it → the
        // last-known mid (250c) is carried forward, not the entry premium (200c).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let leg = short_call(200);
        let snap0 = snapshot(0, Some((510_000, OptionStyle::Call, 250)));
        let Ok(p0) = ledger.settle(
            StepIndex::new(0),
            snap0.ts,
            std::slice::from_ref(&leg),
            &snap0,
        ) else {
            panic!("settle 0");
        };
        assert_eq!(p0.position_value_cents, -25_000);
        let snap1 = snapshot(1, None); // the leg's contract is absent
        let Ok(p1) = ledger.settle(StepIndex::new(1), snap1.ts, &[leg], &snap1) else {
            panic!("settle 1");
        };
        // Carried forward 250c (not the 200c entry premium).
        assert_eq!(p1.position_value_cents, -25_000);
    }
}
