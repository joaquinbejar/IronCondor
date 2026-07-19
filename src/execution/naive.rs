//! The naive fill model: a reference price plus configured slippage and
//! fees, with no order book, no state, and no randomness.
//!
//! [`NaiveFill`] is a **pure function of the snapshot and config**. It always
//! fills the full intent, immediately, single-shot, at a price derived from
//! the current snapshot's quote plus the configured [`SlippageModel`]
//! ([docs/04 §3](../../../docs/04-execution-models.md)). This is the fast path
//! for research loops and parameter sweeps, and the criterion throughput
//! baseline the naive-throughput NFR is measured against.
//!
//! # Reference price: v0.1 fixes it at `mid`
//!
//! [docs/04 §3](../../../docs/04-execution-models.md) writes the reference as
//! `mid ± (half_spread × spread_fraction)`, where the reference
//! `spread_fraction` positions the fill between mid (`0.0`) and the touch
//! (`1.0`). That reference `spread_fraction` is a **separate, future** knob
//! from the [`SlippageModel::SpreadFraction`] slippage variant, and
//! [`crate::config::BacktestConfig`] carries **no** field for it in v0.1.
//! So v0.1 fixes the reference `spread_fraction = 0` ⇒ **`reference = mid`**,
//! and *all* adverse movement comes from the configured [`SlippageModel`]
//! (§4). The [`SlippageModel::SpreadFraction`] variant is the mechanism that
//! reaches the touch: a `fraction` of `1.0` applies a full `half_spread`
//! adverse, moving a buy from `mid` to the `ask` and a sell from `mid` to the
//! `bid` — exactly "1.0 fills at the touch".
//!
//! # Honest framing (not a disclaimer)
//!
//! Naive mode **systematically flatters strategies**: it always fills the full
//! size at a reference price and cannot express "the book was too thin"
//! ([docs/04 §9](../../../docs/04-execution-models.md)). Its slippage is a
//! configured guess, not a measured cost. It is the right tool for fast
//! iteration, parameter sweeps, and sign-of-the-edge checks — and the wrong
//! tool for "does this survive real execution?", which is realistic mode's job
//! (v0.2). A strategy that looks profitable here can still lose on fill risk.
//!
//! # Determinism
//!
//! [`NaiveFill`] holds no RNG and reads no clock; its only state is the two
//! configured values ([`SlippageModel`], [`FeeSchedule`]). Two `fill` calls
//! with identical `(commands, snapshot)` therefore produce byte-identical
//! [`Fill`]s ([rules/global_rules.md](../../../rules/global_rules.md)
//! "Determinism").

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};

use optionstratlib::Side;

use crate::config::{FeeSchedule, SlippageModel};
use crate::domain::{
    ChainSnapshot, ExecutionMode, Fill, OrderCommand, OrderIntent, PriceCents, Quantity, QuoteView,
};
use crate::error::BacktestError;

use super::{ExecutionModel, FeeCharge, FillDraft, assemble_fill};

/// The naive fill model: reference price (`mid`) plus configured adverse
/// slippage and fees, always filling the full intent single-shot.
///
/// It owns the two small config values it needs — the [`SlippageModel`] and
/// the [`FeeSchedule`] — rather than borrowing the whole
/// [`crate::config::BacktestConfig`], so the model is self-contained and the
/// engine can construct it once at `on_start` and drive it every step without
/// a lifetime tie to the config. Both values are small (`FeeSchedule` is
/// `Copy`; `SlippageModel` is a small `Clone` enum), so owning them is cheap.
///
/// The struct has **no RNG field**: naive mode is deterministic by
/// construction and cannot touch a random source.
#[derive(Debug, Clone, PartialEq)]
pub struct NaiveFill {
    /// The configured adverse slippage applied on top of the reference price.
    slippage: SlippageModel,
    /// The fee schedule stamped onto each fill.
    fees: FeeSchedule,
}

impl NaiveFill {
    /// Build a naive fill model from the two config values it needs.
    ///
    /// The `slippage` model and `fee` schedule are copied out of the run
    /// config so the model is self-contained and holds no borrow of it.
    #[must_use = "the constructed fill model must be used to produce fills"]
    pub const fn new(slippage: SlippageModel, fees: FeeSchedule) -> Self {
        Self { slippage, fees }
    }

    /// The adverse offset in integer cents for one fill, per the configured
    /// [`SlippageModel`] (§4). Always a non-negative magnitude that *worsens*
    /// the fill; never reads an RNG.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] when a size-proportional
    /// or spread-fraction product exceeds range, and
    /// [`BacktestError::CrossedQuote`] if a (malformed) quote has `bid > ask`.
    #[must_use = "the computed adverse offset must be applied to the fill price"]
    #[inline]
    fn adverse_offset_cents(
        &self,
        quote: &QuoteView,
        quantity: Quantity,
    ) -> Result<u64, BacktestError> {
        match &self.slippage {
            SlippageModel::None => Ok(0),
            SlippageModel::FixedCents { cents } => Ok(*cents),
            SlippageModel::SpreadFraction { fraction } => {
                spread_fraction_offset_cents(quote.bid, quote.ask, *fraction)
            }
            SlippageModel::SizeProportional { cents_per_contract } => cents_per_contract
                .checked_mul(u64::from(quantity.value()))
                .ok_or(BacktestError::ArithmeticOverflow),
        }
    }

    /// Fill one `Submit` intent against the snapshot's quote, producing the
    /// single [`Fill`] naive mode always yields for it.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when the intent's contract is not
    /// quoted in the snapshot (nothing to fill against), and propagates the
    /// arithmetic/quote errors from the slippage and assembly helpers.
    #[inline]
    fn fill_intent(
        &self,
        intent: &OrderIntent,
        snap: &ChainSnapshot,
    ) -> Result<Fill, BacktestError> {
        let quote = snap.quotes.get(&intent.contract).ok_or_else(|| {
            BacktestError::Execution(format!(
                "naive fill: contract with strike {} not quoted in snapshot at step {}",
                intent.contract.strike.value(),
                snap.step.value()
            ))
        })?;
        let offset = self.adverse_offset_cents(quote, intent.quantity)?;
        let price = naive_fill_price(quote.mid, intent.side, offset)?;
        // The single `ContractKey` clone per emitted fill: built once from the
        // borrowed intent and moved into `assemble_fill` by value.
        let draft = FillDraft {
            ts: snap.ts,
            step: snap.step,
            contract: intent.contract.clone(),
            side: intent.side,
            quantity: intent.quantity,
            price,
            decision_mid: intent.decision_mid,
        };
        // Naive fills are single-shot, so the order's only fill is its first:
        // it carries the once-per-order fee (`FeeCharge::FirstFill`).
        assemble_fill(
            draft,
            ExecutionMode::Naive,
            &self.fees,
            FeeCharge::FirstFill,
        )
    }
}

impl ExecutionModel for NaiveFill {
    /// Append one [`Fill`] per `Submit` intent to `out_fills`.
    ///
    /// Naive mode keeps **no resting book**: every `Submit` fills immediately
    /// and single-shot, so there is never a resting order for a `Cancel` or a
    /// `Replace` to act on. Both therefore append **no** fills. The trait's
    /// "process `Cancel`/`Replace` before `Submit`" ordering is honoured
    /// vacuously — their effect is empty, so the output is identical whatever
    /// the command order. (A strategy that relies on resting orders belongs in
    /// realistic mode, v0.2.)
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when a `Submit`'s contract is not
    /// quoted in `snap`, or an arithmetic error from the fill computation.
    fn fill(
        &mut self,
        commands: &[OrderCommand],
        _submit_ids: &[crate::domain::OrderId],
        snap: &ChainSnapshot,
        out_fills: &mut Vec<Fill>,
    ) -> Result<(), BacktestError> {
        for command in commands {
            match command {
                OrderCommand::Submit(intent) => {
                    out_fills.push(self.fill_intent(intent, snap)?);
                }
                // No resting order exists in naive mode; nothing to cancel or
                // replace, so both append nothing (see the method docs).
                OrderCommand::Cancel(_) | OrderCommand::Replace { .. } => {}
            }
        }
        Ok(())
    }

    #[inline]
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Naive
    }
}

/// `round(half_spread × fraction)` in integer cents, with
/// `half_spread = (ask − bid) / 2`.
///
/// The half-spread is kept in `Decimal` (as `(ask − bid) × fraction / 2`) so
/// the half-cent is not lost before rounding, then rounded to whole cents with
/// **banker's rounding (half-to-even)** — the repo's single fixed
/// `Decimal → cents` policy ([`PriceCents::from_decimal_dollars`]). The
/// `fraction` is validated `≥ 0` at config time, so the result is a
/// non-negative magnitude.
///
/// # Errors
///
/// Returns [`BacktestError::CrossedQuote`] if `bid > ask` (a malformed quote —
/// crossed quotes are rejected at ingest, so this is defence in depth),
/// [`BacktestError::ArithmeticOverflow`] if the `Decimal` product/quotient
/// overflows, and [`BacktestError::Execution`] if the rounded offset does not
/// fit a `u64`.
#[must_use = "the computed spread-fraction offset must be applied to the fill price"]
#[inline]
fn spread_fraction_offset_cents(
    bid: PriceCents,
    ask: PriceCents,
    fraction: Decimal,
) -> Result<u64, BacktestError> {
    let spread = ask
        .value()
        .checked_sub(bid.value())
        .ok_or(BacktestError::CrossedQuote {
            bid: bid.value(),
            ask: ask.value(),
        })?;
    let scaled = Decimal::from(spread)
        .checked_mul(fraction)
        .ok_or(BacktestError::ArithmeticOverflow)?
        .checked_div(Decimal::TWO)
        .ok_or(BacktestError::ArithmeticOverflow)?
        .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven);
    scaled.to_u64().ok_or_else(|| {
        BacktestError::Execution(format!(
            "naive spread-fraction slippage {scaled} out of range for u64 cents"
        ))
    })
}

/// The naive fill price in integer cents: the `reference` (`mid` — v0.1 fixes
/// the reference `spread_fraction` at `0`, see the module docs) moved by the
/// adverse `offset` on the side that worsens the fill — **up** for a buy/long,
/// **down** for a sell/short.
///
/// A sell whose offset exceeds the reference **floors at `0`**: a premium
/// cannot be negative, so the fill price is clamped to zero rather than
/// underflowing `u64`. This is an explicit, documented floor via `max(0)` on a
/// signed `i128` — never a silent `saturating_sub` on money. A buy whose
/// reference plus offset exceeds the `u64` range is a typed overflow error.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] when a buy's `reference +
/// offset` exceeds the `u64` range.
#[must_use = "the computed fill price must be recorded on the fill"]
#[inline]
fn naive_fill_price(
    reference: PriceCents,
    side: Side,
    offset: u64,
) -> Result<PriceCents, BacktestError> {
    // Both operands are `u64` widened to `i128`, so neither `+` nor `−` can
    // overflow `i128` — the single overflow guard is the `u64::try_from` below
    // (matching the `sign_convention::slippage_cents` idiom).
    let signed = match side {
        Side::Long => i128::from(reference.value()) + i128::from(offset),
        Side::Short => i128::from(reference.value()) - i128::from(offset),
    };
    // A premium cannot be negative: a sell whose adverse offset would push the
    // fill below zero floors at 0 (documented edge). Explicit floor, not a
    // saturating subtraction on money.
    let floored = signed.max(0);
    let cents = u64::try_from(floored).map_err(|_| BacktestError::ArithmeticOverflow)?;
    Ok(PriceCents::new(cents))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rust_decimal_macros::dec;

    use super::{NaiveFill, naive_fill_price, spread_fraction_offset_cents};
    use crate::config::{FeeSchedule, SlippageModel};
    use crate::domain::{
        ChainSnapshot, ContractKey, ExecutionMode, InstrumentSpec, OrderCommand, OrderId,
        OrderIntent, PositionAction, PositionId, PriceCents, Quantity, QuoteView, SimTime,
        StepIndex, TimeInForce, Underlying,
    };
    use crate::execution::ExecutionModel;

    const TS0: i64 = 1_750_291_200_000_000_000;
    const STRIKE: u64 = 510_000;

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    fn contract() -> ContractKey {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        ContractKey {
            underlying,
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(TS0)),
            strike: PriceCents::new(STRIKE),
            style: OptionStyle::Call,
        }
    }

    /// A quote with the given touch and a precomputed `mid` (floor of the
    /// midpoint, matching the ingest rule).
    fn quote(bid: u64, ask: u64) -> QuoteView {
        QuoteView {
            contract: contract(),
            bid: PriceCents::new(bid),
            ask: PriceCents::new(ask),
            mid: PriceCents::new((bid + ask) / 2),
            bid_size: qty(10),
            ask_size: qty(10),
            implied_volatility: dec!(0.2),
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    fn snapshot_with(bid: u64, ask: u64) -> ChainSnapshot {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("5-cent tick and 100x multiplier are valid");
        };
        let mut quotes = BTreeMap::new();
        quotes.insert(contract(), quote(bid, ask));
        ChainSnapshot {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            underlying,
            underlying_price: PriceCents::new(STRIKE),
            spec,
            quotes,
        }
    }

    fn empty_snapshot() -> ChainSnapshot {
        let mut snap = snapshot_with(100, 110);
        snap.quotes.clear();
        snap
    }

    /// A `Submit` of `quantity` on `side` with `decision_mid` set to the
    /// snapshot mid (the common case; slippage-vs-mid then equals the adverse
    /// offset).
    fn submit(side: Side, quantity: u32, decision_mid: u64) -> OrderCommand {
        OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Open,
            side,
            quantity: qty(quantity),
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(decision_mid),
        })
    }

    fn model(slippage: SlippageModel) -> NaiveFill {
        NaiveFill::new(
            slippage,
            FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
        )
    }

    /// Drive one `fill` call and return the produced fills.
    fn run(
        model: &mut NaiveFill,
        commands: &[OrderCommand],
        snap: &ChainSnapshot,
    ) -> Vec<crate::domain::Fill> {
        let mut out = Vec::new();
        // Naive holds no resting book and ignores the pre-minted order ids.
        let result = model.fill(commands, &[], snap, &mut out);
        assert!(matches!(result, Ok(())), "naive fill must succeed");
        out
    }

    #[test]
    fn test_naive_fill_applies_slippage_vs_mid() {
        // bid 100 / ask 110 → mid 105. FixedCents{3}, buy: fill = 105 + 3 = 108,
        // slippage vs decision_mid(105) = (108 − 105) × 1 × (+1) = +3.
        let mut m = model(SlippageModel::FixedCents { cents: 3 });
        let out = run(
            &mut m,
            &[submit(Side::Long, 1, 105)],
            &snapshot_with(100, 110),
        );
        let Some(fill) = out.first() else {
            panic!("expected one fill");
        };
        assert_eq!(fill.price.value(), 108);
        assert_eq!(fill.slippage.value(), 3);
        assert_eq!(fill.mode, ExecutionMode::Naive);
    }

    #[test]
    fn test_naive_fill_at_touch_when_spread_fraction_one() {
        // bid 100 / ask 110 → mid 105, half_spread 5. SpreadFraction{1.0} moves
        // the fill a full half-spread: buy → ask (110), sell → bid (100).
        let snap = snapshot_with(100, 110);
        let mut m = model(SlippageModel::SpreadFraction { fraction: dec!(1) });
        let buy = run(&mut m, &[submit(Side::Long, 1, 105)], &snap);
        assert!(matches!(buy.first(), Some(f) if f.price.value() == 110));
        let sell = run(&mut m, &[submit(Side::Short, 1, 105)], &snap);
        assert!(matches!(sell.first(), Some(f) if f.price.value() == 100));
    }

    #[test]
    fn test_naive_fill_is_single_shot_fill_seq_zero() {
        // Each Submit yields exactly one fill at the full intent quantity —
        // there is no partial or multi-level fill in naive mode.
        let mut m = model(SlippageModel::None);
        let out = run(
            &mut m,
            &[submit(Side::Long, 7, 105), submit(Side::Short, 4, 105)],
            &snapshot_with(100, 110),
        );
        assert_eq!(out.len(), 2);
        assert!(matches!(out.first(), Some(f) if f.quantity.value() == 7));
        assert!(matches!(out.get(1), Some(f) if f.quantity.value() == 4));
    }

    #[test]
    fn test_naive_fill_none_slippage_fills_at_mid() {
        // None → fill at the reference (mid), zero adverse offset.
        let mut m = model(SlippageModel::None);
        let out = run(
            &mut m,
            &[submit(Side::Long, 1, 105)],
            &snapshot_with(100, 110),
        );
        let Some(fill) = out.first() else {
            panic!("expected one fill");
        };
        assert_eq!(fill.price.value(), 105);
        assert_eq!(fill.slippage.value(), 0);
    }

    #[test]
    fn test_naive_fill_fixed_cents_offset_adverse() {
        // FixedCents{7}: buy fills 7 above mid, sell fills 7 below mid.
        let snap = snapshot_with(100, 110);
        let mut m = model(SlippageModel::FixedCents { cents: 7 });
        let buy = run(&mut m, &[submit(Side::Long, 1, 105)], &snap);
        assert!(matches!(buy.first(), Some(f) if f.price.value() == 112));
        let sell = run(&mut m, &[submit(Side::Short, 1, 105)], &snap);
        assert!(matches!(sell.first(), Some(f) if f.price.value() == 98));
    }

    #[test]
    fn test_naive_fill_spread_fraction_half_spread_adverse() {
        // bid 100 / ask 110 → half_spread 5. SpreadFraction{0.5} → 2.5 → 2
        // (banker's, half-to-even). Buy fill = 105 + 2 = 107.
        let mut m = model(SlippageModel::SpreadFraction {
            fraction: dec!(0.5),
        });
        let out = run(
            &mut m,
            &[submit(Side::Long, 1, 105)],
            &snapshot_with(100, 110),
        );
        assert!(matches!(out.first(), Some(f) if f.price.value() == 107));
    }

    #[test]
    fn test_naive_fill_size_proportional_scales_with_quantity() {
        // SizeProportional{2}, qty 4 → 8 adverse. Buy fill = 105 + 8 = 113,
        // slippage = (113 − 105) × 4 × (+1) = +32.
        let mut m = model(SlippageModel::SizeProportional {
            cents_per_contract: 2,
        });
        let out = run(
            &mut m,
            &[submit(Side::Long, 4, 105)],
            &snapshot_with(100, 110),
        );
        let Some(fill) = out.first() else {
            panic!("expected one fill");
        };
        assert_eq!(fill.price.value(), 113);
        assert_eq!(fill.slippage.value(), 32);
    }

    #[test]
    fn test_naive_fill_buy_records_positive_adverse_slippage() {
        // A buy filled above decision_mid is adverse: positive slippage.
        let mut m = model(SlippageModel::FixedCents { cents: 5 });
        let out = run(
            &mut m,
            &[submit(Side::Long, 3, 105)],
            &snapshot_with(100, 110),
        );
        // fill 110, slippage = (110 − 105) × 3 × (+1) = +15.
        assert!(
            matches!(out.first(), Some(f) if f.slippage.value() == 15 && f.slippage.value() > 0)
        );
    }

    #[test]
    fn test_naive_fill_sell_records_positive_adverse_slippage() {
        // A sell filled below decision_mid is adverse: positive slippage.
        let mut m = model(SlippageModel::FixedCents { cents: 5 });
        let out = run(
            &mut m,
            &[submit(Side::Short, 3, 105)],
            &snapshot_with(100, 110),
        );
        // fill 100, slippage = (100 − 105) × 3 × (−1) = +15.
        assert!(
            matches!(out.first(), Some(f) if f.slippage.value() == 15 && f.slippage.value() > 0)
        );
    }

    #[test]
    fn test_naive_fill_sell_slippage_floors_price_at_zero() {
        // A sell whose adverse offset exceeds the reference floors the price at
        // 0 (a premium cannot be negative). mid 105, FixedCents{200}, sell →
        // fill 0, slippage = (0 − 105) × 1 × (−1) = +105 (adverse).
        let mut m = model(SlippageModel::FixedCents { cents: 200 });
        let out = run(
            &mut m,
            &[submit(Side::Short, 1, 105)],
            &snapshot_with(100, 110),
        );
        let Some(fill) = out.first() else {
            panic!("expected one fill");
        };
        assert_eq!(fill.price.value(), 0);
        assert_eq!(fill.slippage.value(), 105);
    }

    #[test]
    fn test_naive_fill_is_deterministic_across_identical_calls() {
        // Two independent models over identical inputs produce identical fills.
        let snap = snapshot_with(100, 110);
        let commands = [submit(Side::Long, 2, 105), submit(Side::Short, 3, 105)];
        let mut a = model(SlippageModel::FixedCents { cents: 4 });
        let mut b = model(SlippageModel::FixedCents { cents: 4 });
        let out_a = run(&mut a, &commands, &snap);
        let out_b = run(&mut b, &commands, &snap);
        assert_eq!(out_a, out_b);
    }

    #[test]
    fn test_naive_fill_cancel_and_replace_append_no_fills() {
        // Cancel/Replace target no resting order in naive mode → no fills; only
        // the Submit fills.
        let mut m = model(SlippageModel::None);
        let commands = [
            OrderCommand::Cancel(OrderId::new(1)),
            OrderCommand::Replace {
                order_id: OrderId::new(2),
                replacement: OrderIntent {
                    contract: contract(),
                    action: PositionAction::Close(PositionId::new(9)),
                    side: Side::Short,
                    quantity: qty(1),
                    limit: Some(PriceCents::new(100)),
                    tif: TimeInForce::Gtc,
                    decision_mid: PriceCents::new(105),
                },
            },
            submit(Side::Long, 1, 105),
        ];
        let out = run(&mut m, &commands, &snapshot_with(100, 110));
        assert_eq!(out.len(), 1, "only the Submit fills");
        assert!(matches!(out.first(), Some(f) if f.side == Side::Long));
    }

    #[test]
    fn test_naive_fill_missing_quote_execution_error() {
        let mut m = model(SlippageModel::None);
        let mut out = Vec::new();
        let result = m.fill(
            &[submit(Side::Long, 1, 105)],
            &[],
            &empty_snapshot(),
            &mut out,
        );
        assert!(matches!(
            result,
            Err(crate::error::BacktestError::Execution(_))
        ));
        assert!(out.is_empty(), "a failed fill appends nothing");
    }

    #[test]
    fn test_naive_fill_mode_returns_naive() {
        let m = model(SlippageModel::None);
        assert_eq!(m.mode(), ExecutionMode::Naive);
    }

    #[test]
    fn test_naive_fill_appends_into_caller_buffer_without_clearing() {
        // `fill` appends; a pre-existing entry survives.
        let mut m = model(SlippageModel::None);
        let snap = snapshot_with(100, 110);
        let mut out = run(&mut m, &[submit(Side::Long, 1, 105)], &snap);
        let before = out.len();
        let result = m.fill(&[submit(Side::Short, 1, 105)], &[], &snap, &mut out);
        assert!(matches!(result, Ok(())));
        assert_eq!(out.len(), before + 1);
    }

    #[test]
    fn test_spread_fraction_offset_rounds_half_to_even() {
        // spread 1, fraction 1.0 → 0.5 → 0 (nearest even).
        assert!(matches!(
            spread_fraction_offset_cents(PriceCents::new(100), PriceCents::new(101), dec!(1)),
            Ok(0)
        ));
        // spread 3, fraction 1.0 → 1.5 → 2 (nearest even).
        assert!(matches!(
            spread_fraction_offset_cents(PriceCents::new(100), PriceCents::new(103), dec!(1)),
            Ok(2)
        ));
        // spread 5, fraction 1.0 → 2.5 → 2 (nearest even).
        assert!(matches!(
            spread_fraction_offset_cents(PriceCents::new(100), PriceCents::new(105), dec!(1)),
            Ok(2)
        ));
    }

    #[test]
    fn test_spread_fraction_offset_rejects_crossed_quote() {
        // bid > ask is defence in depth (rejected at ingest): CrossedQuote.
        assert!(matches!(
            spread_fraction_offset_cents(PriceCents::new(110), PriceCents::new(100), dec!(0.5)),
            Err(crate::error::BacktestError::CrossedQuote { bid: 110, ask: 100 })
        ));
    }

    #[test]
    fn test_naive_fill_price_buy_and_sell_directions() {
        // Buy raises, sell lowers, both from the same reference.
        assert!(matches!(
            naive_fill_price(PriceCents::new(105), Side::Long, 5),
            Ok(p) if p.value() == 110
        ));
        assert!(matches!(
            naive_fill_price(PriceCents::new(105), Side::Short, 5),
            Ok(p) if p.value() == 100
        ));
        // Sell underflow floors at 0.
        assert!(matches!(
            naive_fill_price(PriceCents::new(105), Side::Short, 200),
            Ok(p) if p.value() == 0
        ));
    }

    #[test]
    fn test_naive_fill_price_buy_overflow_is_typed_error() {
        // A buy whose reference + offset exceeds u64 is a typed overflow.
        assert!(matches!(
            naive_fill_price(PriceCents::new(u64::MAX), Side::Long, 1),
            Err(crate::error::BacktestError::ArithmeticOverflow)
        ));
    }
}
