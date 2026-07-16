//! Per-strike book seeding for the realistic fill model (v0.2, feature
//! `orderbook`, issue #23).
//!
//! A backtest has quotes, not a live limit-order book. Realistic mode **seeds**
//! each per-strike [`OptionOrderBook`](option_chain_orderbook::OptionOrderBook)
//! from the current chain snapshot before routing the strategy's intents, giving
//! the depth structure that queue position and market impact (#024) act on
//! ([docs/04 §6](../../../docs/04-execution-models.md), [ADR-0002](../../../docs/adr/0002-order-book-level-fill-simulation.md)).
//!
//! # The seeded ladder
//!
//! For every [`QuoteView`] in the snapshot the seeder builds a **multi-level
//! ladder** on each side:
//!
//! - **Touch level** at the quoted `bid` (bid side) / `ask` (ask side), sized
//!   from the [`LiquidityProfile`]'s [`TouchSize`] function.
//! - **`L` deeper levels** stepping one `tick_size_cents` at a time away from
//!   mid (bid side `bid − i·tick`; ask side `ask + i·tick`), every price
//!   tick-aligned **by construction** — a multiple of the tick added to an
//!   already-aligned touch.
//! - **Geometric size decay:** level `i` size `= round(touch_size × decayⁱ)`
//!   with **half-to-even** rounding (the repo's single money-rounding policy,
//!   [`crate::domain::PriceCents::from_decimal_dollars`]); a level rounding to
//!   zero **terminates** that side's ladder (never a zero-size order). A bid
//!   level whose price would underflow below `0` likewise terminates that side.
//!
//! # Determinism
//!
//! The ladder is a **pure function of `(profile, quote, tick)`** — no RNG (the
//! run `seed` is not drawn upon here), no wall clock, no `f64` (sizes decay in
//! `Decimal`, prices step in integer cents). Orders are planned and submitted in
//! a **fixed order** — ascending [`ContractKey`] (the snapshot's `BTreeMap`
//! iteration order), and within each contract bid side before ask side, touch
//! before deeper levels — and every seed `OrderId` is drawn from the
//! seeded-maker range via [`RealisticFill::seed_maker_limit`] (disjoint from
//! strategy ids, #022). So two seedings of the same snapshot produce
//! byte-identical depth.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};

use crate::config::{LiquidityProfile, TouchSize};
use crate::domain::{ChainSnapshot, ContractKey, PriceCents, Quantity, QuoteView};
use crate::error::BacktestError;

use super::realistic::RealisticFill;

/// One planned seed order: a resting maker limit on `contract`'s book.
///
/// Purely the *plan* — [`plan_seed`] computes the whole deterministic ladder as
/// a `Vec<SeedOrder>` in fixed submission order, and [`seed_book`] submits it.
/// Keeping planning separate from submission makes the ladder unit-testable
/// without a live book and pins determinism at the plan level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeedOrder {
    /// The contract whose leaf book receives the resting order.
    pub contract: ContractKey,
    /// `true` rests a sell (ask) at `price`; `false` a bid.
    pub is_ask: bool,
    /// The resting price in integer cents (tick-aligned by construction).
    pub price: PriceCents,
    /// The resting size in contracts (`> 0`).
    pub size: Quantity,
}

/// The touch size in contracts for one side of one quote, per the profile.
///
/// [`TouchSize::QuotedSize`] uses the quote's own per-side size (`ask_size` for
/// the ask touch, `bid_size` for the bid touch); [`TouchSize::Flat`] uses the
/// configured flat depth. Both are `> 0` (a [`Quantity`] is strictly positive;
/// a flat size is validated `> 0`).
#[must_use]
fn touch_size_contracts(profile: &LiquidityProfile, quote: &QuoteView, is_ask: bool) -> u32 {
    match profile.touch_size {
        TouchSize::QuotedSize => {
            if is_ask {
                quote.ask_size.value()
            } else {
                quote.bid_size.value()
            }
        }
        TouchSize::Flat { contracts } => contracts,
    }
}

/// The geometrically-decayed size at one level: `round(touch × factor)` with
/// half-to-even rounding, as an integer contract count.
///
/// `factor` is `decayⁱ` for level `i`. Because `decay ∈ (0, 1]` the factor is
/// non-increasing, so once this returns `0` every deeper level is also `0` — the
/// caller terminates the ladder.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] when the `Decimal` product or
/// the narrowing to `u32` exceeds range (unreachable for `decay ≤ 1`, but
/// checked rather than assumed).
#[must_use = "the decayed size determines whether the ladder continues"]
fn decayed_size(touch: u32, factor: Decimal) -> Result<u32, BacktestError> {
    let raw = Decimal::from(touch)
        .checked_mul(factor)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    // Half-to-even, matching the single money-rounding policy in the domain.
    let rounded = raw.round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven);
    let size = rounded.to_u32().ok_or(BacktestError::ArithmeticOverflow)?;
    Ok(size)
}

/// Plan the full deterministic seed ladder for a snapshot, in fixed submission
/// order — ascending [`ContractKey`], bid side before ask side, touch before
/// deeper levels.
///
/// A pure function of `(snap, profile)`: same inputs ⇒ byte-identical plan. The
/// returned `Vec` is the one-time initial-seed plan (#023); the per-step refresh
/// that reuses buffers is #025.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] on cents/size overflow while
/// building a ladder.
#[must_use = "the seed plan must be submitted to have any effect"]
pub(crate) fn plan_seed(
    snap: &ChainSnapshot,
    profile: &LiquidityProfile,
) -> Result<Vec<SeedOrder>, BacktestError> {
    let tick_cents = snap.spec.tick_size_cents.value();
    if tick_cents == 0 {
        // Defence in depth: `InstrumentSpec` validates `tick > 0` at ingest.
        return Err(BacktestError::Execution(
            "instrument tick_size_cents is zero at the book seeder".to_string(),
        ));
    }
    let mut plan: Vec<SeedOrder> = Vec::new();
    // `snap.quotes` is a `BTreeMap`, so iteration is ascending `ContractKey`
    // (stable, deterministic) — the fixed submission order.
    for (contract, quote) in &snap.quotes {
        push_side_ladder(
            &mut plan,
            contract,
            false,
            quote.bid.value(),
            tick_cents,
            profile,
            quote,
        )?;
        push_side_ladder(
            &mut plan,
            contract,
            true,
            quote.ask.value(),
            tick_cents,
            profile,
            quote,
        )?;
    }
    Ok(plan)
}

/// Append one side's ladder for `contract` into `plan` (touch then deeper).
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] on cents/size overflow, and
/// propagates [`Quantity::new`] which cannot fail here (a level is planned only
/// when its rounded size is `> 0`).
fn push_side_ladder(
    plan: &mut Vec<SeedOrder>,
    contract: &ContractKey,
    is_ask: bool,
    touch_cents: u64,
    tick_cents: u64,
    profile: &LiquidityProfile,
    quote: &QuoteView,
) -> Result<(), BacktestError> {
    let touch = touch_size_contracts(profile, quote, is_ask);
    let mut factor = Decimal::ONE; // decay⁰ = 1 at the touch
    for level in 0..=profile.depth_levels {
        let size = decayed_size(touch, factor)?;
        if size == 0 {
            // A zero-rounding level terminates the ladder (no zero-size order);
            // deeper levels only decay further, so none can revive.
            break;
        }
        let offset = tick_cents
            .checked_mul(u64::from(level))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let price = if is_ask {
            touch_cents
                .checked_add(offset)
                .ok_or(BacktestError::ArithmeticOverflow)?
        } else {
            match touch_cents.checked_sub(offset) {
                Some(price) => price,
                // `bid − i·tick` underflows below 0 → stop this side's ladder.
                None => break,
            }
        };
        let qty = Quantity::new(size)?;
        plan.push(SeedOrder {
            contract: contract.clone(),
            is_ask,
            price: PriceCents::new(price),
            size: qty,
        });
        factor = factor
            .checked_mul(profile.decay)
            .ok_or(BacktestError::ArithmeticOverflow)?;
    }
    Ok(())
}

/// Seed every strike's leaf book in `model` from `snap`'s quotes per `profile`.
///
/// Builds the deterministic [`plan_seed`] ladder and submits each level through
/// [`RealisticFill::seed_maker_limit`], which draws a seeded-maker `OrderId`
/// (disjoint from strategy ids), scales cents → ticks, and rests the order.
/// Submission follows the plan's fixed order, so the seeded book is
/// byte-identical across runs.
///
/// This is the **one-time initial seed** (#023): the between-snapshot refresh
/// that cancels stale seed and reseeds each step is #025.
///
/// # Errors
///
/// Propagates [`plan_seed`] errors and every [`RealisticFill::seed_maker_limit`]
/// error ([`BacktestError::PriceNotTickAligned`] for a mis-aligned quote,
/// [`BacktestError::Conversion`] for an unresolved expiration,
/// [`BacktestError::OrderBook`] when the book rejects an order,
/// [`BacktestError::Execution`] when the maker id range is exhausted).
pub(crate) fn seed_book(
    model: &mut RealisticFill,
    snap: &ChainSnapshot,
    profile: &LiquidityProfile,
) -> Result<(), BacktestError> {
    let tick = snap.spec.tick_size_cents;
    let plan = plan_seed(snap, profile)?;
    for order in &plan {
        model.seed_maker_limit(&order.contract, order.is_ask, order.price, order.size, tick)?;
    }
    Ok(())
}

#[cfg(all(test, feature = "orderbook"))]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{SeedOrder, decayed_size, plan_seed};
    use crate::config::{LiquidityProfile, TouchSize};
    use crate::domain::{
        ChainSnapshot, ContractKey, InstrumentSpec, PriceCents, Quantity, QuoteView, SimTime,
        StepIndex, Underlying,
    };

    const TS0: i64 = 1_750_291_200_000_000_000;
    const TICK: u64 = 5;

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    fn contract(strike: u64) -> ContractKey {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        ContractKey {
            underlying,
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(TS0)),
            strike: PriceCents::new(strike),
            style: OptionStyle::Call,
        }
    }

    fn quote(strike: u64, bid: u64, ask: u64, bid_size: u32, ask_size: u32) -> QuoteView {
        QuoteView {
            contract: contract(strike),
            bid: PriceCents::new(bid),
            ask: PriceCents::new(ask),
            mid: PriceCents::new((bid + ask) / 2),
            bid_size: qty(bid_size),
            ask_size: qty(ask_size),
            implied_volatility: dec!(0.2),
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    fn snapshot(quotes_in: &[QuoteView]) -> ChainSnapshot {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(TICK), 100) else {
            panic!("valid spec");
        };
        let mut quotes = BTreeMap::new();
        for q in quotes_in {
            quotes.insert(q.contract.clone(), q.clone());
        }
        ChainSnapshot {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            underlying,
            underlying_price: PriceCents::new(510_000),
            spec,
            quotes,
        }
    }

    fn profile(touch_size: TouchSize, depth_levels: u32, decay: Decimal) -> LiquidityProfile {
        LiquidityProfile {
            touch_size,
            depth_levels,
            decay,
        }
    }

    /// A compact `(is_ask, price, size)` view of a plan for terse assertions.
    fn triples(plan: &[SeedOrder]) -> Vec<(bool, u64, u32)> {
        plan.iter()
            .map(|o| (o.is_ask, o.price.value(), o.size.value()))
            .collect()
    }

    // --- geometric decay sizes ------------------------------------------------

    #[test]
    fn test_decayed_size_matches_round_touch_times_r_pow_i() {
        // touch 8, r 0.5: 8, 4, 2, 1, round(0.5)=0 (half-to-even).
        let mut factor = Decimal::ONE;
        let expected = [8u32, 4, 2, 1, 0];
        for want in expected {
            let got = decayed_size(8, factor);
            assert!(
                matches!(got, Ok(n) if n == want),
                "level factor {factor} → {want}"
            );
            let Some(next) = factor.checked_mul(dec!(0.5)) else {
                panic!("decay multiply must not overflow");
            };
            factor = next;
        }
    }

    #[test]
    fn test_decayed_size_half_to_even_rounds_two_and_a_half_to_two() {
        // touch 5, factor 0.5 → 2.5 → 2 (nearest even), not 3.
        assert!(matches!(decayed_size(5, dec!(0.5)), Ok(2)));
    }

    // --- ladder construction --------------------------------------------------

    #[test]
    fn test_plan_seed_builds_touch_plus_l_levels_stepping_by_tick() {
        // QuotedSize, L=3, r=0.5, sizes 8/8 → per side 8,4,2,1 (level 4 not built).
        let snap = snapshot(&[quote(510_000, 490, 500, 8, 8)]);
        let plan = plan_seed(&snap, &profile(TouchSize::QuotedSize, 3, dec!(0.5)));
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        assert_eq!(
            triples(&plan),
            vec![
                // bid side first: 490, 485, 480, 475 stepping down by tick.
                (false, 490, 8),
                (false, 485, 4),
                (false, 480, 2),
                (false, 475, 1),
                // ask side: 500, 505, 510, 515 stepping up by tick.
                (true, 500, 8),
                (true, 505, 4),
                (true, 510, 2),
                (true, 515, 1),
            ]
        );
    }

    #[test]
    fn test_plan_seed_geometric_decay_terminates_before_l_at_zero_rounding() {
        // touch 1, r 0.5, L=5: level 0 = 1, level 1 = round(0.5) = 0 → stop.
        let snap = snapshot(&[quote(510_000, 490, 500, 1, 1)]);
        let plan = plan_seed(&snap, &profile(TouchSize::QuotedSize, 5, dec!(0.5)));
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        // one touch per side, no deeper levels despite L = 5.
        assert_eq!(triples(&plan), vec![(false, 490, 1), (true, 500, 1)]);
    }

    #[test]
    fn test_plan_seed_deeper_levels_are_tick_aligned() {
        let snap = snapshot(&[quote(510_000, 490, 500, 32, 32)]);
        let plan = plan_seed(&snap, &profile(TouchSize::QuotedSize, 5, dec!(0.5)));
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        for order in &plan {
            assert_eq!(
                order.price.value() % TICK,
                0,
                "seeded price {} must be tick-aligned",
                order.price.value()
            );
        }
    }

    #[test]
    fn test_plan_seed_bid_ladder_stops_on_price_underflow() {
        // bid 10, tick 5, big touch, big L: bid levels 10, 5, 0, then −5 → stop.
        let snap = snapshot(&[quote(510_000, 10, 40, 64, 1)]);
        let plan = plan_seed(
            &snap,
            &profile(TouchSize::Flat { contracts: 64 }, 5, dec!(1)),
        );
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        let bids: Vec<u64> = plan
            .iter()
            .filter(|o| !o.is_ask)
            .map(|o| o.price.value())
            .collect();
        // 10, 5, 0 — the next level (−5) underflows and terminates the side.
        assert_eq!(bids, vec![10, 5, 0]);
    }

    #[test]
    fn test_plan_seed_uniform_decay_of_one_fills_all_levels() {
        // r = 1 never rounds to zero → full L+1 levels per side (uniform depth).
        let snap = snapshot(&[quote(510_000, 490, 500, 7, 7)]);
        let plan = plan_seed(&snap, &profile(TouchSize::QuotedSize, 3, dec!(1)));
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        assert_eq!(plan.len(), 8, "4 bid + 4 ask levels, all size 7");
        assert!(plan.iter().all(|o| o.size.value() == 7));
    }

    // --- touch-size function --------------------------------------------------

    #[test]
    fn test_plan_seed_quoted_touch_uses_per_side_size() {
        // bid_size 6, ask_size 9 → bid touch 6, ask touch 9 (L=0, touch only).
        let snap = snapshot(&[quote(510_000, 490, 500, 6, 9)]);
        let plan = plan_seed(&snap, &profile(TouchSize::QuotedSize, 0, dec!(0.5)));
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        assert_eq!(triples(&plan), vec![(false, 490, 6), (true, 500, 9)]);
    }

    #[test]
    fn test_plan_seed_flat_touch_uses_configured_depth_both_sides() {
        let snap = snapshot(&[quote(510_000, 490, 500, 6, 9)]);
        let plan = plan_seed(
            &snap,
            &profile(TouchSize::Flat { contracts: 20 }, 0, dec!(0.5)),
        );
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        // flat ignores the quoted sizes: both touches are 20.
        assert_eq!(triples(&plan), vec![(false, 490, 20), (true, 500, 20)]);
    }

    // --- fixed submission order + determinism ---------------------------------

    #[test]
    fn test_plan_seed_orders_by_contract_then_bid_before_ask() {
        // Two strikes: every 500k order precedes every 520k order (ascending
        // ContractKey), and within a contract the bid side precedes the ask side.
        let snap = snapshot(&[
            quote(520_000, 90, 100, 4, 4),
            quote(500_000, 190, 200, 4, 4),
        ]);
        let plan = plan_seed(&snap, &profile(TouchSize::QuotedSize, 0, dec!(0.5)));
        let Ok(plan) = plan else {
            panic!("plan must build");
        };
        let seen: Vec<(u64, bool)> = plan
            .iter()
            .map(|o| (o.contract.strike.value(), o.is_ask))
            .collect();
        assert_eq!(
            seen,
            vec![
                (500_000, false),
                (500_000, true),
                (520_000, false),
                (520_000, true),
            ]
        );
    }

    #[test]
    fn test_plan_seed_is_byte_identical_across_two_builds() {
        let snap = snapshot(&[
            quote(500_000, 190, 200, 8, 8),
            quote(520_000, 90, 100, 8, 8),
        ]);
        let prof = profile(TouchSize::QuotedSize, 4, dec!(0.5));
        let (Ok(first), Ok(second)) = (plan_seed(&snap, &prof), plan_seed(&snap, &prof)) else {
            panic!("both plans must build");
        };
        // Same (snapshot, profile) ⇒ identical price + size + order sequence.
        assert_eq!(first, second);
    }
}
