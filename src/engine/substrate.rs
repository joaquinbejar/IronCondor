//! The engine→analytics **attribution substrate** — the per-step, per-leg
//! inputs the post-run P&L-attribution pass (#31) needs, collected in the
//! replay loop and carried on [`crate::engine::BacktestRun`].
//!
//! # Why a post-run substrate (layering)
//!
//! `analytics` **consumes** engine output and the engine must **not** import
//! `analytics` ([CLAUDE.md](../../../CLAUDE.md) Module Boundaries), so the Greek
//! decomposition cannot run *inside* the loop. Instead the loop **collects**
//! the raw decomposition inputs into owned records that survive past the
//! ledger's ephemeral [`crate::engine::Ledger::position_marks`] hand-off (that
//! slice is reused scratch, valid only until the next `settle`). The analytics
//! pass then reads this owned substrate post-run — no tape look-back, no
//! re-computation, no reach back into the loop
//! ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
//!
//! # Representation (PB-1-safe)
//!
//! Two **flat** `Vec`s, **not** a `Vec`-of-`Vec` (a fresh inner `Vec` per step
//! would allocate every step and trip the `zero_alloc` PB-1 gate):
//!
//! - [`AttributionSubstrate::steps`] — one [`StepAttributionScalars`] per step,
//!   in step order. Reserved to the step count at `on_start`, pushed once per
//!   step (amortised).
//! - [`AttributionSubstrate::legs`] — one [`LegAttributionSample`] per
//!   beginning-of-step leg per step, contiguous and in
//!   `(step order, open-inventory order)`. Each step's slice is
//!   `legs[offset .. offset + StepAttributionScalars::leg_count]`. Reserved to
//!   `step_count × steady-state leg count` once the leg count is known (after
//!   `on_start`), so warm steps push without reallocating.
//!
//! Every field is `Copy` (integer cents, `Decimal`, [`Side`],
//! [`crate::engine::UnitGreeks`]), so a push clones no heap — the substrate is
//! allocation-free on a warm step (PB-1,
//! [docs/07 §4](../../../docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop)).
//!
//! # Determinism
//!
//! The collector reads no wall clock and draws no randomness; it reads the
//! current snapshot `S_n`'s scalars and the ledger's ordered per-leg marks, and
//! records the previous snapshot's `ts`/underlying as the fixed observation
//! endpoints. The same `(seed, config, data)` yields a byte-identical substrate
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use optionstratlib::Side;
use rust_decimal::Decimal;

use crate::domain::{Cents, ChainSnapshot};
use crate::engine::ledger::{PositionMark, UnitGreeks};

/// One beginning-of-step leg's attribution inputs for one step — a **unit**
/// (quantity = 1) sensitivity set plus the size to weight it by, captured so the
/// post-run pass weights `quantity × contract_multiplier` **exactly once** and
/// never trips the `N²` double-quantity trap
/// ([specs/optionstratlib §4.1](../../../docs/specs/optionstratlib.md#41-exact-units-and-position-weighting-pinned-contract)).
///
/// All fields are `Copy`, so pushing one into [`AttributionSubstrate::legs`]
/// clones no heap.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LegAttributionSample {
    /// The per-contract **unit** Greeks as of the **previous** snapshot
    /// `S_{n-1}` (delta per $1, theta per day, vega per percentage point, and
    /// `iv_{n-1}`) — the fixed beginning-of-step observation endpoint. `None` at
    /// step 0 (no `S_{-1}`) or before the leg's contract has ever been quoted;
    /// its Greek terms are then `0` for the step.
    pub prior_greeks: Option<UnitGreeks>,
    /// The leg's contract implied volatility this step, `iv_n` — the second
    /// endpoint of `ΔIV_pp = (iv_n − iv_{n-1}) × 100`. When the leg's contract
    /// is **absent** this step, its last-known IV (`iv_{n-1}`) is carried
    /// forward, so `ΔIV = 0` — the same carry-forward rule the ledger applies to
    /// a stale mark ([docs/01 §6](../../../docs/01-domain-model.md#6-market-data)).
    pub current_iv: Decimal,
    /// The open contract count (`> 0`) — one of the two weighting factors,
    /// applied exactly once.
    pub quantity: u32,
    /// Contracts → underlying units this step — the other weighting factor,
    /// applied exactly once.
    pub contract_multiplier: u32,
    /// The direction the leg is open in (`Long` `+`, `Short` `−`) — the
    /// `side_sign` factor every Greek term carries, matching the ledger's
    /// `step_pnl` so the residual never absorbs a wrong-signed term
    /// ([docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
    pub side: Side,
}

/// One step's scalar attribution inputs — the step-level market moves and the
/// ledger's already-computed frictions and target, all in integer cents (money)
/// or raw endpoints (time / underlying).
///
/// `leg_count` is the number of [`LegAttributionSample`]s this step contributes
/// to the flat [`AttributionSubstrate::legs`]; the post-run pass walks them with
/// a running offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepAttributionScalars {
    /// The 0-based step ordinal.
    pub step: u32,
    /// This step's snapshot timestamp `ts_n` (ns since epoch, UTC).
    pub ts_ns: i64,
    /// The previous snapshot's timestamp `ts_{n-1}` (ns) — the `Δt` endpoint.
    /// Equal to `ts_ns` at step 0, so `Δt_days = 0` and every Greek term is `0`.
    pub prior_ts_ns: i64,
    /// This step's underlying price `underlying_n` in integer cents — the `ΔS`
    /// endpoint.
    pub underlying_cents: u64,
    /// The previous snapshot's underlying price `underlying_{n-1}` in integer
    /// cents. Equal to `underlying_cents` at step 0, so `ΔS = 0`.
    pub prior_underlying_cents: u64,
    /// The step's `spread_capture = −Σ Fill.slippage` in integer cents (signed;
    /// positive = favourable) — computed once by the ledger.
    pub spread_capture_cents: i64,
    /// The step's `Σ Fill.fees` in integer cents (always `≥ 0`, subtracted in
    /// the identity) — computed once by the ledger.
    pub fees_cents: i64,
    /// The step's mark-to-market `step_pnl = equity_n − equity_{n-1}` in integer
    /// cents (step 0: `equity_0 − initial_capital`) — the target the residual
    /// closes exactly. Computed once by the ledger, never recomputed here.
    pub step_pnl_cents: i64,
    /// The number of [`LegAttributionSample`]s this step owns in the flat
    /// [`AttributionSubstrate::legs`] — the length of its contiguous slice.
    pub leg_count: usize,
}

/// The whole run's attribution substrate — the two flat `Vec`s the post-run
/// pass consumes. Owned by [`crate::engine::BacktestRun`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AttributionSubstrate {
    /// One [`StepAttributionScalars`] per step, in step order.
    pub steps: Vec<StepAttributionScalars>,
    /// One [`LegAttributionSample`] per beginning-of-step leg per step,
    /// contiguous and in `(step order, open-inventory order)`.
    pub legs: Vec<LegAttributionSample>,
}

/// The in-loop collector that builds an [`AttributionSubstrate`] one step at a
/// time (PB-1-safe), tracking the previous snapshot's `ts`/underlying as the
/// fixed observation endpoints.
///
/// Construct with [`AttributionCollector::with_capacity`] at startup, call
/// [`AttributionCollector::reserve_legs`] once the steady-state leg count is
/// known (after `on_start`), [`AttributionCollector::collect`] once per step
/// (right after the ledger settles), and [`AttributionCollector::into_substrate`]
/// at the end.
#[derive(Debug)]
pub(crate) struct AttributionCollector {
    steps: Vec<StepAttributionScalars>,
    legs: Vec<LegAttributionSample>,
    /// The previous snapshot's `ts` (ns) — `None` before step 0.
    prev_ts: Option<i64>,
    /// The previous snapshot's underlying price (cents) — `None` before step 0.
    prev_underlying: Option<u64>,
}

impl AttributionCollector {
    /// A collector sized for `step_capacity` steps.
    ///
    /// The step `Vec` is pre-sized to `step_capacity`; the leg `Vec` starts
    /// empty and is sized by [`Self::reserve_legs`] once the leg count is known.
    #[must_use]
    pub fn with_capacity(step_capacity: usize) -> Self {
        Self {
            steps: Vec::with_capacity(step_capacity),
            legs: Vec::new(),
            prev_ts: None,
            prev_underlying: None,
        }
    }

    /// Reserve the leg `Vec` for `step_capacity × legs_per_step` samples, so a
    /// steady-state run pushes without reallocating on a warm step (PB-1).
    ///
    /// Called once after `on_start`, when the opening inventory (hence the
    /// steady-state leg count) is known. A run whose leg count later *grows*
    /// beyond this reservation reallocates amortised at that transient — never
    /// on a steady-state step where the leg count is constant.
    pub fn reserve_legs(&mut self, step_capacity: usize, legs_per_step: usize) {
        // Capacity hint only, computed once after `on_start` — outside the PB-1
        // measured window. `checked_mul` (not `saturating_*`, per
        // global_rules.md): on the unreachable overflow we simply skip the
        // pre-reservation and let the flat `Vec` grow by amortised `push`, which
        // stays PB-1-safe (never per-step-linear).
        let Some(want) = step_capacity.checked_mul(legs_per_step) else {
            return;
        };
        if want > self.legs.capacity() {
            // `want > capacity >= len`, so the shortfall never underflows.
            self.legs.reserve(want - self.legs.len());
        }
    }

    /// Collect one step's substrate right after the ledger settled it.
    ///
    /// Reads the step scalars from the current snapshot `S_n` and the ledger's
    /// already-computed `spread_capture` / `fees` / `step_pnl`, and one
    /// [`LegAttributionSample`] per beginning-of-step leg from the ledger's
    /// ordered `marks` (the [`crate::engine::Ledger::position_marks`] hand-off).
    /// The previous snapshot's `ts`/underlying are the `Δt` / `ΔS` endpoints;
    /// at step 0 they default to this step's own values, so the market deltas
    /// are `0` ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
    ///
    /// Reading `iv_n` from `S_n`'s quote is **not** a tape look-back — `S_n` is
    /// this step's own snapshot; an absent leg carries `iv_{n-1}` forward
    /// (`ΔIV = 0`).
    pub fn collect(
        &mut self,
        snapshot: &ChainSnapshot,
        marks: &[PositionMark],
        spread_capture: Cents,
        fees: Cents,
        step_pnl: Cents,
    ) {
        let ts_ns = snapshot.ts.value();
        let underlying_cents = snapshot.underlying_price.value();
        // Step 0 has no S_{-1}: default the endpoints to this step's own values
        // so Δt = ΔS = 0 and every Greek term is 0.
        let prior_ts_ns = self.prev_ts.unwrap_or(ts_ns);
        let prior_underlying_cents = self.prev_underlying.unwrap_or(underlying_cents);

        for mark in marks {
            // iv_n from this step's quote; carried forward (→ ΔIV = 0) if the
            // leg's contract is absent this step, matching the stale-mark rule.
            let current_iv = snapshot
                .quotes
                .get(&mark.contract)
                .map(|quote| quote.implied_volatility)
                .or_else(|| mark.prior_greeks.map(|g| g.implied_volatility))
                .unwrap_or(Decimal::ZERO);
            self.legs.push(LegAttributionSample {
                prior_greeks: mark.prior_greeks,
                current_iv,
                quantity: mark.quantity.value(),
                contract_multiplier: mark.contract_multiplier,
                side: mark.side,
            });
        }

        self.steps.push(StepAttributionScalars {
            step: snapshot.step.value(),
            ts_ns,
            prior_ts_ns,
            underlying_cents,
            prior_underlying_cents,
            spread_capture_cents: spread_capture.value(),
            fees_cents: fees.value(),
            step_pnl_cents: step_pnl.value(),
            leg_count: marks.len(),
        });

        self.prev_ts = Some(ts_ns);
        self.prev_underlying = Some(underlying_cents);
    }

    /// Consume the collector into the owned [`AttributionSubstrate`].
    #[must_use]
    pub fn into_substrate(self) -> AttributionSubstrate {
        AttributionSubstrate {
            steps: self.steps,
            legs: self.legs,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rust_decimal_macros::dec;

    use super::AttributionCollector;
    use crate::domain::{
        Cents, ChainSnapshot, ContractKey, InstrumentSpec, PositionId, PriceCents, Quantity,
        QuoteView, SimTime, StepIndex, Underlying,
    };
    use crate::engine::ledger::{PositionMark, UnitGreeks};

    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        u
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

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is valid");
        };
        q
    }

    fn snapshot(step: u32, ts: i64, underlying: u64, iv: rust_decimal::Decimal) -> ChainSnapshot {
        let mut quotes = BTreeMap::new();
        let quote = QuoteView {
            contract: key(510_000),
            bid: PriceCents::new(199),
            ask: PriceCents::new(201),
            mid: PriceCents::new(200),
            bid_size: qty(10),
            ask_size: qty(10),
            implied_volatility: iv,
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        };
        quotes.insert(quote.contract.clone(), quote);
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(1), 100) else {
            panic!("valid spec");
        };
        ChainSnapshot {
            ts: SimTime::new(ts),
            step: StepIndex::new(step),
            underlying: und(),
            underlying_price: PriceCents::new(underlying),
            spec,
            quotes,
        }
    }

    fn mark(prior: Option<UnitGreeks>) -> PositionMark {
        PositionMark {
            position_id: PositionId::new(1),
            contract: key(510_000),
            side: Side::Short,
            quantity: qty(2),
            contract_multiplier: 100,
            mark: PriceCents::new(200),
            stale_mark: false,
            prior_greeks: prior,
        }
    }

    #[test]
    fn test_collect_step_zero_endpoints_equal_so_market_deltas_are_zero() {
        let mut collector = AttributionCollector::with_capacity(4);
        let snap = snapshot(0, TS0, 500_000, dec!(0.20));
        collector.collect(
            &snap,
            &[mark(None)],
            Cents::new(-60),
            Cents::new(520),
            Cents::new(1_480),
        );
        let sub = collector.into_substrate();
        let Some(step) = sub.steps.first() else {
            panic!("one step scalars");
        };
        // Step 0: prior endpoints default to this step's own values → Δ = 0.
        assert_eq!(step.prior_ts_ns, step.ts_ns);
        assert_eq!(step.prior_underlying_cents, step.underlying_cents);
        assert_eq!(step.leg_count, 1);
        assert_eq!(step.step_pnl_cents, 1_480);
        assert_eq!(step.spread_capture_cents, -60);
        assert_eq!(step.fees_cents, 520);
        // The single leg carries prior_greeks = None (no S_-1) at step 0.
        let Some(leg) = sub.legs.first() else {
            panic!("one leg sample");
        };
        assert!(leg.prior_greeks.is_none());
        assert_eq!(leg.quantity, 2);
        assert_eq!(leg.contract_multiplier, 100);
    }

    #[test]
    fn test_collect_tracks_prior_endpoints_across_steps() {
        let mut collector = AttributionCollector::with_capacity(4);
        let prior = UnitGreeks {
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
            implied_volatility: dec!(0.20),
        };
        let snap0 = snapshot(0, TS0, 500_000, dec!(0.20));
        collector.collect(
            &snap0,
            &[mark(None)],
            Cents::new(0),
            Cents::new(0),
            Cents::new(0),
        );
        let snap1 = snapshot(1, TS0 + NANOS_PER_DAY, 500_100, dec!(0.21));
        collector.collect(
            &snap1,
            &[mark(Some(prior))],
            Cents::new(0),
            Cents::new(0),
            Cents::new(50),
        );
        let sub = collector.into_substrate();
        let Some(step1) = sub.steps.get(1) else {
            panic!("two step scalars");
        };
        // Step 1's prior endpoints are step 0's snapshot values.
        assert_eq!(step1.prior_ts_ns, TS0);
        assert_eq!(step1.ts_ns, TS0 + NANOS_PER_DAY);
        assert_eq!(step1.prior_underlying_cents, 500_000);
        assert_eq!(step1.underlying_cents, 500_100);
        // The step-1 leg carries S_0's Greeks as prior and S_1's IV as current.
        let Some(leg1) = sub.legs.get(1) else {
            panic!("two leg samples");
        };
        assert_eq!(leg1.prior_greeks, Some(prior));
        assert_eq!(leg1.current_iv, dec!(0.21));
    }

    #[test]
    fn test_collect_absent_leg_carries_iv_forward_so_delta_iv_is_zero() {
        // A leg whose contract is absent this step reads iv_n = iv_{n-1}.
        let mut collector = AttributionCollector::with_capacity(2);
        let prior = UnitGreeks {
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
            implied_volatility: dec!(0.20),
        };
        // Snapshot without the leg's contract quoted.
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(1), 100) else {
            panic!("valid spec");
        };
        let snap = ChainSnapshot {
            ts: SimTime::new(TS0 + NANOS_PER_DAY),
            step: StepIndex::new(1),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes: BTreeMap::new(),
        };
        collector.collect(
            &snap,
            &[mark(Some(prior))],
            Cents::new(0),
            Cents::new(0),
            Cents::new(0),
        );
        let sub = collector.into_substrate();
        let Some(leg) = sub.legs.first() else {
            panic!("one leg sample");
        };
        // iv_n carried forward from iv_{n-1} = 0.20 → ΔIV = 0.
        assert_eq!(leg.current_iv, dec!(0.20));
    }

    #[test]
    fn test_reserve_legs_grows_capacity_without_changing_len() {
        let mut collector = AttributionCollector::with_capacity(64);
        collector.reserve_legs(64, 4);
        assert!(
            collector.legs.capacity() >= 256,
            "leg capacity covers the steady-state run"
        );
        assert_eq!(collector.legs.len(), 0, "reservation does not push");
    }
}
