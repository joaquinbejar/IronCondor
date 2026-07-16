//! P&L attribution by Greek — the post-run decomposition of each step's
//! mark-to-market `step_pnl` into theta / delta / vega contributions plus
//! spread capture minus fees, closed by an **exact** integer-cents residual
//! ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
//!
//! Analytics **consumes** the engine's output — the owned
//! [`AttributionSubstrate`] the replay loop collected
//! ([`crate::engine::substrate`]) — and never imports the `engine` loop or
//! reaches back into the tape. This module imports `domain`, `engine`'s
//! **substrate** output, and `error`; the layering (`analytics → engine
//! output`) is not inverted.
//!
//! # The reconciliation identity (exact, always holds)
//!
//! ```text
//! ΔP&L = theta·Δt_days + delta·ΔS + vega·ΔIV_pp + spread_capture − fees + residual
//! ```
//!
//! with the fixed observation endpoints (beginning-of-step inventory and Greeks
//! as of `S_{n-1}`, applied to the market moves to `S_n`):
//!
//! - `Δt_days = (ts_n − ts_{n-1}) / (86_400 × 1e9)` — elapsed calendar days,
//!   paired with **daily** theta.
//! - `ΔS = underlying_n − underlying_{n-1}` — the underlying move in integer
//!   **cents**, paired with per-$1 delta.
//! - `ΔIV_pp = (iv_n − iv_{n-1}) × 100` — the IV move in **percentage points**,
//!   paired with **per-percentage-point** vega.
//!
//! `residual = step_pnl − (theta + delta + vega + spread_capture − fees)` is
//! computed from the **already-rounded** terms, so it is the **exact** remainder
//! and the identity holds in integer cents **for every step, by construction**
//! ([docs/05 §3.1](../../../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
//! A large `|residual|` is an **advisory** model-quality signal (a
//! `tracing::warn`), never a run failure and never folded into another term.
//!
//! # Exact units and the `N²` trap
//!
//! The substrate carries the chain's **per-contract unit** (quantity = 1)
//! Greeks in their native `optionstratlib` bases — delta per $1 (a dimensionless
//! ratio), theta per **day**, vega per **percentage point**, both in dollars —
//! plus the position's `quantity` and `contract_multiplier` **separately**. Each
//! term weights the unit Greek by `quantity × contract_multiplier × side_sign`
//! **exactly once**; it never calls the free `optionstratlib::greeks::*`
//! functions on a sized position and re-multiplies (they already fold in
//! `option.quantity` — the `N²` trap,
//! [specs/optionstratlib §4.1](../../../docs/specs/optionstratlib.md#41-exact-units-and-position-weighting-pinned-contract)).
//!
//! The cents scaling of each term is:
//!
//! - **delta** — `delta · ΔS_cents · quantity · multiplier · side_sign`. Delta
//!   is a **dimensionless ratio** ($ of premium per $ of underlying), so
//!   `delta · ΔS_cents` is already the per-contract premium move **in cents** —
//!   **no** dollars→cents factor.
//! - **theta** — `theta · Δt_days · 100 · quantity · multiplier · side_sign`.
//!   Theta is **dollars** per day, so a `×100` converts the per-contract move to
//!   cents.
//! - **vega** — `vega · ΔIV_pp · 100 · quantity · multiplier · side_sign`, with
//!   `ΔIV_pp = (iv_n − iv_{n-1}) × 100`. Vega is **dollars** per percentage
//!   point, so a `×100` converts to cents; the separate `×100` inside `ΔIV_pp`
//!   is the decimal→percentage-point conversion the doc mandates.
//!
//! Each term is accumulated in `Decimal` (the documented analytic exception)
//! and **rounded once per term** with banker's rounding
//! ([ADR-0003](../../../docs/adr/0003-money-as-integer-cents.md)); `residual` is
//! then computed from the rounded integer-cents terms.
//!
//! # Determinism
//!
//! Every row is a pure function of the substrate — no wall clock, no RNG, no map
//! iteration — so the same `(seed, config, data)` yields byte-identical
//! attribution ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
//! The advisory `tracing::warn` writes to the log only; it never changes a row.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};

use crate::domain::GreeksAttributionRow;
use crate::engine::{
    AttributionSubstrate, LegAttributionSample, StepAttributionScalars, UnitGreeks,
};
use crate::error::BacktestError;

/// Nanoseconds in one calendar day of exactly `86_400` s (UTC) — the day
/// convention `Δt_days` and expiry resolution share
/// ([docs/01 §5.1](../../../docs/01-domain-model.md#51-expiration-resolves-to-one-absolute-instant)).
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Dollars → cents. Theta (per day) and vega (per pp) are dollar-denominated, so
/// their per-contract cents contribution multiplies by this. **Not** applied to
/// the delta term — delta is a dimensionless ratio (see the [module docs](self)).
const DOLLARS_TO_CENTS: i64 = 100;

/// Decimal IV → percentage points: a `0.01` decimal move in σ is `1` pp, so
/// `ΔIV_pp = (iv_n − iv_{n-1}) × 100`. This is the unit-match the per-pp vega
/// base requires ([specs/optionstratlib §4.1](../../../docs/specs/optionstratlib.md#41-exact-units-and-position-weighting-pinned-contract)).
const IV_DECIMAL_TO_PP: i64 = 100;

/// Advisory absolute floor in integer cents: a residual whose magnitude is below
/// this is never worth a warning, so near-zero steps (tiny `|step_pnl|`) stay
/// quiet. **Advisory only** — it never gates a run
/// ([docs/05 §3.1](../../../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
pub const RESIDUAL_WARN_FLOOR_CENTS: i64 = 100;

/// Decompose each step's mark-to-market `step_pnl` into its Greek terms plus
/// spread capture minus fees, closed by an exact residual — one
/// [`GreeksAttributionRow`] per step, in step order.
///
/// This is the post-run analytics pass: it reads the engine-collected
/// [`AttributionSubstrate`] and produces the rows a bundle writer later
/// serialises. The identity holds **exactly in integer cents** for every step
/// by construction (see the [module docs](self)); a large residual triggers an
/// **advisory** [`tracing::warn`] via [`residual_is_notable`] but **never** an
/// error.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] only if the checked `Decimal`
/// or integer-cents arithmetic overflows (a pathological magnitude), never
/// because of the residual's size — the residual is always reported.
#[must_use = "the attribution rows are the analytics output and must be used"]
pub fn attribute(
    substrate: &AttributionSubstrate,
) -> Result<Vec<GreeksAttributionRow>, BacktestError> {
    let mut rows = Vec::with_capacity(substrate.steps.len());
    // Walk the flat leg buffer with a running offset: step `n` owns the slice
    // `legs[offset .. offset + leg_count]`, in open-inventory order.
    let mut offset: usize = 0;
    for step in &substrate.steps {
        let end = offset
            .checked_add(step.leg_count)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let legs = substrate.legs.get(offset..end).ok_or_else(|| {
            BacktestError::Bundle(format!(
                "attribution substrate leg slice {offset}..{end} is out of range for step {}",
                step.step
            ))
        })?;
        offset = end;

        let row = attribute_step(step, legs)?;
        if residual_is_notable(row.residual_cents, step.step_pnl_cents) {
            // Advisory model-quality signal only — never a run failure. Logging
            // does not change the row (determinism).
            tracing::warn!(
                step = step.step,
                residual_cents = row.residual_cents,
                step_pnl_cents = step.step_pnl_cents,
                "attribution residual exceeds the advisory model-quality threshold"
            );
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Decompose one step. Pure; the advisory warning is applied by the caller.
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] on `Decimal`/cents overflow.
fn attribute_step(
    step: &StepAttributionScalars,
    legs: &[LegAttributionSample],
) -> Result<GreeksAttributionRow, BacktestError> {
    // Step-level market deltas (0 at step 0: prior endpoints equal this step's).
    let delta_t_days = step_delta_t_days(step)?;
    let delta_s_cents = step_delta_s_cents(step)?;

    // Accumulate each term in Decimal over the beginning-of-step legs.
    let mut theta_dec = Decimal::ZERO;
    let mut delta_dec = Decimal::ZERO;
    let mut vega_dec = Decimal::ZERO;

    for leg in legs {
        // A leg with no S_{n-1} Greeks (step 0, or a never-quoted contract)
        // contributes 0 to every term.
        let Some(greeks) = leg.prior_greeks else {
            continue;
        };
        let signed_weight = signed_weight(leg)?;

        // theta = theta_daily · Δt_days · (dollars→cents) · weight · side_sign
        let theta_leg = mul4(
            greeks.theta,
            delta_t_days,
            Decimal::from(DOLLARS_TO_CENTS),
            signed_weight,
        )?;
        theta_dec = theta_dec
            .checked_add(theta_leg)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        // delta = delta · ΔS_cents · weight · side_sign  (delta is a ratio —
        // no dollars→cents factor).
        let delta_leg = mul3(greeks.delta, delta_s_cents, signed_weight)?;
        delta_dec = delta_dec
            .checked_add(delta_leg)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        // vega = vega · ΔIV_pp · (dollars→cents) · weight · side_sign, with
        // ΔIV_pp = (iv_n − iv_{n-1}) × 100.
        let delta_iv_pp = leg_delta_iv_pp(leg, greeks)?;
        let vega_leg = mul4(
            greeks.vega,
            delta_iv_pp,
            Decimal::from(DOLLARS_TO_CENTS),
            signed_weight,
        )?;
        vega_dec = vega_dec
            .checked_add(vega_leg)
            .ok_or(BacktestError::ArithmeticOverflow)?;
    }

    // Round each term ONCE (banker's, ADR-0003), then close the residual from
    // the already-rounded integer-cents terms — the exact remainder.
    let theta_cents = round_to_cents(theta_dec)?;
    let delta_cents = round_to_cents(delta_dec)?;
    let vega_cents = round_to_cents(vega_dec)?;
    let spread_capture_cents = step.spread_capture_cents;
    let fees_cents = step.fees_cents;

    // residual = step_pnl − (theta + delta + vega + spread_capture − fees)
    let attributed = theta_cents
        .checked_add(delta_cents)
        .and_then(|s| s.checked_add(vega_cents))
        .and_then(|s| s.checked_add(spread_capture_cents))
        .and_then(|s| s.checked_sub(fees_cents))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let residual_cents = step
        .step_pnl_cents
        .checked_sub(attributed)
        .ok_or(BacktestError::ArithmeticOverflow)?;

    Ok(GreeksAttributionRow::new(
        step.step,
        step.ts_ns,
        theta_cents,
        delta_cents,
        vega_cents,
        spread_capture_cents,
        fees_cents,
        residual_cents,
    ))
}

/// `Δt_days = (ts_n − ts_{n-1}) / (86_400 × 1e9)` as an exact `Decimal`.
///
/// `0` at step 0 (`prior_ts_ns == ts_ns`).
fn step_delta_t_days(step: &StepAttributionScalars) -> Result<Decimal, BacktestError> {
    let diff = step
        .ts_ns
        .checked_sub(step.prior_ts_ns)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    Decimal::from(diff)
        .checked_div(Decimal::from(NANOS_PER_DAY))
        .ok_or(BacktestError::ArithmeticOverflow)
}

/// `ΔS = underlying_n − underlying_{n-1}` in integer cents, as a `Decimal`.
///
/// `0` at step 0 (`prior_underlying_cents == underlying_cents`).
fn step_delta_s_cents(step: &StepAttributionScalars) -> Result<Decimal, BacktestError> {
    let now =
        i64::try_from(step.underlying_cents).map_err(|_| BacktestError::ArithmeticOverflow)?;
    let prior = i64::try_from(step.prior_underlying_cents)
        .map_err(|_| BacktestError::ArithmeticOverflow)?;
    let diff = now
        .checked_sub(prior)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    Ok(Decimal::from(diff))
}

/// The leg's `ΔIV_pp = (iv_n − iv_{n-1}) × 100`. A leg whose contract was absent
/// this step carries `iv_n = iv_{n-1}` (the collector's carry-forward), so
/// `ΔIV_pp = 0`.
fn leg_delta_iv_pp(
    leg: &LegAttributionSample,
    greeks: UnitGreeks,
) -> Result<Decimal, BacktestError> {
    leg.current_iv
        .checked_sub(greeks.implied_volatility)
        .and_then(|d| d.checked_mul(Decimal::from(IV_DECIMAL_TO_PP)))
        .ok_or(BacktestError::ArithmeticOverflow)
}

/// The leg's single signed weight `quantity × contract_multiplier × side_sign`
/// as a `Decimal` — the **only** quantity multiplication (no `N²`).
fn signed_weight(leg: &LegAttributionSample) -> Result<Decimal, BacktestError> {
    let weight = u64::from(leg.quantity)
        .checked_mul(u64::from(leg.contract_multiplier))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let weight = Decimal::from(weight);
    Ok(match sign(leg.side) {
        Sign::Positive => weight,
        Sign::Negative => -weight,
    })
}

/// The leg's side sign, `+` for `Long`, `−` for `Short`
/// ([docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
fn sign(side: optionstratlib::Side) -> Sign {
    match side {
        optionstratlib::Side::Long => Sign::Positive,
        optionstratlib::Side::Short => Sign::Negative,
    }
}

/// A leg's side sign — the `±1` factor every Greek term carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sign {
    Positive,
    Negative,
}

/// Checked `a · b · c`.
fn mul3(a: Decimal, b: Decimal, c: Decimal) -> Result<Decimal, BacktestError> {
    a.checked_mul(b)
        .and_then(|ab| ab.checked_mul(c))
        .ok_or(BacktestError::ArithmeticOverflow)
}

/// Checked `a · b · c · d`.
fn mul4(a: Decimal, b: Decimal, c: Decimal, d: Decimal) -> Result<Decimal, BacktestError> {
    mul3(a, b, c)?
        .checked_mul(d)
        .ok_or(BacktestError::ArithmeticOverflow)
}

/// Round a `Decimal` cents accumulator to whole integer cents **once**, with the
/// fixed banker's-rounding policy (half-to-even, ADR-0003), and narrow to `i64`.
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if the rounded value exceeds `i64`.
fn round_to_cents(value: Decimal) -> Result<i64, BacktestError> {
    value
        .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
        .to_i64()
        .ok_or(BacktestError::ArithmeticOverflow)
}

/// The advisory model-quality test: `true` when `|residual|` exceeds **both** an
/// absolute floor and `|step_pnl|` — i.e. the first-order expansion explained
/// less than half the move and the move is not negligibly small. Purely a
/// warning trigger; it never gates the run
/// ([docs/05 §3.1](../../../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
#[must_use]
pub fn residual_is_notable(residual_cents: i64, step_pnl_cents: i64) -> bool {
    let residual = residual_cents.unsigned_abs();
    let target = step_pnl_cents.unsigned_abs();
    let floor = RESIDUAL_WARN_FLOOR_CENTS.unsigned_abs();
    residual > floor && residual > target
}

#[cfg(test)]
mod tests {
    use optionstratlib::Side;
    use rust_decimal_macros::dec;

    use super::{attribute, residual_is_notable};
    use crate::domain::GreeksAttributionRow;
    use crate::engine::{
        AttributionSubstrate, LegAttributionSample, StepAttributionScalars, UnitGreeks,
    };

    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;

    /// A `UnitGreeks` with only the driver of interest non-zero.
    fn greeks(
        delta: rust_decimal::Decimal,
        theta: rust_decimal::Decimal,
        vega: rust_decimal::Decimal,
        iv: rust_decimal::Decimal,
    ) -> UnitGreeks {
        UnitGreeks {
            delta,
            gamma: dec!(0),
            theta,
            vega,
            implied_volatility: iv,
        }
    }

    /// One leg sample.
    fn leg(
        prior: Option<UnitGreeks>,
        current_iv: rust_decimal::Decimal,
        quantity: u32,
        multiplier: u32,
        side: Side,
    ) -> LegAttributionSample {
        LegAttributionSample {
            prior_greeks: prior,
            current_iv,
            quantity,
            contract_multiplier: multiplier,
            side,
        }
    }

    /// A one-step substrate with the given scalars and legs.
    #[allow(clippy::too_many_arguments)]
    fn one_step(
        ts_ns: i64,
        prior_ts_ns: i64,
        underlying: u64,
        prior_underlying: u64,
        spread_capture: i64,
        fees: i64,
        step_pnl: i64,
        legs: Vec<LegAttributionSample>,
    ) -> AttributionSubstrate {
        let scalars = StepAttributionScalars {
            step: 0,
            ts_ns,
            prior_ts_ns,
            underlying_cents: underlying,
            prior_underlying_cents: prior_underlying,
            spread_capture_cents: spread_capture,
            fees_cents: fees,
            step_pnl_cents: step_pnl,
            leg_count: legs.len(),
        };
        AttributionSubstrate {
            steps: vec![scalars],
            legs,
        }
    }

    /// The identity holds exactly in integer cents for a row.
    fn identity_holds(row: &GreeksAttributionRow, step_pnl: i64) -> bool {
        row.theta_pnl_cents + row.delta_pnl_cents + row.vega_pnl_cents + row.spread_capture_cents
            - row.fees_cents
            + row.residual_cents
            == step_pnl
    }

    #[test]
    fn test_attribute_step_zero_worked_reconciliation_matches_doc() {
        // docs/05 §3 worked step-0 example: step_pnl 1_480, spread_capture −60,
        // fees 520, Greek terms 0 (no S_-1) ⇒ residual = 1_480 − (0 − 60 − 520)
        // = 2_060.
        let substrate = one_step(
            TS0,
            TS0, // prior_ts == ts ⇒ Δt = 0
            500_000,
            500_000, // ΔS = 0
            -60,
            520,
            1_480,
            vec![leg(None, dec!(0.20), 4, 100, Side::Short)],
        );
        let Ok(rows) = attribute(&substrate) else {
            panic!("attribution succeeds");
        };
        let Some(row) = rows.first() else {
            panic!("one attribution row");
        };
        assert_eq!(row.theta_pnl_cents, 0);
        assert_eq!(row.delta_pnl_cents, 0);
        assert_eq!(row.vega_pnl_cents, 0);
        assert_eq!(row.spread_capture_cents, -60);
        assert_eq!(row.fees_cents, 520);
        assert_eq!(
            row.residual_cents, 2_060,
            "residual closes the step-0 baseline"
        );
        assert!(identity_holds(row, 1_480));
    }

    #[test]
    fn test_attribute_single_theta_driver_uses_daily_greek_and_day_delta() {
        // One short leg, theta −0.05/day, qty 1, mult 100, Δt 2 days, ΔS 0,
        // ΔIV 0. theta term = −0.05 × 2 × 100(¢) × (−1 × 100) = +1_000¢.
        // A ~365× annual mis-scale (Δt in years) would give ≈ 3¢, not 1_000.
        let substrate = one_step(
            TS0 + 2 * NANOS_PER_DAY,
            TS0,
            500_000,
            500_000,
            0,
            0,
            1_000,
            vec![leg(
                Some(greeks(dec!(0), dec!(-0.05), dec!(0), dec!(0.20))),
                dec!(0.20),
                1,
                100,
                Side::Short,
            )],
        );
        let Ok(rows) = attribute(&substrate) else {
            panic!("attribution succeeds");
        };
        let Some(row) = rows.first() else {
            panic!("one row");
        };
        assert_eq!(row.theta_pnl_cents, 1_000);
        assert_eq!(row.delta_pnl_cents, 0);
        assert_eq!(row.vega_pnl_cents, 0);
        assert_eq!(
            row.residual_cents, 0,
            "step_pnl matches the theta term exactly"
        );
        assert!(identity_holds(row, 1_000));
    }

    #[test]
    fn test_attribute_single_delta_driver_uses_per_dollar_ratio() {
        // One long leg, delta 0.5, qty 1, mult 100, ΔS +100¢ (=$1), Δt 0.
        // delta term = 0.5 × 100(¢ move) × (100 weight) = 5_000¢ (delta is a
        // ratio — NO dollars→cents factor).
        let substrate = one_step(
            TS0,
            TS0,
            500_100,
            500_000,
            0,
            0,
            5_000,
            vec![leg(
                Some(greeks(dec!(0.5), dec!(0), dec!(0), dec!(0.20))),
                dec!(0.20),
                1,
                100,
                Side::Long,
            )],
        );
        let Ok(rows) = attribute(&substrate) else {
            panic!("attribution succeeds");
        };
        let Some(row) = rows.first() else {
            panic!("one row");
        };
        assert_eq!(row.delta_pnl_cents, 5_000);
        assert_eq!(row.theta_pnl_cents, 0);
        assert_eq!(row.vega_pnl_cents, 0);
        assert_eq!(row.residual_cents, 0);
        assert!(identity_holds(row, 5_000));
    }

    #[test]
    fn test_attribute_single_vega_driver_uses_per_point_greek_and_pp_delta() {
        // One long leg, vega 0.1/pp, qty 1, mult 100, iv 0.20 → 0.21 (+1pp),
        // Δt 0, ΔS 0. vega term = 0.1 × 1(pp) × 100(¢) × 100(weight) = 1_000¢.
        // A 100× decimal-IV mis-scale (using 0.01 not 1pp) would give 10¢.
        let substrate = one_step(
            TS0,
            TS0,
            500_000,
            500_000,
            0,
            0,
            1_000,
            vec![leg(
                Some(greeks(dec!(0), dec!(0), dec!(0.1), dec!(0.20))),
                dec!(0.21),
                1,
                100,
                Side::Long,
            )],
        );
        let Ok(rows) = attribute(&substrate) else {
            panic!("attribution succeeds");
        };
        let Some(row) = rows.first() else {
            panic!("one row");
        };
        assert_eq!(row.vega_pnl_cents, 1_000);
        assert_eq!(row.theta_pnl_cents, 0);
        assert_eq!(row.delta_pnl_cents, 0);
        assert_eq!(row.residual_cents, 0);
        assert!(identity_holds(row, 1_000));
    }

    #[test]
    fn test_attribute_weights_quantity_once_not_squared() {
        // qty 3, mult 100, delta 0.5, ΔS +100¢. Weight applied once = 300 ⇒
        // 0.5 × 100 × 300 = 15_000¢. A double-quantity (N²) bug would give
        // 0.5 × 100 × 900 = 45_000¢.
        let substrate = one_step(
            TS0,
            TS0,
            500_100,
            500_000,
            0,
            0,
            15_000,
            vec![leg(
                Some(greeks(dec!(0.5), dec!(0), dec!(0), dec!(0.20))),
                dec!(0.20),
                3,
                100,
                Side::Long,
            )],
        );
        let Ok(rows) = attribute(&substrate) else {
            panic!("attribution succeeds");
        };
        let Some(row) = rows.first() else {
            panic!("one row");
        };
        assert_eq!(
            row.delta_pnl_cents, 15_000,
            "quantity weighted once, not squared"
        );
        assert!(identity_holds(row, 15_000));
    }

    #[test]
    fn test_attribute_short_leg_greek_sign_is_opposite_a_long() {
        // Same delta driver, opposite sides: a short leg's delta term is the
        // negation of the long leg's, so side_sign is applied.
        let long = one_step(
            TS0,
            TS0,
            500_100,
            500_000,
            0,
            0,
            5_000,
            vec![leg(
                Some(greeks(dec!(0.5), dec!(0), dec!(0), dec!(0.20))),
                dec!(0.20),
                1,
                100,
                Side::Long,
            )],
        );
        let short = one_step(
            TS0,
            TS0,
            500_100,
            500_000,
            0,
            0,
            -5_000,
            vec![leg(
                Some(greeks(dec!(0.5), dec!(0), dec!(0), dec!(0.20))),
                dec!(0.20),
                1,
                100,
                Side::Short,
            )],
        );
        let (Ok(long_rows), Ok(short_rows)) = (attribute(&long), attribute(&short)) else {
            panic!("both runs attribute");
        };
        let (Some(long_row), Some(short_row)) = (long_rows.first(), short_rows.first()) else {
            panic!("one row each");
        };
        assert_eq!(long_row.delta_pnl_cents, 5_000);
        assert_eq!(short_row.delta_pnl_cents, -5_000);
    }

    #[test]
    fn test_attribute_large_residual_is_reported_never_an_error() {
        // A step where the first-order delta term (+5_000) badly overshoots a
        // tiny actual move (step_pnl 100) — gamma/curvature dominated in the
        // other direction. The residual (100 − 5_000 = −4_900) absorbs it
        // exactly, the pass returns Ok, the row is present, and the residual is
        // "notable" (advisory warn) — but it NEVER fails the run.
        let substrate = one_step(
            TS0,
            TS0,
            500_100,
            500_000, // ΔS = +100¢
            0,
            0,
            100, // step_pnl far smaller than the delta term
            vec![leg(
                Some(greeks(dec!(0.5), dec!(0), dec!(0), dec!(0.20))),
                dec!(0.20),
                1,
                100,
                Side::Long,
            )],
        );
        let Ok(rows) = attribute(&substrate) else {
            panic!("a large residual never errors the pass");
        };
        let Some(row) = rows.first() else {
            panic!("one row");
        };
        assert_eq!(row.delta_pnl_cents, 5_000);
        assert_eq!(
            row.residual_cents, -4_900,
            "residual closes the identity exactly"
        );
        assert!(identity_holds(row, 100));
        assert!(
            residual_is_notable(row.residual_cents, 100),
            "|residual| 4_900 exceeds both the floor and |step_pnl| 100 → advisory notable"
        );
    }

    #[test]
    fn test_residual_is_notable_only_above_floor_and_step_pnl() {
        // Below the floor: never notable, whatever the step_pnl.
        assert!(!residual_is_notable(50, 0));
        // Above the floor but not above |step_pnl|: not notable.
        assert!(!residual_is_notable(500, 10_000));
        // Above both: notable (advisory warn).
        assert!(residual_is_notable(20_000, 10_000));
    }

    #[test]
    fn test_attribute_is_deterministic_for_same_substrate() {
        let substrate = one_step(
            TS0 + NANOS_PER_DAY,
            TS0,
            500_150,
            500_000,
            -12,
            130,
            4_321,
            vec![leg(
                Some(greeks(dec!(0.4), dec!(-0.05), dec!(0.1), dec!(0.20))),
                dec!(0.205),
                2,
                100,
                Side::Short,
            )],
        );
        let (Ok(a), Ok(b)) = (attribute(&substrate), attribute(&substrate)) else {
            panic!("both attributions succeed");
        };
        assert_eq!(a, b, "same substrate ⇒ byte-identical rows");
    }
}
