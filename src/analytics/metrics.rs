//! Minimal v0.1 summary metrics from the mark-to-market equity series.
//!
//! Analytics **consumes** the engine's output — the ordered
//! [`crate::domain::EquityPoint`] series the ledger emits
//! ([docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)) —
//! and **populates the result structs that already exist upstream** in
//! `optionstratlib::backtesting`; it never invents parallel result types
//! ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics),
//! [specs/optionstratlib.md §6](../../../docs/specs/optionstratlib.md#6-backtesting-result-and-metric-types)).
//! This module imports `domain`, `error`, and `optionstratlib` — **never**
//! `engine` — so the layering (`analytics → engine output`) is not inverted.
//!
//! # What v0.1 populates (minimal and honest)
//!
//! The v0.1 set is deliberately the **minimal** slice derivable from the
//! equity series alone. [`populate`] writes, in place, into the upstream
//! [`BacktestResult`]:
//!
//! - `general_performance.sharpe_ratio` — mean ÷ standard deviation of the
//!   **per-step equity returns**, **per-step (NOT annualized)**. docs/05 §4
//!   pins Sharpe to "per-step equity returns" and names **no** annualization
//!   factor, so none is fabricated. `None` when it cannot be computed (fewer
//!   than two equity points, or a flat curve whose return standard deviation
//!   is zero).
//! - `general_performance.volatility` — the population standard deviation of
//!   those per-step returns (`≥ 0`; `Some(0)` for a flat curve).
//! - `general_performance.total_return` — `(final − initial) ÷ initial`, a
//!   ratio computed from integer cents.
//! - `drawdown_analysis.max_drawdown` — the worst drawdown from the ledger's
//!   `drawdown` series, stored as its **non-negative magnitude** to match the
//!   upstream field's documented convention (`0.1` = a 10 % peak-to-trough
//!   loss); the raw ledger ratio is `≤ 0`.
//! - `custom_metrics["max_drawdown_ratio"]` — the raw signed ratio `≤ 0`.
//! - `custom_metrics["max_drawdown_cents"]` — the max peak-to-trough decline
//!   in **integer cents** (no dedicated upstream field exists for the cents
//!   amount; `custom_metrics` is the upstream escape hatch for it).
//!
//! Everything else stays `Default` and lands at **v0.3**: `annualized_return`,
//! `downside_deviation`, `sortino_ratio`, `calmar_ratio` and the win/loss slice
//! of [`GeneralPerformanceMetrics`]; `OptionsSpecificMetrics`,
//! `TradeStatistics`, `CapitalUtilization`, `AdvancedRiskMetrics`, the per-event
//! fields of [`DrawdownAnalysis`], and `TimeSeriesData`. The **canonical v0.1
//! equity-curve artifact is the ordered `Vec<EquityPoint>`** on
//! [`crate::BacktestRun`] (integer cents + the one drawdown float), so
//! `TimeSeriesData.equity_curve` (a parallel `Vec<Decimal>` in dollars) is left
//! `Default` rather than duplicated.
//!
//! # Money and floats
//!
//! Every monetary value stays **integer cents**; the drawdown cents magnitude
//! is written into `custom_metrics` as an integer-valued `Decimal`
//! (`Decimal::from(i64)`, scale 0 — exact, no fraction). The **only** floats
//! are the documented analytic ratios (returns, Sharpe, volatility, drawdown
//! ratio), guarded for `NaN`/`Inf` before they enter a field
//! ([`Decimal::from_f64_retain`] rejects a non-finite `f64`).
//!
//! # Determinism
//!
//! Every metric is a pure function of `(equity series, initial capital)` — no
//! wall clock, no RNG — so the same inputs always yield the same output
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use optionstratlib::backtesting::BacktestResult;
use optionstratlib::prelude::Positive;
use rust_decimal::Decimal;

use crate::domain::EquityPoint;
use crate::error::BacktestError;

/// `custom_metrics` key for the signed worst-drawdown ratio (`≤ 0`).
pub const MAX_DRAWDOWN_RATIO_KEY: &str = "max_drawdown_ratio";
/// `custom_metrics` key for the max peak-to-trough decline in integer cents.
pub const MAX_DRAWDOWN_CENTS_KEY: &str = "max_drawdown_cents";

/// Populate the minimal v0.1 summary metrics into `result` in place, from the
/// ledger's ordered per-step `equity_curve` and the run's `initial_capital_cents`.
///
/// The written fields are enumerated in the [module docs](self); every other
/// field of the upstream [`BacktestResult`] is left as it was (`Default` for a
/// fresh result, or the scalars [`crate::BacktestRun`] already carries). This
/// **populates the upstream struct** — there is no parallel IronCondor result
/// type.
///
/// `initial_capital_cents` is the ledger's step-`−1` baseline (the same value
/// the running drawdown peak is seeded with, [docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)),
/// so `total_return` and the drawdown cents magnitude share the run's baseline.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] if the checked integer-cents
/// arithmetic for the peak-to-trough drawdown magnitude overflows `i64`.
pub fn populate(
    result: &mut BacktestResult,
    equity_curve: &[EquityPoint],
    initial_capital_cents: i64,
) -> Result<(), BacktestError> {
    // --- total return: (final − initial) / initial, from integer cents ------
    // `final` is the last equity point (the ledger's terminal equity); for an
    // empty curve it degenerates to the baseline, giving a 0 return.
    let final_cents = equity_curve
        .last()
        .map_or(initial_capital_cents, |point| point.equity_cents);
    if initial_capital_cents != 0 {
        let delta = final_cents
            .checked_sub(initial_capital_cents)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        // Decimal division carries the default banker's-rounding to 28 sig
        // digits; the quotient is a ratio, the documented analytic exception.
        if let Some(total_return) =
            Decimal::from(delta).checked_div(Decimal::from(initial_capital_cents))
        {
            result.general_performance.total_return = total_return;
        }
    }

    // --- Sharpe + volatility from the per-step returns -----------------------
    let returns = step_returns(equity_curve);
    if let Some(mean) = mean(&returns)
        && let Some(stddev) = population_stddev(&returns, mean)
    {
        // Volatility is the return standard deviation (`≥ 0`); `Some(0)` for a
        // flat curve. Guard NaN/Inf via `from_f64_retain`, then `Positive`.
        if let Some(stddev_dec) = Decimal::from_f64_retain(stddev)
            && let Ok(volatility) = Positive::new_decimal(stddev_dec)
        {
            result.general_performance.volatility = Some(volatility);
        }
        // Sharpe is undefined for a zero standard deviation (a flat curve) —
        // left `None`, matching the upstream field's documented semantics.
        if let Some(sharpe) = sharpe(mean, stddev) {
            result.general_performance.sharpe_ratio = Decimal::from_f64_retain(sharpe);
        }
    }

    // --- max drawdown: ratio (from the ledger series) + cents magnitude ------
    // The ledger's `drawdown` is `≤ 0` (0 at a peak); the worst is its minimum.
    let worst_ratio = equity_curve
        .iter()
        .map(|point| point.drawdown)
        .fold(0.0_f64, f64::min);
    // Upstream `max_drawdown` is a non-negative magnitude ("0.1" = 10 % loss).
    if let Some(magnitude) = Decimal::from_f64_retain(-worst_ratio) {
        result.drawdown_analysis.max_drawdown = magnitude;
    }
    // Preserve the raw signed ratio (`≤ 0`) too, so no representation is lost.
    if let Some(ratio) = Decimal::from_f64_retain(worst_ratio) {
        result
            .custom_metrics
            .insert(MAX_DRAWDOWN_RATIO_KEY.to_string(), ratio);
    }
    // The peak-to-trough decline in integer cents (checked; no dedicated
    // upstream field, so it lives in `custom_metrics` as an exact integer).
    // This absolute-cents extremum and the ratio extremum above are computed
    // independently and need not fall on the same step.
    let drawdown_cents = max_drawdown_cents(equity_curve, initial_capital_cents)?;
    result.custom_metrics.insert(
        MAX_DRAWDOWN_CENTS_KEY.to_string(),
        Decimal::from(drawdown_cents),
    );

    Ok(())
}

/// The per-step equity returns between **consecutive** equity points:
/// `r = (equity_n − equity_{n-1}) / equity_{n-1}` ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
///
/// A pair whose prior equity is **zero** is undefined (division by zero) and is
/// **omitted** from the series; a non-finite result is likewise skipped. Fewer
/// than two points yields an empty series (Sharpe/volatility then undefined).
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "per-step returns are the documented analytic float exception (docs/05 §4); equity cents cast to f64 for the ratio"
)]
fn step_returns(equity_curve: &[EquityPoint]) -> Vec<f64> {
    let mut returns = Vec::with_capacity(equity_curve.len());
    for pair in equity_curve.windows(2) {
        let [prev, curr] = pair else { continue };
        let prev_cents = prev.equity_cents;
        if prev_cents == 0 {
            // zero-prior-equity guard: the return is undefined, omit it.
            continue;
        }
        let ret = (curr.equity_cents as f64 - prev_cents as f64) / prev_cents as f64;
        if ret.is_finite() {
            returns.push(ret);
        }
    }
    returns
}

/// The arithmetic mean of `values`, or `None` for an empty slice or a
/// non-finite sum.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "the count is cast to f64 to average the analytic-float returns"
)]
fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let sum: f64 = values.iter().sum();
    let mean = sum / values.len() as f64;
    mean.is_finite().then_some(mean)
}

/// The **population** standard deviation (divide by `N`) of `values` about
/// `mean`, or `None` for an empty slice or a non-finite result.
///
/// Population (not sample) standard deviation is the fixed v0.1 convention, so
/// a single return yields `0` and a flat curve yields `0` — both leaving Sharpe
/// undefined via [`sharpe`].
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "the count is cast to f64 to normalise the analytic-float variance"
)]
fn population_stddev(values: &[f64], mean: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    let stddev = variance.sqrt();
    stddev.is_finite().then_some(stddev)
}

/// The per-step Sharpe ratio `mean / stddev`, or `None` when the standard
/// deviation is zero (or the quotient is non-finite).
///
/// Not annualized — docs/05 §4 pins Sharpe to per-step returns with no
/// annualization factor.
#[must_use]
fn sharpe(mean: f64, stddev: f64) -> Option<f64> {
    if stddev <= 0.0 {
        return None;
    }
    let sharpe = mean / stddev;
    sharpe.is_finite().then_some(sharpe)
}

/// The maximum peak-to-trough equity decline in **integer cents**, with the
/// running peak seeded at `initial_capital_cents` (the ledger's step-`−1`
/// baseline, [docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)).
///
/// The running peak is `max(peak, equity)` so far, and `peak − equity ≥ 0` at
/// every point; the returned value is the largest such decline (`0` for a curve
/// that never dips below the baseline).
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] if `peak − equity` overflows
/// `i64` (a deeply negative equity against a large peak).
#[must_use = "the computed drawdown magnitude must be used"]
fn max_drawdown_cents(
    equity_curve: &[EquityPoint],
    initial_capital_cents: i64,
) -> Result<i64, BacktestError> {
    let mut peak = initial_capital_cents;
    let mut max_decline: i64 = 0;
    for point in equity_curve {
        if point.equity_cents > peak {
            peak = point.equity_cents;
        }
        let decline = peak
            .checked_sub(point.equity_cents)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if decline > max_decline {
            max_decline = decline;
        }
    }
    Ok(max_decline)
}

#[cfg(test)]
mod tests {
    use optionstratlib::backtesting::BacktestResult;
    use rust_decimal::Decimal;

    use super::{
        MAX_DRAWDOWN_CENTS_KEY, MAX_DRAWDOWN_RATIO_KEY, max_drawdown_cents, mean, populate,
        population_stddev, sharpe, step_returns,
    };
    use crate::domain::EquityPoint;

    const TS0: i64 = 1_750_291_200_000_000_000;
    /// Tolerance for float comparisons — never an exact `==` on `f64`.
    const TOL: f64 = 1e-9;

    /// One equity point carrying an explicit `equity_cents` and `drawdown`
    /// (cash/position split is irrelevant to the metrics, which read only these
    /// two fields).
    fn point(step: u32, equity_cents: i64, drawdown: f64) -> EquityPoint {
        EquityPoint::new(
            step,
            TS0 + i64::from(step),
            equity_cents,
            0,
            equity_cents,
            drawdown,
        )
    }

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < TOL
    }

    /// Convert a `Decimal` to `f64` for a tolerance comparison in tests only.
    fn dec_to_f64(d: Decimal) -> f64 {
        use rust_decimal::prelude::ToPrimitive;
        d.to_f64().unwrap_or(f64::NAN)
    }

    // --- returns + Sharpe (hand-computed) -----------------------------------

    #[test]
    fn test_step_returns_consecutive_points_known_values() {
        // equity [100, 200, 100] → returns [(200-100)/100, (100-200)/200].
        let curve = [point(0, 100, 0.0), point(1, 200, 0.0), point(2, 100, -0.5)];
        let returns = step_returns(&curve);
        assert_eq!(returns.len(), 2);
        assert!(matches!(returns.first(), Some(r) if approx(*r, 1.0)));
        assert!(matches!(returns.get(1), Some(r) if approx(*r, -0.5)));
    }

    #[test]
    fn test_sharpe_hand_built_series_matches_population_formula() {
        // returns [1.0, -0.5]: mean 0.25, population stddev 0.75, Sharpe 1/3.
        let returns = [1.0_f64, -0.5];
        let Some(m) = mean(&returns) else {
            panic!("mean is defined for a non-empty series");
        };
        assert!(approx(m, 0.25));
        let Some(sd) = population_stddev(&returns, m) else {
            panic!("stddev is defined for a non-empty series");
        };
        assert!(approx(sd, 0.75));
        let Some(sh) = sharpe(m, sd) else {
            panic!("Sharpe is defined for a non-zero stddev");
        };
        assert!(approx(sh, 1.0 / 3.0));
    }

    #[test]
    fn test_step_returns_zero_prior_equity_is_omitted() {
        // equity [0, 100, 200]: the (0 → 100) pair is undefined and omitted;
        // only the (100 → 200) return survives.
        let curve = [point(0, 0, 0.0), point(1, 100, 0.0), point(2, 200, 0.0)];
        let returns = step_returns(&curve);
        assert_eq!(returns.len(), 1, "the zero-prior-equity pair is skipped");
        assert!(matches!(returns.first(), Some(r) if approx(*r, 1.0)));
    }

    #[test]
    fn test_sharpe_empty_and_single_return_is_none() {
        assert!(mean(&[]).is_none());
        // A single return has population stddev 0 → Sharpe undefined.
        let single = [0.2_f64];
        let Some(m) = mean(&single) else {
            panic!("mean of one value is defined");
        };
        let Some(sd) = population_stddev(&single, m) else {
            panic!("stddev of one value is defined (0)");
        };
        assert!(approx(sd, 0.0));
        assert!(sharpe(m, sd).is_none(), "zero stddev → Sharpe none");
    }

    #[test]
    fn test_sharpe_flat_curve_stddev_zero_is_none() {
        // A flat equity curve → all returns 0 → stddev 0 → Sharpe undefined.
        let curve = [point(0, 100, 0.0), point(1, 100, 0.0), point(2, 100, 0.0)];
        let returns = step_returns(&curve);
        assert_eq!(returns.len(), 2);
        let Some(m) = mean(&returns) else {
            panic!("mean defined");
        };
        let Some(sd) = population_stddev(&returns, m) else {
            panic!("stddev defined");
        };
        assert!(approx(sd, 0.0));
        assert!(sharpe(m, sd).is_none());
    }

    // --- max drawdown (ratio + cents) ---------------------------------------

    #[test]
    fn test_max_drawdown_cents_seeded_at_initial_capital() {
        // initial 10_000; equity dips to 9_000 then recovers to 9_800.
        // running peak stays 10_000 → worst decline 1_000c.
        let curve = [
            point(0, 10_000, 0.0),
            point(1, 9_500, -0.05),
            point(2, 9_000, -0.10),
            point(3, 9_800, -0.02),
        ];
        let Ok(decline) = max_drawdown_cents(&curve, 10_000) else {
            panic!("the checked cents arithmetic succeeds");
        };
        assert_eq!(decline, 1_000);
    }

    #[test]
    fn test_max_drawdown_cents_handles_negative_equity_without_overflow() {
        // Re-exercise the zero/negative-equity drawdown regime through the
        // metrics summary: equity crosses zero into negative territory.
        let curve = [
            point(0, 10_000, 0.0),
            point(1, 0, -1.0),
            point(2, -5_000, -1.5),
        ];
        let Ok(decline) = max_drawdown_cents(&curve, 10_000) else {
            panic!("negative equity stays within checked i64 arithmetic");
        };
        // peak 10_000, trough −5_000 → 15_000c decline.
        assert_eq!(decline, 15_000);
    }

    // --- populate reads back from the UPSTREAM BacktestResult ----------------

    #[test]
    fn test_metrics_populate_backtest_result() {
        // A curve with a clear drawdown and non-zero return dispersion.
        // equity [10_000, 10_200, 9_800, 10_100]; drawdowns per the ledger.
        let curve = [
            point(0, 10_000, 0.0),
            point(1, 10_200, 0.0),
            point(2, 9_800, -0.039_215_686),
            point(3, 10_100, -0.009_803_921),
        ];
        // Populate the UPSTREAM optionstratlib result — not a parallel type.
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &curve, 10_000) else {
            panic!("populate succeeds on a well-formed curve");
        };

        // total_return = (10_100 − 10_000) / 10_000 = 0.01, read back from the
        // upstream general_performance slice.
        assert!(approx(
            dec_to_f64(result.general_performance.total_return),
            0.01
        ));

        // Sharpe + volatility are populated on the upstream struct (Some here,
        // since the returns have non-zero dispersion).
        assert!(
            result.general_performance.sharpe_ratio.is_some(),
            "Sharpe populated for a dispersed return series"
        );
        assert!(
            result.general_performance.volatility.is_some(),
            "volatility populated for a non-empty return series"
        );

        // max_drawdown is the non-negative magnitude of the worst ledger ratio
        // (min = −0.039215686 → magnitude 0.039215686), on the upstream field.
        assert!(approx(
            dec_to_f64(result.drawdown_analysis.max_drawdown),
            0.039_215_686
        ));

        // The signed ratio and the cents magnitude are in custom_metrics.
        let ratio = result.custom_metrics.get(MAX_DRAWDOWN_RATIO_KEY);
        assert!(matches!(ratio, Some(r) if dec_to_f64(*r) < 0.0));
        // worst cents decline: peak 10_200, trough 9_800 → 400c.
        assert!(matches!(
            result.custom_metrics.get(MAX_DRAWDOWN_CENTS_KEY),
            Some(v) if *v == Decimal::from(400)
        ));
    }

    #[test]
    fn test_populate_money_field_is_integer_cents_only_ratios_float() {
        let curve = [point(0, 10_000, 0.0), point(1, 9_400, -0.06)];
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &curve, 10_000) else {
            panic!("populate succeeds");
        };
        // The cents magnitude is an EXACT integer-valued Decimal (scale 0, no
        // fraction) — money stays integer cents.
        let Some(cents) = result.custom_metrics.get(MAX_DRAWDOWN_CENTS_KEY) else {
            panic!("the cents magnitude is populated");
        };
        assert!(cents.fract().is_zero(), "cents magnitude has no fraction");
        assert_eq!(*cents, Decimal::from(600), "peak 10_000 − trough 9_400");
        // The drawdown ratio, by contrast, IS a fractional analytic float.
        let Some(ratio) = result.custom_metrics.get(MAX_DRAWDOWN_RATIO_KEY) else {
            panic!("the ratio is populated");
        };
        assert!(!ratio.fract().is_zero(), "the ratio is a fractional value");
    }

    #[test]
    fn test_populate_empty_curve_leaves_metrics_undefined() {
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &[], 10_000) else {
            panic!("populate is a no-op-safe on an empty curve");
        };
        assert!(result.general_performance.sharpe_ratio.is_none());
        assert!(result.general_performance.volatility.is_none());
        // final degenerates to the baseline → 0 total return, 0 drawdown.
        assert!(result.general_performance.total_return.is_zero());
        assert!(result.drawdown_analysis.max_drawdown.is_zero());
        assert!(matches!(
            result.custom_metrics.get(MAX_DRAWDOWN_CENTS_KEY),
            Some(v) if v.is_zero()
        ));
    }

    #[test]
    fn test_populate_flat_curve_volatility_zero_sharpe_none() {
        let curve = [
            point(0, 10_000, 0.0),
            point(1, 10_000, 0.0),
            point(2, 10_000, 0.0),
        ];
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &curve, 10_000) else {
            panic!("populate succeeds on a flat curve");
        };
        // Flat curve: volatility defined (0), Sharpe undefined.
        assert!(result.general_performance.sharpe_ratio.is_none());
        let Some(volatility) = result.general_performance.volatility else {
            panic!("volatility is defined (0) for a flat curve");
        };
        assert!(volatility.to_dec().is_zero());
    }

    #[test]
    fn test_populate_is_deterministic_for_same_inputs() {
        let curve = [
            point(0, 10_000, 0.0),
            point(1, 10_500, 0.0),
            point(2, 9_900, -0.057_142_857),
        ];
        let mut a = BacktestResult::default();
        let mut b = BacktestResult::default();
        let Ok(()) = populate(&mut a, &curve, 10_000) else {
            panic!("populate a succeeds");
        };
        let Ok(()) = populate(&mut b, &curve, 10_000) else {
            panic!("populate b succeeds");
        };
        assert_eq!(
            a.general_performance.sharpe_ratio,
            b.general_performance.sharpe_ratio
        );
        assert_eq!(
            a.general_performance.total_return,
            b.general_performance.total_return
        );
        assert_eq!(
            a.drawdown_analysis.max_drawdown,
            b.drawdown_analysis.max_drawdown
        );
        assert_eq!(
            a.custom_metrics.get(MAX_DRAWDOWN_CENTS_KEY),
            b.custom_metrics.get(MAX_DRAWDOWN_CENTS_KEY)
        );
    }
}
