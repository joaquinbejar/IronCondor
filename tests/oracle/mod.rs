//! The **single comparison oracle** for every golden and parity test.
//!
//! One implementation, reused by the v0.1 equity-curve + minimal-metrics golden
//! ([`tests/golden.rs`](../golden.rs)) and by later golden/parity tests, exactly
//! as the determinism contract requires: "one implementation, used by every
//! golden and parity test"
//! ([docs/02 §7](../../docs/02-engine-architecture.md#7-determinism-and-reproducibility),
//! [docs/TESTING.md §4](../../docs/TESTING.md#4-golden-file-backtests)).
//!
//! # What it compares
//!
//! The oracle decodes the produced and expected artifacts, **sorts by the
//! table's key** (`step` for the equity curve), then:
//!
//! - compares every **money / integer** column (`cash_cents`,
//!   `position_value_cents`, `equity_cents`, `ts_ns`, `step`, and the
//!   `max_drawdown_cents` metric) **exactly** as integers, and
//! - compares every **analytic float** column (`drawdown`) and every **ratio**
//!   metric (`sharpe_ratio`, `volatility`, `total_return`, `max_drawdown`,
//!   `max_drawdown_ratio`) within the fixed cross-environment tolerance from
//!   [docs/05 §12.5](../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause).
//!
//! A mismatch returns a structured [`OracleDiff`] naming the diverging
//! `context`, `step`, and `field` so a failure is self-locating.
//!
//! # Tolerance ([docs/05 §12.5](../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause))
//!
//! The fixed tolerance is `|a − b| ≤ max(1e-9, 1e-6 × max(|a|,|b|))`, with signed
//! zero treated as equal, `NaN` **never** equal (a produced `NaN` is a bug the
//! oracle surfaces as a mismatch), and `±∞` equal only to the same infinity.
//! Integer cents are compared exactly and never through this tolerance.
//!
//! This module is compiled into whichever test binary includes it (`mod
//! oracle;`); each binary uses a subset, so `dead_code` is allowed.

#![allow(dead_code)]

use std::fmt;

use ironcondor::EquityPoint;
use ironcondor::analytics::metrics::{MAX_DRAWDOWN_CENTS_KEY, MAX_DRAWDOWN_RATIO_KEY};
use optionstratlib::backtesting::BacktestResult;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

/// The fixed cross-environment float tolerance
/// ([docs/05 §12.5](../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)):
/// `|a − b| ≤ max(ABS_TOLERANCE, REL_TOLERANCE × max(|a|,|b|))`.
pub const ABS_TOLERANCE: f64 = 1e-9;
/// The relative arm of the tolerance (see [`ABS_TOLERANCE`]).
pub const REL_TOLERANCE: f64 = 1e-6;

/// The minimal v0.1 metrics summary the golden freezes — the six values
/// derivable from the equity series
/// ([docs/05 §4](../../docs/05-analytics-and-reporting.md#4-summary-metrics)),
/// extracted from the upstream [`BacktestResult`] via [`Self::from_result`].
///
/// `max_drawdown_cents` is an **integer-cents** money value (compared exactly);
/// every other field is an analytic ratio (compared within the tolerance).
/// `sharpe_ratio` / `volatility` are `Option` — `None` when the curve cannot
/// define them (fewer than two points, or a flat curve).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsSummary {
    /// Per-step Sharpe ratio, or `None` for a flat / too-short curve.
    pub sharpe_ratio: Option<Decimal>,
    /// Per-step return volatility, or `None` for a too-short curve.
    pub volatility: Option<Decimal>,
    /// `(final − initial) / initial`, a ratio.
    pub total_return: Decimal,
    /// Worst drawdown as a non-negative magnitude.
    pub max_drawdown: Decimal,
    /// Worst drawdown as the raw signed ratio (`≤ 0`).
    pub max_drawdown_ratio: Decimal,
    /// Worst peak-to-trough decline in **integer cents** (exact).
    pub max_drawdown_cents: i64,
}

impl MetricsSummary {
    /// Extract the minimal v0.1 metrics from the upstream [`BacktestResult`]
    /// that [`ironcondor::analytics::metrics::populate`] filled.
    ///
    /// # Errors
    ///
    /// Returns a message string if a required `custom_metrics` key is missing or
    /// the cents magnitude is not an integer — either indicates the metrics pass
    /// did not run or a schema drift, both of which must fail the golden loudly.
    pub fn from_result(result: &BacktestResult) -> Result<Self, String> {
        let ratio = result
            .custom_metrics
            .get(MAX_DRAWDOWN_RATIO_KEY)
            .copied()
            .ok_or_else(|| format!("missing custom metric {MAX_DRAWDOWN_RATIO_KEY}"))?;
        let cents_dec = result
            .custom_metrics
            .get(MAX_DRAWDOWN_CENTS_KEY)
            .copied()
            .ok_or_else(|| format!("missing custom metric {MAX_DRAWDOWN_CENTS_KEY}"))?;
        let max_drawdown_cents = cents_dec
            .to_i64()
            .ok_or_else(|| format!("{MAX_DRAWDOWN_CENTS_KEY} is not an integer: {cents_dec}"))?;
        Ok(Self {
            sharpe_ratio: result.general_performance.sharpe_ratio,
            volatility: result
                .general_performance
                .volatility
                .as_ref()
                .map(|v| v.to_dec()),
            total_return: result.general_performance.total_return,
            max_drawdown: result.drawdown_analysis.max_drawdown,
            max_drawdown_ratio: ratio,
            max_drawdown_cents,
        })
    }
}

/// A single located divergence: which artifact, which step (if a table row),
/// which field, and the two rendered values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleDiff {
    /// The artifact context — `"equity_curve"` or `"metrics"`.
    pub context: &'static str,
    /// The row's `step` for a table divergence; `None` for a scalar metric.
    pub step: Option<u32>,
    /// The diverging field name.
    pub field: &'static str,
    /// The produced value, rendered.
    pub produced: String,
    /// The expected value, rendered.
    pub expected: String,
}

impl OracleDiff {
    fn new(
        context: &'static str,
        step: Option<u32>,
        field: &'static str,
        produced: String,
        expected: String,
    ) -> Self {
        Self {
            context,
            step,
            field,
            produced,
            expected,
        }
    }
}

impl fmt::Display for OracleDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.step {
            Some(step) => write!(
                f,
                "{}[step {}].{}: produced {} != expected {}",
                self.context, step, self.field, self.produced, self.expected
            ),
            None => write!(
                f,
                "{}.{}: produced {} != expected {}",
                self.context, self.field, self.produced, self.expected
            ),
        }
    }
}

/// Two finite floats are equal within the fixed tolerance; `NaN` never equal;
/// `±∞` equal only to the same infinity; signed zero equal.
#[must_use]
pub fn floats_equal(a: f64, b: f64) -> bool {
    if a.is_nan() || b.is_nan() {
        // A produced NaN is itself a load error (docs/05 §12.5) — never equal.
        return false;
    }
    if a.is_infinite() || b.is_infinite() {
        // ±∞ equal only to the same infinity (exact bit-pattern sign match).
        return a == b;
    }
    let tol = f64::max(ABS_TOLERANCE, REL_TOLERANCE * f64::max(a.abs(), b.abs()));
    (a - b).abs() <= tol
}

/// A copy of `points` sorted by `step` (the equity-curve table key). Stable so
/// equal keys keep their input order; the sort is the oracle's normalisation
/// step before any comparison.
#[must_use]
pub fn sorted_by_step(points: &[EquityPoint]) -> Vec<EquityPoint> {
    let mut sorted = points.to_vec();
    sorted.sort_by_key(|point| point.step);
    sorted
}

/// Compare two equity curves under the oracle: sort by `step`, then compare
/// every integer column exactly and `drawdown` within the tolerance.
///
/// # Errors
///
/// Returns the first [`OracleDiff`] — a length mismatch, an unequal integer
/// column, or an out-of-tolerance `drawdown` — naming the diverging step+field.
pub fn compare_equity_curves(
    produced: &[EquityPoint],
    expected: &[EquityPoint],
) -> Result<(), OracleDiff> {
    let produced = sorted_by_step(produced);
    let expected = sorted_by_step(expected);
    if produced.len() != expected.len() {
        return Err(OracleDiff::new(
            "equity_curve",
            None,
            "len",
            produced.len().to_string(),
            expected.len().to_string(),
        ));
    }
    for (p, e) in produced.iter().zip(expected.iter()) {
        let step = Some(p.step);
        cmp_int(
            "equity_curve",
            step,
            "step",
            i64::from(p.step),
            i64::from(e.step),
        )?;
        cmp_int("equity_curve", step, "ts_ns", p.ts_ns, e.ts_ns)?;
        cmp_int(
            "equity_curve",
            step,
            "cash_cents",
            p.cash_cents,
            e.cash_cents,
        )?;
        cmp_int(
            "equity_curve",
            step,
            "position_value_cents",
            p.position_value_cents,
            e.position_value_cents,
        )?;
        cmp_int(
            "equity_curve",
            step,
            "equity_cents",
            p.equity_cents,
            e.equity_cents,
        )?;
        if !floats_equal(p.drawdown, e.drawdown) {
            return Err(OracleDiff::new(
                "equity_curve",
                step,
                "drawdown",
                p.drawdown.to_string(),
                e.drawdown.to_string(),
            ));
        }
    }
    Ok(())
}

/// Compare two [`MetricsSummary`] under the oracle: `max_drawdown_cents` exactly
/// as integer cents, every ratio within the tolerance.
///
/// # Errors
///
/// Returns the first [`OracleDiff`] naming the diverging metric field.
pub fn compare_metrics(
    produced: &MetricsSummary,
    expected: &MetricsSummary,
) -> Result<(), OracleDiff> {
    cmp_int(
        "metrics",
        None,
        "max_drawdown_cents",
        produced.max_drawdown_cents,
        expected.max_drawdown_cents,
    )?;
    cmp_dec_tol("total_return", produced.total_return, expected.total_return)?;
    cmp_dec_tol("max_drawdown", produced.max_drawdown, expected.max_drawdown)?;
    cmp_dec_tol(
        "max_drawdown_ratio",
        produced.max_drawdown_ratio,
        expected.max_drawdown_ratio,
    )?;
    cmp_opt_dec_tol("sharpe_ratio", produced.sharpe_ratio, expected.sharpe_ratio)?;
    cmp_opt_dec_tol("volatility", produced.volatility, expected.volatility)?;
    Ok(())
}

fn cmp_int(
    context: &'static str,
    step: Option<u32>,
    field: &'static str,
    a: i64,
    b: i64,
) -> Result<(), OracleDiff> {
    if a == b {
        Ok(())
    } else {
        Err(OracleDiff::new(
            context,
            step,
            field,
            a.to_string(),
            b.to_string(),
        ))
    }
}

fn cmp_dec_tol(field: &'static str, a: Decimal, b: Decimal) -> Result<(), OracleDiff> {
    match (a.to_f64(), b.to_f64()) {
        (Some(x), Some(y)) if floats_equal(x, y) => Ok(()),
        _ => Err(OracleDiff::new(
            "metrics",
            None,
            field,
            a.to_string(),
            b.to_string(),
        )),
    }
}

fn cmp_opt_dec_tol(
    field: &'static str,
    a: Option<Decimal>,
    b: Option<Decimal>,
) -> Result<(), OracleDiff> {
    match (a, b) {
        (None, None) => Ok(()),
        (Some(x), Some(y)) => cmp_dec_tol(field, x, y),
        (produced, expected) => Err(OracleDiff::new(
            "metrics",
            None,
            field,
            opt_dec_str(produced),
            opt_dec_str(expected),
        )),
    }
}

fn opt_dec_str(value: Option<Decimal>) -> String {
    value.map_or_else(|| "None".to_string(), |d| d.to_string())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        MetricsSummary, compare_equity_curves, compare_metrics, floats_equal, sorted_by_step,
    };
    use ironcondor::EquityPoint;

    const TS0: i64 = 1_750_291_200_000_000_000;

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

    fn metrics(total_return: Decimal, max_drawdown_cents: i64) -> MetricsSummary {
        MetricsSummary {
            sharpe_ratio: None,
            volatility: Some(Decimal::ZERO),
            total_return,
            max_drawdown: Decimal::ZERO,
            max_drawdown_ratio: Decimal::ZERO,
            max_drawdown_cents,
        }
    }

    #[test]
    fn test_floats_equal_within_tolerance_passes() {
        // A sub-1e-9 gap and a relative gap under 1e-6 both compare equal.
        assert!(floats_equal(0.123_456_789, 0.123_456_789_5));
        assert!(floats_equal(1_000.0, 1_000.000_5));
        // Signed zero compares equal.
        assert!(floats_equal(0.0, -0.0));
    }

    #[test]
    fn test_floats_equal_out_of_tolerance_fails() {
        assert!(!floats_equal(0.10, 0.1001));
        // NaN never equal, even to itself; ±∞ only to the same infinity.
        assert!(!floats_equal(f64::NAN, f64::NAN));
        assert!(!floats_equal(f64::INFINITY, f64::NEG_INFINITY));
        assert!(floats_equal(f64::INFINITY, f64::INFINITY));
    }

    #[test]
    fn test_compare_equity_curves_identical_passes() {
        let curve = [point(0, 100, 0.0), point(1, 90, -0.1)];
        assert!(matches!(compare_equity_curves(&curve, &curve), Ok(())));
    }

    #[test]
    fn test_compare_equity_curves_cents_mismatch_fails() {
        // Hold cash and position fixed so the *only* diverging column is
        // `equity_cents` (columns are compared in field order).
        let produced = [EquityPoint::new(1, TS0 + 1, 100, 0, 90, -0.1)];
        let expected = [EquityPoint::new(1, TS0 + 1, 100, 0, 91, -0.1)];
        let Err(diff) = compare_equity_curves(&produced, &expected) else {
            panic!("a cents divergence must fail the oracle");
        };
        assert_eq!(diff.field, "equity_cents");
        assert_eq!(diff.step, Some(1));
    }

    #[test]
    fn test_compare_equity_curves_drawdown_within_tolerance_passes() {
        // The drawdown differs only below the absolute tolerance floor.
        let produced = [point(0, 100, -0.250_000_000_1)];
        let expected = [point(0, 100, -0.25)];
        assert!(matches!(
            compare_equity_curves(&produced, &expected),
            Ok(())
        ));
    }

    #[test]
    fn test_compare_equity_curves_drawdown_out_of_tolerance_fails() {
        let produced = [point(0, 100, -0.250)];
        let expected = [point(0, 100, -0.251)];
        let Err(diff) = compare_equity_curves(&produced, &expected) else {
            panic!("an out-of-tolerance drawdown must fail the oracle");
        };
        assert_eq!(diff.field, "drawdown");
        assert_eq!(diff.step, Some(0));
    }

    #[test]
    fn test_sorted_by_step_shuffled_input_orders_by_step() {
        let shuffled = [point(2, 80, -0.2), point(0, 100, 0.0), point(1, 90, -0.1)];
        let sorted = sorted_by_step(&shuffled);
        let steps: Vec<u32> = sorted.iter().map(|p| p.step).collect();
        assert_eq!(steps, vec![0, 1, 2]);
    }

    #[test]
    fn test_compare_equity_curves_shuffled_input_sorted_before_compare() {
        // Same rows, different input order → equal after the oracle sorts both.
        let produced = [point(2, 80, -0.2), point(0, 100, 0.0), point(1, 90, -0.1)];
        let expected = [point(0, 100, 0.0), point(1, 90, -0.1), point(2, 80, -0.2)];
        assert!(matches!(
            compare_equity_curves(&produced, &expected),
            Ok(())
        ));
    }

    #[test]
    fn test_compare_metrics_cents_mismatch_fails() {
        let produced = metrics(Decimal::new(1, 2), 400);
        let expected = metrics(Decimal::new(1, 2), 401);
        let Err(diff) = compare_metrics(&produced, &expected) else {
            panic!("a cents divergence must fail the metrics oracle");
        };
        assert_eq!(diff.field, "max_drawdown_cents");
    }

    #[test]
    fn test_compare_metrics_ratio_within_tolerance_passes() {
        // total_return differs below the tolerance floor; cents are identical.
        // 0.1000000001 vs 0.1000000000 (scale 10) — a 1e-10 gap, under the floor.
        let produced = metrics(Decimal::new(1_000_000_001, 10), 400);
        let expected = metrics(Decimal::new(1_000_000_000, 10), 400);
        assert!(matches!(compare_metrics(&produced, &expected), Ok(())));
    }

    #[test]
    fn test_compare_metrics_option_shape_mismatch_fails() {
        let mut produced = metrics(Decimal::ZERO, 0);
        produced.sharpe_ratio = Some(Decimal::ZERO);
        let expected = metrics(Decimal::ZERO, 0); // sharpe_ratio None
        let Err(diff) = compare_metrics(&produced, &expected) else {
            panic!("a Some/None shape mismatch must fail the oracle");
        };
        assert_eq!(diff.field, "sharpe_ratio");
    }
}
