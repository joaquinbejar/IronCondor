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

use ironcondor::analytics::metrics::{
    LONG_LEGS_REALIZED_CENTS_KEY, MAX_DRAWDOWN_CENTS_KEY, MAX_DRAWDOWN_RATIO_KEY,
    NET_PREMIUM_CENTS_KEY, REALIZED_PNL_CENTS_KEY, SHORT_LEGS_REALIZED_CENTS_KEY,
};
use ironcondor::{
    EquityPoint, FillRow, GreeksAttributionRow, PositionRow, ValidatedBundle, fill_sort_key,
    greeks_sort_key, position_sort_key,
};
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

/// The metrics summary the golden freezes — the equity-derived ratios plus the
/// #32 trade-log slice (win/loss counts, Sortino, per-leg realised split), all
/// extracted from the upstream [`BacktestResult`] via [`Self::from_result`]
/// ([docs/05 §4](../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
///
/// The `*_cents` fields are **integer-cents** money values (compared exactly);
/// every ratio field is an analytic float (compared within the tolerance).
/// `Option` ratios are `None` when the curve/log cannot define them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsSummary {
    /// Per-step Sharpe ratio, or `None` for a flat / too-short curve.
    pub sharpe_ratio: Option<Decimal>,
    /// Per-step Sortino ratio, or `None` when downside deviation is zero.
    pub sortino_ratio: Option<Decimal>,
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
    /// Number of realised leg closes.
    pub number_of_trades: usize,
    /// Realised leg closes that made a profit.
    pub winners: usize,
    /// Realised leg closes that took a loss.
    pub losers: usize,
    /// Win rate `winners / number_of_trades`, or `None` with no trades.
    pub win_rate: Option<Decimal>,
    /// Total realised P&L across all closes in **integer cents** (exact).
    pub realized_pnl_cents: i64,
    /// Realised P&L of the short legs (short strikes) in **integer cents**.
    pub short_legs_realized_cents: i64,
    /// Realised P&L of the long legs (long wings) in **integer cents**.
    pub long_legs_realized_cents: i64,
    /// Net entry premium (received − paid) in **integer cents** (exact).
    pub net_premium_cents: i64,
}

impl MetricsSummary {
    /// Extract the metrics summary from the upstream [`BacktestResult`] that
    /// [`ironcondor::analytics::metrics::populate`] filled.
    ///
    /// # Errors
    ///
    /// Returns a message string if a required `custom_metrics` key is missing or
    /// a cents value is not an integer — either indicates the metrics pass did
    /// not run or a schema drift, both of which must fail the golden loudly.
    pub fn from_result(result: &BacktestResult) -> Result<Self, String> {
        let ratio = result
            .custom_metrics
            .get(MAX_DRAWDOWN_RATIO_KEY)
            .copied()
            .ok_or_else(|| format!("missing custom metric {MAX_DRAWDOWN_RATIO_KEY}"))?;
        Ok(Self {
            sharpe_ratio: result.general_performance.sharpe_ratio,
            sortino_ratio: result.general_performance.sortino_ratio,
            volatility: result
                .general_performance
                .volatility
                .as_ref()
                .map(|v| v.to_dec()),
            total_return: result.general_performance.total_return,
            max_drawdown: result.drawdown_analysis.max_drawdown,
            max_drawdown_ratio: ratio,
            max_drawdown_cents: cents_metric(result, MAX_DRAWDOWN_CENTS_KEY)?,
            number_of_trades: result.trade_statistics.number_of_trades,
            winners: result.trade_statistics.winners,
            losers: result.trade_statistics.losers,
            win_rate: result.general_performance.win_rate,
            realized_pnl_cents: cents_metric(result, REALIZED_PNL_CENTS_KEY)?,
            short_legs_realized_cents: cents_metric(result, SHORT_LEGS_REALIZED_CENTS_KEY)?,
            long_legs_realized_cents: cents_metric(result, LONG_LEGS_REALIZED_CENTS_KEY)?,
            net_premium_cents: cents_metric(result, NET_PREMIUM_CENTS_KEY)?,
        })
    }
}

/// Read an integer-cents `custom_metrics` value, failing loudly if it is missing
/// or not an exact integer.
///
/// A fractional `Decimal` is **rejected** (not silently truncated): `to_i64`
/// discards the fractional part, so `1234.5` would otherwise pass as `1234` and
/// let a real cents divergence slip through the golden. The fractional check runs
/// **before** the conversion so a non-integer cents value is a loud oracle error.
fn cents_metric(result: &BacktestResult, key: &str) -> Result<i64, String> {
    let value = result
        .custom_metrics
        .get(key)
        .copied()
        .ok_or_else(|| format!("missing custom metric {key}"))?;
    if !value.fract().is_zero() {
        return Err(format!(
            "{key} is not an integer number of cents: {value} has a fractional part"
        ));
    }
    value
        .to_i64()
        .ok_or_else(|| format!("{key} is not representable as i64 cents: {value}"))
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
    // Integer-cents money and integer counts compared exactly.
    cmp_int(
        "metrics",
        None,
        "max_drawdown_cents",
        produced.max_drawdown_cents,
        expected.max_drawdown_cents,
    )?;
    cmp_int(
        "metrics",
        None,
        "realized_pnl_cents",
        produced.realized_pnl_cents,
        expected.realized_pnl_cents,
    )?;
    cmp_int(
        "metrics",
        None,
        "short_legs_realized_cents",
        produced.short_legs_realized_cents,
        expected.short_legs_realized_cents,
    )?;
    cmp_int(
        "metrics",
        None,
        "long_legs_realized_cents",
        produced.long_legs_realized_cents,
        expected.long_legs_realized_cents,
    )?;
    cmp_int(
        "metrics",
        None,
        "net_premium_cents",
        produced.net_premium_cents,
        expected.net_premium_cents,
    )?;
    cmp_usize(
        "number_of_trades",
        produced.number_of_trades,
        expected.number_of_trades,
    )?;
    cmp_usize("winners", produced.winners, expected.winners)?;
    cmp_usize("losers", produced.losers, expected.losers)?;
    // Analytic ratios compared within the tolerance.
    cmp_dec_tol("total_return", produced.total_return, expected.total_return)?;
    cmp_dec_tol("max_drawdown", produced.max_drawdown, expected.max_drawdown)?;
    cmp_dec_tol(
        "max_drawdown_ratio",
        produced.max_drawdown_ratio,
        expected.max_drawdown_ratio,
    )?;
    cmp_opt_dec_tol("sharpe_ratio", produced.sharpe_ratio, expected.sharpe_ratio)?;
    cmp_opt_dec_tol(
        "sortino_ratio",
        produced.sortino_ratio,
        expected.sortino_ratio,
    )?;
    cmp_opt_dec_tol("volatility", produced.volatility, expected.volatility)?;
    cmp_opt_dec_tol("win_rate", produced.win_rate, expected.win_rate)?;
    Ok(())
}

/// Compare two `usize` counts exactly, naming the field on a mismatch.
fn cmp_usize(field: &'static str, a: usize, b: usize) -> Result<(), OracleDiff> {
    if a == b {
        Ok(())
    } else {
        Err(OracleDiff::new(
            "metrics",
            None,
            field,
            a.to_string(),
            b.to_string(),
        ))
    }
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

// ---------------------------------------------------------------------------
// The result-bundle oracle (#36): the four Parquet tables + the canonical-JSON
// manifest. The **single** cross-environment logical-equivalence oracle for the
// result bundle (docs/05 §12.5): decode (done by `read_bundle`), sort by each
// table's pinned key, compare money/id columns EXACTLY as integer cents, the
// one float column (`drawdown`) within the tolerance, and the manifest as
// canonical JSON with the provenance/operational fields excluded.
// ---------------------------------------------------------------------------

/// Compare two decoded [`ValidatedBundle`]s' four tables under the oracle:
/// each table sorted by its pinned key, every integer/id/string column exact,
/// and `equity_curve.drawdown` within the tolerance. The manifest is compared
/// separately by [`compare_manifest_json`] (canonical JSON, `created_utc`
/// excluded), because [`ValidatedBundle`]'s manifest keeps `metrics` opaque.
///
/// # Errors
///
/// The first [`OracleDiff`] across the four tables, naming the diverging
/// table + step + field.
pub fn compare_bundle_tables(
    produced: &ValidatedBundle,
    expected: &ValidatedBundle,
) -> Result<(), OracleDiff> {
    compare_fills(&produced.fills, &expected.fills)?;
    compare_equity_curves(&produced.equity_curve, &expected.equity_curve)?;
    compare_positions(&produced.positions, &expected.positions)?;
    compare_greeks(&produced.greeks_attribution, &expected.greeks_attribution)?;
    Ok(())
}

/// Compare two `fills.parquet` decodings: sort by `(step, order_id, fill_seq)`
/// (the unique key), then every column exact (no float columns).
///
/// # Errors
///
/// The first diverging `(step, field)` as an [`OracleDiff`].
pub fn compare_fills(produced: &[FillRow], expected: &[FillRow]) -> Result<(), OracleDiff> {
    let produced = sorted(produced, fill_sort_key);
    let expected = sorted(expected, fill_sort_key);
    len_check("fills", produced.len(), expected.len())?;
    for (p, e) in produced.iter().zip(expected.iter()) {
        let step = Some(p.step);
        cmp_int("fills", step, "step", i64::from(p.step), i64::from(e.step))?;
        cmp_int("fills", step, "ts_ns", p.ts_ns, e.ts_ns)?;
        cmp_str(
            "fills",
            step,
            "strategy_run_id",
            &p.strategy_run_id,
            &e.strategy_run_id,
        )?;
        cmp_u64("fills", step, "trade_id", p.trade_id, e.trade_id)?;
        cmp_u64("fills", step, "position_id", p.position_id, e.position_id)?;
        cmp_u64("fills", step, "order_id", p.order_id, e.order_id)?;
        cmp_int(
            "fills",
            step,
            "fill_seq",
            i64::from(p.fill_seq),
            i64::from(e.fill_seq),
        )?;
        cmp_str("fills", step, "underlying", &p.underlying, &e.underlying)?;
        cmp_int(
            "fills",
            step,
            "expiration_ns",
            p.expiration_ns,
            e.expiration_ns,
        )?;
        cmp_str("fills", step, "contract_id", &p.contract_id, &e.contract_id)?;
        cmp_u64(
            "fills",
            step,
            "strike_cents",
            p.strike_cents,
            e.strike_cents,
        )?;
        cmp_str("fills", step, "style", &p.style, &e.style)?;
        cmp_str("fills", step, "side", &p.side, &e.side)?;
        cmp_int(
            "fills",
            step,
            "quantity",
            i64::from(p.quantity),
            i64::from(e.quantity),
        )?;
        cmp_u64("fills", step, "price_cents", p.price_cents, e.price_cents)?;
        cmp_int("fills", step, "fees_cents", p.fees_cents, e.fees_cents)?;
        cmp_int(
            "fills",
            step,
            "slippage_cents",
            p.slippage_cents,
            e.slippage_cents,
        )?;
        cmp_str("fills", step, "mode", &p.mode, &e.mode)?;
    }
    Ok(())
}

/// Compare two `positions.parquet` decodings: sort by `(step, position_id)`,
/// then every column exact (`exit_reason` the only nullable column; no floats).
///
/// # Errors
///
/// The first diverging `(step, field)` as an [`OracleDiff`].
pub fn compare_positions(
    produced: &[PositionRow],
    expected: &[PositionRow],
) -> Result<(), OracleDiff> {
    let produced = sorted(produced, position_sort_key);
    let expected = sorted(expected, position_sort_key);
    len_check("positions", produced.len(), expected.len())?;
    for (p, e) in produced.iter().zip(expected.iter()) {
        let step = Some(p.step);
        cmp_int(
            "positions",
            step,
            "step",
            i64::from(p.step),
            i64::from(e.step),
        )?;
        cmp_int("positions", step, "ts_ns", p.ts_ns, e.ts_ns)?;
        cmp_u64(
            "positions",
            step,
            "position_id",
            p.position_id,
            e.position_id,
        )?;
        cmp_u64("positions", step, "trade_id", p.trade_id, e.trade_id)?;
        cmp_str(
            "positions",
            step,
            "contract_id",
            &p.contract_id,
            &e.contract_id,
        )?;
        cmp_str("positions", step, "side", &p.side, &e.side)?;
        cmp_int(
            "positions",
            step,
            "quantity",
            i64::from(p.quantity),
            i64::from(e.quantity),
        )?;
        cmp_u64(
            "positions",
            step,
            "avg_price_cents",
            p.avg_price_cents,
            e.avg_price_cents,
        )?;
        cmp_u64("positions", step, "mark_cents", p.mark_cents, e.mark_cents)?;
        cmp_int(
            "positions",
            step,
            "unrealized_cents",
            p.unrealized_cents,
            e.unrealized_cents,
        )?;
        cmp_bool("positions", step, "stale_mark", p.stale_mark, e.stale_mark)?;
        cmp_opt_str(
            "positions",
            step,
            "exit_reason",
            p.exit_reason.as_deref(),
            e.exit_reason.as_deref(),
        )?;
        cmp_bool(
            "positions",
            step,
            "open_at_end",
            p.open_at_end,
            e.open_at_end,
        )?;
    }
    Ok(())
}

/// Compare two `greeks_attribution.parquet` decodings: sort by `step`, every
/// integer-cents term exact.
///
/// # Errors
///
/// The first diverging `(step, field)` as an [`OracleDiff`].
pub fn compare_greeks(
    produced: &[GreeksAttributionRow],
    expected: &[GreeksAttributionRow],
) -> Result<(), OracleDiff> {
    let produced = sorted(produced, greeks_sort_key);
    let expected = sorted(expected, greeks_sort_key);
    len_check("greeks_attribution", produced.len(), expected.len())?;
    for (p, e) in produced.iter().zip(expected.iter()) {
        let step = Some(p.step);
        cmp_int(
            "greeks_attribution",
            step,
            "step",
            i64::from(p.step),
            i64::from(e.step),
        )?;
        cmp_int("greeks_attribution", step, "ts_ns", p.ts_ns, e.ts_ns)?;
        cmp_int(
            "greeks_attribution",
            step,
            "theta_pnl_cents",
            p.theta_pnl_cents,
            e.theta_pnl_cents,
        )?;
        cmp_int(
            "greeks_attribution",
            step,
            "delta_pnl_cents",
            p.delta_pnl_cents,
            e.delta_pnl_cents,
        )?;
        cmp_int(
            "greeks_attribution",
            step,
            "vega_pnl_cents",
            p.vega_pnl_cents,
            e.vega_pnl_cents,
        )?;
        cmp_int(
            "greeks_attribution",
            step,
            "spread_capture_cents",
            p.spread_capture_cents,
            e.spread_capture_cents,
        )?;
        cmp_int(
            "greeks_attribution",
            step,
            "fees_cents",
            p.fees_cents,
            e.fees_cents,
        )?;
        cmp_int(
            "greeks_attribution",
            step,
            "residual_cents",
            p.residual_cents,
            e.residual_cents,
        )?;
    }
    Ok(())
}

/// Compare two raw `manifest.json` values as **canonical JSON** under the
/// **cross-environment** equality oracle (docs/05 §12.5, the CONSUMER role — the
/// same comparison ChainView does). Excluded from the comparison:
///
/// - `created_utc` — the sole wall-clock field, provenance only (§6);
/// - `config.output_dir` — the operational output path, non-semantic and
///   excluded from the `run_id` (§11) — canonicalised;
/// - **`metrics`** — a **versioned, opaque** object cross-environment (§12.5):
///   the consumer never compares it field-by-field, and some of its fields
///   (`optionstratlib`'s `Positive` — e.g. `volatility`, `downside_deviation`)
///   serialise as **bare `f64`**, which must not be compared for exact equality
///   across environments (`rules/global_rules.md` float discipline). `metrics`
///   byte-stability is instead pinned **same-environment** by the run-twice test
///   (the PRODUCER role, §12.5); its value-correctness is pinned by the v0.1
///   metrics golden (tolerance-based `MetricsSummary`) and the metrics unit tests.
///
/// Everything else — `run_id`, `code_version`, `lockfile_sha256`, `seed`, the
/// semantic `config`, `strategy`, `data_source`, `row_counts` (all strings /
/// integers, no raw `f64`) — is compared exactly.
///
/// # Errors
///
/// An [`OracleDiff`] naming the first divergence (the canonical strings differ).
pub fn compare_manifest_json(
    produced: &serde_json::Value,
    expected: &serde_json::Value,
) -> Result<(), OracleDiff> {
    let p = strip_opaque_metrics(canonical_manifest(produced));
    let e = strip_opaque_metrics(canonical_manifest(expected));
    if p == e {
        return Ok(());
    }
    // Locate the first differing top-level key for a self-locating message.
    let (pobj, eobj) = (as_object(&p), as_object(&e));
    let mut field = "manifest";
    for key in pobj.keys().chain(eobj.keys()) {
        if pobj.get(key) != eobj.get(key) {
            field = leak(key.clone());
            break;
        }
    }
    Err(OracleDiff::new(
        "manifest",
        None,
        field,
        p.get(field)
            .map_or_else(|| "∅".to_string(), ToString::to_string),
        e.get(field)
            .map_or_else(|| "∅".to_string(), ToString::to_string),
    ))
}

/// Normalise a raw `manifest.json` value for the **same-environment** byte
/// comparison (the run-twice test, PRODUCER role, docs/05 §12.5): drop the
/// wall-clock `created_utc` and canonicalise the operational `config.output_dir`
/// (both non-semantic, §11). It **keeps `metrics`**, so two same-environment runs
/// must be byte-identical including the metrics object (`Positive` bare-`f64`
/// fields have identical bits in the same environment). The cross-environment
/// golden comparison ([`compare_manifest_json`]) additionally drops the opaque
/// `metrics`. `serde_json::Map` is `BTreeMap`-backed (sorted keys) with the
/// crate's default features, so the result is order-independent and
/// `==`-comparable.
#[must_use]
pub fn canonical_manifest(value: &serde_json::Value) -> serde_json::Value {
    let mut value = value.clone();
    if let Some(obj) = value.as_object_mut() {
        obj.remove("created_utc");
        if let Some(config) = obj
            .get_mut("config")
            .and_then(serde_json::Value::as_object_mut)
        {
            // Operational output path — non-semantic, excluded from run_id
            // (docs/05 §11); canonicalise so a tempdir write target never
            // reaches the comparison.
            if config.contains_key("output_dir") {
                config.insert(
                    "output_dir".to_string(),
                    serde_json::Value::String("<output_dir>".to_string()),
                );
            }
        }
    }
    value
}

/// Drop the **versioned-opaque** `metrics` object — the cross-environment
/// consumer never compares it field-by-field (docs/05 §12.5), and some of its
/// fields serialise as bare `f64`, which the float-discipline rule forbids
/// comparing for exact equality across environments.
#[must_use]
fn strip_opaque_metrics(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object_mut() {
        obj.remove("metrics");
    }
    value
}

/// Read `dir/manifest.json` as a JSON value (test helper; panics on a failure —
/// a golden fixture that will not read is a hard test failure).
#[must_use]
pub fn read_manifest_json(dir: &std::path::Path) -> serde_json::Value {
    let path = dir.join("manifest.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        panic!("the bundle manifest.json must be readable at {path:?}");
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
        panic!("the bundle manifest.json must be valid JSON at {path:?}");
    };
    value
}

/// A stable-sorted copy of `rows` by `key` — the oracle's per-table
/// normalisation before comparison (mirrors [`sorted_by_step`]).
fn sorted<T: Clone, K: Ord>(rows: &[T], key: impl Fn(&T) -> K) -> Vec<T> {
    let mut out = rows.to_vec();
    out.sort_by_key(|row| key(row));
    out
}

/// A located `usize`-length divergence for a bundle table.
fn len_check(context: &'static str, produced: usize, expected: usize) -> Result<(), OracleDiff> {
    if produced == expected {
        Ok(())
    } else {
        Err(OracleDiff::new(
            context,
            None,
            "len",
            produced.to_string(),
            expected.to_string(),
        ))
    }
}

/// Compare two `u64` id/cents columns exactly.
fn cmp_u64(
    context: &'static str,
    step: Option<u32>,
    field: &'static str,
    a: u64,
    b: u64,
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

/// Compare two string columns exactly.
fn cmp_str(
    context: &'static str,
    step: Option<u32>,
    field: &'static str,
    a: &str,
    b: &str,
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

/// Compare the only nullable column (`exit_reason`) exactly.
fn cmp_opt_str(
    context: &'static str,
    step: Option<u32>,
    field: &'static str,
    a: Option<&str>,
    b: Option<&str>,
) -> Result<(), OracleDiff> {
    if a == b {
        Ok(())
    } else {
        Err(OracleDiff::new(
            context,
            step,
            field,
            a.map_or_else(|| "null".to_string(), ToString::to_string),
            b.map_or_else(|| "null".to_string(), ToString::to_string),
        ))
    }
}

/// Compare two boolean columns exactly.
fn cmp_bool(
    context: &'static str,
    step: Option<u32>,
    field: &'static str,
    a: bool,
    b: bool,
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

/// A `serde_json::Value`'s object map, or an empty map for a non-object (a
/// malformed manifest is caught by `read_bundle`, not here).
fn as_object(value: &serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().cloned().unwrap_or_default()
}

/// Leak a `String` into a `&'static str` for an [`OracleDiff::field`] label —
/// test-only, bounded by the fixed manifest key set.
fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        MetricsSummary, cents_metric, compare_equity_curves, compare_metrics, floats_equal,
        sorted_by_step,
    };
    use ironcondor::EquityPoint;
    use optionstratlib::backtesting::BacktestResult;

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
            sortino_ratio: None,
            volatility: Some(Decimal::ZERO),
            total_return,
            max_drawdown: Decimal::ZERO,
            max_drawdown_ratio: Decimal::ZERO,
            max_drawdown_cents,
            number_of_trades: 0,
            winners: 0,
            losers: 0,
            win_rate: None,
            realized_pnl_cents: 0,
            short_legs_realized_cents: 0,
            long_legs_realized_cents: 0,
            net_premium_cents: 0,
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
    fn test_cents_metric_accepts_integer_rejects_fractional() {
        // A whole-cent Decimal converts exactly; a fractional one is rejected
        // rather than silently truncated (1234.5 must NOT read as 1234).
        let mut result = BacktestResult::default();
        result
            .custom_metrics
            .insert("whole".to_string(), Decimal::from(1_234));
        // 1234.5 (mantissa 12345, scale 1).
        result
            .custom_metrics
            .insert("frac".to_string(), Decimal::new(12_345, 1));

        assert!(
            matches!(cents_metric(&result, "whole"), Ok(1_234)),
            "a whole-cent value converts exactly"
        );
        assert!(
            cents_metric(&result, "frac").is_err(),
            "a fractional value is a loud oracle error, never truncated"
        );
        // A negative whole-cent value also converts exactly.
        result
            .custom_metrics
            .insert("neg".to_string(), Decimal::from(-42));
        assert!(matches!(cents_metric(&result, "neg"), Ok(-42)));
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
