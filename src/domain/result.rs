//! Result-bundle record types (the ones the analytics layer serialises).
//!
//! This module holds the Rust shape of the result-bundle rows
//! ([docs/01 §9](../../../docs/01-domain-model.md#9-result-bundle-record-types));
//! the on-disk Parquet column schema is the wire authority in
//! [docs/05 §7–§10](../../../docs/05-analytics-and-reporting.md#7-fillsparquet).
//!
//! v0.1 lands [`EquityPoint`] — the one row the replay loop's mark-to-market
//! ledger emits per step (issue #14). Issue #31 adds [`GreeksAttributionRow`],
//! the per-step P&L decomposition the analytics layer produces post-run
//! ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
//! The remaining sibling rows (`FillRow`, `PositionRow`) land with the bundle
//! writer (#33/#34) and join this module in place.

use serde::{Deserialize, Serialize};

/// One row of `equity_curve.parquet` — the mark-to-market ledger at one step.
///
/// The engine emits **exactly one** `EquityPoint` per step (including the last)
/// from the single ledger-mutation phase of the replay loop
/// ([docs/02 §3.2 step f](../../../docs/02-engine-architecture.md#32-per-step-for-each-snapshot-s_n-on-the-tape-in-order)).
/// Money columns are integer cents; `drawdown` is the one analytic float — the
/// documented exception to the integer-cents rule.
///
/// # Fields and their invariants
///
/// - `equity_cents == cash_cents + position_value_cents` (the valuation
///   identity, [docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)).
/// - `drawdown = (equity − running_peak) / running_peak`, in `(−∞, 0]`: `0` at
///   a fresh peak, `−1` at zero equity, below `−1` when equity goes negative.
///   It is **reported as-is, never clamped** — clamping would hide a wipe-out
///   ([docs/01 §9 drawdown rule](../../../docs/01-domain-model.md#9-result-bundle-record-types)).
///
/// `Eq` / `Ord` / `Hash` are **not** derived because `drawdown` is `f64`;
/// determinism tests compare two same-environment runs, where the same
/// computation yields the same bits, via `PartialEq`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EquityPoint {
    /// The 0-based step ordinal this point belongs to.
    pub step: u32,
    /// The step's snapshot timestamp (nanoseconds since the Unix epoch, UTC).
    pub ts_ns: i64,
    /// Cash in integer cents — moves only through fills and fees, never
    /// through revaluation ([docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)).
    pub cash_cents: i64,
    /// Mark-to-market value of the open legs in integer cents:
    /// `Σ (mark × quantity × contract_multiplier × side_sign)`.
    pub position_value_cents: i64,
    /// `cash_cents + position_value_cents`.
    pub equity_cents: i64,
    /// The running drawdown ratio against the equity peak (`≤ 0`, never
    /// clamped). The single analytic float on the row.
    pub drawdown: f64,
}

impl EquityPoint {
    /// Assemble one equity point from its already-computed cents figures and
    /// drawdown ratio.
    ///
    /// A pure constructor — the ledger computes the values and calls this so
    /// the field order is written in exactly one place.
    #[must_use]
    pub const fn new(
        step: u32,
        ts_ns: i64,
        cash_cents: i64,
        position_value_cents: i64,
        equity_cents: i64,
        drawdown: f64,
    ) -> Self {
        Self {
            step,
            ts_ns,
            cash_cents,
            position_value_cents,
            equity_cents,
            drawdown,
        }
    }
}

/// One row of `greeks_attribution.parquet` — the per-step P&L decomposition.
///
/// The analytics layer emits **exactly one** `GreeksAttributionRow` per step
/// (issue #31), decomposing the ledger's mark-to-market `step_pnl` into the
/// position's Greek contributions plus the execution frictions, closed by an
/// **exact** integer-cents residual
/// ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
/// Every column is integer cents; there is **no** float on this row (the
/// Greeks and market deltas are analytic `Decimal`s only *during* the
/// decomposition — each term is rounded once into cents before it reaches a
/// field, [docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
///
/// # The reconciliation identity (exact, always holds)
///
/// ```text
/// step_pnl = theta_pnl + delta_pnl + vega_pnl + spread_capture − fees + residual
/// ```
///
/// By the definition of `residual_cents = step_pnl − (theta + delta + vega +
/// spread_capture − fees)` — computed from the **already-rounded** terms — the
/// identity holds **exactly in integer cents for every step, by construction**
/// ([docs/05 §3.1](../../../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
/// A large `|residual|` is an *advisory* model-quality signal (a `tracing::warn`),
/// never a run failure and never silently folded into another term.
///
/// # Fields and their signs ([docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table))
///
/// - `theta_pnl_cents` / `delta_pnl_cents` / `vega_pnl_cents` — signed Greek
///   contributions, `Σ greek_i · Δmarket · quantity × contract_multiplier ×
///   side_sign` over **beginning-of-step** legs (all `0` at step 0, no prior
///   snapshot).
/// - `spread_capture_cents` — signed, `= −Σ Fill.slippage`; **positive =
///   favourable** execution.
/// - `fees_cents` — **always ≥ 0**; **subtracted** in the identity.
/// - `residual_cents` — signed **exact** remainder that closes the identity.
///
/// `Eq` / `Ord` / `Hash` are derivable (no float), but only `PartialEq` is
/// derived to mirror [`EquityPoint`] and keep the record family uniform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GreeksAttributionRow {
    /// The 0-based step ordinal this decomposition belongs to.
    pub step: u32,
    /// The step's snapshot timestamp (nanoseconds since the Unix epoch, UTC).
    pub ts_ns: i64,
    /// The theta contribution in integer cents: `Σ theta_i · Δt_days ×
    /// quantity × contract_multiplier × side_sign` (daily theta × elapsed
    /// days). `0` at step 0.
    pub theta_pnl_cents: i64,
    /// The delta contribution in integer cents: `Σ delta_i · ΔS × quantity ×
    /// contract_multiplier × side_sign` (per-$1 delta × underlying move). `0`
    /// at step 0.
    pub delta_pnl_cents: i64,
    /// The vega contribution in integer cents: `Σ vega_i · ΔIV_pp × quantity ×
    /// contract_multiplier × side_sign` (per-percentage-point vega × IV move in
    /// percentage points). `0` at step 0.
    pub vega_pnl_cents: i64,
    /// The step's spread capture in integer cents, `= −Σ Fill.slippage`;
    /// signed, **positive = favourable** ([docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
    pub spread_capture_cents: i64,
    /// The step's fees in integer cents, `Σ Fill.fees`; **always ≥ 0** and
    /// **subtracted** in the identity.
    pub fees_cents: i64,
    /// The **exact** un-attributed remainder in integer cents: `step_pnl −
    /// (theta + delta + vega + spread_capture − fees)`. Absorbs gamma /
    /// second-order curvature, cross-Greek terms, and the per-term rounding;
    /// it is reported, never hidden ([docs/05 §3.1](../../../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
    pub residual_cents: i64,
}

impl GreeksAttributionRow {
    /// Assemble one attribution row from its already-rounded integer-cents
    /// terms.
    ///
    /// A pure constructor — the analytics decomposition computes and rounds the
    /// terms and calls this so the field order is written in exactly one place.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per wire column of the fixed GreeksAttributionRow schema (docs/05 §10); the constructor centralises the field order in one place"
    )]
    pub const fn new(
        step: u32,
        ts_ns: i64,
        theta_pnl_cents: i64,
        delta_pnl_cents: i64,
        vega_pnl_cents: i64,
        spread_capture_cents: i64,
        fees_cents: i64,
        residual_cents: i64,
    ) -> Self {
        Self {
            step,
            ts_ns,
            theta_pnl_cents,
            delta_pnl_cents,
            vega_pnl_cents,
            spread_capture_cents,
            fees_cents,
            residual_cents,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EquityPoint, GreeksAttributionRow};

    #[test]
    fn test_greeks_attribution_row_serialises_money_as_bare_integers() {
        let row = GreeksAttributionRow::new(0, 1_000, 0, 0, 0, -60, 520, 2_060);
        let Ok(json) = serde_json::to_value(&row) else {
            panic!("attribution row serialises");
        };
        for (field, expected) in [
            ("theta_pnl_cents", 0_i64),
            ("delta_pnl_cents", 0),
            ("vega_pnl_cents", 0),
            ("spread_capture_cents", -60),
            ("fees_cents", 520),
            ("residual_cents", 2_060),
        ] {
            assert!(
                matches!(json.get(field).and_then(serde_json::Value::as_i64), Some(v) if v == expected),
                "{field} must serialise as the bare integer {expected}"
            );
        }
    }

    #[test]
    fn test_greeks_attribution_row_round_trips_through_json() {
        let row = GreeksAttributionRow::new(3, 42, -120, 340, -55, 12, 130, 7);
        let Ok(json) = serde_json::to_string(&row) else {
            panic!("serialises");
        };
        let back: Result<GreeksAttributionRow, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(r) if r == row));
    }

    #[test]
    fn test_equity_point_serialises_money_as_bare_integers() {
        let point = EquityPoint::new(3, 1_000, 9_998_000, -1_500, 9_996_500, -0.000_15);
        let Ok(json) = serde_json::to_value(&point) else {
            panic!("equity point serialises");
        };
        assert!(matches!(
            json.get("cash_cents").and_then(|v| v.as_i64()),
            Some(9_998_000)
        ));
        assert!(matches!(
            json.get("position_value_cents").and_then(|v| v.as_i64()),
            Some(-1_500)
        ));
        assert!(matches!(
            json.get("equity_cents").and_then(|v| v.as_i64()),
            Some(9_996_500)
        ));
        assert!(json.get("drawdown").and_then(|v| v.as_f64()).is_some());
    }

    #[test]
    fn test_equity_point_round_trips_through_json() {
        let point = EquityPoint::new(0, 42, 10_000_000, 0, 10_000_000, 0.0);
        let Ok(json) = serde_json::to_string(&point) else {
            panic!("serialises");
        };
        let back: Result<EquityPoint, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(p) if p == point));
    }
}
