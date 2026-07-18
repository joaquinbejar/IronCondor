//! Result-bundle record types (the ones the analytics layer serialises).
//!
//! This module holds the Rust shape of the result-bundle rows
//! ([docs/01 §9](../../../docs/01-domain-model.md#9-result-bundle-record-types));
//! the on-disk Parquet column schema is the wire authority in
//! [docs/05 §7–§10](../../../docs/05-analytics-and-reporting.md#7-fillsparquet).
//!
//! v0.1 lands [`EquityPoint`] — the one row the replay loop's mark-to-market
//! ledger emits per step (issue #14). The sibling rows (`FillRow`,
//! `PositionRow`, `GreeksAttributionRow`) land with the analytics/bundle work
//! (#16/#34) and join this module in place.

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

#[cfg(test)]
mod tests {
    use super::EquityPoint;

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
