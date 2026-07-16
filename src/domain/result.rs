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
//! Issue #33 adds the two remaining sibling rows — [`FillRow`] (one executed
//! fill) and [`PositionRow`] (a leg's per-step state) — so the four bundle row
//! types now live together in this one module, next to the [`crate::bundle`]
//! [`Manifest`](crate::bundle::Manifest) that references them
//! ([docs/01 §9](../../../docs/01-domain-model.md#9-result-bundle-record-types)).
//!
//! # Placement (domain, not bundle)
//!
//! All four row types live in `domain` — the lowest layer — even though the
//! bundle writer ([`crate::bundle`], #34) is what serialises them to Parquet.
//! `domain` cannot import `bundle`, so keeping the pure serialisable row records
//! here lets `bundle` depend on `domain` (never the reverse) and keeps the four
//! wire rows grouped exactly as [docs/01 §9](../../../docs/01-domain-model.md#9-result-bundle-record-types)
//! specifies them.
//!
//! # Money is integer cents, never a newtype on the wire
//!
//! The wire rows carry **bare** integer-cents scalars (`u64` / `i64`), not the
//! [`crate::domain::Cents`] / [`crate::domain::PriceCents`] newtypes: the on-disk
//! Parquet column is a physical `INT32` / `INT64`, and matching the wire
//! authority in [docs/05 §7–§10](../../../docs/05-analytics-and-reporting.md#7-fillsparquet)
//! field-for-field means the Rust shape uses the same bare scalars the sibling
//! [`EquityPoint`] / [`GreeksAttributionRow`] already do. There is **no `f64`
//! money column** anywhere; `drawdown` (on [`EquityPoint`]) is the only float
//! column in the whole bundle.

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

/// One row of `fills.parquet` — one executed fill
/// ([docs/05 §7](../../../docs/05-analytics-and-reporting.md#7-fillsparquet)).
///
/// This is the wire projection of a [`crate::domain::Fill`] **plus** the
/// engine-assigned lifecycle ids ([docs/01 §9](../../../docs/01-domain-model.md#9-result-bundle-record-types)),
/// so a consumer reconstructs the `trade → position → order → fill` tree from
/// the bundle alone. One `OrderIntent` becomes one order (`order_id`); in
/// realistic mode a marketable order can walk several price levels, and **each
/// level is a separate `FillRow`** with its own `price_cents` and matched
/// `quantity` and an incrementing `fill_seq` (0, 1, 2, …), so
/// **`(step, order_id, fill_seq)` is the unique key** the table is sorted by.
/// Naive mode always fills an order in one shot (`fill_seq = 0`).
///
/// Every column is required (no `Option`): the fills table has **no nullable
/// column** ([docs/05 §12.2](../../../docs/05-analytics-and-reporting.md#122-parquet-table-columns-types-nullability)).
/// Money columns are integer cents; there is no float column on this row.
///
/// # Numeric-id types ([docs/05 §12.4](../../../docs/05-analytics-and-reporting.md#124-identifier-grammars-producer-defined-delimiter-safe))
///
/// `trade_id` / `position_id` / `order_id` are `u64`; `fill_seq` / `step` are
/// `u32`. On the wire they are physical signed `INT32` / `INT64`, crossed with
/// checked `try_from` at read-back (#35), never an `as` cast — that mapping is
/// the writer's / reader's job, not this type's.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillRow {
    /// The 0-based step ordinal the fill executed in.
    pub step: u32,
    /// The step's snapshot timestamp (nanoseconds since the Unix epoch, UTC).
    pub ts_ns: i64,
    /// The bundle's [`crate::bundle::RunId`] as a string — stable across every
    /// row of one bundle.
    pub strategy_run_id: String,
    /// The multi-leg trade this fill belongs to (the legs a strategy opened
    /// together in one step share one `trade_id`).
    pub trade_id: u64,
    /// The single leg/position this fill opens or closes.
    pub position_id: u64,
    /// Unique per submitted order (a seeded counter).
    pub order_id: u64,
    /// The 0-based fill index **within one order** (the price levels a
    /// marketable realistic order walked); always `0` in naive mode.
    pub fill_seq: u32,
    /// The canonical uppercase ticker.
    pub underlying: String,
    /// The resolved absolute expiration instant (nanoseconds since epoch, UTC,
    /// [docs/01 §5.1](../../../docs/01-domain-model.md#51-expiration-resolves-to-one-absolute-instant)).
    pub expiration_ns: i64,
    /// The canonical versioned contract identifier — the join key
    /// ([docs/01 §5.2](../../../docs/01-domain-model.md#52-the-canonical-contract-identifier)).
    pub contract_id: String,
    /// The strike in integer cents.
    pub strike_cents: u64,
    /// The option style, `"call"` | `"put"`.
    pub style: String,
    /// The fill side, `"long"` | `"short"`.
    pub side: String,
    /// The filled contract count (`> 0`) — may be `<` the intent in realistic
    /// mode.
    pub quantity: u32,
    /// The executed premium per contract, integer cents.
    pub price_cents: u64,
    /// Fees for this fill, integer cents, **always ≥ 0** and subtracted in the
    /// P&L identity ([docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
    pub fees_cents: i64,
    /// Signed slippage vs the decision-time mid; **positive = adverse**
    /// ([docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
    pub slippage_cents: i64,
    /// Which fill model produced this fill, `"naive"` | `"realistic"`.
    pub mode: String,
}

/// One row of `positions.parquet` — a leg's state at a step
/// ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet)).
///
/// One row per leg for **every step the leg is open**, plus **one terminal row**
/// at the step the leg is closed (carrying the non-null `exit_reason`). Ordered
/// by `(step, position_id)` — at most one row per leg per step. A leg still open
/// when the feed exhausts has no terminal row and instead carries
/// `open_at_end = true` on its last open row
/// ([docs/02 §3.3](../../../docs/02-engine-architecture.md#33-termination-feed-exhausted)).
///
/// **`exit_reason` is the ONLY `Option` (nullable) column in the whole bundle**
/// ([docs/05 §12.2](../../../docs/05-analytics-and-reporting.md#122-parquet-table-columns-types-nullability)):
/// it is `None` while the leg is open and carries the `ExitReason` string once
/// the leg is closed. Every other column on this row — and in every other table
/// — is required. Money columns are integer cents; there is no float column on
/// this row.
///
/// # Terminal-row values (the closing step)
///
/// The terminal row is observed at the **same beginning-of-step endpoint** as
/// every other row (immediately before the closing fill is applied): `quantity`
/// = the contracts closed (`> 0`), `mark_cents` = the closing step's snapshot
/// mid, `unrealized_cents` = the leg's P&L at that mid. The closing fill's
/// execution price / fees live in [`FillRow`] and the ledger, not here
/// ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionRow {
    /// The 0-based step ordinal this observation belongs to.
    pub step: u32,
    /// The step's snapshot timestamp (nanoseconds since the Unix epoch, UTC).
    pub ts_ns: i64,
    /// The stable leg id — the `positions` sort key with `step`.
    pub position_id: u64,
    /// The multi-leg trade this leg belongs to.
    pub trade_id: u64,
    /// The canonical versioned contract identifier
    /// ([docs/01 §5.2](../../../docs/01-domain-model.md#52-the-canonical-contract-identifier)).
    pub contract_id: String,
    /// The leg side, `"long"` | `"short"`.
    pub side: String,
    /// The open contract count (`> 0`); on the terminal row, the contracts
    /// closed at this step.
    pub quantity: u32,
    /// The average entry premium, integer cents.
    pub avg_price_cents: u64,
    /// The current mark (snapshot mid); the last-known mid carried forward if
    /// `stale_mark`.
    pub mark_cents: u64,
    /// The leg's unrealised P&L at the mark, signed integer cents.
    pub unrealized_cents: i64,
    /// `true` if this contract had no quote this step and its last-known mark
    /// was carried forward ([docs/01 §6](../../../docs/01-domain-model.md#6-market-data)).
    pub stale_mark: bool,
    /// `None` while the leg is open; the `ExitReason` string once it is closed.
    /// The **only** nullable column in the bundle
    /// ([docs/05 §12.2](../../../docs/05-analytics-and-reporting.md#122-parquet-table-columns-types-nullability)).
    pub exit_reason: Option<String>,
    /// `true` if the leg was still open when the feed exhausted
    /// ([docs/02 §3.3](../../../docs/02-engine-architecture.md#33-termination-feed-exhausted)).
    pub open_at_end: bool,
}

#[cfg(test)]
mod tests {
    use super::{EquityPoint, FillRow, GreeksAttributionRow, PositionRow};

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

    /// A representative fill row for the field-set and round-trip tests.
    fn sample_fill_row() -> FillRow {
        FillRow {
            step: 2,
            ts_ns: 1_750_291_200_000_000_000,
            strategy_run_id: "deadbeef".to_string(),
            trade_id: 7,
            position_id: 11,
            order_id: 42,
            fill_seq: 1,
            underlying: "SPX".to_string(),
            expiration_ns: 1_750_291_200_000_000_000,
            contract_id: "v1:SPX:1750291200000000000:510000:C".to_string(),
            strike_cents: 510_000,
            style: "call".to_string(),
            side: "short".to_string(),
            quantity: 3,
            price_cents: 1_970,
            fees_cents: 65,
            slippage_cents: -2,
            mode: "realistic".to_string(),
        }
    }

    /// A representative open position row (`exit_reason == None`).
    fn sample_position_row(exit_reason: Option<String>) -> PositionRow {
        PositionRow {
            step: 4,
            ts_ns: 1_750_291_200_000_000_000,
            position_id: 11,
            trade_id: 7,
            contract_id: "v1:SPX:1750291200000000000:510000:C".to_string(),
            side: "short".to_string(),
            quantity: 3,
            avg_price_cents: 2_000,
            mark_cents: 1_950,
            unrealized_cents: 150,
            stale_mark: false,
            exit_reason,
            open_at_end: false,
        }
    }

    #[test]
    fn test_fill_row_has_the_docs_09_field_set() {
        let Ok(json) = serde_json::to_value(sample_fill_row()) else {
            panic!("fill row serialises");
        };
        let Some(obj) = json.as_object() else {
            panic!("fill row is a JSON object");
        };
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut expected = [
            "step",
            "ts_ns",
            "strategy_run_id",
            "trade_id",
            "position_id",
            "order_id",
            "fill_seq",
            "underlying",
            "expiration_ns",
            "contract_id",
            "strike_cents",
            "style",
            "side",
            "quantity",
            "price_cents",
            "fees_cents",
            "slippage_cents",
            "mode",
        ];
        expected.sort_unstable();
        assert_eq!(
            keys, expected,
            "fill row must carry exactly the docs/01 §9 columns"
        );
        // Money columns serialise as bare integers, never floats.
        assert!(matches!(
            obj.get("price_cents").and_then(serde_json::Value::as_u64),
            Some(1_970)
        ));
        assert!(matches!(
            obj.get("fees_cents").and_then(serde_json::Value::as_i64),
            Some(65)
        ));
        assert!(matches!(
            obj.get("slippage_cents")
                .and_then(serde_json::Value::as_i64),
            Some(-2)
        ));
    }

    #[test]
    fn test_fill_row_round_trips_through_json() {
        let row = sample_fill_row();
        let Ok(json) = serde_json::to_string(&row) else {
            panic!("serialises");
        };
        let back: Result<FillRow, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(r) if r == row));
    }

    #[test]
    fn test_position_row_has_the_docs_09_field_set() {
        let Ok(json) = serde_json::to_value(sample_position_row(None)) else {
            panic!("position row serialises");
        };
        let Some(obj) = json.as_object() else {
            panic!("position row is a JSON object");
        };
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut expected = [
            "step",
            "ts_ns",
            "position_id",
            "trade_id",
            "contract_id",
            "side",
            "quantity",
            "avg_price_cents",
            "mark_cents",
            "unrealized_cents",
            "stale_mark",
            "exit_reason",
            "open_at_end",
        ];
        expected.sort_unstable();
        assert_eq!(
            keys, expected,
            "position row must carry exactly the docs/01 §9 columns"
        );
    }

    #[test]
    fn test_position_row_exit_reason_is_the_only_nullable_column() {
        // Open leg: exit_reason serialises as JSON null; every other field is a
        // concrete value.
        let Ok(json) = serde_json::to_value(sample_position_row(None)) else {
            panic!("open position row serialises");
        };
        let Some(obj) = json.as_object() else {
            panic!("object");
        };
        assert!(
            matches!(obj.get("exit_reason"), Some(serde_json::Value::Null)),
            "an open leg's exit_reason is null"
        );
        for (key, value) in obj {
            if key == "exit_reason" {
                continue;
            }
            assert!(
                !value.is_null(),
                "no column other than exit_reason may be null ({key})"
            );
        }
        // Closed leg: exit_reason carries the reason string.
        let Ok(json) = serde_json::to_value(sample_position_row(Some("expiration".to_string())))
        else {
            panic!("closed position row serialises");
        };
        assert!(matches!(
            json.get("exit_reason").and_then(serde_json::Value::as_str),
            Some("expiration")
        ));
    }

    #[test]
    fn test_position_row_round_trips_through_json_open_and_closed() {
        for reason in [None, Some("stop_loss".to_string())] {
            let row = sample_position_row(reason);
            let Ok(json) = serde_json::to_string(&row) else {
                panic!("serialises");
            };
            let back: Result<PositionRow, _> = serde_json::from_str(&json);
            assert!(matches!(back, Ok(r) if r == row));
        }
    }
}
