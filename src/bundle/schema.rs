//! The frozen `ironcondor.bundle.v1` wire contract — schema tag, run identity,
//! manifest, per-table row counts, and the pinned per-table sort keys.
//!
//! This module holds the **bundle-level** types the writer (#34) serialises and
//! the reader (#35) validates. The four Parquet **row** types live one layer
//! down in [`crate::domain::result`] ([`crate::domain::FillRow`],
//! [`crate::domain::EquityPoint`], [`crate::domain::PositionRow`],
//! [`crate::domain::GreeksAttributionRow`]) so `bundle` depends on `domain`,
//! never the reverse; this module owns everything *around* those rows.
//!
//! # The contract, precisely
//!
//! A result bundle is a directory of `manifest.json` plus the four Parquet
//! tables ([docs/05 §5](../../../docs/05-analytics-and-reporting.md#5-the-ironcondor-result-bundle)).
//! The [`Manifest`] record is the **single serialization source** for
//! `manifest.json` — the [docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson)
//! field table is its wire projection and no parallel manifest type exists. The
//! schema tag [`BUNDLE_SCHEMA`] gates major incompatibility; its `v1:` lineage is
//! coordinated with [ChainView](https://github.com/joaquinbejar/ChainView).
//!
//! # Required vs optional (version negotiation, [docs/SEMVER.md](../../../docs/SEMVER.md))
//!
//! **Every field of `ironcondor.bundle.v1` is required.** Within the same
//! `schema` tag, a later minor release may add **only optional fields with a
//! documented reader default**, so older readers stay compatible; adding a
//! *required* field is a `schema`-tag bump and a major release. `metrics` and
//! `row_counts` are **required** ([docs/01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
//!
//! # Currency (fixed by the tag, [docs/05 §12.3](../../../docs/05-analytics-and-reporting.md#123-currency))
//!
//! `ironcondor.bundle.v1` is scoped to **USD, minor-unit exponent 2**. This is
//! fixed by the schema tag itself, so there is **no `currency` field** in the
//! manifest — a reader formats every integer-cents amount as USD by contract. A
//! future multi-currency bundle is a new schema tag, not a v1 field.
//!
//! # Determinism
//!
//! Nothing here reads a wall clock or iterates a `HashMap`: [`RowCounts`] is a
//! fixed-field struct (not a map), and [`RunId::derive`] hashes a fixed
//! field-ordered canonical-JSON preimage. `Manifest::created_utc` is the one
//! wall-clock provenance field — it is set by the writer (#34), is **excluded**
//! from the `run_id` hash, and is stripped before the byte comparison
//! ([docs/05 §11](../../../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism)).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::analytics::metrics::Metrics;
use crate::config::{BacktestConfig, FeeSchedule, LiquidityProfile, ResourceLimits, SlippageModel};
use crate::data::DataSourceSpec;
use crate::domain::{
    EquityPoint, ExecutionMode, FillRow, GreeksAttributionRow, PositionRow, StrategySpec,
};
use crate::error::BacktestError;

/// The frozen result-bundle schema tag written to `manifest.schema` and checked
/// by every consumer before parsing
/// ([docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson)).
///
/// A change to this tag (`v1` → `v2`) is a major SemVer event coordinated with
/// ChainView ([docs/SEMVER.md](../../../docs/SEMVER.md)).
pub const BUNDLE_SCHEMA: &str = "ironcondor.bundle.v1";

// --- pinned per-table sort keys ---------------------------------------------
//
// The deterministic order each table is written in — also the comparison
// oracle's sort ([docs/05 §7–§10](../../../docs/05-analytics-and-reporting.md#7-fillsparquet),
// [docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

/// `fills.parquet` sort columns — `(step, order_id, fill_seq)`, a **unique** key.
pub const FILLS_SORT_COLUMNS: &[&str] = &["step", "order_id", "fill_seq"];
/// `positions.parquet` sort columns — `(step, position_id)`, ≤ 1 row per step.
pub const POSITIONS_SORT_COLUMNS: &[&str] = &["step", "position_id"];
/// `equity_curve.parquet` sort column — `step`, one row per step.
pub const EQUITY_CURVE_SORT_COLUMNS: &[&str] = &["step"];
/// `greeks_attribution.parquet` sort column — `step`, one row per step.
pub const GREEKS_ATTRIBUTION_SORT_COLUMNS: &[&str] = &["step"];

/// The `fills.parquet` sort key of one row — `(step, order_id, fill_seq)`, the
/// **unique** key ([docs/05 §7](../../../docs/05-analytics-and-reporting.md#7-fillsparquet)).
/// The single source the writer (#34) sorts by, so the pinned order lives in
/// exactly one place.
#[must_use]
pub const fn fill_sort_key(row: &FillRow) -> (u32, u64, u32) {
    (row.step, row.order_id, row.fill_seq)
}

/// The `positions.parquet` sort key of one row — `(step, position_id)`
/// ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet)).
#[must_use]
pub const fn position_sort_key(row: &PositionRow) -> (u32, u64) {
    (row.step, row.position_id)
}

/// The `equity_curve.parquet` sort key of one row — `step`
/// ([docs/05 §8](../../../docs/05-analytics-and-reporting.md#8-equity_curveparquet)).
#[must_use]
pub const fn equity_sort_key(row: &EquityPoint) -> u32 {
    row.step
}

/// The `greeks_attribution.parquet` sort key of one row — `step`
/// ([docs/05 §10](../../../docs/05-analytics-and-reporting.md#10-greeks_attributionparquet)).
#[must_use]
pub const fn greeks_sort_key(row: &GreeksAttributionRow) -> u32 {
    row.step
}

/// The deterministic run identity — a lower-hex `sha256` string
/// ([docs/01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
///
/// Serialises as its **bare** inner string (`#[serde(transparent)]`), so a
/// `RunId` is `"9f2c…"` on the wire, never `{"RunId":"9f2c…"}`. It is also the
/// directory name of the bundle and the `strategy_run_id` stamped on every
/// [`FillRow`].
///
/// See [`RunId::derive`] for the exact hashed preimage.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    /// Derive the run identity from the **complete reproducibility tuple**
    /// ([docs/01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)):
    ///
    /// ```text
    /// run_id = sha256_hex( canonical(preimage) )
    /// preimage = {
    ///     seed,                    // the engine RNG seed
    ///     semantic_config,         // BacktestConfig MINUS overwrite + output_dir
    ///     strategy,                // StrategySpec (kind + parameters)
    ///     tape_identity,           // file sha256 | materialised-tape sha256
    ///     build_identity = { code_version, lockfile_sha256 },
    /// }
    /// ```
    ///
    /// **The preimage composition is a stable contract.** The fields are hashed
    /// in exactly this order via a fixed field-ordered canonical-JSON encoding
    /// (structs serialise in declaration order; the config carries no `HashMap`),
    /// so the same tuple yields a byte-identical preimage — and therefore the
    /// same `run_id` — across runs and environments.
    ///
    /// **`overwrite` and the output path are EXCLUDED** — they are operational
    /// output controls, not run behaviour, so two configs differing only there
    /// hash identically (enabling `overwrite` authorises replacing the *same*
    /// `<run_id>` directory, it never re-points the run). Every other semantic
    /// input — `seed`, execution `mode`, capital, fees, slippage, liquidity
    /// profile, limits, `data_source`, `strategy`, `tape_identity`, and the
    /// build identity — is hashed, so any behavioural difference gives a distinct
    /// `run_id`.
    ///
    /// `seed` is passed explicitly (the caller passes `config.seed`); it is also
    /// carried inside the semantic config, which is harmless redundancy — the
    /// hash needs distinctness and determinism, not minimality.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Bundle`] if the preimage fails to serialise
    /// (an infallible path in practice, surfaced as a typed error rather than a
    /// panic).
    #[must_use = "the derived run id must be used"]
    pub fn derive(
        seed: u64,
        config: &BacktestConfig,
        strategy: &StrategySpec,
        tape_identity: &str,
        code_version: &str,
        lockfile_sha256: &str,
    ) -> Result<Self, BacktestError> {
        let preimage = RunIdPreimage {
            seed,
            semantic_config: SemanticConfig {
                data_source: &config.data_source,
                mode: config.mode,
                seed: config.seed,
                initial_capital: config.initial_capital,
                fees: &config.fees,
                slippage: &config.slippage,
                marketable_cap_ticks: config.marketable_cap_ticks,
                liquidity_profile: &config.liquidity_profile,
                limits: &config.limits,
            },
            strategy,
            tape_identity,
            build_identity: BuildIdentity {
                code_version,
                lockfile_sha256,
            },
        };
        let bytes = serde_json::to_vec(&preimage).map_err(|error| {
            BacktestError::Bundle(format!("run_id preimage serialisation failed: {error}"))
        })?;
        Ok(Self(to_hex(&Sha256::digest(&bytes))))
    }

    /// Wrap an already-computed run-id string (e.g. one read back from a
    /// manifest). Prefer [`RunId::derive`] to mint a fresh identity.
    #[must_use]
    pub const fn from_hex(hex: String) -> Self {
        Self(hex)
    }

    /// The bare run-id string — the bundle directory name and the value stamped
    /// on every [`FillRow::strategy_run_id`].
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The semantic view of [`BacktestConfig`] the `run_id` hashes — every field
/// **except** the operational output controls (`overwrite`, `output_dir`).
///
/// Borrowing view so no clone happens; the field order here is part of the
/// stable preimage contract.
#[derive(Serialize)]
struct SemanticConfig<'a> {
    data_source: &'a DataSourceSpec,
    mode: ExecutionMode,
    seed: u64,
    initial_capital: u64,
    fees: &'a FeeSchedule,
    slippage: &'a SlippageModel,
    marketable_cap_ticks: u32,
    liquidity_profile: &'a LiquidityProfile,
    limits: &'a ResourceLimits,
}

/// The build identity hashed into the `run_id`: crate version + lockfile hash.
#[derive(Serialize)]
struct BuildIdentity<'a> {
    code_version: &'a str,
    lockfile_sha256: &'a str,
}

/// The full, fixed-order `run_id` hash preimage
/// ([docs/01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
#[derive(Serialize)]
struct RunIdPreimage<'a> {
    seed: u64,
    semantic_config: SemanticConfig<'a>,
    strategy: &'a StrategySpec,
    tape_identity: &'a str,
    build_identity: BuildIdentity<'a>,
}

/// The per-table row count recorded in `manifest.row_counts` — a cheap reader
/// integrity check cross-validated against the decoded table lengths
/// ([docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson),
/// [docs/05 §12.5](../../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)).
///
/// A **fixed-field struct**, not a `HashMap`, so it serialises deterministically
/// as the exact `{ "fills", "equity_curve", "positions", "greeks_attribution" }`
/// → `u64` object the consumer validates. Every field is **required** at
/// `ironcondor.bundle.v1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowCounts {
    /// Rows in `fills.parquet`.
    pub fills: u64,
    /// Rows in `equity_curve.parquet`.
    pub equity_curve: u64,
    /// Rows in `positions.parquet`.
    pub positions: u64,
    /// Rows in `greeks_attribution.parquet`.
    pub greeks_attribution: u64,
}

impl RowCounts {
    /// Assemble the per-table row counts.
    #[must_use]
    pub const fn new(
        fills: u64,
        equity_curve: u64,
        positions: u64,
        greeks_attribution: u64,
    ) -> Self {
        Self {
            fills,
            equity_curve,
            positions,
            greeks_attribution,
        }
    }
}

/// The bundle manifest — the **single serialization source** for
/// `manifest.json` ([docs/01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest),
/// [docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson)).
///
/// It carries **exactly** the [docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson)
/// fields — no more, no fewer, and **no `currency` field** (fixed by the tag,
/// [docs/05 §12.3](../../../docs/05-analytics-and-reporting.md#123-currency)).
/// There is no parallel manifest type: `metrics` and `row_counts` are required
/// fields living on this record. The writer (#34) sets [`Manifest::created_utc`]
/// at write time; the reader (#35) validates [`Manifest::schema`] before parsing.
///
/// `PartialEq` is **not** derived: [`Metrics`] embeds the upstream
/// `optionstratlib::backtesting` summary structs, which do not implement
/// `PartialEq` — the same reason [`Metrics`] itself omits it. Manifest equality
/// is defined by the canonical-JSON comparison oracle (with `created_utc`
/// stripped), not by `==` ([docs/05 §12.5](../../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// The wire tag, [`BUNDLE_SCHEMA`] (`"ironcondor.bundle.v1"`).
    pub schema: String,
    /// The deterministic run identity ([`RunId::derive`]).
    pub run_id: RunId,
    /// RFC 3339 wall-clock — **provenance only**, never read back by the engine
    /// and excluded from the `run_id` hash and the byte comparison.
    pub created_utc: String,
    /// `ironcondor` crate version (`CARGO_PKG_VERSION`) — build identity (hashed
    /// into `run_id`). Deliberately **not** a per-commit git sha: a sha would
    /// change the `run_id` (the bundle directory name) every commit and make a
    /// frozen golden impossible. Build identity is `code_version +
    /// lockfile_sha256`, both stable across commits; git provenance, if ever
    /// wanted, would be a manifest-only field excluded from `run_id` (and from
    /// the byte comparison, like `created_utc`).
    pub code_version: String,
    /// sha256 of `Cargo.lock` at build — build identity (hashed into `run_id`).
    pub lockfile_sha256: String,
    /// The engine RNG seed.
    pub seed: u64,
    /// The full run config, embedded verbatim (money fields integer cents).
    pub config: BacktestConfig,
    /// The strategy kind + parameters.
    pub strategy: StrategySpec,
    /// The data-source provenance + re-read locator (also listed for
    /// readability; its identity is hashed into `run_id`).
    pub data_source: DataSourceSpec,
    /// The populated `optionstratlib::backtesting` summary structs — **required**.
    pub metrics: Metrics,
    /// The per-table row counts — a reader integrity check — **required**.
    pub row_counts: RowCounts,
}

/// Lower-hex encode a byte slice — the `run_id` string form. Infallible (writes
/// into a pre-sized `String`), so no `.unwrap()` on the discarded `fmt::Result`.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        BUNDLE_SCHEMA, EQUITY_CURVE_SORT_COLUMNS, FILLS_SORT_COLUMNS,
        GREEKS_ATTRIBUTION_SORT_COLUMNS, Manifest, POSITIONS_SORT_COLUMNS, RowCounts, RunId,
    };
    use crate::analytics::metrics::Metrics;
    use crate::config::{BacktestConfig, FeeSchedule, SlippageModel};
    use crate::data::DataSourceSpec;
    use crate::domain::{
        ExecutionMode, IronCondorSpec, PriceCents, Quantity, StrategySpec, Underlying,
    };
    use optionstratlib::ExpirationDate;
    use optionstratlib::backtesting::BacktestResult;

    const CODE_VERSION: &str = "0.3.0";
    const LOCKFILE_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const TAPE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn base_config() -> BacktestConfig {
        BacktestConfig {
            data_source: DataSourceSpec::Parquet {
                path: "chains/spx.parquet".to_string(),
                sha256: TAPE_SHA.to_string(),
            },
            mode: ExecutionMode::Naive,
            seed: 42,
            initial_capital: 10_000_000,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            slippage: SlippageModel::None,
            marketable_cap_ticks: 10,
            liquidity_profile: crate::config::LiquidityProfile::default(),
            limits: crate::config::ResourceLimits::default(),
            output_dir: "runs/out".into(),
            overwrite: false,
        }
    }

    fn strategy() -> StrategySpec {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        let Ok(quantity) = Quantity::new(1) else {
            panic!("1 is valid");
        };
        StrategySpec::IronCondor(IronCondorSpec {
            underlying,
            underlying_price: PriceCents::new(500_000),
            short_call_strike: PriceCents::new(510_000),
            short_put_strike: PriceCents::new(490_000),
            long_call_strike: PriceCents::new(520_000),
            long_put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(
                1_750_291_200_000_000_000,
            )),
            implied_volatility: Decimal::new(20, 2),
            risk_free_rate: Decimal::new(5, 2),
            dividend_yield: Decimal::ZERO,
            quantity,
            premium_short_call: PriceCents::new(2_000),
            premium_short_put: PriceCents::new(1_800),
            premium_long_call: PriceCents::new(800),
            premium_long_put: PriceCents::new(700),
            open_fee: PriceCents::new(65),
            close_fee: PriceCents::new(65),
        })
    }

    fn derive(config: &BacktestConfig) -> RunId {
        match RunId::derive(
            config.seed,
            config,
            &strategy(),
            TAPE_SHA,
            CODE_VERSION,
            LOCKFILE_SHA,
        ) {
            Ok(id) => id,
            Err(error) => panic!("run_id derives: {error}"),
        }
    }

    #[test]
    fn test_bundle_schema_tag_is_frozen_v1() {
        assert_eq!(BUNDLE_SCHEMA, "ironcondor.bundle.v1");
    }

    #[test]
    fn test_sort_columns_match_the_pinned_keys() {
        assert_eq!(FILLS_SORT_COLUMNS, ["step", "order_id", "fill_seq"]);
        assert_eq!(POSITIONS_SORT_COLUMNS, ["step", "position_id"]);
        assert_eq!(EQUITY_CURVE_SORT_COLUMNS, ["step"]);
        assert_eq!(GREEKS_ATTRIBUTION_SORT_COLUMNS, ["step"]);
    }

    #[test]
    fn test_run_id_serialises_transparently_as_a_bare_string() {
        let id = RunId::from_hex("abc123".to_string());
        let Ok(json) = serde_json::to_string(&id) else {
            panic!("run id serialises");
        };
        assert_eq!(json, "\"abc123\"");
        let back: Result<RunId, _> = serde_json::from_str("\"abc123\"");
        assert!(matches!(back, Ok(ref r) if r.as_str() == "abc123"));
    }

    #[test]
    fn test_run_id_is_byte_stable_for_the_same_tuple() {
        let config = base_config();
        // Deriving twice from the same tuple yields byte-identical ids: the
        // preimage serialises deterministically (no HashMap, no wall clock).
        assert_eq!(derive(&config), derive(&config));
    }

    #[test]
    fn test_run_id_excludes_overwrite_and_output_path() {
        let config = base_config();
        let mut only_operational = base_config();
        only_operational.overwrite = true;
        only_operational.output_dir = "a/totally/different/dir".into();
        // Two configs differing ONLY in the operational output controls hash
        // identically — the semantic config excludes them.
        assert_eq!(
            derive(&config),
            derive(&only_operational),
            "overwrite / output_dir must not change the run_id"
        );
    }

    #[test]
    fn test_run_id_changes_when_any_hashed_input_changes() {
        let base = derive(&base_config());

        // seed (passed explicitly and inside the semantic config).
        let mut seeded = base_config();
        seeded.seed = 43;
        assert_ne!(
            base,
            match RunId::derive(
                seeded.seed,
                &seeded,
                &strategy(),
                TAPE_SHA,
                CODE_VERSION,
                LOCKFILE_SHA
            ) {
                Ok(id) => id,
                Err(error) => panic!("derive: {error}"),
            },
            "a different seed must change the run_id"
        );

        // a semantic config field (execution mode).
        let mut moded = base_config();
        moded.mode = ExecutionMode::Realistic;
        assert_ne!(
            base,
            derive(&moded),
            "a different mode must change the run_id"
        );

        // the strategy spec.
        let config = base_config();
        let mut other_strategy = strategy();
        if let StrategySpec::IronCondor(ref mut spec) = other_strategy {
            spec.short_call_strike = PriceCents::new(515_000);
        }
        let strategy_changed = match RunId::derive(
            config.seed,
            &config,
            &other_strategy,
            TAPE_SHA,
            CODE_VERSION,
            LOCKFILE_SHA,
        ) {
            Ok(id) => id,
            Err(error) => panic!("derive: {error}"),
        };
        assert_ne!(
            base, strategy_changed,
            "a different strategy must change the run_id"
        );

        // the tape identity.
        let tape_changed = match RunId::derive(
            config.seed,
            &config,
            &strategy(),
            "2222222222222222222222222222222222222222222222222222222222222222",
            CODE_VERSION,
            LOCKFILE_SHA,
        ) {
            Ok(id) => id,
            Err(error) => panic!("derive: {error}"),
        };
        assert_ne!(
            base, tape_changed,
            "a different tape identity must change the run_id"
        );

        // the build identity (code version).
        let build_changed = match RunId::derive(
            config.seed,
            &config,
            &strategy(),
            TAPE_SHA,
            "0.3.1",
            LOCKFILE_SHA,
        ) {
            Ok(id) => id,
            Err(error) => panic!("derive: {error}"),
        };
        assert_ne!(
            base, build_changed,
            "a different build identity must change the run_id"
        );
    }

    #[test]
    fn test_run_id_is_a_lower_hex_sha256() {
        let id = derive(&base_config());
        assert_eq!(id.as_str().len(), 64, "sha256 is 32 bytes = 64 hex chars");
        assert!(
            id.as_str()
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "run_id is lower-hex"
        );
    }

    #[test]
    fn test_row_counts_serialises_as_the_four_keyed_object() {
        let counts = RowCounts::new(12, 5, 7, 5);
        let Ok(json) = serde_json::to_value(counts) else {
            panic!("row counts serialise");
        };
        let Some(obj) = json.as_object() else {
            panic!("object");
        };
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["equity_curve", "fills", "greeks_attribution", "positions"]
        );
        assert!(matches!(
            obj.get("fills").and_then(serde_json::Value::as_u64),
            Some(12)
        ));
    }

    fn sample_manifest() -> Manifest {
        let config = base_config();
        let run_id = derive(&config);
        Manifest {
            schema: BUNDLE_SCHEMA.to_string(),
            run_id,
            created_utc: "2026-07-16T00:00:00Z".to_string(),
            code_version: CODE_VERSION.to_string(),
            lockfile_sha256: LOCKFILE_SHA.to_string(),
            seed: config.seed,
            config: config.clone(),
            strategy: strategy(),
            data_source: config.data_source.clone(),
            metrics: Metrics::from_result(&BacktestResult::default()),
            row_counts: RowCounts::new(4, 3, 4, 3),
        }
    }

    #[test]
    fn test_manifest_carries_exactly_the_docs_06_fields() {
        let Ok(json) = serde_json::to_value(sample_manifest()) else {
            panic!("manifest serialises");
        };
        let Some(obj) = json.as_object() else {
            panic!("manifest is a JSON object");
        };
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut expected = [
            "schema",
            "run_id",
            "created_utc",
            "code_version",
            "lockfile_sha256",
            "seed",
            "config",
            "strategy",
            "data_source",
            "metrics",
            "row_counts",
        ];
        expected.sort_unstable();
        assert_eq!(
            keys, expected,
            "manifest must carry exactly the docs/05 §6 fields"
        );
        // The tag-fixed currency rule: there is NO currency field.
        assert!(!obj.contains_key("currency"), "v1 has no currency field");
        // metrics and row_counts are present (required).
        assert!(obj.contains_key("metrics"));
        assert!(obj.contains_key("row_counts"));
    }

    #[test]
    fn test_manifest_round_trips_through_json() {
        let Ok(json) = serde_json::to_string(&sample_manifest()) else {
            panic!("manifest serialises");
        };
        let back: Result<Manifest, _> = serde_json::from_str(&json);
        let Ok(back) = back else {
            panic!("manifest deserialises");
        };
        // Compare the fields that carry PartialEq (Metrics has none by design).
        assert_eq!(back.schema, BUNDLE_SCHEMA);
        assert_eq!(back.run_id, sample_manifest().run_id);
        assert_eq!(back.seed, 42);
        assert_eq!(back.config, base_config());
        assert_eq!(back.row_counts, RowCounts::new(4, 3, 4, 3));
    }
}
