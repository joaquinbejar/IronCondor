//! Backtest configuration.
//!
//! [`BacktestConfig`] is the untrusted-config boundary: every ceiling and
//! range check lives in [`BacktestConfig::validate`], once — the engine loop
//! assumes a config it receives is already valid. Unknown keys are a
//! deserialisation error (`deny_unknown_fields`).
//!
//! The **operational output controls** (`output_dir`, `overwrite`) are
//! excluded from the semantic config that the `run_id` hashes (v0.3):
//! toggling `overwrite` or redirecting output never changes run behaviour.

use std::path::{Component, PathBuf};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::data::DataSourceSpec;
use crate::domain::ExecutionMode;
use crate::error::BacktestError;

/// Broker/exchange fees applied to fills, in integer cents.
///
/// `per_contract_cents` is charged on every fill as
/// `fill.quantity × per_contract_cents`; `per_order_cents` is charged once
/// per order, on that order's first fill (`fill_seq = 0`), and is `0` on
/// every later fill of the same order. An order that produces no fill pays
/// no per-order fee. Fees are always `≥ 0` and are subtracted in every P&L
/// identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeSchedule {
    /// Fee per contract in integer cents, charged on every fill.
    pub per_contract_cents: u64,
    /// Fee per order in integer cents, charged once on the first fill.
    pub per_order_cents: u64,
}

/// The configured slippage applied by the **naive** fill model.
///
/// Configured slippage is always adverse (worsens the fill) and is applied
/// on top of the reference price. All models are integer-cents and
/// deterministic — none reads a random source. Realistic mode ignores this
/// entirely: its slippage is an emergent property of the order book.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "model", rename_all = "snake_case")]
pub enum SlippageModel {
    /// No slippage — fills at the reference price.
    None,
    /// A flat adverse offset in integer cents per fill.
    FixedCents {
        /// The adverse offset in integer cents.
        cents: u64,
    },
    /// A fraction of the half-spread, per fill (must be `≥ 0`).
    SpreadFraction {
        /// The multiplier applied to the half-spread.
        fraction: Decimal,
    },
    /// An adverse offset scaling with the filled quantity.
    SizeProportional {
        /// The adverse offset in integer cents per contract.
        cents_per_contract: u64,
    },
}

/// How the realistic-mode book seeder sizes a strike's **touch** level — the
/// resting order placed at the quoted `bid` / `ask` before the geometric decay
/// fills the deeper levels ([docs/04 §6](../docs/04-execution-models.md)).
///
/// Only realistic mode (feature `orderbook`) consumes this; naive mode has no
/// book. The type is defined unconditionally so [`BacktestConfig`] carries the
/// profile in every build and the manifest records it identically regardless of
/// features (reproducibility).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TouchSize {
    /// Touch size = the quote's own size for that side: `bid_size` for the bid
    /// touch, `ask_size` for the ask touch. The default — it tracks the actual
    /// quoted depth.
    QuotedSize,
    /// A flat touch size in contracts on both sides, independent of the quote
    /// (`> 0`). Useful when the feed carries no reliable per-side sizes.
    Flat {
        /// The flat touch depth in contracts; must be `> 0`.
        contracts: u32,
    },
}

impl Default for TouchSize {
    /// The default touch-size function tracks the quoted per-side size.
    fn default() -> Self {
        Self::QuotedSize
    }
}

/// The default deeper-level count `L` ([docs/04 §6](../docs/04-execution-models.md), `5`).
const fn default_depth_levels() -> u32 {
    5
}

/// The default geometric decay factor `r` ([docs/04 §6](../docs/04-execution-models.md), `0.5`).
fn default_decay() -> Decimal {
    // `0.5` as mantissa `5` at scale `1`; no `f64`, no `dec!` (test-only macro).
    Decimal::new(5, 1)
}

/// The per-strike liquidity profile that realistic mode seeds each book from
/// ([docs/04 §6](../docs/04-execution-models.md)).
///
/// For every quoted contract the seeder rests a **touch** level at the quoted
/// `bid` / `ask` (sized by [`TouchSize`]) plus `depth_levels` deeper levels
/// stepping one `tick_size_cents` away from mid, with a **geometric size decay**
/// `round(touch_size × decayⁱ)` that terminates the ladder at the first level
/// rounding to zero. Recorded in the run config (and thus the manifest) so a
/// seeded book is reproducible. Only realistic mode consumes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiquidityProfile {
    /// How the touch level is sized (default [`TouchSize::QuotedSize`]).
    #[serde(default)]
    pub touch_size: TouchSize,
    /// `L`: the number of deeper levels seeded on each side beyond the touch,
    /// stepping one tick at a time away from mid (default `5`, capped at
    /// [`Self::MAX_DEPTH_LEVELS`]). `0` seeds the touch only.
    #[serde(default = "default_depth_levels")]
    pub depth_levels: u32,
    /// `r`: the geometric size-decay factor applied per level away from the
    /// touch, in `(0, 1]` (default `0.5`). `1` gives a uniform ladder; smaller
    /// values thin out the deeper levels faster.
    #[serde(default = "default_decay")]
    pub decay: Decimal,
}

impl Default for LiquidityProfile {
    /// The documented defaults: quoted-size touch, `L = 5`, `r = 0.5`.
    fn default() -> Self {
        Self {
            touch_size: TouchSize::default(),
            depth_levels: default_depth_levels(),
            decay: default_decay(),
        }
    }
}

impl LiquidityProfile {
    /// Hard cap for [`Self::depth_levels`]: 64 deeper levels per side.
    pub const MAX_DEPTH_LEVELS: u32 = 64;

    /// Validate the profile: `depth_levels` within its cap, `decay ∈ (0, 1]`,
    /// and a flat touch size `> 0`.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Config`] naming the first rejected field and
    /// its offending value.
    #[must_use = "an unvalidated liquidity profile must not reach the seeder"]
    pub fn validate(&self) -> Result<(), BacktestError> {
        if self.depth_levels > Self::MAX_DEPTH_LEVELS {
            return Err(BacktestError::Config(format!(
                "liquidity_profile.depth_levels = {} exceeds hard cap {}",
                self.depth_levels,
                Self::MAX_DEPTH_LEVELS
            )));
        }
        if self.decay <= Decimal::ZERO || self.decay > Decimal::ONE {
            return Err(BacktestError::Config(format!(
                "liquidity_profile.decay must be in (0, 1], got {}",
                self.decay
            )));
        }
        if let TouchSize::Flat { contracts } = self.touch_size
            && contracts == 0
        {
            return Err(BacktestError::Config(
                "liquidity_profile.touch_size flat contracts must be > 0, got 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Named resource ceilings governing every untrusted input surface.
///
/// Each field has a unit, a generous default, and an absolute hard cap —
/// [`BacktestConfig::validate`] rejects a configured value above its cap.
/// One struct governs CSV, Parquet, simulator materialisation, and bundle
/// read-back, so no path can allocate without a bound. A crossed ceiling at
/// run time is a typed [`BacktestError::TapeTooLarge`] (or a typed read-back
/// error), never a panic or an OOM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimits {
    /// Maximum tape steps materialised, all feeds. Unit: steps.
    pub max_steps: u64,
    /// Maximum contracts accepted in one chain snapshot. Unit: contracts.
    pub max_contracts_per_snapshot: u32,
    /// Maximum total bytes of a materialised tape. Unit: bytes.
    pub max_total_bytes: u64,
    /// Maximum size of one input file (CSV/Parquet). Unit: bytes.
    pub max_file_bytes: u64,
    /// Maximum decompressed bytes while decoding a file feed
    /// (decompression-bomb guard). Unit: bytes.
    pub max_decompressed_bytes: u64,
    /// Maximum size of a bundle `manifest.json` at read-back. Unit: bytes.
    pub max_manifest_bytes: u64,
    /// Maximum length of one manifest/Parquet string at read-back.
    /// Unit: bytes.
    pub max_string_len: u32,
    /// Maximum rows decoded per bundle Parquet table at read-back.
    /// Unit: rows.
    pub max_rows_per_table: u64,
}

impl ResourceLimits {
    /// Hard cap for [`Self::max_steps`]: 10 million steps.
    pub const CAP_MAX_STEPS: u64 = 10_000_000;
    /// Hard cap for [`Self::max_contracts_per_snapshot`]: 5 million contracts.
    pub const CAP_MAX_CONTRACTS_PER_SNAPSHOT: u32 = 5_000_000;
    /// Hard cap for [`Self::max_total_bytes`]: 32 GiB.
    pub const CAP_MAX_TOTAL_BYTES: u64 = 32 * (1 << 30);
    /// Hard cap for [`Self::max_file_bytes`]: 64 GiB.
    pub const CAP_MAX_FILE_BYTES: u64 = 64 * (1 << 30);
    /// Hard cap for [`Self::max_decompressed_bytes`]: 64 GiB.
    pub const CAP_MAX_DECOMPRESSED_BYTES: u64 = 64 * (1 << 30);
    /// Hard cap for [`Self::max_manifest_bytes`]: 256 MiB.
    pub const CAP_MAX_MANIFEST_BYTES: u64 = 256 * (1 << 20);
    /// Hard cap for [`Self::max_string_len`]: 16 MiB.
    pub const CAP_MAX_STRING_LEN: u32 = 16 * (1 << 20);
    /// Hard cap for [`Self::max_rows_per_table`]: 2^40 rows.
    pub const CAP_MAX_ROWS_PER_TABLE: u64 = 1 << 40;

    /// Check every field against its hard cap.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Config`] naming the first field whose
    /// configured value exceeds its hard cap.
    #[must_use = "the limits are not enforced unless the result is checked"]
    pub fn validate(&self) -> Result<(), BacktestError> {
        fn over(field: &str, value: u64, cap: u64) -> BacktestError {
            BacktestError::Config(format!("limits.{field} = {value} exceeds hard cap {cap}"))
        }
        if self.max_steps > Self::CAP_MAX_STEPS {
            return Err(over("max_steps", self.max_steps, Self::CAP_MAX_STEPS));
        }
        if self.max_contracts_per_snapshot > Self::CAP_MAX_CONTRACTS_PER_SNAPSHOT {
            return Err(over(
                "max_contracts_per_snapshot",
                u64::from(self.max_contracts_per_snapshot),
                u64::from(Self::CAP_MAX_CONTRACTS_PER_SNAPSHOT),
            ));
        }
        if self.max_total_bytes > Self::CAP_MAX_TOTAL_BYTES {
            return Err(over(
                "max_total_bytes",
                self.max_total_bytes,
                Self::CAP_MAX_TOTAL_BYTES,
            ));
        }
        if self.max_file_bytes > Self::CAP_MAX_FILE_BYTES {
            return Err(over(
                "max_file_bytes",
                self.max_file_bytes,
                Self::CAP_MAX_FILE_BYTES,
            ));
        }
        if self.max_decompressed_bytes > Self::CAP_MAX_DECOMPRESSED_BYTES {
            return Err(over(
                "max_decompressed_bytes",
                self.max_decompressed_bytes,
                Self::CAP_MAX_DECOMPRESSED_BYTES,
            ));
        }
        if self.max_manifest_bytes > Self::CAP_MAX_MANIFEST_BYTES {
            return Err(over(
                "max_manifest_bytes",
                self.max_manifest_bytes,
                Self::CAP_MAX_MANIFEST_BYTES,
            ));
        }
        if self.max_string_len > Self::CAP_MAX_STRING_LEN {
            return Err(over(
                "max_string_len",
                u64::from(self.max_string_len),
                u64::from(Self::CAP_MAX_STRING_LEN),
            ));
        }
        if self.max_rows_per_table > Self::CAP_MAX_ROWS_PER_TABLE {
            return Err(over(
                "max_rows_per_table",
                self.max_rows_per_table,
                Self::CAP_MAX_ROWS_PER_TABLE,
            ));
        }
        Ok(())
    }
}

impl Default for ResourceLimits {
    /// The documented generous defaults.
    fn default() -> Self {
        Self {
            max_steps: 100_000,
            max_contracts_per_snapshot: 50_000,
            max_total_bytes: 2 * (1 << 30),        // 2 GiB
            max_file_bytes: 4 * (1 << 30),         // 4 GiB
            max_decompressed_bytes: 8 * (1 << 30), // 8 GiB
            max_manifest_bytes: 16 * (1 << 20),    // 16 MiB
            max_string_len: 64 * (1 << 10),        // 64 KiB
            max_rows_per_table: 100_000_000,
        }
    }
}

/// The default realistic-mode marketable price cap in ticks
/// ([docs/04 §5.2](../docs/04-execution-models.md), default `10`).
const fn default_marketable_cap_ticks() -> u32 {
    10
}

/// The full configuration of one backtest run.
///
/// Everything downstream reads this config; [`Self::validate`] is the single
/// untrusted-config seam. The `output_dir` / `overwrite` fields are
/// **operational** — excluded from the semantic config the `run_id` hashes
/// (v0.3), so toggling them never changes the `run_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestConfig {
    /// Where the chain data comes from.
    pub data_source: DataSourceSpec,
    /// Which fill model executes order intents.
    pub mode: ExecutionMode,
    /// The engine seed driving every seeded RNG in the run.
    pub seed: u64,
    /// Starting capital in integer cents; must be `> 0`.
    pub initial_capital: u64,
    /// Broker/exchange fees, integer cents.
    pub fees: FeeSchedule,
    /// The naive-mode slippage model (ignored by realistic mode).
    pub slippage: SlippageModel,
    /// Realistic-mode marketable price cap, in ticks off the touch
    /// ([docs/04 §5.2](../docs/04-execution-models.md)). A marketable intent
    /// (`OrderIntent.limit = None`) becomes a tick-aligned aggressive limit
    /// `k` ticks through the touch (`ask + k·tick` for a buy, `bid − k·tick`
    /// for a sell); the order walks book levels only within `k` ticks and any
    /// remainder beyond the cap is cancelled, not chased. Must be `> 0`.
    /// Ignored by naive mode. Defaults to `10`.
    #[serde(default = "default_marketable_cap_ticks")]
    pub marketable_cap_ticks: u32,
    /// Realistic-mode per-strike book-seeding profile
    /// ([docs/04 §6](../docs/04-execution-models.md)): the touch-size function,
    /// the deeper-level count `L`, and the geometric decay `r`. Recorded here
    /// so a seeded book is reproducible from the manifest. Ignored by naive
    /// mode. Defaults to the documented profile (quoted-size touch, `L = 5`,
    /// `r = 0.5`).
    #[serde(default)]
    pub liquidity_profile: LiquidityProfile,
    /// Resource ceilings for every untrusted input surface.
    #[serde(default)]
    pub limits: ResourceLimits,
    /// Operational: where result bundles are written. Excluded from the
    /// semantic config the `run_id` hashes.
    pub output_dir: PathBuf,
    /// Operational: whether an existing bundle directory for the same
    /// `run_id` may be replaced. Excluded from the semantic config the
    /// `run_id` hashes.
    #[serde(default)]
    pub overwrite: bool,
}

impl BacktestConfig {
    /// Validate every field once, at the untrusted-config boundary.
    ///
    /// Checks: `initial_capital > 0`, every [`ResourceLimits`] field within
    /// its hard cap, a non-negative slippage fraction, and an output path
    /// that cannot escape its target directory (no `..` components, not
    /// empty).
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Config`] describing the first rejected field
    /// and its offending value.
    #[must_use = "an unvalidated config must not reach the engine"]
    pub fn validate(&self) -> Result<(), BacktestError> {
        if self.initial_capital == 0 {
            return Err(BacktestError::Config(
                "initial capital must be positive, got 0".to_string(),
            ));
        }
        if self.marketable_cap_ticks == 0 {
            return Err(BacktestError::Config(
                "marketable_cap_ticks must be positive, got 0".to_string(),
            ));
        }
        self.limits.validate()?;
        self.liquidity_profile.validate()?;
        if let SlippageModel::SpreadFraction { fraction } = &self.slippage
            && fraction.is_sign_negative()
        {
            return Err(BacktestError::Config(format!(
                "slippage spread fraction must be non-negative, got {fraction}"
            )));
        }
        if self.output_dir.as_os_str().is_empty() {
            return Err(BacktestError::Config(
                "output dir must not be empty".to_string(),
            ));
        }
        if self
            .output_dir
            .components()
            .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(BacktestError::Config(format!(
                "output dir must not contain '..' components, got {}",
                self.output_dir.display()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{
        BacktestConfig, FeeSchedule, LiquidityProfile, ResourceLimits, SlippageModel, TouchSize,
    };
    use crate::data::DataSourceSpec;
    use crate::domain::ExecutionMode;
    use crate::error::BacktestError;

    fn valid_config() -> BacktestConfig {
        BacktestConfig {
            data_source: DataSourceSpec::Parquet {
                path: "chains/spx.parquet".to_string(),
                sha256: String::new(),
            },
            mode: ExecutionMode::Naive,
            seed: 42,
            initial_capital: 10_000_000,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            slippage: SlippageModel::SpreadFraction {
                fraction: dec!(0.5),
            },
            marketable_cap_ticks: 10,
            liquidity_profile: LiquidityProfile::default(),
            limits: ResourceLimits::default(),
            output_dir: "runs/out".into(),
            overwrite: false,
        }
    }

    fn assert_config_error(result: Result<(), BacktestError>, needle: &str) {
        match result {
            Err(BacktestError::Config(msg)) => {
                assert!(msg.contains(needle), "message {msg:?} misses {needle:?}");
            }
            other => panic!("expected BacktestError::Config, got {other:?}"),
        }
    }

    #[test]
    fn test_config_accepts_valid_config_ok() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn test_config_rejects_zero_capital_config_error() {
        let mut cfg = valid_config();
        cfg.initial_capital = 0;
        assert_config_error(cfg.validate(), "initial capital");
    }

    type LimitBump = fn(&mut ResourceLimits);

    #[test]
    fn test_config_rejects_each_limits_field_over_cap_config_error() {
        let cases: [(&str, LimitBump); 8] = [
            ("max_steps", |l| {
                l.max_steps = ResourceLimits::CAP_MAX_STEPS + 1;
            }),
            ("max_contracts_per_snapshot", |l| {
                l.max_contracts_per_snapshot = ResourceLimits::CAP_MAX_CONTRACTS_PER_SNAPSHOT + 1;
            }),
            ("max_total_bytes", |l| {
                l.max_total_bytes = ResourceLimits::CAP_MAX_TOTAL_BYTES + 1;
            }),
            ("max_file_bytes", |l| {
                l.max_file_bytes = ResourceLimits::CAP_MAX_FILE_BYTES + 1;
            }),
            ("max_decompressed_bytes", |l| {
                l.max_decompressed_bytes = ResourceLimits::CAP_MAX_DECOMPRESSED_BYTES + 1;
            }),
            ("max_manifest_bytes", |l| {
                l.max_manifest_bytes = ResourceLimits::CAP_MAX_MANIFEST_BYTES + 1;
            }),
            ("max_string_len", |l| {
                l.max_string_len = ResourceLimits::CAP_MAX_STRING_LEN + 1;
            }),
            ("max_rows_per_table", |l| {
                l.max_rows_per_table = ResourceLimits::CAP_MAX_ROWS_PER_TABLE + 1;
            }),
        ];
        for (field, bump) in cases {
            let mut cfg = valid_config();
            bump(&mut cfg.limits);
            assert_config_error(cfg.validate(), field);
        }
    }

    #[test]
    fn test_config_rejects_parent_dir_output_path_config_error() {
        let mut cfg = valid_config();
        cfg.output_dir = "runs/../../etc".into();
        assert_config_error(cfg.validate(), "..");
    }

    #[test]
    fn test_config_rejects_empty_output_path_config_error() {
        let mut cfg = valid_config();
        cfg.output_dir = std::path::PathBuf::new();
        assert_config_error(cfg.validate(), "output dir");
    }

    #[test]
    fn test_config_rejects_zero_marketable_cap_ticks_config_error() {
        let mut cfg = valid_config();
        cfg.marketable_cap_ticks = 0;
        assert_config_error(cfg.validate(), "marketable_cap_ticks");
    }

    #[test]
    fn test_config_marketable_cap_ticks_defaults_to_ten_when_absent() {
        // A config JSON omitting the field deserialises with the documented
        // default of 10 (serde default), so old configs stay valid.
        let json = r#"{
            "data_source": {"kind": "parquet", "path": "p.parquet", "sha256": ""},
            "mode": "naive",
            "seed": 1,
            "initial_capital": 1000,
            "fees": {"per_contract_cents": 0, "per_order_cents": 0},
            "slippage": {"model": "none"},
            "output_dir": "out"
        }"#;
        let parsed: Result<BacktestConfig, _> = serde_json::from_str(json);
        assert!(matches!(parsed, Ok(ref c) if c.marketable_cap_ticks == 10));
    }

    #[test]
    fn test_liquidity_profile_defaults_match_documented_values() {
        let profile = LiquidityProfile::default();
        assert_eq!(profile.touch_size, TouchSize::QuotedSize);
        assert_eq!(profile.depth_levels, 5);
        assert_eq!(profile.decay, dec!(0.5));
    }

    #[test]
    fn test_config_rejects_depth_levels_over_cap_config_error() {
        let mut cfg = valid_config();
        cfg.liquidity_profile.depth_levels = LiquidityProfile::MAX_DEPTH_LEVELS + 1;
        assert_config_error(cfg.validate(), "depth_levels");
    }

    #[test]
    fn test_config_rejects_decay_out_of_range_config_error() {
        for bad in [dec!(0), dec!(-0.1), dec!(1.5)] {
            let mut cfg = valid_config();
            cfg.liquidity_profile.decay = bad;
            assert_config_error(cfg.validate(), "decay");
        }
    }

    #[test]
    fn test_config_accepts_decay_of_one_uniform_ladder_ok() {
        // `r = 1` is a valid (uniform-depth) profile: the boundary is inclusive.
        let mut cfg = valid_config();
        cfg.liquidity_profile.decay = dec!(1);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_config_rejects_zero_flat_touch_size_config_error() {
        let mut cfg = valid_config();
        cfg.liquidity_profile.touch_size = TouchSize::Flat { contracts: 0 };
        assert_config_error(cfg.validate(), "flat contracts");
    }

    #[test]
    fn test_config_liquidity_profile_defaults_when_absent() {
        // A config JSON omitting the profile deserialises with the documented
        // default (quoted-size touch, L = 5, r = 0.5), so old configs stay valid.
        let json = r#"{
            "data_source": {"kind": "parquet", "path": "p.parquet", "sha256": ""},
            "mode": "realistic",
            "seed": 1,
            "initial_capital": 1000,
            "fees": {"per_contract_cents": 0, "per_order_cents": 0},
            "slippage": {"model": "none"},
            "output_dir": "out"
        }"#;
        let parsed: Result<BacktestConfig, _> = serde_json::from_str(json);
        assert!(matches!(
            parsed,
            Ok(ref c)
                if c.liquidity_profile == LiquidityProfile::default()
                && c.liquidity_profile.decay == dec!(0.5)
        ));
    }

    #[test]
    fn test_config_liquidity_profile_flat_touch_round_trips() {
        let mut cfg = valid_config();
        cfg.liquidity_profile = LiquidityProfile {
            touch_size: TouchSize::Flat { contracts: 25 },
            depth_levels: 3,
            decay: dec!(0.7),
        };
        let json = serde_json::to_string(&cfg).unwrap_or_default();
        let back: Result<BacktestConfig, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref c) if *c == cfg));
    }

    #[test]
    fn test_config_rejects_negative_slippage_fraction_config_error() {
        let mut cfg = valid_config();
        cfg.slippage = SlippageModel::SpreadFraction {
            fraction: dec!(-0.1),
        };
        assert_config_error(cfg.validate(), "non-negative");
    }

    #[test]
    fn test_limits_defaults_match_documented_values() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_steps, 100_000);
        assert_eq!(limits.max_contracts_per_snapshot, 50_000);
        assert_eq!(limits.max_total_bytes, 2 * (1 << 30));
        assert_eq!(limits.max_file_bytes, 4 * (1 << 30));
        assert_eq!(limits.max_decompressed_bytes, 8 * (1 << 30));
        assert_eq!(limits.max_manifest_bytes, 16 * (1 << 20));
        assert_eq!(limits.max_string_len, 64 * (1 << 10));
        assert_eq!(limits.max_rows_per_table, 100_000_000);
    }

    #[test]
    fn test_config_unknown_field_fails_deserialisation() {
        let json = r#"{
            "data_source": {"kind": "parquet", "path": "p.parquet", "sha256": ""},
            "mode": "naive",
            "seed": 1,
            "initial_capital": 1000,
            "fees": {"per_contract_cents": 0, "per_order_cents": 0},
            "slippage": {"model": "none"},
            "output_dir": "out",
            "not_a_real_field": true
        }"#;
        let parsed: Result<BacktestConfig, _> = serde_json::from_str(json);
        assert!(parsed.is_err(), "unknown key must fail deserialisation");
    }

    #[test]
    fn test_limits_unknown_field_fails_deserialisation() {
        let json = r#"{
            "max_steps": 1, "max_contracts_per_snapshot": 1, "max_total_bytes": 1,
            "max_file_bytes": 1, "max_decompressed_bytes": 1, "max_manifest_bytes": 1,
            "max_string_len": 1, "max_rows_per_table": 1, "bogus": 0
        }"#;
        let parsed: Result<ResourceLimits, _> = serde_json::from_str(json);
        assert!(parsed.is_err(), "unknown key must fail deserialisation");
    }

    #[test]
    fn test_config_serde_round_trip_preserves_fields() {
        let cfg = valid_config();
        let json = serde_json::to_string(&cfg).unwrap_or_default();
        let back: Result<BacktestConfig, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref c) if *c == cfg));
    }
}
