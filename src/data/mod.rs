//! Chain data feeds.
//!
//! The `DataFeed` seam, the historical Parquet/CSV loaders, the
//! OptionChain-Simulator session client and its materialised-tape feed
//! (feature `simulator`), and the single `ChainResponse` → `OptionChain`
//! conversion boundary (roadmap issues #7–#9, #27, #44, #45). The
//! [`DataFeed`] trait, its [`TapeMeta`], the [`DataSourceSpec`] provenance
//! enum, and the [`FeedKind`] / [`feed_catalogue`] seam land here (issue #8);
//! the migrated session `simulator` client (issue #6), and the
//! `SimulatorFeed` that materialises a whole session to a validated tape
//! before the loop (issue #45), sit behind the `simulator` feature.

pub mod convert;
pub mod feed;
pub mod historical;
#[cfg(feature = "simulator")]
pub mod simulator;

#[cfg(feature = "simulator")]
pub use convert::chain_response_to_snapshot;
pub use convert::{RawQuote, SnapshotMeta, raw_quotes_to_snapshot, snapshot_to_option_chain};
pub use feed::{DataFeed, FeedKind, TapeMeta, feed_catalogue};
pub use historical::{CsvFeed, ParquetFeed};
#[cfg(feature = "simulator")]
pub use simulator::SimulatorFeed;

use serde::{Deserialize, Serialize};

#[cfg(feature = "simulator")]
use crate::data::simulator::CreateSessionRequest;

/// Where a run's chain data comes from — provenance and a re-read locator.
///
/// The spec is recorded verbatim in the run manifest so a run is re-runnable
/// by re-reading the same inputs; the file `sha256` verifies the re-read
/// bytes are unchanged. At configuration time the `sha256` may be empty — it
/// is computed and pinned when the tape is materialised, and a non-empty
/// configured value is verified against the bytes actually read.
///
/// The `Simulator` variant is feature-gated behind `simulator`
/// ([docs/03 §2](../../../docs/03-data-layer.md#2-the-datafeed-trait)).
///
/// `Eq` is **not** derived: the `Simulator` variant embeds a
/// [`CreateSessionRequest`], whose wire `f64` fields are not `Eq`. The design
/// pins exactly `Debug + Clone + PartialEq + Serialize + Deserialize`
/// ([docs/03 §2](../../../docs/03-data-layer.md#2-the-datafeed-trait)).
// The `Simulator` variant is intentionally larger than the file variants — the
// design pins the full `CreateSessionRequest` in its payload (docs/03 §2), and
// boxing it would deviate from the pinned shape for a spec that is constructed
// once per run, never on a hot path. The size lint is therefore silenced here.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DataSourceSpec {
    /// A directory of per-step CSV chain files (v0.2 breadth).
    Csv {
        /// Path to the CSV data on disk.
        path: String,
        /// Hex-encoded sha256 of the file contents; empty until pinned.
        sha256: String,
    },
    /// A Parquet chain dataset (the v0.1 release feed).
    Parquet {
        /// Path to the Parquet data on disk.
        path: String,
        /// Hex-encoded sha256 of the file contents; empty until pinned.
        sha256: String,
    },
    /// A synthetic OptionChain-Simulator session (feature `simulator`).
    ///
    /// Recorded verbatim in the manifest as **provenance and a re-read
    /// locator**; the `run_id` data identity, however, is the `tape_sha256`
    /// (the materialised-tape hash) — same-seed tape identity across sessions
    /// is asserted only by the issue #45 closing test
    /// ([docs/03 §2](../../../docs/03-data-layer.md#2-the-datafeed-trait),
    /// [docs/03 §6](../../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
    ///
    /// The payload is a newtype over [`SimulatorSourceSpec`] rather than an
    /// inline struct variant: serde silently ignores `deny_unknown_fields` on
    /// an internally-tagged enum (serde-rs/serde#1600), so rejecting unknown
    /// keys — mandated for this spec — requires the fields to live on a
    /// struct that carries the attribute itself. The wire shape is unchanged
    /// (the inner fields inline beside the `kind` tag).
    #[cfg(feature = "simulator")]
    Simulator(SimulatorSourceSpec),
}

/// The provenance payload of [`DataSourceSpec::Simulator`]
/// ([docs/03 §2](../../../docs/03-data-layer.md#2-the-datafeed-trait)).
///
/// `deny_unknown_fields` holds here (and on the nested request DTO), so a
/// manifest or config carrying a mistyped or unexpected key fails loudly at
/// the boundary instead of being silently dropped.
#[cfg(feature = "simulator")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorSourceSpec {
    /// The walk parameters sent to the simulator on `create_session`.
    pub session: CreateSessionRequest,
    /// Which simulator instance served the session (its base URL).
    pub base_url: String,
    /// The **data** seed — distinct from the engine seed (`config.seed`).
    /// Since upstream v0.1.0 the simulator accepts a walk seed
    /// (`CreateSessionRequest::seed`), so this value **can drive the server
    /// walk**: tape materialisation (issue #45) sends it as `session.seed`,
    /// defaulting to `config.seed` when the run config does not set it
    /// ([docs/03 §6](../../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
    pub data_seed: u64,
    /// The `sha256` of the materialised tape
    /// ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)) —
    /// the `run_id` data identity, persisted so a simulator run's tape can
    /// be verified later.
    pub tape_sha256: String,
    /// The server build id when it exposes one; `None` otherwise.
    pub simulator_version: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::DataSourceSpec;

    #[test]
    fn test_data_source_spec_parquet_kind_tagged_round_trip() {
        let spec = DataSourceSpec::Parquet {
            path: "chains/spx.parquet".to_string(),
            sha256: String::new(),
        };
        let json = serde_json::to_string(&spec).unwrap_or_default();
        assert!(json.contains("\"kind\":\"parquet\""));
        let back: Result<DataSourceSpec, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref s) if *s == spec));
    }

    #[test]
    fn test_data_source_spec_csv_kind_tagged_round_trip() {
        let spec = DataSourceSpec::Csv {
            path: "chains/spx/".to_string(),
            sha256: "deadbeef".to_string(),
        };
        let json = serde_json::to_string(&spec).unwrap_or_default();
        assert!(json.contains("\"kind\":\"csv\""));
        let back: Result<DataSourceSpec, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref s) if *s == spec));
    }

    #[cfg(feature = "simulator")]
    fn simulator_spec() -> DataSourceSpec {
        use crate::data::simulator::CreateSessionRequest;
        use serde_json::json;

        DataSourceSpec::Simulator(super::SimulatorSourceSpec {
            session: CreateSessionRequest {
                symbol: "SPX".to_string(),
                steps: 20,
                initial_price: 100.0,
                days_to_expiration: 30.0,
                volatility: 0.2,
                risk_free_rate: 0.03,
                dividend_yield: 0.0,
                method: json!({"GeometricBrownian": {"dt": 0.004, "drift": 0.05, "volatility": 0.25}}),
                time_frame: "Day".to_string(),
                chain_size: Some(15),
                strike_interval: Some(5.0),
                skew_slope: None,
                smile_curve: None,
                spread: Some(0.02),
                seed: Some(7),
            },
            base_url: "http://localhost:7070".to_string(),
            data_seed: 7,
            tape_sha256: "cafef00d".to_string(),
            simulator_version: Some("0.1.0".to_string()),
        })
    }

    #[cfg(feature = "simulator")]
    #[test]
    fn test_data_source_spec_simulator_kind_tagged_round_trip() {
        let spec = simulator_spec();
        let json = serde_json::to_string(&spec).unwrap_or_default();
        assert!(json.contains("\"kind\":\"simulator\""));
        assert!(
            json.contains("\"base_url\""),
            "the newtype payload inlines beside the kind tag: {json}"
        );
        let back: Result<DataSourceSpec, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref s) if *s == spec));
    }

    #[cfg(feature = "simulator")]
    #[test]
    fn test_data_source_spec_simulator_rejects_unknown_top_level_field() {
        // deny_unknown_fields must hold through the internal `kind` tag — the
        // reason the payload is a struct, not an inline variant.
        let spec = simulator_spec();
        let json = serde_json::to_string(&spec).unwrap_or_default();
        let Some(poisoned) = json
            .strip_suffix('}')
            .map(|j| format!("{j},\"surprise\":1}}"))
        else {
            panic!("serialised spec must be a JSON object");
        };
        let back: Result<DataSourceSpec, _> = serde_json::from_str(&poisoned);
        assert!(
            back.is_err(),
            "an unknown key in the simulator payload must be rejected"
        );
    }

    #[cfg(feature = "simulator")]
    #[test]
    fn test_data_source_spec_simulator_rejects_unknown_session_field() {
        let spec = simulator_spec();
        let json = serde_json::to_string(&spec).unwrap_or_default();
        let poisoned = json.replace("\"session\":{", "\"session\":{\"surprise\":1,");
        assert_ne!(poisoned, json, "the session object must be present");
        let back: Result<DataSourceSpec, _> = serde_json::from_str(&poisoned);
        assert!(
            back.is_err(),
            "an unknown key nested in the session request must be rejected"
        );
    }
}
