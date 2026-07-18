//! Chain data feeds.
//!
//! The `DataFeed` seam, the historical Parquet/CSV loaders, the
//! OptionChain-Simulator session client (feature `simulator`), and the single
//! `ChainResponse` → `OptionChain` conversion boundary (roadmap issues #7–#9,
//! #27, #44, #45). The [`DataFeed`] trait, its [`TapeMeta`], the
//! [`DataSourceSpec`] provenance enum, and the [`FeedKind`] / [`feed_catalogue`]
//! seam land here (issue #8); the migrated session `simulator` client (issue
//! #6) sits behind the `simulator` feature; the concrete feed loaders land with
//! the remaining issues.

pub mod convert;
pub mod feed;
pub mod historical;
#[cfg(feature = "simulator")]
pub mod simulator;

#[cfg(feature = "simulator")]
pub use convert::chain_response_to_snapshot;
pub use convert::{RawQuote, SnapshotMeta, raw_quotes_to_snapshot, snapshot_to_option_chain};
pub use feed::{DataFeed, FeedKind, TapeMeta, feed_catalogue};
pub use historical::ParquetFeed;

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
// design pins `session: CreateSessionRequest` inline (docs/03 §2), and boxing
// it would deviate from the pinned shape for a spec that is constructed once
// per run, never on a hot path. The size lint is therefore silenced here.
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
    /// (the materialised-tape hash), because the recorded `session` +
    /// `data_seed` cannot pin the tape — the upstream server walk is unseeded
    /// ([docs/03 §2](../../../docs/03-data-layer.md#2-the-datafeed-trait),
    /// [docs/03 §6](../../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
    #[cfg(feature = "simulator")]
    Simulator {
        /// The walk parameters sent to the simulator on `create_session`.
        session: CreateSessionRequest,
        /// Which simulator instance served the session (its base URL).
        base_url: String,
        /// The **data** seed — distinct from the engine seed (`config.seed`).
        /// Controls **no** RNG today (the server walk is unseeded); it only
        /// **records intent** and will drive the server walk once the upstream
        /// seed channel exists
        /// ([docs/03 §6](../../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
        /// Defaults to `config.seed` then.
        data_seed: u64,
        /// The `sha256` of the materialised tape
        /// ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)) —
        /// the `run_id` data identity, persisted so a simulator run's tape can
        /// be verified later.
        tape_sha256: String,
        /// The server build id when it exposes one; `None` otherwise.
        simulator_version: Option<String>,
    },
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
    #[test]
    fn test_data_source_spec_simulator_kind_tagged_round_trip() {
        use crate::data::simulator::CreateSessionRequest;
        use serde_json::json;

        let spec = DataSourceSpec::Simulator {
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
            },
            base_url: "http://localhost:7070".to_string(),
            data_seed: 7,
            tape_sha256: "cafef00d".to_string(),
            simulator_version: Some("0.0.2".to_string()),
        };
        let json = serde_json::to_string(&spec).unwrap_or_default();
        assert!(json.contains("\"kind\":\"simulator\""));
        let back: Result<DataSourceSpec, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref s) if *s == spec));
    }
}
