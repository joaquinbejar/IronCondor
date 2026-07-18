//! Chain data feeds.
//!
//! The `DataFeed` seam, the historical Parquet/CSV loaders, the
//! OptionChain-Simulator session client (feature `simulator`), and the single
//! `ChainResponse` → `OptionChain` conversion boundary (roadmap issues #7–#9,
//! #27, #44, #45). [`DataSourceSpec`] and — behind the `simulator` feature —
//! the migrated session `simulator` client (issue #6) exist so far; the feed
//! seam and loaders land with the remaining issues.

pub mod convert;
#[cfg(feature = "simulator")]
pub mod simulator;

#[cfg(feature = "simulator")]
pub use convert::chain_response_to_snapshot;
pub use convert::{RawQuote, SnapshotMeta, raw_quotes_to_snapshot, snapshot_to_option_chain};

use serde::{Deserialize, Serialize};

/// Where a run's chain data comes from — provenance and a re-read locator.
///
/// The spec is recorded verbatim in the run manifest so a run is re-runnable
/// by re-reading the same inputs; the file `sha256` verifies the re-read
/// bytes are unchanged. At configuration time the `sha256` may be empty — it
/// is computed and pinned when the tape is materialised, and a non-empty
/// configured value is verified against the bytes actually read.
///
/// A `Simulator` variant (feature `simulator`) joins in v0.5.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

#[cfg(test)]
mod tests {
    use super::DataSourceSpec;

    #[test]
    fn test_data_source_spec_kind_tagged_round_trip() {
        let spec = DataSourceSpec::Parquet {
            path: "chains/spx.parquet".to_string(),
            sha256: String::new(),
        };
        let json = serde_json::to_string(&spec).unwrap_or_default();
        assert!(json.contains("\"kind\":\"parquet\""));
        let back: Result<DataSourceSpec, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref s) if *s == spec));
    }
}
