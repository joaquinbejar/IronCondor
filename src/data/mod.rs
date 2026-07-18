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
// The batch parse cache (a shareable read-only Parquet tape) is an internal
// optimisation for the scenario batch runner — not part of the public surface.
pub(crate) use historical::SharedParquetTape;
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
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorSourceSpec {
    /// The walk parameters sent to the simulator on `create_session`.
    pub session: CreateSessionRequest,
    /// Which simulator instance served the session (its base URL).
    ///
    /// A credential embedded here as URL userinfo (`http://user:token@host`)
    /// is what `reqwest` turns into a transport `Authorization` header, so the
    /// in-memory value the client is built from **keeps** it — but it is
    /// **redacted on serialisation** (custom [`Serialize`] impl below), so no
    /// recorded copy (the manifest `data_source`, the nested
    /// `config.data_source`, or the `run_id` preimage) ever carries the
    /// plaintext credential ([docs/07 §9](../../../docs/07-performance-and-security.md#9-secrets-handling)).
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

/// Strip any `userinfo@` (embedded HTTP basic-auth credentials) from a URL,
/// keeping `scheme://host[:port][/path…]`. A credential embedded in a
/// simulator base URL (`http://user:token@host`) is honoured by `reqwest` for
/// transport, but the plaintext URL must never reach a manifest, a log, or an
/// error ([docs/07 §9](../../../docs/07-performance-and-security.md#9-secrets-handling)).
///
/// Applied at serialisation, so every recorded copy is redacted uniformly.
/// Fail-closed: any input that does not parse as `scheme://authority…` and
/// still cannot be shown credential-free collapses to `"[redacted-url]"`
/// rather than risk leaking. A URL with no userinfo is returned unchanged.
#[cfg(feature = "simulator")]
fn redact_url_userinfo(url: &str) -> String {
    let Some(scheme_sep) = url.find("://") else {
        // No authority component — nothing that can hold userinfo credentials.
        return url.to_string();
    };
    let authority_start = scheme_sep + "://".len();
    let (Some(prefix), Some(rest)) = (url.get(..authority_start), url.get(authority_start..))
    else {
        return "[redacted-url]".to_string();
    };
    // The authority runs until the first '/', '?' or '#' (or end of string).
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let Some(authority) = rest.get(..authority_end) else {
        return "[redacted-url]".to_string();
    };
    let Some(at) = authority.rfind('@') else {
        // No userinfo — the URL is already credential-free.
        return url.to_string();
    };
    // Everything after the last '@' in the authority is host[:port], and the
    // path/query/fragment tail beyond it is preserved verbatim.
    match rest.get(at + 1..) {
        Some(host_and_tail) => format!("{prefix}{host_and_tail}"),
        None => "[redacted-url]".to_string(),
    }
}

/// Custom [`Serialize`] mirroring the derived shape exactly (same five fields,
/// same order, `deny_unknown_fields` still governs deserialisation), except
/// `base_url` is passed through `redact_url_userinfo` so a credential embedded
/// as URL userinfo never lands in a manifest, a bundle, or a `run_id` preimage.
/// Deserialisation stays derived — a redacted URL round-trips to itself.
#[cfg(feature = "simulator")]
impl Serialize for SimulatorSourceSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("SimulatorSourceSpec", 5)?;
        state.serialize_field("session", &self.session)?;
        state.serialize_field("base_url", &redact_url_userinfo(&self.base_url))?;
        state.serialize_field("data_seed", &self.data_seed)?;
        state.serialize_field("tape_sha256", &self.tape_sha256)?;
        state.serialize_field("simulator_version", &self.simulator_version)?;
        state.end()
    }
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

    #[cfg(feature = "simulator")]
    #[test]
    fn test_redact_url_userinfo_strips_credentials_but_keeps_identity() {
        use super::redact_url_userinfo;
        // user:token@ credentials are stripped; scheme/host/port/path survive.
        assert_eq!(
            redact_url_userinfo("http://ci-bot:s3cr3t@sim.internal:7070/api/v1/chain"),
            "http://sim.internal:7070/api/v1/chain"
        );
        // A lone user (no password) is still stripped.
        assert_eq!(
            redact_url_userinfo("https://token@host:443"),
            "https://host:443"
        );
        // The last '@' delimits host, so a '@' inside the userinfo is dropped too.
        assert_eq!(
            redact_url_userinfo("http://user:p@ss@host:7070"),
            "http://host:7070"
        );
        // A credential-free URL is returned byte-for-byte unchanged (the common
        // case — every existing golden / offline fixture takes this path).
        assert_eq!(
            redact_url_userinfo("http://localhost:7070"),
            "http://localhost:7070"
        );
        assert_eq!(
            redact_url_userinfo("http://127.0.0.1:59440"),
            "http://127.0.0.1:59440"
        );
        // No scheme separator: left as-is (no authority that can hold a secret).
        assert_eq!(redact_url_userinfo("localhost:7070"), "localhost:7070");
    }

    #[cfg(feature = "simulator")]
    #[test]
    fn test_simulator_spec_serialisation_redacts_userinfo_credential() {
        use crate::data::simulator::CreateSessionRequest;
        use serde_json::json;

        const SECRET: &str = "SUPERSECRETTOKEN123";
        let spec = DataSourceSpec::Simulator(super::SimulatorSourceSpec {
            session: CreateSessionRequest {
                symbol: "SPX".to_string(),
                steps: 5,
                initial_price: 100.0,
                days_to_expiration: 30.0,
                volatility: 0.2,
                risk_free_rate: 0.03,
                dividend_yield: 0.0,
                method: json!({"GeometricBrownian": {"dt": 0.004, "drift": 0.0, "volatility": 0.2}}),
                time_frame: "Day".to_string(),
                chain_size: Some(15),
                strike_interval: Some(5.0),
                skew_slope: None,
                smile_curve: None,
                spread: Some(0.02),
                seed: Some(42),
            },
            base_url: format!("http://ci-bot:{SECRET}@sim.internal:7070"),
            data_seed: 42,
            tape_sha256: "cafef00d".to_string(),
            simulator_version: None,
        });
        let json = serde_json::to_string(&spec).unwrap_or_default();
        assert!(
            !json.contains(SECRET),
            "the credential must never reach a serialised (manifest) copy: {json}"
        );
        assert!(
            !json.contains('@'),
            "no userinfo separator survives: {json}"
        );
        assert!(
            json.contains("sim.internal:7070"),
            "the host identity is still recorded: {json}"
        );
        assert!(
            json.contains("cafef00d"),
            "the tape identity is still recorded: {json}"
        );
        // The redacted form round-trips to itself (deserialise stays derived).
        let back: DataSourceSpec = match serde_json::from_str(&json) {
            Ok(spec) => spec,
            Err(e) => panic!("redacted simulator spec must round-trip: {e}"),
        };
        let super::DataSourceSpec::Simulator(back) = back else {
            panic!("a simulator spec must round-trip to a simulator spec");
        };
        assert_eq!(back.base_url, "http://sim.internal:7070");
    }
}
