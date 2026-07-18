//! The synthetic feed's session client (feature `simulator`).
//!
//! Migrated from OptionStratBacktest's `ApiClient` and `MarketSimulator` — the
//! one fully-implemented piece of that project
//! ([ADR-0001](../../../docs/adr/0001-migrate-optionstratbacktest-core.md)).
//! Everything in this module is gated behind the `simulator` feature so the
//! default build links neither `reqwest` nor `tokio`.
//!
//! # What lives here
//!
//! - **Wire DTOs** ([`CreateSessionRequest`], [`ChainResponse`], …) defined
//!   **locally** rather than depending on the `optionchain_simulator` server
//!   crate (that crate drags actix-web/utoipa/… into the graph), verified
//!   field-for-field against the upstream checkout at **v0.1.0**
//!   (commit `63eac033efea`, read 2026-07-17;
//!   [docs/specs/optionstratlib.md §7](../../../docs/specs/optionstratlib.md#7-optionchain-simulator-dtos)).
//!   Their `f64` price fields are **wire-only**; the single
//!   `ChainResponse` → `ChainSnapshot` conversion lives in
//!   `src/data/convert.rs` and nowhere else.
//! - **The seed channel (upstream v0.1.0).** `CreateSessionRequest` carries an
//!   optional `seed: Option<u64>`: two sessions created with identical
//!   parameters and the same seed produce the same snapshot sequence, and when
//!   `seed` is omitted the server draws a random one and **echoes the
//!   effective seed back** in [`SessionResponse`]`.parameters.seed` so it can
//!   be recorded. The field is skip-serialized when `None`, so requests stay
//!   byte-compatible with the older seedless (v0.0.2) surface. End-to-end
//!   same-seed reproducibility of a materialised tape is asserted by the
//!   issue #45 closing test, not here.
//! - [`ApiClient`] — the async REST client over `/api/v1/chain`. Per the
//!   v0.1.0 surface, `GET /api/v1/chain?sessionid=` is a **read-only peek**
//!   and the advance is `POST /api/v1/chain/step?sessionid=`
//!   ([`ApiClient::get_next_step`]). Async is confined to this module; the
//!   engine loop never `.await`s.
//! - [`MarketSimulator`] — the session step wrapper, with the two migrated
//!   `MarketSimulator` bugs fixed in flight
//!   ([docs/02 §10](../../../docs/02-engine-architecture.md#10-migrated-scaffold-and-its-known-bugs),
//!   [docs/03 §6.2](../../../docs/03-data-layer.md#62-migrated-bug-fixes)):
//!   1. termination is **derived** from the feed's real state — no session ⇒
//!      terminated (was a hardcoded `false`);
//!   2. session state is **read from what the server returns** each step (the
//!      `ChainResponse.session_info` step counters) — no longer a hardcoded
//!      `InProgress`.
//!
//! - [`SimulatorFeed`] — the materialised-tape [`DataFeed`] (issue #45).
//!   [`SimulatorFeed::open`] drives one whole session to completion on a
//!   current-thread runtime **before the loop exists** — create (sending the
//!   configured data seed) → advance until the server's own `session_info`
//!   reports the session ended → convert every response through the single
//!   conversion boundary — enforcing the three [`ResourceLimits`] tape
//!   ceilings incrementally while draining, deleting the server session on
//!   **every** exit path, and pinning the validated tape's identity as its
//!   canonical `sha256` ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)).
//!   [`DataFeed::next`] is then a pure indexed `Vec` read — no I/O, no
//!   `.await`, nothing blocking in the loop.
//!
//! # Reproducibility (honest)
//!
//! The seed **flows** end to end: materialisation sends
//! `SimulatorSourceSpec::data_seed` as `CreateSessionRequest::seed` and
//! records the **effective** seed the server echoes back. Whether two
//! same-seed sessions materialise **identical tapes** is *server* behaviour:
//! upstream v0.1.0 stamps each `ChainResponse.timestamp` (and renders each
//! relative expiry) from the **wall clock** (verified 2026-07-17 against the
//! sibling checkout), so the full-tape identity claim is still upstream-
//! blocked even though the seeded *walk* (prices) repeats. The SIM_LIVE-gated
//! closing tests in `tests/simulator_live.rs` assert exactly that split, and
//! [`crate::data::FeedKind::is_reproducible`] stays `false` for this feed
//! until the full claim is verified against a live server.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::ResourceLimits;
use crate::data::convert::chain_response_to_snapshot;
use crate::data::feed::{DataFeed, TapeMeta};
use crate::data::historical::{push_checked, to_hex};
use crate::data::{DataSourceSpec, SimulatorSourceSpec};
use crate::domain::{ChainSnapshot, InstrumentSpec, Quantity, SimTime, StepIndex};
use crate::error::BacktestError;

// ---------------------------------------------------------------------------
// Wire DTOs (locally defined; simulator v0.1.0 shape, `f64` prices wire-only)
// ---------------------------------------------------------------------------

/// A request to create a new simulation session (`POST /api/v1/chain`).
///
/// Field types mirror the wire shape in
/// [docs/specs §7](../../../docs/specs/optionstratlib.md#7-optionchain-simulator-dtos):
/// prices and rates are `f64` **on the wire**, and `method` is a tagged walk
/// enum (mirroring `optionstratlib::simulation::WalkType`) carried as raw JSON
/// so this crate need not vendor the full walk taxonomy. This DTO is only ever
/// **serialised outbound**. The optional `seed` is the upstream v0.1.0 seed
/// channel — the server walk is **seeded when this is set** and the effective
/// seed is echoed back in [`SessionResponse`]`.parameters.seed`
/// ([docs/03 §6](../../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSessionRequest {
    /// Underlying ticker symbol.
    pub symbol: String,
    /// Number of discrete steps the session will walk.
    pub steps: usize,
    /// Initial underlying price (wire `f64`).
    pub initial_price: f64,
    /// Days until expiration (wire `f64`).
    pub days_to_expiration: f64,
    /// Annualised volatility (wire `f64`).
    pub volatility: f64,
    /// Annualised risk-free rate (wire `f64`).
    pub risk_free_rate: f64,
    /// Annualised dividend yield (wire `f64`).
    pub dividend_yield: f64,
    /// The tagged walk method (e.g. `{"GeometricBrownian": {dt, drift,
    /// volatility}}`), carried as raw JSON.
    pub method: Value,
    /// The step time frame (e.g. `"Day"`, `"Week"`), a plain wire string.
    pub time_frame: String,
    /// Optional number of strikes in the generated chain.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_size: Option<usize>,
    /// Optional interval between strikes (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strike_interval: Option<f64>,
    /// Optional volatility-skew slope (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skew_slope: Option<f64>,
    /// Optional volatility-smile curvature (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub smile_curve: Option<f64>,
    /// Optional bid-ask spread factor (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spread: Option<f64>,
    /// Optional RNG seed for the server-side walk (upstream v0.1.0). When set,
    /// two sessions with identical parameters and the same seed produce the
    /// same snapshot sequence; when `None` (omitted on the wire, keeping the
    /// request compatible with older servers) the server draws a random seed
    /// and reports the effective value back in
    /// [`SessionResponse`]`.parameters.seed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

/// A partial update to an existing session (`PATCH /api/v1/chain`).
///
/// Every field is optional; absent fields (serialised as omitted) keep their
/// current server value. Upstream v0.1.0 models the optional fields as a
/// tri-state `Patch` (absent = keep, value = replace, `null` = clear /
/// re-seed); this outbound-only mirror deliberately expresses the
/// **keep-or-replace subset** with plain `Option` — an omitted `Option::None`
/// serialises exactly like `Patch::Absent` and a `Some` like `Patch::Value`,
/// while the explicit-`null` "clear" signal is not sent by this client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct UpdateSessionRequest {
    /// Replacement underlying symbol.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Replacement step count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<usize>,
    /// Replacement initial price (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_price: Option<f64>,
    /// Replacement days-to-expiration (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days_to_expiration: Option<f64>,
    /// Replacement volatility (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volatility: Option<f64>,
    /// Replacement risk-free rate (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_free_rate: Option<f64>,
    /// Replacement dividend yield (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dividend_yield: Option<f64>,
    /// Replacement walk method, as raw JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<Value>,
    /// Replacement step time frame.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_frame: Option<String>,
    /// Replacement chain size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_size: Option<usize>,
    /// Replacement strike interval (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strike_interval: Option<f64>,
    /// Replacement skew slope (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skew_slope: Option<f64>,
    /// Replacement smile curve (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub smile_curve: Option<f64>,
    /// Replacement spread factor (wire `f64`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spread: Option<f64>,
    /// Replacement walk seed. `Some` re-seeds the walk with the given value
    /// (upstream `Patch::Value`); `None` keeps the current seed
    /// (`Patch::Absent`). The upstream `null` = "re-seed randomly" signal is
    /// deliberately not expressible here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

/// The session metadata returned by create / replace / update
/// (`POST` / `PUT` / `PATCH`).
///
/// `state` is a **raw wire string** (e.g. `"Initialized"`, `"In Progress"`);
/// [`SessionState::from_wire`] parses it into the typed [`SessionState`]. The
/// echoed `parameters` block is typed ([`SessionParametersResponse`]) because
/// it carries the **effective walk seed** the server actually used — the value
/// a reproducible re-run must record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionResponse {
    /// Server-issued session id (a UUID string).
    pub id: String,
    /// Creation timestamp (wire string).
    pub created_at: String,
    /// Last-update timestamp (wire string).
    pub updated_at: String,
    /// The session's current parameters as echoed by the server, including
    /// the effective walk seed.
    pub parameters: SessionParametersResponse,
    /// The current step index the session is at.
    pub current_step: usize,
    /// The total number of steps in the session.
    pub total_steps: usize,
    /// The server's current session state, as a raw wire string.
    pub state: String,
}

/// The parameters block echoed inside every [`SessionResponse`]
/// (upstream `SessionParametersResponse`, v0.1.0).
///
/// Lenient on deserialize (no `deny_unknown_fields`) so a newer server field
/// never breaks parsing. Note the upstream echo carries **fewer** fields than
/// the request (no `steps` / `days_to_expiration` / `chain_size` /
/// `strike_interval`); the step counters live on the enclosing
/// [`SessionResponse`] instead. The one field downstream code consumes is
/// `seed` — the **effective walk seed** (always populated by a v0.1.0 server,
/// whether the request set one or the server drew one at random).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SessionParametersResponse {
    /// Underlying ticker symbol.
    pub symbol: String,
    /// Initial underlying price (wire `f64`).
    pub initial_price: f64,
    /// Annualised volatility (wire `f64`).
    pub volatility: f64,
    /// Annualised risk-free rate (wire `f64`).
    pub risk_free_rate: f64,
    /// The tagged walk method, carried as raw JSON.
    pub method: Value,
    /// The step time frame, a plain wire string.
    pub time_frame: String,
    /// Annualised dividend yield (wire `f64`).
    pub dividend_yield: f64,
    /// Volatility-skew slope (wire `f64`).
    pub skew_slope: Option<f64>,
    /// Volatility-smile curvature (wire `f64`).
    pub smile_curve: Option<f64>,
    /// Bid-ask spread factor (wire `f64`).
    pub spread: Option<f64>,
    /// The **effective** RNG seed driving the session's walk — the requested
    /// seed when one was sent, otherwise the random seed the server drew.
    pub seed: Option<u64>,
}

/// A single option chain snapshot, returned both by the advance
/// (`POST /api/v1/chain/step?sessionid=`) and the read-only peek
/// (`GET /api/v1/chain?sessionid=`).
///
/// Its `contracts` carry `f64` prices that are **wire-only** — the single
/// conversion into integer-cents types is
/// [`crate::data::chain_response_to_snapshot`] in `src/data/convert.rs`.
/// Note the `session_info` block carries **no state field** (only step
/// counters), which is why the wrapper *derives* the session state from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ChainResponse {
    /// The underlying's identifier.
    pub underlying: String,
    /// Snapshot timestamp (wire string).
    pub timestamp: String,
    /// Current underlying price (wire `f64`).
    pub price: f64,
    /// The option contracts in this snapshot.
    pub contracts: Vec<OptionContractResponse>,
    /// Progress metadata for the session that produced this snapshot.
    pub session_info: SessionInfoResponse,
}

/// One contract row (a strike carrying its call and put) in a [`ChainResponse`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct OptionContractResponse {
    /// Strike price (wire `f64`).
    pub strike: f64,
    /// Expiration instant (wire ISO-8601 string).
    pub expiration: String,
    /// Call-side prices and delta.
    pub call: OptionPriceResponse,
    /// Put-side prices and delta.
    pub put: OptionPriceResponse,
    /// Implied volatility (wire `f64`).
    pub implied_volatility: Option<f64>,
    /// Gamma, shared by call and put (wire `f64`).
    pub gamma: Option<f64>,
}

/// Bid / ask / mid / delta for one side of a contract (all wire `f64`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct OptionPriceResponse {
    /// Bid price (wire `f64`).
    pub bid: Option<f64>,
    /// Ask price (wire `f64`).
    pub ask: Option<f64>,
    /// Mid price (wire `f64`).
    pub mid: Option<f64>,
    /// Delta (wire `f64`).
    pub delta: Option<f64>,
}

/// The progress block embedded in every [`ChainResponse`].
///
/// Crucially, it carries **only** step counters — **no** state field — so the
/// session's state at each step must be *derived* from these counters
/// (bug-fix 2, [docs/03 §6.2](../../../docs/03-data-layer.md#62-migrated-bug-fixes)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SessionInfoResponse {
    /// The session id.
    pub id: String,
    /// The current step index.
    pub current_step: usize,
    /// The total number of steps.
    pub total_steps: usize,
}

/// A structured error body returned by the simulator on a non-2xx response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ErrorResponse {
    /// The server's human-readable error message.
    pub error: String,
}

// ---------------------------------------------------------------------------
// Session state (typed, derived — never fabricated)
// ---------------------------------------------------------------------------

/// The typed session state.
///
/// Parsed from the raw wire string on create / replace / update
/// ([`Self::from_wire`]) and **derived** from the step counters on each advance
/// ([`Self::from_progress`]), because the per-step `ChainResponse.session_info`
/// carries no state field. A wire string the client does not recognise becomes
/// [`SessionState::Unknown`] rather than being guessed at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SessionState {
    /// Freshly created, not yet advanced.
    Initialized = 0,
    /// Actively walking (more steps remain).
    InProgress = 1,
    /// Modified via a partial update.
    Modified = 2,
    /// Reset after a modification or completion.
    Reinitialized = 3,
    /// Reached the final step — terminal.
    Completed = 4,
    /// The server reported an error — terminal.
    Error = 5,
    /// A wire state string the client did not recognise (forward-compat);
    /// treated as non-terminal so the step-count guard still bounds the run.
    Unknown = 6,
}

impl SessionState {
    /// Parse a raw wire state string (case- and space-insensitive).
    ///
    /// The simulator's `Display` yields values such as `"Initialized"` and
    /// `"In Progress"`; both the spaced and the serde-variant (`"InProgress"`)
    /// spellings are accepted. An unrecognised string maps to
    /// [`SessionState::Unknown`].
    #[must_use]
    pub fn from_wire(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().replace(' ', "").as_str() {
            "initialized" => Self::Initialized,
            "inprogress" => Self::InProgress,
            "modified" => Self::Modified,
            "reinitialized" => Self::Reinitialized,
            "completed" => Self::Completed,
            "error" => Self::Error,
            _ => Self::Unknown,
        }
    }

    /// Derive the state from the server's step counters.
    ///
    /// A snapshot at or past the final step is [`SessionState::Completed`];
    /// otherwise the session is still [`SessionState::InProgress`]. This is the
    /// state source for each advance, since the per-step response carries no
    /// state field.
    #[must_use]
    pub const fn from_progress(current_step: usize, total_steps: usize) -> Self {
        if current_step >= total_steps {
            Self::Completed
        } else {
            Self::InProgress
        }
    }

    /// Whether this state is terminal (no further advance is possible).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Error)
    }
}

/// The wrapper's view of the market after the latest session event.
///
/// Holds the derived [`SessionState`] plus the server's step counters and the
/// raw [`ChainResponse`]. The chain's `f64` fields are **wire-only** and are
/// not yet consumed (conversion is issue #7).
#[derive(Debug, Clone, PartialEq)]
pub struct MarketState {
    /// The current step index reported by the server.
    pub current_step: usize,
    /// The total number of steps reported by the server.
    pub total_steps: usize,
    /// The session state — real (parsed on create) or derived (each step).
    pub state: SessionState,
    /// The most recent chain snapshot (raw wire DTO; unconsumed for now).
    pub chain: ChainResponse,
}

// ---------------------------------------------------------------------------
// ApiClient — async REST client (async confined here)
// ---------------------------------------------------------------------------

/// An async REST client for an OptionChain-Simulator session service.
///
/// All methods are `async` and confined to this data adapter; the engine loop
/// never `.await`s. Transport, HTTP-status, and body-parse failures all convert
/// to [`BacktestError::Session`] **at this seam** — a `reqwest::Error` never
/// crosses a public signature.
#[derive(Debug, Clone)]
pub struct ApiClient {
    client: reqwest::Client,
    base_url: String,
    /// The maximum bytes any single response body may buffer. Applied **while
    /// streaming** the body (not after a full buffer), so a hostile or runaway
    /// simulator cannot OOM the client with an unbounded response before a size
    /// ceiling is even consulted.
    max_response_bytes: u64,
}

impl ApiClient {
    /// The conventional default simulator host.
    pub const DEFAULT_BASE_URL: &'static str = "http://localhost:7070";

    /// The default response-body cap when none is configured — mirrors the
    /// default [`ResourceLimits::max_file_bytes`] (4 GiB): a generous anti-OOM
    /// ceiling, not a tight bound. The tape materialiser overrides it with the
    /// run's own `max_file_bytes` via [`Self::with_max_response_bytes`].
    pub const DEFAULT_MAX_RESPONSE_BYTES: u64 = 4 * (1 << 30);

    /// The base URL to use when the caller does not name one: the `API_URL`
    /// environment variable when set, otherwise [`Self::DEFAULT_BASE_URL`].
    #[must_use]
    pub fn base_url_from_env() -> String {
        Self::base_url_from(std::env::var("API_URL").ok())
    }

    /// The pure resolution rule behind [`Self::base_url_from_env`], split out
    /// so it is testable without mutating process-global environment state.
    #[must_use]
    fn base_url_from(api_url: Option<String>) -> String {
        match api_url {
            Some(url) if !url.trim().is_empty() => url,
            _ => Self::DEFAULT_BASE_URL.to_string(),
        }
    }

    /// Build a client for `base_url` with the given request `timeout`.
    ///
    /// A trailing `/` on `base_url` is trimmed so path joins never double up.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Session`] if the underlying HTTP client cannot
    /// be constructed (e.g. the TLS backend fails to initialise). Migrated from
    /// an infallible `-> Self` that `expect`ed on this path; made fallible to
    /// honour the no-panic rule.
    pub fn new(base_url: &str, timeout: Duration) -> Result<Self, BacktestError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| BacktestError::Session(format!("http client build failed: {e}")))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            max_response_bytes: Self::DEFAULT_MAX_RESPONSE_BYTES,
        })
    }

    /// Cap the bytes any single response body may buffer before deserialisation,
    /// reusing the run's `max_file_bytes` resource ceiling. Set by the tape
    /// materialiser from [`ResourceLimits`] so a hostile / runaway simulator
    /// cannot OOM the client with an unbounded body.
    #[must_use = "the reconfigured client must be used"]
    pub fn with_max_response_bytes(mut self, max_response_bytes: u64) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    /// Read a response body **capped while streaming** — the length is checked
    /// chunk by chunk and the read aborts the moment it would exceed
    /// `max_response_bytes`, so an oversized body is a typed
    /// [`BacktestError::Session`] before it is ever fully buffered. The bounded
    /// bytes are then deserialised.
    ///
    /// The deserialise failure keeps the established `"{what} failed: {serde
    /// detail}"` wording (e.g. `"parse chain response failed: …"`) — the streaming
    /// cap changed only *how* the body is buffered, not the error contract, so a
    /// caller matching on that fragment still works.
    async fn read_capped_json<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
        what: &str,
    ) -> Result<T, BacktestError> {
        let bytes = self.read_capped_body(response, what).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| BacktestError::Session(format!("{what} failed: {e}")))
    }

    /// Accumulate a response body into a bounded `Vec`, streaming
    /// [`reqwest::Response::chunk`] by chunk and aborting the moment the running
    /// length would exceed `max_response_bytes` (checked arithmetic, no wrap).
    async fn read_capped_body(
        &self,
        mut response: reqwest::Response,
        what: &str,
    ) -> Result<Vec<u8>, BacktestError> {
        let mut body: Vec<u8> = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| BacktestError::Session(format!("{what}: read body failed: {e}")))?
        {
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or(BacktestError::ArithmeticOverflow)?;
            let next_len =
                u64::try_from(next_len).map_err(|_| BacktestError::ArithmeticOverflow)?;
            if next_len > self.max_response_bytes {
                return Err(BacktestError::Session(format!(
                    "{what}: response body exceeds the {}-byte cap",
                    self.max_response_bytes
                )));
            }
            body.extend_from_slice(chunk.as_ref());
        }
        Ok(body)
    }

    /// Create a new session.
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] on transport failure, a non-2xx status, or an
    /// unparseable response body.
    pub async fn create_session(
        &self,
        params: CreateSessionRequest,
    ) -> Result<SessionResponse, BacktestError> {
        let url = format!("{}/api/v1/chain", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&params)
            .send()
            .await
            .map_err(|e| BacktestError::Session(format!("create_session request failed: {e}")))?;
        if !response.status().is_success() {
            return Err(self.error_from_response(response).await);
        }
        self.read_capped_json::<SessionResponse>(response, "parse session response")
            .await
    }

    /// Advance the session one step and return the served chain snapshot
    /// (`POST /api/v1/chain/step?sessionid=`).
    ///
    /// Per the upstream v0.1.0 surface the plain `GET /api/v1/chain` is a
    /// **read-only peek** that never moves the cursor; the advance is this
    /// dedicated `POST` step resource. This bare form sends no
    /// `expected_step` precondition — see [`Self::get_next_step_expecting`]
    /// for the retry-safe variant the tape materialiser uses.
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] on transport failure, a non-2xx status
    /// (including `410 Gone` once the session has completed), or an
    /// unparseable response body.
    pub async fn get_next_step(&self, session_id: &str) -> Result<ChainResponse, BacktestError> {
        self.get_next_step_expecting(session_id, None).await
    }

    /// Advance the session one step, optionally sending the upstream
    /// `expected_step` precondition (the session's current cursor **before**
    /// this advance; the server answers `412` without advancing on a
    /// mismatch).
    ///
    /// The precondition is what makes a **bounded retry** of this
    /// non-idempotent `POST` safe: if an earlier attempt actually advanced
    /// the session but its response was lost, the retry gets a typed `412`
    /// instead of silently advancing a second time and skipping a snapshot.
    /// The tape materialiser ([`SimulatorFeed::open`]) always sends it.
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] on transport failure, a non-2xx status
    /// (including `412 Precondition Failed` on a cursor mismatch and
    /// `410 Gone` once the session has completed), or an unparseable
    /// response body.
    pub async fn get_next_step_expecting(
        &self,
        session_id: &str,
        expected_step: Option<usize>,
    ) -> Result<ChainResponse, BacktestError> {
        let url = match expected_step {
            Some(expected) => format!(
                "{}/api/v1/chain/step?sessionid={session_id}&expected_step={expected}",
                self.base_url
            ),
            None => format!("{}/api/v1/chain/step?sessionid={session_id}", self.base_url),
        };
        let response =
            self.client.post(&url).send().await.map_err(|e| {
                BacktestError::Session(format!("get_next_step request failed: {e}"))
            })?;
        if !response.status().is_success() {
            return Err(self.error_from_response(response).await);
        }
        self.read_capped_json::<ChainResponse>(response, "parse chain response")
            .await
    }

    /// Replace the session's parameters wholesale (`PUT`).
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] on transport failure, a non-2xx status, or an
    /// unparseable response body.
    pub async fn replace_session(
        &self,
        session_id: &str,
        params: CreateSessionRequest,
    ) -> Result<SessionResponse, BacktestError> {
        let url = format!("{}/api/v1/chain?sessionid={session_id}", self.base_url);
        let response = self
            .client
            .put(&url)
            .json(&params)
            .send()
            .await
            .map_err(|e| BacktestError::Session(format!("replace_session request failed: {e}")))?;
        if !response.status().is_success() {
            return Err(self.error_from_response(response).await);
        }
        self.read_capped_json::<SessionResponse>(response, "parse session response")
            .await
    }

    /// Apply a partial update to the session (`PATCH`).
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] on transport failure, a non-2xx status, or an
    /// unparseable response body.
    pub async fn update_session(
        &self,
        session_id: &str,
        params: UpdateSessionRequest,
    ) -> Result<SessionResponse, BacktestError> {
        let url = format!("{}/api/v1/chain?sessionid={session_id}", self.base_url);
        let response = self
            .client
            .patch(&url)
            .json(&params)
            .send()
            .await
            .map_err(|e| BacktestError::Session(format!("update_session request failed: {e}")))?;
        if !response.status().is_success() {
            return Err(self.error_from_response(response).await);
        }
        self.read_capped_json::<SessionResponse>(response, "parse session response")
            .await
    }

    /// Terminate the session (`DELETE`).
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] on transport failure or a non-2xx status.
    pub async fn delete_session(&self, session_id: &str) -> Result<(), BacktestError> {
        let url = format!("{}/api/v1/chain?sessionid={session_id}", self.base_url);
        let response =
            self.client.delete(&url).send().await.map_err(|e| {
                BacktestError::Session(format!("delete_session request failed: {e}"))
            })?;
        if !response.status().is_success() {
            return Err(self.error_from_response(response).await);
        }
        Ok(())
    }

    /// Build a typed [`BacktestError::Session`] from a non-2xx response,
    /// including the status and (when present) the server's error message.
    #[cold]
    async fn error_from_response(&self, response: reqwest::Response) -> BacktestError {
        let status = response.status().as_u16();
        match self.read_capped_body(response, "error body").await {
            Ok(bytes) => match serde_json::from_slice::<ErrorResponse>(&bytes) {
                Ok(body) => BacktestError::Session(format!("http {status}: {}", body.error)),
                Err(_) => BacktestError::Session(format!("http {status}: unparseable error body")),
            },
            Err(_) => BacktestError::Session(format!(
                "http {status}: error body exceeded the response cap"
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// MarketSimulator — the session step wrapper (both migrated bugs fixed)
// ---------------------------------------------------------------------------

/// The session step wrapper over an [`ApiClient`].
///
/// Owns the current session id and derived [`MarketState`], and exposes the
/// create / advance / terminate lifecycle. Both migrated `MarketSimulator`
/// bugs are fixed here:
///
/// - [`Self::is_terminated`] returns `true` when **no** session exists (the
///   original returned `false`), so termination is derived from real feed state
///   ([docs/02 §10](../../../docs/02-engine-architecture.md#10-migrated-scaffold-and-its-known-bugs)).
/// - [`Self::next_step`] reports the state the server actually implies (derived
///   from `ChainResponse.session_info`), not a hardcoded `InProgress`
///   ([docs/03 §6.2](../../../docs/03-data-layer.md#62-migrated-bug-fixes)).
///
/// The wrapper is intentionally **not** generic over a strategy: the migrated
/// `SimulationEnvironment` / `SimulationConfig` skeleton is out of scope, so the
/// old `<Strategy: PositionableStrategy>` parameter and its `Default` bound are
/// dropped.
#[derive(Debug, Clone)]
pub struct MarketSimulator {
    api_client: Arc<ApiClient>,
    session_id: Option<String>,
    current_state: Option<MarketState>,
}

impl MarketSimulator {
    /// Create a wrapper with no active session.
    #[must_use]
    pub fn new(api_client: Arc<ApiClient>) -> Self {
        Self {
            api_client,
            session_id: None,
            current_state: None,
        }
    }

    /// The active session id, if any.
    #[must_use]
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Record a create/replace [`SessionResponse`] into wrapper state, parsing
    /// the **real** wire state string (never fabricated). Pure — no I/O — so the
    /// state seam is testable against stub responses.
    fn record_session_response(&mut self, resp: &SessionResponse) {
        self.session_id = Some(resp.id.clone());
        self.current_state = Some(MarketState {
            current_step: resp.current_step,
            total_steps: resp.total_steps,
            state: SessionState::from_wire(&resp.state),
            chain: ChainResponse::default(),
        });
    }

    /// Record an advance [`ChainResponse`] into wrapper state, **deriving** the
    /// session state from the server's step counters (bug-fix 2). Pure — no
    /// I/O — so the state seam is testable against stub responses.
    fn record_chain_response(&mut self, resp: &ChainResponse) {
        let info = &resp.session_info;
        self.current_state = Some(MarketState {
            current_step: info.current_step,
            total_steps: info.total_steps,
            state: SessionState::from_progress(info.current_step, info.total_steps),
            chain: resp.clone(),
        });
    }

    /// Create a new simulation session and record its initial state.
    ///
    /// # Errors
    ///
    /// Propagates [`BacktestError::Session`] from the underlying create call.
    pub async fn create_simulation(
        &mut self,
        request: CreateSessionRequest,
    ) -> Result<(), BacktestError> {
        let response = self.api_client.create_session(request).await?;
        self.record_session_response(&response);
        Ok(())
    }

    /// Advance the simulation one step, returning the new [`MarketState`] whose
    /// `state` reflects the server's real progress.
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] if no session is active, or propagated from
    /// the underlying advance call.
    pub async fn next_step(&mut self) -> Result<MarketState, BacktestError> {
        let session_id = self
            .session_id
            .clone()
            .ok_or_else(|| BacktestError::Session("no active simulation session".to_string()))?;
        let response = self.api_client.get_next_step(&session_id).await?;
        self.record_chain_response(&response);
        self.get_current_state()
    }

    /// The current market state.
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] if no session has produced a state yet.
    pub fn get_current_state(&self) -> Result<MarketState, BacktestError> {
        self.current_state
            .clone()
            .ok_or_else(|| BacktestError::Session("no current market state".to_string()))
    }

    /// Whether the simulation has terminated.
    ///
    /// Bug-fix 1: with **no** session the answer is `true` (was `false`), so a
    /// caller can treat an uninitialised wrapper as "nothing to advance". With a
    /// session, termination is derived from the real feed state — a terminal
    /// [`SessionState`] or the step counter reaching the total.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        match &self.current_state {
            Some(state) => state.state.is_terminal() || state.current_step >= state.total_steps,
            None => true,
        }
    }

    /// Terminate the active session (if any) and clear wrapper state.
    ///
    /// # Errors
    ///
    /// Propagates [`BacktestError::Session`] from the underlying delete call.
    pub async fn reset(&mut self) -> Result<(), BacktestError> {
        if let Some(session_id) = &self.session_id {
            self.api_client.delete_session(session_id).await?;
        }
        self.session_id = None;
        self.current_state = None;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SimulatorFeed — the materialised, validated, immutable session tape (#45)
// ---------------------------------------------------------------------------

/// Attempts per materialisation request: one initial call plus two bounded
/// retries. Retrying the step advance is safe because the materialiser always
/// sends the `expected_step` precondition (see
/// [`ApiClient::get_next_step_expecting`]); there is no backoff —
/// materialisation happens once, before the run, and the bound is small.
const MATERIALISE_ATTEMPTS: u32 = 3;

/// Domain tag prefixed to the canonical tape serialisation, so the tape hash
/// can never collide with a hash of the same bytes in another role.
const TAPE_HASH_TAG: &[u8] = b"ironcondor.tape.v1\0";

/// Run one materialisation request with the bounded retry policy: up to
/// [`MATERIALISE_ATTEMPTS`] attempts, each failure logged, the **last** error
/// returned when the bound is exhausted. Retry exists only here, at
/// materialisation time — the replay loop never touches the network.
///
/// An ambiguous failure (the request may have reached the server but its
/// response was lost) can at worst orphan a `create_session` server-side or
/// turn a re-sent advance into a typed `412` (never a silent double-advance,
/// because the materialiser sends `expected_step`).
async fn with_retry<T, F, Fut>(what: &'static str, mut call: F) -> Result<T, BacktestError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, BacktestError>>,
{
    let mut last: Option<BacktestError> = None;
    for attempt in 1..=MATERIALISE_ATTEMPTS {
        match call().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                tracing::warn!(
                    request = what,
                    attempt,
                    max_attempts = MATERIALISE_ATTEMPTS,
                    error = %error,
                    "materialisation request failed"
                );
                last = Some(error);
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        BacktestError::Session(format!("{what}: retry bound exhausted without an error"))
    }))
}

/// Parse one `ChainResponse.timestamp` into a [`SimTime`].
///
/// Used **only** to seed the tape anchor `ts_0` before the first snapshot
/// exists — every stored `ts` still comes from the single conversion boundary
/// (`src/data/convert.rs`), which parses the same RFC-3339 field with the
/// same rule.
fn response_ts(resp: &ChainResponse) -> Result<SimTime, BacktestError> {
    let ts_dt = chrono::DateTime::parse_from_rfc3339(&resp.timestamp).map_err(|e| {
        BacktestError::Conversion(format!(
            "unparseable snapshot timestamp {:?}: {e}",
            resp.timestamp
        ))
    })?;
    let ts_ns = ts_dt.timestamp_nanos_opt().ok_or_else(|| {
        BacktestError::Conversion(format!(
            "snapshot timestamp {:?} is outside the nanosecond range",
            resp.timestamp
        ))
    })?;
    Ok(SimTime::new(ts_ns))
}

/// Whether the session has ended, derived from **real** server state: a
/// terminal wire state (create/replace responses) or the step counters
/// reaching the total (every response). This is the migrated-bug-free
/// termination rule — never hardcoded
/// ([docs/03 §6.2](../../../docs/03-data-layer.md#62-migrated-bug-fixes)).
fn session_ended(state: SessionState, current_step: usize, total_steps: usize) -> bool {
    state.is_terminal() || SessionState::from_progress(current_step, total_steps).is_terminal()
}

/// Compute the tape's pinned data identity: the `sha256` (lowercase hex) of
/// the canonical serialisation of the ordered, validated snapshots.
///
/// The canonical serialisation is a deterministic byte fold over the
/// **converted** values — never the wire text, so JSON whitespace, field
/// order, or float formatting cannot perturb the identity. Layout, after the
/// [`TAPE_HASH_TAG`] domain tag, per snapshot in tape order:
///
/// - `ts` (`i64`), `step` (`u32`) — little-endian;
/// - `underlying` — `u64` length + UTF-8 bytes;
/// - `underlying_price`, `tick_size_cents` (`u64`),
///   `contract_multiplier` (`u32`) — little-endian;
/// - `quotes.len()` (`u64`), then per quote in `BTreeMap` key order: the
///   canonical `contract_id` (`u64` length + bytes,
///   [`crate::domain::ContractKey::to_contract_id`]), `bid` / `ask` / `mid`
///   (`u64`), `bid_size` / `ask_size` (`u32`) — little-endian — and the five
///   analytics as `rust_decimal::Decimal::serialize` (16 bytes each).
///
/// Two sessions whose walks differ in any converted value (or timestamp) get
/// distinct identities; replaying identical responses yields the identical
/// identity.
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] if a contract key still carries an
/// unresolved relative expiry (impossible for a converted snapshot, but
/// propagated rather than unwrapped), or [`BacktestError::ArithmeticOverflow`]
/// if a length does not fit `u64`.
#[must_use = "the tape identity must be used"]
fn tape_sha256(tape: &[ChainSnapshot]) -> Result<String, BacktestError> {
    let mut hasher = Sha256::new();
    hasher.update(TAPE_HASH_TAG);
    for snapshot in tape {
        hasher.update(snapshot.ts.value().to_le_bytes());
        hasher.update(snapshot.step.value().to_le_bytes());
        update_bytes(&mut hasher, snapshot.underlying.as_str().as_bytes())?;
        hasher.update(snapshot.underlying_price.value().to_le_bytes());
        hasher.update(snapshot.spec.tick_size_cents.value().to_le_bytes());
        hasher.update(snapshot.spec.contract_multiplier.to_le_bytes());
        let quote_count =
            u64::try_from(snapshot.quotes.len()).map_err(|_| BacktestError::ArithmeticOverflow)?;
        hasher.update(quote_count.to_le_bytes());
        for (key, view) in &snapshot.quotes {
            update_bytes(&mut hasher, key.to_contract_id()?.as_bytes())?;
            hasher.update(view.bid.value().to_le_bytes());
            hasher.update(view.ask.value().to_le_bytes());
            hasher.update(view.mid.value().to_le_bytes());
            hasher.update(view.bid_size.value().to_le_bytes());
            hasher.update(view.ask_size.value().to_le_bytes());
            hasher.update(view.implied_volatility.serialize());
            hasher.update(view.delta.serialize());
            hasher.update(view.gamma.serialize());
            hasher.update(view.theta.serialize());
            hasher.update(view.vega.serialize());
        }
    }
    Ok(to_hex(&hasher.finalize()))
}

/// Fold one length-prefixed byte string into the tape hash.
fn update_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), BacktestError> {
    let len = u64::try_from(bytes.len()).map_err(|_| BacktestError::ArithmeticOverflow)?;
    hasher.update(len.to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

/// The synthetic-session [`DataFeed`] — a materialised, validated, immutable
/// tape over one OptionChain-Simulator session (issue #45).
///
/// Construct with [`SimulatorFeed::open`]; **all** network work happens
/// there, once, before the loop. [`DataFeed::next`] is a pure in-memory read
/// that never blocks or `.await`s, and returns `None` exactly at the tape's
/// last index — end of stream is where the **server** said the session ended,
/// never a hardcoded count.
#[derive(Debug)]
#[must_use = "a SimulatorFeed does nothing unless its snapshots are consumed via DataFeed::next"]
pub struct SimulatorFeed {
    /// The validated, strictly `ts`-ordered snapshots (the replay tape).
    tape: Vec<ChainSnapshot>,
    /// The next index [`DataFeed::next`] yields.
    cursor: usize,
    /// The pinned tape metadata (identity, non-empty, first ts, final step).
    meta: TapeMeta,
    /// The manifest provenance: the configured spec with the **effective**
    /// walk seed and the materialised tape's `sha256` recorded.
    source: SimulatorSourceSpec,
}

impl SimulatorFeed {
    /// Materialise one simulator session into a validated, immutable tape —
    /// async-once, **before** the loop
    /// ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)).
    ///
    /// Drives the whole session on a private current-thread tokio runtime:
    /// `create_session` (sending `spec.data_seed` as the walk seed) →
    /// `get_next_step` — with the `expected_step` precondition — until the
    /// server's own `session_info` reports the session ended → each
    /// `ChainResponse` converted through the single boundary
    /// ([`chain_response_to_snapshot`]). The wire shape carries neither the
    /// tick grid nor per-quote depth, so the caller supplies `instrument`
    /// and `quote_size` ([docs/03 §7](../../../docs/03-data-layer.md#7-chainresponse--optionchain-conversion));
    /// relative expiries anchor on the tape's first timestamp.
    ///
    /// **Ceilings are enforced incrementally while draining** — the first
    /// crossing aborts immediately (the materialiser does not keep reading):
    /// `max_contracts_per_snapshot` before each conversion, `max_steps` and
    /// the running `max_total_bytes` before each push (the same
    /// `push_checked` accounting the file feeds use). Timestamps must be
    /// **strictly increasing** (gaps allowed and preserved); an empty
    /// session fails to construct. Retry and timeout exist **only** here:
    /// each request gets `timeout` and the bounded `MATERIALISE_ATTEMPTS`
    /// (3-attempt) retry.
    ///
    /// **The server session is deleted on every path** — success, every
    /// error, and the ceiling cut-off. A failed delete never masks the
    /// primary error (it is logged via `tracing::warn!` and, on the success
    /// path, does not fail the open — the tape is already complete and
    /// validated).
    ///
    /// The returned feed's [`DataFeed::meta`] carries the spec with the
    /// **effective** walk seed the server echoed back (`data_seed` and
    /// `session.seed` both record it) and the tape identity in
    /// `tape_sha256`; [`DataFeed::tape_meta`] exposes the same identity as
    /// [`TapeMeta::data_identity`].
    ///
    /// Must not be called from inside an async runtime (the materialiser
    /// blocks on its own current-thread runtime); doing so is a typed
    /// [`BacktestError::Session`], never a panic.
    ///
    /// # Errors
    ///
    /// - [`BacktestError::Session`] — runtime construction, a request that
    ///   still fails after the bounded retry, a `412` cursor mismatch, or
    ///   calling from inside an async runtime.
    /// - [`BacktestError::TapeTooLarge`] — the first crossed `max_steps` /
    ///   `max_contracts_per_snapshot` / `max_total_bytes` ceiling
    ///   (`limit` names the field).
    /// - [`BacktestError::DataOutOfOrder`] — a duplicate or reversed
    ///   snapshot timestamp.
    /// - [`BacktestError::Conversion`] — a snapshot that fails the
    ///   conversion boundary, or an **empty** tape (a session that was
    ///   already ended at creation).
    /// - The tick-alignment / crossed-quote / overflow variants the
    ///   conversion core raises.
    pub fn open(
        spec: &SimulatorSourceSpec,
        instrument: InstrumentSpec,
        quote_size: Quantity,
        timeout: Duration,
        limits: &ResourceLimits,
    ) -> Result<Self, BacktestError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(BacktestError::Session(
                "SimulatorFeed::open must not be called from inside an async runtime; \
                 it blocks on its own current-thread runtime"
                    .to_string(),
            ));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| BacktestError::Session(format!("tokio runtime build failed: {e}")))?;
        runtime.block_on(Self::open_async(
            spec, instrument, quote_size, timeout, limits,
        ))
    }

    /// The async body of [`Self::open`]: create → drain → **always** delete →
    /// pin the identity and provenance.
    async fn open_async(
        spec: &SimulatorSourceSpec,
        instrument: InstrumentSpec,
        quote_size: Quantity,
        timeout: Duration,
        limits: &ResourceLimits,
    ) -> Result<Self, BacktestError> {
        let client =
            ApiClient::new(&spec.base_url, timeout)?.with_max_response_bytes(limits.max_file_bytes);

        // Create the session, sending the configured data seed as the walk
        // seed (the cross-layer contract, docs/03 §6). If this fails there is
        // no session id to clean up.
        let mut request = spec.session.clone();
        request.seed = Some(spec.data_seed);
        let session =
            with_retry("create_session", || client.create_session(request.clone())).await?;
        let session_id = session.id.clone();

        // Drain the session to a tape; then delete the server session on
        // EVERY path — success, conversion/validation errors, and the
        // ceiling cut-off alike ([docs/03 §6.1]). A delete failure is
        // logged, never allowed to mask the primary outcome.
        let drained = Self::drain(&client, &session, instrument, quote_size, limits).await;
        let deleted = with_retry("delete_session", || client.delete_session(&session_id)).await;
        if let Err(delete_error) = &deleted {
            tracing::warn!(
                session_id = %session_id,
                error = %delete_error,
                materialisation_failed = drained.is_err(),
                "delete_session failed; the server session may be leaked"
            );
        }
        let tape = drained?;
        if tape.is_empty() {
            return Err(BacktestError::Conversion(format!(
                "simulator session {session_id} produced an empty tape; \
                 an empty tape fails to construct"
            )));
        }

        // Pin the tape identity (the run_id data identity) and validate the
        // tape shape once more through the shared core.
        let sha256 = tape_sha256(&tape)?;
        let meta = TapeMeta::from_tape(sha256.clone(), &tape)?;

        // Record the provenance: the EFFECTIVE walk seed the server echoed
        // back (equal to the configured seed on a v0.1.0 server; the honest
        // value either way) and the materialised tape's sha256.
        let mut source = spec.clone();
        source.session.seed = Some(spec.data_seed);
        match session.parameters.seed {
            Some(effective) => {
                if effective != spec.data_seed {
                    tracing::warn!(
                        configured = spec.data_seed,
                        effective,
                        "simulator echoed a different effective walk seed; recording the effective value"
                    );
                }
                source.data_seed = effective;
                source.session.seed = Some(effective);
            }
            None => {
                tracing::warn!(
                    configured = spec.data_seed,
                    "simulator echoed no effective walk seed (pre-v0.1.0 server?); the \
                     configured data_seed cannot be claimed to have driven the walk"
                );
            }
        }
        source.tape_sha256 = sha256;

        Ok(Self {
            tape,
            cursor: 0,
            meta,
            source,
        })
    }

    /// Advance the session until the **server** reports it ended, converting
    /// and validating each response into the tape.
    ///
    /// Termination derives from real state only (never hardcoded): the
    /// create response's wire state and, per advance, the
    /// `ChainResponse.session_info` counters — the two migrated
    /// `MarketSimulator` bugs stay fixed here
    /// ([docs/03 §6.2](../../../docs/03-data-layer.md#62-migrated-bug-fixes)).
    async fn drain(
        client: &ApiClient,
        session: &SessionResponse,
        instrument: InstrumentSpec,
        quote_size: Quantity,
        limits: &ResourceLimits,
    ) -> Result<Vec<ChainSnapshot>, BacktestError> {
        let mut tape: Vec<ChainSnapshot> = Vec::new();
        let mut total_bytes: u64 = 0;
        let mut anchor_ts: Option<SimTime> = None;
        let mut prev_ts: Option<SimTime> = None;

        // Initial state from the create response: the REAL wire state plus
        // the step counters (a session created already-ended yields no steps
        // and therefore an empty tape, rejected by the caller).
        let mut ended = session_ended(
            SessionState::from_wire(&session.state),
            session.current_step,
            session.total_steps,
        );
        // The cursor the next advance expects — the retry-safe precondition.
        let mut expected_step = session.current_step;

        while !ended {
            let resp = with_retry("get_next_step", || {
                client.get_next_step_expecting(&session.id, Some(expected_step))
            })
            .await?;

            // Ceiling: contracts per snapshot, BEFORE the conversion builds
            // anything (one DTO contract row becomes a call and a put quote).
            let quote_count = u64::try_from(resp.contracts.len())
                .map_err(|_| BacktestError::ArithmeticOverflow)?
                .checked_mul(2)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            let contracts_cap = u64::from(limits.max_contracts_per_snapshot);
            if quote_count > contracts_cap {
                return Err(BacktestError::TapeTooLarge {
                    limit: "max_contracts_per_snapshot",
                    value: quote_count,
                    cap: contracts_cap,
                });
            }

            // The 0-based tape ordinal (bounded by max_steps ≤ 10M, so the
            // u32 conversion cannot fail before the ceiling fires).
            let step = StepIndex::new(
                u32::try_from(tape.len()).map_err(|_| BacktestError::ArithmeticOverflow)?,
            );

            // The tape anchor ts_0: the FIRST snapshot's own timestamp.
            let anchor = match anchor_ts {
                Some(existing) => existing,
                None => {
                    let first = response_ts(&resp)?;
                    anchor_ts = Some(first);
                    first
                }
            };

            // The single conversion boundary — never a second path.
            let snapshot = chain_response_to_snapshot(&resp, step, instrument, anchor, quote_size)?;

            // Strictly increasing timestamps, checked incrementally so a bad
            // session is cut off without draining the rest (gaps are allowed
            // and preserved — never fabricated over).
            if let Some(prev) = prev_ts
                && snapshot.ts <= prev
            {
                return Err(BacktestError::DataOutOfOrder {
                    step: step.value(),
                    ts: snapshot.ts.value(),
                    prev: prev.value(),
                });
            }
            prev_ts = Some(snapshot.ts);

            // Ceilings: max_steps after this advance BEFORE the push, and the
            // running max_total_bytes — the shared accounting the file feeds
            // use, so the first crossing aborts identically.
            push_checked(&mut tape, snapshot, &mut total_bytes, limits)?;

            // Termination from the server's own counters (bug-fix 2).
            let info = &resp.session_info;
            ended = SessionState::from_progress(info.current_step, info.total_steps).is_terminal();
            expected_step = info.current_step;
        }

        Ok(tape)
    }
}

impl DataFeed for SimulatorFeed {
    fn next(&mut self) -> Result<Option<ChainSnapshot>, BacktestError> {
        match self.tape.get(self.cursor) {
            Some(snapshot) => {
                // `get` matched, so `cursor < tape.len() <= isize::MAX` — the
                // increment cannot overflow; plain `+= 1` keeps the codebase's
                // no-saturating/wrapping convention.
                self.cursor += 1;
                Ok(Some(snapshot.clone()))
            }
            None => Ok(None),
        }
    }

    fn meta(&self) -> DataSourceSpec {
        DataSourceSpec::Simulator(self.source.clone())
    }

    fn tape_meta(&self) -> &TapeMeta {
        &self.meta
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ApiClient, ChainResponse, CreateSessionRequest, MarketSimulator, OptionContractResponse,
        OptionPriceResponse, SessionInfoResponse, SessionParametersResponse, SessionResponse,
        SessionState,
    };
    use crate::error::BacktestError;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    fn stub_sim() -> MarketSimulator {
        // Building a reqwest client is offline and does not connect. A build
        // failure is unrecoverable for the test, so fail it explicitly — the
        // same `panic!`-in-match style the config tests use, not unwrap/expect.
        match ApiClient::new(ApiClient::DEFAULT_BASE_URL, Duration::from_secs(5)) {
            Ok(client) => MarketSimulator::new(Arc::new(client)),
            Err(e) => panic!("reqwest client must build offline: {e}"),
        }
    }

    fn created(state: &str, current: usize, total: usize) -> SessionResponse {
        SessionResponse {
            id: "11111111-1111-1111-1111-111111111111".to_string(),
            created_at: "2026-07-15T00:00:00Z".to_string(),
            updated_at: "2026-07-15T00:00:00Z".to_string(),
            parameters: SessionParametersResponse {
                symbol: "SPX".to_string(),
                seed: Some(42),
                ..SessionParametersResponse::default()
            },
            current_step: current,
            total_steps: total,
            state: state.to_string(),
        }
    }

    fn advanced(current: usize, total: usize) -> ChainResponse {
        ChainResponse {
            underlying: "SPX".to_string(),
            timestamp: "2026-07-15T00:00:01Z".to_string(),
            price: 100.0,
            contracts: Vec::new(),
            session_info: SessionInfoResponse {
                id: "11111111-1111-1111-1111-111111111111".to_string(),
                current_step: current,
                total_steps: total,
            },
        }
    }

    // ---- Bug-fix 1: no session ⇒ terminated (was `false`) ------------------

    #[test]
    fn test_is_terminated_true_with_no_session() {
        let sim = stub_sim();
        assert!(
            sim.is_terminated(),
            "no session must report terminated so termination derives from real feed state"
        );
    }

    // ---- Bug-fix 2: state is read/derived from the server, not hardcoded ---

    #[test]
    fn test_step_reads_real_session_state() {
        let mut sim = stub_sim();

        // create → the server's real "Initialized" state.
        sim.record_session_response(&created("Initialized", 0, 2));
        let s0 = sim.get_current_state();
        assert!(matches!(s0, Ok(ref s) if s.state == SessionState::Initialized));
        assert!(!sim.is_terminated(), "a fresh session is not terminated");

        // advance to step 1 of 2 → derived InProgress.
        sim.record_chain_response(&advanced(1, 2));
        let s1 = sim.get_current_state();
        assert!(matches!(s1, Ok(ref s) if s.state == SessionState::InProgress));
        assert!(!sim.is_terminated());

        // advance to step 2 of 2 → derived Completed (the hardcoded-InProgress
        // bug would wrongly report InProgress and never terminate).
        sim.record_chain_response(&advanced(2, 2));
        let s2 = sim.get_current_state();
        assert!(matches!(s2, Ok(ref s) if s.state == SessionState::Completed));
        assert!(sim.is_terminated(), "reaching the final step terminates");
    }

    #[tokio::test]
    async fn test_next_step_without_session_is_session_error() {
        let mut sim = stub_sim();
        // The no-session guard fires before any HTTP is attempted, so this
        // needs no live server.
        let result = sim.next_step().await;
        assert!(matches!(result, Err(BacktestError::Session(_))));
    }

    #[tokio::test]
    async fn test_response_body_cap_rejects_oversized_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // A scripted responder on an ephemeral port answers a create_session
        // POST with a 200 whose JSON body is far larger than the client cap. The
        // capped streaming read must abort with a typed Session error before the
        // whole body is buffered — an oversized body cannot OOM the client.
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(e) => panic!("bind failed: {e}"),
        };
        let addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(e) => panic!("local_addr failed: {e}"),
        };

        let server = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                // Best-effort drain of the request; we need not read all of it
                // to answer.
                let mut scratch = [0u8; 1024];
                let _ = socket.read(&mut scratch).await;
                let body = vec![b'x'; 4096]; // 4 KiB — well past the 64-byte cap.
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(header.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.flush().await;
            }
        });

        let base_url = format!("http://{addr}");
        let client = match ApiClient::new(&base_url, Duration::from_secs(5)) {
            Ok(client) => client.with_max_response_bytes(64),
            Err(e) => panic!("client build failed: {e}"),
        };
        let request = CreateSessionRequest {
            symbol: "SPX".to_string(),
            steps: 1,
            initial_price: 100.0,
            days_to_expiration: 30.0,
            volatility: 0.2,
            risk_free_rate: 0.03,
            dividend_yield: 0.0,
            method: json!({"Brownian": {"dt": 0.004, "drift": 0.0, "volatility": 0.2}}),
            time_frame: "Day".to_string(),
            chain_size: None,
            strike_interval: None,
            skew_slope: None,
            smile_curve: None,
            spread: None,
            seed: None,
        };
        let result = client.create_session(request).await;
        assert!(
            matches!(result, Err(BacktestError::Session(_))),
            "an oversized response body must be a typed Session error, got {result:?}"
        );
        let _ = server.await;
    }

    // ---- SessionState parsing / derivation --------------------------------

    #[test]
    fn test_session_state_from_wire_parses_known_spellings() {
        assert_eq!(
            SessionState::from_wire("Initialized"),
            SessionState::Initialized
        );
        assert_eq!(
            SessionState::from_wire("In Progress"),
            SessionState::InProgress
        );
        assert_eq!(
            SessionState::from_wire("InProgress"),
            SessionState::InProgress
        );
        assert_eq!(
            SessionState::from_wire("Completed"),
            SessionState::Completed
        );
        assert_eq!(SessionState::from_wire("Error"), SessionState::Error);
        assert_eq!(
            SessionState::from_wire("weird-new-state"),
            SessionState::Unknown
        );
    }

    #[test]
    fn test_session_state_from_progress_and_terminality() {
        assert_eq!(SessionState::from_progress(1, 3), SessionState::InProgress);
        assert_eq!(SessionState::from_progress(3, 3), SessionState::Completed);
        assert!(SessionState::Completed.is_terminal());
        assert!(SessionState::Error.is_terminal());
        assert!(!SessionState::InProgress.is_terminal());
        assert!(!SessionState::Unknown.is_terminal());
    }

    // ---- DTO serde round-trips --------------------------------------------

    #[test]
    fn test_create_session_request_serde_round_trip() {
        let req = CreateSessionRequest {
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
            seed: None,
        };
        let text = serde_json::to_string(&req).unwrap_or_default();
        assert!(text.contains("\"GeometricBrownian\""));
        assert!(
            !text.contains("skew_slope"),
            "None optional fields are omitted"
        );
        assert!(
            !text.contains("seed"),
            "an unset seed is omitted on the wire, keeping the request \
             compatible with older seedless servers"
        );
        let back: Result<CreateSessionRequest, _> = serde_json::from_str(&text);
        assert!(matches!(back, Ok(ref r) if *r == req));
    }

    #[test]
    fn test_create_session_request_seed_serialised_when_set() {
        let req = CreateSessionRequest {
            symbol: "SPX".to_string(),
            steps: 3,
            initial_price: 100.0,
            days_to_expiration: 30.0,
            volatility: 0.2,
            risk_free_rate: 0.03,
            dividend_yield: 0.0,
            method: json!({"Brownian": {"dt": 0.004, "drift": 0.0, "volatility": 0.2}}),
            time_frame: "Day".to_string(),
            chain_size: None,
            strike_interval: None,
            skew_slope: None,
            smile_curve: None,
            spread: None,
            seed: Some(1_234_567),
        };
        let text = serde_json::to_string(&req).unwrap_or_default();
        assert!(
            text.contains("\"seed\":1234567"),
            "a set data seed travels on the wire: {text}"
        );
        let back: Result<CreateSessionRequest, _> = serde_json::from_str(&text);
        assert!(matches!(back, Ok(ref r) if r.seed == Some(1_234_567)));
    }

    #[test]
    fn test_base_url_from_resolution_rule() {
        assert_eq!(
            ApiClient::base_url_from(None),
            ApiClient::DEFAULT_BASE_URL,
            "unset API_URL falls back to the conventional default"
        );
        assert_eq!(
            ApiClient::base_url_from(Some(String::new())),
            ApiClient::DEFAULT_BASE_URL,
            "an empty API_URL falls back to the conventional default"
        );
        assert_eq!(
            ApiClient::base_url_from(Some("http://sim.internal:9090".to_string())),
            "http://sim.internal:9090"
        );
    }

    #[test]
    fn test_chain_response_serde_round_trip() {
        let resp = ChainResponse {
            underlying: "SPX".to_string(),
            timestamp: "2026-07-15T00:00:00Z".to_string(),
            price: 4321.5,
            contracts: vec![OptionContractResponse {
                strike: 4300.0,
                expiration: "2026-08-15T00:00:00Z".to_string(),
                call: OptionPriceResponse {
                    bid: Some(12.0),
                    ask: Some(12.5),
                    mid: Some(12.25),
                    delta: Some(0.55),
                },
                put: OptionPriceResponse::default(),
                implied_volatility: Some(0.19),
                gamma: Some(0.001),
            }],
            session_info: SessionInfoResponse {
                id: "s".to_string(),
                current_step: 1,
                total_steps: 5,
            },
        };
        let text = serde_json::to_string(&resp).unwrap_or_default();
        let back: Result<ChainResponse, _> = serde_json::from_str(&text);
        assert!(matches!(back, Ok(ref r) if *r == resp));
    }

    #[test]
    fn test_session_response_deserialises_raw_state_string() {
        let text = r#"{
            "id": "abc",
            "created_at": "t0",
            "updated_at": "t0",
            "parameters": {
                "symbol": "SPX",
                "initial_price": 100.0,
                "volatility": 0.2,
                "risk_free_rate": 0.03,
                "method": {"Brownian": {"dt": 0.004, "drift": 0.0, "volatility": 0.2}},
                "time_frame": "Day",
                "dividend_yield": 0.0,
                "skew_slope": null,
                "smile_curve": null,
                "spread": 0.02,
                "seed": 42
            },
            "current_step": 0,
            "total_steps": 10,
            "state": "In Progress"
        }"#;
        let parsed: Result<SessionResponse, _> = serde_json::from_str(text);
        assert!(matches!(parsed, Ok(ref r) if r.state == "In Progress"));
        if let Ok(r) = parsed {
            assert_eq!(SessionState::from_wire(&r.state), SessionState::InProgress);
            assert_eq!(
                r.parameters.seed,
                Some(42),
                "the effective walk seed is read back from the echoed parameters"
            );
        }
    }

    #[test]
    fn test_session_parameters_lenient_to_unknown_and_missing_optionals() {
        // Responses stay lenient: a newer server field must never break
        // parsing, and absent optional fields deserialise to None.
        let text = r#"{
            "symbol": "SPX",
            "initial_price": 100.0,
            "volatility": 0.2,
            "risk_free_rate": 0.03,
            "method": "Brownian",
            "time_frame": "Day",
            "dividend_yield": 0.0,
            "some_future_field": true
        }"#;
        let parsed: Result<SessionParametersResponse, _> = serde_json::from_str(text);
        match parsed {
            Ok(p) => {
                assert_eq!(p.symbol, "SPX");
                assert_eq!(p.seed, None, "a seedless echo parses to None");
                assert_eq!(p.spread, None);
            }
            Err(e) => panic!("a response with an unknown field must parse: {e}"),
        }
    }
}

#[cfg(test)]
mod tape_tests {
    use optionstratlib::{ExpirationDate, OptionStyle};

    use super::{SessionState, session_ended, tape_sha256};
    use crate::data::convert::{RawQuote, SnapshotMeta, raw_quotes_to_snapshot};
    use crate::domain::{ChainSnapshot, PriceCents, Quantity, SimTime, StepIndex, Underlying};

    const TS0: i64 = 1_784_073_601_000_000_000;

    /// One validated snapshot at `step`, built through the single conversion
    /// core (the same path materialisation uses), with `bid` cents at the one
    /// quoted call.
    fn snapshot(step: u32, ts_offset_ns: i64, bid: u64) -> ChainSnapshot {
        let underlying = match Underlying::new("SPX") {
            Ok(u) => u,
            Err(e) => panic!("SPX must be a valid underlying: {e}"),
        };
        let bid_size = match Quantity::new(10) {
            Ok(q) => q,
            Err(e) => panic!("10 must be a valid quantity: {e}"),
        };
        let quote = RawQuote {
            expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(
                TS0 + 30 * 86_400_000_000_000,
            )),
            strike: PriceCents::new(430_000),
            style: OptionStyle::Call,
            bid: PriceCents::new(bid),
            ask: PriceCents::new(1_250),
            bid_size,
            ask_size: bid_size,
            implied_volatility: 0.19,
            delta: 0.55,
            gamma: 0.001,
            theta: 0.0,
            vega: 0.0,
        };
        let meta = SnapshotMeta {
            ts: SimTime::new(TS0 + ts_offset_ns),
            step: StepIndex::new(step),
            anchor_ts: SimTime::new(TS0),
            underlying,
            underlying_price: PriceCents::new(431_025),
            tick_size_cents: PriceCents::new(5),
            contract_multiplier: 100,
        };
        match raw_quotes_to_snapshot(&meta, &[quote]) {
            Ok(s) => s,
            Err(e) => panic!("conversion must succeed: {e}"),
        }
    }

    #[test]
    fn test_tape_sha256_stable_for_identical_tapes() {
        let tape_a = vec![snapshot(0, 0, 1_200), snapshot(1, 1_000_000_000, 1_205)];
        let tape_b = vec![snapshot(0, 0, 1_200), snapshot(1, 1_000_000_000, 1_205)];
        let sha_a = match tape_sha256(&tape_a) {
            Ok(s) => s,
            Err(e) => panic!("hashing must succeed: {e}"),
        };
        let sha_b = match tape_sha256(&tape_b) {
            Ok(s) => s,
            Err(e) => panic!("hashing must succeed: {e}"),
        };
        assert_eq!(
            sha_a, sha_b,
            "the identical ordered tape hashes identically"
        );
        assert_eq!(sha_a.len(), 64, "sha256 is 64 lowercase hex chars");
        assert!(sha_a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_tape_sha256_sensitive_to_any_converted_value() {
        let base = vec![snapshot(0, 0, 1_200), snapshot(1, 1_000_000_000, 1_205)];
        let bid_changed = vec![snapshot(0, 0, 1_195), snapshot(1, 1_000_000_000, 1_205)];
        let ts_changed = vec![snapshot(0, 0, 1_200), snapshot(1, 2_000_000_000, 1_205)];
        let sha = |tape: &[ChainSnapshot]| match tape_sha256(tape) {
            Ok(s) => s,
            Err(e) => panic!("hashing must succeed: {e}"),
        };
        assert_ne!(
            sha(&base),
            sha(&bid_changed),
            "a differing quoted value must yield a distinct identity"
        );
        assert_ne!(
            sha(&base),
            sha(&ts_changed),
            "a differing timestamp must yield a distinct identity"
        );
    }

    #[test]
    fn test_session_ended_derives_from_wire_state_and_counters() {
        // A fresh session with steps ahead is not ended.
        assert!(!session_ended(SessionState::Initialized, 0, 3));
        // The counters end the session even under a non-terminal wire state.
        assert!(session_ended(SessionState::Initialized, 0, 0));
        assert!(session_ended(SessionState::InProgress, 3, 3));
        // A terminal wire state ends it regardless of the counters.
        assert!(session_ended(SessionState::Completed, 0, 3));
        assert!(session_ended(SessionState::Error, 0, 3));
        // An unknown forward-compat state stays non-terminal; the counters
        // (and the max_steps ceiling) still bound the drain.
        assert!(!session_ended(SessionState::Unknown, 1, 3));
    }
}
