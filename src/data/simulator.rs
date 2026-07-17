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
//! The first-classed materialised-tape `SimulatorFeed` (async materialisation,
//! ceilings, tape hashing) is v0.5 work (issue #45); this module lands the
//! client and the bug-fixed wrapper it builds on.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
}

impl ApiClient {
    /// The conventional default simulator host.
    pub const DEFAULT_BASE_URL: &'static str = "http://localhost:7070";

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
        })
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
            return Err(Self::error_from_response(response).await);
        }
        response
            .json::<SessionResponse>()
            .await
            .map_err(|e| BacktestError::Session(format!("parse session response failed: {e}")))
    }

    /// Advance the session one step and return the served chain snapshot
    /// (`POST /api/v1/chain/step?sessionid=`).
    ///
    /// Per the upstream v0.1.0 surface the plain `GET /api/v1/chain` is a
    /// **read-only peek** that never moves the cursor; the advance is this
    /// dedicated `POST` step resource. The server also accepts an optional
    /// `expected_step` precondition (412 on mismatch) for ambiguous-retry
    /// resolution; this client does not retry, so it does not send one.
    ///
    /// # Errors
    ///
    /// [`BacktestError::Session`] on transport failure, a non-2xx status
    /// (including `410 Gone` once the session has completed), or an
    /// unparseable response body.
    pub async fn get_next_step(&self, session_id: &str) -> Result<ChainResponse, BacktestError> {
        let url = format!("{}/api/v1/chain/step?sessionid={session_id}", self.base_url);
        let response =
            self.client.post(&url).send().await.map_err(|e| {
                BacktestError::Session(format!("get_next_step request failed: {e}"))
            })?;
        if !response.status().is_success() {
            return Err(Self::error_from_response(response).await);
        }
        response
            .json::<ChainResponse>()
            .await
            .map_err(|e| BacktestError::Session(format!("parse chain response failed: {e}")))
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
            return Err(Self::error_from_response(response).await);
        }
        response
            .json::<SessionResponse>()
            .await
            .map_err(|e| BacktestError::Session(format!("parse session response failed: {e}")))
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
            return Err(Self::error_from_response(response).await);
        }
        response
            .json::<SessionResponse>()
            .await
            .map_err(|e| BacktestError::Session(format!("parse session response failed: {e}")))
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
            return Err(Self::error_from_response(response).await);
        }
        Ok(())
    }

    /// Build a typed [`BacktestError::Session`] from a non-2xx response,
    /// including the status and (when present) the server's error message.
    #[cold]
    async fn error_from_response(response: reqwest::Response) -> BacktestError {
        let status = response.status().as_u16();
        match response.json::<ErrorResponse>().await {
            Ok(body) => BacktestError::Session(format!("http {status}: {}", body.error)),
            Err(_) => BacktestError::Session(format!("http {status}: unparseable error body")),
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
