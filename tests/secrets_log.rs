//! Captured-log credential test (issue #53, feature `simulator`;
//! [docs/TESTING.md §12.3](../docs/TESTING.md#123-captured-log-credential-test),
//! [docs/07 §9](../docs/07-performance-and-security.md#9-secrets-handling)).
//!
//! `ironcondor` holds no long-lived secret in its core — the one place a
//! credential can enter is the optional OptionChain-Simulator client. The
//! **current** `simulator` surface (frozen at #49) exposes **no auth-token
//! field**: the sole way a credential reaches the client is as URL userinfo
//! (`http://user:token@host`), which `reqwest` turns into a transport
//! `Authorization` header. This test drives a **dummy credential** through that
//! channel across a full materialise → provenance → manifest path (and a
//! failure path), captures **every** `tracing` event, the resulting manifest
//! provenance, and the returned error, and asserts the credential substring
//! appears **nowhere** — not in a log, not in a manifest copy, not in a
//! `run_id`, not in an error.
//!
//! # Constraint for a future auth knob
//!
//! Should a dedicated auth-token config field ever be added to the simulator
//! surface, it MUST extend this test: drive the token through the new field and
//! re-assert the same non-leak properties. The redaction proven here is
//! specific to the *URL-userinfo* channel that is the only credential path on
//! the current frozen surface.
#![cfg(feature = "simulator")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ironcondor::{
    BacktestConfig, CreateSessionRequest, DataFeed, DataSourceSpec, InstrumentSpec, PriceCents,
    Quantity, ResourceLimits, RunId, SimulatorFeed, SimulatorSourceSpec,
};

#[path = "common/mod.rs"]
mod common;

/// The dummy credential driven through every path; it must surface nowhere.
const SECRET: &str = "SUPERSECRETTOKEN123";

// The committed v0.1.0-shape wire fixtures (shared with `simulator_offline.rs`).
const CREATE_FIXTURE: &str = include_str!("fixtures/simulator/create.json");
const STEP_FIXTURES: [&str; 3] = [
    include_str!("fixtures/simulator/step_1.json"),
    include_str!("fixtures/simulator/step_2.json"),
    include_str!("fixtures/simulator/step_3.json"),
];

// ---------------------------------------------------------------------------
// A minimal zero-dependency scripted HTTP responder (the same approach as
// `simulator_offline.rs`): one canned response per accepted connection, in
// order. It never inspects the `Authorization` header — the credential's only
// on-wire home — so nothing it does can echo the secret back.
// ---------------------------------------------------------------------------

struct ScriptedServer {
    addr: String,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ScriptedServer {
    fn spawn(responses: Vec<(u16, &'static str, String)>) -> Self {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(l) => l,
            Err(e) => panic!("scripted server must bind an ephemeral port: {e}"),
        };
        let addr = match listener.local_addr() {
            Ok(a) => a.to_string(),
            Err(e) => panic!("scripted server must report its address: {e}"),
        };
        let handle = std::thread::spawn(move || {
            for (status, reason, body) in responses {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                drain_request(&mut stream);
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                // A lost client is part of some scripts, not a failure here.
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Self {
            addr,
            handle: Some(handle),
        }
    }

    /// The `http://user:token@host:port` base URL the client connects to.
    fn credentialed_base_url(&self) -> String {
        format!("http://ci-bot:{SECRET}@{}", self.addr)
    }

    fn join(mut self) {
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            panic!("scripted server thread panicked");
        }
    }
}

/// Read (and discard) one HTTP request head so the client's write completes,
/// with a hard size ceiling so the reader never blocks unbounded.
fn drain_request(stream: &mut std::net::TcpStream) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 64 * 1024 {
                    return;
                }
            }
        }
    }
}

/// The full recorded session: create → 3 steps → delete.
fn full_session_script() -> Vec<(u16, &'static str, String)> {
    vec![
        (201, "Created", CREATE_FIXTURE.to_string()),
        (200, "OK", STEP_FIXTURES[0].to_string()),
        (200, "OK", STEP_FIXTURES[1].to_string()),
        (200, "OK", STEP_FIXTURES[2].to_string()),
        (200, "OK", "\"Session deleted\"".to_string()),
    ]
}

// ---------------------------------------------------------------------------
// A minimal zero-dependency `tracing` capture subscriber: it records the level,
// target, and every field of every event into a shared buffer. Implementing
// only `record_debug` captures all field types (the other `Visit` methods
// default to it) and both `%value` (Display) and `?value` (Debug) fields.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<String>>>);

impl CapturedLog {
    fn lines(&self) -> Vec<String> {
        match self.0.lock() {
            Ok(buf) => buf.clone(),
            Err(e) => panic!("captured-log mutex poisoned: {e}"),
        }
    }

    /// Every captured line joined — the whole log as one haystack.
    fn joined(&self) -> String {
        self.lines().join("\n")
    }
}

struct FieldCollector<'a>(&'a mut String);

impl tracing::field::Visit for FieldCollector<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, " {}={value:?}", field.name());
    }
}

struct CapturingSubscriber(CapturedLog);

impl tracing::Subscriber for CapturingSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        // A capture-only subscriber does not track spans; a fixed non-zero id
        // is sufficient (events, not spans, carry the fields under test).
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        use std::fmt::Write;
        let mut line = String::new();
        let meta = event.metadata();
        let _ = write!(line, "{} {}", meta.level(), meta.target());
        event.record(&mut FieldCollector(&mut line));
        if let Ok(mut buf) = self.0.0.lock() {
            buf.push(line);
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Run `f` with the capturing subscriber installed as the thread-local default,
/// returning `f`'s result and the captured log. The simulator materialiser
/// blocks on its own current-thread runtime on THIS thread, so every event it
/// emits is captured.
fn with_captured_log<T>(f: impl FnOnce() -> T) -> (T, CapturedLog) {
    let captured = CapturedLog::default();
    let subscriber = CapturingSubscriber(captured.clone());
    let result = tracing::subscriber::with_default(subscriber, f);
    (result, captured)
}

// ---------------------------------------------------------------------------
// Fixtures matching the committed wire shape (5-cent / 100x grid, seed 42).
// ---------------------------------------------------------------------------

fn create_request() -> CreateSessionRequest {
    CreateSessionRequest {
        symbol: "SPX".to_string(),
        steps: 3,
        initial_price: 4300.0,
        days_to_expiration: 30.0,
        volatility: 0.2,
        risk_free_rate: 0.03,
        dividend_yield: 0.0,
        method: serde_json::json!({"GeometricBrownian": {"dt": 0.004, "drift": 0.0, "volatility": 0.2}}),
        time_frame: "Day".to_string(),
        chain_size: Some(15),
        strike_interval: Some(5.0),
        skew_slope: None,
        smile_curve: None,
        spread: Some(0.02),
        seed: Some(42),
    }
}

fn simulator_spec(base_url: String) -> SimulatorSourceSpec {
    SimulatorSourceSpec {
        session: create_request(),
        base_url,
        data_seed: 42,
        tape_sha256: String::new(),
        simulator_version: None,
    }
}

fn instrument() -> InstrumentSpec {
    match InstrumentSpec::new(PriceCents::new(5), 100) {
        Ok(spec) => spec,
        Err(e) => panic!("5c tick / 100x multiplier must be a valid spec: {e}"),
    }
}

fn quote_size() -> Quantity {
    match Quantity::new(10) {
        Ok(q) => q,
        Err(e) => panic!("10 must be a valid quantity: {e}"),
    }
}

/// A `BacktestConfig` whose data source is a simulator session at `base_url`
/// (the config copy the manifest embeds under its `config` field).
fn simulator_config(base_url: String) -> BacktestConfig {
    let mut config = common::condor_config(Path::new("unused-for-serialisation"), 42);
    config.data_source = DataSourceSpec::Simulator(simulator_spec(base_url));
    config
}

// ---------------------------------------------------------------------------
// Success path: a full materialisation authenticates via the URL, and no
// recorded copy — the manifest `data_source` (feed provenance), the nested
// `config.data_source`, or the `run_id` — carries the credential.
// ---------------------------------------------------------------------------

#[test]
fn test_credential_never_reaches_manifest_provenance_or_run_id() {
    let server = ScriptedServer::spawn(full_session_script());
    let base_url = server.credentialed_base_url();
    let spec = simulator_spec(base_url.clone());

    let (feed, log) = with_captured_log(|| {
        SimulatorFeed::open(
            &spec,
            instrument(),
            quote_size(),
            Duration::from_secs(5),
            &ResourceLimits::default(),
        )
    });
    let feed = match feed {
        Ok(feed) => feed,
        Err(e) => panic!("materialisation (auth via the URL) must succeed: {e}"),
    };
    server.join();

    // 1. The manifest's top-level `data_source` is the feed provenance. Its
    //    serialised form is what `write_manifest` writes — assert the credential
    //    is gone but the data-source IDENTITY (host + tape sha256) remains.
    let tape_sha = feed.tape_meta().data_identity.clone();
    let provenance = feed.meta();
    let provenance_json = match serde_json::to_string(&provenance) {
        Ok(json) => json,
        Err(e) => panic!("provenance must serialise: {e}"),
    };
    assert!(
        !provenance_json.contains(SECRET),
        "the credential must never reach the manifest data_source: {provenance_json}"
    );
    assert!(
        !provenance_json.contains('@'),
        "no URL userinfo separator survives in provenance: {provenance_json}"
    );
    assert!(
        provenance_json.contains(&server_host(&base_url)),
        "the data-source host identity is still recorded: {provenance_json}"
    );
    assert_eq!(
        tape_sha.len(),
        64,
        "the tape sha256 identity is 64 hex chars"
    );
    assert!(
        provenance_json.contains(&tape_sha),
        "the tape sha256 identity is still recorded: {provenance_json}"
    );

    // 2. The manifest's nested `config.data_source` copy is equally clean.
    let config_json = match serde_json::to_string(&simulator_config(base_url.clone())) {
        Ok(json) => json,
        Err(e) => panic!("config must serialise: {e}"),
    };
    assert!(
        !config_json.contains(SECRET),
        "the credential must never reach the manifest config copy: {config_json}"
    );

    // 3. The `run_id` preimage excludes the credential: two configs differing
    //    ONLY in the embedded credential derive the IDENTICAL run_id, so the
    //    secret is fully excluded from the recorded run identity.
    let strategy = common::iron_condor_spec();
    let host = server_host(&base_url);
    let id_a = derive_run_id(
        &simulator_config(format!("http://alice:{SECRET}-A@{host}")),
        &strategy,
    );
    let id_b = derive_run_id(
        &simulator_config(format!("http://bob:{SECRET}-B@{host}")),
        &strategy,
    );
    assert_eq!(
        id_a.as_str(),
        id_b.as_str(),
        "the credential must not perturb the run_id (it is excluded from identity)"
    );
    assert!(
        !id_a.as_str().contains(SECRET),
        "a sha256 run_id never embeds plaintext"
    );

    // 4. Every captured tracing event (if any) is credential-free.
    assert!(
        !log.joined().contains(SECRET),
        "no tracing event may carry the credential: {:?}",
        log.lines()
    );
}

// ---------------------------------------------------------------------------
// Failure path: a refused connection flows a URL-bearing `reqwest` error through
// the bounded-retry `tracing::warn!` events AND the returned error. Neither may
// carry the credential (reqwest redacts URL userinfo in its error Display; the
// crate never embeds `base_url` in an error string).
// ---------------------------------------------------------------------------

#[test]
fn test_credential_never_reaches_error_or_tracing_on_failure() {
    // Bind then immediately drop the listener, so the address refuses.
    let refused_addr = {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(l) => l,
            Err(e) => panic!("must bind an ephemeral port: {e}"),
        };
        match listener.local_addr() {
            Ok(a) => a.to_string(),
            Err(e) => panic!("must report the bound address: {e}"),
        }
    };
    let base_url = format!("http://ci-bot:{SECRET}@{refused_addr}");
    let spec = simulator_spec(base_url);

    let (result, log) = with_captured_log(|| {
        SimulatorFeed::open(
            &spec,
            instrument(),
            quote_size(),
            Duration::from_secs(2),
            &ResourceLimits::default(),
        )
    });
    let err = match result {
        Ok(_) => panic!("a refused connection must fail materialisation"),
        Err(e) => e,
    };

    // The returned error carries structured context, not the credential.
    let message = err.to_string();
    assert!(
        !message.contains(SECRET),
        "the credential must never reach an error message: {message}"
    );

    // The bounded-retry warnings were actually captured (so the non-leak
    // assertion below is meaningful, not vacuous) and none carries the secret.
    let lines = log.lines();
    assert!(
        !lines.is_empty(),
        "the bounded-retry tracing warnings must have been captured"
    );
    assert!(
        !log.joined().contains(SECRET),
        "no tracing event may carry the credential on the failure path: {lines:?}"
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The `host:port` of a `http://userinfo@host:port` URL — the identity the
/// manifest is allowed to record.
fn server_host(base_url: &str) -> String {
    match base_url.rsplit('@').next() {
        Some(host) => host.to_string(),
        None => base_url.to_string(),
    }
}

fn derive_run_id(config: &BacktestConfig, strategy: &ironcondor::StrategySpec) -> RunId {
    match RunId::derive(
        config.seed,
        config,
        strategy,
        "tape-identity-fixed",
        "code-version-fixed",
        "lockfile-sha-fixed",
    ) {
        Ok(id) => id,
        Err(e) => panic!("run_id derivation must succeed: {e}"),
    }
}
