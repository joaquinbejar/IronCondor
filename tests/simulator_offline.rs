//! Offline integration tests for the OptionChain-Simulator client
//! (feature `simulator`): the **recorded-fixture round-trip** required by
//! issue #44 — create → advance × N → delete — driven through the real
//! `reqwest` transport against an in-process scripted HTTP responder, plus
//! one test per client failure mode (HTTP error status with a parseable
//! `ErrorResponse` body, an unparseable error body, a malformed success
//! body, a timeout, and a refused connection), each asserting the typed
//! `BacktestError::Session` mapping.
//!
//! The responder is a deliberately tiny hand-rolled HTTP/1.1 server on a
//! `std::net::TcpListener` — three routes need no mock-server dependency, so
//! the default build's supply-chain surface stays unchanged. Each scripted
//! response closes its connection (`connection: close`), so every client
//! call arrives as its own connection in script order. The response bodies
//! are the committed fixtures in `tests/fixtures/simulator/`, matching the
//! upstream v0.1.0 wire shape (verified against the sibling checkout).
#![cfg(feature = "simulator")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ironcondor::{
    ApiClient, BacktestError, CreateSessionRequest, DataFeed, DataSourceSpec, InstrumentSpec,
    MarketSimulator, PriceCents, Quantity, ResourceLimits, SessionState, SimTime, SimulatorFeed,
    SimulatorSourceSpec, StepIndex, UpdateSessionRequest,
};

const CREATE_FIXTURE: &str = include_str!("fixtures/simulator/create.json");
const STEP_FIXTURES: [&str; 3] = [
    include_str!("fixtures/simulator/step_1.json"),
    include_str!("fixtures/simulator/step_2.json"),
    include_str!("fixtures/simulator/step_3.json"),
];
const STEP_2_OUT_OF_ORDER_FIXTURE: &str =
    include_str!("fixtures/simulator/step_2_out_of_order.json");
const ERROR_NOT_FOUND_FIXTURE: &str = include_str!("fixtures/simulator/error_not_found.json");

/// `2026-07-15T00:00:01Z` (the `step_1.json` timestamp) in nanoseconds since
/// the Unix epoch; the later step fixtures advance by exactly one second.
const STEP_1_TS_NS: i64 = 1_784_073_601_000_000_000;

/// One scripted HTTP exchange: the response to send to the next connection.
struct Scripted {
    status: u16,
    reason: &'static str,
    body: String,
    /// Delay before responding — used to trip the client timeout.
    delay: Option<Duration>,
}

impl Scripted {
    fn ok(body: &str) -> Self {
        Self {
            status: 200,
            reason: "OK",
            body: body.to_string(),
            delay: None,
        }
    }

    fn created(body: &str) -> Self {
        Self {
            status: 201,
            reason: "Created",
            body: body.to_string(),
            delay: None,
        }
    }
}

/// A recorded inbound request: `"METHOD /path?query"` plus the raw body.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Recorded {
    target: String,
    body: String,
}

/// The in-process scripted responder: serves exactly one scripted response
/// per accepted connection, in order, recording each request.
struct ScriptedServer {
    base_url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ScriptedServer {
    fn spawn(script: Vec<Scripted>) -> Self {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(l) => l,
            Err(e) => panic!("scripted server must bind an ephemeral port: {e}"),
        };
        let addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => panic!("scripted server must report its address: {e}"),
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        let handle = std::thread::spawn(move || {
            for canned in script {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                if let Some(recorded) = read_request(&mut stream)
                    && let Ok(mut log) = log.lock()
                {
                    log.push(recorded);
                }
                if let Some(delay) = canned.delay {
                    std::thread::sleep(delay);
                }
                let response = format!(
                    "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{}",
                    canned.status,
                    canned.reason,
                    canned.body.len(),
                    canned.body
                );
                // The client may have hung up already (the timeout test); a
                // write failure is part of the script, not a test failure.
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            requests,
            handle: Some(handle),
        }
    }

    /// Join the responder (the whole script must have been consumed) and
    /// return the recorded requests in arrival order.
    fn finish(mut self) -> Vec<Recorded> {
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            panic!("scripted server thread panicked");
        }
        match self.requests.lock() {
            Ok(log) => log.clone(),
            Err(e) => panic!("request log must be readable: {e}"),
        }
    }
}

/// Read one HTTP/1.1 request (head + `content-length` body) off the stream,
/// returning its `"METHOD /path?query"` line and body. Returns `None` on a
/// malformed or truncated request — the test's later assertions will then
/// fail loudly on the missing log entry.
fn read_request(stream: &mut TcpStream) -> Option<Recorded> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; 1024];
    let head_end = loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = find_head_end(&buf) {
                    break pos;
                }
                if buf.len() > 64 * 1024 {
                    return None; // request-size ceiling: never read unbounded
                }
            }
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.trim().eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    let body_start = head_end.checked_add(4)?;
    while buf.len().checked_sub(body_start)? < content_length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let request_line = head.lines().next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;
    Some(Recorded {
        target: format!("{method} {target}"),
        body: String::from_utf8_lossy(buf.get(body_start..)?).to_string(),
    })
}

/// The index just before `\r\n\r\n`, if the head is complete.
fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn client_for(server: &ScriptedServer) -> ApiClient {
    match ApiClient::new(&server.base_url, Duration::from_secs(5)) {
        Ok(c) => c,
        Err(e) => panic!("client must build against the scripted server: {e}"),
    }
}

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

// ---------------------------------------------------------------------------
// The recorded-fixture round-trip: create → advance × 3 → delete, no server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_offline_roundtrip_create_advance_delete_against_recorded_fixtures() {
    let server = ScriptedServer::spawn(vec![
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(STEP_FIXTURES[0]),
        Scripted::ok(STEP_FIXTURES[1]),
        Scripted::ok(STEP_FIXTURES[2]),
        Scripted::ok("\"Session deleted\""),
    ]);
    let mut sim = MarketSimulator::new(Arc::new(client_for(&server)));

    // create — the wrapper records the server's REAL "Initialized" state.
    match sim.create_simulation(create_request()).await {
        Ok(()) => {}
        Err(e) => panic!("create against the recorded fixture failed: {e}"),
    }
    assert_eq!(sim.session_id(), Some("sess-1"));
    assert!(!sim.is_terminated(), "a fresh session is not terminated");
    match sim.get_current_state() {
        Ok(state) => assert_eq!(state.state, SessionState::Initialized),
        Err(e) => panic!("state must exist after create: {e}"),
    }

    // advance × 3 — the derived state flips to Completed exactly at 3 of 3.
    for (idx, expected_state) in [
        (1_usize, SessionState::InProgress),
        (2, SessionState::InProgress),
        (3, SessionState::Completed),
    ] {
        match sim.next_step().await {
            Ok(state) => {
                assert_eq!(state.current_step, idx);
                assert_eq!(state.total_steps, 3);
                assert_eq!(state.state, expected_state);
                assert_eq!(state.chain.underlying, "SPX");
                assert_eq!(state.chain.contracts.len(), 1);
            }
            Err(e) => panic!("advance {idx} against the recorded fixture failed: {e}"),
        }
    }
    assert!(sim.is_terminated(), "3 of 3 derives a terminal state");

    // delete — reset clears the wrapper and releases the server session.
    match sim.reset().await {
        Ok(()) => {}
        Err(e) => panic!("delete against the recorded fixture failed: {e}"),
    }
    assert_eq!(sim.session_id(), None);

    // The wire trace: exact methods, routes, and step query — the v0.1.0
    // surface (advance is POST /step; the seed travels on create).
    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "POST /api/v1/chain/step?sessionid=sess-1",
            "POST /api/v1/chain/step?sessionid=sess-1",
            "POST /api/v1/chain/step?sessionid=sess-1",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ]
    );
    match requests.first() {
        Some(create) => assert!(
            create.body.contains("\"seed\":42"),
            "the data seed must travel on create_session: {}",
            create.body
        ),
        None => panic!("the create request must have been recorded"),
    }
}

#[tokio::test]
async fn test_offline_create_session_reads_back_effective_seed() {
    let server = ScriptedServer::spawn(vec![Scripted::created(CREATE_FIXTURE)]);
    let client = client_for(&server);
    match client.create_session(create_request()).await {
        Ok(session) => {
            assert_eq!(session.id, "sess-1");
            assert_eq!(
                session.parameters.seed,
                Some(42),
                "the effective walk seed is echoed back for recording"
            );
            assert_eq!(
                SessionState::from_wire(&session.state),
                SessionState::Initialized
            );
        }
        Err(e) => panic!("create against the recorded fixture failed: {e}"),
    }
    drop(server.finish());
}

#[tokio::test]
async fn test_offline_replace_and_update_use_put_and_patch() {
    let server = ScriptedServer::spawn(vec![
        Scripted::ok(CREATE_FIXTURE),
        Scripted::ok(CREATE_FIXTURE),
    ]);
    let client = client_for(&server);

    match client.replace_session("sess-1", create_request()).await {
        Ok(session) => assert_eq!(session.id, "sess-1"),
        Err(e) => panic!("replace against the recorded fixture failed: {e}"),
    }
    let update = UpdateSessionRequest {
        volatility: Some(0.3),
        seed: Some(99),
        ..UpdateSessionRequest::default()
    };
    match client.update_session("sess-1", update).await {
        Ok(session) => assert_eq!(session.id, "sess-1"),
        Err(e) => panic!("update against the recorded fixture failed: {e}"),
    }

    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "PUT /api/v1/chain?sessionid=sess-1",
            "PATCH /api/v1/chain?sessionid=sess-1",
        ]
    );
    match requests.get(1) {
        Some(update) => {
            assert!(
                update.body.contains("\"seed\":99"),
                "a set replacement seed travels on update: {}",
                update.body
            );
            assert!(
                !update.body.contains("symbol"),
                "unset optional fields are omitted from the patch: {}",
                update.body
            );
        }
        None => panic!("the update request must have been recorded"),
    }
}

// ---------------------------------------------------------------------------
// Failure modes — every one maps to a typed BacktestError::Session
// ---------------------------------------------------------------------------

/// Assert the result is `Err(BacktestError::Session)` whose message contains
/// every fragment, returning the message for context on failure.
fn assert_session_error<T: std::fmt::Debug>(result: Result<T, BacktestError>, fragments: &[&str]) {
    match result {
        Err(BacktestError::Session(msg)) => {
            for fragment in fragments {
                assert!(
                    msg.contains(fragment),
                    "session error must mention {fragment:?}, got: {msg}"
                );
            }
        }
        other => panic!("expected a Session error, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_offline_http_status_with_error_body_maps_to_session() {
    let server = ScriptedServer::spawn(vec![Scripted {
        status: 404,
        reason: "Not Found",
        body: ERROR_NOT_FOUND_FIXTURE.to_string(),
        delay: None,
    }]);
    let client = client_for(&server);
    let result = client.get_next_step("sess-missing").await;
    assert_session_error(result, &["http 404", "Session not found"]);
    drop(server.finish());
}

#[tokio::test]
async fn test_offline_http_status_with_unparseable_body_maps_to_session() {
    let server = ScriptedServer::spawn(vec![Scripted {
        status: 500,
        reason: "Internal Server Error",
        body: "boom".to_string(),
        delay: None,
    }]);
    let client = client_for(&server);
    let result = client.get_next_step("sess-1").await;
    assert_session_error(result, &["http 500", "unparseable error body"]);
    drop(server.finish());
}

#[tokio::test]
async fn test_offline_malformed_success_body_maps_to_session() {
    let server = ScriptedServer::spawn(vec![Scripted::ok("this is not json")]);
    let client = client_for(&server);
    let result = client.get_next_step("sess-1").await;
    assert_session_error(result, &["parse chain response failed"]);
    drop(server.finish());
}

#[tokio::test]
async fn test_offline_timeout_maps_to_session() {
    let server = ScriptedServer::spawn(vec![Scripted {
        status: 200,
        reason: "OK",
        body: STEP_FIXTURES[0].to_string(),
        delay: Some(Duration::from_millis(1_500)),
    }]);
    let client = match ApiClient::new(&server.base_url, Duration::from_millis(200)) {
        Ok(c) => c,
        Err(e) => panic!("client must build against the scripted server: {e}"),
    };
    let result = client.get_next_step("sess-1").await;
    assert_session_error(result, &["get_next_step request failed"]);
    // Deliberately no finish(): the responder is still sleeping through its
    // scripted delay; it exits on its own once the write fails.
}

#[tokio::test]
async fn test_offline_connection_refused_maps_to_session() {
    // Bind an ephemeral port, then drop the listener so the address refuses.
    let refused_url = {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(l) => l,
            Err(e) => panic!("must bind an ephemeral port: {e}"),
        };
        match listener.local_addr() {
            Ok(addr) => format!("http://{addr}"),
            Err(e) => panic!("must report the bound address: {e}"),
        }
    };
    let client = match ApiClient::new(&refused_url, Duration::from_secs(1)) {
        Ok(c) => c,
        Err(e) => panic!("client must build offline: {e}"),
    };
    assert_session_error(
        client.create_session(create_request()).await,
        &["create_session request failed"],
    );
    assert_session_error(
        client.delete_session("sess-1").await,
        &["delete_session request failed"],
    );
}

// ---------------------------------------------------------------------------
// SimulatorFeed materialisation (#45): fixtures → a validated immutable tape,
// with the session deleted on EVERY path (success, error, ceiling cut-off)
// ---------------------------------------------------------------------------

/// The provenance spec the feed tests open: the shared request against the
/// scripted server, with the configured data seed `42` (matching the
/// effective seed the recorded create fixture echoes back).
fn feed_spec(server: &ScriptedServer) -> SimulatorSourceSpec {
    SimulatorSourceSpec {
        session: create_request(),
        base_url: server.base_url.clone(),
        data_seed: 42,
        tape_sha256: String::new(),
        simulator_version: None,
    }
}

/// The 5-cent / 100x instrument grid the wire shape cannot carry; every
/// fixture price is 5-cent-aligned.
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

fn open_feed(
    server: &ScriptedServer,
    limits: &ResourceLimits,
) -> Result<SimulatorFeed, BacktestError> {
    SimulatorFeed::open(
        &feed_spec(server),
        instrument(),
        quote_size(),
        Duration::from_secs(5),
        limits,
    )
}

/// The full recorded session: create → 3 steps → delete.
fn full_session_script() -> Vec<Scripted> {
    vec![
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(STEP_FIXTURES[0]),
        Scripted::ok(STEP_FIXTURES[1]),
        Scripted::ok(STEP_FIXTURES[2]),
        Scripted::ok("\"Session deleted\""),
    ]
}

#[test]
fn test_offline_feed_materialises_validated_tape_and_deletes_session() {
    let server = ScriptedServer::spawn(full_session_script());
    let mut feed = match open_feed(&server, &ResourceLimits::default()) {
        Ok(feed) => feed,
        Err(e) => panic!("materialisation against the recorded fixtures failed: {e}"),
    };

    // The pinned tape metadata is available before anything is consumed.
    let meta = feed.tape_meta().clone();
    assert!(meta.non_empty);
    assert_eq!(meta.first_ts, SimTime::new(STEP_1_TS_NS));
    assert_eq!(meta.final_step, StepIndex::new(2));
    assert_eq!(meta.data_identity.len(), 64, "sha256 is 64 hex chars");
    assert!(meta.data_identity.chars().all(|c| c.is_ascii_hexdigit()));

    // The provenance records the tape identity and the EFFECTIVE walk seed
    // the server echoed back (42, per the recorded create fixture).
    match feed.meta() {
        DataSourceSpec::Simulator(source) => {
            assert_eq!(source.tape_sha256, meta.data_identity);
            assert_eq!(source.data_seed, 42, "the effective seed is recorded");
            assert_eq!(source.session.seed, Some(42));
            assert_eq!(source.base_url, server.base_url);
        }
        other => panic!("the simulator feed must describe a simulator source, got {other:?}"),
    }

    // next() is a pure indexed read over the validated tape: three strictly
    // ts-increasing snapshots (one second apart), 0-based tape steps, one
    // call + one put quote per fixture contract row, then None forever.
    for (index, expected_price_cents) in [(0_u32, 431_025_u64), (1, 429_575), (2, 432_150)] {
        match feed.next() {
            Ok(Some(snapshot)) => {
                assert_eq!(snapshot.step, StepIndex::new(index));
                assert_eq!(
                    snapshot.ts,
                    SimTime::new(STEP_1_TS_NS + i64::from(index) * 1_000_000_000)
                );
                assert_eq!(snapshot.underlying.as_str(), "SPX");
                assert_eq!(
                    snapshot.underlying_price,
                    PriceCents::new(expected_price_cents)
                );
                assert_eq!(snapshot.quotes.len(), 2, "one contract row = call + put");
            }
            other => panic!("expected snapshot {index}, got {other:?}"),
        }
    }
    assert!(matches!(feed.next(), Ok(None)));
    assert!(matches!(feed.next(), Ok(None)), "exhaustion is permanent");

    // The wire trace: the data seed travels on create, every advance carries
    // the expected_step precondition, and the session is deleted at the end.
    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=0",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=1",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=2",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ]
    );
    match requests.first() {
        Some(create) => assert!(
            create.body.contains("\"seed\":42"),
            "the configured data seed must travel on create_session: {}",
            create.body
        ),
        None => panic!("the create request must have been recorded"),
    }
}

#[test]
fn test_offline_feed_out_of_order_ts_rejected_and_session_deleted() {
    // step 2 repeats step 1's timestamp — a duplicate ts is DataOutOfOrder.
    let server = ScriptedServer::spawn(vec![
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(STEP_FIXTURES[0]),
        Scripted::ok(STEP_2_OUT_OF_ORDER_FIXTURE),
        Scripted::ok("\"Session deleted\""),
    ]);
    let result = open_feed(&server, &ResourceLimits::default());
    assert!(
        matches!(
            result,
            Err(BacktestError::DataOutOfOrder {
                step: 1,
                ts: STEP_1_TS_NS,
                prev: STEP_1_TS_NS,
            })
        ),
        "a duplicate ts must be DataOutOfOrder, got {result:?}"
    );

    // The violation aborts the drain (no third advance) and the session is
    // STILL deleted.
    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=0",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=1",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ]
    );
}

#[test]
fn test_offline_feed_max_steps_ceiling_cuts_off_and_deletes() {
    // 3 canned steps against a 2-step ceiling: the crossing is detected on
    // the third advance, BEFORE it is pushed, and nothing further is read.
    let server = ScriptedServer::spawn(full_session_script());
    let limits = ResourceLimits {
        max_steps: 2,
        ..ResourceLimits::default()
    };
    let result = open_feed(&server, &limits);
    assert!(
        matches!(
            result,
            Err(BacktestError::TapeTooLarge {
                limit: "max_steps",
                value: 3,
                cap: 2,
            })
        ),
        "the first crossed ceiling must abort with TapeTooLarge, got {result:?}"
    );

    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=0",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=1",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=2",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ],
        "no further step request may follow the crossing; only the DELETE"
    );
}

#[test]
fn test_offline_feed_max_contracts_ceiling_cuts_off_and_deletes() {
    // One fixture contract row = 2 quotes, against a 1-quote ceiling: the
    // very first snapshot crosses at conversion time.
    let server = ScriptedServer::spawn(vec![
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(STEP_FIXTURES[0]),
        Scripted::ok("\"Session deleted\""),
    ]);
    let limits = ResourceLimits {
        max_contracts_per_snapshot: 1,
        ..ResourceLimits::default()
    };
    let result = open_feed(&server, &limits);
    assert!(
        matches!(
            result,
            Err(BacktestError::TapeTooLarge {
                limit: "max_contracts_per_snapshot",
                value: 2,
                cap: 1,
            })
        ),
        "an oversized snapshot must abort with TapeTooLarge, got {result:?}"
    );

    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=0",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ]
    );
}

#[test]
fn test_offline_feed_max_total_bytes_ceiling_cuts_off_and_deletes() {
    // A 1-byte tape budget: the first converted snapshot crosses the running
    // byte total before it is pushed.
    let server = ScriptedServer::spawn(vec![
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(STEP_FIXTURES[0]),
        Scripted::ok("\"Session deleted\""),
    ]);
    let limits = ResourceLimits {
        max_total_bytes: 1,
        ..ResourceLimits::default()
    };
    let result = open_feed(&server, &limits);
    match result {
        Err(BacktestError::TapeTooLarge {
            limit: "max_total_bytes",
            value,
            cap: 1,
        }) => assert!(value > 1, "the observed byte total must exceed the cap"),
        other => panic!("expected the max_total_bytes cut-off, got {other:?}"),
    }

    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=0",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ]
    );
}

#[test]
fn test_offline_feed_same_fixtures_twice_identical_sha256() {
    // The offline half of the reproducibility claim: replaying the identical
    // recorded responses through the materialiser twice yields the identical
    // tape identity — the materialiser itself adds no nondeterminism.
    let first = ScriptedServer::spawn(full_session_script());
    let second = ScriptedServer::spawn(full_session_script());
    let feed_a = match open_feed(&first, &ResourceLimits::default()) {
        Ok(feed) => feed,
        Err(e) => panic!("first materialisation failed: {e}"),
    };
    let feed_b = match open_feed(&second, &ResourceLimits::default()) {
        Ok(feed) => feed,
        Err(e) => panic!("second materialisation failed: {e}"),
    };
    assert_eq!(
        feed_a.tape_meta().data_identity,
        feed_b.tape_meta().data_identity,
        "identical canned responses must materialise to the identical tape sha256"
    );
    drop(first.finish());
    drop(second.finish());
}

#[test]
fn test_offline_feed_different_fixtures_distinct_sha256() {
    // Two sessions from the identical request whose walks differ must get
    // DISTINCT identities (and hence distinct run_ids downstream).
    let diverged_step = STEP_FIXTURES[0].replace("4310.25", "4390.25");
    assert_ne!(diverged_step, STEP_FIXTURES[0], "the walk must diverge");
    let first = ScriptedServer::spawn(full_session_script());
    let second = ScriptedServer::spawn(vec![
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(&diverged_step),
        Scripted::ok(STEP_FIXTURES[1]),
        Scripted::ok(STEP_FIXTURES[2]),
        Scripted::ok("\"Session deleted\""),
    ]);
    let feed_a = match open_feed(&first, &ResourceLimits::default()) {
        Ok(feed) => feed,
        Err(e) => panic!("first materialisation failed: {e}"),
    };
    let feed_b = match open_feed(&second, &ResourceLimits::default()) {
        Ok(feed) => feed,
        Err(e) => panic!("second (diverged) materialisation failed: {e}"),
    };
    assert_ne!(
        feed_a.tape_meta().data_identity,
        feed_b.tape_meta().data_identity,
        "differing walks from the identical request must get distinct identities"
    );
    drop(first.finish());
    drop(second.finish());
}

#[test]
fn test_offline_feed_empty_session_fails_to_construct_and_deletes() {
    // A session that is already ended at creation yields no steps: the empty
    // tape fails to construct — and the session is still deleted.
    let completed_create =
        CREATE_FIXTURE.replace("\"state\": \"Initialized\"", "\"state\": \"Completed\"");
    assert_ne!(
        completed_create, CREATE_FIXTURE,
        "the state must be patched"
    );
    let server = ScriptedServer::spawn(vec![
        Scripted::created(&completed_create),
        Scripted::ok("\"Session deleted\""),
    ]);
    let result = open_feed(&server, &ResourceLimits::default());
    match result {
        Err(BacktestError::Conversion(msg)) => assert!(
            msg.contains("empty tape"),
            "the error must name the empty tape, got: {msg}"
        ),
        other => panic!("an empty session must fail construction, got {other:?}"),
    }

    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ],
        "no advance is attempted on an already-ended session; delete still runs"
    );
}

#[test]
fn test_offline_feed_create_retry_is_bounded_and_recovers() {
    // The bounded materialisation retry: a transient 500 on create is
    // retried and the session then materialises normally.
    let server = ScriptedServer::spawn(vec![
        Scripted {
            status: 500,
            reason: "Internal Server Error",
            body: "boom".to_string(),
            delay: None,
        },
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(STEP_FIXTURES[0]),
        Scripted::ok(STEP_FIXTURES[1]),
        Scripted::ok(STEP_FIXTURES[2]),
        Scripted::ok("\"Session deleted\""),
    ]);
    let feed = match open_feed(&server, &ResourceLimits::default()) {
        Ok(feed) => feed,
        Err(e) => panic!("materialisation must recover from one transient failure: {e}"),
    };
    assert_eq!(feed.tape_meta().final_step, StepIndex::new(2));

    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets.first().copied(),
        Some("POST /api/v1/chain"),
        "the failed create comes first"
    );
    assert_eq!(
        targets.get(1).copied(),
        Some("POST /api/v1/chain"),
        "the bounded retry re-sends the create"
    );
    assert_eq!(targets.len(), 6, "one retry, then the normal session flow");
}

#[test]
fn test_offline_feed_step_failure_exhausts_retries_aborts_and_deletes() {
    // A persistent advance failure: all bounded attempts fail, open aborts
    // with the typed Session error — and the session is STILL deleted.
    let step_error = |status: u16, reason: &'static str| Scripted {
        status,
        reason,
        body: ERROR_NOT_FOUND_FIXTURE.to_string(),
        delay: None,
    };
    let server = ScriptedServer::spawn(vec![
        Scripted::created(CREATE_FIXTURE),
        Scripted::ok(STEP_FIXTURES[0]),
        step_error(404, "Not Found"),
        step_error(404, "Not Found"),
        step_error(404, "Not Found"),
        Scripted::ok("\"Session deleted\""),
    ]);
    let result = open_feed(&server, &ResourceLimits::default());
    match result {
        Err(BacktestError::Session(msg)) => assert!(
            msg.contains("http 404"),
            "the last retry error surfaces, got: {msg}"
        ),
        other => panic!("a persistent advance failure must abort with Session, got {other:?}"),
    }

    let requests = server.finish();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        vec![
            "POST /api/v1/chain",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=0",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=1",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=1",
            "POST /api/v1/chain/step?sessionid=sess-1&expected_step=1",
            "DELETE /api/v1/chain?sessionid=sess-1",
        ],
        "three bounded attempts at the same expected_step, then the DELETE"
    );
}
