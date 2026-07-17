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
    ApiClient, BacktestError, CreateSessionRequest, MarketSimulator, SessionState,
    UpdateSessionRequest,
};

const CREATE_FIXTURE: &str = include_str!("fixtures/simulator/create.json");
const STEP_FIXTURES: [&str; 3] = [
    include_str!("fixtures/simulator/step_1.json"),
    include_str!("fixtures/simulator/step_2.json"),
    include_str!("fixtures/simulator/step_3.json"),
];
const ERROR_NOT_FOUND_FIXTURE: &str = include_str!("fixtures/simulator/error_not_found.json");

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
