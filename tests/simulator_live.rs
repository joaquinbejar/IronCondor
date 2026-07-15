//! Integration tests for the migrated OptionChain-Simulator client
//! (feature `simulator`).
//!
//! The whole file is gated behind `simulator`, so `cargo test` without the
//! feature compiles it to nothing. The **default** path is offline: it drives
//! the public wire DTOs and the derived [`SessionState`] from recorded stub
//! fixtures. The **live** path (`test_live_session_*`) is `#[ignore]`-by-default
//! and only touches a real simulator when `SIM_LIVE=1`.
#![cfg(feature = "simulator")]

use std::sync::Arc;
use std::time::Duration;

use ironcondor::{
    ApiClient, ChainResponse, CreateSessionRequest, MarketSimulator, SessionResponse, SessionState,
};

/// A recorded `GET /api/v1/chain` body — the wire shape the client must parse
/// without a live server.
const CHAIN_FIXTURE: &str = r#"{
    "underlying": "SPX",
    "timestamp": "2026-07-15T00:00:01Z",
    "price": 4321.5,
    "contracts": [
        {
            "strike": 4300.0,
            "expiration": "2026-08-15T00:00:00Z",
            "call": {"bid": 12.0, "ask": 12.5, "mid": 12.25, "delta": 0.55},
            "put": {"bid": 9.0, "ask": 9.5, "mid": 9.25, "delta": -0.45},
            "implied_volatility": 0.19,
            "gamma": 0.001
        }
    ],
    "session_info": {"id": "sess-1", "current_step": 3, "total_steps": 3}
}"#;

/// A recorded `POST /api/v1/chain` body.
const SESSION_FIXTURE: &str = r#"{
    "id": "sess-1",
    "created_at": "2026-07-15T00:00:00Z",
    "updated_at": "2026-07-15T00:00:00Z",
    "parameters": {"symbol": "SPX"},
    "current_step": 0,
    "total_steps": 3,
    "state": "Initialized"
}"#;

#[test]
fn test_stub_chain_fixture_parses_and_derives_terminal_state() {
    let Ok(chain) = serde_json::from_str::<ChainResponse>(CHAIN_FIXTURE) else {
        panic!("recorded chain body must parse");
    };
    assert_eq!(chain.underlying, "SPX");
    assert_eq!(chain.contracts.len(), 1);
    // At step 3 of 3 the derived state is terminal — the bug-fix-2 rule.
    let state = SessionState::from_progress(
        chain.session_info.current_step,
        chain.session_info.total_steps,
    );
    assert_eq!(state, SessionState::Completed);
    assert!(state.is_terminal());
}

#[test]
fn test_stub_session_fixture_parses_real_state_string() {
    let Ok(session) = serde_json::from_str::<SessionResponse>(SESSION_FIXTURE) else {
        panic!("recorded session body must parse");
    };
    assert_eq!(
        SessionState::from_wire(&session.state),
        SessionState::Initialized
    );
    assert_eq!(session.total_steps, 3);
}

/// End-to-end against a real OptionChain-Simulator. `#[ignore]` by default;
/// even when explicitly run it no-ops unless `SIM_LIVE=1` is set. Requires a
/// simulator on [`ApiClient::DEFAULT_BASE_URL`].
#[tokio::test]
#[ignore = "requires a live OptionChain-Simulator; set SIM_LIVE=1 and run with --ignored"]
async fn test_live_session_roundtrip_creates_advances_deletes() {
    if std::env::var("SIM_LIVE").as_deref() != Ok("1") {
        return;
    }
    let client = match ApiClient::new(ApiClient::DEFAULT_BASE_URL, Duration::from_secs(10)) {
        Ok(c) => Arc::new(c),
        Err(e) => panic!("client build failed: {e}"),
    };
    let mut sim = MarketSimulator::new(client);

    let request = CreateSessionRequest {
        symbol: "SPX".to_string(),
        steps: 5,
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
    };

    match sim.create_simulation(request).await {
        Ok(()) => {}
        Err(e) => panic!("create_simulation failed: {e}"),
    }
    assert!(
        !sim.is_terminated(),
        "a fresh live session is not terminated"
    );

    // Advance until the server reports the session ended.
    let mut steps = 0_u32;
    while !sim.is_terminated() && steps < 100 {
        match sim.next_step().await {
            Ok(_) => steps += 1,
            Err(e) => panic!("next_step failed at step {steps}: {e}"),
        }
    }
    assert!(
        sim.is_terminated(),
        "session must terminate within the step budget"
    );

    match sim.reset().await {
        Ok(()) => {}
        Err(e) => panic!("reset/delete failed: {e}"),
    }
}
