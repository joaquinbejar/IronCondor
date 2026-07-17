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
    ApiClient, ChainResponse, CreateSessionRequest, DataFeed, InstrumentSpec, MarketSimulator,
    PriceCents, Quantity, ResourceLimits, SessionResponse, SessionState, SimulatorFeed,
    SimulatorSourceSpec,
};

/// A recorded advance body (`POST /api/v1/chain/step`, also served by the
/// read-only `GET /api/v1/chain` peek) — the wire shape the client must parse
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

/// A recorded `POST /api/v1/chain` body (simulator v0.1.0: the echoed
/// `parameters` block carries the effective walk `seed`).
const SESSION_FIXTURE: &str = r#"{
    "id": "sess-1",
    "created_at": "2026-07-15T00:00:00Z",
    "updated_at": "2026-07-15T00:00:00Z",
    "parameters": {
        "symbol": "SPX",
        "initial_price": 4300.0,
        "volatility": 0.2,
        "risk_free_rate": 0.03,
        "method": {"GeometricBrownian": {"dt": 0.004, "drift": 0.0, "volatility": 0.2}},
        "time_frame": "Day",
        "dividend_yield": 0.0,
        "skew_slope": null,
        "smile_curve": null,
        "spread": 0.02,
        "seed": 42
    },
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
    assert_eq!(
        session.parameters.seed,
        Some(42),
        "the effective walk seed is read back from the echoed parameters"
    );
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
        seed: Some(42),
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

// ---------------------------------------------------------------------------
// The #45 same-seed closing tests (SIM_LIVE-gated; `#[ignore]` because they
// need a live OptionChain-Simulator — the seed channel itself is wired and
// covered offline)
// ---------------------------------------------------------------------------

/// The shared same-seed session request the closing tests materialise twice.
fn same_seed_spec(data_seed: u64) -> SimulatorSourceSpec {
    SimulatorSourceSpec {
        session: CreateSessionRequest {
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
            // Overridden by the materialiser with `data_seed` regardless.
            seed: Some(data_seed),
        },
        base_url: ApiClient::base_url_from_env(),
        data_seed,
        tape_sha256: String::new(),
        simulator_version: None,
    }
}

/// Materialise one live session to a tape. A 1-cent tick accepts any
/// dollar-rounded live price; the wire shape carries no grid of its own.
fn open_live_feed(data_seed: u64) -> SimulatorFeed {
    let instrument = match InstrumentSpec::new(PriceCents::new(1), 100) {
        Ok(spec) => spec,
        Err(e) => panic!("1c tick / 100x multiplier must be a valid spec: {e}"),
    };
    let quote_size = match Quantity::new(10) {
        Ok(q) => q,
        Err(e) => panic!("10 must be a valid quantity: {e}"),
    };
    match SimulatorFeed::open(
        &same_seed_spec(data_seed),
        instrument,
        quote_size,
        Duration::from_secs(10),
        &ResourceLimits::default(),
    ) {
        Ok(feed) => feed,
        Err(e) => panic!("live materialisation failed: {e}"),
    }
}

/// Drain a feed into its timestamp- and expiry-independent walk content: per
/// step the underlying price plus every quote's (strike, style, bid, ask,
/// mid, sizes, analytics). The `ts` and the contract expiry are deliberately
/// excluded — upstream v0.1.0 stamps both from the wall clock, so they are
/// transport metadata, not walk content.
#[allow(clippy::type_complexity)]
fn walk_content(
    feed: &mut SimulatorFeed,
) -> Vec<(
    u64,
    Vec<(
        u64,
        optionstratlib::OptionStyle,
        u64,
        u64,
        u64,
        u32,
        u32,
        [u8; 16],
        [u8; 16],
    )>,
)> {
    let mut steps = Vec::new();
    loop {
        match feed.next() {
            Ok(Some(snapshot)) => {
                let quotes: Vec<_> = snapshot
                    .quotes
                    .values()
                    .map(|view| {
                        (
                            view.contract.strike.value(),
                            view.contract.style,
                            view.bid.value(),
                            view.ask.value(),
                            view.mid.value(),
                            // Sizes + analytics are seeded walk content and must
                            // repeat too — comparing them strengthens the
                            // walk-identity evidence (IV + delta stand in for
                            // the full Greek set, all derived from the same
                            // seeded state).
                            view.bid_size.value(),
                            view.ask_size.value(),
                            view.implied_volatility.serialize(),
                            view.delta.serialize(),
                        )
                    })
                    .collect();
                steps.push((snapshot.underlying_price.value(), quotes));
            }
            Ok(None) => break,
            Err(e) => panic!("tape read failed: {e}"),
        }
    }
    steps
}

/// Same seed ⇒ the same **walk**: two sessions created with the identical
/// `(CreateSessionRequest, data_seed)` materialise tapes whose price content
/// is identical step for step. This is the strongest same-seed claim the
/// verified upstream v0.1.0 supports (its walk is seeded; its per-response
/// `timestamp` and rendered expiry are wall-clock).
#[test]
#[ignore = "requires a live OptionChain-Simulator; set SIM_LIVE=1 and run with --ignored"]
fn test_live_same_seed_sessions_repeat_the_walk() {
    if std::env::var("SIM_LIVE").as_deref() != Ok("1") {
        return;
    }
    let mut first = open_live_feed(424_242);
    let mut second = open_live_feed(424_242);
    let walk_a = walk_content(&mut first);
    let walk_b = walk_content(&mut second);
    assert_eq!(
        walk_a.len(),
        walk_b.len(),
        "same-seed sessions must serve the same number of steps"
    );
    assert_eq!(
        walk_a, walk_b,
        "same (request, data_seed) must repeat the identical price walk"
    );
}

/// The full roadmap gate: same seed ⇒ the identical **tape sha256** (and
/// hence the identical `run_id` data identity).
///
/// KNOWN UPSTREAM GAP — expected to FAIL against upstream v0.1.0: the server
/// stamps every `ChainResponse.timestamp` from `Utc::now()` and renders the
/// relative expiry against the wall clock (verified 2026-07-17 in the sibling
/// checkout, `src/api/rest/handlers.rs::build_chain_response`), so two
/// same-seed sessions differ in `ts` even though the walk repeats. This test
/// is the flip switch for `FeedKind::is_reproducible`: it goes green (and the
/// flag can flip) only once upstream serves deterministic timestamps.
#[test]
#[ignore = "requires a live OptionChain-Simulator AND upstream deterministic timestamps \
            (v0.1.0 stamps ChainResponse.timestamp from the wall clock); \
            set SIM_LIVE=1 and run with --ignored"]
fn test_live_same_seed_sessions_materialise_identical_tape_sha256() {
    if std::env::var("SIM_LIVE").as_deref() != Ok("1") {
        return;
    }
    let first = open_live_feed(424_242);
    let second = open_live_feed(424_242);
    assert_eq!(
        first.tape_meta().data_identity,
        second.tape_meta().data_identity,
        "same (request, data_seed) must materialise the identical tape sha256"
    );
}
