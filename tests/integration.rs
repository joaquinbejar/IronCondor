//! End-to-end integration test for the Parquet historical feed (#9).
//!
//! Builds a small canonical IronCondor-shaped Parquet chain **programmatically**
//! into a tempdir (no committed binary fixture), then drives
//! [`ironcondor::ParquetFeed`] through [`ironcondor::DataFeed::next`] to
//! exhaustion, asserting the tape is ordered and that
//! `TapeMeta.data_identity` equals an independently recomputed file `sha256`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};

use ironcondor::{CsvFeed, DataFeed, DataSourceSpec, ParquetFeed, ResourceLimits};

mod common;

#[cfg(feature = "orderbook")]
#[path = "fixtures/liquidity.rs"]
mod liquidity_fixture;

const TS0: i64 = 1_750_291_200_000_000_000;
const NANOS_PER_DAY: i64 = 86_400_000_000_000;
const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;

/// The canonical Parquet schema for the historical feed, in column order.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("underlying", DataType::Utf8, false),
        Field::new("underlying_price", DataType::Int64, false),
        Field::new("tick_size", DataType::Int64, false),
        Field::new("contract_multiplier", DataType::Int32, false),
        Field::new("expiration", DataType::Int64, false),
        Field::new("strike", DataType::Int64, false),
        Field::new("style", DataType::Utf8, false),
        Field::new("bid", DataType::Int64, false),
        Field::new("ask", DataType::Int64, false),
        Field::new("bid_size", DataType::Int32, false),
        Field::new("ask_size", DataType::Int32, false),
        Field::new("implied_volatility", DataType::Float64, false),
        Field::new("delta", DataType::Float64, false),
        Field::new("gamma", DataType::Float64, false),
        Field::new("theta", DataType::Float64, false),
        Field::new("vega", DataType::Float64, false),
    ]))
}

/// One quote row: `(step, ts, strike, style, bid, ask)` on a 5c-tick 100x SPX
/// chain with a fixed absolute expiry.
type Row = (i32, i64, i64, &'static str, i64, i64);

/// A three-step iron-condor-shaped chain: two strikes, call + put, per step.
fn canonical_rows() -> Vec<Row> {
    let mut rows = Vec::new();
    for (step, ts) in [
        (0i32, TS0),
        (1, TS0 + NANOS_PER_DAY),
        (2, TS0 + 2 * NANOS_PER_DAY),
    ] {
        rows.push((step, ts, 500_000, "call", 200, 210));
        rows.push((step, ts, 500_000, "put", 180, 190));
        rows.push((step, ts, 520_000, "call", 90, 100));
        rows.push((step, ts, 520_000, "put", 140, 150));
    }
    rows
}

/// Encode the rows as a single Parquet batch and write it to `path`.
fn write_fixture(path: &std::path::Path, rows: &[Row]) -> Result<(), String> {
    let step: Vec<i32> = rows.iter().map(|r| r.0).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let strike: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let style: Vec<&str> = rows.iter().map(|r| r.3).collect();
    let bid: Vec<i64> = rows.iter().map(|r| r.4).collect();
    let ask: Vec<i64> = rows.iter().map(|r| r.5).collect();
    let n = rows.len();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(step)) as ArrayRef,
        Arc::new(Int64Array::from(ts)),
        Arc::new(StringArray::from(vec!["SPX"; n])),
        Arc::new(Int64Array::from(vec![510_000i64; n])),
        Arc::new(Int64Array::from(vec![5i64; n])),
        Arc::new(Int32Array::from(vec![100i32; n])),
        Arc::new(Int64Array::from(vec![EXPIRY; n])),
        Arc::new(Int64Array::from(strike)),
        Arc::new(StringArray::from(style)),
        Arc::new(Int64Array::from(bid)),
        Arc::new(Int64Array::from(ask)),
        Arc::new(Int32Array::from(vec![10i32; n])),
        Arc::new(Int32Array::from(vec![10i32; n])),
        Arc::new(Float64Array::from(vec![0.2f64; n])),
        Arc::new(Float64Array::from(vec![0.5f64; n])),
        Arc::new(Float64Array::from(vec![0.01f64; n])),
        Arc::new(Float64Array::from(vec![-0.05f64; n])),
        Arc::new(Float64Array::from(vec![0.1f64; n])),
    ];

    let batch = RecordBatch::try_new(schema(), columns).map_err(|e| e.to_string())?;
    let file = std::fs::File::create(path).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema(), None).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}

/// Independently recompute the file `sha256` as lowercase hex.
fn recompute_sha256(path: &std::path::Path) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let digest = Sha256::digest(&bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

#[test]
fn test_parquet_feed_drives_ordered_tape_to_exhaustion_with_verified_identity() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("canonical_chain.parquet");
    let rows = canonical_rows();
    if let Err(e) = write_fixture(&path, &rows) {
        panic!("the canonical fixture must write: {e}");
    }

    let Ok(mut feed) = ParquetFeed::open(&path, &ResourceLimits::default()) else {
        panic!("the canonical fixture must open");
    };

    // The tape identity equals an independently recomputed file sha256.
    let Ok(expected_sha) = recompute_sha256(&path) else {
        panic!("recomputing the fixture sha256 must succeed");
    };
    assert_eq!(feed.tape_meta().data_identity, expected_sha);
    assert!(feed.tape_meta().non_empty);
    assert_eq!(feed.tape_meta().first_ts.value(), TS0);
    assert_eq!(feed.tape_meta().final_step.value(), 2);

    // meta() carries the same identity as the manifest provenance locator.
    match feed.meta() {
        DataSourceSpec::Parquet { path: p, sha256 } => {
            assert_eq!(sha256, expected_sha);
            assert!(p.ends_with("canonical_chain.parquet"));
        }
        other => panic!("expected a Parquet data source, got {other:?}"),
    }

    // Drive the feed to exhaustion; snapshots arrive in ascending (step, ts).
    let mut count = 0u32;
    let mut prev_ts = i64::MIN;
    loop {
        match feed.next() {
            Ok(Some(snap)) => {
                assert_eq!(snap.step.value(), count);
                assert!(snap.ts.value() > prev_ts, "ts must strictly increase");
                assert_eq!(snap.quotes.len(), 4, "two strikes × call/put");
                assert_eq!(snap.spec.tick_size_cents.value(), 5);
                assert_eq!(snap.spec.contract_multiplier, 100);
                prev_ts = snap.ts.value();
                count += 1;
            }
            Ok(None) => break,
            Err(e) => panic!("the well-formed tape must yield Ok until exhaustion: {e}"),
        }
    }
    assert_eq!(count, 3, "three steps materialise into three snapshots");
    // Exhaustion is sticky.
    assert!(matches!(feed.next(), Ok(None)));
}

#[test]
fn test_csv_feed_matches_parquet_for_the_same_logical_data() {
    // The same logical chain written as BOTH a Parquet file and a directory of
    // per-step CSV files must yield byte-identical `ChainSnapshot`s — the CSV
    // acceptance criterion (docs/03 §4, #27).
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let rows = common::condor_rows(3, None);
    let parquet_path = dir.path().join("chain.parquet");
    if let Err(e) = common::write_parquet(&parquet_path, &rows) {
        panic!("the parquet fixture must write: {e}");
    }
    let csv_dir = dir.path().join("csv");
    if let Err(e) = common::write_csv_dir(&csv_dir, &rows) {
        panic!("the csv fixture must write: {e}");
    }

    let (Ok(mut parquet), Ok(mut csv)) = (
        ParquetFeed::open(&parquet_path, &ResourceLimits::default()),
        CsvFeed::open(&csv_dir, &ResourceLimits::default()),
    ) else {
        panic!("both feeds over the same logical data must open");
    };

    let mut count = 0u32;
    loop {
        match (parquet.next(), csv.next()) {
            (Ok(Some(p)), Ok(Some(c))) => {
                assert_eq!(p, c, "the CSV snapshot must equal the Parquet snapshot");
                count += 1;
            }
            (Ok(None), Ok(None)) => break,
            other => panic!("the two feeds diverged in length or errored: {other:?}"),
        }
    }
    assert_eq!(count, 3, "three steps in both feeds");
}

#[test]
fn test_csv_feed_runs_end_to_end_to_equity_curve() {
    // A CSV directory drives the full replay loop to a non-empty equity curve.
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let csv_dir = dir.path().join("csv");
    let rows = common::condor_rows(3, None);
    if let Err(e) = common::write_csv_dir(&csv_dir, &rows) {
        panic!("the csv fixture must write: {e}");
    }
    let Ok(run) = common::run_condor_csv(&csv_dir, 42) else {
        panic!("the CSV-sourced iron-condor run must complete");
    };
    assert!(
        !run.equity_curve.is_empty(),
        "the CSV run must produce an equity curve"
    );
}

#[test]
fn test_csv_committed_fixtures_open_and_replay_in_order() {
    // The committed CSV chain fixtures ship with the loader and must parse,
    // ordering strictly by ts across their name-sorted files.
    for (name, expected_steps) in [
        ("normal", 3u32),
        ("wide_spreads", 2),
        ("missing_strikes", 2),
        ("0dte", 2),
    ] {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/csv")
            .join(name);
        let Ok(mut feed) = CsvFeed::open(&dir, &ResourceLimits::default()) else {
            panic!("committed CSV fixture {name} must open");
        };
        assert_eq!(feed.tape_meta().data_identity.len(), 64);
        let mut count = 0u32;
        let mut prev_ts = i64::MIN;
        loop {
            match feed.next() {
                Ok(Some(snap)) => {
                    assert!(
                        snap.ts.value() > prev_ts,
                        "ts must strictly increase in fixture {name}"
                    );
                    prev_ts = snap.ts.value();
                    count += 1;
                }
                Ok(None) => break,
                Err(e) => panic!("committed fixture {name} must yield Ok until exhaustion: {e}"),
            }
        }
        assert_eq!(count, expected_steps, "step count for fixture {name}");
    }
}

#[test]
fn test_iron_condor_run_over_parquet_fixture_produces_equity_curve() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(4, None);
    if let Err(e) = common::write_parquet(&path, &rows) {
        panic!("the condor fixture must write: {e}");
    }

    let Ok(run) = common::run_condor(&path, 11) else {
        panic!("the iron-condor run over the fixture must succeed");
    };

    // One equity point per step (four snapshots), in step order.
    assert_eq!(run.equity_curve.len(), 4);
    let steps: Vec<u32> = run.equity_curve.iter().map(|p| p.step).collect();
    assert_eq!(steps, vec![0, 1, 2, 3]);

    // on_end closed every leg at the terminal step (no leg left open).
    assert!(run.open_at_end.is_empty(), "on_end closes all four legs");

    // The upstream result carries the fields #14 populates.
    assert_eq!(run.result.strategy_name, "iron_condor");
    assert_eq!(
        run.result.initial_capital,
        rust_decimal::Decimal::new(10_000_000, 2)
    );
    // The test period spans the tape's first and last snapshot timestamps (UTC),
    // derived from SimTime ns — never Utc::now.
    assert_eq!(
        run.result.test_period_start.timestamp_nanos_opt(),
        Some(common::TS0)
    );
    assert_eq!(
        run.result.test_period_end.timestamp_nanos_opt(),
        Some(common::TS0 + 3 * common::NANOS_PER_DAY)
    );
}

#[test]
fn test_run_backtest_over_parquet_fixture_populates_minimal_metrics() {
    use optionstratlib::simulation::ExitPolicy;
    use rust_decimal::Decimal;

    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(6, None);
    if let Err(e) = common::write_parquet(&path, &rows) {
        panic!("the condor fixture must write: {e}");
    }

    // The v0.1 end-to-end entry: Parquet in, equity curve + metrics out.
    let config = common::condor_config(&path, 7);
    let spec = common::iron_condor_spec();
    // Non-triggering exit so on_end performs the single clean close at the end.
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let Ok(run) = ironcondor::run_backtest(&config, &spec, exit) else {
        panic!("the run_backtest slice over the fixture must succeed");
    };

    // The equity-curve artifact is non-empty: one point per snapshot, in order.
    assert!(
        !run.equity_curve.is_empty(),
        "the equity curve is the artifact"
    );
    assert_eq!(run.equity_curve.len(), 6);
    let steps: Vec<u32> = run.equity_curve.iter().map(|p| p.step).collect();
    assert_eq!(steps, vec![0, 1, 2, 3, 4, 5]);

    // The minimal metrics are populated on the UPSTREAM BacktestResult — the
    // cents magnitude key is always inserted by the metrics pass.
    assert!(
        run.result
            .custom_metrics
            .contains_key(ironcondor::analytics::metrics::MAX_DRAWDOWN_CENTS_KEY),
        "the drawdown cents magnitude is populated"
    );
    // max_drawdown is the non-negative magnitude of the worst ledger ratio.
    assert!(run.result.drawdown_analysis.max_drawdown >= Decimal::ZERO);

    // The populated cents magnitude equals an INDEPENDENT recomputation from
    // the returned equity curve + the run's initial capital — proving the
    // metrics ran and are correct (not fabricated).
    let mut peak = 10_000_000_i64;
    let mut worst = 0_i64;
    for pnt in &run.equity_curve {
        if pnt.equity_cents > peak {
            peak = pnt.equity_cents;
        }
        let decline = peak - pnt.equity_cents;
        if decline > worst {
            worst = decline;
        }
    }
    assert!(
        matches!(
            run.result
                .custom_metrics
                .get(ironcondor::analytics::metrics::MAX_DRAWDOWN_CENTS_KEY),
            Some(v) if *v == Decimal::from(worst)
        ),
        "the populated cents magnitude matches an independent recomputation"
    );
}

#[test]
fn test_run_backtest_populates_greeks_attribution_end_to_end() {
    // The #31 end-to-end wiring: `run_backtest` runs the engine (which collects
    // the attribution substrate) THEN the analytics attribution pass, landing
    // one `GreeksAttributionRow` per step in `run.greeks_attribution`. This
    // proves the composition-root wiring, not just the analytics pass in
    // isolation (which `tests/property.rs` covers).
    use optionstratlib::simulation::ExitPolicy;

    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(6, None);
    if let Err(e) = common::write_parquet(&path, &rows) {
        panic!("the condor fixture must write: {e}");
    }

    let config = common::condor_config(&path, 7);
    let spec = common::iron_condor_spec();
    let Ok(run) = ironcondor::run_backtest(&config, &spec, ExitPolicy::TimeSteps(1_000_000)) else {
        panic!("the run_backtest slice must succeed");
    };

    // One attribution row per step, in step order — filled by run.rs post-run.
    assert_eq!(
        run.greeks_attribution.len(),
        run.equity_curve.len(),
        "one attribution row per equity point"
    );
    let attr_steps: Vec<u32> = run.greeks_attribution.iter().map(|r| r.step).collect();
    assert_eq!(attr_steps, vec![0, 1, 2, 3, 4, 5], "rows are in step order");

    // The golden reconciliation invariant holds EXACTLY in integer cents at
    // every step, checked against the independent equity curve.
    let mut prev_equity: i64 = 10_000_000; // condor_config initial capital
    for (row, point) in run.greeks_attribution.iter().zip(run.equity_curve.iter()) {
        let expected = i128::from(point.equity_cents) - i128::from(prev_equity);
        prev_equity = point.equity_cents;
        let lhs = i128::from(row.theta_pnl_cents)
            + i128::from(row.delta_pnl_cents)
            + i128::from(row.vega_pnl_cents)
            + i128::from(row.spread_capture_cents)
            - i128::from(row.fees_cents)
            + i128::from(row.residual_cents);
        assert_eq!(
            lhs, expected,
            "attribution reconciles to step_pnl at step {}",
            row.step
        );
    }

    // Step 0's Greek terms are all zero (no S_-1), and the residual closes the
    // initial-capital baseline.
    let Some(row0) = run.greeks_attribution.first() else {
        panic!("a step-0 row exists");
    };
    assert_eq!(row0.theta_pnl_cents, 0);
    assert_eq!(row0.delta_pnl_cents, 0);
    assert_eq!(row0.vega_pnl_cents, 0);
}

#[test]
fn test_short_strangle_run_over_parquet_fixture_produces_equity_curve() {
    // The v0.2 second strategy runs end to end through the UNCHANGED engine and
    // generic adapter (#28): a two-leg short strangle, opened at entry and closed
    // by on_end at the terminal step, producing one equity point per snapshot.
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("strangle.parquet");
    let rows = common::strangle_rows(4);
    if let Err(e) = common::write_parquet(&path, &rows) {
        panic!("the strangle fixture must write: {e}");
    }

    let Ok(run) = common::run_strangle(&path, 11) else {
        panic!("the short strangle run over the fixture must succeed");
    };

    // One equity point per step (four snapshots), in step order.
    assert_eq!(run.equity_curve.len(), 4);
    let steps: Vec<u32> = run.equity_curve.iter().map(|p| p.step).collect();
    assert_eq!(steps, vec![0, 1, 2, 3]);

    // on_end closed both legs at the terminal step (no leg left open).
    assert!(
        run.open_at_end.is_empty(),
        "on_end closes both strangle legs"
    );

    // The upstream result carries the second strategy's name.
    assert_eq!(run.result.strategy_name, "short_strangle");
    assert_eq!(
        run.result.initial_capital,
        rust_decimal::Decimal::new(10_000_000, 2)
    );
    // The test period spans the tape's first and last snapshot timestamps (UTC),
    // derived from SimTime ns — never Utc::now.
    assert_eq!(
        run.result.test_period_start.timestamp_nanos_opt(),
        Some(common::TS0)
    );
    assert_eq!(
        run.result.test_period_end.timestamp_nanos_opt(),
        Some(common::TS0 + 3 * common::NANOS_PER_DAY)
    );
}

#[test]
fn test_iron_condor_run_is_byte_identical_across_two_runs() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(5, None);
    if let Err(e) = common::write_parquet(&path, &rows) {
        panic!("the condor fixture must write: {e}");
    }

    let Ok(first) = common::run_condor(&path, 99) else {
        panic!("first run must succeed");
    };
    let Ok(second) = common::run_condor(&path, 99) else {
        panic!("second run must succeed");
    };

    // Same (seed, config, data) ⇒ byte-identical equity curve, open tail, and
    // the populated result scalars.
    assert_eq!(first.equity_curve, second.equity_curve);
    assert_eq!(first.open_at_end, second.open_at_end);
    assert_eq!(first.result.final_capital, second.result.final_capital);
    assert_eq!(first.result.initial_capital, second.result.initial_capital);
}

/// Realistic-mode adapter (feature `orderbook`, #22): submit → capture → `Fill`
/// round-trip against a **hand-seeded leaf book**. Seeds a two-level ask ladder
/// through the public [`ironcondor::RealisticFill::seed_maker_limit`] primitive,
/// routes a marketable buy through [`ironcondor::ExecutionModel::fill`], and
/// asserts the two per-level fills come back at the seeded prices with the
/// once-per-order fee only on the first — all through the public API, with no
/// `option_chain_orderbook` type crossing the seam.
#[cfg(feature = "orderbook")]
#[test]
fn test_realistic_marketable_buy_round_trips_two_seeded_levels() {
    use ironcondor::{
        ChainSnapshot, ContractKey, ExecutionMode, ExecutionModel, FeeSchedule, Fill,
        InstrumentSpec, OrderCommand, OrderIntent, PositionAction, PriceCents, Quantity, QuoteView,
        RealisticFill, SimTime, StepIndex, TimeInForce, Underlying,
    };
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use std::collections::BTreeMap;

    const TICK: u64 = 5;

    let underlying = match Underlying::new("SPX") {
        Ok(u) => u,
        Err(e) => panic!("SPX is valid: {e}"),
    };
    let contract = ContractKey {
        underlying: underlying.clone(),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        strike: PriceCents::new(510_000),
        style: OptionStyle::Call,
    };
    let (Ok(spec), Ok(three), Ok(two), Ok(size)) = (
        InstrumentSpec::new(PriceCents::new(TICK), 100),
        Quantity::new(3),
        Quantity::new(2),
        Quantity::new(10),
    ) else {
        panic!("fixture spec/quantities must be valid");
    };

    let mut model = RealisticFill::new(
        FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        10,
        7,
    );
    // Hand-seed a two-level ask ladder: 3 @ 500, 2 @ 505.
    for (price, qty) in [(500u64, three), (505u64, two)] {
        let seeded = model.seed_maker_limit(
            &contract,
            true,
            PriceCents::new(price),
            qty,
            PriceCents::new(TICK),
        );
        assert!(matches!(seeded, Ok(())), "the maker ladder must rest");
    }

    let quote = QuoteView {
        contract: contract.clone(),
        bid: PriceCents::new(490),
        ask: PriceCents::new(500),
        mid: PriceCents::new(495),
        bid_size: size,
        ask_size: size,
        implied_volatility: rust_decimal::Decimal::ZERO,
        delta: rust_decimal::Decimal::ZERO,
        gamma: rust_decimal::Decimal::ZERO,
        theta: rust_decimal::Decimal::ZERO,
        vega: rust_decimal::Decimal::ZERO,
    };
    let mut quotes = BTreeMap::new();
    quotes.insert(contract.clone(), quote);
    let snap = ChainSnapshot {
        ts: SimTime::new(TS0),
        step: StepIndex::new(0),
        underlying,
        underlying_price: PriceCents::new(510_000),
        spec,
        quotes,
    };

    // Marketable buy for 5 (= 3 + 2), captured back as two fills.
    let buy = OrderCommand::Submit(OrderIntent {
        contract: contract.clone(),
        action: PositionAction::Open,
        side: Side::Long,
        quantity: match Quantity::new(5) {
            Ok(q) => q,
            Err(e) => panic!("5 is a valid quantity: {e}"),
        },
        limit: None,
        tif: TimeInForce::Ioc,
        decision_mid: PriceCents::new(500),
    });
    let mut out: Vec<Fill> = Vec::new();
    let result = model.fill(&[buy], &snap, &mut out);
    assert!(matches!(result, Ok(())), "the marketable buy must route");
    assert_eq!(out.len(), 2, "the order walks two seeded levels");

    let (Some(first), Some(second)) = (out.first(), out.get(1)) else {
        panic!("two fills expected");
    };
    assert_eq!(first.price.value(), 500);
    assert_eq!(first.quantity.value(), 3);
    assert_eq!(first.fees.value(), 3 * 65 + 100); // per-contract + once-per-order
    assert_eq!(first.mode, ExecutionMode::Realistic);
    assert_eq!(second.price.value(), 505);
    assert_eq!(second.quantity.value(), 2);
    assert_eq!(second.fees.value(), 2 * 65); // per-contract only (later fill)
    assert_eq!(first.contract, second.contract);
}

/// #023 ladder-walk: a marketable buy walks the **auto-seeded** ask ladder
/// (touch + geometrically-decaying deeper levels) built from the chain snapshot,
/// filling one `Fill` per level at progressively worse prices. Exercises #022's
/// multi-level capture through #023's seeded depth via the public API only.
#[cfg(feature = "orderbook")]
#[test]
fn test_realistic_ladder_walk_through_auto_seeded_depth_yields_per_level_fills() {
    use ironcondor::{
        ExecutionMode, ExecutionModel, FeeSchedule, Fill, OrderCommand, OrderIntent,
        PositionAction, PriceCents, Quantity, RealisticFill, TimeInForce,
    };
    use optionstratlib::Side;

    // ask_size 8, canonical profile (L=3, r=0.5) → ask ladder 8@500,4@505,2@510,1@515.
    let snap = liquidity_fixture::snapshot(490, 500, 8, 8);
    let contract = liquidity_fixture::contract();
    let mut model = RealisticFill::with_liquidity_profile(
        FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        10,
        7,
        liquidity_fixture::canonical_profile(),
    );

    // marketable buy for 14 = 8 + 4 + 2 → walks the first three seeded levels.
    let qty14 = match Quantity::new(14) {
        Ok(q) => q,
        Err(e) => panic!("14 is a valid quantity: {e}"),
    };
    let buy = OrderCommand::Submit(OrderIntent {
        contract,
        action: PositionAction::Open,
        side: Side::Long,
        quantity: qty14,
        limit: None,
        tif: TimeInForce::Ioc,
        decision_mid: PriceCents::new(500),
    });
    let mut out: Vec<Fill> = Vec::new();
    let result = model.fill(&[buy], &snap, &mut out);
    assert!(matches!(result, Ok(())), "the marketable buy must route");
    assert_eq!(out.len(), 3, "the buy walks three auto-seeded ask levels");

    // per-level: (price, size) queue-behind the touch, then walk deeper.
    let levels: Vec<(u64, u32)> = out
        .iter()
        .map(|f| (f.price.value(), f.quantity.value()))
        .collect();
    assert_eq!(levels, vec![(500, 8), (505, 4), (510, 2)]);

    // the once-per-order fee sits on the first level only.
    let (Some(first), Some(second), Some(third)) = (out.first(), out.get(1), out.get(2)) else {
        panic!("three fills expected");
    };
    assert_eq!(first.fees.value(), 8 * 65 + 100);
    assert_eq!(second.fees.value(), 4 * 65);
    assert_eq!(third.fees.value(), 2 * 65);
    assert!(out.iter().all(|f| f.mode == ExecutionMode::Realistic));
}

/// #023 determinism: two models seeded from the same snapshot with the same
/// profile produce **byte-identical** depth — a marketable walk through each
/// yields the identical per-level `Fill` sequence (price + size + order).
#[cfg(feature = "orderbook")]
#[test]
fn test_realistic_seeding_is_byte_identical_across_two_models() {
    use ironcondor::{
        ExecutionModel, FeeSchedule, Fill, OrderCommand, OrderIntent, PositionAction, PriceCents,
        Quantity, RealisticFill, TimeInForce,
    };
    use optionstratlib::Side;

    let fees = FeeSchedule {
        per_contract_cents: 65,
        per_order_cents: 100,
    };
    let walk = |seed: u64| -> Vec<(u64, u32, i64)> {
        let snap = liquidity_fixture::snapshot(490, 500, 8, 8);
        let mut model = RealisticFill::with_liquidity_profile(
            fees,
            10,
            seed,
            liquidity_fixture::canonical_profile(),
        );
        let qty = match Quantity::new(15) {
            Ok(q) => q,
            Err(e) => panic!("15 is a valid quantity: {e}"),
        };
        let buy = OrderCommand::Submit(OrderIntent {
            contract: liquidity_fixture::contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty,
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(500),
        });
        let mut out: Vec<Fill> = Vec::new();
        match model.fill(&[buy], &snap, &mut out) {
            Ok(()) => {}
            Err(e) => panic!("the marketable buy must route: {e}"),
        }
        out.iter()
            .map(|f| (f.price.value(), f.quantity.value(), f.fees.value()))
            .collect()
    };

    // Identical seed ⇒ identical seeded depth ⇒ identical walk.
    assert_eq!(walk(7), walk(7));
    // The seed does not perturb the (deterministic) ladder, either.
    assert_eq!(walk(7), walk(99));
}

/// #024 market impact: the **same order** into a **thin** strike vs a **deep**
/// strike produces different fill quality — the thin strike partial-fills at a
/// worse average price (the order walks up the ladder), the deep strike fills
/// the full intent at the touch. Fill quality is a property of the seeded depth,
/// not a configured knob.
#[cfg(feature = "orderbook")]
#[test]
fn test_realistic_thin_vs_deep_strike_fill_quality_differs_with_depth() {
    use ironcondor::{
        ExecutionModel, FeeSchedule, Fill, OrderCommand, OrderIntent, PositionAction, PriceCents,
        Quantity, RealisticFill, TimeInForce,
    };
    use optionstratlib::Side;

    let fees = FeeSchedule {
        per_contract_cents: 65,
        per_order_cents: 100,
    };
    // The SAME marketable buy for 5, routed into two books of different depth.
    let route = |ask_size: u32| -> Vec<(u64, u32)> {
        // canonical profile (QuotedSize, L=3, r=0.5): the ask ladder is sized
        // from `ask_size` and decays geometrically away from the touch (500).
        let snap = liquidity_fixture::snapshot(490, 500, ask_size, ask_size);
        let mut model = RealisticFill::with_liquidity_profile(
            fees,
            10,
            7,
            liquidity_fixture::canonical_profile(),
        );
        let qty = match Quantity::new(5) {
            Ok(q) => q,
            Err(e) => panic!("5 is a valid quantity: {e}"),
        };
        let buy = OrderCommand::Submit(OrderIntent {
            contract: liquidity_fixture::contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty,
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(500),
        });
        let mut out: Vec<Fill> = Vec::new();
        match model.fill(&[buy], &snap, &mut out) {
            Ok(()) => {}
            Err(e) => panic!("the marketable buy must route: {e}"),
        }
        out.iter()
            .map(|f| (f.price.value(), f.quantity.value()))
            .collect()
    };

    // Thin: ask_size 2 → ladder 2@500, 1@505 (round(2·0.25)=0 stops). A buy for
    // 5 fills only 3 (partial), walking up to 505.
    let thin = route(2);
    // Deep: ask_size 64 → touch 64@500 alone swallows the whole order at 500.
    let deep = route(64);

    let matched = |fills: &[(u64, u32)]| -> u32 { fills.iter().map(|f| f.1).sum() };
    let worst_price =
        |fills: &[(u64, u32)]| -> u64 { fills.iter().map(|f| f.0).max().unwrap_or(0) };

    // The deep strike fills MORE of the intent than the thin one.
    assert!(
        matched(&deep) > matched(&thin),
        "deep filled {} vs thin {}",
        matched(&deep),
        matched(&thin)
    );
    // The thin strike is a PARTIAL fill (< 5); the deep strike is FULL (= 5).
    assert_eq!(matched(&thin), 3, "thin strike partial-fills 3 of 5");
    assert_eq!(matched(&deep), 5, "deep strike fills the full intent");
    // The deep strike's worst executed price is no worse than the thin's — the
    // deep book never walks past the touch, the thin one does (impact).
    assert!(
        worst_price(&deep) < worst_price(&thin),
        "deep worst price {} must beat thin worst price {} (market impact)",
        worst_price(&deep),
        worst_price(&thin)
    );
    // The deep strike is a single-level fill; the thin one walks two levels.
    assert_eq!(deep.len(), 1, "deep fills at the touch in one level");
    assert_eq!(thin.len(), 2, "thin walks two levels");
}

/// #024 `SlippageModel` has **no effect** in realistic mode: two runs whose
/// configs differ **only** in `slippage` produce byte-identical fills, because
/// `RealisticFill` never reads `config.slippage` (its slippage is emergent from
/// the book). Assembled via `BacktestEngine::run` over the full iron condor.
#[cfg(feature = "orderbook")]
#[test]
fn test_realistic_ignores_slippage_model_config() {
    use ironcondor::{BacktestConfig, BacktestEngine, BacktestRun, RealisticFill, SlippageModel};
    use optionstratlib::simulation::ExitPolicy;
    use optionstratlib::strategies::IronCondor;

    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(6, None);
    if let Err(e) = common::write_parquet(&path, &rows) {
        panic!("the condor fixture must write: {e}");
    }

    // Build a realistic run whose config carries `slippage`, and construct the
    // RealisticFill exactly as the config-driven engine path does — from fees,
    // cap, seed, and profile ONLY. `config.slippage` is never consulted.
    let run_with_slippage = |slippage: SlippageModel| -> BacktestRun {
        let mut config: BacktestConfig = common::condor_config(&path, 7);
        config.mode = ironcondor::ExecutionMode::Realistic;
        config.slippage = slippage; // the field under test — must be ignored
        let Ok(feed) = ironcondor::ParquetFeed::open(&path, &ironcondor::ResourceLimits::default())
        else {
            panic!("the fixture opens");
        };
        let Ok(adapter) = ironcondor::OptStratAdapter::<IronCondor>::from_spec(
            &common::iron_condor_spec(),
            ExitPolicy::TimeSteps(1_000_000),
        ) else {
            panic!("the adapter builds");
        };
        let execution = RealisticFill::with_liquidity_profile(
            config.fees,
            config.marketable_cap_ticks,
            config.seed,
            config.liquidity_profile,
        );
        let Ok(run) = BacktestEngine::run(&config, feed, execution, adapter, "iron_condor") else {
            panic!("the realistic run succeeds");
        };
        run
    };

    // No slippage vs a large fixed slippage — realistic mode must ignore both.
    let none = run_with_slippage(SlippageModel::None);
    let fixed = run_with_slippage(SlippageModel::FixedCents { cents: 500 });

    assert_eq!(
        none.equity_curve, fixed.equity_curve,
        "realistic fills must be identical regardless of config.slippage"
    );
    assert_eq!(none.open_at_end, fixed.open_at_end);
}

/// #025 consecutive-snapshot refresh (feature `orderbook`): a resting strategy
/// limit fills **exactly** when a later snapshot's quotes cross it — via a
/// refresh-generated fill on reseed — its price-time priority preserved across
/// refreshes, and earlier seeded depth never leaking into a later step. Driven
/// through the public `RealisticFill` + `ExecutionModel` API over four
/// consecutive snapshots, the normative between-snapshot transition
/// ([docs/04 §6.1](../docs/04-execution-models.md)).
#[cfg(feature = "orderbook")]
#[test]
fn test_realistic_resting_limit_fills_when_consecutive_snapshot_crosses() {
    use ironcondor::{
        ChainSnapshot, ContractKey, ExecutionMode, ExecutionModel, FeeSchedule, Fill,
        InstrumentSpec, LiquidityProfile, OrderCommand, OrderIntent, PositionAction, PriceCents,
        Quantity, QuoteView, RealisticFill, SimTime, StepIndex, TimeInForce, TouchSize, Underlying,
    };
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use std::collections::BTreeMap;

    const TICK: u64 = 5;

    let underlying = match Underlying::new("SPX") {
        Ok(u) => u,
        Err(e) => panic!("SPX is valid: {e}"),
    };
    let contract = ContractKey {
        underlying: underlying.clone(),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        strike: PriceCents::new(510_000),
        style: OptionStyle::Call,
    };

    // A stepped, per-side-sized single-contract snapshot builder.
    let snap = |step: u32, bid: u64, ask: u64, bid_size: u32, ask_size: u32| -> ChainSnapshot {
        let (spec, bq, aq) = match (
            InstrumentSpec::new(PriceCents::new(TICK), 100),
            Quantity::new(bid_size),
            Quantity::new(ask_size),
        ) {
            (Ok(s), Ok(b), Ok(a)) => (s, b, a),
            _ => panic!("spec/sizes must be valid"),
        };
        let quote = QuoteView {
            contract: contract.clone(),
            bid: PriceCents::new(bid),
            ask: PriceCents::new(ask),
            mid: PriceCents::new((bid + ask) / 2),
            bid_size: bq,
            ask_size: aq,
            implied_volatility: rust_decimal::Decimal::ZERO,
            delta: rust_decimal::Decimal::ZERO,
            gamma: rust_decimal::Decimal::ZERO,
            theta: rust_decimal::Decimal::ZERO,
            vega: rust_decimal::Decimal::ZERO,
        };
        let mut quotes = BTreeMap::new();
        quotes.insert(contract.clone(), quote);
        ChainSnapshot {
            ts: SimTime::new(TS0 + i64::from(step)),
            step: StepIndex::new(step),
            underlying: underlying.clone(),
            underlying_price: PriceCents::new(510_000),
            spec,
            quotes,
        }
    };

    // QuotedSize touch, L = 0 — one resting level per side at the quoted touch.
    let profile = LiquidityProfile {
        touch_size: TouchSize::QuotedSize,
        depth_levels: 0,
        decay: rust_decimal::Decimal::new(5, 1),
    };
    let mut model = RealisticFill::with_liquidity_profile(
        FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        10,
        7,
        profile,
    );

    // A resting GTC strategy buy limit at 500 for 1 (decision mid 550).
    let one = match Quantity::new(1) {
        Ok(q) => q,
        Err(e) => panic!("1 is a valid quantity: {e}"),
    };
    let buy = OrderCommand::Submit(OrderIntent {
        contract: contract.clone(),
        action: PositionAction::Open,
        side: Side::Long,
        quantity: one,
        limit: Some(PriceCents::new(500)),
        tif: TimeInForce::Gtc,
        decision_mid: PriceCents::new(550),
    });

    let mut out: Vec<Fill> = Vec::new();

    // snap0: wide ask 600, DEEP size 100. The buy rests below the ask — no fill.
    // This deep seed must NOT leak into later steps.
    match model.fill(&[buy], &snap(0, 490, 600, 100, 100), &mut out) {
        Ok(()) => {}
        Err(e) => panic!("snap0 must route: {e}"),
    }
    assert!(
        out.is_empty(),
        "the buy at 500 does not cross the 600 ask (no fill yet)"
    );

    // snap1: ask narrows to 540 but stays above 500 — the refresh cancels the
    // deep snap0 seed and reseeds thin depth; the strategy order survives,
    // unfilled, its aged priority intact (never cancelled or reinserted).
    out.clear();
    match model.fill(&[], &snap(1, 490, 540, 4, 4), &mut out) {
        Ok(()) => {}
        Err(e) => panic!("snap1 must refresh: {e}"),
    }
    assert!(
        out.is_empty(),
        "the resting strategy order survives the refresh unfilled"
    );

    // snap2: ask crosses at 500 — the resting buy fills via a refresh-generated
    // fill, at its own limit price (aged priority), tagged to the crossing step.
    out.clear();
    match model.fill(&[], &snap(2, 490, 500, 4, 4), &mut out) {
        Ok(()) => {}
        Err(e) => panic!("snap2 must refresh: {e}"),
    }
    assert_eq!(
        out.len(),
        1,
        "exactly one refresh fill when the market crosses"
    );
    let Some(fill) = out.first() else {
        panic!("one refresh fill expected");
    };
    assert_eq!(fill.side, Side::Long);
    assert_eq!(
        fill.price.value(),
        500,
        "fills at the resting limit's own price"
    );
    assert_eq!(fill.quantity.value(), 1);
    assert_eq!(
        fill.step.value(),
        2,
        "the fill belongs to the crossing step"
    );
    assert_eq!(fill.mode, ExecutionMode::Realistic);
    // decision mid 550, bought at 500 (below mid) ⇒ favourable: negative slippage.
    assert_eq!(fill.slippage.value(), -50);

    // No leak: at snap2 the reseed ask (4 @ 500) crossed the buy (1), leaving 3
    // resting. snap3 must cancel that stale 3 and reseed a fresh 4 @ 500. A
    // marketable buy for 10 then fills EXACTLY 4 — were the stale 3 still resting
    // it would fill 7.
    let ten = match Quantity::new(10) {
        Ok(q) => q,
        Err(e) => panic!("10 is a valid quantity: {e}"),
    };
    out.clear();
    match model.fill(
        &[OrderCommand::Submit(OrderIntent {
            contract: contract.clone(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: ten,
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(500),
        })],
        &snap(3, 490, 500, 4, 4),
        &mut out,
    ) {
        Ok(()) => {}
        Err(e) => panic!("snap3 must route: {e}"),
    }
    let matched: u32 = out.iter().map(|f| f.quantity.value()).sum();
    assert_eq!(
        matched, 4,
        "only snap3's fresh 4 @ 500 fills — no earlier seed leaked"
    );
    assert!(
        out.iter().all(|f| f.price.value() == 500),
        "every fill at the fresh touch, none at a stale level"
    );
}

/// #026 cross-mode parity (feature `orderbook`): the **same strategy** over
/// **one scenario** in **both** fill models — selected purely by `config.mode`
/// through the config-driven `run_backtest` dispatch — produces a `Fill` that is
/// **byte-shape identical** across modes (same serialised JSON key set) while
/// the two runs' **P&L diverges** (terminal equity differs). This is the v0.2
/// acceptance headline: the strategy code runs unchanged under either model, and
/// analytics cannot tell which model produced a fill from its structure
/// ([docs/04 §2](../docs/04-execution-models.md#2-the-executionmodel-trait-and-the-shared-fill-report)).
#[cfg(feature = "orderbook")]
#[test]
fn test_cross_mode_parity_same_strategy_shape_identical_pnl_differs() {
    use ironcondor::{
        ExecutionMode, ExecutionModel, FeeSchedule, Fill, NaiveFill, OrderCommand, OrderIntent,
        PositionAction, PriceCents, Quantity, RealisticFill, SlippageModel, TimeInForce,
    };
    use optionstratlib::Side;
    use optionstratlib::simulation::ExitPolicy;

    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir must create");
    };
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(6, None);
    if let Err(e) = common::write_parquet(&path, &rows) {
        panic!("the condor fixture must write: {e}");
    }
    let spec = common::iron_condor_spec();

    // --- (1) P&L divergence: the SAME strategy+scenario, both modes, selected
    // ONLY by `config.mode` through the #026 run_backtest dispatch. ------------
    let naive_config = common::condor_config(&path, 7); // condor_config is mode = Naive
    let mut realistic_config = common::condor_config(&path, 7);
    realistic_config.mode = ExecutionMode::Realistic;

    let (Ok(naive_run), Ok(realistic_run)) = (
        ironcondor::run_backtest(&naive_config, &spec, ExitPolicy::TimeSteps(1_000_000)),
        ironcondor::run_backtest(&realistic_config, &spec, ExitPolicy::TimeSteps(1_000_000)),
    ) else {
        panic!("both config-selected runs must succeed");
    };

    // Same tape ⇒ same number of equity points; the mode changes values, not shape.
    assert_eq!(
        naive_run.equity_curve.len(),
        realistic_run.equity_curve.len()
    );
    let (Some(n_last), Some(r_last)) = (
        naive_run.equity_curve.last(),
        realistic_run.equity_curve.last(),
    ) else {
        panic!("both curves have a terminal point");
    };
    // The two modes DIVERGE: realistic crosses the seeded spread on entry and
    // exit, naive fills at mid — so terminal equity differs (the fill-risk signal
    // the mode exists to surface). An equality here means realistic collapsed
    // into naive.
    assert_ne!(
        n_last.equity_cents, r_last.equity_cents,
        "naive and realistic must produce different P&L on the same scenario"
    );

    // --- (2) Shape parity: route the SAME intent through each fill model and
    // assert the produced `Fill` is byte-shape identical (same serialised JSON
    // key set), so analytics cannot tell which mode produced a bundle. ---------
    let sorted_keys = |fill: &Fill| -> Vec<String> {
        let Ok(value) = serde_json::to_value(fill) else {
            panic!("a Fill must serialise");
        };
        let Some(obj) = value.as_object() else {
            panic!("a Fill serialises as a JSON object");
        };
        let mut keys: Vec<String> = obj.keys().cloned().collect();
        keys.sort();
        keys
    };

    let fees = FeeSchedule {
        per_contract_cents: 65,
        per_order_cents: 100,
    };
    // A depth-seeded snapshot so the realistic model fills the marketable buy.
    let snap = liquidity_fixture::snapshot(490, 500, 8, 8);
    let contract = liquidity_fixture::contract();
    let Ok(three) = Quantity::new(3) else {
        panic!("3 is a valid quantity");
    };
    let buy = OrderCommand::Submit(OrderIntent {
        contract,
        action: PositionAction::Open,
        side: Side::Long,
        quantity: three,
        limit: None,
        tif: TimeInForce::Ioc,
        decision_mid: PriceCents::new(500),
    });

    let mut naive_model = NaiveFill::new(SlippageModel::None, fees);
    let mut realistic_model =
        RealisticFill::with_liquidity_profile(fees, 10, 7, liquidity_fixture::canonical_profile());

    let mut naive_out: Vec<Fill> = Vec::new();
    let mut realistic_out: Vec<Fill> = Vec::new();
    let (Ok(()), Ok(())) = (
        naive_model.fill(std::slice::from_ref(&buy), &snap, &mut naive_out),
        realistic_model.fill(std::slice::from_ref(&buy), &snap, &mut realistic_out),
    ) else {
        panic!("both models must fill the marketable buy");
    };

    let (Some(naive_fill), Some(realistic_fill)) = (naive_out.first(), realistic_out.first())
    else {
        panic!("both models produce at least one fill");
    };
    // Different mode tags — and different prices (naive mid 495 vs realistic touch
    // 500), so the fills genuinely differ in VALUE...
    assert_eq!(naive_fill.mode, ExecutionMode::Naive);
    assert_eq!(realistic_fill.mode, ExecutionMode::Realistic);
    assert_ne!(naive_fill.price, realistic_fill.price);
    // ...but their serialised field SHAPE is identical (mode-agnostic).
    assert_eq!(
        sorted_keys(naive_fill),
        sorted_keys(realistic_fill),
        "the Fill shape must be mode-agnostic (identical serialised key set)"
    );
}
