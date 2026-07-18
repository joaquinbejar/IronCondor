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

use ironcondor::{DataFeed, DataSourceSpec, ParquetFeed, ResourceLimits};

mod common;

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
