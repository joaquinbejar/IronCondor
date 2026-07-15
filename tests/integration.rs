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
