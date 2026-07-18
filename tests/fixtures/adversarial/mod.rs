//! Adversarial Parquet-feed fixtures — deterministic generators, committed as
//! Rust source (not opaque binary blobs).
//!
//! # Fixture-generation decision (docs/TESTING.md §12.1)
//!
//! §12.1 lists `tests/fixtures/adversarial/` and requires the fixtures be
//! **committed permanent regressions**. This module satisfies that with
//! deterministic *generators* rather than committed `.parquet` binaries,
//! following the established repo convention: golden §4 ("the repo convention
//! is 'no committed binary blob'") and the Cargo.toml dev-dependency note
//! ("Scratch Parquet fixtures are generated into a tempdir in-test — no
//! committed binary"). Every #21 adversarial case is cleanly synthesizable
//! from the shared `tests/common` builder — crossed quote (bid > ask row),
//! negative strike, NaN analytic, reversed / duplicate timestamps, oversized
//! row / contract counts, a decompression bomb (many identical highly-
//! compressible rows), a truncated footer (chopped bytes), and a corrupt row
//! group (flipped data-page bytes) — so none needs an opaque binary. A
//! generator is a reviewable, source-pinned permanent regression; a binary
//! would be neither.
//!
//! Each generator returns `(TempDir, PathBuf)`: the caller holds the `TempDir`
//! to keep the file alive for the duration of the assertion.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::common;

/// Write `rows` (the shared canonical schema, valid analytics) into a fresh
/// tempdir under `name` and return the directory + path.
fn write_rows(name: &str, rows: &[common::Row]) -> Result<(TempDir, PathBuf), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let path = dir.path().join(name);
    common::write_parquet(&path, rows)?;
    Ok((dir, path))
}

/// Re-timestamp a row set by step (steps stay strictly ascending; only `ts`
/// changes), so the tape can be made out-of-order or duplicate-`ts`.
fn retimed(rows: Vec<common::Row>, ts_of: impl Fn(i32) -> i64) -> Vec<common::Row> {
    rows.into_iter()
        .map(|(step, _ts, strike, style, bid, ask)| (step, ts_of(step), strike, style, bid, ask))
        .collect()
}

/// A well-formed single-step tape — the valid base for a ceiling cut-off test
/// (e.g. `max_total_bytes`) that needs a valid file plus a low ceiling.
pub fn well_formed() -> Result<(TempDir, PathBuf), String> {
    write_rows("well_formed.parquet", &common::condor_rows(1, None))
}

/// A crossed quote: a tick-aligned row whose bid exceeds its ask. Expected:
/// [`ironcondor::BacktestError::CrossedQuote`] (the specific typed variant the
/// conversion core raises; §12.1 groups it under `Conversion`).
pub fn crossed_quote() -> Result<(TempDir, PathBuf), String> {
    let rows = common::condor_rows(
        1,
        Some(common::Perturb {
            step: 0,
            strike: 510_000,
            style: "call",
            bid: 2_010,
            ask: 2_000,
        }),
    );
    write_rows("crossed_quote.parquet", &rows)
}

/// A negative strike. Expected: `BacktestError::Conversion` (the feed's
/// integer-cents `u64::try_from` rejects the sign).
pub fn negative_strike() -> Result<(TempDir, PathBuf), String> {
    let rows: Vec<common::Row> = vec![(0, common::TS0, -510_000, "call", 1_995, 2_005)];
    write_rows("negative_strike.parquet", &rows)
}

/// A NaN implied-volatility analytic. Expected: `BacktestError::Conversion`
/// (the conversion core rejects a non-finite analytic).
pub fn nan_analytic() -> Result<(TempDir, PathBuf), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let path = dir.path().join("nan_analytic.parquet");
    write_single_row_with_iv(&path, f64::NAN)?;
    Ok((dir, path))
}

/// A reversed-timestamp tape (steps ascending, `ts` descending). Expected:
/// `BacktestError::DataOutOfOrder`.
pub fn out_of_order_ts() -> Result<(TempDir, PathBuf), String> {
    let rows = retimed(common::condor_rows(2, None), |step| {
        if step == 0 {
            common::TS0 + common::NANOS_PER_DAY
        } else {
            common::TS0
        }
    });
    write_rows("out_of_order_ts.parquet", &rows)
}

/// A duplicate-timestamp tape (steps ascending, `ts` equal). Expected:
/// `BacktestError::DataOutOfOrder` (`ts` must be strictly increasing).
pub fn duplicate_ts() -> Result<(TempDir, PathBuf), String> {
    let rows = retimed(common::condor_rows(2, None), |_| common::TS0);
    write_rows("duplicate_ts.parquet", &rows)
}

/// A multi-step tape; drive it with a low `max_steps` to trip the ceiling.
/// Expected: `BacktestError::TapeTooLarge { limit: "max_steps", .. }`.
pub fn oversized_steps() -> Result<(TempDir, PathBuf), String> {
    write_rows("oversized_steps.parquet", &common::condor_rows(3, None))
}

/// A single step with four contracts; drive it with a low
/// `max_contracts_per_snapshot` to trip the ceiling. Expected:
/// `BacktestError::TapeTooLarge { limit: "max_contracts_per_snapshot", .. }`.
pub fn oversized_contracts() -> Result<(TempDir, PathBuf), String> {
    write_rows("oversized_contracts.parquet", &common::condor_rows(1, None))
}

/// A decompression bomb: many identical, highly-compressible rows — a tiny
/// Snappy footprint whose declared uncompressed row-group size explodes far
/// past a low `max_decompressed_bytes`. The declared-size guard fires BEFORE
/// any decode, so the (deliberately duplicate) rows are never materialised.
/// Expected: `BacktestError::TapeTooLarge { limit: "max_decompressed_bytes", .. }`.
pub fn decompression_bomb() -> Result<(TempDir, PathBuf), String> {
    let rows: Vec<common::Row> = vec![(0, common::TS0, 510_000, "call", 1_995, 2_005); 50_000];
    write_rows("decompression_bomb.parquet", &rows)
}

/// A truncated Parquet footer (the file chopped in half). Expected:
/// `BacktestError::Data` (metadata read fails cleanly, no panic).
pub fn truncated_footer() -> Result<(TempDir, PathBuf), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let valid = dir.path().join("valid.parquet");
    common::write_parquet(&valid, &common::condor_rows(1, None))?;
    let bytes = std::fs::read(&valid).map_err(|e| e.to_string())?;
    let half = bytes.len() / 2;
    let truncated = bytes
        .get(..half)
        .ok_or_else(|| "truncation slice out of range".to_string())?;
    let path = dir.path().join("truncated_footer.parquet");
    std::fs::write(&path, truncated).map_err(|e| e.to_string())?;
    Ok((dir, path))
}

/// A corrupt row group: a valid file whose first data-page bytes (just past the
/// leading `PAR1` magic) are overwritten, leaving the trailing footer intact so
/// the metadata still parses and the failure surfaces in row-group decode.
/// Expected: `BacktestError::Data` (decode fails cleanly, no panic).
pub fn corrupt_row_group() -> Result<(TempDir, PathBuf), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let valid = dir.path().join("valid.parquet");
    common::write_parquet(&valid, &common::condor_rows(1, None))?;
    let mut bytes = std::fs::read(&valid).map_err(|e| e.to_string())?;
    // Flip a run of the first data page (skip the 4-byte "PAR1" header); the
    // trailing footer is left untouched.
    for b in bytes.iter_mut().skip(4).take(48) {
        *b = 0xFF;
    }
    let path = dir.path().join("corrupt_row_group.parquet");
    std::fs::write(&path, &bytes).map_err(|e| e.to_string())?;
    Ok((dir, path))
}

// ---------------------------------------------------------------------------
// CSV adversarial generators (issue #27): each writes a directory of per-step
// CSV chain files to a tempdir and returns the directory path. They mirror the
// Parquet generators above — deterministic, committed as reviewable Rust source.
// ---------------------------------------------------------------------------

/// One canonical CSV quote row (SPX / 5c tick / 100x / absolute 30-day expiry).
fn csv_row(ts: i64, strike: i64, style: &str, bid: i64, ask: i64) -> String {
    format!(
        "{ts},SPX,500000,5,100,{},{strike},{style},{bid},{ask},50,50,0.2,0.3,0.01,-0.05,0.1",
        common::EXPIRY
    )
}

/// Write `(name, content)` CSV files into a fresh tempdir; return dir + the
/// directory path (the [`ironcondor::CsvFeed`] input is the directory itself).
fn write_csv_files(files: &[(&str, String)]) -> Result<(TempDir, PathBuf), String> {
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    for (name, content) in files {
        std::fs::write(dir.path().join(name), content).map_err(|e| e.to_string())?;
    }
    let path = dir.path().to_path_buf();
    Ok((dir, path))
}

/// A well-formed single-step CSV directory — the valid base for a ceiling
/// cut-off test (e.g. `max_total_bytes`) that needs a valid dir + a low ceiling.
pub fn csv_well_formed() -> Result<(TempDir, PathBuf), String> {
    let content = format!(
        "{}\n{}\n{}",
        common::CSV_HEADER,
        csv_row(common::TS0, 510_000, "call", 1_995, 2_005),
        csv_row(common::TS0, 490_000, "put", 1_795, 1_805)
    );
    write_csv_files(&[("step_000.csv", content)])
}

/// A crossed CSV quote (bid > ask, both tick-aligned). Expected:
/// [`ironcondor::BacktestError::CrossedQuote`].
pub fn csv_crossed_quote() -> Result<(TempDir, PathBuf), String> {
    let content = format!(
        "{}\n{}",
        common::CSV_HEADER,
        csv_row(common::TS0, 510_000, "call", 2_010, 2_000)
    );
    write_csv_files(&[("step_000.csv", content)])
}

/// A negative CSV strike. Expected: `BacktestError::Conversion` (the unsigned
/// integer-cents parse rejects the sign).
pub fn csv_negative_strike() -> Result<(TempDir, PathBuf), String> {
    let content = format!(
        "{}\n{}",
        common::CSV_HEADER,
        csv_row(common::TS0, -510_000, "call", 1_995, 2_005)
    );
    write_csv_files(&[("step_000.csv", content)])
}

/// A zero CSV strike. Expected: `BacktestError::Conversion` (the conversion
/// core rejects a non-positive strike).
pub fn csv_zero_strike() -> Result<(TempDir, PathBuf), String> {
    let content = format!(
        "{}\n{}",
        common::CSV_HEADER,
        csv_row(common::TS0, 0, "call", 1_995, 2_005)
    );
    write_csv_files(&[("step_000.csv", content)])
}

/// A NaN implied-volatility CSV analytic. Expected: `BacktestError::Conversion`
/// (the conversion core rejects a non-finite analytic).
pub fn csv_nan_analytic() -> Result<(TempDir, PathBuf), String> {
    let content = format!(
        "{}\n{},SPX,500000,5,100,{},510000,call,1995,2005,50,50,nan,0.3,0.01,-0.05,0.1",
        common::CSV_HEADER,
        common::TS0,
        common::EXPIRY
    );
    write_csv_files(&[("step_000.csv", content)])
}

/// A dollar-denominated float in a money column (`bid = 19.95`). Expected:
/// `BacktestError::Conversion` (money is integer cents; dollar floats rejected).
pub fn csv_dollar_float_money() -> Result<(TempDir, PathBuf), String> {
    let content = format!(
        "{}\n{},SPX,500000,5,100,{},510000,call,19.95,20.05,50,50,0.2,0.3,0.01,-0.05,0.1",
        common::CSV_HEADER,
        common::TS0,
        common::EXPIRY
    );
    write_csv_files(&[("step_000.csv", content)])
}

/// A multi-step CSV directory; drive it with a low `max_steps` to trip the
/// ceiling. Expected: `BacktestError::TapeTooLarge { limit: "max_steps", .. }`.
pub fn csv_oversized_steps() -> Result<(TempDir, PathBuf), String> {
    let files: Vec<(&str, String)> = vec![
        (
            "step_000.csv",
            format!(
                "{}\n{}",
                common::CSV_HEADER,
                csv_row(common::TS0, 510_000, "call", 1_995, 2_005)
            ),
        ),
        (
            "step_001.csv",
            format!(
                "{}\n{}",
                common::CSV_HEADER,
                csv_row(
                    common::TS0 + common::NANOS_PER_DAY,
                    510_000,
                    "call",
                    1_995,
                    2_005
                )
            ),
        ),
        (
            "step_002.csv",
            format!(
                "{}\n{}",
                common::CSV_HEADER,
                csv_row(
                    common::TS0 + 2 * common::NANOS_PER_DAY,
                    510_000,
                    "call",
                    1_995,
                    2_005
                )
            ),
        ),
    ];
    write_csv_files(&files)
}

/// Write one valid canonical row but with `iv` in the `implied_volatility`
/// column — used to inject a NaN analytic the shared builder cannot express.
fn write_single_row_with_iv(path: &Path, iv: f64) -> Result<(), String> {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
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
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![0i32])),
        Arc::new(Int64Array::from(vec![common::TS0])),
        Arc::new(StringArray::from(vec!["SPX"])),
        Arc::new(Int64Array::from(vec![500_000i64])),
        Arc::new(Int64Array::from(vec![5i64])),
        Arc::new(Int32Array::from(vec![100i32])),
        Arc::new(Int64Array::from(vec![common::EXPIRY])),
        Arc::new(Int64Array::from(vec![510_000i64])),
        Arc::new(StringArray::from(vec!["call"])),
        Arc::new(Int64Array::from(vec![1_995i64])),
        Arc::new(Int64Array::from(vec![2_005i64])),
        Arc::new(Int32Array::from(vec![50i32])),
        Arc::new(Int32Array::from(vec![50i32])),
        Arc::new(Float64Array::from(vec![iv])),
        Arc::new(Float64Array::from(vec![0.3f64])),
        Arc::new(Float64Array::from(vec![0.01f64])),
        Arc::new(Float64Array::from(vec![-0.05f64])),
        Arc::new(Float64Array::from(vec![0.1f64])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.to_string())?;
    let file = std::fs::File::create(path).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema, None).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}
