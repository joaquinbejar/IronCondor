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

// ---------------------------------------------------------------------------
// Bundle read-back adversarial generators (issue #35): each builds a VALID
// `ironcondor.bundle.v1` directory (writer #34), then tampers exactly one facet
// so `ironcondor::read_bundle` rejects it with a typed error — never a panic /
// hang / OOM. Committed as reviewable deterministic generators, not blobs.
// ---------------------------------------------------------------------------

/// Build a valid result bundle: run a small iron condor over a canonical Parquet
/// chain, then publish the bundle. Returns the tempdir (kept alive by the caller)
/// and the published `<run_id>/` directory.
fn build_valid_bundle() -> Result<(TempDir, PathBuf), String> {
    use optionstratlib::simulation::ExitPolicy;

    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let chain = dir.path().join("chain.parquet");
    common::write_parquet(&chain, &common::condor_rows(6, None))?;
    let mut config = common::condor_config(&chain, 7);
    config.output_dir = dir.path().join("bundles");
    let spec = common::iron_condor_spec();
    let run = ironcondor::run_backtest(&config, &spec, ExitPolicy::TimeSteps(1_000_000))
        .map_err(|e| e.to_string())?;
    let bundle = ironcondor::write_bundle(&run, &config, &spec).map_err(|e| e.to_string())?;
    Ok((dir, bundle))
}

/// Mutate `manifest.json` in place through its top-level object.
fn patch_bundle_manifest(
    bundle: &Path,
    mutate: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) -> Result<(), String> {
    let text = std::fs::read_to_string(bundle.join("manifest.json")).map_err(|e| e.to_string())?;
    let mut value: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    let obj = value
        .as_object_mut()
        .ok_or_else(|| "manifest is not an object".to_string())?;
    mutate(obj);
    let bytes = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
    std::fs::write(bundle.join("manifest.json"), bytes).map_err(|e| e.to_string())
}

/// Overwrite `fills.parquet` with a single row whose `contract_id` carries a
/// lowercase underlying — a violation of the `UNDERLYING` grammar, so the id
/// cannot round-trip back to a `ContractKey`.
fn write_one_bad_contract_fill(bundle: &Path, run_id: &str) -> Result<(), String> {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("strategy_run_id", DataType::Utf8, false),
        Field::new("trade_id", DataType::Int64, false),
        Field::new("position_id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("fill_seq", DataType::Int32, false),
        Field::new("underlying", DataType::Utf8, false),
        Field::new("expiration_ns", DataType::Int64, false),
        Field::new("contract_id", DataType::Utf8, false),
        Field::new("strike_cents", DataType::Int64, false),
        Field::new("style", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("price_cents", DataType::Int64, false),
        Field::new("fees_cents", DataType::Int64, false),
        Field::new("slippage_cents", DataType::Int64, false),
        Field::new("mode", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![0i32])) as ArrayRef,
        Arc::new(Int64Array::from(vec![common::TS0])),
        Arc::new(StringArray::from(vec![run_id])),
        Arc::new(Int64Array::from(vec![1i64])),
        Arc::new(Int64Array::from(vec![1i64])),
        Arc::new(Int64Array::from(vec![1i64])),
        Arc::new(Int32Array::from(vec![0i32])),
        Arc::new(StringArray::from(vec!["SPX"])),
        Arc::new(Int64Array::from(vec![common::EXPIRY])),
        // Lowercase underlying ⇒ non-round-trippable contract_id.
        Arc::new(StringArray::from(vec![format!(
            "v1:spx:{}:510000:C",
            common::EXPIRY
        )])),
        Arc::new(Int64Array::from(vec![510_000i64])),
        Arc::new(StringArray::from(vec!["call"])),
        Arc::new(StringArray::from(vec!["short"])),
        Arc::new(Int32Array::from(vec![1i32])),
        Arc::new(Int64Array::from(vec![2_000i64])),
        Arc::new(Int64Array::from(vec![65i64])),
        Arc::new(Int64Array::from(vec![0i64])),
        Arc::new(StringArray::from(vec!["naive"])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.to_string())?;
    let file = std::fs::File::create(bundle.join("fills.parquet")).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema, None).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}

/// A well-formed bundle — the valid base for a read-back ceiling cut-off test.
pub fn bundle_well_formed() -> Result<(TempDir, PathBuf), String> {
    build_valid_bundle()
}

/// A bundle whose `manifest.schema` is an unknown major tag. Expected:
/// `BacktestError::Bundle`, rejected **before** any table is parsed.
pub fn bundle_wrong_schema_tag() -> Result<(TempDir, PathBuf), String> {
    let (dir, bundle) = build_valid_bundle()?;
    patch_bundle_manifest(&bundle, |obj| {
        obj.insert(
            "schema".to_string(),
            serde_json::Value::String("ironcondor.bundle.v2".to_string()),
        );
    })?;
    Ok((dir, bundle))
}

/// A bundle whose `manifest.row_counts.fills` is inflated far past the decoded
/// table length. Expected: `BacktestError::Bundle` (`row_counts` mismatch).
pub fn bundle_oversized_row_counts() -> Result<(TempDir, PathBuf), String> {
    let (dir, bundle) = build_valid_bundle()?;
    patch_bundle_manifest(&bundle, |obj| {
        if let Some(counts) = obj.get_mut("row_counts").and_then(|v| v.as_object_mut()) {
            counts.insert(
                "fills".to_string(),
                serde_json::Value::from(1_000_000_000_u64),
            );
        }
    })?;
    Ok((dir, bundle))
}

/// A bundle whose `fills.parquet` is truncated to half its bytes (a chopped
/// footer). Expected: `BacktestError::Bundle` (metadata read fails cleanly).
pub fn bundle_truncated_parquet_footer() -> Result<(TempDir, PathBuf), String> {
    let (dir, bundle) = build_valid_bundle()?;
    let table = bundle.join("fills.parquet");
    let bytes = std::fs::read(&table).map_err(|e| e.to_string())?;
    let half = bytes.len() / 2;
    let truncated = bytes
        .get(..half)
        .ok_or_else(|| "truncation slice out of range".to_string())?;
    std::fs::write(&table, truncated).map_err(|e| e.to_string())?;
    Ok((dir, bundle))
}

/// A bundle whose `fills.parquet` carries a non-round-trippable `contract_id`
/// (a lowercase underlying). Expected: `BacktestError::Bundle` (round-trip fails
/// before the id is used as a join key).
pub fn bundle_non_round_trippable_contract_id() -> Result<(TempDir, PathBuf), String> {
    let (dir, bundle) = build_valid_bundle()?;
    let run_id = bundle
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| "bundle dir name".to_string())?
        .to_string();
    write_one_bad_contract_fill(&bundle, &run_id)?;
    // Keep row_counts consistent with the single replacement row, so the failure
    // is specifically the contract_id round-trip (not a row_counts mismatch).
    patch_bundle_manifest(&bundle, |obj| {
        if let Some(counts) = obj.get_mut("row_counts").and_then(|v| v.as_object_mut()) {
            counts.insert("fills".to_string(), serde_json::Value::from(1_u64));
        }
    })?;
    Ok((dir, bundle))
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
