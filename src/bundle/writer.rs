//! The result-bundle **writer** — `manifest.json` plus the four Parquet tables,
//! published atomically ([docs/05 §5](../../../docs/05-analytics-and-reporting.md#5-the-ironcondor-result-bundle),
//! [docs/05 §11](../../../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism),
//! [ADR-0004](../../../docs/adr/0004-parquet-result-bundle.md)).
//!
//! # What it writes
//!
//! [`write_bundle`] turns a completed [`BacktestRun`] into the portable
//! `<run_id>/` directory ChainView and notebooks consume:
//!
//! ```text
//! <run_id>/
//! ├── manifest.json               # canonical JSON, sorted keys (§6)
//! ├── fills.parquet               # one row per executed fill (§7)
//! ├── equity_curve.parquet        # per-step mark-to-market ledger (§8)
//! ├── positions.parquet           # per-step open-leg snapshots (§9)
//! └── greeks_attribution.parquet  # per-step P&L decomposition (§10)
//! ```
//!
//! The [`Manifest`] is the **single serialization source** for `manifest.json`
//! (#33); it is written **last**, so a reader that sees the manifest sees the
//! complete tables. `created_utc` is the **only** wall-clock value in the whole
//! system and lives in the JSON manifest **only** — never in Parquet metadata.
//!
//! # Determinism ([docs/05 §11](../../../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism))
//!
//! In the same environment two identical runs produce **byte-identical** Parquet
//! tables and a manifest byte-identical **after stripping `created_utc`**:
//!
//! - Every table is written in its fixed sort order (`fills` by
//!   `(step, order_id, fill_seq)`, `positions` by `(step, position_id)`, the two
//!   per-step tables by `step`) — no `HashMap` iteration reaches a row.
//! - Parquet is written with a **pinned codec** ([`Compression::SNAPPY`]) and a
//!   **pinned `created_by`** string, so the file bytes do not vary with the
//!   `parquet` crate version or a per-run timestamp.
//! - `run_id` is [`RunId::derive`]d from the reproducibility tuple; the manifest
//!   is canonical JSON (sorted keys, [`Decimal`](rust_decimal::Decimal)s as
//!   lossless strings) via a single `serde_json::Value` round-trip.
//!
//! # Atomic publish
//!
//! The bundle is built in a **temporary sibling** directory and `rename`d into
//! `<output_dir>/<run_id>` on success, so a reader sees a complete bundle or
//! none; a failed run cleans the temp directory and leaves nothing behind. A
//! destination collision fails [`BacktestError::Bundle`] unless `config.overwrite`
//! is set, which replaces the **same** `<run_id>` (never re-points the run —
//! `overwrite` and the output path are excluded from the `run_id` hash).
//!
//! # Streaming (PB-5)
//!
//! Each table is encoded in fixed-size row-group batches
//! ([`WRITE_BATCH_ROWS`]), so the encode is a single O(rows) pass with peak
//! memory bounded by one batch, not the run length
//! ([docs/07 §3](../../../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite),
//! measured in #37). The writer is termination-phase, **outside** the PB-1
//! zero-alloc boundary — free to allocate.
//!
//! # Errors
//!
//! Every I/O, Arrow, and Parquet failure converts into [`BacktestError::Bundle`];
//! there is no `.unwrap()` / `.expect()` on the write path, and every `u64 → i64`
//! / `u32 → i32` wire narrowing is a checked `try_from`.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use chrono::Utc;
use optionstratlib::backtesting::ExitReason;
use optionstratlib::{OptionStyle, Side};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};

use crate::analytics::metrics::Metrics;
use crate::bundle::schema::{
    BUNDLE_SCHEMA, Manifest, RowCounts, RunId, equity_sort_key, greeks_sort_key,
};
use crate::config::BacktestConfig;
use crate::domain::{EquityPoint, ExecutionMode, GreeksAttributionRow, StrategySpec};
use crate::engine::{BacktestRun, FillRecord, PositionSnapshot};
use crate::error::BacktestError;

/// Rows encoded per Parquet row-group batch — bounds the writer's peak memory
/// independently of the run length (PB-5).
const WRITE_BATCH_ROWS: usize = 8_192;

/// The pinned Parquet `created_by` string — fixed so the file bytes do not vary
/// with the `parquet` crate version (reproducibility,
/// [docs/05 §11](../../../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism)).
const PARQUET_CREATED_BY: &str = "ironcondor result bundle v1";

/// The `ironcondor` crate version — the build identity's `code_version` hashed
/// into `run_id` and recorded in the manifest.
const CODE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The build's `Cargo.lock`, embedded at compile time so `lockfile_sha256`
/// (the build identity hashed into `run_id`) is a pure function of the build,
/// with no runtime file read.
const CARGO_LOCK: &str = include_str!("../../Cargo.lock");

/// Write the result bundle for `run` into `config.output_dir`, returning the
/// published `<output_dir>/<run_id>` directory.
///
/// The bundle is built in a temporary sibling directory and renamed into place
/// on success (atomic publish); the four Parquet tables are written in their
/// fixed sort order with a pinned codec, and `manifest.json` (canonical JSON) is
/// written last. `run` must carry the analytics the manifest reports — the
/// composition root runs [`crate::analytics::metrics::populate`] and
/// [`crate::analytics::attribution::attribute`] before calling this.
///
/// # Errors
///
/// - [`BacktestError::Bundle`] if the destination `<run_id>` directory already
///   exists and `config.overwrite` is `false`, or on any I/O / Arrow / Parquet
///   failure while encoding a table or the manifest (the temp directory is
///   cleaned and nothing is published).
/// - [`BacktestError::ArithmeticOverflow`] / [`BacktestError::Conversion`] if a
///   row's wire narrowing overflows or a contract's identity cannot be built.
pub fn write_bundle(
    run: &BacktestRun,
    config: &BacktestConfig,
    strategy: &StrategySpec,
) -> Result<PathBuf, BacktestError> {
    let lockfile_sha = to_hex(&Sha256::digest(CARGO_LOCK.as_bytes()));
    let run_id = RunId::derive(
        config.seed,
        config,
        strategy,
        &run.data_identity,
        CODE_VERSION,
        &lockfile_sha,
    )?;

    let output_dir = config.output_dir.as_path();
    let dest = output_dir.join(run_id.as_str());

    // Destination collision — fail unless `overwrite` authorises replacing the
    // SAME <run_id> directory (it never re-points the run).
    let dest_exists = dest
        .try_exists()
        .map_err(|e| bundle_err("stat destination", &e))?;
    if dest_exists && !config.overwrite {
        return Err(BacktestError::Bundle(format!(
            "bundle directory already exists: {} (set overwrite to replace the same run_id)",
            dest.display()
        )));
    }

    std::fs::create_dir_all(output_dir).map_err(|e| bundle_err("create output directory", &e))?;

    // Build in a temporary sibling directory, cleaning any leftover from a
    // crashed run first.
    let temp = output_dir.join(format!(".{}.partial", run_id.as_str()));
    if temp
        .try_exists()
        .map_err(|e| bundle_err("stat temp directory", &e))?
    {
        std::fs::remove_dir_all(&temp).map_err(|e| bundle_err("remove leftover temp", &e))?;
    }
    std::fs::create_dir(&temp).map_err(|e| bundle_err("create temp directory", &e))?;

    // Encode the tables + manifest into the temp directory; on ANY failure clean
    // the temp directory and propagate, so no partial bundle is ever published.
    let row_counts = match build_into(run, config, strategy, &run_id, &lockfile_sha, &temp) {
        Ok(counts) => counts,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&temp);
            return Err(error);
        }
    };

    // Publish atomically: replace an existing SAME-run_id destination (overwrite)
    // then rename the temp into place.
    if dest_exists {
        std::fs::remove_dir_all(&dest)
            .map_err(|e| bundle_err("remove existing destination", &e))?;
    }
    if let Err(error) = std::fs::rename(&temp, &dest) {
        let _ = std::fs::remove_dir_all(&temp);
        return Err(bundle_err("atomic rename into place", &error));
    }

    tracing::info!(
        run_id = run_id.as_str(),
        path = %dest.display(),
        fills = row_counts.fills,
        equity_curve = row_counts.equity_curve,
        positions = row_counts.positions,
        greeks_attribution = row_counts.greeks_attribution,
        "result bundle written"
    );
    Ok(dest)
}

/// Encode the four tables and the manifest into `temp`, returning the per-table
/// row counts. The manifest is written **last**, so its presence implies the
/// complete tables.
fn build_into(
    run: &BacktestRun,
    config: &BacktestConfig,
    strategy: &StrategySpec,
    run_id: &RunId,
    lockfile_sha: &str,
    temp: &Path,
) -> Result<RowCounts, BacktestError> {
    let fills = encode_fills(run, run_id.as_str(), &temp.join("fills.parquet"))?;
    let equity = encode_equity(run, &temp.join("equity_curve.parquet"))?;
    let positions = encode_positions(run, &temp.join("positions.parquet"))?;
    let greeks = encode_greeks(run, &temp.join("greeks_attribution.parquet"))?;
    let row_counts = RowCounts::new(fills, equity, positions, greeks);

    let manifest = Manifest {
        schema: BUNDLE_SCHEMA.to_string(),
        run_id: run_id.clone(),
        // The ONLY wall-clock value in the system — provenance only, stripped
        // before the byte comparison, and never in Parquet metadata.
        created_utc: Utc::now().to_rfc3339(),
        code_version: CODE_VERSION.to_string(),
        lockfile_sha256: lockfile_sha.to_string(),
        seed: config.seed,
        config: config.clone(),
        strategy: strategy.clone(),
        data_source: run.data_source.clone(),
        metrics: Metrics::from_result(&run.result),
        row_counts,
    };
    write_manifest(&manifest, &temp.join("manifest.json"))?;
    Ok(row_counts)
}

/// Serialise the manifest as **canonical JSON** (sorted keys, via a single
/// `serde_json::Value` round-trip — `serde_json` Maps are `BTreeMap`, and
/// `Decimal` serialises as a lossless string) and write it, newline-terminated.
fn write_manifest(manifest: &Manifest, path: &Path) -> Result<(), BacktestError> {
    let value =
        serde_json::to_value(manifest).map_err(|e| bundle_err("manifest to json value", &e))?;
    let mut json =
        serde_json::to_string_pretty(&value).map_err(|e| bundle_err("manifest to json", &e))?;
    json.push('\n');
    std::fs::write(path, json).map_err(|e| bundle_err("write manifest.json", &e))
}

// ---------------------------------------------------------------------------
// Table encoders — each sorts into its fixed key order, then streams row-group
// batches with pinned writer properties.
// ---------------------------------------------------------------------------

/// The pinned Parquet writer properties — a fixed codec and `created_by`, so two
/// identical runs produce byte-identical files (no per-run metadata).
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .build()
}

/// Open a pinned Parquet writer at `path` over `schema`.
fn open_writer(schema: &SchemaRef, path: &Path) -> Result<ArrowWriter<File>, BacktestError> {
    let file = File::create(path).map_err(|e| bundle_err("create parquet file", &e))?;
    ArrowWriter::try_new(file, Arc::clone(schema), Some(writer_properties()))
        .map_err(|e| bundle_err("open parquet writer", &e))
}

/// Encode `fills.parquet` — one row per executed fill, sorted by the unique key
/// `(step, order_id, fill_seq)` ([docs/05 §7](../../../docs/05-analytics-and-reporting.md#7-fillsparquet)).
fn encode_fills(run: &BacktestRun, run_id: &str, path: &Path) -> Result<u64, BacktestError> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
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

    // Sort by FILLS_SORT_COLUMNS = (step, order_id, fill_seq), the unique key.
    let mut order: Vec<&FillRecord> = run.fills.iter().collect();
    order.sort_by_key(|record| (record.fill.step.value(), record.order_id, record.fill_seq));
    let count = row_count(order.len())?;

    let mut writer = open_writer(&schema, path)?;
    for chunk in order.chunks(WRITE_BATCH_ROWS) {
        let n = chunk.len();
        let (mut step, mut ts) = (Vec::with_capacity(n), Vec::with_capacity(n));
        let mut run_ids = Vec::with_capacity(n);
        let (mut trade, mut position, mut order_ids) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let mut fill_seq = Vec::with_capacity(n);
        let (mut underlying, mut expiration, mut contract) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let (mut strike, mut style, mut side) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let (mut quantity, mut price, mut fees, mut slippage, mut mode) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        for record in chunk {
            let fill = &record.fill;
            step.push(i32_from_u32(fill.step.value())?);
            ts.push(fill.ts.value());
            run_ids.push(run_id);
            trade.push(i64_from_u64(record.trade_id)?);
            position.push(i64_from_u64(record.position_id)?);
            order_ids.push(i64_from_u64(record.order_id)?);
            fill_seq.push(i32_from_u32(record.fill_seq)?);
            underlying.push(fill.contract.underlying.as_str());
            expiration.push(fill.contract.expiration_ns()?);
            contract.push(fill.contract.to_contract_id()?);
            strike.push(i64_from_u64(fill.contract.strike.value())?);
            style.push(style_str(fill.contract.style));
            side.push(side_str(fill.side));
            quantity.push(i32_from_u32(fill.quantity.value())?);
            price.push(i64_from_u64(fill.price.value())?);
            fees.push(fill.fees.value());
            slippage.push(fill.slippage.value());
            mode.push(mode_str(fill.mode));
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(step)) as ArrayRef,
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from_iter_values(run_ids)),
            Arc::new(Int64Array::from(trade)),
            Arc::new(Int64Array::from(position)),
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Int32Array::from(fill_seq)),
            Arc::new(StringArray::from_iter_values(underlying)),
            Arc::new(Int64Array::from(expiration)),
            Arc::new(StringArray::from_iter_values(contract)),
            Arc::new(Int64Array::from(strike)),
            Arc::new(StringArray::from_iter_values(style)),
            Arc::new(StringArray::from_iter_values(side)),
            Arc::new(Int32Array::from(quantity)),
            Arc::new(Int64Array::from(price)),
            Arc::new(Int64Array::from(fees)),
            Arc::new(Int64Array::from(slippage)),
            Arc::new(StringArray::from_iter_values(mode)),
        ];
        write_batch(&mut writer, &schema, columns)?;
    }
    close_writer(writer)?;
    Ok(count)
}

/// Encode `equity_curve.parquet` — one row per step, sorted by `step`
/// ([docs/05 §8](../../../docs/05-analytics-and-reporting.md#8-equity_curveparquet)).
fn encode_equity(run: &BacktestRun, path: &Path) -> Result<u64, BacktestError> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("cash_cents", DataType::Int64, false),
        Field::new("position_value_cents", DataType::Int64, false),
        Field::new("equity_cents", DataType::Int64, false),
        Field::new("drawdown", DataType::Float64, false),
    ]));

    let mut order: Vec<&EquityPoint> = run.equity_curve.iter().collect();
    order.sort_by_key(|point| equity_sort_key(point));
    let count = row_count(order.len())?;

    let mut writer = open_writer(&schema, path)?;
    for chunk in order.chunks(WRITE_BATCH_ROWS) {
        let n = chunk.len();
        let (mut step, mut ts, mut cash, mut pos_value, mut equity, mut drawdown) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        for point in chunk {
            step.push(i32_from_u32(point.step)?);
            ts.push(point.ts_ns);
            cash.push(point.cash_cents);
            pos_value.push(point.position_value_cents);
            equity.push(point.equity_cents);
            drawdown.push(guard_finite(point.drawdown, "drawdown")?);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(step)) as ArrayRef,
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int64Array::from(cash)),
            Arc::new(Int64Array::from(pos_value)),
            Arc::new(Int64Array::from(equity)),
            Arc::new(Float64Array::from(drawdown)),
        ];
        write_batch(&mut writer, &schema, columns)?;
    }
    close_writer(writer)?;
    Ok(count)
}

/// Encode `positions.parquet` — one row per open leg per step (plus a terminal
/// row at close), sorted by `(step, position_id)`; `exit_reason` is the only
/// nullable column ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet)).
fn encode_positions(run: &BacktestRun, path: &Path) -> Result<u64, BacktestError> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("position_id", DataType::Int64, false),
        Field::new("trade_id", DataType::Int64, false),
        Field::new("contract_id", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("avg_price_cents", DataType::Int64, false),
        Field::new("mark_cents", DataType::Int64, false),
        Field::new("unrealized_cents", DataType::Int64, false),
        Field::new("stale_mark", DataType::Boolean, false),
        Field::new("exit_reason", DataType::Utf8, true),
        Field::new("open_at_end", DataType::Boolean, false),
    ]));

    // Sort by POSITIONS_SORT_COLUMNS = (step, position_id), ≤ 1 row per step.
    let mut order: Vec<&PositionSnapshot> = run.positions.iter().collect();
    order.sort_by_key(|row| (row.step, row.position_id));
    let count = row_count(order.len())?;

    let mut writer = open_writer(&schema, path)?;
    for chunk in order.chunks(WRITE_BATCH_ROWS) {
        let n = chunk.len();
        let (mut step, mut ts, mut position, mut trade, mut contract) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let (mut side, mut quantity, mut avg_price, mut mark, mut unrealized) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let (mut stale, mut exit_reason, mut open_at_end) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        for row in chunk {
            step.push(i32_from_u32(row.step)?);
            ts.push(row.ts_ns);
            position.push(i64_from_u64(row.position_id)?);
            trade.push(i64_from_u64(row.trade_id)?);
            contract.push(row.contract.to_contract_id()?);
            side.push(side_str(row.side));
            quantity.push(i32_from_u32(row.quantity)?);
            avg_price.push(i64_from_u64(row.avg_price_cents)?);
            mark.push(i64_from_u64(row.mark_cents)?);
            unrealized.push(row.unrealized_cents);
            stale.push(row.stale_mark);
            exit_reason.push(row.exit_reason.as_ref().map(exit_reason_str));
            open_at_end.push(row.open_at_end);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(step)) as ArrayRef,
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int64Array::from(position)),
            Arc::new(Int64Array::from(trade)),
            Arc::new(StringArray::from_iter_values(contract)),
            Arc::new(StringArray::from_iter_values(side)),
            Arc::new(Int32Array::from(quantity)),
            Arc::new(Int64Array::from(avg_price)),
            Arc::new(Int64Array::from(mark)),
            Arc::new(Int64Array::from(unrealized)),
            Arc::new(BooleanArray::from(stale)),
            Arc::new(StringArray::from_iter(exit_reason)),
            Arc::new(BooleanArray::from(open_at_end)),
        ];
        write_batch(&mut writer, &schema, columns)?;
    }
    close_writer(writer)?;
    Ok(count)
}

/// Encode `greeks_attribution.parquet` — one row per step, sorted by `step`
/// ([docs/05 §10](../../../docs/05-analytics-and-reporting.md#10-greeks_attributionparquet)).
fn encode_greeks(run: &BacktestRun, path: &Path) -> Result<u64, BacktestError> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("theta_pnl_cents", DataType::Int64, false),
        Field::new("delta_pnl_cents", DataType::Int64, false),
        Field::new("vega_pnl_cents", DataType::Int64, false),
        Field::new("spread_capture_cents", DataType::Int64, false),
        Field::new("fees_cents", DataType::Int64, false),
        Field::new("residual_cents", DataType::Int64, false),
    ]));

    let mut order: Vec<&GreeksAttributionRow> = run.greeks_attribution.iter().collect();
    order.sort_by_key(|row| greeks_sort_key(row));
    let count = row_count(order.len())?;

    let mut writer = open_writer(&schema, path)?;
    for chunk in order.chunks(WRITE_BATCH_ROWS) {
        let n = chunk.len();
        let (mut step, mut ts, mut theta, mut delta) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        let (mut vega, mut spread, mut fees, mut residual) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        for row in chunk {
            step.push(i32_from_u32(row.step)?);
            ts.push(row.ts_ns);
            theta.push(row.theta_pnl_cents);
            delta.push(row.delta_pnl_cents);
            vega.push(row.vega_pnl_cents);
            spread.push(row.spread_capture_cents);
            fees.push(row.fees_cents);
            residual.push(row.residual_cents);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(step)) as ArrayRef,
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int64Array::from(theta)),
            Arc::new(Int64Array::from(delta)),
            Arc::new(Int64Array::from(vega)),
            Arc::new(Int64Array::from(spread)),
            Arc::new(Int64Array::from(fees)),
            Arc::new(Int64Array::from(residual)),
        ];
        write_batch(&mut writer, &schema, columns)?;
    }
    close_writer(writer)?;
    Ok(count)
}

// ---------------------------------------------------------------------------
// Small, no-panic helpers
// ---------------------------------------------------------------------------

/// Build and write one row-group batch.
fn write_batch(
    writer: &mut ArrowWriter<File>,
    schema: &SchemaRef,
    columns: Vec<ArrayRef>,
) -> Result<(), BacktestError> {
    let batch = RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| bundle_err("build record batch", &e))?;
    writer
        .write(&batch)
        .map_err(|e| bundle_err("write record batch", &e))
}

/// Flush the writer's footer.
fn close_writer(writer: ArrowWriter<File>) -> Result<(), BacktestError> {
    writer
        .close()
        .map(|_metadata| ())
        .map_err(|e| bundle_err("close parquet writer", &e))
}

/// A table's row count as `u64` for `RowCounts` (checked).
fn row_count(len: usize) -> Result<u64, BacktestError> {
    u64::try_from(len).map_err(|_| BacktestError::ArithmeticOverflow)
}

/// Narrow a `u64` id / cents value to the physical signed `INT64` wire type.
fn i64_from_u64(value: u64) -> Result<i64, BacktestError> {
    i64::try_from(value)
        .map_err(|_| BacktestError::Bundle(format!("value {value} exceeds the INT64 wire range")))
}

/// Narrow a `u32` step / quantity / fill_seq to the physical signed `INT32`
/// wire type.
fn i32_from_u32(value: u32) -> Result<i32, BacktestError> {
    i32::try_from(value)
        .map_err(|_| BacktestError::Bundle(format!("value {value} exceeds the INT32 wire range")))
}

/// Guard a derived analytic `f64` before it enters a Parquet column — a `NaN` or
/// `±∞` is a typed error, never written ([`rules/global_rules.md`] float
/// discipline).
fn guard_finite(value: f64, column: &str) -> Result<f64, BacktestError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(BacktestError::Bundle(format!(
            "column {column} is not finite ({value})"
        )))
    }
}

/// The wire string for an option style.
const fn style_str(style: OptionStyle) -> &'static str {
    match style {
        OptionStyle::Call => "call",
        OptionStyle::Put => "put",
    }
}

/// The wire string for a side.
const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Long => "long",
        Side::Short => "short",
    }
}

/// The wire string for an execution mode.
const fn mode_str(mode: ExecutionMode) -> &'static str {
    match mode {
        ExecutionMode::Naive => "naive",
        ExecutionMode::Realistic => "realistic",
    }
}

/// The wire string for an [`ExitReason`] — snake_case for the unit variants, the
/// descriptive string verbatim for [`ExitReason::Other`]
/// ([docs/05 §9](../../../docs/05-analytics-and-reporting.md#9-positionsparquet)).
fn exit_reason_str(reason: &ExitReason) -> String {
    match reason {
        ExitReason::TargetReached => "target_reached".to_string(),
        ExitReason::StopLoss => "stop_loss".to_string(),
        ExitReason::Expiration => "expiration".to_string(),
        ExitReason::RollOver => "roll_over".to_string(),
        ExitReason::ManualClose => "manual_close".to_string(),
        ExitReason::MarginCall => "margin_call".to_string(),
        ExitReason::Other(text) => text.clone(),
    }
}

/// Wrap a failed I/O / Arrow / Parquet operation as a [`BacktestError::Bundle`].
fn bundle_err(context: &str, error: &dyn std::fmt::Display) -> BacktestError {
    BacktestError::Bundle(format!("{context}: {error}"))
}

/// Lower-hex encode a byte slice (the `lockfile_sha256` string form).
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use chrono::DateTime;
    use optionstratlib::backtesting::{BacktestResult, ExitReason};
    use optionstratlib::{ExpirationDate, OptionStyle, Side};

    use super::write_bundle;
    use crate::config::{BacktestConfig, FeeSchedule, SlippageModel};
    use crate::data::DataSourceSpec;
    use crate::domain::{
        Cents, ContractKey, ExecutionMode, Fill, IronCondorSpec, PriceCents, Quantity, SimTime,
        StepIndex, StrategySpec, Underlying,
    };
    use crate::engine::{BacktestRun, FillRecord, PositionSnapshot};
    use crate::error::BacktestError;

    const TS0: i64 = 1_750_291_200_000_000_000;

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX valid");
        };
        u
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} valid");
        };
        q
    }

    fn key() -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(TS0)),
            strike: PriceCents::new(510_000),
            style: OptionStyle::Call,
        }
    }

    fn unresolved_key() -> ContractKey {
        let Ok(days) = positive::Positive::new(30.0) else {
            panic!("30 valid");
        };
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::Days(days),
            strike: PriceCents::new(510_000),
            style: OptionStyle::Call,
        }
    }

    fn fill(contract: ContractKey) -> Fill {
        Fill {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            contract,
            side: Side::Short,
            quantity: qty(1),
            price: PriceCents::new(2_000),
            fees: Cents::new(65),
            slippage: Cents::new(0),
            mode: ExecutionMode::Naive,
        }
    }

    fn strategy() -> StrategySpec {
        StrategySpec::IronCondor(IronCondorSpec {
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            short_call_strike: PriceCents::new(510_000),
            short_put_strike: PriceCents::new(490_000),
            long_call_strike: PriceCents::new(520_000),
            long_put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(TS0)),
            implied_volatility: rust_decimal::Decimal::new(20, 2),
            risk_free_rate: rust_decimal::Decimal::new(5, 2),
            dividend_yield: rust_decimal::Decimal::ZERO,
            quantity: qty(1),
            premium_short_call: PriceCents::new(2_000),
            premium_short_put: PriceCents::new(1_800),
            premium_long_call: PriceCents::new(800),
            premium_long_put: PriceCents::new(700),
            open_fee: PriceCents::new(65),
            close_fee: PriceCents::new(65),
        })
    }

    fn config(output_dir: &Path, overwrite: bool) -> BacktestConfig {
        BacktestConfig {
            data_source: DataSourceSpec::Parquet {
                path: "chains/spx.parquet".to_string(),
                sha256: "abc".to_string(),
            },
            mode: ExecutionMode::Naive,
            seed: 42,
            initial_capital: 10_000_000,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            slippage: SlippageModel::None,
            marketable_cap_ticks: 10,
            liquidity_profile: crate::config::LiquidityProfile::default(),
            limits: crate::config::ResourceLimits::default(),
            output_dir: output_dir.to_path_buf(),
            overwrite,
        }
    }

    /// A minimal but complete run: one open fill, one open position row.
    fn sample_run(contract: ContractKey) -> BacktestRun {
        BacktestRun {
            result: BacktestResult::default(),
            equity_curve: vec![crate::domain::EquityPoint::new(
                0, TS0, 10_020_000, -20_000, 10_000_000, 0.0,
            )],
            open_at_end: Vec::new(),
            trade_log: Vec::new(),
            attribution_substrate: crate::engine::AttributionSubstrate::default(),
            greeks_attribution: vec![crate::domain::GreeksAttributionRow::new(
                0, TS0, 0, 0, 0, 0, 65, 1_935,
            )],
            fills: vec![FillRecord {
                fill: fill(contract.clone()),
                trade_id: 1,
                position_id: 1,
                order_id: 1,
                fill_seq: 0,
            }],
            positions: vec![PositionSnapshot {
                step: 0,
                ts_ns: TS0,
                position_id: 1,
                trade_id: 1,
                contract,
                side: Side::Short,
                quantity: 1,
                avg_price_cents: 2_000,
                mark_cents: 1_950,
                unrealized_cents: 5_000,
                stale_mark: false,
                exit_reason: None,
                open_at_end: true,
            }],
            data_source: DataSourceSpec::Parquet {
                path: "chains/spx.parquet".to_string(),
                sha256: "tape-sha".to_string(),
            },
            data_identity: "tape-sha".to_string(),
        }
    }

    #[test]
    fn test_write_bundle_publishes_complete_directory_atomically() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir");
        };
        let cfg = config(dir.path(), false);
        let run = sample_run(key());
        let Ok(dest) = write_bundle(&run, &cfg, &strategy()) else {
            panic!("the bundle writes");
        };
        // The published directory holds the manifest + four tables.
        for name in [
            "manifest.json",
            "fills.parquet",
            "equity_curve.parquet",
            "positions.parquet",
            "greeks_attribution.parquet",
        ] {
            assert!(dest.join(name).is_file(), "{name} must be present");
        }
        // No temp directory is left behind.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with('.'))
            .collect();
        assert!(leftovers.is_empty(), "no partial temp directory remains");
    }

    #[test]
    fn test_write_bundle_collision_without_overwrite_fails_typed() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir");
        };
        let cfg = config(dir.path(), false);
        let run = sample_run(key());
        let Ok(_dest) = write_bundle(&run, &cfg, &strategy()) else {
            panic!("first write succeeds");
        };
        // A second write to the same run_id without overwrite is a typed Bundle error.
        let second = write_bundle(&run, &cfg, &strategy());
        assert!(matches!(second, Err(BacktestError::Bundle(_))));
    }

    #[test]
    fn test_write_bundle_overwrite_replaces_same_run_id() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir");
        };
        let run = sample_run(key());
        let Ok(first) = write_bundle(&run, &config(dir.path(), false), &strategy()) else {
            panic!("first write");
        };
        // overwrite = true replaces the SAME run_id directory (never re-points).
        let Ok(second) = write_bundle(&run, &config(dir.path(), true), &strategy()) else {
            panic!("overwrite write");
        };
        assert_eq!(first, second, "overwrite targets the same run_id directory");
        assert!(second.join("manifest.json").is_file());
    }

    #[test]
    fn test_write_bundle_mid_write_failure_leaves_no_partial_bundle() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir");
        };
        let cfg = config(dir.path(), false);
        // An unresolved Days expiration makes to_contract_id fail mid-encode.
        let run = sample_run(unresolved_key());
        let result = write_bundle(&run, &cfg, &strategy());
        assert!(
            matches!(result, Err(BacktestError::Conversion(_))),
            "an unresolved contract fails the encode"
        );
        // Nothing (destination or temp) is left behind on failure.
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .into_iter()
            .flatten()
            .flatten()
            .collect();
        assert!(
            entries.is_empty(),
            "a failed write publishes nothing and cleans the temp directory"
        );
    }

    #[test]
    fn test_exit_reason_wire_strings_are_snake_case_or_verbatim_other() {
        use super::exit_reason_str;
        assert_eq!(
            exit_reason_str(&ExitReason::TargetReached),
            "target_reached"
        );
        assert_eq!(exit_reason_str(&ExitReason::Expiration), "expiration");
        assert_eq!(exit_reason_str(&ExitReason::ManualClose), "manual_close");
        assert_eq!(
            exit_reason_str(&ExitReason::Other("end_of_data".to_string())),
            "end_of_data"
        );
    }
}
