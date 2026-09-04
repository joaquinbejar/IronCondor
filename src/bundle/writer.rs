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
//! - Every table is written in its fixed sort order via the **pinned sort-key
//!   helpers** ([`fill_sort_key`] `(step, order_id, fill_seq)`,
//!   [`position_sort_key`] `(step, position_id)`, [`equity_sort_key`] /
//!   [`greeks_sort_key`] by `step`) — the **single** source of each table's
//!   order, shared with the reader — and no `HashMap` iteration reaches a row.
//!
//! # Write = encode-time build of the wire rows (the #35 orphan reconciled, #36)
//!
//! The engine collects the in-loop write carriers ([`FillRecord`] /
//! [`PositionSnapshot`], carrying an [`Arc<str>`](std::sync::Arc)-interned
//! `ContractKey` so a per-step push stays heap-allocation-free inside the PB-1
//! zero-alloc boundary). The writer, being **termination-phase** and free to
//! allocate, **builds the flat wire rows** ([`FillRow`] / [`PositionRow`], the
//! reader's decode target) from those carriers at encode time — deriving the
//! wire `contract_id`, stamping `strategy_run_id`, and mapping the `Side` /
//! `OptionStyle` / `ExecutionMode` enums to their wire strings **once**, in the
//! row builder — then sorts them through the pinned [`fill_sort_key`] /
//! [`position_sort_key`] helpers. This unifies the wire-row representation
//! (write = encode-time build, read = decode target) and removes the previous
//! duplication where the sort order lived inline in the writer and again in the
//! reader; the produced Parquet bytes are unchanged.
//! - Parquet is written with **fully pinned writer settings** (#131): every
//!   `parquet` setting that affects the byte layout — writer version, codec,
//!   `created_by`, dictionary on plus its explicit `PLAIN` fallback, the page
//!   and row-group limits, the write batch size, statistics level and
//!   truncation lengths, bloom filters off — is named on the builder in
//!   `writer_properties()`, so the file bytes do not vary with a `parquet`
//!   default change or a per-run timestamp.
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
//! Each table is encoded in a single O(rows) pass: its wire rows are sorted
//! through the pinned sort-key helpers, then streamed to Parquet in fixed-size
//! row-group batches ([`WRITE_BATCH_ROWS`]). Two footprints, kept distinct: the
//! **per-batch Arrow encode buffer** is bounded by one batch
//! ([`WRITE_BATCH_ROWS`]) independent of run length, but each table is first
//! materialised and sorted into a `Vec` of wire rows, so the writer's **total
//! peak is O(rows) — linear, not quadratic** (measured #37,
//! [docs/07 §3](../../../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
//! That linear footprint is the appropriate bar for a **termination-phase**
//! component: the writer is **outside** the PB-1 zero-alloc boundary and free
//! to allocate O(rows). Making the *total* peak row-group-bounded would need a
//! streaming external sort or a pre-sorted-input contract (a future #34
//! change); PB-5's "linear, not quadratic" clause is what is met and gated.
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
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use sha2::{Digest, Sha256};

use crate::analytics::metrics::Metrics;
use crate::bundle::schema::{
    BUNDLE_SCHEMA, Manifest, RowCounts, RunId, equity_sort_key, fill_sort_key, greeks_sort_key,
    position_sort_key,
};
use crate::config::BacktestConfig;
use crate::domain::{
    EquityPoint, ExecutionMode, FillRow, GreeksAttributionRow, PositionRow, StrategySpec,
};
use crate::engine::{BacktestRun, FillRecord, PositionSnapshot};
use crate::error::BacktestError;

/// Rows per streamed Parquet row-group batch — bounds the **per-batch Arrow
/// encode buffer** independently of run length. (It does **not** bound the
/// writer's total peak, which is O(rows): each table is materialised and sorted
/// into a `Vec` of wire rows before streaming — the appropriate linear
/// termination-phase footprint, measured #37, PB-5.)
const WRITE_BATCH_ROWS: usize = 8_192;

/// The pinned Parquet `created_by` string — fixed so the file bytes do not vary
/// with the `parquet` crate version (reproducibility,
/// [docs/05 §11](../../../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism)).
const PARQUET_CREATED_BY: &str = "ironcondor result bundle v1";

// ---------------------------------------------------------------------------
// Pinned Parquet writer settings (#131)
//
// Every `parquet` writer setting that can move the encoded bytes is named
// explicitly here and set on the builder in `writer_properties()`, pinned to
// the value that was the crate default when the goldens were blessed. The
// point is that a `parquet` upgrade which changes one of those defaults can
// no longer move our bundle bytes silently: the pin holds the old behaviour,
// and `test_writer_properties_pin_every_layout_setting` fails loudly if a
// setter or getter is renamed. Two settings are deliberately absent because
// they are unreachable under this configuration: the bloom-filter position
// (bloom filters are pinned off) and the Data Page v2 compression-ratio
// threshold (the writer version is pinned to 1.0, which has no v2 pages).
// ---------------------------------------------------------------------------

/// Pinned Parquet writer version. 1.0 also fixes the dictionary **fallback**
/// encoding family: under 2.0 the fallback would become the `DELTA_*` codecs.
const PARQUET_WRITER_VERSION: WriterVersion = WriterVersion::PARQUET_1_0;

/// Pinned compression codec for every column.
const PARQUET_COMPRESSION: Compression = Compression::SNAPPY;

/// Pinned dictionary encoding, on for every column that supports it.
const PARQUET_DICTIONARY_ENABLED: bool = true;

/// Pinned **fallback** encoding used once a dictionary page overflows
/// [`PARQUET_DICTIONARY_PAGE_SIZE_LIMIT`]. Setting it explicitly takes
/// precedence over the writer-version-dependent default.
const PARQUET_FALLBACK_ENCODING: Encoding = Encoding::PLAIN;

/// Pinned dictionary page size limit, in bytes.
const PARQUET_DICTIONARY_PAGE_SIZE_LIMIT: usize = 1 << 20;

/// Pinned data page size limit, in bytes.
const PARQUET_DATA_PAGE_SIZE_LIMIT: usize = 1 << 20;

/// Pinned data page row-count limit, in rows.
const PARQUET_DATA_PAGE_ROW_COUNT_LIMIT: usize = 20_000;

/// Pinned internal write batch size, in rows — the granularity at which the
/// column writer re-estimates page sizes, so it moves page boundaries.
const PARQUET_WRITE_BATCH_SIZE: usize = 1024;

/// Pinned maximum row-group row count, in rows.
const PARQUET_MAX_ROW_GROUP_ROW_COUNT: usize = 1 << 20;

/// Pinned maximum row-group size in bytes — `None` = unlimited, so the row
/// count above is the only row-group boundary.
const PARQUET_MAX_ROW_GROUP_BYTES: Option<usize> = None;

/// Pinned statistics level: page-level min/max/null counts.
const PARQUET_STATISTICS: EnabledStatistics = EnabledStatistics::Page;

/// Pinned page-header statistics: off (statistics live in the column index).
const PARQUET_WRITE_PAGE_HEADER_STATISTICS: bool = false;

/// Pinned bloom filters: off, so no bloom pages reach the file.
const PARQUET_BLOOM_FILTER_ENABLED: bool = false;

/// Pinned offset index: enabled (i.e. **not** disabled).
const PARQUET_OFFSET_INDEX_DISABLED: bool = false;

/// Pinned truncation length for column-index min/max values, in bytes.
const PARQUET_COLUMN_INDEX_TRUNCATE_LENGTH: Option<usize> = Some(64);

/// Pinned truncation length for statistics min/max values, in bytes.
const PARQUET_STATISTICS_TRUNCATE_LENGTH: Option<usize> = Some(64);

/// Pinned Arrow-to-Parquet type coercion: off, so the schema is written as the
/// bundle declares it.
const PARQUET_COERCE_TYPES: bool = false;

/// Pinned `path_in_schema` in the Thrift column metadata: written.
const PARQUET_WRITE_PATH_IN_SCHEMA: bool = true;

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
    // Canonicalise ONCE, here: the manifest records exactly the spec the
    // `run_id` hashed, so an explicit leg set written in a different leg order
    // lands in the same `<run_id>` directory with byte-identical manifest bytes
    // (the named kinds clone unchanged — their frozen goldens are untouched).
    let strategy = &strategy.canonical();
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

    // Publish atomically. A fresh destination is a single rename. An overwrite
    // must never destroy the existing bundle before the replacement is in place:
    // the old code removed `dest` and then, on a rename failure, also removed the
    // temp — losing the ONLY bundle. Instead move the old bundle aside to a
    // sibling backup, rename the temp into the destination, and only then drop
    // the backup; if the final rename fails, restore the backup so a complete
    // bundle always remains on disk (at `dest`, or recoverable from the backup).
    if dest_exists {
        let backup = output_dir.join(format!(".{}.backup", run_id.as_str()));
        // Clean any stale backup left by a previously-crashed overwrite.
        if backup
            .try_exists()
            .map_err(|e| bundle_err("stat backup directory", &e))?
        {
            std::fs::remove_dir_all(&backup)
                .map_err(|e| bundle_err("remove stale backup directory", &e))?;
        }
        // Move the existing bundle aside (a rename, atomic on one filesystem):
        // on failure the destination is untouched, so its complete bundle
        // survives; clean the temp and propagate.
        if let Err(error) = std::fs::rename(&dest, &backup) {
            let _ = std::fs::remove_dir_all(&temp);
            return Err(bundle_err("move existing bundle aside to backup", &error));
        }
        // Move the new bundle into place. On failure restore the backup so the
        // destination keeps its (old) complete bundle rather than being left
        // empty, then clean the temp and propagate.
        if let Err(error) = std::fs::rename(&temp, &dest) {
            let _ = std::fs::rename(&backup, &dest);
            let _ = std::fs::remove_dir_all(&temp);
            return Err(bundle_err(
                "atomic rename into place (existing bundle restored from backup)",
                &error,
            ));
        }
        // The replacement is published; drop the backup. A leftover backup would
        // never corrupt the bundle, so a failed cleanup is logged, not fatal.
        if let Err(error) = std::fs::remove_dir_all(&backup) {
            tracing::warn!(
                run_id = run_id.as_str(),
                backup = %backup.display(),
                error = %error,
                "failed to remove the overwrite backup directory; the published bundle is intact"
            );
        }
    } else if let Err(error) = std::fs::rename(&temp, &dest) {
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

/// The pinned Parquet writer properties.
///
/// **The rule: every `parquet` setting that affects the byte layout is named
/// here.** Each one is set explicitly, to the value that was the crate default
/// when the goldens were blessed, so a `parquet` version bump that changes a
/// default cannot move our bundle bytes silently — and a renamed setter breaks
/// the build instead of the contract. Nothing per-run enters the file: the
/// codec and `created_by` are fixed, and the only wall-clock value in the whole
/// system (`created_utc`) lives in the JSON manifest, never in Parquet metadata
/// ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility),
/// [docs/05 §11](../../../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism)).
///
/// The two settings not pinned here are unreachable under this configuration:
/// the bloom-filter position (bloom filters are off) and the Data Page v2
/// compression-ratio threshold (the writer version is 1.0).
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(PARQUET_WRITER_VERSION)
        .set_compression(PARQUET_COMPRESSION)
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_dictionary_enabled(PARQUET_DICTIONARY_ENABLED)
        .set_encoding(PARQUET_FALLBACK_ENCODING)
        .set_dictionary_page_size_limit(PARQUET_DICTIONARY_PAGE_SIZE_LIMIT)
        .set_data_page_size_limit(PARQUET_DATA_PAGE_SIZE_LIMIT)
        .set_data_page_row_count_limit(PARQUET_DATA_PAGE_ROW_COUNT_LIMIT)
        .set_write_batch_size(PARQUET_WRITE_BATCH_SIZE)
        .set_max_row_group_row_count(Some(PARQUET_MAX_ROW_GROUP_ROW_COUNT))
        .set_max_row_group_bytes(PARQUET_MAX_ROW_GROUP_BYTES)
        .set_statistics_enabled(PARQUET_STATISTICS)
        .set_write_page_header_statistics(PARQUET_WRITE_PAGE_HEADER_STATISTICS)
        .set_bloom_filter_enabled(PARQUET_BLOOM_FILTER_ENABLED)
        .set_offset_index_disabled(PARQUET_OFFSET_INDEX_DISABLED)
        .set_column_index_truncate_length(PARQUET_COLUMN_INDEX_TRUNCATE_LENGTH)
        .set_statistics_truncate_length(PARQUET_STATISTICS_TRUNCATE_LENGTH)
        .set_coerce_types(PARQUET_COERCE_TYPES)
        .set_write_path_in_schema(PARQUET_WRITE_PATH_IN_SCHEMA)
        .set_content_defined_chunking(None)
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

    // Build the flat wire rows from the collector records at encode time, then
    // sort by the pinned FILLS_SORT_COLUMNS = (step, order_id, fill_seq) unique
    // key via `fill_sort_key` — the single source of the table's order.
    let mut rows: Vec<FillRow> = Vec::with_capacity(run.fills.len());
    for record in &run.fills {
        rows.push(fill_row(record, run_id)?);
    }
    rows.sort_by_key(fill_sort_key);
    let count = row_count(rows.len())?;

    let mut writer = open_writer(&schema, path)?;
    for chunk in rows.chunks(WRITE_BATCH_ROWS) {
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
        for row in chunk {
            step.push(i32_from_u32(row.step)?);
            ts.push(row.ts_ns);
            run_ids.push(row.strategy_run_id.as_str());
            trade.push(i64_from_u64(row.trade_id)?);
            position.push(i64_from_u64(row.position_id)?);
            order_ids.push(i64_from_u64(row.order_id)?);
            fill_seq.push(i32_from_u32(row.fill_seq)?);
            underlying.push(row.underlying.as_str());
            expiration.push(row.expiration_ns);
            contract.push(row.contract_id.as_str());
            strike.push(i64_from_u64(row.strike_cents)?);
            style.push(row.style.as_str());
            side.push(row.side.as_str());
            quantity.push(i32_from_u32(row.quantity)?);
            price.push(i64_from_u64(row.price_cents)?);
            fees.push(row.fees_cents);
            slippage.push(row.slippage_cents);
            mode.push(row.mode.as_str());
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

    // Build the flat wire rows from the collector snapshots at encode time, then
    // sort by the pinned POSITIONS_SORT_COLUMNS = (step, position_id) key via
    // `position_sort_key` — the single source of the table's order (≤ 1 row/step).
    let mut rows: Vec<PositionRow> = Vec::with_capacity(run.positions.len());
    for snapshot in &run.positions {
        rows.push(position_row(snapshot)?);
    }
    rows.sort_by_key(position_sort_key);
    let count = row_count(rows.len())?;

    let mut writer = open_writer(&schema, path)?;
    for chunk in rows.chunks(WRITE_BATCH_ROWS) {
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
            contract.push(row.contract_id.as_str());
            side.push(row.side.as_str());
            quantity.push(i32_from_u32(row.quantity)?);
            avg_price.push(i64_from_u64(row.avg_price_cents)?);
            mark.push(i64_from_u64(row.mark_cents)?);
            unrealized.push(row.unrealized_cents);
            stale.push(row.stale_mark);
            exit_reason.push(row.exit_reason.clone());
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
// Encode-time wire-row builders (collector carrier -> flat wire row)
// ---------------------------------------------------------------------------

/// Build one `fills.parquet` wire row ([`FillRow`]) from an in-loop
/// [`FillRecord`] carrier: stamp `strategy_run_id`, derive the wire
/// `contract_id` / `expiration_ns` from the [`crate::domain::ContractKey`], and
/// map the `Side` / `OptionStyle` / `ExecutionMode` enums to their wire strings.
/// Termination-phase, free to allocate.
///
/// # Errors
///
/// [`BacktestError::Conversion`] if the fill's contract cannot resolve its
/// absolute expiration or its canonical `contract_id` (e.g. an unresolved
/// `Days` expiration).
fn fill_row(record: &FillRecord, run_id: &str) -> Result<FillRow, BacktestError> {
    let fill = &record.fill;
    Ok(FillRow {
        step: fill.step.value(),
        ts_ns: fill.ts.value(),
        strategy_run_id: run_id.to_string(),
        trade_id: record.trade_id,
        position_id: record.position_id,
        order_id: record.order_id,
        fill_seq: record.fill_seq,
        underlying: fill.contract.underlying.as_str().to_string(),
        expiration_ns: fill.contract.expiration_ns()?,
        contract_id: fill.contract.to_contract_id()?,
        strike_cents: fill.contract.strike.value(),
        style: style_str(fill.contract.style).to_string(),
        side: side_str(fill.side).to_string(),
        quantity: fill.quantity.value(),
        price_cents: fill.price.value(),
        fees_cents: fill.fees.value(),
        slippage_cents: fill.slippage.value(),
        mode: mode_str(fill.mode).to_string(),
    })
}

/// Build one `positions.parquet` wire row ([`PositionRow`]) from an in-loop
/// [`PositionSnapshot`] carrier: derive the wire `contract_id`, map the leg
/// `Side` and the applied [`ExitReason`] to their wire strings (`exit_reason`
/// the only nullable column). Termination-phase, free to allocate.
///
/// # Errors
///
/// [`BacktestError::Conversion`] if the leg's contract cannot resolve its
/// canonical `contract_id`.
fn position_row(snapshot: &PositionSnapshot) -> Result<PositionRow, BacktestError> {
    Ok(PositionRow {
        step: snapshot.step,
        ts_ns: snapshot.ts_ns,
        position_id: snapshot.position_id,
        trade_id: snapshot.trade_id,
        contract_id: snapshot.contract.to_contract_id()?,
        side: side_str(snapshot.side).to_string(),
        quantity: snapshot.quantity,
        avg_price_cents: snapshot.avg_price_cents,
        mark_cents: snapshot.mark_cents,
        unrealized_cents: snapshot.unrealized_cents,
        stale_mark: snapshot.stale_mark,
        exit_reason: snapshot.exit_reason.as_ref().map(exit_reason_str),
        open_at_end: snapshot.open_at_end,
    })
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
    fn test_write_bundle_overwrite_leaves_no_backup_or_temp() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir");
        };
        let run = sample_run(key());
        // First write (fresh), then overwrite the SAME run_id.
        let Ok(_first) = write_bundle(&run, &config(dir.path(), false), &strategy()) else {
            panic!("first write");
        };
        let Ok(dest) = write_bundle(&run, &config(dir.path(), true), &strategy()) else {
            panic!("overwrite write");
        };
        // The published bundle is complete after the move-aside overwrite.
        assert!(dest.join("manifest.json").is_file());
        // No backup / temp dotfile directory remains after a successful
        // overwrite — the only leftover under output_dir is the published bundle.
        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with('.'))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no backup/temp directory must remain after overwrite: {leftovers:?}"
        );
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

    // -----------------------------------------------------------------------
    // Pinned Parquet writer settings (#131)
    // -----------------------------------------------------------------------

    /// Reads every pinned setting back off the built [`WriterProperties`] and
    /// compares it with the constant it was pinned to. This is the guard the
    /// pin exists for: a `parquet` bump that renames a setter breaks the build,
    /// and one that silently changes a default without renaming anything fails
    /// here rather than moving the bundle bytes.
    #[test]
    fn test_writer_properties_pin_every_layout_setting() {
        use parquet::schema::types::ColumnPath;

        use super::{
            PARQUET_BLOOM_FILTER_ENABLED, PARQUET_COERCE_TYPES,
            PARQUET_COLUMN_INDEX_TRUNCATE_LENGTH, PARQUET_COMPRESSION,
            PARQUET_DATA_PAGE_ROW_COUNT_LIMIT, PARQUET_DATA_PAGE_SIZE_LIMIT,
            PARQUET_DICTIONARY_ENABLED, PARQUET_DICTIONARY_PAGE_SIZE_LIMIT,
            PARQUET_FALLBACK_ENCODING, PARQUET_MAX_ROW_GROUP_BYTES,
            PARQUET_MAX_ROW_GROUP_ROW_COUNT, PARQUET_OFFSET_INDEX_DISABLED, PARQUET_STATISTICS,
            PARQUET_STATISTICS_TRUNCATE_LENGTH, PARQUET_WRITE_BATCH_SIZE,
            PARQUET_WRITE_PAGE_HEADER_STATISTICS, PARQUET_WRITE_PATH_IN_SCHEMA,
            PARQUET_WRITER_VERSION, writer_properties,
        };

        let props = writer_properties();
        // A column that was never named individually, so every getter resolves
        // through the pinned defaults.
        let col = ColumnPath::from("any_column");

        assert_eq!(props.writer_version(), PARQUET_WRITER_VERSION);
        assert_eq!(props.compression(&col), PARQUET_COMPRESSION);
        assert_eq!(props.created_by(), super::PARQUET_CREATED_BY);
        assert_eq!(props.dictionary_enabled(&col), PARQUET_DICTIONARY_ENABLED);
        assert_eq!(props.encoding(&col), Some(PARQUET_FALLBACK_ENCODING));
        assert_eq!(
            props.dictionary_page_size_limit(),
            PARQUET_DICTIONARY_PAGE_SIZE_LIMIT
        );
        assert_eq!(props.data_page_size_limit(), PARQUET_DATA_PAGE_SIZE_LIMIT);
        assert_eq!(
            props.data_page_row_count_limit(),
            PARQUET_DATA_PAGE_ROW_COUNT_LIMIT
        );
        assert_eq!(props.write_batch_size(), PARQUET_WRITE_BATCH_SIZE);
        assert_eq!(
            props.max_row_group_row_count(),
            Some(PARQUET_MAX_ROW_GROUP_ROW_COUNT)
        );
        assert_eq!(props.max_row_group_bytes(), PARQUET_MAX_ROW_GROUP_BYTES);
        assert_eq!(props.statistics_enabled(&col), PARQUET_STATISTICS);
        assert_eq!(
            props.write_page_header_statistics(&col),
            PARQUET_WRITE_PAGE_HEADER_STATISTICS
        );
        assert_eq!(
            props.bloom_filter_properties(&col).is_some(),
            PARQUET_BLOOM_FILTER_ENABLED,
            "bloom filters are pinned off, so no column carries bloom properties"
        );
        assert_eq!(props.offset_index_disabled(), PARQUET_OFFSET_INDEX_DISABLED);
        assert_eq!(
            props.column_index_truncate_length(),
            PARQUET_COLUMN_INDEX_TRUNCATE_LENGTH
        );
        assert_eq!(
            props.statistics_truncate_length(),
            PARQUET_STATISTICS_TRUNCATE_LENGTH
        );
        assert_eq!(props.coerce_types(), PARQUET_COERCE_TYPES);
        assert_eq!(props.write_path_in_schema(), PARQUET_WRITE_PATH_IN_SCHEMA);
        assert!(
            props.content_defined_chunking().is_none(),
            "content-defined chunking is pinned off"
        );
        // Not written by this configuration, so they are not pinned: no
        // key-value metadata and no sorting columns are set by the writer.
        assert!(props.key_value_metadata().is_none());
        assert!(props.sorting_columns().is_none());
    }

    /// The SHA-256 of the dictionary-overflow fixture written by
    /// [`test_dictionary_fallback_is_plain_and_byte_stable`].
    ///
    /// It moves **only** when `parquet` changes its byte layout under the
    /// settings pinned in [`writer_properties`] — which is exactly the event
    /// this issue exists to make loud. Re-blessing it is a one-line edit,
    /// sanctioned the same way a golden re-bless is: driven by a lockfile
    /// change, recorded in `docs/SEMVER.md`, and reviewed as a contract move.
    const DICT_FALLBACK_FIXTURE_SHA256: &str =
        "aa8b9b2cd79f65010ab171c5d066baae3da2f33da11222142b37c13452b54434";

    /// Rows in the dictionary-overflow fixture. 40 000 distinct 40-byte values
    /// is ~1.6 MiB of dictionary, comfortably past the 1 MiB dictionary page
    /// limit, so the column writer is forced onto the fallback encoding.
    const DICT_FALLBACK_ROWS: usize = 40_000;

    /// Writes a single non-null `Utf8` column of [`DICT_FALLBACK_ROWS`]
    /// distinct values through the module's own pinned writer path, and
    /// returns the file bytes.
    fn write_dict_fallback_fixture(path: &Path) -> Result<Vec<u8>, BacktestError> {
        use std::sync::Arc;

        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

        use super::{close_writer, open_writer, write_batch};

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let values: Vec<String> = (0..DICT_FALLBACK_ROWS)
            .map(|i| format!("{i:040}"))
            .collect();

        let mut writer = open_writer(&schema, path)?;
        for chunk in values.chunks(super::WRITE_BATCH_ROWS) {
            let array: ArrayRef = Arc::new(StringArray::from_iter_values(chunk));
            write_batch(&mut writer, &schema, vec![array])?;
        }
        close_writer(writer)?;
        std::fs::read(path).map_err(|e| super::bundle_err("read fixture back", &e))
    }

    /// The dictionary fallback is `PLAIN` and the bytes are frozen.
    ///
    /// Three claims in one fixture: the dictionary really does overflow (the
    /// chunk carries both `RLE_DICTIONARY` and the fallback), the fallback is
    /// `PLAIN` and not one of the `DELTA_*` codecs a writer-version bump would
    /// select, and the produced file is byte-stable run to run and equal to a
    /// frozen digest.
    #[test]
    fn test_dictionary_fallback_is_plain_and_byte_stable() {
        use parquet::basic::Encoding;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use sha2::{Digest, Sha256};

        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir");
        };
        let first_path = dir.path().join("dict_fallback_a.parquet");
        let second_path = dir.path().join("dict_fallback_b.parquet");

        let Ok(first) = write_dict_fallback_fixture(&first_path) else {
            panic!("write the first fixture");
        };
        let Ok(second) = write_dict_fallback_fixture(&second_path) else {
            panic!("write the second fixture");
        };
        assert_eq!(
            first, second,
            "two writes of the same rows under the pinned settings are byte-identical"
        );

        let Ok(file) = std::fs::File::open(&first_path) else {
            panic!("open the fixture");
        };
        let Ok(reader) = SerializedFileReader::new(file) else {
            panic!("read the fixture footer");
        };
        let metadata = reader.metadata();
        assert_eq!(
            metadata.num_row_groups(),
            1,
            "{DICT_FALLBACK_ROWS} rows fit in one pinned row group"
        );
        let encodings: Vec<Encoding> = metadata.row_group(0).column(0).encodings().collect();
        assert!(
            encodings.contains(&Encoding::RLE_DICTIONARY),
            "the column starts dictionary-encoded, encodings: {encodings:?}"
        );
        assert!(
            encodings.contains(&Encoding::PLAIN),
            "the dictionary overflowed and fell back to PLAIN, encodings: {encodings:?}"
        );
        assert!(
            !encodings.contains(&Encoding::DELTA_BYTE_ARRAY),
            "the fallback is never a writer-version-2.0 DELTA codec, encodings: {encodings:?}"
        );

        let digest = super::to_hex(&Sha256::digest(&first));
        assert_eq!(
            digest,
            DICT_FALLBACK_FIXTURE_SHA256,
            "the pinned-settings fixture is {} bytes with sha256 {digest}, encodings: {encodings:?}",
            first.len()
        );
    }
}
