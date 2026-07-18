//! The result-bundle **reader** — the read-back **security gate** that validates
//! a bundle handed back across a trust boundary before a consumer touches it
//! ([docs/05 §12](../../../docs/05-analytics-and-reporting.md#12-producer-side-contract-matrix-ironcondorbundlev1),
//! [docs/07 §8](../../../docs/07-performance-and-security.md#8-untrusted-input-hardening)).
//!
//! # Why this is a security gate, not ergonomics
//!
//! A result bundle read back (by ChainView, a notebook, a re-run) is **untrusted
//! input** — it may be crafted to crash, hang, or exhaust memory inside a firm's
//! CI ([docs/07 §7](../../../docs/07-performance-and-security.md#7-threat-model)).
//! [`read_bundle`] therefore follows the **tape-validation contract**
//! ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)):
//! validate **once, at the boundary**, with a named [`ResourceLimits`] ceiling
//! checked **before** any allocation is sized from an untrusted count, and a
//! typed [`BacktestError`] on every failure — **never** a panic, hang, or OOM.
//! There is no `.unwrap()` / `.expect()` / unchecked `[]` / `as` cast on this
//! path; every `INT32` / `INT64` → unsigned crossing is a checked `try_from`.
//!
//! # Validation order (fail fast at the boundary)
//!
//! 1. **Schema tag first.** `manifest.json` is read **bounded by
//!    `max_manifest_bytes`** (a filesystem-size check *before* the read, then a
//!    [`Read::take`] cap), parsed as JSON, and its `schema` field is checked
//!    against [`BUNDLE_SCHEMA`] **before any Parquet table is opened** — an
//!    unknown major tag is a hard, explicit incompatibility.
//! 2. **Manifest required fields.** All [docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson)
//!    fields must be present and typed. `metrics` is **versioned-opaque**: it is
//!    kept as a [`serde_json::Value`] and never parsed — only `row_counts` is
//!    validated and cross-checked ([docs/05 §12.5](../../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)).
//!    `max_string_len` bounds every manifest string.
//! 3. **Table schemas.** Each of the four tables is validated against its exact
//!    column set / physical type / nullability (`exit_reason` the only nullable
//!    column, `drawdown` the only `DOUBLE`), `max_rows_per_table` /
//!    `max_decompressed_bytes` are enforced **before** decoding, and each row is
//!    decoded **into its canonical wire row type** ([`FillRow`] / [`EquityPoint`]
//!    / [`PositionRow`] / [`GreeksAttributionRow`]).
//! 4. **`row_counts` cross-check.** Decoded table lengths must equal the manifest
//!    `row_counts`.
//! 5. **Referenced-input `sha256`.** A single-file Parquet `data_source`, **where
//!    reachable**, is re-hashed and checked against the recorded `sha256` — a
//!    mismatch is a typed error, never a silent divergent run.
//! 6. **`contract_id` round-trip.** Every `fills` / `positions` `contract_id`
//!    parses back through [`ContractKey::from_contract_id`] equal to the row's
//!    own columns (and the `UNDERLYING` grammar holds) before it is a join key.
//!
//! # The collector / wire-row split (the #35 orphan reconciled)
//!
//! Two record families describe the same rows at two lifecycle phases:
//!
//! - **In-loop write carriers** — [`crate::engine::FillRecord`] /
//!   [`crate::engine::PositionSnapshot`] carry a [`ContractKey`] (its
//!   `underlying` an `Arc<str>`) so a per-step push is heap-allocation-free
//!   inside the PB-1 zero-alloc boundary; the **writer** ([`crate::bundle::writer`])
//!   consumes them and derives the wire `contract_id` at encode time.
//! - **Wire rows** — [`FillRow`] / [`PositionRow`] / [`EquityPoint`] /
//!   [`GreeksAttributionRow`] ([`crate::domain::result`]) are the flat,
//!   `serde`-able read / validate / serialize targets. The **reader** decodes
//!   Parquet **into** these, so they (and the [`fill_sort_key`] /
//!   [`position_sort_key`] extractors) are the reader's canonical
//!   decode-and-sort target.
//!
//! Since #36 the **writer also sorts through the `*_sort_key` helpers** — it
//! builds the [`FillRow`] / [`PositionRow`] wire rows at encode time and sorts
//! them by [`fill_sort_key`] / [`position_sort_key`], so the pinned per-table
//! order lives in exactly one place, shared by write and read; the #35
//! write-side sort duplication is fully reconciled.
//!
//! # Resource ceilings (each checked before an allocation is sized)
//!
//! | Ceiling | Surface | Enforcement point |
//! |---------|---------|-------------------|
//! | `max_manifest_bytes` | `manifest.json` | filesystem size **before** read, then `Read::take` |
//! | `max_string_len` | every manifest / Parquet string | after parse / per decoded cell |
//! | `max_rows_per_table` | each Parquet table | declared footer count **before** decode, then incrementally |
//! | `max_decompressed_bytes` | each Parquet table | declared row-group size **before** decode, then incrementally (decompression-bomb guard, reused from the feed) |
//!
//! A crossed ceiling is a [`BacktestError::TapeTooLarge`] naming the field;
//! every other structural / semantic failure is a [`BacktestError::Bundle`].

use std::fs::File;
use std::io::Read;
use std::path::Path;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Schema};
use optionstratlib::OptionStyle;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::bundle::schema::{
    BUNDLE_SCHEMA, RowCounts, equity_sort_key, fill_sort_key, greeks_sort_key, position_sort_key,
};
use crate::config::{BacktestConfig, ResourceLimits};
use crate::data::DataSourceSpec;
use crate::domain::ContractKey;
use crate::domain::{EquityPoint, FillRow, GreeksAttributionRow, PositionRow, StrategySpec};
use crate::error::BacktestError;

/// Rows decoded per Arrow batch — bounds the per-batch working set (and the
/// per-batch pre-sizing) independently of the untrusted footer row count.
const READ_BATCH_ROWS: usize = 8_192;

/// The streaming buffer size for re-hashing a referenced file input; the read is
/// bounded by `max_file_bytes`.
const HASH_CHUNK_BYTES: usize = 65_536;

/// Every required `manifest.json` field ([docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson));
/// each is checked present before the typed parse so a missing field is a clear
/// typed error.
const REQUIRED_MANIFEST_FIELDS: [&str; 11] = [
    "schema",
    "run_id",
    "created_utc",
    "code_version",
    "lockfile_sha256",
    "seed",
    "config",
    "strategy",
    "data_source",
    "metrics",
    "row_counts",
];

/// The validated manifest of a read-back bundle — the [docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson)
/// fields, with `metrics` kept **versioned-opaque** as a [`serde_json::Value`]
/// (never parsed) and only `row_counts` validated + cross-checked
/// ([docs/05 §12.5](../../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)).
///
/// Deserialised **without** `deny_unknown_fields`: within the `ironcondor.bundle.v1`
/// tag a later minor release may add optional fields, which an older reader
/// tolerates ([docs/SEMVER.md](../../../docs/SEMVER.md)).
#[derive(Debug, Clone, Deserialize)]
pub struct ValidatedManifest {
    /// The wire tag — always [`BUNDLE_SCHEMA`] once validated.
    pub schema: String,
    /// The deterministic run identity (the bundle directory name), bare string.
    pub run_id: String,
    /// The RFC 3339 wall-clock provenance field (never read back by the engine).
    pub created_utc: String,
    /// The `ironcondor` crate version (`CARGO_PKG_VERSION`, build identity — no
    /// per-commit git sha, so the `run_id` is stable across commits).
    pub code_version: String,
    /// The sha256 of `Cargo.lock` at build (build identity).
    pub lockfile_sha256: String,
    /// The engine RNG seed.
    pub seed: u64,
    /// The full run config, verbatim (money fields integer cents).
    pub config: BacktestConfig,
    /// The strategy kind + parameters.
    pub strategy: StrategySpec,
    /// The data-source provenance + re-read locator.
    pub data_source: DataSourceSpec,
    /// The populated metrics object — **versioned-opaque**; kept as raw JSON and
    /// never parsed by the reader.
    pub metrics: Value,
    /// The per-table row counts — validated and cross-checked against the
    /// decoded tables.
    pub row_counts: RowCounts,
}

/// A fully validated, decoded result bundle — the successful output of
/// [`read_bundle`]. Every row is decoded into its canonical wire type and each
/// table is returned in its pinned sort-key order (the canonical ordering the
/// equality oracle uses).
#[derive(Debug, Clone)]
pub struct ValidatedBundle {
    /// The validated manifest (`metrics` opaque).
    pub manifest: ValidatedManifest,
    /// `fills.parquet`, sorted by `(step, order_id, fill_seq)`.
    pub fills: Vec<FillRow>,
    /// `equity_curve.parquet`, sorted by `step`.
    pub equity_curve: Vec<EquityPoint>,
    /// `positions.parquet`, sorted by `(step, position_id)`.
    pub positions: Vec<PositionRow>,
    /// `greeks_attribution.parquet`, sorted by `step`.
    pub greeks_attribution: Vec<GreeksAttributionRow>,
}

/// Read, validate, and decode the result bundle in `dir` into a
/// [`ValidatedBundle`], enforcing the [`ResourceLimits`] ceilings.
///
/// The validation order is **schema tag → manifest fields → table schemas +
/// decode → `row_counts` cross-check → referenced-input `sha256` → `contract_id`
/// round-trip** (see the [module docs](self)); the schema tag is checked before
/// any table is opened, and every ceiling is checked before an allocation is
/// sized from an untrusted count.
///
/// # Errors
///
/// - [`BacktestError::Bundle`] — a wrong / unknown `schema` tag, a missing or
///   mistyped manifest field, a wrong table column / type / nullability, a
///   `row_counts` mismatch, a referenced-input `sha256` mismatch, a negative id
///   / step / quantity / non-negative-cents field on the wire, a
///   non-round-trippable `contract_id`, a non-finite `drawdown`, or a truncated
///   / corrupt Parquet footer or row group.
/// - [`BacktestError::TapeTooLarge`] — a crossed `max_manifest_bytes` /
///   `max_string_len` / `max_rows_per_table` / `max_decompressed_bytes` ceiling
///   (`limit` names the field).
/// - [`BacktestError::DataIo`] — an underlying filesystem failure.
/// - [`BacktestError::ArithmeticOverflow`] — a checked-arithmetic overflow while
///   accumulating a ceiling counter (unreachable in practice, surfaced typed).
pub fn read_bundle(
    dir: impl AsRef<Path>,
    limits: &ResourceLimits,
) -> Result<ValidatedBundle, BacktestError> {
    let dir = dir.as_ref();

    // (1) + (2) manifest: schema tag FIRST (before any table), then required
    //     fields, typed parse (metrics opaque), and string-length ceilings.
    let manifest = read_manifest(dir, limits)?;

    // (3) tables: exact schema, ceilings before decode, decode into wire rows.
    let fills = decode_table(
        &dir.join("fills.parquet"),
        "fills",
        &fills_schema(),
        limits,
        |batch, out| decode_fills_batch(batch, out, limits),
    )?;
    let equity_curve = decode_table(
        &dir.join("equity_curve.parquet"),
        "equity_curve",
        &equity_schema(),
        limits,
        decode_equity_batch,
    )?;
    let positions = decode_table(
        &dir.join("positions.parquet"),
        "positions",
        &positions_schema(),
        limits,
        |batch, out| decode_positions_batch(batch, out, limits),
    )?;
    let greeks_attribution = decode_table(
        &dir.join("greeks_attribution.parquet"),
        "greeks_attribution",
        &greeks_schema(),
        limits,
        decode_greeks_batch,
    )?;

    // (4) row_counts cross-check — decoded lengths must equal the manifest.
    check_count("fills", fills.len(), manifest.row_counts.fills)?;
    check_count(
        "equity_curve",
        equity_curve.len(),
        manifest.row_counts.equity_curve,
    )?;
    check_count("positions", positions.len(), manifest.row_counts.positions)?;
    check_count(
        "greeks_attribution",
        greeks_attribution.len(),
        manifest.row_counts.greeks_attribution,
    )?;

    // (5) referenced-input sha256, where reachable (never a silent divergence).
    verify_referenced_input(&manifest.data_source, limits, false)?;

    // (6) contract_id round-trip / UNDERLYING grammar before it is a join key.
    for row in &fills {
        verify_fill_contract_id(row)?;
    }
    for row in &positions {
        verify_position_contract_id(row)?;
    }

    // Canonical ordering (revives the pinned sort-key helpers).
    let mut fills = fills;
    fills.sort_by_key(fill_sort_key);
    let mut equity_curve = equity_curve;
    equity_curve.sort_by_key(equity_sort_key);
    let mut positions = positions;
    positions.sort_by_key(position_sort_key);
    let mut greeks_attribution = greeks_attribution;
    greeks_attribution.sort_by_key(greeks_sort_key);

    Ok(ValidatedBundle {
        manifest,
        fills,
        equity_curve,
        positions,
        greeks_attribution,
    })
}

// ---------------------------------------------------------------------------
// (1) + (2) Manifest read + validation
// ---------------------------------------------------------------------------

/// Read and validate `manifest.json`: bounded read, schema tag first, required
/// fields present, typed parse (`metrics` opaque), string-length ceilings.
fn read_manifest(dir: &Path, limits: &ResourceLimits) -> Result<ValidatedManifest, BacktestError> {
    let path = dir.join("manifest.json");

    // Bounded read: reject a non-regular file and an oversized manifest from the
    // filesystem metadata BEFORE reading a byte, then cap the read with `take`.
    let meta = std::fs::metadata(&path).map_err(|e| bundle_err("stat manifest.json", &e))?;
    if !meta.is_file() {
        return Err(BacktestError::Bundle(
            "bundle manifest.json is not a regular file".to_string(),
        ));
    }
    let len = meta.len();
    if len > limits.max_manifest_bytes {
        return Err(BacktestError::TapeTooLarge {
            limit: "max_manifest_bytes",
            value: len,
            cap: limits.max_manifest_bytes,
        });
    }
    let file = File::open(&path).map_err(|e| bundle_err("open manifest.json", &e))?;
    let cap = usize::try_from(len).map_err(|_| BacktestError::ArithmeticOverflow)?;
    let mut buf = Vec::with_capacity(cap);
    file.take(limits.max_manifest_bytes)
        .read_to_end(&mut buf)
        .map_err(|e| bundle_err("read manifest.json", &e))?;

    let value: Value =
        serde_json::from_slice(&buf).map_err(|e| bundle_err("parse manifest.json", &e))?;
    let obj = value
        .as_object()
        .ok_or_else(|| BacktestError::Bundle("manifest.json is not a JSON object".to_string()))?;

    // (1) schema tag FIRST — reject an unknown major tag before any table parse.
    let schema_tag = obj.get("schema").and_then(Value::as_str).ok_or_else(|| {
        BacktestError::Bundle("manifest.json has no string schema tag".to_string())
    })?;
    if schema_tag != BUNDLE_SCHEMA {
        return Err(BacktestError::Bundle(format!(
            "unknown bundle schema tag {schema_tag:?}, expected {BUNDLE_SCHEMA:?}"
        )));
    }

    // (2) every required field present, then the typed parse (metrics opaque).
    for field in REQUIRED_MANIFEST_FIELDS {
        if !obj.contains_key(field) {
            return Err(BacktestError::Bundle(format!(
                "manifest.json is missing required field {field}"
            )));
        }
    }
    let manifest: ValidatedManifest =
        serde_json::from_value(value).map_err(|e| bundle_err("validate manifest fields", &e))?;

    // String-length ceilings on the manifest scalar strings + the referenced
    // data-source locator strings.
    check_string_len(manifest.schema.len(), limits)?;
    check_string_len(manifest.run_id.len(), limits)?;
    check_string_len(manifest.created_utc.len(), limits)?;
    check_string_len(manifest.code_version.len(), limits)?;
    check_string_len(manifest.lockfile_sha256.len(), limits)?;
    for s in data_source_strings(&manifest.data_source) {
        check_string_len(s.len(), limits)?;
    }
    Ok(manifest)
}

/// The re-read locator strings of a data source (bounded by `max_string_len`).
fn data_source_strings(spec: &DataSourceSpec) -> Vec<&str> {
    match spec {
        DataSourceSpec::Csv { path, sha256 } | DataSourceSpec::Parquet { path, sha256 } => {
            vec![path.as_str(), sha256.as_str()]
        }
        #[cfg(feature = "simulator")]
        DataSourceSpec::Simulator(sim) => {
            vec![sim.base_url.as_str(), sim.tape_sha256.as_str()]
        }
    }
}

// ---------------------------------------------------------------------------
// (3) Table schemas + decode
// ---------------------------------------------------------------------------

/// `fills.parquet` schema — `(name, physical type, nullable)`, mirroring the
/// writer ([docs/05 §7](../../../docs/05-analytics-and-reporting.md#7-fillsparquet)).
fn fills_schema() -> [(&'static str, DataType, bool); 18] {
    [
        ("step", DataType::Int32, false),
        ("ts_ns", DataType::Int64, false),
        ("strategy_run_id", DataType::Utf8, false),
        ("trade_id", DataType::Int64, false),
        ("position_id", DataType::Int64, false),
        ("order_id", DataType::Int64, false),
        ("fill_seq", DataType::Int32, false),
        ("underlying", DataType::Utf8, false),
        ("expiration_ns", DataType::Int64, false),
        ("contract_id", DataType::Utf8, false),
        ("strike_cents", DataType::Int64, false),
        ("style", DataType::Utf8, false),
        ("side", DataType::Utf8, false),
        ("quantity", DataType::Int32, false),
        ("price_cents", DataType::Int64, false),
        ("fees_cents", DataType::Int64, false),
        ("slippage_cents", DataType::Int64, false),
        ("mode", DataType::Utf8, false),
    ]
}

/// `equity_curve.parquet` schema (`drawdown` the only `DOUBLE`).
fn equity_schema() -> [(&'static str, DataType, bool); 6] {
    [
        ("step", DataType::Int32, false),
        ("ts_ns", DataType::Int64, false),
        ("cash_cents", DataType::Int64, false),
        ("position_value_cents", DataType::Int64, false),
        ("equity_cents", DataType::Int64, false),
        ("drawdown", DataType::Float64, false),
    ]
}

/// `positions.parquet` schema (`exit_reason` the only nullable column).
fn positions_schema() -> [(&'static str, DataType, bool); 13] {
    [
        ("step", DataType::Int32, false),
        ("ts_ns", DataType::Int64, false),
        ("position_id", DataType::Int64, false),
        ("trade_id", DataType::Int64, false),
        ("contract_id", DataType::Utf8, false),
        ("side", DataType::Utf8, false),
        ("quantity", DataType::Int32, false),
        ("avg_price_cents", DataType::Int64, false),
        ("mark_cents", DataType::Int64, false),
        ("unrealized_cents", DataType::Int64, false),
        ("stale_mark", DataType::Boolean, false),
        ("exit_reason", DataType::Utf8, true),
        ("open_at_end", DataType::Boolean, false),
    ]
}

/// `greeks_attribution.parquet` schema.
fn greeks_schema() -> [(&'static str, DataType, bool); 8] {
    [
        ("step", DataType::Int32, false),
        ("ts_ns", DataType::Int64, false),
        ("theta_pnl_cents", DataType::Int64, false),
        ("delta_pnl_cents", DataType::Int64, false),
        ("vega_pnl_cents", DataType::Int64, false),
        ("spread_capture_cents", DataType::Int64, false),
        ("fees_cents", DataType::Int64, false),
        ("residual_cents", DataType::Int64, false),
    ]
}

/// Open, validate, and decode one Parquet table into `Vec<T>`, enforcing
/// `max_rows_per_table` and `max_decompressed_bytes` **before** decoding (and
/// again incrementally against a lying footer). Per-batch builders are pre-sized
/// from the **batch** row count (bounded by [`READ_BATCH_ROWS`]), never from the
/// untrusted declared footer count.
fn decode_table<T>(
    path: &Path,
    table: &str,
    expected: &[(&str, DataType, bool)],
    limits: &ResourceLimits,
    mut decode_batch: impl FnMut(&RecordBatch, &mut Vec<T>) -> Result<(), BacktestError>,
) -> Result<Vec<T>, BacktestError> {
    let meta =
        std::fs::metadata(path).map_err(|e| bundle_err(&format!("stat {table}.parquet"), &e))?;
    if !meta.is_file() {
        return Err(BacktestError::Bundle(format!(
            "bundle table {table}.parquet is not a regular file"
        )));
    }
    let file = File::open(path).map_err(|e| bundle_err(&format!("open {table}.parquet"), &e))?;
    // `with_skip_arrow_metadata(true)` is REQUIRED for no-panic on untrusted
    // input: a bundle table's Parquet footer may embed an `ARROW:schema` IPC
    // flatbuffer whose parse panics inside arrow-ipc (`unimplemented!` in
    // `get_data_type`), which unwinds instead of returning `Err`. Skipping it
    // derives the schema from the Parquet schema itself — which `validate_schema`
    // below already checks column-for-column, so the accepted set is unchanged.
    // The call is ALSO run under the #52 catch_unwind backstop
    // (`guard_bundle_parquet`): parquet 59.1.0 (the latest published) still has a
    // known metadata-panic class on malformed input (arrow-rs #9840, #9868,
    // #5382) that unwinds instead of returning `Err`, so any such panic is
    // contained and mapped to a typed `BacktestError::Bundle`.
    let builder = guard_bundle_parquet(table, "metadata read", || {
        ParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            ArrowReaderOptions::new().with_skip_arrow_metadata(true),
        )
    })?
    .map_err(|e| {
        bundle_err(
            &format!("read {table}.parquet metadata (truncated or corrupt footer)"),
            &e,
        )
    })?;

    // Declared row-count ceiling, BEFORE any Vec is sized.
    let declared_rows = builder.metadata().file_metadata().num_rows();
    let declared_rows = u64::try_from(declared_rows).map_err(|_| {
        BacktestError::Bundle(format!("{table}.parquet declares a negative row count"))
    })?;
    if declared_rows > limits.max_rows_per_table {
        return Err(BacktestError::TapeTooLarge {
            limit: "max_rows_per_table",
            value: declared_rows,
            cap: limits.max_rows_per_table,
        });
    }

    // Declared decompressed-size ceiling (decompression-bomb guard), BEFORE any
    // row group is decoded.
    let mut declared_bytes: u64 = 0;
    for group in builder.metadata().row_groups() {
        // (#52) Reject a negative column-chunk offset/size BEFORE the decode
        // loop. This exists to PREVENT (not merely catch) the known
        // `ColumnChunkMetaData::byte_range()` assert (parquet mod.rs:1063), which
        // PANICS during decode on a crafted negative offset: the
        // `guard_bundle_parquet` catch_unwind backstop contains that in
        // production (unwind), but cargo-fuzz builds `panic=abort` where a caught
        // panic still aborts — so the fuzz targets only stay green if the panic
        // never fires. catch_unwind remains the backstop for the residual,
        // still-unhardened parquet metadata-panic class (arrow-rs #9840, #9868,
        // #5382). A well-formed table has non-negative offsets, so this never
        // rejects a valid bundle (the goldens are unaffected).
        for col in group.columns() {
            if col.compressed_size() < 0
                || col.data_page_offset() < 0
                || col.dictionary_page_offset().is_some_and(|off| off < 0)
            {
                return Err(BacktestError::Bundle(format!(
                    "{table}.parquet column chunk declares a negative offset or size \
                     (would trip byte_range)"
                )));
            }
        }
        let size = u64::try_from(group.total_byte_size()).map_err(|_| {
            BacktestError::Bundle(format!(
                "{table}.parquet row group reports a negative byte size"
            ))
        })?;
        declared_bytes = declared_bytes
            .checked_add(size)
            .ok_or(BacktestError::ArithmeticOverflow)?;
    }
    if declared_bytes > limits.max_decompressed_bytes {
        return Err(BacktestError::TapeTooLarge {
            limit: "max_decompressed_bytes",
            value: declared_bytes,
            cap: limits.max_decompressed_bytes,
        });
    }

    // Exact schema (columns / types / nullability).
    validate_schema(table, builder.schema(), expected)?;

    let mut reader = guard_bundle_parquet(table, "reader build", || {
        builder.with_batch_size(READ_BATCH_ROWS).build()
    })?
    .map_err(|e| bundle_err(&format!("build {table}.parquet reader"), &e))?;

    let mut out: Vec<T> = Vec::new();
    let mut decoded_rows: u64 = 0;
    let mut decoded_bytes: u64 = 0;
    loop {
        // Upstream row-group decode under the #52 backstop (see the feed's
        // `guard_parquet` doc): a parquet metadata assert (e.g. a negative
        // column-chunk offset in `byte_range`) is contained as a typed error,
        // not an unwind. Our own validation below stays OUTSIDE the wrap.
        let Some(batch) = guard_bundle_parquet(table, "row-group decode", || reader.next())? else {
            break;
        };
        let batch =
            batch.map_err(|e| bundle_err(&format!("decode {table}.parquet row group"), &e))?;
        let n = batch.num_rows();

        // Incremental row + byte guards catch a footer that lied low.
        decoded_rows = decoded_rows
            .checked_add(u64::try_from(n).map_err(|_| BacktestError::ArithmeticOverflow)?)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if decoded_rows > limits.max_rows_per_table {
            return Err(BacktestError::TapeTooLarge {
                limit: "max_rows_per_table",
                value: decoded_rows,
                cap: limits.max_rows_per_table,
            });
        }
        let batch_bytes: usize = batch
            .columns()
            .iter()
            .map(|c| c.get_array_memory_size())
            .sum();
        decoded_bytes = decoded_bytes
            .checked_add(u64::try_from(batch_bytes).map_err(|_| BacktestError::ArithmeticOverflow)?)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if decoded_bytes > limits.max_decompressed_bytes {
            return Err(BacktestError::TapeTooLarge {
                limit: "max_decompressed_bytes",
                value: decoded_bytes,
                cap: limits.max_decompressed_bytes,
            });
        }

        out.reserve(n);
        decode_batch(&batch, &mut out)?;
    }
    Ok(out)
}

/// Validate a table's Arrow schema against the exact expected columns / types /
/// nullability.
fn validate_schema(
    table: &str,
    schema: &Schema,
    expected: &[(&str, DataType, bool)],
) -> Result<(), BacktestError> {
    if schema.fields().len() != expected.len() {
        return Err(BacktestError::Bundle(format!(
            "{table}.parquet has {} columns, expected exactly {}",
            schema.fields().len(),
            expected.len()
        )));
    }
    for (name, want_type, want_nullable) in expected {
        let field = schema.field_with_name(name).map_err(|_| {
            BacktestError::Bundle(format!("{table}.parquet is missing required column {name}"))
        })?;
        if field.data_type() != want_type {
            return Err(BacktestError::Bundle(format!(
                "{table}.parquet column {name} has type {:?}, expected {want_type:?}",
                field.data_type()
            )));
        }
        if field.is_nullable() != *want_nullable {
            return Err(BacktestError::Bundle(format!(
                "{table}.parquet column {name} nullability {} does not match the expected {want_nullable}",
                field.is_nullable()
            )));
        }
    }
    Ok(())
}

/// Decode one `fills.parquet` batch into [`FillRow`]s (checked crossings).
fn decode_fills_batch(
    batch: &RecordBatch,
    out: &mut Vec<FillRow>,
    limits: &ResourceLimits,
) -> Result<(), BacktestError> {
    let step = col_i32(batch, "step")?;
    let ts_ns = col_i64(batch, "ts_ns")?;
    let strategy_run_id = col_str(batch, "strategy_run_id")?;
    let trade_id = col_i64(batch, "trade_id")?;
    let position_id = col_i64(batch, "position_id")?;
    let order_id = col_i64(batch, "order_id")?;
    let fill_seq = col_i32(batch, "fill_seq")?;
    let underlying = col_str(batch, "underlying")?;
    let expiration_ns = col_i64(batch, "expiration_ns")?;
    let contract_id = col_str(batch, "contract_id")?;
    let strike_cents = col_i64(batch, "strike_cents")?;
    let style = col_str(batch, "style")?;
    let side = col_str(batch, "side")?;
    let quantity = col_i32(batch, "quantity")?;
    let price_cents = col_i64(batch, "price_cents")?;
    let fees_cents = col_i64(batch, "fees_cents")?;
    let slippage_cents = col_i64(batch, "slippage_cents")?;
    let mode = col_str(batch, "mode")?;
    for row in 0..batch.num_rows() {
        out.push(FillRow {
            step: u32_from_i32(step, row, "step")?,
            ts_ns: i64_cell(ts_ns, row, "ts_ns")?,
            strategy_run_id: string_cell(strategy_run_id, row, "strategy_run_id", limits)?,
            trade_id: u64_from_i64(trade_id, row, "trade_id")?,
            position_id: u64_from_i64(position_id, row, "position_id")?,
            order_id: u64_from_i64(order_id, row, "order_id")?,
            fill_seq: u32_from_i32(fill_seq, row, "fill_seq")?,
            underlying: string_cell(underlying, row, "underlying", limits)?,
            expiration_ns: i64_cell(expiration_ns, row, "expiration_ns")?,
            contract_id: string_cell(contract_id, row, "contract_id", limits)?,
            strike_cents: u64_from_i64(strike_cents, row, "strike_cents")?,
            style: string_cell(style, row, "style", limits)?,
            side: string_cell(side, row, "side", limits)?,
            quantity: u32_from_i32(quantity, row, "quantity")?,
            price_cents: u64_from_i64(price_cents, row, "price_cents")?,
            fees_cents: nonneg_i64_cell(fees_cents, row, "fees_cents")?,
            slippage_cents: i64_cell(slippage_cents, row, "slippage_cents")?,
            mode: string_cell(mode, row, "mode", limits)?,
        });
    }
    Ok(())
}

/// Decode one `equity_curve.parquet` batch into [`EquityPoint`]s.
fn decode_equity_batch(
    batch: &RecordBatch,
    out: &mut Vec<EquityPoint>,
) -> Result<(), BacktestError> {
    let step = col_i32(batch, "step")?;
    let ts_ns = col_i64(batch, "ts_ns")?;
    let cash = col_i64(batch, "cash_cents")?;
    let position_value = col_i64(batch, "position_value_cents")?;
    let equity = col_i64(batch, "equity_cents")?;
    let drawdown = col_f64(batch, "drawdown")?;
    for row in 0..batch.num_rows() {
        out.push(EquityPoint {
            step: u32_from_i32(step, row, "step")?,
            ts_ns: i64_cell(ts_ns, row, "ts_ns")?,
            cash_cents: i64_cell(cash, row, "cash_cents")?,
            position_value_cents: i64_cell(position_value, row, "position_value_cents")?,
            equity_cents: i64_cell(equity, row, "equity_cents")?,
            drawdown: finite_f64_cell(drawdown, row, "drawdown")?,
        });
    }
    Ok(())
}

/// Decode one `positions.parquet` batch into [`PositionRow`]s (`exit_reason` the
/// only nullable column).
fn decode_positions_batch(
    batch: &RecordBatch,
    out: &mut Vec<PositionRow>,
    limits: &ResourceLimits,
) -> Result<(), BacktestError> {
    let step = col_i32(batch, "step")?;
    let ts_ns = col_i64(batch, "ts_ns")?;
    let position_id = col_i64(batch, "position_id")?;
    let trade_id = col_i64(batch, "trade_id")?;
    let contract_id = col_str(batch, "contract_id")?;
    let side = col_str(batch, "side")?;
    let quantity = col_i32(batch, "quantity")?;
    let avg_price = col_i64(batch, "avg_price_cents")?;
    let mark = col_i64(batch, "mark_cents")?;
    let unrealized = col_i64(batch, "unrealized_cents")?;
    let stale = col_bool(batch, "stale_mark")?;
    let exit_reason = col_str(batch, "exit_reason")?;
    let open_at_end = col_bool(batch, "open_at_end")?;
    for row in 0..batch.num_rows() {
        out.push(PositionRow {
            step: u32_from_i32(step, row, "step")?,
            ts_ns: i64_cell(ts_ns, row, "ts_ns")?,
            position_id: u64_from_i64(position_id, row, "position_id")?,
            trade_id: u64_from_i64(trade_id, row, "trade_id")?,
            contract_id: string_cell(contract_id, row, "contract_id", limits)?,
            side: string_cell(side, row, "side", limits)?,
            quantity: u32_from_i32(quantity, row, "quantity")?,
            avg_price_cents: u64_from_i64(avg_price, row, "avg_price_cents")?,
            mark_cents: u64_from_i64(mark, row, "mark_cents")?,
            unrealized_cents: i64_cell(unrealized, row, "unrealized_cents")?,
            stale_mark: bool_cell(stale, row, "stale_mark")?,
            exit_reason: opt_string_cell(exit_reason, row, "exit_reason", limits)?,
            open_at_end: bool_cell(open_at_end, row, "open_at_end")?,
        });
    }
    Ok(())
}

/// Decode one `greeks_attribution.parquet` batch into [`GreeksAttributionRow`]s.
fn decode_greeks_batch(
    batch: &RecordBatch,
    out: &mut Vec<GreeksAttributionRow>,
) -> Result<(), BacktestError> {
    let step = col_i32(batch, "step")?;
    let ts_ns = col_i64(batch, "ts_ns")?;
    let theta = col_i64(batch, "theta_pnl_cents")?;
    let delta = col_i64(batch, "delta_pnl_cents")?;
    let vega = col_i64(batch, "vega_pnl_cents")?;
    let spread = col_i64(batch, "spread_capture_cents")?;
    let fees = col_i64(batch, "fees_cents")?;
    let residual = col_i64(batch, "residual_cents")?;
    for row in 0..batch.num_rows() {
        out.push(GreeksAttributionRow {
            step: u32_from_i32(step, row, "step")?,
            ts_ns: i64_cell(ts_ns, row, "ts_ns")?,
            theta_pnl_cents: i64_cell(theta, row, "theta_pnl_cents")?,
            delta_pnl_cents: i64_cell(delta, row, "delta_pnl_cents")?,
            vega_pnl_cents: i64_cell(vega, row, "vega_pnl_cents")?,
            spread_capture_cents: i64_cell(spread, row, "spread_capture_cents")?,
            fees_cents: nonneg_i64_cell(fees, row, "fees_cents")?,
            residual_cents: i64_cell(residual, row, "residual_cents")?,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// (4) row_counts cross-check
// ---------------------------------------------------------------------------

/// Cross-check a decoded table length against the manifest `row_counts` value.
fn check_count(table: &str, decoded: usize, expected: u64) -> Result<(), BacktestError> {
    let decoded = u64::try_from(decoded).map_err(|_| BacktestError::ArithmeticOverflow)?;
    if decoded != expected {
        return Err(BacktestError::Bundle(format!(
            "row_counts.{table} = {expected} but {decoded} rows decoded"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// (5) Referenced-input sha256 (where reachable)
// ---------------------------------------------------------------------------

/// Verify the recorded `sha256` of a referenced **single-file Parquet** input
/// where reachable.
///
/// - An unpinned (`sha256 == ""`) source, a CSV directory source (its identity
///   is a directory hash re-verified by [`crate::CsvFeed::open_verified`], not a
///   single file), and a simulator source are skipped.
/// - With `missing_is_error == false` (the [`read_bundle`] path) an unreachable
///   file is skipped so a **portable** bundle read succeeds without its inputs; a
///   reachable file whose bytes differ from the recorded hash is a typed error.
/// - With `missing_is_error == true` a missing / non-regular file is itself a
///   typed error (a strict re-verification context).
fn verify_referenced_input(
    spec: &DataSourceSpec,
    limits: &ResourceLimits,
    missing_is_error: bool,
) -> Result<(), BacktestError> {
    let (path, recorded) = match spec {
        DataSourceSpec::Parquet { path, sha256 } => (path.as_str(), sha256.as_str()),
        DataSourceSpec::Csv { .. } => return Ok(()),
        #[cfg(feature = "simulator")]
        DataSourceSpec::Simulator { .. } => return Ok(()),
    };
    if recorded.is_empty() {
        return Ok(());
    }
    let path_ref = Path::new(path);
    let meta = match std::fs::metadata(path_ref) {
        Ok(meta) => meta,
        Err(_) if !missing_is_error => return Ok(()),
        Err(error) => {
            return Err(bundle_err(
                &format!("referenced input {path} is unreachable"),
                &error,
            ));
        }
    };
    if !meta.is_file() {
        if missing_is_error {
            return Err(BacktestError::Bundle(format!(
                "referenced input {path} is not a regular file"
            )));
        }
        return Ok(());
    }
    let recomputed = file_sha256_bounded(path_ref, limits.max_file_bytes)?;
    if recomputed != recorded {
        return Err(BacktestError::Bundle(format!(
            "referenced input {path} sha256 mismatch: recorded {recorded}, recomputed {recomputed}"
        )));
    }
    Ok(())
}

/// Streaming sha256 (lower-hex) of a file, bounded by `max_bytes`
/// ([`Read::take`]) so a hostile path can never cause an unbounded read.
fn file_sha256_bounded(path: &Path, max_bytes: u64) -> Result<String, BacktestError> {
    let file = File::open(path).map_err(|e| bundle_err("open referenced input", &e))?;
    let mut reader = file.take(max_bytes);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; HASH_CHUNK_BYTES];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| bundle_err("read referenced input", &e))?;
        if n == 0 {
            break;
        }
        let chunk = buf
            .get(..n)
            .ok_or_else(|| BacktestError::Bundle("hash chunk out of range".to_string()))?;
        hasher.update(chunk);
    }
    Ok(to_hex(&hasher.finalize()))
}

// ---------------------------------------------------------------------------
// (6) contract_id round-trip + UNDERLYING grammar
// ---------------------------------------------------------------------------

/// A `fills` `contract_id` must parse back (grammar included) equal to the row's
/// own `underlying` / `expiration_ns` / `strike_cents` / `style` columns.
fn verify_fill_contract_id(row: &FillRow) -> Result<(), BacktestError> {
    let key = ContractKey::from_contract_id(&row.contract_id).map_err(|e| {
        BacktestError::Bundle(format!(
            "fills contract_id {:?} is not parseable: {e}",
            row.contract_id
        ))
    })?;
    let expiration_ns = key.expiration_ns().map_err(|e| {
        BacktestError::Bundle(format!("fills contract_id {:?}: {e}", row.contract_id))
    })?;
    if key.underlying.as_str() != row.underlying
        || expiration_ns != row.expiration_ns
        || key.strike.value() != row.strike_cents
        || style_str(key.style) != row.style
    {
        return Err(BacktestError::Bundle(format!(
            "fills contract_id {:?} does not round-trip to its own columns",
            row.contract_id
        )));
    }
    let rebuilt = key.to_contract_id().map_err(|e| {
        BacktestError::Bundle(format!(
            "fills contract_id {:?} rebuild: {e}",
            row.contract_id
        ))
    })?;
    if rebuilt != row.contract_id {
        return Err(BacktestError::Bundle(format!(
            "fills contract_id {:?} is not round-trippable (rebuilt {rebuilt:?})",
            row.contract_id
        )));
    }
    Ok(())
}

/// A `positions` `contract_id` must parse (grammar included) and re-serialise to
/// exactly itself.
fn verify_position_contract_id(row: &PositionRow) -> Result<(), BacktestError> {
    let key = ContractKey::from_contract_id(&row.contract_id).map_err(|e| {
        BacktestError::Bundle(format!(
            "positions contract_id {:?} is not parseable: {e}",
            row.contract_id
        ))
    })?;
    let rebuilt = key.to_contract_id().map_err(|e| {
        BacktestError::Bundle(format!(
            "positions contract_id {:?} rebuild: {e}",
            row.contract_id
        ))
    })?;
    if rebuilt != row.contract_id {
        return Err(BacktestError::Bundle(format!(
            "positions contract_id {:?} is not round-trippable (rebuilt {rebuilt:?})",
            row.contract_id
        )));
    }
    Ok(())
}

/// The wire string for an option style.
const fn style_str(style: OptionStyle) -> &'static str {
    match style {
        OptionStyle::Call => "call",
        OptionStyle::Put => "put",
    }
}

// ---------------------------------------------------------------------------
// Typed column access + no-panic cell readers
// ---------------------------------------------------------------------------

/// Downcast a batch column to a concrete Arrow array by name — total after
/// [`validate_schema`], but typed-fallible rather than panicking.
fn downcast<'b, A: Array + 'static>(
    batch: &'b RecordBatch,
    name: &str,
) -> Result<&'b A, BacktestError> {
    let column = batch.column_by_name(name).ok_or_else(|| {
        BacktestError::Bundle(format!("bundle table batch missing column {name}"))
    })?;
    column.as_any().downcast_ref::<A>().ok_or_else(|| {
        BacktestError::Bundle(format!(
            "bundle column {name} has an unexpected physical type"
        ))
    })
}

fn col_i32<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b Int32Array, BacktestError> {
    downcast::<Int32Array>(batch, name)
}
fn col_i64<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b Int64Array, BacktestError> {
    downcast::<Int64Array>(batch, name)
}
fn col_str<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b StringArray, BacktestError> {
    downcast::<StringArray>(batch, name)
}
fn col_bool<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b BooleanArray, BacktestError> {
    downcast::<BooleanArray>(batch, name)
}
fn col_f64<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b Float64Array, BacktestError> {
    downcast::<Float64Array>(batch, name)
}

/// A null in a required column is a typed error, never an unchecked read.
fn null_err(column: &str, row: usize) -> BacktestError {
    BacktestError::Bundle(format!(
        "null value in required column {column} at row {row}"
    ))
}

/// Cross a signed `INT32` to a non-negative `u32` (a negative value is a typed
/// error, never an `as` cast).
fn u32_from_i32(array: &Int32Array, row: usize, column: &str) -> Result<u32, BacktestError> {
    if array.is_null(row) {
        return Err(null_err(column, row));
    }
    let value = array.value(row);
    u32::try_from(value).map_err(|_| {
        BacktestError::Bundle(format!(
            "column {column} has a negative value {value} at row {row}"
        ))
    })
}

/// Read a required non-null signed `INT64`.
fn i64_cell(array: &Int64Array, row: usize, column: &str) -> Result<i64, BacktestError> {
    if array.is_null(row) {
        return Err(null_err(column, row));
    }
    Ok(array.value(row))
}

/// Cross a signed `INT64` to a `u64` (a negative value is a typed error).
fn u64_from_i64(array: &Int64Array, row: usize, column: &str) -> Result<u64, BacktestError> {
    if array.is_null(row) {
        return Err(null_err(column, row));
    }
    let value = array.value(row);
    u64::try_from(value).map_err(|_| {
        BacktestError::Bundle(format!(
            "column {column} has a negative value {value} at row {row}"
        ))
    })
}

/// Read a non-negative-cents `INT64` (a negative value violates the contract and
/// is a typed error).
fn nonneg_i64_cell(array: &Int64Array, row: usize, column: &str) -> Result<i64, BacktestError> {
    let value = i64_cell(array, row, column)?;
    if value < 0 {
        return Err(BacktestError::Bundle(format!(
            "column {column} must be non-negative, got {value} at row {row}"
        )));
    }
    Ok(value)
}

/// Read a required non-null boolean.
fn bool_cell(array: &BooleanArray, row: usize, column: &str) -> Result<bool, BacktestError> {
    if array.is_null(row) {
        return Err(null_err(column, row));
    }
    Ok(array.value(row))
}

/// Read a required finite `DOUBLE` (a `NaN` / `±∞` is a load error).
fn finite_f64_cell(array: &Float64Array, row: usize, column: &str) -> Result<f64, BacktestError> {
    if array.is_null(row) {
        return Err(null_err(column, row));
    }
    let value = array.value(row);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(BacktestError::Bundle(format!(
            "column {column} is not finite ({value}) at row {row}"
        )))
    }
}

/// Read a required non-null string, bounded by `max_string_len`.
fn string_cell(
    array: &StringArray,
    row: usize,
    column: &str,
    limits: &ResourceLimits,
) -> Result<String, BacktestError> {
    if array.is_null(row) {
        return Err(null_err(column, row));
    }
    let value = array.value(row);
    check_string_len(value.len(), limits)?;
    Ok(value.to_string())
}

/// Read an optional (nullable) string, bounded by `max_string_len` when present.
fn opt_string_cell(
    array: &StringArray,
    row: usize,
    column: &str,
    limits: &ResourceLimits,
) -> Result<Option<String>, BacktestError> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(string_cell(array, row, column, limits)?))
}

/// Enforce `max_string_len` on one string length.
fn check_string_len(len: usize, limits: &ResourceLimits) -> Result<(), BacktestError> {
    let len = u64::try_from(len).map_err(|_| BacktestError::ArithmeticOverflow)?;
    let cap = u64::from(limits.max_string_len);
    if len > cap {
        return Err(BacktestError::TapeTooLarge {
            limit: "max_string_len",
            value: len,
            cap,
        });
    }
    Ok(())
}

/// Wrap a failed I/O / Arrow / Parquet operation as a [`BacktestError::Bundle`].
fn bundle_err(context: &str, error: &dyn std::fmt::Display) -> BacktestError {
    BacktestError::Bundle(format!("{context}: {error}"))
}

/// Run one untrusted-Parquet upstream call under the #52 catch_unwind backstop:
/// a panic inside arrow/parquet's metadata parse or row-group decode is
/// **contained** and mapped to a typed [`BacktestError::Bundle`], never an
/// unwind across the read-back boundary
/// ([docs/07 §8](../../../docs/07-performance-and-security.md#8-untrusted-input-hardening)).
/// It wraps ONLY the upstream call — the reader's own validation stays outside —
/// so a bug in THIS crate still panics loudly in tests. The feed side
/// (`crate::data::historical::guard_parquet`) is the twin for the CSV/Parquet
/// feeds; parquet 59.1.0 (the latest published) still carries a known
/// metadata-panic class on malformed input (arrow-rs #9840, #9868, #5382).
///
/// `AssertUnwindSafe` is justified: every value the closure touches (the `File`,
/// the builder, the reader) is local to the call and is dropped on the unwind
/// path; nothing shared or observable survives a caught panic, and the caller
/// returns immediately without reusing the poisoned value.
fn guard_bundle_parquet<T>(
    table: &str,
    op: &str,
    f: impl FnOnce() -> T,
) -> Result<T, BacktestError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|payload| {
        let msg = crate::error::panic_payload_message(&*payload);
        tracing::warn!(
            target: "ironcondor::bundle",
            table,
            op,
            panic = %msg,
            "contained an upstream parquet panic (#52 backstop)"
        );
        BacktestError::Bundle(format!(
            "{table}.parquet {op} panicked inside arrow/parquet and was contained by the #52 backstop: {msg}"
        ))
    })
}

/// Lower-hex encode a byte slice.
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
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use chrono::DateTime;
    use optionstratlib::backtesting::BacktestResult;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use parquet::arrow::ArrowWriter;
    use serde_json::Value;

    use super::{read_bundle, verify_referenced_input};
    use crate::bundle::write_bundle;
    use crate::config::{BacktestConfig, FeeSchedule, ResourceLimits, SlippageModel};
    use crate::data::DataSourceSpec;
    use crate::domain::{
        Cents, ContractKey, ExecutionMode, Fill, IronCondorSpec, PriceCents, Quantity, SimTime,
        StepIndex, StrategySpec, Underlying,
    };
    use crate::engine::{BacktestRun, FillRecord, PositionSnapshot};
    use crate::error::BacktestError;

    const TS0: i64 = 1_750_291_200_000_000_000;
    const CONTRACT_ID: &str = "v1:SPX:1750291200000000000:510000:C";

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

    fn fill() -> Fill {
        Fill {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            contract: key(),
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

    fn config(output_dir: &Path) -> BacktestConfig {
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
            limits: ResourceLimits::default(),
            output_dir: output_dir.to_path_buf(),
            overwrite: false,
        }
    }

    /// A minimal but complete run: one open fill, one open position row, one
    /// equity point, one attribution row — a valid `ironcondor.bundle.v1`.
    fn sample_run() -> BacktestRun {
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
                fill: fill(),
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
                contract: key(),
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

    /// Write a valid sample bundle into a fresh subdir and return its `<run_id>`
    /// directory.
    fn write_sample_bundle(output_dir: &Path) -> PathBuf {
        let Ok(dest) = write_bundle(&sample_run(), &config(output_dir), &strategy()) else {
            panic!("the sample bundle must write");
        };
        dest
    }

    fn tmp() -> tempfile::TempDir {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir");
        };
        dir
    }

    /// Read `manifest.json` as a JSON value.
    fn manifest_value(bundle: &Path) -> Value {
        let Ok(text) = std::fs::read_to_string(bundle.join("manifest.json")) else {
            panic!("manifest readable");
        };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            panic!("manifest is JSON");
        };
        value
    }

    /// Mutate `manifest.json` in place through its top-level object.
    fn patch_manifest(bundle: &Path, mutate: impl FnOnce(&mut serde_json::Map<String, Value>)) {
        let mut value = manifest_value(bundle);
        let Some(obj) = value.as_object_mut() else {
            panic!("manifest is an object");
        };
        mutate(obj);
        let Ok(bytes) = serde_json::to_vec(&value) else {
            panic!("manifest re-serialises");
        };
        if std::fs::write(bundle.join("manifest.json"), bytes).is_err() {
            panic!("manifest re-writes");
        }
    }

    fn fills_schema_ref() -> SchemaRef {
        Arc::new(Schema::new(vec![
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
        ]))
    }

    /// Overwrite `fills.parquet` with a single row carrying the given `order_id`
    /// and `contract_id` (all other columns valid), so a specific decode /
    /// round-trip rejection can be exercised.
    fn overwrite_one_fill(bundle: &Path, run_id: &str, contract_id: &str, order_id: i64) {
        let schema = fills_schema_ref();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![0i32])) as ArrayRef,
            Arc::new(Int64Array::from(vec![TS0])),
            Arc::new(StringArray::from(vec![run_id])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![order_id])),
            Arc::new(Int32Array::from(vec![0i32])),
            Arc::new(StringArray::from(vec!["SPX"])),
            Arc::new(Int64Array::from(vec![TS0])),
            Arc::new(StringArray::from(vec![contract_id])),
            Arc::new(Int64Array::from(vec![510_000i64])),
            Arc::new(StringArray::from(vec!["call"])),
            Arc::new(StringArray::from(vec!["short"])),
            Arc::new(Int32Array::from(vec![1i32])),
            Arc::new(Int64Array::from(vec![2_000i64])),
            Arc::new(Int64Array::from(vec![65i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(StringArray::from(vec!["naive"])),
        ];
        write_batch_to(bundle.join("fills.parquet").as_path(), &schema, columns);
    }

    /// Overwrite `equity_curve.parquet` with a single row whose `step` column is
    /// `INT64` instead of the contract's `INT32` — a wrong-column-type fixture.
    fn overwrite_equity_wrong_step_type(bundle: &Path) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("step", DataType::Int64, false),
            Field::new("ts_ns", DataType::Int64, false),
            Field::new("cash_cents", DataType::Int64, false),
            Field::new("position_value_cents", DataType::Int64, false),
            Field::new("equity_cents", DataType::Int64, false),
            Field::new("drawdown", DataType::Float64, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![TS0])),
            Arc::new(Int64Array::from(vec![10_020_000i64])),
            Arc::new(Int64Array::from(vec![-20_000i64])),
            Arc::new(Int64Array::from(vec![10_000_000i64])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0f64])),
        ];
        write_batch_to(
            bundle.join("equity_curve.parquet").as_path(),
            &schema,
            columns,
        );
    }

    fn write_batch_to(path: &Path, schema: &SchemaRef, columns: Vec<ArrayRef>) {
        let Ok(batch) = RecordBatch::try_new(Arc::clone(schema), columns) else {
            panic!("record batch builds");
        };
        let Ok(file) = std::fs::File::create(path) else {
            panic!("table file creates");
        };
        let Ok(mut writer) = ArrowWriter::try_new(file, Arc::clone(schema), None) else {
            panic!("parquet writer opens");
        };
        if writer.write(&batch).is_err() {
            panic!("batch writes");
        }
        if writer.close().is_err() {
            panic!("writer closes");
        }
    }

    fn run_id_of(bundle: &Path) -> String {
        let Some(name) = bundle.file_name().and_then(|n| n.to_str()) else {
            panic!("bundle dir has a name");
        };
        name.to_string()
    }

    #[test]
    fn test_read_bundle_round_trips_a_written_sample_bundle() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        let Ok(read) = read_bundle(&bundle, &ResourceLimits::default()) else {
            panic!("the written bundle must read back");
        };
        assert_eq!(read.manifest.schema, super::BUNDLE_SCHEMA);
        assert_eq!(read.fills.len(), 1);
        assert_eq!(read.positions.len(), 1);
        assert_eq!(read.greeks_attribution, sample_run().greeks_attribution);
        assert_eq!(read.equity_curve, sample_run().equity_curve);
        // metrics stays opaque (a JSON value, not parsed into fields).
        assert!(read.manifest.metrics.is_object());
    }

    #[test]
    fn test_read_bundle_wrong_schema_tag_rejected_before_tables() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        // An unknown major tag AND a corrupt table: the schema tag fails first,
        // proving it is checked before any table is parsed.
        patch_manifest(&bundle, |obj| {
            obj.insert(
                "schema".to_string(),
                Value::String("ironcondor.bundle.v2".to_string()),
            );
        });
        if std::fs::write(bundle.join("fills.parquet"), b"not parquet").is_err() {
            panic!("corrupt the fills table");
        }
        match read_bundle(&bundle, &ResourceLimits::default()) {
            Err(BacktestError::Bundle(msg)) => {
                assert!(msg.contains("schema tag"), "schema error first: {msg}");
            }
            other => panic!("expected a schema-tag Bundle error, got {other:?}"),
        }
    }

    #[test]
    fn test_read_bundle_missing_required_field_rejected() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        patch_manifest(&bundle, |obj| {
            obj.remove("seed");
        });
        assert!(matches!(
            read_bundle(&bundle, &ResourceLimits::default()),
            Err(BacktestError::Bundle(_))
        ));
    }

    #[test]
    fn test_read_bundle_wrong_column_type_rejected() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        overwrite_equity_wrong_step_type(&bundle);
        assert!(matches!(
            read_bundle(&bundle, &ResourceLimits::default()),
            Err(BacktestError::Bundle(_))
        ));
    }

    #[test]
    fn test_read_bundle_row_counts_mismatch_rejected() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        patch_manifest(&bundle, |obj| {
            if let Some(counts) = obj.get_mut("row_counts").and_then(Value::as_object_mut) {
                counts.insert("fills".to_string(), Value::from(999u64));
            }
        });
        assert!(matches!(
            read_bundle(&bundle, &ResourceLimits::default()),
            Err(BacktestError::Bundle(_))
        ));
    }

    #[test]
    fn test_read_bundle_negative_id_rejected_via_checked_try_from() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        let run_id = run_id_of(&bundle);
        overwrite_one_fill(&bundle, &run_id, CONTRACT_ID, -1);
        assert!(matches!(
            read_bundle(&bundle, &ResourceLimits::default()),
            Err(BacktestError::Bundle(_))
        ));
    }

    #[test]
    fn test_read_bundle_non_round_trippable_contract_id_rejected() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        let run_id = run_id_of(&bundle);
        // A lowercase underlying violates the UNDERLYING grammar, so the
        // contract_id cannot round-trip.
        overwrite_one_fill(&bundle, &run_id, "v1:spx:1750291200000000000:510000:C", 1);
        assert!(matches!(
            read_bundle(&bundle, &ResourceLimits::default()),
            Err(BacktestError::Bundle(_))
        ));
    }

    #[test]
    fn test_read_bundle_over_max_manifest_bytes_yields_tape_too_large() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        let limits = ResourceLimits {
            max_manifest_bytes: 1,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            read_bundle(&bundle, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_manifest_bytes",
                ..
            })
        ));
    }

    #[test]
    fn test_read_bundle_over_max_rows_per_table_yields_tape_too_large() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        let limits = ResourceLimits {
            max_rows_per_table: 0,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            read_bundle(&bundle, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_rows_per_table",
                ..
            })
        ));
    }

    #[test]
    fn test_read_bundle_over_max_string_len_yields_tape_too_large() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        // A 3-byte cap trips on the manifest's own strings (schema tag alone is
        // longer), never allocating unbounded.
        let limits = ResourceLimits {
            max_string_len: 3,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            read_bundle(&bundle, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_string_len",
                ..
            })
        ));
    }

    #[test]
    fn test_verify_referenced_input_hash_mismatch_yields_error() {
        let dir = tmp();
        let path = dir.path().join("input.parquet");
        if std::fs::write(&path, b"some bytes").is_err() {
            panic!("write referenced input");
        }
        let spec = DataSourceSpec::Parquet {
            path: path.display().to_string(),
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        };
        // Reachable file + a non-matching recorded hash ⇒ typed error (never a
        // silent divergent run).
        assert!(matches!(
            verify_referenced_input(&spec, &ResourceLimits::default(), false),
            Err(BacktestError::Bundle(_))
        ));
    }

    #[test]
    fn test_verify_referenced_input_missing_file_strict_yields_error() {
        let spec = DataSourceSpec::Parquet {
            path: "/definitely/not/here.parquet".to_string(),
            sha256: "abc".to_string(),
        };
        // Strict mode: a missing artifact is a typed error.
        assert!(matches!(
            verify_referenced_input(&spec, &ResourceLimits::default(), true),
            Err(BacktestError::Bundle(_))
        ));
        // Where-reachable mode: a missing artifact is skipped (portable read).
        assert!(matches!(
            verify_referenced_input(&spec, &ResourceLimits::default(), false),
            Ok(())
        ));
    }

    #[test]
    fn test_verify_referenced_input_unpinned_or_csv_skipped() {
        // An empty recorded sha256 is skipped (not yet pinned).
        let unpinned = DataSourceSpec::Parquet {
            path: "/nope".to_string(),
            sha256: String::new(),
        };
        assert!(matches!(
            verify_referenced_input(&unpinned, &ResourceLimits::default(), true),
            Ok(())
        ));
        // A CSV directory source is re-verified by the feed, not the bundle reader.
        let csv = DataSourceSpec::Csv {
            path: "/nope".to_string(),
            sha256: "abc".to_string(),
        };
        assert!(matches!(
            verify_referenced_input(&csv, &ResourceLimits::default(), true),
            Ok(())
        ));
    }

    #[test]
    fn test_read_bundle_missing_table_file_rejected() {
        let dir = tmp();
        let bundle = write_sample_bundle(dir.path());
        if std::fs::remove_file(bundle.join("positions.parquet")).is_err() {
            panic!("remove a table file");
        }
        assert!(read_bundle(&bundle, &ResourceLimits::default()).is_err());
    }
}
