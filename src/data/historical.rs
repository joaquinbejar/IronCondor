//! The historical **Parquet** feed — the v0.1 release feed.
//!
//! [`ParquetFeed`] reads one columnar file (one row per quote across every
//! step), groups the rows into [`ChainSnapshot`]s by ascending `step`, and
//! funnels each group through the single conversion boundary
//! ([`raw_quotes_to_snapshot`], [docs/03 §7](../../../docs/03-data-layer.md#7-chainresponse--optionchain-conversion)).
//! It materialises a **validated, strictly-ordered, immutable tape at
//! construction** and yields from it synchronously, so the replay loop never
//! blocks ([docs/03 §5](../../../docs/03-data-layer.md#5-historical-parquet-schema),
//! [§6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)).
//!
//! # The on-disk schema (validated once, at the boundary)
//!
//! An explicit `step` (`INT32`, 0-based) groups the rows; the remaining
//! columns mirror the shared CSV schema
//! ([docs/03 §4](../../../docs/03-data-layer.md#4-historical-csv-schema)) —
//! money columns are **integer** (`INT64` / `INT32`), analytics are `DOUBLE`.
//! The reader validates the exact column set and dtype up front and **refuses
//! a file whose money columns are floats** (→ [`BacktestError::Conversion`]).
//! Because money already arrives as integer cents, no rounding happens here:
//! the feed constructs [`PriceCents`] directly and reuses the same validation
//! core the simulator feed uses — there is **no second validation path**.
//!
//! # Untrusted-input hardening
//!
//! The file is untrusted ([docs/07 §8](../../../docs/07-performance-and-security.md#8-untrusted-input-hardening)):
//! every failure is a typed [`BacktestError`], never a panic, hang, or OOM.
//! The [`ResourceLimits`] ceilings are enforced with no unbounded read —
//! `max_file_bytes` from the filesystem metadata **before** any byte is read,
//! `max_decompressed_bytes` from the Parquet row-group metadata **before**
//! decoding (and again incrementally while decoding), and `max_steps` /
//! `max_contracts_per_snapshot` **during** materialisation. There is no
//! `.unwrap()` / `.expect()` / unchecked `[]` on the ingestion path.
//!
//! # Corrupt bytes vs semantic failures
//!
//! Undecodable input — a truncated or corrupt Parquet footer or row group —
//! maps to [`BacktestError::Data`]. A semantic failure on data that decoded
//! cleanly — a float money column, a missing column, a crossed quote, a
//! non-tick-aligned price — maps to [`BacktestError::Conversion`]. Ceilings
//! map to [`BacktestError::TapeTooLarge`]. The split keeps the error message
//! honest ("your file is corrupt" vs "your data is semantically wrong") and
//! is what the PyO3 boundary later maps to distinct Python exceptions.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Schema};
use chrono::DateTime;
use optionstratlib::{ExpirationDate, OptionStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};

use crate::config::ResourceLimits;
use crate::data::DataSourceSpec;
use crate::data::convert::{RawQuote, SnapshotMeta, raw_quotes_to_snapshot};
use crate::data::feed::{DataFeed, TapeMeta};
use crate::domain::{ChainSnapshot, PriceCents, Quantity, SimTime, StepIndex, Underlying};
use crate::error::BacktestError;

/// Rows decoded per Arrow batch. Bounds the per-batch working set independently
/// of the file's row-group sizing; the incremental decompression guard reads
/// each batch's materialised bytes as it goes.
const READ_BATCH_ROWS: usize = 8_192;

/// The size, in bytes, of the streaming buffer used to hash the file for its
/// [`TapeMeta::data_identity`]. The read is bounded by `max_file_bytes`.
const HASH_CHUNK_BYTES: usize = 65_536;

/// The historical Parquet [`DataFeed`] — a materialised, validated, immutable
/// tape over one columnar chain file.
///
/// Construct with [`ParquetFeed::open`]; all I/O happens there. [`DataFeed::next`]
/// is a pure in-memory read that never blocks or `.await`s.
#[derive(Debug)]
#[must_use = "a ParquetFeed does nothing unless its snapshots are consumed via DataFeed::next"]
pub struct ParquetFeed {
    /// The validated, strictly `ts`-ordered snapshots (the replay tape).
    tape: Vec<ChainSnapshot>,
    /// The next index [`DataFeed::next`] yields.
    cursor: usize,
    /// The pinned tape metadata (identity, non-empty, first ts, final step).
    meta: TapeMeta,
    /// The source path, recorded verbatim in the manifest provenance.
    path: String,
    /// The file `sha256` (hex) — the tape's data identity.
    sha256: String,
}

impl ParquetFeed {
    /// Open a Parquet chain file, materialising and validating the whole tape.
    ///
    /// Enforces, in order: `max_file_bytes` (filesystem metadata, before any
    /// read), the file `sha256` (streaming, bounded), the row-group
    /// `max_decompressed_bytes` guard (metadata, before decoding), the exact
    /// column/dtype schema (refusing float money columns), then per-group
    /// materialisation with `max_steps` / `max_contracts_per_snapshot` and the
    /// incremental decompression guard. The tape is finally checked non-empty
    /// and strictly `ts`-increasing via [`TapeMeta::from_tape`].
    ///
    /// # Errors
    ///
    /// - [`BacktestError::DataIo`] — the file cannot be stat-ed, opened, or read.
    /// - [`BacktestError::TapeTooLarge`] — a `max_file_bytes` /
    ///   `max_decompressed_bytes` / `max_steps` / `max_contracts_per_snapshot`
    ///   ceiling is crossed (`limit` names the field).
    /// - [`BacktestError::Data`] — undecodable bytes: a truncated / corrupt
    ///   Parquet footer or row group.
    /// - [`BacktestError::Conversion`] — a schema or dtype mismatch (including
    ///   a float money column), a null in a required column, a bad ticker /
    ///   style, a negative money value, an empty tape, or any snapshot-level
    ///   validation the conversion core raises on data that decoded cleanly.
    /// - [`BacktestError::PriceNotTickAligned`] / [`BacktestError::CrossedQuote`]
    ///   / [`BacktestError::InvalidQuantity`] / [`BacktestError::ArithmeticOverflow`]
    ///   — raised by the conversion core on a bad quote.
    /// - [`BacktestError::DataOutOfOrder`] — a duplicate or reversed `ts`.
    pub fn open(path: impl AsRef<Path>, limits: &ResourceLimits) -> Result<Self, BacktestError> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().into_owned();

        // (1) File-size ceiling, from the filesystem metadata, BEFORE any read.
        //     Reject a non-regular file first: a FIFO / pipe / device reports
        //     `len() == 0`, which would slip past the size guard and let the
        //     streaming hash below block indefinitely on a hostile path.
        let metadata = std::fs::metadata(path_ref)?;
        if !metadata.is_file() {
            return Err(BacktestError::Conversion(format!(
                "feed path is not a regular file: {path_str}"
            )));
        }
        let file_len = metadata.len();
        if file_len > limits.max_file_bytes {
            return Err(BacktestError::TapeTooLarge {
                limit: "max_file_bytes",
                value: file_len,
                cap: limits.max_file_bytes,
            });
        }

        // (2) Streaming file sha256 = the tape's data identity (bounded by the
        //     size ceiling checked above).
        let sha256 = file_sha256(path_ref)?;

        // (3) Read the Parquet footer metadata; a truncated / corrupt footer is
        //     a typed Conversion, never a panic.
        let file = File::open(path_ref)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            conv(
                "failed to read parquet metadata (truncated or corrupt footer)",
                &e,
            )
        })?;

        // (3a) Decompression-bomb guard: the summed uncompressed row-group size
        //      is bounded BEFORE any data is decoded.
        let mut declared_bytes: u64 = 0;
        for group in builder.metadata().row_groups() {
            let size = u64::try_from(group.total_byte_size()).map_err(|_| {
                BacktestError::Data("parquet row group reports a negative byte size".to_string())
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

        // (3b) Exact schema + dtype validation (refuses float money columns).
        let schema = builder.schema().clone();
        validate_schema(&schema)?;

        // (4) Materialise the tape, grouping rows by ascending `step`.
        let reader = builder
            .with_batch_size(READ_BATCH_ROWS)
            .build()
            .map_err(|e| conv("failed to build parquet reader", &e))?;

        let mut tape: Vec<ChainSnapshot> = Vec::new();
        let mut anchor_ts: Option<SimTime> = None;
        let mut current: Option<GroupBuilder> = None;
        let mut prev_step: Option<i32> = None;
        let mut decoded_bytes: u64 = 0;

        for batch in reader {
            let batch = batch.map_err(|e| conv("failed to decode a parquet row group", &e))?;

            // Incremental decompression guard: bound the actual materialised
            // bytes as we go, so a lying footer cannot slip a bomb past (3a).
            let batch_bytes: usize = batch
                .columns()
                .iter()
                .map(|c| c.get_array_memory_size())
                .sum();
            let batch_bytes =
                u64::try_from(batch_bytes).map_err(|_| BacktestError::ArithmeticOverflow)?;
            decoded_bytes = decoded_bytes
                .checked_add(batch_bytes)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            if decoded_bytes > limits.max_decompressed_bytes {
                return Err(BacktestError::TapeTooLarge {
                    limit: "max_decompressed_bytes",
                    value: decoded_bytes,
                    cap: limits.max_decompressed_bytes,
                });
            }

            let columns = Columns::new(&batch)?;
            for row in 0..columns.len {
                let step_raw = req_i32(columns.step, row, "step")?;
                let new_group = match &current {
                    Some(group) => group.step_raw != step_raw,
                    None => true,
                };
                if new_group {
                    if let Some(group) = current.take() {
                        push_snapshot(&mut tape, group, &mut anchor_ts, limits)?;
                    }
                    if let Some(previous) = prev_step
                        && step_raw <= previous
                    {
                        return Err(BacktestError::Conversion(format!(
                            "step {step_raw} is not strictly after previous step {previous}; \
                             parquet rows must be grouped and ascending by step"
                        )));
                    }
                    prev_step = Some(step_raw);
                    current = Some(GroupBuilder::start(&columns, row, step_raw)?);
                }
                match current.as_mut() {
                    Some(group) => group.push_row(&columns, row, limits)?,
                    None => {
                        return Err(BacktestError::Conversion(
                            "internal grouping error: no active step group".to_string(),
                        ));
                    }
                }
            }
        }
        if let Some(group) = current.take() {
            push_snapshot(&mut tape, group, &mut anchor_ts, limits)?;
        }

        // (5) Non-empty + strictly-increasing `ts`, via the shared tape core.
        //     An empty tape (zero data rows) fails to construct here.
        let meta = TapeMeta::from_tape(sha256.clone(), &tape)?;

        Ok(Self {
            tape,
            cursor: 0,
            meta,
            path: path_str,
            sha256,
        })
    }
}

impl DataFeed for ParquetFeed {
    fn next(&mut self) -> Result<Option<ChainSnapshot>, BacktestError> {
        match self.tape.get(self.cursor) {
            Some(snapshot) => {
                // `get` matched, so `cursor < tape.len() <= isize::MAX` — the
                // increment cannot overflow; plain `+= 1` keeps the codebase's
                // no-saturating/wrapping convention.
                self.cursor += 1;
                Ok(Some(snapshot.clone()))
            }
            None => Ok(None),
        }
    }

    fn meta(&self) -> DataSourceSpec {
        DataSourceSpec::Parquet {
            path: self.path.clone(),
            sha256: self.sha256.clone(),
        }
    }

    fn tape_meta(&self) -> &TapeMeta {
        &self.meta
    }
}

// ---------------------------------------------------------------------------
// Snapshot grouping
// ---------------------------------------------------------------------------

/// Accumulates the rows of one `step` into a snapshot's raw quotes, verifying
/// the snapshot-level fields are constant across the group.
struct GroupBuilder {
    step: StepIndex,
    step_raw: i32,
    ts: SimTime,
    underlying: Underlying,
    underlying_price: PriceCents,
    tick_size_cents: PriceCents,
    contract_multiplier: u32,
    quotes: Vec<RawQuote>,
}

impl GroupBuilder {
    /// Begin a group, capturing the snapshot-level fields from the first row.
    fn start(columns: &Columns, row: usize, step_raw: i32) -> Result<Self, BacktestError> {
        Ok(Self {
            step: StepIndex::new(count_u32(step_raw, "step")?),
            step_raw,
            ts: SimTime::new(req_i64(columns.ts, row, "ts")?),
            underlying: Underlying::new(req_str(columns.underlying, row, "underlying")?)?,
            underlying_price: price_cents(
                req_i64(columns.underlying_price, row, "underlying_price")?,
                "underlying_price",
            )?,
            tick_size_cents: price_cents(
                req_i64(columns.tick_size, row, "tick_size")?,
                "tick_size",
            )?,
            contract_multiplier: count_u32(
                req_i32(columns.contract_multiplier, row, "contract_multiplier")?,
                "contract_multiplier",
            )?,
            quotes: Vec::new(),
        })
    }

    /// Add one row's quote to the group, first checking the snapshot-level
    /// fields still agree and enforcing `max_contracts_per_snapshot`.
    fn push_row(
        &mut self,
        columns: &Columns,
        row: usize,
        limits: &ResourceLimits,
    ) -> Result<(), BacktestError> {
        // Snapshot-level fields must be constant within a step group.
        ensure_const(
            "ts",
            self.step_raw,
            req_i64(columns.ts, row, "ts")?,
            self.ts.value(),
        )?;
        ensure_const(
            "underlying",
            self.step_raw,
            req_str(columns.underlying, row, "underlying")?,
            self.underlying.as_str(),
        )?;
        ensure_const(
            "underlying_price",
            self.step_raw,
            price_cents(
                req_i64(columns.underlying_price, row, "underlying_price")?,
                "underlying_price",
            )?
            .value(),
            self.underlying_price.value(),
        )?;
        ensure_const(
            "tick_size",
            self.step_raw,
            price_cents(req_i64(columns.tick_size, row, "tick_size")?, "tick_size")?.value(),
            self.tick_size_cents.value(),
        )?;
        ensure_const(
            "contract_multiplier",
            self.step_raw,
            count_u32(
                req_i32(columns.contract_multiplier, row, "contract_multiplier")?,
                "contract_multiplier",
            )?,
            self.contract_multiplier,
        )?;

        // Contracts-per-snapshot ceiling, BEFORE the quote is built or pushed.
        let next_count = u64::try_from(self.quotes.len())
            .map_err(|_| BacktestError::ArithmeticOverflow)?
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let cap = u64::from(limits.max_contracts_per_snapshot);
        if next_count > cap {
            return Err(BacktestError::TapeTooLarge {
                limit: "max_contracts_per_snapshot",
                value: next_count,
                cap,
            });
        }

        // Disk expiries are absolute i64 ns → a DateTime pass-through in the
        // conversion core (Days(n) anchoring is a no-op here, but the anchor is
        // still threaded correctly as ts_0).
        let expiration_ns = req_i64(columns.expiration, row, "expiration")?;
        let quote = RawQuote {
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(expiration_ns)),
            strike: price_cents(req_i64(columns.strike, row, "strike")?, "strike")?,
            style: parse_style(req_str(columns.style, row, "style")?)?,
            bid: price_cents(req_i64(columns.bid, row, "bid")?, "bid")?,
            ask: price_cents(req_i64(columns.ask, row, "ask")?, "ask")?,
            bid_size: Quantity::new(count_u32(
                req_i32(columns.bid_size, row, "bid_size")?,
                "bid_size",
            )?)?,
            ask_size: Quantity::new(count_u32(
                req_i32(columns.ask_size, row, "ask_size")?,
                "ask_size",
            )?)?,
            // IV is required; the Greeks are optional and default to 0 when
            // absent (a null cell), the documented v0.1 placeholder.
            implied_volatility: req_f64(columns.implied_volatility, row, "implied_volatility")?,
            delta: greek_f64(columns.delta, row),
            gamma: greek_f64(columns.gamma, row),
            theta: greek_f64(columns.theta, row),
            vega: greek_f64(columns.vega, row),
        };
        self.quotes.push(quote);
        Ok(())
    }

    /// Finalise the group into a validated [`ChainSnapshot`] via the single
    /// conversion core, anchoring relative expiries on `anchor_ts` (`ts_0`).
    fn finish(self, anchor_ts: SimTime) -> Result<ChainSnapshot, BacktestError> {
        let meta = SnapshotMeta {
            ts: self.ts,
            step: self.step,
            anchor_ts,
            underlying: self.underlying,
            underlying_price: self.underlying_price,
            tick_size_cents: self.tick_size_cents,
            contract_multiplier: self.contract_multiplier,
        };
        raw_quotes_to_snapshot(&meta, &self.quotes)
    }
}

/// Finalise a group, thread the tape anchor `ts_0`, enforce `max_steps`, and
/// push the resulting snapshot.
fn push_snapshot(
    tape: &mut Vec<ChainSnapshot>,
    group: GroupBuilder,
    anchor_ts: &mut Option<SimTime>,
    limits: &ResourceLimits,
) -> Result<(), BacktestError> {
    // The first flushed group (first in replay order) sets ts_0 for the tape.
    let anchor = match *anchor_ts {
        Some(existing) => existing,
        None => {
            *anchor_ts = Some(group.ts);
            group.ts
        }
    };
    let snapshot = group.finish(anchor)?;

    let next_len = u64::try_from(tape.len())
        .map_err(|_| BacktestError::ArithmeticOverflow)?
        .checked_add(1)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    if next_len > limits.max_steps {
        return Err(BacktestError::TapeTooLarge {
            limit: "max_steps",
            value: next_len,
            cap: limits.max_steps,
        });
    }
    tape.push(snapshot);
    Ok(())
}

// ---------------------------------------------------------------------------
// Typed column access over one Arrow batch
// ---------------------------------------------------------------------------

/// The downcast, typed column arrays of one record batch (validated against the
/// canonical schema up front, so every downcast is total).
struct Columns<'b> {
    step: &'b Int32Array,
    ts: &'b Int64Array,
    underlying: &'b StringArray,
    underlying_price: &'b Int64Array,
    tick_size: &'b Int64Array,
    contract_multiplier: &'b Int32Array,
    expiration: &'b Int64Array,
    strike: &'b Int64Array,
    style: &'b StringArray,
    bid: &'b Int64Array,
    ask: &'b Int64Array,
    bid_size: &'b Int32Array,
    ask_size: &'b Int32Array,
    implied_volatility: &'b Float64Array,
    delta: &'b Float64Array,
    gamma: &'b Float64Array,
    theta: &'b Float64Array,
    vega: &'b Float64Array,
    len: usize,
}

impl<'b> Columns<'b> {
    fn new(batch: &'b RecordBatch) -> Result<Self, BacktestError> {
        Ok(Self {
            step: downcast::<Int32Array>(batch, "step")?,
            ts: downcast::<Int64Array>(batch, "ts")?,
            underlying: downcast::<StringArray>(batch, "underlying")?,
            underlying_price: downcast::<Int64Array>(batch, "underlying_price")?,
            tick_size: downcast::<Int64Array>(batch, "tick_size")?,
            contract_multiplier: downcast::<Int32Array>(batch, "contract_multiplier")?,
            expiration: downcast::<Int64Array>(batch, "expiration")?,
            strike: downcast::<Int64Array>(batch, "strike")?,
            style: downcast::<StringArray>(batch, "style")?,
            bid: downcast::<Int64Array>(batch, "bid")?,
            ask: downcast::<Int64Array>(batch, "ask")?,
            bid_size: downcast::<Int32Array>(batch, "bid_size")?,
            ask_size: downcast::<Int32Array>(batch, "ask_size")?,
            implied_volatility: downcast::<Float64Array>(batch, "implied_volatility")?,
            delta: downcast::<Float64Array>(batch, "delta")?,
            gamma: downcast::<Float64Array>(batch, "gamma")?,
            theta: downcast::<Float64Array>(batch, "theta")?,
            vega: downcast::<Float64Array>(batch, "vega")?,
            len: batch.num_rows(),
        })
    }
}

/// Downcast a batch column to a concrete Arrow array by name — total after
/// [`validate_schema`], but still fallible-typed rather than panicking.
fn downcast<'b, A: Array + 'static>(
    batch: &'b RecordBatch,
    name: &str,
) -> Result<&'b A, BacktestError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| BacktestError::Conversion(format!("parquet batch missing column {name}")))?;
    column.as_any().downcast_ref::<A>().ok_or_else(|| {
        BacktestError::Conversion(format!("column {name} has an unexpected physical type"))
    })
}

// ---------------------------------------------------------------------------
// Schema validation
// ---------------------------------------------------------------------------

/// The canonical column set and Arrow dtypes the feed accepts, in schema order.
fn expected_schema() -> [(&'static str, DataType); 18] {
    [
        ("step", DataType::Int32),
        ("ts", DataType::Int64),
        ("underlying", DataType::Utf8),
        ("underlying_price", DataType::Int64),
        ("tick_size", DataType::Int64),
        ("contract_multiplier", DataType::Int32),
        ("expiration", DataType::Int64),
        ("strike", DataType::Int64),
        ("style", DataType::Utf8),
        ("bid", DataType::Int64),
        ("ask", DataType::Int64),
        ("bid_size", DataType::Int32),
        ("ask_size", DataType::Int32),
        ("implied_volatility", DataType::Float64),
        ("delta", DataType::Float64),
        ("gamma", DataType::Float64),
        ("theta", DataType::Float64),
        ("vega", DataType::Float64),
    ]
}

/// A money column, for the "money must be integer cents" rejection message.
fn is_money_column(name: &str) -> bool {
    matches!(
        name,
        "underlying_price" | "tick_size" | "strike" | "bid" | "ask"
    )
}

/// Whether a dtype is any float — used to flag a float money column precisely.
fn is_float(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// Validate the file's schema against the canonical column set and dtypes.
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] for a wrong column count, a missing
/// column, or a dtype mismatch (a float money column carries an explicit hint).
fn validate_schema(schema: &Schema) -> Result<(), BacktestError> {
    let expected = expected_schema();
    if schema.fields().len() != expected.len() {
        let names: Vec<&str> = expected.iter().map(|(name, _)| *name).collect();
        return Err(BacktestError::Conversion(format!(
            "parquet schema has {} columns, expected exactly {}: {}",
            schema.fields().len(),
            expected.len(),
            names.join(", ")
        )));
    }
    for (name, want) in expected {
        let field = schema.field_with_name(name).map_err(|_| {
            BacktestError::Conversion(format!("parquet schema is missing required column {name}"))
        })?;
        let actual = field.data_type();
        if actual != &want {
            let hint = if is_money_column(name) && is_float(actual) {
                " (money columns must be integer cents, not float)"
            } else {
                ""
            };
            return Err(BacktestError::Conversion(format!(
                "column {name} has arrow type {actual:?}, expected {want:?}{hint}"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cell readers and small conversions (no panic, no unchecked indexing)
// ---------------------------------------------------------------------------

/// Read a required non-null `i64` cell (`row < array.len()` by construction).
fn req_i64(array: &Int64Array, row: usize, column: &str) -> Result<i64, BacktestError> {
    if array.is_null(row) {
        return Err(BacktestError::Conversion(format!(
            "null value in column {column} at row {row}"
        )));
    }
    Ok(array.value(row))
}

/// Read a required non-null `i32` cell.
fn req_i32(array: &Int32Array, row: usize, column: &str) -> Result<i32, BacktestError> {
    if array.is_null(row) {
        return Err(BacktestError::Conversion(format!(
            "null value in column {column} at row {row}"
        )));
    }
    Ok(array.value(row))
}

/// Read a required non-null string cell.
fn req_str<'b>(array: &'b StringArray, row: usize, column: &str) -> Result<&'b str, BacktestError> {
    if array.is_null(row) {
        return Err(BacktestError::Conversion(format!(
            "null value in column {column} at row {row}"
        )));
    }
    Ok(array.value(row))
}

/// Read a required non-null `f64` cell (the analytic that must be present: IV).
fn req_f64(array: &Float64Array, row: usize, column: &str) -> Result<f64, BacktestError> {
    if array.is_null(row) {
        return Err(BacktestError::Conversion(format!(
            "null value in column {column} at row {row}"
        )));
    }
    Ok(array.value(row))
}

/// Read an optional Greek `f64` cell; an absent (null) Greek is the documented
/// `0` placeholder (the NaN / infinite check happens in the conversion core).
fn greek_f64(array: &Float64Array, row: usize) -> f64 {
    if array.is_null(row) {
        0.0
    } else {
        array.value(row)
    }
}

/// Construct a non-negative integer-cents [`PriceCents`] from a disk `i64`
/// (money is already integer on disk — no rounding, just a sign check).
fn price_cents(value: i64, column: &str) -> Result<PriceCents, BacktestError> {
    let cents = u64::try_from(value).map_err(|_| {
        BacktestError::Conversion(format!(
            "negative {column} {value}; money is a non-negative integer cent value"
        ))
    })?;
    Ok(PriceCents::new(cents))
}

/// Convert a disk `i32` count to a non-negative `u32`.
fn count_u32(value: i32, column: &str) -> Result<u32, BacktestError> {
    u32::try_from(value).map_err(|_| {
        BacktestError::Conversion(format!("negative {column} {value}; a count must be >= 0"))
    })
}

/// Parse the `style` column into an [`OptionStyle`] (case-insensitive on the
/// canonical `call` / `put`).
fn parse_style(style: &str) -> Result<OptionStyle, BacktestError> {
    if style.eq_ignore_ascii_case("call") {
        Ok(OptionStyle::Call)
    } else if style.eq_ignore_ascii_case("put") {
        Ok(OptionStyle::Put)
    } else {
        Err(BacktestError::Conversion(format!(
            "unknown option style {style:?}, expected \"call\" or \"put\""
        )))
    }
}

/// Reject a snapshot-level field that varies within a step group.
fn ensure_const<T: PartialEq + std::fmt::Display>(
    field: &str,
    step: i32,
    got: T,
    want: T,
) -> Result<(), BacktestError> {
    if got == want {
        Ok(())
    } else {
        Err(BacktestError::Conversion(format!(
            "inconsistent {field} within step group {step}: {got} vs {want}"
        )))
    }
}

/// Compute the streaming `sha256` (lowercase hex) of a file's bytes.
///
/// # Errors
///
/// Returns [`BacktestError::DataIo`] if the file cannot be opened or read.
fn file_sha256(path: &Path) -> Result<String, BacktestError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        // `read <= buffer.len()`, so this slice is always in bounds.
        match buffer.get(..read) {
            Some(chunk) => hasher.update(chunk),
            None => {
                return Err(BacktestError::Conversion(
                    "internal hash error: read count exceeds buffer".to_string(),
                ));
            }
        }
    }
    Ok(to_hex(&hasher.finalize()))
}

/// Encode bytes as lowercase hex without an extra dependency or indexing.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        // Writing to a `String` is infallible; the Result is discarded on the
        // (unreachable) error arm without an `.unwrap()`.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Map an upstream `arrow` / `parquet` decode error into a typed
/// [`BacktestError::Data`], so no foreign error type leaks through a public
/// signature. These are undecodable-bytes failures (truncated footer, corrupt
/// row group) — distinct from a semantic [`BacktestError::Conversion`] on data
/// that decoded cleanly.
fn conv<E: std::fmt::Display>(context: &str, error: &E) -> BacktestError {
    BacktestError::Data(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use super::ParquetFeed;
    use crate::config::ResourceLimits;
    use crate::data::feed::DataFeed;
    use crate::error::BacktestError;

    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;

    /// A column-oriented, IronCondor-shaped chain fixture built row by row.
    #[derive(Default)]
    struct Chain {
        step: Vec<i32>,
        ts: Vec<i64>,
        underlying: Vec<String>,
        underlying_price: Vec<i64>,
        tick_size: Vec<i64>,
        contract_multiplier: Vec<i32>,
        expiration: Vec<i64>,
        strike: Vec<i64>,
        style: Vec<String>,
        bid: Vec<i64>,
        ask: Vec<i64>,
        bid_size: Vec<i32>,
        ask_size: Vec<i32>,
        iv: Vec<f64>,
        delta: Vec<f64>,
        gamma: Vec<f64>,
        theta: Vec<f64>,
        vega: Vec<f64>,
    }

    impl Chain {
        /// Append one quote row (constant SPX / 5c tick / 100x, absolute expiry).
        fn row(&mut self, step: i32, ts: i64, strike: i64, style: &str, bid: i64, ask: i64) {
            self.step.push(step);
            self.ts.push(ts);
            self.underlying.push("SPX".to_string());
            self.underlying_price.push(510_000);
            self.tick_size.push(5);
            self.contract_multiplier.push(100);
            self.expiration.push(EXPIRY);
            self.strike.push(strike);
            self.style.push(style.to_string());
            self.bid.push(bid);
            self.ask.push(ask);
            self.bid_size.push(10);
            self.ask_size.push(10);
            self.iv.push(0.2);
            self.delta.push(0.5);
            self.gamma.push(0.01);
            self.theta.push(-0.05);
            self.vega.push(0.1);
        }

        fn strings(values: &[String]) -> StringArray {
            StringArray::from(values.iter().map(String::as_str).collect::<Vec<&str>>())
        }

        /// The canonical Arrow columns, in schema order.
        fn columns(&self) -> Vec<ArrayRef> {
            vec![
                Arc::new(Int32Array::from(self.step.clone())) as ArrayRef,
                Arc::new(Int64Array::from(self.ts.clone())),
                Arc::new(Self::strings(&self.underlying)),
                Arc::new(Int64Array::from(self.underlying_price.clone())),
                Arc::new(Int64Array::from(self.tick_size.clone())),
                Arc::new(Int32Array::from(self.contract_multiplier.clone())),
                Arc::new(Int64Array::from(self.expiration.clone())),
                Arc::new(Int64Array::from(self.strike.clone())),
                Arc::new(Self::strings(&self.style)),
                Arc::new(Int64Array::from(self.bid.clone())),
                Arc::new(Int64Array::from(self.ask.clone())),
                Arc::new(Int32Array::from(self.bid_size.clone())),
                Arc::new(Int32Array::from(self.ask_size.clone())),
                Arc::new(Float64Array::from(self.iv.clone())),
                Arc::new(Float64Array::from(self.delta.clone())),
                Arc::new(Float64Array::from(self.gamma.clone())),
                Arc::new(Float64Array::from(self.theta.clone())),
                Arc::new(Float64Array::from(self.vega.clone())),
            ]
        }
    }

    /// The canonical schema fields, in order (Greeks nullable so a null → 0).
    fn standard_fields() -> Vec<Field> {
        vec![
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
            Field::new("delta", DataType::Float64, true),
            Field::new("gamma", DataType::Float64, true),
            Field::new("theta", DataType::Float64, true),
            Field::new("vega", DataType::Float64, true),
        ]
    }

    /// Write a batch (schema + columns) to `path` as Parquet (Snappy default).
    fn write_parquet(
        path: &Path,
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<(), BacktestError> {
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| BacktestError::Conversion(format!("test batch build: {e}")))?;
        let file = std::fs::File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| BacktestError::Conversion(format!("test writer: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| BacktestError::Conversion(format!("test write: {e}")))?;
        writer
            .close()
            .map_err(|e| BacktestError::Conversion(format!("test close: {e}")))?;
        Ok(())
    }

    /// Write the standard-schema fixture and return its path (kept alive by the
    /// returned `TempDir`).
    fn write_standard(chain: &Chain) -> Result<(TempDir, PathBuf), BacktestError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("chain.parquet");
        let schema = Arc::new(Schema::new(standard_fields()));
        write_parquet(&path, schema, chain.columns())?;
        Ok((dir, path))
    }

    /// A two-strike, call+put single-step condor slice at `step` / `ts`.
    fn condor_step(chain: &mut Chain, step: i32, ts: i64) {
        chain.row(step, ts, 500_000, "call", 200, 210);
        chain.row(step, ts, 500_000, "put", 180, 190);
        chain.row(step, ts, 520_000, "call", 90, 100);
        chain.row(step, ts, 520_000, "put", 140, 150);
    }

    #[test]
    fn test_open_reads_ordered_tape_and_yields_to_exhaustion() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        condor_step(&mut chain, 1, TS0 + NANOS_PER_DAY);
        condor_step(&mut chain, 2, TS0 + 2 * NANOS_PER_DAY);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("the canonical fixture must write");
        };

        let Ok(mut feed) = ParquetFeed::open(&path, &ResourceLimits::default()) else {
            panic!("the canonical fixture must open");
        };
        let meta = feed.tape_meta();
        assert!(meta.non_empty);
        assert_eq!(meta.first_ts.value(), TS0);
        assert_eq!(meta.final_step.value(), 2);
        assert_eq!(meta.data_identity.len(), 64, "sha256 hex is 64 chars");

        for (expected_step, expected_ts) in [
            (0u32, TS0),
            (1, TS0 + NANOS_PER_DAY),
            (2, TS0 + 2 * NANOS_PER_DAY),
        ] {
            match feed.next() {
                Ok(Some(snap)) => {
                    assert_eq!(snap.step.value(), expected_step);
                    assert_eq!(snap.ts.value(), expected_ts);
                    // Four rows → four quotes (two strikes × call/put).
                    assert_eq!(snap.quotes.len(), 4);
                }
                other => panic!("expected snapshot at step {expected_step}, got {other:?}"),
            }
        }
        assert!(matches!(feed.next(), Ok(None)));
        assert!(matches!(feed.next(), Ok(None)));
    }

    #[test]
    fn test_open_rejects_float_money_column_conversion() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);

        // Same data, but `bid` is a DOUBLE column — a float money column.
        let mut fields = standard_fields();
        fields[9] = Field::new("bid", DataType::Float64, false);
        let schema = Arc::new(Schema::new(fields));
        let mut columns = chain.columns();
        let bids: Vec<f64> = chain.bid.iter().map(|&b| b as f64).collect();
        columns[9] = Arc::new(Float64Array::from(bids)) as ArrayRef;

        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        let path = dir.path().join("floatmoney.parquet");
        let Ok(()) = write_parquet(&path, schema, columns) else {
            panic!("the float-money fixture must write");
        };
        assert!(matches!(
            ParquetFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_open_rejects_missing_column_conversion() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        // Drop the `vega` column entirely (17 columns, not 18).
        let mut fields = standard_fields();
        let _ = fields.pop();
        let schema = Arc::new(Schema::new(fields));
        let mut columns = chain.columns();
        let _ = columns.pop();

        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        let path = dir.path().join("missingcol.parquet");
        let Ok(()) = write_parquet(&path, schema, columns) else {
            panic!("the missing-column fixture must write");
        };
        assert!(matches!(
            ParquetFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_open_rejects_out_of_order_ts_data_out_of_order() {
        // step ascending (0, 1) but ts descending → strictly-increasing ts fails.
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0 + NANOS_PER_DAY);
        condor_step(&mut chain, 1, TS0);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("the out-of-order fixture must write");
        };
        assert!(matches!(
            ParquetFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::DataOutOfOrder {
                step: 1,
                ts,
                prev
            }) if ts == TS0 && prev == TS0 + NANOS_PER_DAY
        ));
    }

    #[test]
    fn test_open_rejects_empty_tape_conversion() {
        // A valid-schema file with zero data rows → an empty tape.
        let chain = Chain::default();
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("the empty fixture must write");
        };
        assert!(matches!(
            ParquetFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_open_enforces_max_file_bytes_tape_too_large() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_file_bytes: 1,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            ParquetFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_file_bytes",
                ..
            })
        ));
    }

    #[test]
    fn test_open_enforces_max_decompressed_bytes_tape_too_large() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_decompressed_bytes: 1,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            ParquetFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_decompressed_bytes",
                ..
            })
        ));
    }

    #[test]
    fn test_open_enforces_max_steps_tape_too_large() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        condor_step(&mut chain, 1, TS0 + NANOS_PER_DAY);
        condor_step(&mut chain, 2, TS0 + 2 * NANOS_PER_DAY);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_steps: 2,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            ParquetFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_steps",
                value: 3,
                cap: 2
            })
        ));
    }

    #[test]
    fn test_open_enforces_max_contracts_per_snapshot_tape_too_large() {
        // One step with four contracts, ceiling of two.
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_contracts_per_snapshot: 2,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            ParquetFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_contracts_per_snapshot",
                value: 3,
                cap: 2
            })
        ));
    }

    #[test]
    fn test_open_rejects_non_regular_file_conversion() {
        // A directory is not a regular file — rejected before hashing, so a
        // FIFO / pipe cannot slip past the size guard and hang the hash.
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        assert!(matches!(
            ParquetFeed::open(dir.path(), &ResourceLimits::default()),
            Err(BacktestError::Conversion(_)) | Err(BacktestError::DataIo(_))
        ));
    }

    #[test]
    fn test_open_rejects_corrupt_parquet_data_not_panic() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        let path = dir.path().join("corrupt.parquet");
        // Not a Parquet file at all (bad magic) — a typed error, never a panic.
        if std::fs::write(&path, b"this is definitely not parquet data").is_err() {
            panic!("writing the corrupt fixture must succeed");
        }
        assert!(matches!(
            ParquetFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Data(_))
        ));
    }

    #[test]
    fn test_open_rejects_truncated_footer_data() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        let Ok(bytes) = std::fs::read(&path) else {
            panic!("the fixture must be readable");
        };
        // Chop the Parquet footer off — the metadata read must fail cleanly.
        let truncated = &bytes[..bytes.len() / 2];
        let Ok(dir2) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        let path2 = dir2.path().join("truncated.parquet");
        if std::fs::write(&path2, truncated).is_err() {
            panic!("writing the truncated fixture must succeed");
        }
        assert!(matches!(
            ParquetFeed::open(&path2, &ResourceLimits::default()),
            Err(BacktestError::Data(_))
        ));
    }

    #[test]
    fn test_open_rejects_non_tick_aligned_price_price_not_tick_aligned() {
        // ask 107 is not a multiple of the 5-cent tick → the conversion core
        // raises PriceNotTickAligned through the feed path.
        let mut chain = Chain::default();
        chain.row(0, TS0, 500_000, "call", 100, 107);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            ParquetFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::PriceNotTickAligned {
                price: 107,
                tick: 5
            })
        ));
    }

    #[test]
    fn test_open_sha256_stable_across_two_opens() {
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        let (Ok(a), Ok(b)) = (
            ParquetFeed::open(&path, &ResourceLimits::default()),
            ParquetFeed::open(&path, &ResourceLimits::default()),
        ) else {
            panic!("both opens of the same bytes must succeed");
        };
        assert_eq!(a.tape_meta().data_identity, b.tape_meta().data_identity);
        // meta() reports the same identity in the manifest provenance.
        assert!(matches!(
            (a.meta(), b.meta()),
            (
                crate::data::DataSourceSpec::Parquet { sha256: sa, .. },
                crate::data::DataSourceSpec::Parquet { sha256: sb, .. }
            ) if sa == sb && sa == a.tape_meta().data_identity
        ));
    }

    #[test]
    fn test_open_treats_null_greek_as_zero_placeholder() {
        // A null theta becomes the documented `0` placeholder, not an error.
        let mut chain = Chain::default();
        chain.row(0, TS0, 500_000, "call", 100, 110);
        let columns_with_null_theta = {
            let mut columns = chain.columns();
            columns[16] = Arc::new(Float64Array::from(vec![None::<f64>])) as ArrayRef;
            columns
        };
        let schema = Arc::new(Schema::new(standard_fields()));
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        let path = dir.path().join("nulltheta.parquet");
        let Ok(()) = write_parquet(&path, schema, columns_with_null_theta) else {
            panic!("the null-theta fixture must write");
        };
        let Ok(mut feed) = ParquetFeed::open(&path, &ResourceLimits::default()) else {
            panic!("null Greeks must be accepted as the 0 placeholder");
        };
        match feed.next() {
            Ok(Some(snap)) => match snap.quotes.values().next() {
                Some(view) => assert!(view.theta.is_zero()),
                None => panic!("one quote must be present"),
            },
            other => panic!("expected one snapshot, got {other:?}"),
        }
    }
}
