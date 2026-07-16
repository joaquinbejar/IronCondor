//! The historical **file feeds** — [`ParquetFeed`] (the v0.1 release feed) and
//! [`CsvFeed`] (the v0.2 breadth feed).
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
//! `max_contracts_per_snapshot` / `max_total_bytes` **during**
//! materialisation. There is no `.unwrap()` / `.expect()` / unchecked `[]` on
//! the ingestion path.
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
//!
//! # The CSV feed (v0.2 breadth)
//!
//! [`CsvFeed`] reads a **directory of per-step chain files**, one full chain
//! snapshot per file, replayed in **name-sorted order** — the byte-wise
//! [`OsStr`](std::ffi::OsStr) order of the file names **is** the replay order,
//! the single ordering source ([docs/03 §9](../../../docs/03-data-layer.md#9-determinism-in-the-feed)).
//! Each file carries the **same integer-cents schema** as the Parquet feed
//! ([docs/03 §4](../../../docs/03-data-layer.md#4-historical-csv-schema)): money
//! columns are integer cents on disk, so a dollar-denominated float in a money
//! column is rejected with [`BacktestError::Conversion`], never silently
//! truncated. Every file funnels through the **one** conversion boundary
//! ([`raw_quotes_to_snapshot`]) — there is no second validation path.
//!
//! ## Why a direct integer-cents parse, not `OptionChain::load_from_csv`
//!
//! [docs/03 §4](../../../docs/03-data-layer.md#4-historical-csv-schema) suggests
//! deferring to `optionstratlib::chains::OptionChain::load_from_csv` "where it
//! fits". It does **not** fit: that loader parses a positional, **dollar /
//! `Decimal`-denominated** chain (`strike, call_bid, call_ask, put_bid, put_ask,
//! iv, delta_call, delta_put, gamma, volume, open_interest`) into a single
//! one-expiry `OptionChain`, carrying none of the per-step fields this schema
//! needs — no `ts`, `tick_size`, `contract_multiplier`, `bid_size` / `ask_size`,
//! per-row `expiration`, `style`, `theta` / `vega` — and its money is float
//! dollars, exactly the shape [ADR-0003](../../../docs/adr/0003-money-as-integer-cents.md)
//! forbids at this boundary. Deferring would mean an f64-dollar → cents
//! round-trip *outside* `convert.rs`: a second conversion path. So the CSV feed
//! parses the integer-cents schema **directly** (via the `csv` crate) into
//! [`RawQuote`] / [`SnapshotMeta`] and goes through the single
//! [`raw_quotes_to_snapshot`] boundary — `f64` dies in exactly one place.
//!
//! ## Directory data identity (the re-read verifier)
//!
//! The tape's `sha256` is a **deterministic directory hash**: one [`Sha256`] is
//! folded, in name-sorted order, over each file's name, a NUL separator, and
//! that file's own streaming content `sha256` (hex). Any change to a file's
//! bytes, name, count, or ordering changes the digest, so
//! [`DataSourceSpec::Csv`]'s `sha256` verifies the re-read bytes; a mismatch
//! fails the re-run with a typed error ([`CsvFeed::open_verified`]), never a
//! silent divergent run.
//!
//! ## Ceilings
//!
//! The same [`ResourceLimits`] contract the Parquet feed enforces:
//! `max_file_bytes` per file (filesystem metadata, before any read),
//! `max_decompressed_bytes` as the cumulative raw input across the directory
//! (CSV is uncompressed, so raw == decompressed), and `max_steps` /
//! `max_contracts_per_snapshot` / `max_total_bytes` during materialisation —
//! the first crossed ceiling aborts with [`BacktestError::TapeTooLarge`] before
//! any unbounded read or allocation.

use std::ffi::OsString;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

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
use crate::domain::{
    ChainSnapshot, ContractKey, PriceCents, Quantity, QuoteView, SimTime, StepIndex, Underlying,
};
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
    /// materialisation with `max_steps` / `max_contracts_per_snapshot` /
    /// `max_total_bytes` and the incremental decompression guard. The tape is
    /// finally checked non-empty
    /// and strictly `ts`-increasing via [`TapeMeta::from_tape`].
    ///
    /// # Errors
    ///
    /// - [`BacktestError::DataIo`] — the file cannot be stat-ed, opened, or read.
    /// - [`BacktestError::TapeTooLarge`] — a `max_file_bytes` /
    ///   `max_decompressed_bytes` / `max_steps` / `max_contracts_per_snapshot`
    ///   / `max_total_bytes` ceiling is crossed (`limit` names the field).
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
        // Running estimate of the materialised tape's in-memory byte size, for
        // the `max_total_bytes` ceiling (accumulated in `push_snapshot`).
        let mut total_bytes: u64 = 0;

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
                        push_snapshot(&mut tape, group, &mut anchor_ts, &mut total_bytes, limits)?;
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
            push_snapshot(&mut tape, group, &mut anchor_ts, &mut total_bytes, limits)?;
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
// The historical CSV feed (v0.2 breadth)
// ---------------------------------------------------------------------------

/// The historical CSV [`DataFeed`] — a materialised, validated, immutable tape
/// over a **directory of per-step chain files** replayed in name-sorted order.
///
/// Construct with [`CsvFeed::open`] (or [`CsvFeed::open_verified`] to also check
/// the directory hash against a recorded identity); all I/O happens there.
/// [`DataFeed::next`] is a pure in-memory read that never blocks or `.await`s.
#[derive(Debug)]
#[must_use = "a CsvFeed does nothing unless its snapshots are consumed via DataFeed::next"]
pub struct CsvFeed {
    /// The validated, strictly `ts`-ordered snapshots (the replay tape).
    tape: Vec<ChainSnapshot>,
    /// The next index [`DataFeed::next`] yields.
    cursor: usize,
    /// The pinned tape metadata (identity, non-empty, first ts, final step).
    meta: TapeMeta,
    /// The source directory path, recorded verbatim in the manifest provenance.
    path: String,
    /// The directory `sha256` (hex) — the tape's data identity.
    sha256: String,
}

impl CsvFeed {
    /// Open a directory of per-step CSV chain files, materialising and
    /// validating the whole tape.
    ///
    /// The directory's regular files are collected and **sorted by file name**
    /// in byte-wise [`OsStr`](std::ffi::OsStr) order — that sort **is** the
    /// replay order ([docs/03 §9](../../../docs/03-data-layer.md#9-determinism-in-the-feed)).
    /// A non-file entry (a sub-directory or a symlink, which could escape the
    /// tree) is rejected. Each file is parsed as one full chain snapshot through
    /// the single conversion boundary ([`raw_quotes_to_snapshot`]).
    ///
    /// Enforces: `max_steps` (directory file count, while collecting, and again
    /// during materialisation), `max_file_bytes` (filesystem metadata, before
    /// any read), `max_decompressed_bytes` (cumulative raw input; CSV is
    /// uncompressed), `max_contracts_per_snapshot` (during row accumulation),
    /// and `max_total_bytes` (during materialisation). The tape is finally
    /// checked non-empty and strictly `ts`-increasing via [`TapeMeta::from_tape`].
    ///
    /// The tape's `data_identity` (and the `sha256` in [`DataFeed::meta`]) is a
    /// deterministic directory hash: one [`Sha256`] folded, in name-sorted
    /// order, over each file's name, a NUL separator, and that file's own
    /// content `sha256` (hex).
    ///
    /// # Errors
    ///
    /// - [`BacktestError::DataIo`] — the directory or a file cannot be stat-ed,
    ///   opened, or read.
    /// - [`BacktestError::Conversion`] — the path is not a directory, a non-file
    ///   entry is present, a required column or value is missing, a money column
    ///   carries a dollar float, a zero tick / multiplier, a non-positive
    ///   strike, a NaN / infinite analytic, or an empty tape.
    /// - [`BacktestError::Data`] — a file is not decodable as CSV (bad UTF-8 or
    ///   an inconsistent record shape).
    /// - [`BacktestError::PriceNotTickAligned`] / [`BacktestError::CrossedQuote`]
    ///   / [`BacktestError::InvalidQuantity`] / [`BacktestError::ArithmeticOverflow`]
    ///   — raised by the conversion core on a bad quote.
    /// - [`BacktestError::TapeTooLarge`] — a `max_file_bytes` /
    ///   `max_decompressed_bytes` / `max_steps` / `max_contracts_per_snapshot`
    ///   / `max_total_bytes` ceiling is crossed (`limit` names the field).
    /// - [`BacktestError::DataOutOfOrder`] — a duplicate or reversed `ts` across
    ///   files.
    pub fn open(path: impl AsRef<Path>, limits: &ResourceLimits) -> Result<Self, BacktestError> {
        let dir_ref = path.as_ref();
        let path_str = dir_ref.to_string_lossy().into_owned();

        // (1) The path must be a directory of per-step files.
        let dir_meta = std::fs::metadata(dir_ref)?;
        if !dir_meta.is_dir() {
            return Err(BacktestError::Conversion(format!(
                "csv feed path is not a directory: {path_str}"
            )));
        }

        // (2) Collect the directory's regular files, rejecting any non-file
        //     entry (a sub-directory or a symlink — the latter could escape the
        //     tree). Bounded by `max_steps` while collecting, so a directory
        //     with more files than the tape ceiling never builds an unbounded
        //     Vec of paths.
        let mut files: Vec<(OsString, PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(dir_ref)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_file() {
                return Err(BacktestError::Conversion(format!(
                    "csv feed directory contains a non-file entry {:?}; every entry must be a \
                     regular per-step chain file",
                    entry.file_name()
                )));
            }
            files.push((entry.file_name(), entry.path()));
            let count =
                u64::try_from(files.len()).map_err(|_| BacktestError::ArithmeticOverflow)?;
            if count > limits.max_steps {
                return Err(BacktestError::TapeTooLarge {
                    limit: "max_steps",
                    value: count,
                    cap: limits.max_steps,
                });
            }
        }

        // (3) Name-sort — the byte-wise OsStr order IS the replay order.
        files.sort_by(|a, b| a.0.cmp(&b.0));

        // (4) Materialise the tape, one snapshot per file, in name-sorted order.
        let mut tape: Vec<ChainSnapshot> = Vec::new();
        let mut anchor_ts: Option<SimTime> = None;
        let mut total_bytes: u64 = 0;
        let mut decoded_bytes: u64 = 0;
        let mut dir_hasher = Sha256::new();

        for (index, (name, file_path)) in files.iter().enumerate() {
            let step = StepIndex::new(
                u32::try_from(index).map_err(|_| BacktestError::ArithmeticOverflow)?,
            );

            // (4a) Per-file size ceiling, from filesystem metadata, before any
            //      read.
            let file_len = std::fs::metadata(file_path)?.len();
            if file_len > limits.max_file_bytes {
                return Err(BacktestError::TapeTooLarge {
                    limit: "max_file_bytes",
                    value: file_len,
                    cap: limits.max_file_bytes,
                });
            }

            // (4b) Cumulative raw-input ceiling (CSV is uncompressed, so the raw
            //      bytes ARE the decompressed bytes).
            decoded_bytes = decoded_bytes
                .checked_add(file_len)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            if decoded_bytes > limits.max_decompressed_bytes {
                return Err(BacktestError::TapeTooLarge {
                    limit: "max_decompressed_bytes",
                    value: decoded_bytes,
                    cap: limits.max_decompressed_bytes,
                });
            }

            // (4c) Fold this file's identity into the directory hash, in
            //      name-sorted order: name, NUL, then the file's own content
            //      sha256 (streaming, bounded by the size ceiling above).
            let file_hex = file_sha256(file_path)?;
            dir_hasher.update(name.as_encoded_bytes());
            dir_hasher.update([0u8]);
            dir_hasher.update(file_hex.as_bytes());

            // (4d) Parse the file into one snapshot's raw quotes + snapshot-level
            //      meta, then convert once through the single boundary.
            let parsed = parse_csv_snapshot(file_path, step, limits)?;
            // The first file (first in replay order) sets ts_0 for the tape.
            let anchor = match anchor_ts {
                Some(existing) => existing,
                None => {
                    anchor_ts = Some(parsed.ts);
                    parsed.ts
                }
            };
            let meta = SnapshotMeta {
                ts: parsed.ts,
                step,
                anchor_ts: anchor,
                underlying: parsed.underlying,
                underlying_price: parsed.underlying_price,
                tick_size_cents: parsed.tick_size_cents,
                contract_multiplier: parsed.contract_multiplier,
            };
            let snapshot = raw_quotes_to_snapshot(&meta, &parsed.quotes)?;
            push_checked(&mut tape, snapshot, &mut total_bytes, limits)?;
        }

        // (5) Pin the directory identity, then check non-empty + strictly
        //     increasing `ts` via the shared tape core (an empty directory is
        //     an empty tape, rejected here).
        let sha256 = to_hex(&dir_hasher.finalize());
        let meta = TapeMeta::from_tape(sha256.clone(), &tape)?;

        Ok(Self {
            tape,
            cursor: 0,
            meta,
            path: path_str,
            sha256,
        })
    }

    /// Open a CSV directory and verify it hashes to `expected_sha256` — the
    /// re-read verifier for a recorded [`DataSourceSpec::Csv`] identity.
    ///
    /// An empty `expected_sha256` (a not-yet-pinned config value) skips the
    /// check and simply pins the computed hash. A non-empty value that does not
    /// match the recomputed directory hash fails with
    /// [`BacktestError::Conversion`] — a re-read divergence is never a silent
    /// divergent run.
    ///
    /// # Errors
    ///
    /// Every error [`Self::open`] can raise, plus [`BacktestError::Conversion`]
    /// when `expected_sha256` is non-empty and does not equal the recomputed
    /// directory hash.
    pub fn open_verified(
        path: impl AsRef<Path>,
        expected_sha256: &str,
        limits: &ResourceLimits,
    ) -> Result<Self, BacktestError> {
        let feed = Self::open(path, limits)?;
        if !expected_sha256.is_empty() && feed.sha256 != expected_sha256 {
            return Err(BacktestError::Conversion(format!(
                "csv directory sha256 mismatch: recorded {expected_sha256}, recomputed {}",
                feed.sha256
            )));
        }
        Ok(feed)
    }
}

impl DataFeed for CsvFeed {
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
        DataSourceSpec::Csv {
            path: self.path.clone(),
            sha256: self.sha256.clone(),
        }
    }

    fn tape_meta(&self) -> &TapeMeta {
        &self.meta
    }
}

// ---------------------------------------------------------------------------
// CSV parsing (one file → one snapshot's raw quotes, bounded and no-panic)
// ---------------------------------------------------------------------------

/// One CSV file's parsed content: the snapshot-level fields (constant across the
/// file's rows) plus the per-contract raw quotes, ready for the single
/// conversion core.
struct ParsedCsvSnapshot {
    ts: SimTime,
    underlying: Underlying,
    underlying_price: PriceCents,
    tick_size_cents: PriceCents,
    contract_multiplier: u32,
    quotes: Vec<RawQuote>,
}

/// The header-derived column indices for the CSV schema
/// ([docs/03 §4](../../../docs/03-data-layer.md#4-historical-csv-schema)). The
/// four Greek columns are optional; every other column is required. Unknown
/// extra columns are tolerated and ignored.
struct CsvColumns {
    ts: usize,
    underlying: usize,
    underlying_price: usize,
    tick_size: usize,
    contract_multiplier: usize,
    expiration: usize,
    strike: usize,
    style: usize,
    bid: usize,
    ask: usize,
    bid_size: usize,
    ask_size: usize,
    implied_volatility: usize,
    delta: Option<usize>,
    gamma: Option<usize>,
    theta: Option<usize>,
    vega: Option<usize>,
}

impl CsvColumns {
    /// Resolve the required and optional columns from the header record,
    /// rejecting a missing required column with [`BacktestError::Conversion`].
    fn from_header(header: &csv::StringRecord) -> Result<Self, BacktestError> {
        let find = |name: &str| header.iter().position(|h| h == name);
        let req = |name: &str| {
            find(name).ok_or_else(|| {
                BacktestError::Conversion(format!("csv header is missing required column {name}"))
            })
        };
        Ok(Self {
            ts: req("ts")?,
            underlying: req("underlying")?,
            underlying_price: req("underlying_price")?,
            tick_size: req("tick_size")?,
            contract_multiplier: req("contract_multiplier")?,
            expiration: req("expiration")?,
            strike: req("strike")?,
            style: req("style")?,
            bid: req("bid")?,
            ask: req("ask")?,
            bid_size: req("bid_size")?,
            ask_size: req("ask_size")?,
            implied_volatility: req("implied_volatility")?,
            delta: find("delta"),
            gamma: find("gamma"),
            theta: find("theta"),
            vega: find("vega"),
        })
    }
}

/// Parse one CSV chain file into a [`ParsedCsvSnapshot`], enforcing a bounded
/// read (`max_file_bytes`) and `max_contracts_per_snapshot`.
///
/// The read is bounded by `max_file_bytes` even under a race that grows the file
/// after its metadata was checked ([`std::io::Read::take`]); a truncation then
/// surfaces as a typed CSV decode error, never an unbounded read.
fn parse_csv_snapshot(
    path: &Path,
    step: StepIndex,
    limits: &ResourceLimits,
) -> Result<ParsedCsvSnapshot, BacktestError> {
    let file = File::open(path)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .trim(csv::Trim::All)
        .from_reader(file.take(limits.max_file_bytes));

    let cols = CsvColumns::from_header(
        reader
            .headers()
            .map_err(|e| csv_decode_err("csv header", &e))?,
    )?;

    let cap = u64::from(limits.max_contracts_per_snapshot);
    let mut record = csv::StringRecord::new();
    // The snapshot-level fields, captured from the first row and required
    // constant across the file's rows.
    let mut header_fields: Option<(SimTime, Underlying, PriceCents, PriceCents, u32)> = None;
    let mut quotes: Vec<RawQuote> = Vec::new();

    while reader
        .read_record(&mut record)
        .map_err(|e| csv_decode_err("csv record", &e))?
    {
        let ts = SimTime::new(parse_i64(req_cell(&record, cols.ts, "ts")?, "ts")?);
        let underlying = Underlying::new(req_cell(&record, cols.underlying, "underlying")?)?;
        let underlying_price = parse_cents(
            req_cell(&record, cols.underlying_price, "underlying_price")?,
            "underlying_price",
        )?;
        let tick = parse_cents(req_cell(&record, cols.tick_size, "tick_size")?, "tick_size")?;
        let multiplier = parse_u32(
            req_cell(&record, cols.contract_multiplier, "contract_multiplier")?,
            "contract_multiplier",
        )?;
        if let Some((ts0, u0, up0, t0, m0)) = &header_fields {
            ensure_const("ts", step.value(), ts.value(), ts0.value())?;
            ensure_const("underlying", step.value(), underlying.as_str(), u0.as_str())?;
            ensure_const(
                "underlying_price",
                step.value(),
                underlying_price.value(),
                up0.value(),
            )?;
            ensure_const("tick_size", step.value(), tick.value(), t0.value())?;
            ensure_const("contract_multiplier", step.value(), multiplier, *m0)?;
        } else {
            header_fields = Some((ts, underlying.clone(), underlying_price, tick, multiplier));
        }

        // Contracts-per-snapshot ceiling, BEFORE the quote is built or pushed.
        let next_count = u64::try_from(quotes.len())
            .map_err(|_| BacktestError::ArithmeticOverflow)?
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if next_count > cap {
            return Err(BacktestError::TapeTooLarge {
                limit: "max_contracts_per_snapshot",
                value: next_count,
                cap,
            });
        }

        // Disk expiries are absolute i64 ns → a DateTime pass-through in the
        // conversion core (relative anchoring is a no-op here).
        let expiration_ns = parse_i64(
            req_cell(&record, cols.expiration, "expiration")?,
            "expiration",
        )?;
        let quote = RawQuote {
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(expiration_ns)),
            strike: parse_cents(req_cell(&record, cols.strike, "strike")?, "strike")?,
            style: parse_style(req_cell(&record, cols.style, "style")?)?,
            bid: parse_cents(req_cell(&record, cols.bid, "bid")?, "bid")?,
            ask: parse_cents(req_cell(&record, cols.ask, "ask")?, "ask")?,
            bid_size: Quantity::new(parse_u32(
                req_cell(&record, cols.bid_size, "bid_size")?,
                "bid_size",
            )?)?,
            ask_size: Quantity::new(parse_u32(
                req_cell(&record, cols.ask_size, "ask_size")?,
                "ask_size",
            )?)?,
            implied_volatility: parse_f64(
                req_cell(&record, cols.implied_volatility, "implied_volatility")?,
                "implied_volatility",
            )?,
            delta: opt_greek(&record, cols.delta, "delta")?,
            gamma: opt_greek(&record, cols.gamma, "gamma")?,
            theta: opt_greek(&record, cols.theta, "theta")?,
            vega: opt_greek(&record, cols.vega, "vega")?,
        };
        quotes.push(quote);
    }

    let (ts, underlying, underlying_price, tick_size_cents, contract_multiplier) = header_fields
        .ok_or_else(|| {
            BacktestError::Conversion(format!(
                "csv file {} has no data rows; every file is one chain snapshot",
                path.display()
            ))
        })?;
    Ok(ParsedCsvSnapshot {
        ts,
        underlying,
        underlying_price,
        tick_size_cents,
        contract_multiplier,
        quotes,
    })
}

/// Read one required cell by column index; a short row (a missing cell) is a
/// typed [`BacktestError::Conversion`], never an unchecked index.
fn req_cell<'r>(
    record: &'r csv::StringRecord,
    index: usize,
    column: &str,
) -> Result<&'r str, BacktestError> {
    record.get(index).ok_or_else(|| {
        BacktestError::Conversion(format!("csv row is missing a value for column {column}"))
    })
}

/// Read an optional Greek cell: an absent column or an empty cell is the
/// documented `0` placeholder; otherwise the value is parsed as an analytic
/// (the NaN / infinite check happens once in the conversion core).
fn opt_greek(
    record: &csv::StringRecord,
    index: Option<usize>,
    column: &str,
) -> Result<f64, BacktestError> {
    match index.and_then(|i| record.get(i)) {
        None | Some("") => Ok(0.0),
        Some(cell) => parse_f64(cell, column),
    }
}

/// Parse a required `i64` cell (`ts` / `expiration`).
fn parse_i64(cell: &str, column: &str) -> Result<i64, BacktestError> {
    cell.parse::<i64>().map_err(|_| {
        BacktestError::Conversion(format!("column {column} value {cell:?} is not a valid i64"))
    })
}

/// Parse a required `u32` count cell (`contract_multiplier` / `bid_size` /
/// `ask_size`).
fn parse_u32(cell: &str, column: &str) -> Result<u32, BacktestError> {
    cell.parse::<u32>().map_err(|_| {
        BacktestError::Conversion(format!(
            "column {column} value {cell:?} is not a valid non-negative count"
        ))
    })
}

/// Parse a required integer-cents money cell into [`PriceCents`]. Money is a
/// non-negative **integer** on disk, so a dollar-denominated float (a `.` in the
/// cell) or a negative value fails with [`BacktestError::Conversion`] — never a
/// silent truncation.
fn parse_cents(cell: &str, column: &str) -> Result<PriceCents, BacktestError> {
    let cents = cell.parse::<u64>().map_err(|_| {
        BacktestError::Conversion(format!(
            "column {column} value {cell:?} is not integer cents; money is a non-negative integer \
             cent value (dollar floats are rejected)"
        ))
    })?;
    Ok(PriceCents::new(cents))
}

/// Parse a required analytic cell (`implied_volatility`, or a present Greek) as
/// `f64`. The NaN / infinite rejection is deferred to the conversion core, the
/// single place a non-finite analytic dies.
fn parse_f64(cell: &str, column: &str) -> Result<f64, BacktestError> {
    cell.parse::<f64>().map_err(|_| {
        BacktestError::Conversion(format!(
            "column {column} value {cell:?} is not a valid number"
        ))
    })
}

/// Map a `csv` decode error (bad UTF-8 or an inconsistent record shape) into a
/// typed [`BacktestError::Data`] — the undecodable-bytes kind, distinct from a
/// semantic [`BacktestError::Conversion`] on data that decoded cleanly.
fn csv_decode_err<E: std::fmt::Display>(context: &str, error: &E) -> BacktestError {
    BacktestError::Data(format!("{context}: {error}"))
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

/// Finalise a group, thread the tape anchor `ts_0`, and push the resulting
/// snapshot under the shared `max_steps` / `max_total_bytes` ceilings.
fn push_snapshot(
    tape: &mut Vec<ChainSnapshot>,
    group: GroupBuilder,
    anchor_ts: &mut Option<SimTime>,
    total_bytes: &mut u64,
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
    push_checked(tape, snapshot, total_bytes, limits)
}

/// Enforce `max_steps` and `max_total_bytes` for one finished snapshot, then
/// push it onto the tape. Shared by both file feeds so the ceiling accounting
/// is byte-for-byte identical regardless of the on-disk format.
fn push_checked(
    tape: &mut Vec<ChainSnapshot>,
    snapshot: ChainSnapshot,
    total_bytes: &mut u64,
    limits: &ResourceLimits,
) -> Result<(), BacktestError> {
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

    // `max_total_bytes` ceiling: accumulate an estimate of the materialised
    // tape's in-memory byte size with **checked** arithmetic on the real
    // counter (no wrapping / saturating), and abort on the FIRST crossing —
    // BEFORE this snapshot is pushed, so the `Vec` never grows past the cap.
    let snapshot_bytes = snapshot_byte_estimate(&snapshot)?;
    let next_total = total_bytes
        .checked_add(snapshot_bytes)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    if next_total > limits.max_total_bytes {
        return Err(BacktestError::TapeTooLarge {
            limit: "max_total_bytes",
            value: next_total,
            cap: limits.max_total_bytes,
        });
    }
    *total_bytes = next_total;

    tape.push(snapshot);
    Ok(())
}

/// Estimate one materialised snapshot's in-memory byte footprint for the
/// `max_total_bytes` tape ceiling.
///
/// Basis (documented): the fixed [`ChainSnapshot`] struct overhead plus, per
/// quote, the `BTreeMap` entry's stack footprint — the key ([`ContractKey`])
/// and the value ([`QuoteView`], which itself embeds a second `ContractKey`).
/// This is a **proportional** estimate, not a strict physical upper bound: it
/// charges the tight per-entry data size (the key stored twice matches physical
/// storage) but does not add the `BTreeMap`'s node slack (B-tree nodes may be
/// only part-filled) and counts the interned `Underlying` `Arc<str>` as its
/// pointer footprint only (its heap bytes are shared across quotes and bounded
/// by the `^[A-Z0-9._]{1,32}$` grammar). Those omitted terms are each bounded
/// by a small constant factor, so peak memory stays within a constant multiple
/// of `max_total_bytes` — growth is bounded, never unbounded, which is the
/// anti-OOM guarantee. The estimate feeds only the ceiling check; it never
/// affects a materialised value.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] if the per-snapshot byte
/// estimate overflows `u64` (unreachable for a snapshot bounded by
/// `max_contracts_per_snapshot`, but checked rather than wrapped).
#[must_use = "the byte estimate must feed the ceiling check"]
fn snapshot_byte_estimate(snapshot: &ChainSnapshot) -> Result<u64, BacktestError> {
    let per_quote = u64::try_from(size_of::<ContractKey>() + size_of::<QuoteView>())
        .map_err(|_| BacktestError::ArithmeticOverflow)?;
    let overhead =
        u64::try_from(size_of::<ChainSnapshot>()).map_err(|_| BacktestError::ArithmeticOverflow)?;
    let quotes =
        u64::try_from(snapshot.quotes.len()).map_err(|_| BacktestError::ArithmeticOverflow)?;
    quotes
        .checked_mul(per_quote)
        .and_then(|q| q.checked_add(overhead))
        .ok_or(BacktestError::ArithmeticOverflow)
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

/// Reject a snapshot-level field that varies within a step group. The `step`
/// is used only in the message, so it is generic over its display type (the
/// Parquet feed passes an `i32`, the CSV feed a `u32`).
fn ensure_const<S: std::fmt::Display, T: PartialEq + std::fmt::Display>(
    field: &str,
    step: S,
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
    fn test_open_enforces_max_total_bytes_tape_too_large() {
        // A multi-step tape opened with a 1-byte total ceiling: the first
        // snapshot's byte estimate (fixed overhead + per-quote entries) already
        // crosses the cap, so materialisation is cut off before the Vec grows.
        let mut chain = Chain::default();
        condor_step(&mut chain, 0, TS0);
        condor_step(&mut chain, 1, TS0 + NANOS_PER_DAY);
        let Ok((_dir, path)) = write_standard(&chain) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_total_bytes: 1,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            ParquetFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_total_bytes",
                ..
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

#[cfg(test)]
mod csv_tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::CsvFeed;
    use crate::config::ResourceLimits;
    use crate::data::DataSourceSpec;
    use crate::data::feed::DataFeed;
    use crate::error::BacktestError;

    const HEADER: &str = "ts,underlying,underlying_price,tick_size,contract_multiplier,\
        expiration,strike,style,bid,ask,bid_size,ask_size,implied_volatility,delta,gamma,theta,vega";
    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;

    /// One canonical quote row (SPX / 5c tick / 100x / absolute 30-day expiry).
    fn row(ts: i64, strike: i64, style: &str, bid: i64, ask: i64) -> String {
        format!(
            "{ts},SPX,500000,5,100,{EXPIRY},{strike},{style},{bid},{ask},50,50,0.2,0.3,0.01,-0.05,0.1"
        )
    }

    /// A well-formed four-contract condor slice (two strikes × call/put) at `ts`.
    fn condor_file(ts: i64) -> String {
        let mut out = String::from(HEADER);
        for line in [
            row(ts, 500_000, "call", 200, 210),
            row(ts, 500_000, "put", 180, 190),
            row(ts, 520_000, "call", 90, 100),
            row(ts, 520_000, "put", 140, 150),
        ] {
            out.push('\n');
            out.push_str(&line);
        }
        out
    }

    /// Write `(name, content)` files into a fresh tempdir; return dir + its path.
    fn write_dir(files: &[(&str, String)]) -> Result<(TempDir, PathBuf), BacktestError> {
        let dir = tempfile::tempdir()?;
        for (name, content) in files {
            std::fs::write(dir.path().join(name), content)?;
        }
        let path = dir.path().to_path_buf();
        Ok((dir, path))
    }

    /// A single-file directory carrying `content` as `step_000.csv`.
    fn single(content: String) -> Result<(TempDir, PathBuf), BacktestError> {
        write_dir(&[("step_000.csv", content)])
    }

    /// One header + one canonical row file (valid base for a perturbation).
    fn one_row(strike: i64, style: &str, bid: i64, ask: i64) -> String {
        format!("{HEADER}\n{}", row(TS0, strike, style, bid, ask))
    }

    #[test]
    fn test_csv_open_reads_name_sorted_tape_and_yields_to_exhaustion() {
        // Files WRITTEN out of name order; the name sort (not fs order) is the
        // replay order, and the ascending ts must follow that name order.
        let Ok((_dir, path)) = write_dir(&[
            ("step_002.csv", condor_file(TS0 + 2 * NANOS_PER_DAY)),
            ("step_000.csv", condor_file(TS0)),
            ("step_001.csv", condor_file(TS0 + NANOS_PER_DAY)),
        ]) else {
            panic!("the canonical fixture must write");
        };
        let Ok(mut feed) = CsvFeed::open(&path, &ResourceLimits::default()) else {
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
                    assert_eq!(snap.quotes.len(), 4);
                }
                other => panic!("expected snapshot at step {expected_step}, got {other:?}"),
            }
        }
        assert!(matches!(feed.next(), Ok(None)));
        assert!(matches!(feed.next(), Ok(None)));
    }

    #[test]
    fn test_csv_open_rejects_dollar_float_money_conversion() {
        // A bid written as dollars (`19.95`) is not integer cents.
        let content = format!(
            "{HEADER}\n{TS0},SPX,500000,5,100,{EXPIRY},510000,call,19.95,20.05,50,50,0.2,0.3,0.01,-0.05,0.1"
        );
        let Ok((_dir, path)) = single(content) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_missing_required_column_conversion() {
        // Header without `tick_size`.
        let header = "ts,underlying,underlying_price,contract_multiplier,expiration,strike,style,\
            bid,ask,bid_size,ask_size,implied_volatility";
        let content =
            format!("{header}\n{TS0},SPX,500000,100,{EXPIRY},510000,call,1995,2005,50,50,0.2");
        let Ok((_dir, path)) = single(content) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_zero_tick_conversion() {
        let content = format!(
            "{HEADER}\n{TS0},SPX,500000,0,100,{EXPIRY},510000,call,1995,2005,50,50,0.2,0.3,0.01,-0.05,0.1"
        );
        let Ok((_dir, path)) = single(content) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_zero_multiplier_conversion() {
        let content = format!(
            "{HEADER}\n{TS0},SPX,500000,5,0,{EXPIRY},510000,call,1995,2005,50,50,0.2,0.3,0.01,-0.05,0.1"
        );
        let Ok((_dir, path)) = single(content) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_non_tick_aligned_price_price_not_tick_aligned() {
        // ask 107 is not a multiple of the 5-cent tick.
        let Ok((_dir, path)) = single(one_row(500_000, "call", 100, 107)) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::PriceNotTickAligned {
                price: 107,
                tick: 5
            })
        ));
    }

    #[test]
    fn test_csv_open_rejects_crossed_quote() {
        // bid 2010 > ask 2000 (both tick-aligned) is crossed.
        let Ok((_dir, path)) = single(one_row(510_000, "call", 2_010, 2_000)) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::CrossedQuote {
                bid: 2_010,
                ask: 2_000
            })
        ));
    }

    #[test]
    fn test_csv_open_rejects_negative_strike_conversion() {
        // A negative strike fails the unsigned integer-cents parse.
        let Ok((_dir, path)) = single(one_row(-510_000, "call", 1_995, 2_005)) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_zero_strike_conversion() {
        let Ok((_dir, path)) = single(one_row(0, "call", 1_995, 2_005)) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_nan_analytic_conversion() {
        // IV `nan` parses to a non-finite f64 the conversion core rejects.
        let content = format!(
            "{HEADER}\n{TS0},SPX,500000,5,100,{EXPIRY},510000,call,1995,2005,50,50,nan,0.3,0.01,-0.05,0.1"
        );
        let Ok((_dir, path)) = single(content) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_out_of_order_ts_data_out_of_order() {
        // Name-sorted step_000 then step_001, but ts descending.
        let Ok((_dir, path)) = write_dir(&[
            ("step_000.csv", condor_file(TS0 + NANOS_PER_DAY)),
            ("step_001.csv", condor_file(TS0)),
        ]) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::DataOutOfOrder {
                step: 1,
                ts,
                prev
            }) if ts == TS0 && prev == TS0 + NANOS_PER_DAY
        ));
    }

    #[test]
    fn test_csv_open_rejects_empty_directory_conversion() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        assert!(matches!(
            CsvFeed::open(dir.path(), &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_file_with_no_data_rows_conversion() {
        let Ok((_dir, path)) = single(HEADER.to_string()) else {
            panic!("fixture must write");
        };
        assert!(matches!(
            CsvFeed::open(&path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_non_directory_path_conversion() {
        // Pointing the feed at a regular file (not a directory) is rejected.
        let Ok((_dir, path)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        let file_path = path.join("step_000.csv");
        assert!(matches!(
            CsvFeed::open(&file_path, &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_rejects_non_file_entry_conversion() {
        // A directory that contains a sub-directory is not a flat set of files.
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir must create");
        };
        if std::fs::create_dir(dir.path().join("nested")).is_err() {
            panic!("nested dir must create");
        }
        assert!(matches!(
            CsvFeed::open(dir.path(), &ResourceLimits::default()),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_enforces_max_file_bytes_tape_too_large() {
        let Ok((_dir, path)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_file_bytes: 1,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            CsvFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_file_bytes",
                ..
            })
        ));
    }

    #[test]
    fn test_csv_open_enforces_max_decompressed_bytes_tape_too_large() {
        let Ok((_dir, path)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_decompressed_bytes: 1,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            CsvFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_decompressed_bytes",
                ..
            })
        ));
    }

    #[test]
    fn test_csv_open_enforces_max_steps_tape_too_large() {
        let Ok((_dir, path)) = write_dir(&[
            ("step_000.csv", condor_file(TS0)),
            ("step_001.csv", condor_file(TS0 + NANOS_PER_DAY)),
            ("step_002.csv", condor_file(TS0 + 2 * NANOS_PER_DAY)),
        ]) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_steps: 2,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            CsvFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_steps",
                ..
            })
        ));
    }

    #[test]
    fn test_csv_open_enforces_max_contracts_per_snapshot_tape_too_large() {
        // One file with four contracts, ceiling of two.
        let Ok((_dir, path)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_contracts_per_snapshot: 2,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            CsvFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_contracts_per_snapshot",
                value: 3,
                cap: 2
            })
        ));
    }

    #[test]
    fn test_csv_open_enforces_max_total_bytes_tape_too_large() {
        let Ok((_dir, path)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        let limits = ResourceLimits {
            max_total_bytes: 1,
            ..ResourceLimits::default()
        };
        assert!(matches!(
            CsvFeed::open(&path, &limits),
            Err(BacktestError::TapeTooLarge {
                limit: "max_total_bytes",
                ..
            })
        ));
    }

    #[test]
    fn test_csv_open_sha256_stable_across_two_opens() {
        let Ok((_dir, path)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        let (Ok(a), Ok(b)) = (
            CsvFeed::open(&path, &ResourceLimits::default()),
            CsvFeed::open(&path, &ResourceLimits::default()),
        ) else {
            panic!("both opens of the same bytes must succeed");
        };
        assert_eq!(a.tape_meta().data_identity, b.tape_meta().data_identity);
        assert_eq!(a.tape_meta().data_identity.len(), 64);
        assert!(matches!(
            (a.meta(), b.meta()),
            (
                DataSourceSpec::Csv { sha256: sa, .. },
                DataSourceSpec::Csv { sha256: sb, .. }
            ) if sa == sb && sa == a.tape_meta().data_identity
        ));
    }

    #[test]
    fn test_csv_open_sha256_changes_when_a_file_byte_changes() {
        let Ok((_dir_a, path_a)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        // Same schema, one different bid → a different content hash.
        let Ok((_dir_b, path_b)) = single(one_row(510_000, "call", 1_990, 2_005)) else {
            panic!("fixture must write");
        };
        let (Ok(a), Ok(b)) = (
            CsvFeed::open(&path_a, &ResourceLimits::default()),
            CsvFeed::open(&path_b, &ResourceLimits::default()),
        ) else {
            panic!("both fixtures must open");
        };
        assert_ne!(a.tape_meta().data_identity, b.tape_meta().data_identity);
    }

    #[test]
    fn test_csv_open_verified_accepts_match_and_rejects_mismatch() {
        let Ok((_dir, path)) = single(condor_file(TS0)) else {
            panic!("fixture must write");
        };
        let Ok(feed) = CsvFeed::open(&path, &ResourceLimits::default()) else {
            panic!("fixture must open");
        };
        let sha = feed.tape_meta().data_identity.clone();
        // Empty expected → skip (config not yet pinned).
        assert!(CsvFeed::open_verified(&path, "", &ResourceLimits::default()).is_ok());
        // Correct expected → verifies.
        assert!(CsvFeed::open_verified(&path, &sha, &ResourceLimits::default()).is_ok());
        // A recorded-but-wrong hash → typed re-read divergence error.
        assert!(matches!(
            CsvFeed::open_verified(
                &path,
                "0000000000000000000000000000000000000000000000000000000000000000",
                &ResourceLimits::default()
            ),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_csv_open_treats_absent_greek_columns_as_zero() {
        // A header without the four Greek columns → each Greek is the 0
        // placeholder; IV stays required and present.
        let header = "ts,underlying,underlying_price,tick_size,contract_multiplier,expiration,\
            strike,style,bid,ask,bid_size,ask_size,implied_volatility";
        let content =
            format!("{header}\n{TS0},SPX,500000,5,100,{EXPIRY},510000,call,1995,2005,50,50,0.2");
        let Ok((_dir, path)) = single(content) else {
            panic!("fixture must write");
        };
        let Ok(mut feed) = CsvFeed::open(&path, &ResourceLimits::default()) else {
            panic!("absent Greek columns must be accepted as the 0 placeholder");
        };
        match feed.next() {
            Ok(Some(snap)) => match snap.quotes.values().next() {
                Some(view) => {
                    assert!(view.delta.is_zero());
                    assert!(view.gamma.is_zero());
                    assert!(view.theta.is_zero());
                    assert!(view.vega.is_zero());
                }
                None => panic!("one quote must be present"),
            },
            other => panic!("expected one snapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_csv_open_treats_empty_greek_cell_as_zero() {
        // An empty theta cell (present column, blank value) → 0 placeholder.
        let content = format!(
            "{HEADER}\n{TS0},SPX,500000,5,100,{EXPIRY},510000,call,1995,2005,50,50,0.2,0.3,0.01,,0.1"
        );
        let Ok((_dir, path)) = single(content) else {
            panic!("fixture must write");
        };
        let Ok(mut feed) = CsvFeed::open(&path, &ResourceLimits::default()) else {
            panic!("an empty Greek cell must be the 0 placeholder");
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
