//! Scenario **batch runner** — the multi-run composition root.
//!
//! [`run_scenario_batch`] turns a [`ScenarioParams`] batch into `N` independent
//! single-threaded backtests, fanned out **across** a bounded worker pool
//! (never *within* a run), each writing its own result bundle under its
//! deterministic `<run_id>/`, plus a batch **parent index**
//! ([docs/02 §9](../../docs/02-engine-architecture.md#9-scenario-orchestration)).
//!
//! # Why the runner lives here, not in `engine/scenario.rs`
//!
//! The **generator** (seed derivation, walk presets, `ScenarioParams → configs`
//! expansion) is a pure, downward-only function and lives in
//! [`crate::engine::scenario`]. The **runner** must call the analytics core
//! ([`crate::run::run_spec_with_feed`]) and the bundle writer
//! ([`crate::bundle::write_bundle`]) — both **upstream** of the engine layer.
//! Placing it inside `src/engine/` would make the engine import `analytics` /
//! `bundle`, inverting the layering the module boundaries forbid
//! ([CLAUDE.md](../../CLAUDE.md)). So the runner sits at the crate top level, a
//! sibling of [`crate::run`], **above** both the engine and analytics layers —
//! the same placement, and for the same reason, as `run_backtest`. (This is a
//! deliberate deviation from the doc's "orchestration in `scenario.rs`" wording;
//! the layering rule wins, and the design doc is flagged for the architect.)
//!
//! # Composition-root dispatch (the simulator arm)
//!
//! [`crate::run::run_backtest`] dispatches only a [`DataSourceSpec::Parquet`]
//! source — its fixed `(config, strategy, exit)` signature cannot open a
//! [`SimulatorFeed`], because a synthetic session needs the tick grid, the
//! per-quote depth, and a network timeout that are deliberately **not** in
//! [`BacktestConfig`] (adding them would change the manifest / `run_id` /
//! bundle schema — out of scope here). Those three materialisation inputs are
//! batch-level: one instrument grid and quote size describe a whole sweep, so
//! they live in [`SimulatorMaterialisation`], which the runner owns. The runner
//! therefore does the per-run feed dispatch itself — `Parquet → ParquetFeed`,
//! `Simulator → SimulatorFeed::open` — then delegates the identical run +
//! analytics + bundle pipeline through the shared [`crate::run::run_spec_with_feed`]
//! core, which builds whichever strategy the spec names (both named kinds and an
//! explicit [`StrategySpec::Legs`] leg set, #117). `Csv` stays deferred exactly
//! as `run_backtest` defers it (a typed [`BacktestError::Config`]).
//!
//! # Independence and determinism
//!
//! Each run owns its feed, execution model, ledger, and bundle writer; the only
//! thing crossing a thread boundary is **immutable** shared data (the strategy
//! spec, the exit policy, the materialisation knobs) and each worker's own
//! disjoint slice of pre-built configs. There is no cross-run mutable state to
//! race. The per-run **engine** seed is the fixed hash
//! [`child_seed`](crate::engine::child_seed)`(base_seed, i)`, computed in the
//! single-threaded generator, so the batch reproduces the same per-run seeds
//! regardless of how the pool schedules it. Per-run bundles are written to
//! distinct `<run_id>/` directories (the seed is part of the `run_id` hash), so
//! runs never collide.
//!
//! # The read-only parse cache ([docs/03 §8](../../docs/03-data-layer.md#8-caching))
//!
//! Before fan-out the runner materialises each **distinct** file-feed path
//! **once**, single-threaded, into a read-only `SharedTapes` cache (a
//! `SharedParquetTape` — an [`Arc`](std::sync::Arc) over the immutable parsed
//! tape). A sweep of `N` runs over the same Parquet file then parses it once and
//! shares the tape; each run gets a cheap `Arc` view with its own cursor. The
//! cache is a pure parse-time optimisation and **never changes results**: a feed
//! over a shared tape yields byte-identically to a per-run
//! [`ParquetFeed::open`], and a run whose path is not cached (a failed
//! pre-materialisation) simply re-opens it itself. The cache stays **off** the
//! single-run path — [`crate::run::run_backtest`] opens its own [`ParquetFeed`].
//!
//! Materialisation verifies each recorded file `sha256`
//! ([`ParquetFeed::open_verified`]): a changed / missing / tampered file is the
//! typed [`BacktestError`] recorded in that run's index entry, never a silent
//! divergent run.
//!
//! # Honest reproducibility ([docs/03 §6](../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator))
//!
//! The batch's reproducibility guarantee covers the **engine** seeds (the fixed
//! `child_seed`) and **file feeds** (Parquet: same inputs ⇒ same `run_id`s and
//! same parent index). The **data** side is only reproducible up to what
//! upstream supports: a per-run `data_seed` is derived
//! ([`child_data_seed`](crate::engine::child_data_seed)) and sent as the walk
//! seed, and recorded in the index — but upstream v0.1.0 still wall-clock-stamps
//! each snapshot timestamp, so two synthetic runs from the identical request get
//! **distinct** tapes and **distinct** `run_id`s today. The parent index records
//! each child's engine seed, data seed, `run_id` (or error), bundle path, and
//! tape `sha256` either way.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use optionstratlib::simulation::ExitPolicy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bundle::write_bundle;
use crate::config::BacktestConfig;
#[cfg(feature = "simulator")]
use crate::data::SimulatorFeed;
use crate::data::historical::to_hex;
use crate::data::{DataSourceSpec, ParquetFeed, SharedParquetTape};
use crate::domain::{InstrumentSpec, Quantity, StrategySpec};
use crate::engine::{BacktestRun, ScenarioParams, ScenarioType, expand};
use crate::error::BacktestError;
use crate::run::run_spec_with_feed;

/// The parent-index schema tag. **Not** part of `ironcondor.bundle.v1` — the
/// batch index is a batch-level convenience artifact whose shape is **not
/// frozen** (the `v0` marks it as evolving); ChainView consumes the per-run
/// bundles, not this index.
pub const BATCH_INDEX_SCHEMA: &str = "ironcondor.batch_index.v0";

/// The batch-level simulator materialisation knobs — the inputs a synthetic
/// session needs that are **not** in [`BacktestConfig`].
///
/// One instrument grid and quote size describe a whole sweep (the wire carries
/// neither the tick grid nor per-quote depth,
/// [docs/03 §7](../../docs/03-data-layer.md#7-chainresponse--optionchain-conversion)),
/// and one network timeout bounds every session materialisation. A
/// [`DataSourceSpec::Parquet`]-only batch does not need this (pass `None`); a
/// simulator source without it is a recorded per-run error, never a panic.
#[derive(Debug, Clone, Copy)]
pub struct SimulatorMaterialisation {
    /// The instrument tick grid + contract multiplier for the synthetic chain.
    pub instrument: InstrumentSpec,
    /// The single per-side quote depth applied to every converted contract.
    pub quote_size: Quantity,
    /// The per-request network timeout for session materialisation.
    pub timeout: Duration,
}

impl SimulatorMaterialisation {
    /// Assemble the materialisation knobs.
    #[must_use]
    pub const fn new(instrument: InstrumentSpec, quote_size: Quantity, timeout: Duration) -> Self {
        Self {
            instrument,
            quote_size,
            timeout,
        }
    }
}

/// The batch **parent index** — one entry per run, ordered by run index.
///
/// Serialised as canonical JSON (sorted keys, ordered `runs`) to
/// `<output_dir>/batch_<batch_id>/index.json`, carrying **no** wall-clock field,
/// so for file feeds the bytes are a pure function of the batch outcomes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchIndex {
    /// The (non-frozen) schema tag ([`BATCH_INDEX_SCHEMA`]).
    pub schema: String,
    /// A deterministic id of this batch (a hash of its params + base config +
    /// strategy) — the `batch_<batch_id>/` directory name.
    pub batch_id: String,
    /// The batch kind ([`ScenarioType`]).
    pub kind: ScenarioType,
    /// The batch root seed.
    pub base_seed: u64,
    /// The configured replica count (`count`).
    pub count: u32,
    /// The realised number of runs (`sweep.len().max(1) × count`).
    pub run_count: u32,
    /// One entry per run, ordered by [`BatchRunEntry::index`].
    pub runs: Vec<BatchRunEntry>,
}

/// One run's entry in the [`BatchIndex`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchRunEntry {
    /// The positional run index (`override_idx × count + replicate`).
    pub index: u32,
    /// The **engine** seed the run used (`config.seed`) — the derived
    /// [`child_seed`](crate::engine::child_seed) or an explicit override.
    pub engine_seed: u64,
    /// The **data** seed the run sent to the simulator walk, when the source is
    /// a simulator session; `None` for file feeds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_seed: Option<u64>,
    /// The run's outcome — a written bundle or a recorded error.
    pub outcome: BatchRunOutcome,
}

/// A run's terminal outcome — a batch does **not** abort on one run's failure;
/// it records it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum BatchRunOutcome {
    /// The run completed and its bundle was written.
    Ok {
        /// The bundle's `run_id` (its directory name).
        run_id: String,
        /// The published bundle directory path.
        bundle_path: String,
        /// The materialised-tape / file `sha256` (the `run_id` data identity).
        tape_sha256: String,
        /// The run's terminal equity in integer cents (last equity point).
        terminal_equity_cents: i64,
    },
    /// The run failed; the typed error is recorded (the batch continues).
    Error {
        /// The run's error message.
        error: String,
    },
}

/// The domain tag folded into the deterministic `batch_id` hash.
const BATCH_ID_TAG: &[u8] = b"ironcondor.scenario.batch.v1\0";

/// Run a [`ScenarioParams`] batch: expand it into `N` independent configs, fan
/// them out across a bounded worker pool (parallel **across** runs only), write
/// each run's bundle, and publish the parent index. Returns the assembled
/// [`BatchIndex`].
///
/// `base_config` is the template every run derives from; `strategy`, `exit`, and
/// `materialisation` are shared **immutable** inputs. For a `Parquet` batch,
/// pass `materialisation = None` (it is unused); a simulator source without it
/// is a recorded per-run error. Each single-run loop stays synchronous,
/// single-threaded, and zero-`.await` — the fan-out wraps **whole** runs.
///
/// # Errors
///
/// - [`BacktestError::ArithmeticOverflow`] if the run count overflows.
/// - [`BacktestError::Conversion`] if the generator rejects a stress shock.
/// - [`BacktestError::Bundle`] if the parent index cannot be serialised or
///   written.
/// - [`BacktestError::Execution`] only if a worker thread **panicked** (a bug —
///   ordinary run failures are recorded as [`BatchRunOutcome::Error`], never
///   propagated).
pub fn run_scenario_batch(
    params: &ScenarioParams,
    base_config: &BacktestConfig,
    strategy: &StrategySpec,
    exit: &ExitPolicy,
    materialisation: Option<&SimulatorMaterialisation>,
) -> Result<BatchIndex, BacktestError> {
    // 1. Expand — pure, deterministic, single-threaded (the seeds are fixed here).
    let configs = expand(params, base_config)?;
    let mut planned = Vec::with_capacity(configs.len());
    for (idx, config) in configs.into_iter().enumerate() {
        let index = u32::try_from(idx).map_err(|_| BacktestError::ArithmeticOverflow)?;
        planned.push(PlannedRun { index, config });
    }

    // 1b. Parse each distinct file-feed tape ONCE, single-threaded, into the
    //     read-only batch cache: a sweep of N runs over the same Parquet path
    //     parses it once and shares the immutable tape. A pure optimisation —
    //     each run falls back to its own verified open on a cache miss, so
    //     correctness never depends on it (docs/03 §8).
    let shared = materialise_shared_tapes(&planned);

    // 2. Fan out across a bounded pool — each worker runs whole, independent runs.
    let runs = fan_out(&planned, strategy, exit, materialisation, &shared)?;

    // 3. Assemble + publish the deterministic parent index.
    let batch_id = derive_batch_id(params, base_config, strategy)?;
    let run_count = u32::try_from(runs.len()).map_err(|_| BacktestError::ArithmeticOverflow)?;
    let index = BatchIndex {
        schema: BATCH_INDEX_SCHEMA.to_string(),
        batch_id,
        kind: params.kind,
        base_seed: params.base_seed,
        count: params.count,
        run_count,
        runs,
    };
    let path = write_batch_index(&index, base_config.output_dir.as_path())?;

    let failures = index
        .runs
        .iter()
        .filter(|entry| matches!(entry.outcome, BatchRunOutcome::Error { .. }))
        .count();
    tracing::info!(
        batch_id = index.batch_id.as_str(),
        kind = ?index.kind,
        run_count = index.run_count,
        failures,
        index = %path.display(),
        "scenario batch finished"
    );
    Ok(index)
}

/// One planned run: its positional index and its fully-derived config.
struct PlannedRun {
    index: u32,
    config: BacktestConfig,
}

/// The batch **parse cache**: each distinct file-feed path materialised once and
/// shared read-only across every run over it ([docs/03 §8](../../docs/03-data-layer.md#8-caching)).
///
/// Keyed by source path; deterministic (a [`BTreeMap`] populated in planned
/// order, read-only during fan-out). A failed pre-materialisation (missing /
/// oversized / tampered file) is cached as an `Err` DESCRIPTOR (#110): every
/// run over that path replays the identical typed error from the cache without
/// re-opening the file, so one hostile input costs one parse, not one per
/// worker. The cache stays observationally invisible on the success path: an
/// `Ok` hit is sha-guarded and never changes a bundle.
/// Path → materialise outcome. An `Err` caches the error DESCRIPTOR (the typed
/// error's rendered message — [`crate::error::BacktestError`] is not `Clone`),
/// so every run sharing a bad path records the identical error without
/// re-opening or re-parsing the file (#110): one hostile input costs one parse,
/// not one per worker.
type SharedTapes = BTreeMap<String, Result<SharedParquetTape, String>>;

/// Materialise each distinct Parquet source path once (single-threaded), pinning
/// and verifying its recorded `sha256`, into the read-only batch cache.
///
/// A materialisation failure does not abort the batch: the rendered error is
/// cached per path (#110) and [`open_and_run`] replays it into each affected
/// run's index entry without re-opening the file. Non-Parquet sources (the
/// simulator feed) are not shared — each session materialises its own tape per
/// run.
///
/// INVARIANT (uniform limits per path): the cache is keyed by path with a sha
/// guard at hit time, NOT by `(path, limits)`. That is safe only because
/// `ConfigOverride` can vary neither `limits` nor `data_source` today, so every
/// run sharing a path shares the exact `(sha256, limits)` a fresh open would
/// use. If a future override gains either knob, key this cache on
/// `(path, limits)` (or split per-limits) — otherwise a cache hit could serve a
/// tape that a stricter-limits fresh open would reject with `TapeTooLarge`.
fn materialise_shared_tapes(planned: &[PlannedRun]) -> SharedTapes {
    let mut shared = SharedTapes::new();
    for run in planned {
        if let DataSourceSpec::Parquet { path, sha256 } = &run.config.data_source
            && !shared.contains_key(path)
        {
            // An Err caches the rendered error per path (#110): every run
            // sharing the path records the identical descriptor below without
            // re-opening a file known to fail.
            let outcome = SharedParquetTape::materialise(path, sha256, &run.config.limits)
                .map_err(|error| error.to_string());
            shared.insert(path.clone(), outcome);
        }
    }
    shared
}

/// A completed run's summary (the [`BatchRunOutcome::Ok`] payload).
struct RunSummary {
    run_id: String,
    bundle_path: String,
    tape_sha256: String,
    terminal_equity_cents: i64,
}

/// The bounded worker count: `min(run_count, available_parallelism)`, at least
/// one. Parallelism is **across** runs; a single run is always single-threaded.
fn worker_count(run_count: usize) -> usize {
    let available = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    run_count.min(available).max(1)
}

/// Fan the planned runs out across scoped worker threads, one disjoint chunk
/// each, and collect their entries in **index** order (scheduling-independent).
///
/// Nothing mutable is shared: each worker borrows the immutable `strategy` /
/// `exit` / `materialisation`, the read-only [`SharedTapes`] parse cache, and its
/// own disjoint `&[PlannedRun]` slice, and returns owned entries. The cache is an
/// [`Arc`](std::sync::Arc)-backed immutable tape (a cheap clone per run view), so
/// it crosses the thread boundary without any cross-run mutable state. A worker
/// only panics on a bug (every run failure is a recorded
/// [`BatchRunOutcome::Error`]); a panic is surfaced as a typed error rather than
/// silently dropping a chunk.
fn fan_out(
    planned: &[PlannedRun],
    strategy: &StrategySpec,
    exit: &ExitPolicy,
    materialisation: Option<&SimulatorMaterialisation>,
    shared: &SharedTapes,
) -> Result<Vec<BatchRunEntry>, BacktestError> {
    if planned.is_empty() {
        return Ok(Vec::new());
    }
    let workers = worker_count(planned.len());
    let chunk_size = planned.len().div_ceil(workers);

    let mut collected: Vec<BatchRunEntry> = Vec::with_capacity(planned.len());
    let mut panicked = false;
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for chunk in planned.chunks(chunk_size) {
            handles.push(scope.spawn(move || {
                let mut local = Vec::with_capacity(chunk.len());
                for run in chunk {
                    local.push(run_one_entry(run, strategy, exit, materialisation, shared));
                }
                local
            }));
        }
        for handle in handles {
            match handle.join() {
                Ok(mut local) => collected.append(&mut local),
                Err(_) => panicked = true,
            }
        }
    });

    if panicked {
        return Err(BacktestError::Execution(
            "a scenario batch worker thread panicked; run failures are recorded, not panicked \
             (this is a bug)"
                .to_string(),
        ));
    }
    collected.sort_by_key(|entry| entry.index);
    Ok(collected)
}

/// Run one planned run to a [`BatchRunEntry`] — always a value, never an error
/// (a failed run becomes [`BatchRunOutcome::Error`], so the batch continues).
fn run_one_entry(
    run: &PlannedRun,
    strategy: &StrategySpec,
    exit: &ExitPolicy,
    materialisation: Option<&SimulatorMaterialisation>,
    shared: &SharedTapes,
) -> BatchRunEntry {
    let engine_seed = run.config.seed;
    let data_seed = data_seed_of(&run.config);
    let outcome = match run_one(&run.config, strategy, exit, materialisation, shared) {
        Ok(summary) => BatchRunOutcome::Ok {
            run_id: summary.run_id,
            bundle_path: summary.bundle_path,
            tape_sha256: summary.tape_sha256,
            terminal_equity_cents: summary.terminal_equity_cents,
        },
        Err(error) => BatchRunOutcome::Error {
            error: error.to_string(),
        },
    };
    BatchRunEntry {
        index: run.index,
        engine_seed,
        data_seed,
        outcome,
    }
}

/// Validate, open the config's feed, run the full single-run pipeline, and write
/// the bundle — the whole synchronous, single-threaded per-run body.
fn run_one(
    config: &BacktestConfig,
    strategy: &StrategySpec,
    exit: &ExitPolicy,
    materialisation: Option<&SimulatorMaterialisation>,
    shared: &SharedTapes,
) -> Result<RunSummary, BacktestError> {
    config.validate()?;
    let run = open_and_run(config, strategy, exit, materialisation, shared)?;
    let bundle_path = write_bundle(&run, config, strategy)?;

    // `<output_dir>/<run_id>` — the final component is the run_id.
    let run_id = bundle_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| {
            BacktestError::Bundle("published bundle path has no run_id component".to_string())
        })?;
    let terminal_equity_cents = run
        .equity_curve
        .last()
        .map_or(0, |point| point.equity_cents);
    Ok(RunSummary {
        run_id,
        bundle_path: bundle_path.display().to_string(),
        tape_sha256: run.data_identity.clone(),
        terminal_equity_cents,
    })
}

/// Open the feed named by `config.data_source` and drive the shared run +
/// analytics core ([`run_spec_with_feed`]). `Parquet` and (feature `simulator`)
/// `Simulator` are dispatched; `Csv` and everything else are deferred, matching
/// `run_backtest`.
///
/// A `Parquet` source uses the batch parse cache when it holds this path's tape
/// **and** the tape's identity matches the run's recorded `sha256` (or the run
/// pins no `sha256`) — a cheap `Arc` view over the once-parsed tape. Otherwise it
/// falls back to a per-run [`ParquetFeed::open_verified`], which re-reads and
/// re-verifies the file, so a changed / missing / tampered file is the typed
/// error recorded in this run's index entry, never a silent divergent run. Either
/// way the feed yields byte-identically, so the cache never changes results.
#[cfg_attr(not(feature = "simulator"), allow(unused_variables))]
fn open_and_run(
    config: &BacktestConfig,
    strategy: &StrategySpec,
    exit: &ExitPolicy,
    materialisation: Option<&SimulatorMaterialisation>,
    shared: &SharedTapes,
) -> Result<BacktestRun, BacktestError> {
    match &config.data_source {
        DataSourceSpec::Parquet { path, sha256 } => {
            let feed = match shared.get(path) {
                // Cache hit — only when the run's recorded identity matches the
                // once-parsed tape (an empty `sha256` pins whatever was read, as
                // `open` does). A mismatch cannot slip past: it falls through to
                // the verified re-read, which fails typed.
                Some(Ok(tape)) if sha256.is_empty() || tape.data_identity() == sha256 => {
                    tape.feed()
                }
                // Cached materialise FAILURE (#110): replay the descriptor as
                // the run's typed error without re-opening the bad path. The
                // re-wrap in `Data` prefixes the recorded string with its
                // Display ("malformed data: …") — the original KIND is not
                // reconstructible (BacktestError is not Clone); the descriptor
                // itself carries the original kind's message, and every cached
                // run records the identical string.
                Some(Err(descriptor)) => {
                    return Err(BacktestError::Data(descriptor.clone()));
                }
                // Cache miss / bypass — a per-run verified open (identical tape).
                _ => ParquetFeed::open_verified(path, sha256, &config.limits)?,
            };
            run_spec_with_feed(config, feed, strategy, exit.clone())
        }
        #[cfg(feature = "simulator")]
        DataSourceSpec::Simulator(spec) => {
            let knobs = materialisation.ok_or_else(|| {
                BacktestError::Config(
                    "a simulator scenario source requires SimulatorMaterialisation \
                     (instrument grid, quote size, timeout)"
                        .to_string(),
                )
            })?;
            let feed = SimulatorFeed::open(
                spec,
                knobs.instrument,
                knobs.quote_size,
                knobs.timeout,
                &config.limits,
            )?;
            run_spec_with_feed(config, feed, strategy, exit.clone())
        }
        other => Err(BacktestError::Config(format!(
            "scenario batch supports a parquet or simulator data source; got {other:?}"
        ))),
    }
}

/// Read a run's data seed for the parent index: `Some` for a simulator source,
/// `None` for a file feed.
fn data_seed_of(config: &BacktestConfig) -> Option<u64> {
    #[cfg(feature = "simulator")]
    if let DataSourceSpec::Simulator(spec) = &config.data_source {
        return Some(spec.data_seed);
    }
    #[cfg(not(feature = "simulator"))]
    let _ = config;
    None
}

/// Derive the deterministic `batch_id`: the SHA-256 (first 16 hex chars) of a
/// domain-tagged, length-prefixed fold over the batch params, base config, and
/// strategy — so re-running the same batch reuses the same `batch_<id>/`.
fn derive_batch_id(
    params: &ScenarioParams,
    base_config: &BacktestConfig,
    strategy: &StrategySpec,
) -> Result<String, BacktestError> {
    let mut hasher = Sha256::new();
    hasher.update(BATCH_ID_TAG);
    // The strategy folds in CANONICAL form, exactly as `RunId::derive` hashes it
    // — an explicit leg set written in a different leg order is the same batch.
    let canonical_strategy = strategy.canonical();
    for part in [
        serde_json::to_vec(params),
        serde_json::to_vec(base_config),
        serde_json::to_vec(&canonical_strategy),
    ] {
        let bytes = part
            .map_err(|e| BacktestError::Bundle(format!("batch id preimage serialisation: {e}")))?;
        let len = u64::try_from(bytes.len()).map_err(|_| BacktestError::ArithmeticOverflow)?;
        hasher.update(len.to_le_bytes());
        hasher.update(&bytes);
    }
    let full = to_hex(&hasher.finalize());
    full.get(..16)
        .map(str::to_string)
        .ok_or_else(|| BacktestError::Bundle("batch id hash is too short".to_string()))
}

/// Write the parent index to `<output_dir>/batch_<batch_id>/index.json` as
/// canonical JSON (sorted keys via a `serde_json::Value` round-trip), published
/// via a temp-then-rename so a reader never sees a half-written index.
fn write_batch_index(index: &BatchIndex, output_dir: &Path) -> Result<PathBuf, BacktestError> {
    let dir = output_dir.join(format!("batch_{}", index.batch_id));
    std::fs::create_dir_all(&dir).map_err(|e| batch_err("create batch index directory", &e))?;

    let value = serde_json::to_value(index).map_err(|e| batch_err("index to json value", &e))?;
    let mut json =
        serde_json::to_string_pretty(&value).map_err(|e| batch_err("index to json", &e))?;
    json.push('\n');

    let temp = dir.join(".index.json.partial");
    std::fs::write(&temp, &json).map_err(|e| batch_err("write temp index", &e))?;
    let final_path = dir.join("index.json");
    if let Err(error) = std::fs::rename(&temp, &final_path) {
        let _ = std::fs::remove_file(&temp);
        return Err(batch_err("publish index.json", &error));
    }
    Ok(final_path)
}

/// Wrap a failed serialise / I/O operation as a [`BacktestError::Bundle`].
fn batch_err(context: &str, error: &dyn std::fmt::Display) -> BacktestError {
    BacktestError::Bundle(format!("batch index {context}: {error}"))
}
