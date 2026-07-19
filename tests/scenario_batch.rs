//! Integration tests for the scenario **batch runner** (#46).
//!
//! A Monte-Carlo batch over a **file** (Parquet) feed — offline, no simulator —
//! fans out into N independent runs, each writing its own verifiable bundle
//! under its deterministic `<run_id>/`, plus a batch parent index. Determinism:
//! two runs of the same batch produce the same per-run engine seeds and the
//! same parent-index structure (file feeds are reproducible; the honest
//! synthetic-data limit is exercised at the unit level in `engine::scenario`).

mod common;
mod oracle;

use std::path::Path;

use ironcondor::{
    BacktestConfig, BatchIndex, BatchRunEntry, BatchRunOutcome, ConfigOverride, DataSourceSpec,
    ResourceLimits, ScenarioParams, ScenarioType, child_seed, read_bundle, run_backtest,
    run_scenario_batch, write_bundle,
};
use optionstratlib::simulation::ExitPolicy;

use common::{condor_config, condor_rows, iron_condor_spec, write_parquet};

/// The four Parquet tables in a result bundle (the manifest is compared
/// separately, canonicalised).
const TABLE_FILES: [&str; 4] = [
    "fills.parquet",
    "equity_curve.parquet",
    "positions.parquet",
    "greeks_attribution.parquet",
];

/// Extract an `Ok` run's `(run_id, bundle_path)`, panicking on a recorded error.
fn ok_entry(entry: &BatchRunEntry) -> (&str, &Path) {
    match &entry.outcome {
        BatchRunOutcome::Ok {
            run_id,
            bundle_path,
            ..
        } => (run_id.as_str(), Path::new(bundle_path)),
        BatchRunOutcome::Error { error } => panic!("expected an ok run, got error: {error}"),
    }
}

/// Assert two bundles' four Parquet tables are **byte-identical** (equivalently,
/// equal `sha256`) and their manifests identical after stripping `created_utc`
/// and canonicalising the operational `output_dir` — the same-environment
/// byte-identity contract ([docs/02 §7](../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
fn assert_bundles_byte_identical(dir_a: &Path, dir_b: &Path) {
    for name in TABLE_FILES {
        let (Ok(a), Ok(b)) = (
            std::fs::read(dir_a.join(name)),
            std::fs::read(dir_b.join(name)),
        ) else {
            panic!("{name} must be readable from both bundles");
        };
        assert_eq!(
            a, b,
            "{name} must be byte-identical (equivalently, equal sha256)"
        );
    }
    let ma = oracle::canonical_manifest(&oracle::read_manifest_json(dir_a));
    let mb = oracle::canonical_manifest(&oracle::read_manifest_json(dir_b));
    assert_eq!(
        ma, mb,
        "manifest byte-identical after stripping created_utc + output_dir"
    );
}

/// Assert two bundles are **logically equal** under the single comparison oracle
/// (decode → sort by each table's key → exact integer cents, tolerance on the one
/// analytic float; manifest as canonical JSON, `created_utc` / `metrics`
/// excluded) — the cross-environment layer reused from `tests/oracle`.
fn assert_bundles_oracle_equal(dir_a: &Path, dir_b: &Path) {
    let limits = ResourceLimits::default();
    let (Ok(a), Ok(b)) = (read_bundle(dir_a, &limits), read_bundle(dir_b, &limits)) else {
        panic!("both bundles must read back through read_bundle");
    };
    if let Err(diff) = oracle::compare_bundle_tables(&a, &b) {
        panic!("bundle tables diverge under the oracle: {diff}");
    }
    if let Err(diff) = oracle::compare_manifest_json(
        &oracle::read_manifest_json(dir_a),
        &oracle::read_manifest_json(dir_b),
    ) {
        panic!("bundle manifests diverge under the oracle: {diff}");
    }
}

/// A batch base config over `parquet_path`, writing bundles into `out_dir`, with
/// `overwrite` set so a re-run of the same batch replaces the same `<run_id>`s.
fn batch_base(parquet_path: &Path, out_dir: &Path) -> BacktestConfig {
    let mut config = condor_config(parquet_path, 0);
    config.output_dir = out_dir.to_path_buf();
    config.overwrite = true;
    config
}

/// A non-triggering exit policy: `on_end` performs the single clean close.
fn exit() -> ExitPolicy {
    ExitPolicy::TimeSteps(1_000_000)
}

#[test]
fn test_batch_over_parquet_fans_out_to_verifiable_bundles_and_indexes_each() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let parquet = dir.path().join("condor.parquet");
    if let Err(e) = write_parquet(&parquet, &condor_rows(6, None)) {
        panic!("write parquet fixture: {e}");
    }
    let out = dir.path().join("out");
    let base = batch_base(&parquet, &out);

    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 42,
        count: 3,
        sweep: Vec::new(),
    };
    let index = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("the batch must run: {e}"),
    };

    // The index records all three runs, ordered by index, with the derived seeds.
    assert_eq!(index.run_count, 3);
    assert_eq!(index.runs.len(), 3);
    assert_eq!(index.base_seed, 42);
    let mut run_ids = Vec::new();
    for (i, entry) in index.runs.iter().enumerate() {
        let i = u32::try_from(i).unwrap_or(u32::MAX);
        assert_eq!(entry.index, i, "runs are ordered by index");
        assert_eq!(
            entry.engine_seed,
            child_seed(42, i),
            "engine seed is child_seed"
        );
        assert_eq!(entry.data_seed, None, "a file feed has no data seed");
        match &entry.outcome {
            BatchRunOutcome::Ok {
                run_id,
                bundle_path,
                ..
            } => {
                // Every run's bundle is on disk and verifies through the reader.
                let path = Path::new(bundle_path);
                assert!(path.is_dir(), "bundle dir {bundle_path} must exist");
                if let Err(e) = read_bundle(path, &ResourceLimits::default()) {
                    panic!("bundle {run_id} must verify: {e}");
                }
                run_ids.push(run_id.clone());
            }
            BatchRunOutcome::Error { error } => panic!("run {i} failed: {error}"),
        }
    }

    // Distinct seeds ⇒ distinct run_ids ⇒ distinct bundle directories.
    run_ids.sort();
    run_ids.dedup();
    assert_eq!(run_ids.len(), 3, "each run has a distinct run_id");

    // The parent index file is published and round-trips.
    let index_path = out
        .join(format!("batch_{}", index.batch_id))
        .join("index.json");
    assert!(index_path.is_file(), "index.json must be published");
    let Ok(bytes) = std::fs::read(&index_path) else {
        panic!("index.json must be readable");
    };
    let parsed: Result<BatchIndex, _> = serde_json::from_slice(&bytes);
    match parsed {
        Ok(round_tripped) => assert_eq!(round_tripped, index, "index round-trips"),
        Err(e) => panic!("index.json must deserialise: {e}"),
    }
}

#[test]
fn test_batch_is_reproducible_over_file_feeds() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let parquet = dir.path().join("condor.parquet");
    if let Err(e) = write_parquet(&parquet, &condor_rows(5, None)) {
        panic!("write parquet fixture: {e}");
    }
    let out = dir.path().join("out");
    let base = batch_base(&parquet, &out);

    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 2024,
        count: 4,
        sweep: Vec::new(),
    };

    // Two runs of the same batch into the same output dir (overwrite = true).
    let first = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("first batch: {e}"),
    };
    let second = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("second batch: {e}"),
    };

    // Same batch_id, same per-run engine seeds, same run_ids, same terminal
    // equity — a batch over file feeds reproduces byte-for-byte.
    assert_eq!(first, second, "a file-feed batch reproduces identically");
    for (i, entry) in first.runs.iter().enumerate() {
        let i = u32::try_from(i).unwrap_or(u32::MAX);
        assert_eq!(entry.engine_seed, child_seed(2024, i));
    }
}

#[test]
fn test_batch_sweep_cardinality_is_sweep_times_count() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let parquet = dir.path().join("condor.parquet");
    if let Err(e) = write_parquet(&parquet, &condor_rows(4, None)) {
        panic!("write parquet fixture: {e}");
    }
    let out = dir.path().join("out");
    let base = batch_base(&parquet, &out);

    // Two sweep entries × count 2 = four runs. (Both entries carry no shock; a
    // file feed ignores shocks — only the seeds distinguish the runs.)
    let params = ScenarioParams {
        kind: ScenarioType::StressTest,
        base_seed: 5,
        count: 2,
        sweep: vec![ConfigOverride::default(), ConfigOverride::default()],
    };
    let index = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("the sweep batch must run: {e}"),
    };
    assert_eq!(index.run_count, 4, "sweep.len() (2) x count (2)");
    for entry in &index.runs {
        assert!(matches!(entry.outcome, BatchRunOutcome::Ok { .. }));
    }
}

#[test]
fn test_batch_records_failures_without_aborting() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    // Point the base config at a Parquet file that does not exist: every run's
    // feed open fails, and each failure is RECORDED (not propagated) so the
    // batch still produces a full index.
    let missing = dir.path().join("does-not-exist.parquet");
    let out = dir.path().join("out");
    let base = batch_base(&missing, &out);

    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 1,
        count: 2,
        sweep: Vec::new(),
    };
    let index = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("a batch of failing runs must still return an index: {e}"),
    };
    assert_eq!(index.run_count, 2);
    for entry in &index.runs {
        assert!(
            matches!(entry.outcome, BatchRunOutcome::Error { .. }),
            "a missing feed is a recorded per-run error, not an abort"
        );
    }
    // The parent index is still published for the failed batch.
    let index_path = out
        .join(format!("batch_{}", index.batch_id))
        .join("index.json");
    assert!(
        index_path.is_file(),
        "the index is published even for failures"
    );
}

/// The **headline** determinism assertion of #47: a file-feed batch re-run in the
/// same environment yields, for every run, four Parquet tables that are
/// byte-identical and a manifest byte-identical after stripping `created_utc`,
/// and the pair is logically equal under the reused comparison oracle. This is
/// stronger than `test_batch_is_reproducible_over_file_feeds` (which asserts
/// parent-index equality): it reaches into each run's on-disk bundle bytes. The
/// two batches write into distinct output dirs (excluded from `run_id`), so both
/// sets of bundles persist for comparison.
#[test]
fn test_file_feed_batch_bundles_byte_identical_across_two_runs() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let parquet = dir.path().join("condor.parquet");
    if let Err(e) = write_parquet(&parquet, &condor_rows(6, None)) {
        panic!("write parquet fixture: {e}");
    }
    let out_a = dir.path().join("out_a");
    let out_b = dir.path().join("out_b");
    let base_a = batch_base(&parquet, &out_a);
    let base_b = batch_base(&parquet, &out_b);

    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 909,
        count: 3,
        sweep: Vec::new(),
    };
    let first = match run_scenario_batch(&params, &base_a, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("first batch: {e}"),
    };
    let second = match run_scenario_batch(&params, &base_b, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("second batch: {e}"),
    };

    assert_eq!(first.run_count, 3);
    assert_eq!(first.run_count, second.run_count);
    // Note: `batch_id` hashes the whole base config (output_dir included), so the
    // two batches get distinct `batch_id`s; the per-run `run_id` excludes
    // output_dir, which is what makes the bundle bytes reproducible below.
    for (a, b) in first.runs.iter().zip(second.runs.iter()) {
        assert_eq!(a.index, b.index, "runs align by index");
        assert_eq!(a.engine_seed, b.engine_seed, "same derived engine seed");
        let (rid_a, path_a) = ok_entry(a);
        let (rid_b, path_b) = ok_entry(b);
        assert_eq!(rid_a, rid_b, "same run_id across the two batch runs");
        assert_bundles_byte_identical(path_a, path_b);
        assert_bundles_oracle_equal(path_a, path_b);
    }
}

/// The cache-bypass equivalence guard at the **bundle** level: a batch run over
/// the shared parse cache (parsed once, `Arc`-shared) produces a bundle
/// byte-identical to — and logically equal under the oracle with — a per-run
/// parse of the SAME file via `run_backtest` (which opens its own `ParquetFeed`,
/// never the cache). Same derived seed ⇒ same `run_id` (output_dir excluded), so
/// the two bundles must match table-for-table. Proves the cache never changes
/// results ([docs/03 §8](../docs/03-data-layer.md#8-caching)).
#[test]
fn test_batch_shared_cache_bundle_equals_per_run_parse_bypass() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let parquet = dir.path().join("condor.parquet");
    if let Err(e) = write_parquet(&parquet, &condor_rows(5, None)) {
        panic!("write parquet fixture: {e}");
    }
    let out_batch = dir.path().join("out_batch");
    let out_bypass = dir.path().join("out_bypass");
    let base = batch_base(&parquet, &out_batch);

    // A single-run batch — run 0 reads the shared cache (cache on).
    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 777,
        count: 1,
        sweep: Vec::new(),
    };
    let index = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("cache batch: {e}"),
    };
    assert_eq!(index.run_count, 1);
    let Some(entry0) = index.runs.first() else {
        panic!("the batch has one run");
    };
    let (rid_cache, path_cache) = ok_entry(entry0);

    // The bypass: `run_backtest` opens a per-run `ParquetFeed` (no cache) over the
    // same file, with run 0's derived engine seed, then writes its bundle.
    let mut bypass = batch_base(&parquet, &out_bypass);
    bypass.seed = child_seed(777, 0);
    let run = match run_backtest(&bypass, &iron_condor_spec(), exit()) {
        Ok(run) => run,
        Err(e) => panic!("bypass run: {e}"),
    };
    let path_bypass = match write_bundle(&run, &bypass, &iron_condor_spec()) {
        Ok(path) => path,
        Err(e) => panic!("bypass write_bundle: {e}"),
    };

    // Same run_id (output_dir is excluded from the hash) ⇒ identical bundle bytes.
    assert_eq!(
        path_cache.file_name(),
        path_bypass.file_name(),
        "cache and bypass derive the same run_id"
    );
    assert_eq!(
        rid_cache,
        path_bypass
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
    );
    assert_bundles_byte_identical(path_cache, &path_bypass);
    assert_bundles_oracle_equal(path_cache, &path_bypass);
}

/// A run whose recorded `sha256` no longer matches the file bytes fails the
/// re-read with a typed error recorded in its index entry — never a silent
/// divergent run. The batch pins a wrong (but well-formed) hash; both the shared
/// pre-materialisation and the per-run fallback verify it, so every run records
/// the typed mismatch and the batch still publishes a full index.
#[test]
fn test_batch_tampered_sha256_is_recorded_typed_error() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let parquet = dir.path().join("condor.parquet");
    if let Err(e) = write_parquet(&parquet, &condor_rows(4, None)) {
        panic!("write parquet fixture: {e}");
    }
    let out = dir.path().join("out");
    let mut base = batch_base(&parquet, &out);
    // Pin a wrong-but-valid-length sha256 so the re-read verification fails typed.
    base.data_source = DataSourceSpec::Parquet {
        path: parquet.display().to_string(),
        sha256: "0".repeat(64),
    };

    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 1,
        count: 2,
        sweep: Vec::new(),
    };
    let index = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("a batch with a bad sha must still return an index: {e}"),
    };
    assert_eq!(index.run_count, 2);
    for entry in &index.runs {
        match &entry.outcome {
            BatchRunOutcome::Error { error } => assert!(
                error.contains("sha256"),
                "a tampered sha must be a typed sha256 mismatch, got: {error}"
            ),
            BatchRunOutcome::Ok { .. } => {
                panic!("a tampered sha must be a recorded error, not a silent run")
            }
        }
    }
    // #110: the materialise failure is cached per path as an error DESCRIPTOR,
    // so every run sharing the path records the IDENTICAL rendered error (one
    // parse of the bad file, not one per worker).
    let descriptors: Vec<&String> = index
        .runs
        .iter()
        .map(|entry| match &entry.outcome {
            BatchRunOutcome::Error { error } => error,
            BatchRunOutcome::Ok { .. } => panic!("all outcomes are errors here"),
        })
        .collect();
    let (Some(first), Some(second)) = (descriptors.first(), descriptors.get(1)) else {
        panic!("two run outcomes");
    };
    assert_eq!(
        first, second,
        "both runs record the identical cached descriptor"
    );
}

/// #110: the recorded outcome error is the MATERIALISE-TIME descriptor — the
/// sha256-mismatch message cached when the batch pre-materialised the path —
/// not a per-run re-read artifact, and it survives the `Data` re-wrap intact.
/// (True no-reopen is enforced by `open_and_run`'s cache-hit arm, which
/// returns the descriptor before any filesystem call; the delete-between-
/// materialise-and-fan-out control is not injectable through the public batch
/// API, so this pins the observable half.)
#[test]
fn test_batch_cached_descriptor_replays_without_reopening() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let parquet = dir.path().join("condor.parquet");
    if let Err(e) = write_parquet(&parquet, &condor_rows(4, None)) {
        panic!("write parquet fixture: {e}");
    }
    let out = dir.path().join("out");
    let mut base = batch_base(&parquet, &out);
    base.data_source = DataSourceSpec::Parquet {
        path: parquet.display().to_string(),
        sha256: "0".repeat(64),
    };
    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 1,
        count: 2,
        sweep: Vec::new(),
    };
    let index = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("a batch with a bad sha must still return an index: {e}"),
    };
    for entry in &index.runs {
        let BatchRunOutcome::Error { error } = &entry.outcome else {
            panic!("a tampered sha must be a recorded error");
        };
        assert!(
            error.contains("sha256"),
            "the cached descriptor carries the original sha256-mismatch message: {error}"
        );
        assert!(
            !error.contains("No such file"),
            "the error is the materialise-time descriptor, not a re-read artifact"
        );
    }
}

/// A run whose recorded file is missing fails the re-read with a typed error
/// recorded in its index entry (the sibling of the tampered-sha case, on the I/O
/// arm) — the batch does not abort and still publishes an index.
#[test]
fn test_batch_missing_file_is_recorded_typed_error() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let missing = dir.path().join("absent.parquet");
    let out = dir.path().join("out");
    let mut base = batch_base(&missing, &out);
    // Pin a (necessarily wrong) sha alongside the missing path — the I/O failure
    // must fire before any hash check.
    base.data_source = DataSourceSpec::Parquet {
        path: missing.display().to_string(),
        sha256: "f".repeat(64),
    };

    let params = ScenarioParams {
        kind: ScenarioType::MonteCarlo,
        base_seed: 3,
        count: 2,
        sweep: Vec::new(),
    };
    let index = match run_scenario_batch(&params, &base, &iron_condor_spec(), &exit(), None) {
        Ok(index) => index,
        Err(e) => panic!("a batch over a missing file must still return an index: {e}"),
    };
    assert_eq!(index.run_count, 2);
    for entry in &index.runs {
        assert!(
            matches!(entry.outcome, BatchRunOutcome::Error { .. }),
            "a missing file is a recorded typed error, not an abort"
        );
    }
}
