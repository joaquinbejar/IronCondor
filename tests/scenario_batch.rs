//! Integration tests for the scenario **batch runner** (#46).
//!
//! A Monte-Carlo batch over a **file** (Parquet) feed — offline, no simulator —
//! fans out into N independent runs, each writing its own verifiable bundle
//! under its deterministic `<run_id>/`, plus a batch parent index. Determinism:
//! two runs of the same batch produce the same per-run engine seeds and the
//! same parent-index structure (file feeds are reproducible; the honest
//! synthetic-data limit is exercised at the unit level in `engine::scenario`).

mod common;

use std::path::Path;

use ironcondor::{
    BacktestConfig, BatchIndex, BatchRunOutcome, ConfigOverride, ResourceLimits, ScenarioParams,
    ScenarioType, child_seed, read_bundle, run_scenario_batch,
};
use optionstratlib::simulation::ExitPolicy;

use common::{condor_config, condor_rows, iron_condor_spec, write_parquet};

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
