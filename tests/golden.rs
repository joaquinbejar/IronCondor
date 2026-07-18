//! The v0.1 golden determinism test — **scoped to the v0.1 artifacts only**.
//!
//! # Scope (binding)
//!
//! v0.1 asserts determinism over the **equity curve + minimal metrics** — the
//! only artifacts the v0.1 slice produces
//! ([docs/ROADMAP.md v0.1 acceptance](../docs/ROADMAP.md#v01--core-engine-and-naive-fills),
//! [milestones/017](../milestones/v0.1-core-engine-naive-fills/017-golden-determinism-test-v01-artifacts.md)).
//! It deliberately does **NOT** assert a `manifest.json` or the four Parquet
//! tables (`fills` / `equity_curve` / `positions` / `greeks_attribution`): the
//! full four-table result bundle and its golden land at **v0.3** (issues #36 /
//! #50). This file must not grow those assertions before then.
//!
//! # What it proves
//!
//! - **Same-environment byte determinism** (the run-twice test): same `(seed,
//!   config, data)` ⇒ the serialised equity-curve bytes and metrics bytes are
//!   byte-identical across two runs
//!   ([docs/02 §7](../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
//! - **Golden equivalence** (the golden test): the produced equity curve +
//!   minimal metrics equal the committed `expected/` under the single
//!   comparison [`oracle`] — money columns exact as integer cents, analytic
//!   floats within the fixed tolerance
//!   ([docs/05 §12.5](../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)).
//!
//! The property-level companion `same_seed_same_result` lives in
//! [`tests/property.rs`](property.rs) (issue #14) and is referenced, not
//! duplicated.
//!
//! # The canonical run
//!
//! `config.json` pins the semantic config (seed, capital, fees, slippage,
//! limits, mode); the strategy is the shared [`common::iron_condor_spec`] and
//! the exit is a non-triggering `TimeSteps` so `on_end` performs the single
//! clean close. The chain is **generated deterministically in-test** from the
//! shared fixture builder ([`common::condor_rows`] / [`common::write_parquet`])
//! rather than committed as a binary blob — the repo convention is "no committed
//! binary; Parquet fixtures are generated into a tempdir" (Cargo.toml
//! dev-deps). `config.json`'s `data_source.path` is a documentary placeholder
//! the test overrides with the generated chain path, so `(seed, config, data)`
//! is fully pinned: config from `config.json`, data from the committed
//! generator.
//!
//! # Regenerating the golden (BLESS)
//!
//! A deliberate behaviour change that alters a produced value fails the golden
//! until `expected/` is regenerated **in the same commit**. Regenerate with:
//!
//! ```text
//! BLESS=1 cargo test --test golden
//! ```
//!
//! which rewrites `expected/equity_curve.json` and `expected/metrics.json` from
//! the current pipeline output (and skips the comparison). Review the diff, then
//! commit it alongside the code change. Without `BLESS`, the test compares and a
//! divergence is a red-flag review finding.

use std::path::{Path, PathBuf};

use ironcondor::{BacktestConfig, DataSourceSpec, EquityPoint, run_backtest};
use optionstratlib::simulation::ExitPolicy;

mod common;
mod oracle;

use oracle::MetricsSummary;

/// Steps in the canonical golden chain — enough to exercise the open, the held
/// mark, and the terminal `on_end` close.
const GOLDEN_STEPS: u32 = 8;

/// Absolute path to the committed golden fixture directory.
fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/iron_condor_naive")
}

/// Whether `BLESS=1` is set — regenerate the committed `expected/` artifacts.
fn bless_enabled() -> bool {
    std::env::var_os("BLESS").is_some_and(|value| value == "1")
}

/// Load the pinned semantic config from `config.json` and repoint its
/// `data_source` at the freshly-generated chain (the path in the file is a
/// documentary placeholder — see the module docs).
fn golden_config(chain_path: &Path) -> BacktestConfig {
    let config_path = fixture_dir().join("config.json");
    let Ok(text) = std::fs::read_to_string(&config_path) else {
        panic!("the golden config.json must be readable at {config_path:?}");
    };
    let Ok(mut config) = serde_json::from_str::<BacktestConfig>(&text) else {
        panic!("config.json must deserialise into a BacktestConfig");
    };
    config.data_source = DataSourceSpec::Parquet {
        path: chain_path.display().to_string(),
        sha256: String::new(),
    };
    config
}

/// Write the deterministic canonical chain into `dir` and return its path.
fn write_golden_chain(dir: &Path) -> PathBuf {
    let chain_path = dir.join("iron_condor.parquet");
    let rows = common::condor_rows(GOLDEN_STEPS, None);
    if common::write_parquet(&chain_path, &rows).is_err() {
        panic!("the canonical golden chain must write");
    }
    chain_path
}

/// Run the canonical golden backtest over `chain_path` and return the produced
/// equity curve plus the extracted minimal metrics.
fn run_golden(chain_path: &Path) -> (Vec<EquityPoint>, MetricsSummary) {
    let config = golden_config(chain_path);
    let spec = common::iron_condor_spec();
    // A non-triggering exit so on_end is the sole closer (matches common::run_condor).
    let Ok(run) = run_backtest(&config, &spec, ExitPolicy::TimeSteps(1_000_000)) else {
        panic!("the canonical golden run must succeed");
    };
    let Ok(metrics) = MetricsSummary::from_result(&run.result) else {
        panic!("the minimal metrics must be extractable from the result");
    };
    (run.equity_curve, metrics)
}

/// Canonical serialisation of the equity curve — pretty JSON with the fixed
/// `EquityPoint` field order, terminated by a newline.
fn serialise_curve(curve: &[EquityPoint]) -> String {
    let Ok(mut json) = serde_json::to_string_pretty(curve) else {
        panic!("the equity curve must serialise");
    };
    json.push('\n');
    json
}

/// Canonical serialisation of the minimal metrics summary.
fn serialise_metrics(metrics: &MetricsSummary) -> String {
    let Ok(mut json) = serde_json::to_string_pretty(metrics) else {
        panic!("the metrics summary must serialise");
    };
    json.push('\n');
    json
}

/// The v0.1 golden: run the canonical fixture and compare the produced equity
/// curve + minimal metrics against the committed `expected/` via the oracle.
/// `BLESS=1` regenerates `expected/` instead of comparing.
#[test]
fn test_golden_iron_condor_naive_matches_committed_v01_artifacts() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let chain_path = write_golden_chain(dir.path());
    let (curve, metrics) = run_golden(&chain_path);

    let expected_dir = fixture_dir().join("expected");
    let curve_path = expected_dir.join("equity_curve.json");
    let metrics_path = expected_dir.join("metrics.json");

    if bless_enabled() {
        if std::fs::write(&curve_path, serialise_curve(&curve)).is_err()
            || std::fs::write(&metrics_path, serialise_metrics(&metrics)).is_err()
        {
            panic!("BLESS must be able to rewrite the expected artifacts");
        }
        // Blessed — regeneration path skips the comparison by design.
        return;
    }

    let Ok(expected_curve_text) = std::fs::read_to_string(&curve_path) else {
        panic!("expected/equity_curve.json must exist — regenerate with BLESS=1");
    };
    let Ok(expected_metrics_text) = std::fs::read_to_string(&metrics_path) else {
        panic!("expected/metrics.json must exist — regenerate with BLESS=1");
    };
    let Ok(expected_curve) = serde_json::from_str::<Vec<EquityPoint>>(&expected_curve_text) else {
        panic!("expected/equity_curve.json must deserialise");
    };
    let Ok(expected_metrics) = serde_json::from_str::<MetricsSummary>(&expected_metrics_text)
    else {
        panic!("expected/metrics.json must deserialise");
    };

    if let Err(diff) = oracle::compare_equity_curves(&curve, &expected_curve) {
        panic!("golden equity-curve divergence: {diff} (regenerate with BLESS=1 if intended)");
    }
    if let Err(diff) = oracle::compare_metrics(&metrics, &expected_metrics) {
        panic!("golden metrics divergence: {diff} (regenerate with BLESS=1 if intended)");
    }
}

/// Same-environment run-twice: same `(seed, config, data)` ⇒ byte-identical
/// serialised equity-curve bytes and identical minimal metrics. This is the
/// byte-determinism half of the contract (docs/02 §7); the golden above is the
/// cross-environment logical-equivalence half.
#[test]
fn test_golden_run_twice_equity_and_metrics_are_byte_identical() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let chain_path = write_golden_chain(dir.path());

    let (curve_a, metrics_a) = run_golden(&chain_path);
    let (curve_b, metrics_b) = run_golden(&chain_path);

    assert_eq!(
        serialise_curve(&curve_a),
        serialise_curve(&curve_b),
        "the serialised equity curve must be byte-identical across two runs"
    );
    assert_eq!(
        serialise_metrics(&metrics_a),
        serialise_metrics(&metrics_b),
        "the serialised minimal metrics must be byte-identical across two runs"
    );
    // The decoded metrics are identical too (money exact, Options aligned).
    assert_eq!(metrics_a, metrics_b);
}

/// A deliberate-change guard, disabled by default: mutating a produced value
/// (here, dropping the last equity point) must make the oracle report a
/// divergence — proving the golden is not vacuous. The real guard is that
/// `expected/` is committed and compared on every CI `golden` run; this makes
/// the "fails on divergence" property directly executable.
#[test]
#[ignore = "demonstrates the golden catches a divergence; the committed expected/ is the real guard"]
fn test_golden_detects_a_deliberate_change() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let chain_path = write_golden_chain(dir.path());
    let (curve, _metrics) = run_golden(&chain_path);

    let mut mutated = curve.clone();
    mutated.pop();
    assert!(
        oracle::compare_equity_curves(&mutated, &curve).is_err(),
        "a dropped equity point must be caught by the oracle"
    );
}
