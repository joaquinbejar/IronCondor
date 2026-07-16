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
//! # The realistic-mode golden (feature `orderbook`, #24)
//!
//! The nested [`realistic`] module adds a **second** golden of the **same v0.1
//! scope** (equity curve + minimal metrics, the same comparison oracle and
//! `BLESS` path) for realistic mode: the run is assembled directly as
//! `ParquetFeed` + [`ironcondor::RealisticFill`] (seeded from
//! `config.liquidity_profile`) + the `IronCondor` adapter →
//! `BacktestEngine::run` → `metrics::populate`, over the **same** canonical
//! chain as the naive golden. Because entries and exits route through the
//! seeded order book they cross the spread, so the realistic curve diverges
//! from (is worse than) the naive one — the fill-risk signal the mode exists to
//! surface. It is gated behind the `orderbook` feature; run it with
//! `cargo test --test golden --features orderbook`.
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

/// The realistic-mode golden (feature `orderbook`, #24): the same v0.1 scope
/// (equity curve + minimal metrics) as the naive golden above, but the run is
/// assembled with [`ironcondor::RealisticFill`] so entries and exits route
/// through the seeded `option-chain-orderbook` book. See the module docs.
#[cfg(feature = "orderbook")]
mod realistic {
    use std::path::{Path, PathBuf};

    use ironcondor::analytics::metrics;
    use ironcondor::{
        BacktestConfig, BacktestEngine, DataSourceSpec, EquityPoint, OptStratAdapter, ParquetFeed,
        RealisticFill, ResourceLimits,
    };
    use optionstratlib::simulation::ExitPolicy;
    use optionstratlib::strategies::IronCondor;

    use super::oracle::MetricsSummary;
    use super::{GOLDEN_STEPS, bless_enabled, common, oracle, serialise_curve, serialise_metrics};

    /// Absolute path to the committed realistic golden fixture directory.
    fn fixture_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/iron_condor_realistic")
    }

    /// Load the pinned realistic `config.json` (mode = realistic, with the
    /// marketable cap + liquidity profile) and repoint its data source at the
    /// freshly-generated chain (the file path is a documentary placeholder).
    fn golden_config(chain_path: &Path) -> BacktestConfig {
        let config_path = fixture_dir().join("config.json");
        let Ok(text) = std::fs::read_to_string(&config_path) else {
            panic!("the realistic golden config.json must be readable at {config_path:?}");
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

    /// Write the **same** deterministic canonical chain the naive golden uses.
    fn write_chain(dir: &Path) -> PathBuf {
        let chain_path = dir.join("iron_condor.parquet");
        let rows = common::condor_rows(GOLDEN_STEPS, None);
        if common::write_parquet(&chain_path, &rows).is_err() {
            panic!("the canonical golden chain must write");
        }
        chain_path
    }

    /// Assemble the realistic run **directly**: a `ParquetFeed`, a
    /// `RealisticFill::with_liquidity_profile` seeded from
    /// `config.liquidity_profile`, and the `IronCondor` adapter, driven through
    /// `BacktestEngine::run` and then `metrics::populate`, returning the equity
    /// curve + minimal metrics. `run_backtest` hardcodes naive, so the realistic
    /// run is composed here.
    fn run_golden(chain_path: &Path) -> (Vec<EquityPoint>, MetricsSummary) {
        let config = golden_config(chain_path);
        let Ok(feed) = ParquetFeed::open(chain_path, &ResourceLimits::default()) else {
            panic!("the canonical golden chain must open");
        };
        // A non-triggering exit so on_end is the sole closer (matches naive).
        let exit = ExitPolicy::TimeSteps(1_000_000);
        let Ok(adapter) =
            OptStratAdapter::<IronCondor>::from_spec(&common::iron_condor_spec(), exit)
        else {
            panic!("the iron condor adapter must build");
        };
        let execution = RealisticFill::with_liquidity_profile(
            config.fees,
            config.marketable_cap_ticks,
            config.seed,
            config.liquidity_profile,
        );
        let Ok(mut run) = BacktestEngine::run(&config, feed, execution, adapter, "iron_condor")
        else {
            panic!("the canonical realistic golden run must succeed");
        };
        let Ok(initial) = i64::try_from(config.initial_capital) else {
            panic!("initial capital must fit i64 cents");
        };
        if metrics::populate(&mut run.result, &run.equity_curve, initial).is_err() {
            panic!("the minimal metrics must populate the realistic result");
        }
        let Ok(metrics) = MetricsSummary::from_result(&run.result) else {
            panic!("the minimal metrics must be extractable from the result");
        };
        (run.equity_curve, metrics)
    }

    /// The realistic golden: run the canonical fixture in realistic mode and
    /// compare the produced equity curve + minimal metrics against the committed
    /// `expected/` via the shared oracle. `BLESS=1` regenerates instead.
    #[test]
    fn test_golden_iron_condor_realistic_matches_committed_v01_artifacts() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("a tempdir for the generated chain must create");
        };
        let chain_path = write_chain(dir.path());
        let (curve, metrics) = run_golden(&chain_path);

        let expected_dir = fixture_dir().join("expected");
        let curve_path = expected_dir.join("equity_curve.json");
        let metrics_path = expected_dir.join("metrics.json");

        if bless_enabled() {
            if std::fs::write(&curve_path, serialise_curve(&curve)).is_err()
                || std::fs::write(&metrics_path, serialise_metrics(&metrics)).is_err()
            {
                panic!("BLESS must be able to rewrite the expected realistic artifacts");
            }
            return;
        }

        let Ok(expected_curve_text) = std::fs::read_to_string(&curve_path) else {
            panic!("expected/equity_curve.json must exist — regenerate with BLESS=1");
        };
        let Ok(expected_metrics_text) = std::fs::read_to_string(&metrics_path) else {
            panic!("expected/metrics.json must exist — regenerate with BLESS=1");
        };
        let Ok(expected_curve) = serde_json::from_str::<Vec<EquityPoint>>(&expected_curve_text)
        else {
            panic!("expected/equity_curve.json must deserialise");
        };
        let Ok(expected_metrics) = serde_json::from_str::<MetricsSummary>(&expected_metrics_text)
        else {
            panic!("expected/metrics.json must deserialise");
        };

        if let Err(diff) = oracle::compare_equity_curves(&curve, &expected_curve) {
            panic!(
                "realistic golden equity-curve divergence: {diff} (regenerate with BLESS=1 if intended)"
            );
        }
        if let Err(diff) = oracle::compare_metrics(&metrics, &expected_metrics) {
            panic!(
                "realistic golden metrics divergence: {diff} (regenerate with BLESS=1 if intended)"
            );
        }
    }

    /// Same-environment run-twice: same `(seed, config, data)` in realistic mode
    /// ⇒ byte-identical serialised equity curve and metrics across two runs.
    #[test]
    fn test_golden_realistic_run_twice_equity_and_metrics_are_byte_identical() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("a tempdir for the generated chain must create");
        };
        let chain_path = write_chain(dir.path());

        let (curve_a, metrics_a) = run_golden(&chain_path);
        let (curve_b, metrics_b) = run_golden(&chain_path);

        assert_eq!(
            serialise_curve(&curve_a),
            serialise_curve(&curve_b),
            "the realistic equity curve must be byte-identical across two runs"
        );
        assert_eq!(
            serialise_metrics(&metrics_a),
            serialise_metrics(&metrics_b),
            "the realistic minimal metrics must be byte-identical across two runs"
        );
        assert_eq!(metrics_a, metrics_b);
    }

    /// The realistic curve must **diverge from** the naive golden's committed
    /// curve — entries and exits cross the seeded spread, so realistic mode is
    /// worse (the fill-risk signal the mode exists to surface). Guards against a
    /// silent collapse of realistic mode into naive.
    #[test]
    fn test_realistic_golden_diverges_from_naive_golden() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("a tempdir for the generated chain must create");
        };
        let chain_path = write_chain(dir.path());
        let (realistic_curve, _metrics) = run_golden(&chain_path);

        let naive_curve_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/iron_condor_naive/expected/equity_curve.json");
        let Ok(naive_text) = std::fs::read_to_string(&naive_curve_path) else {
            panic!("the committed naive golden curve must be readable");
        };
        let Ok(naive_curve) = serde_json::from_str::<Vec<EquityPoint>>(&naive_text) else {
            panic!("the naive golden curve must deserialise");
        };
        assert_eq!(realistic_curve.len(), naive_curve.len());
        // At least one step's equity must differ (realistic pays the spread).
        assert!(
            realistic_curve
                .iter()
                .zip(naive_curve.iter())
                .any(|(r, n)| r.equity_cents != n.equity_cents),
            "realistic mode must diverge from naive — it collapsed into naive fills"
        );
        // And realistic terminal equity must be no better than naive (worse or
        // equal — it crossed the spread on both open and close).
        let (Some(r_last), Some(n_last)) = (realistic_curve.last(), naive_curve.last()) else {
            panic!("both curves have a terminal point");
        };
        assert!(
            r_last.equity_cents <= n_last.equity_cents,
            "realistic terminal equity {} must be <= naive {}",
            r_last.equity_cents,
            n_last.equity_cents
        );
    }
}
