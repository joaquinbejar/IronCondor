//! The **v0.3 golden result bundle** — the frozen `ironcondor.bundle.v1` wire
//! contract, made executable (#36).
//!
//! # What this pins
//!
//! A canonical `(config, data, seed)` naive iron-condor run is written to a
//! **frozen bundle** — `manifest.json` + the four Parquet tables (`fills`,
//! `equity_curve`, `positions`, `greeks_attribution`) — under
//! [`tests/golden/iron_condor_naive/expected/`](../tests/golden/iron_condor_naive).
//! Two tests hold the freeze:
//!
//! - **`write → read → equal`** ([`test_bundle_golden_iron_condor_naive_write_read_equal`]):
//!   run → [`write_bundle`](ironcondor::write_bundle) →
//!   [`read_bundle`](ironcondor::read_bundle) → assert the decoded bundle equals
//!   the committed golden under the **single comparison oracle**
//!   ([`oracle`], [docs/05 §12.5](../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)):
//!   decode, sort by each table's pinned key, compare integer cents **exactly**
//!   and the one float column (`drawdown`) within the tolerance, and compare the
//!   manifest as canonical JSON with `created_utc` excluded. A value-changing
//!   engine / attribution / schema change that leaves the golden untouched
//!   **fails CI**.
//! - **run-twice byte-identical** ([`test_bundle_run_twice_is_byte_identical`]):
//!   same environment ⇒ the four Parquet tables are **byte-for-byte** identical
//!   and `manifest.json` is byte-identical after stripping `created_utc`
//!   ([docs/05 §11](../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism)).
//!
//! # Why the bundle golden is stable across commits (the #36 freeze pins)
//!
//! The bundle's `run_id` (the directory name) and `manifest.json` must be
//! **byte-stable across commits and platforms** so the golden can be frozen:
//!
//! - **Build identity = `code_version` + `lockfile_sha256`, never a git sha**
//!   ([docs/01 §10](../docs/01-domain-model.md#10-run-identity-and-manifest)): a
//!   per-commit git sha would change the `run_id` every commit. `code_version`
//!   is the crate version (`CARGO_PKG_VERSION`) and changes only on a release
//!   bump (which regenerates goldens anyway); `lockfile_sha256` changes only on
//!   a dependency change (ditto). Both are file-content hashes → platform-stable.
//! - **The data-source identity is pinned to a canonical placeholder.** The
//!   chain is **generated in-test** from the committed [`common`] builder into a
//!   tempdir (the repo convention is "no committed binary chain"), so its file
//!   path is random and its Parquet bytes could differ across `arrow`/`parquet`
//!   serialisations. The golden run therefore overrides the run's data-source
//!   provenance and tape identity to fixed canonical constants
//!   ([`CANONICAL_DATA_PATH`] / [`CANONICAL_DATA_IDENTITY`]) — the chain's
//!   *identity* for the golden is the committed generator, not the random bytes.
//!   This decouples the `run_id` and the manifest from any chain-serialisation
//!   or platform variance while keeping the four tables (the substantive P&L)
//!   fully exercised.
//! - **The operational output path is non-semantic.** `config.output_dir` is the
//!   tempdir write target; it is excluded from the semantic `run_id`
//!   ([docs/05 §11](../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism))
//!   and is canonicalised alongside `created_utc` in the manifest comparison
//!   ([`oracle::canonical_manifest`]).
//!
//! # Regenerating the golden (BLESS)
//!
//! A deliberate value-changing change re-blesses the golden **in the same
//! commit**:
//!
//! ```text
//! BLESS=1 cargo test --test bundle_golden
//! ```
//!
//! which copies the four produced Parquet tables into `expected/` and writes a
//! **normalised** `manifest.json` (canonical `created_utc` + canonical
//! `output_dir`, so the committed golden is clean and reproducible-looking — the
//! comparison strips both anyway). Review the diff, then commit it alongside the
//! code change.

use std::path::{Path, PathBuf};

use ironcondor::analytics::attribution::attribute;
use ironcondor::analytics::metrics;
use ironcondor::{
    BacktestConfig, BacktestEngine, BacktestRun, DataSourceSpec, ExecutionMode, FeeSchedule,
    LiquidityProfile, NaiveFill, OptStratAdapter, ParquetFeed, ResourceLimits, SlippageModel,
    StrategySpec, read_bundle, write_bundle,
};
use optionstratlib::simulation::ExitPolicy;
use optionstratlib::strategies::IronCondor;

mod common;
mod oracle;

/// Steps in the canonical golden chain — enough to exercise the open, the held
/// mark, and the terminal `on_end` close of every leg.
const GOLDEN_STEPS: u32 = 8;

/// The canonical (documentary) data-source path recorded in the golden manifest
/// — a placeholder, not the tempdir the chain is generated into (see the module
/// docs).
const CANONICAL_DATA_PATH: &str = "iron_condor.parquet";

/// The canonical tape identity hashed into the golden `run_id` — pinned so the
/// `run_id` is independent of the generated chain's Parquet bytes / platform.
const CANONICAL_DATA_IDENTITY: &str = "golden:iron_condor_naive:ironcondor.bundle.v1";

/// The canonical `created_utc` written into the committed golden manifest (the
/// comparison strips `created_utc`, so any fixed value works; a clean epoch
/// timestamp keeps the committed file reproducible-looking).
const CANONICAL_CREATED_UTC: &str = "1970-01-01T00:00:00+00:00";

/// The canonical operational output path written into the committed golden
/// manifest (the comparison canonicalises `config.output_dir`, so the real
/// tempdir never reaches the frozen file).
const CANONICAL_OUTPUT_DIR: &str = "golden-out";

/// The four Parquet tables of the bundle, in a fixed order.
const TABLE_FILES: [&str; 4] = [
    "fills.parquet",
    "equity_curve.parquet",
    "positions.parquet",
    "greeks_attribution.parquet",
];

/// Absolute path to the committed golden bundle directory.
fn expected_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/iron_condor_naive/expected")
}

/// Whether `BLESS=1` is set — regenerate the committed golden bundle.
fn bless_enabled() -> bool {
    std::env::var_os("BLESS").is_some_and(|value| value == "1")
}

/// The canonical data-source provenance recorded in the golden manifest (an
/// unpinned `sha256`, so `read_bundle` never tries to re-hash the absent chain).
fn canonical_data_source() -> DataSourceSpec {
    DataSourceSpec::Parquet {
        path: CANONICAL_DATA_PATH.to_string(),
        sha256: String::new(),
    }
}

/// The pinned semantic config for the canonical golden run (naive iron condor).
fn golden_config(output_dir: &Path) -> BacktestConfig {
    BacktestConfig {
        data_source: canonical_data_source(),
        mode: ExecutionMode::Naive,
        seed: 42,
        initial_capital: 10_000_000,
        fees: FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        slippage: SlippageModel::None,
        marketable_cap_ticks: 10,
        liquidity_profile: LiquidityProfile::default(),
        limits: ResourceLimits::default(),
        output_dir: output_dir.to_path_buf(),
        overwrite: false,
    }
}

/// Assemble and run the canonical naive iron-condor bundle run: generate the
/// deterministic chain into `chain_dir`, drive `ParquetFeed → BacktestEngine::run
/// → metrics::populate → attribution::attribute`, and pin the data-source
/// identity to the canonical constants so the `run_id` + manifest are stable
/// (see the module docs). Returns the config + strategy spec + the completed run.
fn build_golden_run(
    chain_dir: &Path,
    output_dir: &Path,
) -> (BacktestConfig, StrategySpec, BacktestRun) {
    let chain_path = chain_dir.join("iron_condor.parquet");
    let rows = common::condor_rows(GOLDEN_STEPS, None);
    if common::write_parquet(&chain_path, &rows).is_err() {
        panic!("the canonical golden chain must write");
    }
    let Ok(feed) = ParquetFeed::open(&chain_path, &ResourceLimits::default()) else {
        panic!("the canonical golden chain must open");
    };
    let config = golden_config(output_dir);
    let spec = common::iron_condor_spec();
    // A non-triggering exit so on_end is the sole closer (matches the v0.1 golden).
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let Ok(adapter) = OptStratAdapter::<IronCondor>::from_spec(&spec, exit) else {
        panic!("the iron condor adapter must build");
    };
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    let Ok(mut run) = BacktestEngine::run(&config, feed, execution, adapter, "iron_condor") else {
        panic!("the canonical golden run must succeed");
    };
    let Ok(initial) = i64::try_from(config.initial_capital) else {
        panic!("initial capital must fit i64 cents");
    };
    if metrics::populate(
        &mut run.result,
        &run.equity_curve,
        initial,
        &run.trade_log,
        &run.open_at_end,
    )
    .is_err()
    {
        panic!("the summary metrics must populate the golden result");
    }
    let Ok(rows) = attribute(&run.attribution_substrate) else {
        panic!("the P&L attribution must decompose the golden run");
    };
    run.greeks_attribution = rows;
    // Pin the data-source provenance + tape identity to canonical constants so
    // the run_id (directory name) + manifest are independent of the tempdir
    // chain path and its Parquet-serialisation bytes (see the module docs).
    run.data_source = canonical_data_source();
    run.data_identity = CANONICAL_DATA_IDENTITY.to_string();
    (config, spec, run)
}

/// Copy the four produced Parquet tables into `expected/` and write a normalised
/// `manifest.json` (canonical `created_utc` + `output_dir`) — the BLESS path.
fn bless_bundle(produced_dir: &Path) {
    let expected = expected_dir();
    if std::fs::create_dir_all(&expected).is_err() {
        panic!("the golden expected/ directory must exist");
    }
    for name in TABLE_FILES {
        if std::fs::copy(produced_dir.join(name), expected.join(name)).is_err() {
            panic!("BLESS must copy {name} into expected/");
        }
    }
    let mut manifest = oracle::read_manifest_json(produced_dir);
    if let Some(obj) = manifest.as_object_mut() {
        obj.insert(
            "created_utc".to_string(),
            serde_json::Value::String(CANONICAL_CREATED_UTC.to_string()),
        );
        if let Some(config) = obj
            .get_mut("config")
            .and_then(serde_json::Value::as_object_mut)
        {
            config.insert(
                "output_dir".to_string(),
                serde_json::Value::String(CANONICAL_OUTPUT_DIR.to_string()),
            );
        }
    }
    let Ok(mut text) = serde_json::to_string_pretty(&manifest) else {
        panic!("the normalised golden manifest must serialise");
    };
    text.push('\n');
    if std::fs::write(expected.join("manifest.json"), text).is_err() {
        panic!("BLESS must write the normalised manifest.json");
    }
}

/// The golden write→read→equal: run the canonical fixture, write the bundle,
/// read it back, and compare it to the committed golden under the single oracle
/// (tables decoded/sorted/exact, manifest canonical JSON with `created_utc`
/// excluded). `BLESS=1` regenerates the committed golden instead.
#[test]
fn test_bundle_golden_iron_condor_naive_write_read_equal() {
    let Ok(chain_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let Ok(out_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the bundle output must create");
    };
    let (config, spec, run) = build_golden_run(chain_dir.path(), out_dir.path());
    let Ok(produced_dir) = write_bundle(&run, &config, &spec) else {
        panic!("the golden bundle must write");
    };

    if bless_enabled() {
        bless_bundle(&produced_dir);
        return;
    }

    // write → read → equal, under the single comparison oracle.
    let limits = ResourceLimits::default();
    let Ok(produced) = read_bundle(&produced_dir, &limits) else {
        panic!("the produced bundle must read back");
    };
    let Ok(expected) = read_bundle(expected_dir(), &limits) else {
        panic!("the committed golden bundle must read back — regenerate with BLESS=1");
    };
    if let Err(diff) = oracle::compare_bundle_tables(&produced, &expected) {
        panic!("golden bundle table divergence: {diff} (regenerate with BLESS=1 if intended)");
    }
    let produced_manifest = oracle::read_manifest_json(&produced_dir);
    let expected_manifest = oracle::read_manifest_json(&expected_dir());
    if let Err(diff) = oracle::compare_manifest_json(&produced_manifest, &expected_manifest) {
        panic!("golden bundle manifest divergence: {diff} (regenerate with BLESS=1 if intended)");
    }
}

/// Same-environment run-twice: two identical runs into two fresh output roots
/// produce **byte-identical** Parquet tables and a `manifest.json` identical
/// after stripping `created_utc`. (The two runs use two temp output roots, so
/// the operational `config.output_dir` legitimately differs and is canonicalised
/// alongside `created_utc` — it is non-semantic and excluded from the `run_id`,
/// docs/05 §11; the four tables never embed it and are compared raw byte-for-byte.)
#[test]
fn test_bundle_run_twice_is_byte_identical() {
    let write_once = || -> (PathBuf, tempfile::TempDir, tempfile::TempDir) {
        let Ok(chain_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the generated chain must create");
        };
        let Ok(out_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the bundle output must create");
        };
        let (config, spec, run) = build_golden_run(chain_dir.path(), out_dir.path());
        let Ok(produced_dir) = write_bundle(&run, &config, &spec) else {
            panic!("the golden bundle must write");
        };
        (produced_dir, chain_dir, out_dir)
    };

    let (dir_a, _chain_a, _out_a) = write_once();
    let (dir_b, _chain_b, _out_b) = write_once();

    // Same (seed, config, data) ⇒ same run_id (the directory name).
    assert_eq!(
        dir_a.file_name(),
        dir_b.file_name(),
        "the deterministic run_id (directory name) must match across two runs"
    );

    // The four Parquet tables are byte-for-byte identical (they never embed the
    // output path).
    for name in TABLE_FILES {
        let Ok(a) = std::fs::read(dir_a.join(name)) else {
            panic!("{name} must be readable from run A");
        };
        let Ok(b) = std::fs::read(dir_b.join(name)) else {
            panic!("{name} must be readable from run B");
        };
        assert_eq!(a, b, "{name} must be byte-identical across two runs");
    }

    // The manifest is identical after stripping created_utc (+ canonicalising the
    // operational output_dir, which differs between the two temp output roots).
    let a = oracle::canonical_manifest(&oracle::read_manifest_json(&dir_a));
    let b = oracle::canonical_manifest(&oracle::read_manifest_json(&dir_b));
    assert_eq!(
        a, b,
        "manifest.json must be identical after stripping created_utc + output_dir"
    );
}

/// The oracle's near-boundary float cases ([docs/05 §12.5](../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)):
/// signed zero equal, `NaN` never equal (a produced `NaN` is itself a load
/// error the reader rejects), `±∞` equal only to the same infinity. These pin
/// the `drawdown`-column tolerance behaviour both repos must agree on.
#[test]
fn test_oracle_near_boundary_float_cases() {
    // Signed zero compares equal.
    assert!(oracle::floats_equal(0.0, -0.0));
    assert!(oracle::floats_equal(-0.0, 0.0));

    // NaN is never equal — not even to itself (and the reader rejects a NaN
    // drawdown at load time via its finite-cell guard, so a NaN never reaches
    // this comparison from a valid bundle).
    assert!(!oracle::floats_equal(f64::NAN, f64::NAN));
    assert!(!oracle::floats_equal(f64::NAN, 0.0));
    assert!(!oracle::floats_equal(0.0, f64::NAN));

    // ±∞ equal only to the same infinity.
    assert!(oracle::floats_equal(f64::INFINITY, f64::INFINITY));
    assert!(oracle::floats_equal(f64::NEG_INFINITY, f64::NEG_INFINITY));
    assert!(!oracle::floats_equal(f64::INFINITY, f64::NEG_INFINITY));
    assert!(!oracle::floats_equal(f64::NEG_INFINITY, f64::INFINITY));
    assert!(!oracle::floats_equal(f64::INFINITY, 1.0));
}
