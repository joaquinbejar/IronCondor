//! The **golden result bundles** — the frozen `ironcondor.bundle.v1` wire
//! contract, made executable across **every named golden scenario** (#36, #50).
//!
//! # What this pins
//!
//! For each named scenario a canonical `(config, data, seed)` run is written to a
//! **frozen bundle** — `manifest.json` + the four Parquet tables (`fills`,
//! `equity_curve`, `positions`, `greeks_attribution`) — under
//! `tests/golden/<scenario>/expected/`. Two tests hold each freeze:
//!
//! - **`write → read → equal`**: run → [`write_bundle`](ironcondor::write_bundle)
//!   → [`read_bundle`](ironcondor::read_bundle) → assert the decoded bundle equals
//!   the committed golden under the **single comparison oracle**
//!   ([`oracle`], [docs/05 §12.5](../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)):
//!   decode, sort by each table's pinned key, compare integer cents **exactly**
//!   and the one float column (`drawdown`) within the tolerance, and compare the
//!   manifest as canonical JSON with `created_utc` excluded. A value-changing
//!   engine / attribution / schema change that leaves the golden untouched
//!   **fails CI**.
//! - **run-twice byte-identical**: same environment ⇒ the four Parquet tables are
//!   **byte-for-byte** identical and `manifest.json` is byte-identical after
//!   stripping `created_utc`
//!   ([docs/05 §11](../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism)).
//!
//! # Scenario coverage (#50 — the full four-table bundle for every named golden)
//!
//! The #36 bundle golden froze `iron_condor_naive` only. #50 extends the same
//! full-bundle coverage (four tables + manifest, the single oracle, the run-twice
//! byte test) to **every** named golden scenario the v0.1/v0.2 goldens introduced
//! ([docs/TESTING.md §4](../docs/TESTING.md#4-golden-file-backtests)):
//!
//! - `iron_condor_naive` — the frozen #36 bundle (naive `IronCondor`). Its
//!   committed bytes are the contract and stay untouched.
//! - `short_strangle_naive` — the second strategy (naive `ShortStrangle`),
//!   proving the bundle writer + oracle generalise beyond `IronCondor`.
//! - `iron_condor_realistic` — the same strategy under **realistic** fills
//!   (feature `orderbook`), proving the bundle is mode-agnostic in structure
//!   while its values diverge (realistic pays the spread). The mode-pair
//!   full-bundle schema test ([`realistic::test_bundle_golden_pair_schema_is_mode_agnostic`])
//!   pins that: identical manifest key shape, divergent table values.
//!
//! Each scenario shares the same write→read→compare and run-twice helpers
//! ([`assert_bundle_golden`], [`assert_run_twice`]) and the same `BLESS` path
//! ([`bless_bundle`]) — one implementation, one oracle, per the determinism
//! contract. The v0.1-scoped equity-curve + minimal-metrics goldens for these
//! scenarios stay in [`tests/golden.rs`](golden.rs); this file adds the
//! full-bundle layer on top, it does not replace them.
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
//!   serialisations. Each golden run therefore overrides the run's data-source
//!   provenance and tape identity to fixed canonical constants (one per
//!   scenario) — the chain's *identity* for the golden is the committed
//!   generator, not the random bytes. This decouples the `run_id` and the
//!   manifest from any chain-serialisation or platform variance while keeping the
//!   four tables (the substantive P&L) fully exercised.
//! - **The operational output path is non-semantic.** `config.output_dir` is the
//!   tempdir write target; it is excluded from the semantic `run_id`
//!   ([docs/05 §11](../docs/05-analytics-and-reporting.md#11-atomic-writes-and-determinism))
//!   and is canonicalised alongside `created_utc` in the manifest comparison
//!   ([`oracle::canonical_manifest`]).
//!
//! # Regenerating the goldens (BLESS)
//!
//! A deliberate value-changing change re-blesses the affected golden **in the
//! same commit**:
//!
//! ```text
//! BLESS=1 cargo test --test bundle_golden                     # naive scenarios
//! BLESS=1 cargo test --test bundle_golden --features orderbook  # + realistic
//! ```
//!
//! which copies the four produced Parquet tables into the scenario's `expected/`
//! and writes a **normalised** `manifest.json` (canonical `created_utc` +
//! canonical `output_dir`, so the committed golden is clean and
//! reproducible-looking — the comparison strips both anyway). Review the diff,
//! then commit it alongside the code change.

use std::path::{Path, PathBuf};

use ironcondor::analytics::attribution::attribute;
use ironcondor::analytics::metrics;
use ironcondor::{
    BacktestConfig, BacktestEngine, BacktestRun, DataSourceSpec, ExecutionMode, FeeSchedule,
    LegSetStrategy, LiquidityProfile, NaiveFill, OptStratAdapter, ParquetFeed, ResourceLimits,
    SlippageModel, StrategySpec, read_bundle, write_bundle,
};
use optionstratlib::simulation::ExitPolicy;
use optionstratlib::strategies::{IronCondor, ShortStrangle};

mod common;
mod oracle;

/// Steps in the canonical golden chain — enough to exercise the open, the held
/// mark, and the terminal `on_end` close of every leg.
const GOLDEN_STEPS: u32 = 8;

/// The canonical (documentary) iron-condor data-source path recorded in the
/// golden manifest — a placeholder, not the tempdir the chain is generated into
/// (see the module docs).
const CANONICAL_DATA_PATH: &str = "iron_condor.parquet";

/// The canonical (documentary) short-strangle data-source path.
const CANONICAL_STRANGLE_DATA_PATH: &str = "short_strangle.parquet";

/// The canonical tape identity hashed into the naive iron-condor `run_id` —
/// pinned so the `run_id` is independent of the generated chain's Parquet bytes.
const CANONICAL_DATA_IDENTITY: &str = "golden:iron_condor_naive:ironcondor.bundle.v1";

/// The canonical tape identity hashed into the short-strangle `run_id`.
const CANONICAL_STRANGLE_DATA_IDENTITY: &str = "golden:short_strangle_naive:ironcondor.bundle.v1";

/// The canonical (documentary) multi-expiration leg-set data-source path.
const CANONICAL_LEGS_DATA_PATH: &str = "legs_multi_expiry.parquet";

/// The canonical tape identity hashed into the leg-set `run_id`.
const CANONICAL_LEGS_DATA_IDENTITY: &str = "golden:legs_multi_expiry_naive:ironcondor.bundle.v1";

/// The canonical `created_utc` written into a committed golden manifest (the
/// comparison strips `created_utc`, so any fixed value works; a clean epoch
/// timestamp keeps the committed file reproducible-looking).
const CANONICAL_CREATED_UTC: &str = "1970-01-01T00:00:00+00:00";

/// The canonical operational output path written into a committed golden manifest
/// (the comparison canonicalises `config.output_dir`, so the real tempdir never
/// reaches the frozen file).
const CANONICAL_OUTPUT_DIR: &str = "golden-out";

/// The four Parquet tables of the bundle, in a fixed order.
const TABLE_FILES: [&str; 4] = [
    "fills.parquet",
    "equity_curve.parquet",
    "positions.parquet",
    "greeks_attribution.parquet",
];

/// Absolute path to a scenario's committed golden `expected/` directory.
fn expected_dir(scenario: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(scenario)
        .join("expected")
}

/// Whether `BLESS=1` is set — regenerate the committed golden bundle.
fn bless_enabled() -> bool {
    std::env::var_os("BLESS").is_some_and(|value| value == "1")
}

/// A canonical data-source provenance recorded in a golden manifest (an unpinned
/// `sha256`, so `read_bundle` never tries to re-hash the absent chain).
fn canonical_data_source(path: &str) -> DataSourceSpec {
    DataSourceSpec::Parquet {
        path: path.to_string(),
        sha256: String::new(),
    }
}

/// The pinned semantic config shared by the naive iron-condor golden (#36) and
/// the short-strangle golden — same seed / capital / fees / slippage / limits;
/// the caller supplies the mode and data-source path.
fn base_config(mode: ExecutionMode, data_path: &str, output_dir: &Path) -> BacktestConfig {
    BacktestConfig {
        data_source: canonical_data_source(data_path),
        mode,
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

/// Populate the post-run analytics (summary metrics + per-step P&L attribution)
/// and pin the run's data-source provenance + tape identity to the scenario's
/// canonical constants, so the `run_id` + manifest are independent of the tempdir
/// chain path and its Parquet-serialisation bytes (see the module docs).
fn finalize_run(
    run: &mut BacktestRun,
    config: &BacktestConfig,
    data_path: &str,
    data_identity: &str,
) {
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
    run.data_source = canonical_data_source(data_path);
    run.data_identity = data_identity.to_string();
}

/// Assemble and run the canonical **naive iron-condor** bundle run: generate the
/// deterministic chain into `chain_dir`, drive `ParquetFeed → BacktestEngine::run
/// → metrics::populate → attribution::attribute`, and pin the data-source
/// identity to the canonical constants. Returns the config + strategy spec + the
/// completed run.
fn build_iron_condor_naive_run(
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
    let config = base_config(ExecutionMode::Naive, CANONICAL_DATA_PATH, output_dir);
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
    finalize_run(
        &mut run,
        &config,
        CANONICAL_DATA_PATH,
        CANONICAL_DATA_IDENTITY,
    );
    (config, spec, run)
}

/// Assemble and run the canonical **naive short-strangle** bundle run — the same
/// pipeline as [`build_iron_condor_naive_run`] but with the two-leg
/// [`ShortStrangle`] strategy over the [`common::strangle_rows`] chain, proving
/// the bundle writer + oracle generalise to the second strategy (#28/#50).
fn build_short_strangle_naive_run(
    chain_dir: &Path,
    output_dir: &Path,
) -> (BacktestConfig, StrategySpec, BacktestRun) {
    let chain_path = chain_dir.join("short_strangle.parquet");
    let rows = common::strangle_rows(GOLDEN_STEPS);
    if common::write_parquet(&chain_path, &rows).is_err() {
        panic!("the canonical short strangle golden chain must write");
    }
    let Ok(feed) = ParquetFeed::open(&chain_path, &ResourceLimits::default()) else {
        panic!("the canonical short strangle golden chain must open");
    };
    let config = base_config(
        ExecutionMode::Naive,
        CANONICAL_STRANGLE_DATA_PATH,
        output_dir,
    );
    let spec = common::short_strangle_spec();
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let Ok(adapter) = OptStratAdapter::<ShortStrangle>::from_spec(&spec, exit) else {
        panic!("the short strangle adapter must build");
    };
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    let Ok(mut run) = BacktestEngine::run(&config, feed, execution, adapter, "short_strangle")
    else {
        panic!("the canonical short strangle golden run must succeed");
    };
    finalize_run(
        &mut run,
        &config,
        CANONICAL_STRANGLE_DATA_PATH,
        CANONICAL_STRANGLE_DATA_IDENTITY,
    );
    (config, spec, run)
}

/// Assemble and run the canonical **naive multi-expiration leg-set** bundle run
/// (#117): the same pipeline as [`build_iron_condor_naive_run`], but over the
/// four-leg / two-expiration [`common::legs_rows`] chain driven by
/// [`LegSetStrategy`] — the strategy for a [`StrategySpec::Legs`] spec, which has
/// no upstream object to wrap. This freezes the bundle shape for a position no
/// named spec can describe, and its `run_id` is derived from the CANONICAL leg
/// order ([`common::legs_spec`] deliberately lists its legs unsorted).
fn build_legs_multi_expiry_naive_run(
    chain_dir: &Path,
    output_dir: &Path,
) -> (BacktestConfig, StrategySpec, BacktestRun) {
    let chain_path = chain_dir.join("legs_multi_expiry.parquet");
    let rows = common::legs_rows(GOLDEN_STEPS);
    if common::write_parquet_multi_expiry(&chain_path, &rows).is_err() {
        panic!("the canonical multi-expiry golden chain must write");
    }
    let Ok(feed) = ParquetFeed::open(&chain_path, &ResourceLimits::default()) else {
        panic!("the canonical multi-expiry golden chain must open");
    };
    let config = base_config(ExecutionMode::Naive, CANONICAL_LEGS_DATA_PATH, output_dir);
    let spec = common::legs_spec();
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let Ok(strategy) = LegSetStrategy::from_spec(&spec, exit) else {
        panic!("the leg set strategy must build");
    };
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    let Ok(mut run) = BacktestEngine::run(&config, feed, execution, strategy, "legs") else {
        panic!("the canonical multi-expiry golden run must succeed");
    };
    finalize_run(
        &mut run,
        &config,
        CANONICAL_LEGS_DATA_PATH,
        CANONICAL_LEGS_DATA_IDENTITY,
    );
    (config, spec, run)
}

/// Copy the four produced Parquet tables into the scenario's `expected/` and
/// write a normalised `manifest.json` (canonical `created_utc` + `output_dir`) —
/// the BLESS path.
fn bless_bundle(scenario: &str, produced_dir: &Path) {
    let expected = expected_dir(scenario);
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

/// The shared golden assertion: write the produced `run` to a bundle, read it
/// back, and compare it to the scenario's committed golden under the single
/// oracle (tables decoded/sorted/exact, manifest canonical JSON with
/// `created_utc` excluded). `BLESS=1` regenerates the committed golden instead.
fn assert_bundle_golden(
    scenario: &str,
    config: &BacktestConfig,
    spec: &StrategySpec,
    run: &BacktestRun,
) {
    let Ok(produced_dir) = write_bundle(run, config, spec) else {
        panic!("the golden bundle must write");
    };

    if bless_enabled() {
        bless_bundle(scenario, &produced_dir);
        return;
    }

    let limits = ResourceLimits::default();
    let Ok(produced) = read_bundle(&produced_dir, &limits) else {
        panic!("the produced bundle must read back");
    };
    let Ok(expected) = read_bundle(expected_dir(scenario), &limits) else {
        panic!("the committed golden bundle must read back — regenerate with BLESS=1");
    };
    if let Err(diff) = oracle::compare_bundle_tables(&produced, &expected) {
        panic!(
            "golden bundle table divergence [{scenario}]: {diff} (regenerate with BLESS=1 if intended)"
        );
    }
    let produced_manifest = oracle::read_manifest_json(&produced_dir);
    let expected_manifest = oracle::read_manifest_json(&expected_dir(scenario));
    if let Err(diff) = oracle::compare_manifest_json(&produced_manifest, &expected_manifest) {
        panic!(
            "golden bundle manifest divergence [{scenario}]: {diff} (regenerate with BLESS=1 if intended)"
        );
    }
}

/// The shared run-twice byte assertion: build + write the scenario twice into two
/// fresh output roots and assert the four Parquet tables are **byte-identical**
/// and `manifest.json` is identical after stripping `created_utc` (+
/// canonicalising the operational `output_dir`, which legitimately differs
/// between the two temp roots — it is non-semantic and excluded from the
/// `run_id`, docs/05 §11; the four tables never embed it and are compared raw).
fn assert_run_twice(build: impl Fn(&Path, &Path) -> (BacktestConfig, StrategySpec, BacktestRun)) {
    let write_once = || -> (PathBuf, tempfile::TempDir, tempfile::TempDir) {
        let Ok(chain_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the generated chain must create");
        };
        let Ok(out_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the bundle output must create");
        };
        let (config, spec, run) = build(chain_dir.path(), out_dir.path());
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

/// The `iron_condor_naive` golden write→read→equal (the frozen #36 bundle):
/// run the canonical fixture, write the bundle, read it back, and compare it to
/// the committed golden under the single oracle. `BLESS=1` regenerates instead.
#[test]
fn test_bundle_golden_iron_condor_naive_write_read_equal() {
    let Ok(chain_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let Ok(out_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the bundle output must create");
    };
    let (config, spec, run) = build_iron_condor_naive_run(chain_dir.path(), out_dir.path());
    assert_bundle_golden("iron_condor_naive", &config, &spec, &run);
}

/// Same-environment run-twice for `iron_condor_naive`: two identical runs into two
/// fresh output roots produce byte-identical tables + a manifest identical after
/// stripping `created_utc`.
#[test]
fn test_bundle_run_twice_is_byte_identical() {
    assert_run_twice(build_iron_condor_naive_run);
}

/// The `short_strangle_naive` golden write→read→equal (#50): the same full
/// four-table + manifest freeze as the iron-condor bundle, for the second
/// strategy through the **unchanged** generic writer + oracle.
#[test]
fn test_bundle_golden_short_strangle_naive_write_read_equal() {
    let Ok(chain_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let Ok(out_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the bundle output must create");
    };
    let (config, spec, run) = build_short_strangle_naive_run(chain_dir.path(), out_dir.path());
    assert_bundle_golden("short_strangle_naive", &config, &spec, &run);
}

/// Same-environment run-twice for `short_strangle_naive` (#50): byte-identical
/// four tables + manifest across two runs.
#[test]
fn test_bundle_run_twice_short_strangle_is_byte_identical() {
    assert_run_twice(build_short_strangle_naive_run);
}

/// The `legs_multi_expiry_naive` golden write→read→equal (#117): a four-leg
/// position across TWO expirations round-trips through `write_bundle` /
/// `read_bundle` unchanged, under the same oracle and writer as the named kinds
/// — so a `Legs` bundle is indistinguishable in shape from an `IronCondor` one.
#[test]
fn test_bundle_golden_legs_multi_expiry_naive_write_read_equal() {
    let Ok(chain_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let Ok(out_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the bundle output must create");
    };
    let (config, spec, run) = build_legs_multi_expiry_naive_run(chain_dir.path(), out_dir.path());
    assert_bundle_golden("legs_multi_expiry_naive", &config, &spec, &run);
}

/// Same-environment run-twice for `legs_multi_expiry_naive` (#117):
/// byte-identical four tables + manifest across two runs.
#[test]
fn test_bundle_run_twice_legs_multi_expiry_is_byte_identical() {
    assert_run_twice(build_legs_multi_expiry_naive_run);
}

/// A permuted leg set produces the **same bundle**, tables included (#117).
///
/// The `run_id` and the manifest canonicalise, so the two runs land in the same
/// `<run_id>/` directory — which is exactly why the **four tables** must agree
/// too: one `run_id` names one byte-set, or `overwrite` silently replaces one
/// run's results with another's.
///
/// This drives the **engine** twice, once per leg order, rather than writing one
/// run under two specs. That distinction is the whole test: the engine mints
/// `order_id` / `position_id` / `trade_id` in submission order, so a strategy
/// that opened the caller's leg order would emit permuted `fills` / `positions`
/// under an identical `run_id` and manifest — and a write-only comparison would
/// not see it. `LegSetStrategy::new` canonicalises the legs it runs, so both
/// orders execute identically.
#[test]
fn test_bundle_legs_permuted_input_order_runs_and_writes_the_same_bundle() {
    /// Run the canonical golden pipeline over `spec` — the leg order under test
    /// — and write its bundle, returning the published directory.
    fn run_and_write(spec: &StrategySpec, chain_dir: &Path, out_dir: &Path) -> PathBuf {
        let chain_path = chain_dir.join("legs_multi_expiry.parquet");
        let rows = common::legs_rows(GOLDEN_STEPS);
        if common::write_parquet_multi_expiry(&chain_path, &rows).is_err() {
            panic!("the permutation chain must write");
        }
        let Ok(feed) = ParquetFeed::open(&chain_path, &ResourceLimits::default()) else {
            panic!("the permutation chain must open");
        };
        let config = base_config(ExecutionMode::Naive, CANONICAL_LEGS_DATA_PATH, out_dir);
        let exit = ExitPolicy::TimeSteps(1_000_000);
        let Ok(strategy) = LegSetStrategy::from_spec(spec, exit) else {
            panic!("the leg set strategy must build");
        };
        let execution = NaiveFill::new(config.slippage.clone(), config.fees);
        let Ok(mut run) = BacktestEngine::run(&config, feed, execution, strategy, "legs") else {
            panic!("the permutation run must succeed");
        };
        finalize_run(
            &mut run,
            &config,
            CANONICAL_LEGS_DATA_PATH,
            CANONICAL_LEGS_DATA_IDENTITY,
        );
        let Ok(dir) = write_bundle(&run, &config, spec) else {
            panic!("the permutation bundle must write");
        };
        dir
    }

    let spec = common::legs_spec();
    // The SAME position, its legs written in the reverse input order.
    let StrategySpec::Legs(mut reversed) = spec.clone() else {
        panic!("the leg-set fixture is a leg set");
    };
    reversed.legs.reverse();
    let permuted = StrategySpec::Legs(reversed);
    assert_ne!(permuted, spec, "the fixture permutation changes the input");

    let (Ok(chain_a), Ok(chain_b)) = (tempfile::tempdir(), tempfile::tempdir()) else {
        panic!("two tempdirs for the generated chains must create");
    };
    let (Ok(out_a), Ok(out_b)) = (tempfile::tempdir(), tempfile::tempdir()) else {
        panic!("two tempdirs for the bundle output must create");
    };
    let dir_a = run_and_write(&spec, chain_a.path(), out_a.path());
    let dir_b = run_and_write(&permuted, chain_b.path(), out_b.path());

    assert_eq!(
        dir_a.file_name(),
        dir_b.file_name(),
        "a permuted leg set must derive the same run_id"
    );

    let limits = ResourceLimits::default();
    let (Ok(bundle_a), Ok(bundle_b)) = (read_bundle(&dir_a, &limits), read_bundle(&dir_b, &limits))
    else {
        panic!("both permutation bundles must read back");
    };
    if let Err(diff) = oracle::compare_bundle_tables(&bundle_a, &bundle_b) {
        panic!(
            "a permuted leg set must produce identical tables, not just an \
             identical run_id: {diff}"
        );
    }
    if let Err(diff) = oracle::compare_manifest_json(
        &oracle::read_manifest_json(&dir_a),
        &oracle::read_manifest_json(&dir_b),
    ) {
        panic!("a permuted leg set must record the same canonical manifest: {diff}");
    }

    // The identity the tables carry is the CANONICAL one: position_id 1 is the
    // first leg in canonical order (the near-expiry short put), not whichever
    // leg the caller happened to write first.
    let Some(first) = bundle_a.positions.iter().find(|p| p.position_id == 1) else {
        panic!("the bundle has a position_id 1");
    };
    assert_eq!(
        first.contract_id, "v1:SPX:1752883200000000000:490000:P",
        "position_id 1 is the first leg in CANONICAL order"
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

/// The realistic-mode bundle golden (feature `orderbook`, #24/#50): the same
/// full four-table + manifest freeze as the naive bundle, but the run is
/// assembled with [`ironcondor::RealisticFill`] so entries and exits route
/// through the seeded `option-chain-orderbook` book — the values diverge from
/// naive (the fill-risk signal the mode exists to surface), the structure does
/// not (the mode-pair schema test pins that).
#[cfg(feature = "orderbook")]
mod realistic {
    use std::path::Path;

    use ironcondor::{
        BacktestConfig, BacktestEngine, BacktestRun, ExecutionMode, OptStratAdapter, ParquetFeed,
        RealisticFill, ResourceLimits, StrategySpec, read_bundle,
    };
    use optionstratlib::simulation::ExitPolicy;
    use optionstratlib::strategies::IronCondor;

    use super::{
        CANONICAL_DATA_PATH, GOLDEN_STEPS, assert_bundle_golden, assert_run_twice, base_config,
        common, expected_dir, finalize_run, oracle,
    };

    /// The canonical tape identity hashed into the realistic `run_id`.
    const CANONICAL_REALISTIC_DATA_IDENTITY: &str =
        "golden:iron_condor_realistic:ironcondor.bundle.v1";

    /// Assemble the realistic run **directly**: a `ParquetFeed`, a
    /// `RealisticFill::with_liquidity_profile` seeded from the config's seed +
    /// liquidity profile, and the `IronCondor` adapter, driven through
    /// `BacktestEngine::run` → `metrics::populate` → `attribution::attribute`,
    /// over the **same** canonical chain as the naive bundle.
    fn build_iron_condor_realistic_run(
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
        let config = base_config(ExecutionMode::Realistic, CANONICAL_DATA_PATH, output_dir);
        let spec = common::iron_condor_spec();
        let exit = ExitPolicy::TimeSteps(1_000_000);
        let Ok(adapter) = OptStratAdapter::<IronCondor>::from_spec(&spec, exit) else {
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
        finalize_run(
            &mut run,
            &config,
            CANONICAL_DATA_PATH,
            CANONICAL_REALISTIC_DATA_IDENTITY,
        );
        (config, spec, run)
    }

    /// The `iron_condor_realistic` golden write→read→equal (#50): the full
    /// four-table + manifest freeze under realistic fills.
    #[test]
    fn test_bundle_golden_iron_condor_realistic_write_read_equal() {
        let Ok(chain_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the generated chain must create");
        };
        let Ok(out_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the bundle output must create");
        };
        let (config, spec, run) = build_iron_condor_realistic_run(chain_dir.path(), out_dir.path());
        assert_bundle_golden("iron_condor_realistic", &config, &spec, &run);
    }

    /// Same-environment run-twice for `iron_condor_realistic` (#50): byte-identical
    /// four tables + manifest across two seeded-book runs.
    #[test]
    fn test_bundle_run_twice_realistic_is_byte_identical() {
        assert_run_twice(build_iron_condor_realistic_run);
    }

    /// #50 mode-pair full-bundle schema parity: the committed `iron_condor_naive`
    /// and `iron_condor_realistic` bundles — the **same strategy** under both
    /// modes — share an **identical manifest key shape** and identical decoded
    /// table column sets while their **values diverge** (realistic pays the
    /// spread). This proves the full bundle is mode-agnostic in structure end to
    /// end: analytics cannot tell which mode produced a bundle from its shape
    /// ([04 §2](../docs/04-execution-models.md#2-the-executionmodel-trait-and-the-shared-fill-report)).
    /// It reads the committed `expected/` bundles only — it never blesses.
    #[test]
    fn test_bundle_golden_pair_schema_is_mode_agnostic() {
        let limits = ResourceLimits::default();
        let Ok(naive) = read_bundle(expected_dir("iron_condor_naive"), &limits) else {
            panic!("the committed naive golden bundle must read back");
        };
        let Ok(realistic) = read_bundle(expected_dir("iron_condor_realistic"), &limits) else {
            panic!("the committed realistic golden bundle must read back");
        };

        // Manifest structure parity: identical top-level key shape + dtypes, and
        // identical `config` key shape. (The `mode` VALUE differs — naive vs
        // realistic — which the value check below relies on.)
        let naive_manifest = oracle::read_manifest_json(&expected_dir("iron_condor_naive"));
        let realistic_manifest = oracle::read_manifest_json(&expected_dir("iron_condor_realistic"));
        assert_eq!(
            manifest_schema(&naive_manifest),
            manifest_schema(&realistic_manifest),
            "manifest top-level key shape must be mode-agnostic"
        );
        assert_eq!(
            config_schema(&naive_manifest),
            config_schema(&realistic_manifest),
            "manifest config key shape must be mode-agnostic"
        );

        // The two bundles have the same row counts per table (same scenario, same
        // number of steps / legs) — the decoded row TYPES are identical by
        // construction, so equal lengths is the structural claim at the table
        // level.
        assert_eq!(
            naive.fills.len(),
            realistic.fills.len(),
            "fills row count must be mode-agnostic"
        );
        assert_eq!(
            naive.equity_curve.len(),
            realistic.equity_curve.len(),
            "equity_curve row count must be mode-agnostic"
        );
        assert_eq!(
            naive.positions.len(),
            realistic.positions.len(),
            "positions row count must be mode-agnostic"
        );
        assert_eq!(
            naive.greeks_attribution.len(),
            realistic.greeks_attribution.len(),
            "greeks_attribution row count must be mode-agnostic"
        );

        // Values diverge: realistic crosses the seeded spread on entry and exit,
        // so at least one fill price and at least one equity point must differ.
        assert!(
            naive
                .fills
                .iter()
                .zip(realistic.fills.iter())
                .any(|(n, r)| n.price_cents != r.price_cents),
            "realistic fills must diverge from naive — realistic pays the spread"
        );
        assert!(
            naive
                .equity_curve
                .iter()
                .zip(realistic.equity_curve.iter())
                .any(|(n, r)| n.equity_cents != r.equity_cents),
            "realistic equity must diverge from naive — it collapsed into naive fills"
        );
    }

    /// The JSON kind of a value — its "dtype" at the serialised boundary.
    fn json_kind(value: &serde_json::Value) -> &'static str {
        match value {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        }
    }

    /// The sorted `(key, dtype)` pairs of a JSON object — its schema.
    fn schema_of(value: &serde_json::Value) -> Vec<(String, &'static str)> {
        let Some(obj) = value.as_object() else {
            panic!("expected a JSON object to extract a schema from");
        };
        let mut pairs: Vec<(String, &'static str)> =
            obj.iter().map(|(k, v)| (k.clone(), json_kind(v))).collect();
        pairs.sort();
        pairs
    }

    /// The manifest's top-level key shape, with `created_utc` (wall-clock) and the
    /// opaque `metrics` object excluded — the structural surface a consumer sees.
    fn manifest_schema(manifest: &serde_json::Value) -> Vec<(String, &'static str)> {
        let mut pairs = schema_of(manifest);
        pairs.retain(|(k, _)| k != "created_utc" && k != "metrics");
        pairs
    }

    /// The manifest's embedded `config` key shape.
    fn config_schema(manifest: &serde_json::Value) -> Vec<(String, &'static str)> {
        let Some(config) = manifest.as_object().and_then(|o| o.get("config")) else {
            panic!("the manifest must carry a config object");
        };
        schema_of(config)
    }
}
