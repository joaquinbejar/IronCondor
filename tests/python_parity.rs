//! **Python ↔ Rust bundle parity** — the v0.4 binding-correctness gate (#42).
//!
//! Proves that the *same inputs* produce a **logically identical bundle** from
//! the Python API and the Rust API, and — within one environment — a
//! **byte-for-byte** identical one, diffed under the **single comparison oracle**
//! shared with the golden suite ([`oracle`], [docs/02 §7](../docs/02-engine-architecture.md#7-determinism-and-reproducibility),
//! [docs/06 §9](../docs/06-python-bindings.md#9-determinism-parity)). This is what
//! lets us claim a Python quant runs the *same* engine, not a look-alike.
//!
//! # Why this is CI-gated (not a local default)
//!
//! The test is `#[cfg(feature = "python")]`, so it compiles only under the
//! `python` feature. That feature links PyO3 **without** `extension-module`
//! (maturin injects that flag for the wheel build only, see `Cargo.toml`), so the
//! test binary embeds `libpython` and needs an interpreter **≥ the abi3-py310
//! floor**. CI's `test` job (`cargo test --all-features`) pins Python 3.12 and
//! runs it; a local box below 3.10 cannot link it (same constraint as the
//! `src/python/errors.rs` embedding tests). Locally it runs with
//! `PYO3_PYTHON=python3.12 cargo test --features python --test python_parity`.
//!
//! # How the Python path runs in-process
//!
//! The shipped extension is imported by an **embedding** interpreter via
//! [`pyo3::append_to_inittab!`] on the *real* `#[pymodule]` registration
//! ([`ironcondor::python::ironcondor`], made `pub` for exactly this), so
//! `import ironcondor` inside the embedded interpreter resolves to the genuine
//! module — the same classes, builders, `run`, and `Bundle` a `pip install`
//! user gets. The config is then built through the **real chainable builders**
//! (`BacktestConfig(...).data_parquet(...).strategy_iron_condor(...)…`) and run
//! through the real `ic.run()`, so the whole binding marshalling path
//! (`to_rust` + the GIL-releasing writer) is exercised, not a shortcut.
//!
//! # What is compared, and against what (the #36 pinned-constants caveat)
//!
//! The comparison target is the **Rust API over the exact same marshalled
//! inputs**, *not* the committed `iron_condor_naive` golden. The golden's
//! `manifest`/`run_id` are pinned to canonical data-identity constants
//! (`CANONICAL_DATA_IDENTITY` / `CANONICAL_DATA_PATH` in
//! [`tests/bundle_golden.rs`](bundle_golden.rs)) so they are decoupled from the
//! tempdir chain bytes; a plain `ic.run()` over a freshly generated chain
//! computes the *real* tape `sha256`, so its `run_id`/manifest legitimately
//! differ from the golden's. Both paths here read the **same** generated chain
//! file and marshal to the **same** config, so they share the same real tape
//! identity → same `run_id` → byte-identical manifest (minus the wall-clock
//! `created_utc` and the operational `output_dir`, canonicalised exactly as the
//! #36 oracle does).
//!
//! # Decimal scale mirrors the binding
//!
//! The Python builder converts an analytic `f64` (implied volatility, rates)
//! to `Decimal` with `Decimal::try_from` (`src/python/config.rs::decimal_from_f64`),
//! e.g. `0.20 → "0.2"` (scale 1) — value-equal to but serialised differently
//! from the golden's `Decimal::new(20, 2)` (`"0.20"`). The `run_id` preimage and
//! the manifest serialise the `Decimal` by its scale-preserving `Display`, so the
//! Rust path here reconstructs the strategy spec with the **same** `try_from`
//! conversion (see [`parity_strategy_spec`]) to stay byte-identical. The
//! numeric *value* is unchanged, so the P&L tables are identical either way —
//! only the serialised string scale matters, and both paths pick the same one.

#![cfg(feature = "python")]

use std::path::{Path, PathBuf};
use std::sync::Once;

use chrono::DateTime;
use ironcondor::python::ironcondor as ic_module;
use ironcondor::{
    BacktestConfig, IronCondorSpec, PriceCents, Quantity, ResourceLimits, StrategySpec, Underlying,
    read_bundle, run_backtest, write_bundle,
};
use optionstratlib::ExpirationDate;
use optionstratlib::simulation::ExitPolicy;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rust_decimal::Decimal;

mod common;
mod oracle;

/// Steps in the canonical parity chain — the same length as the golden
/// (`GOLDEN_STEPS`), enough to open, hold, and terminally close every leg.
const PARITY_STEPS: u32 = 8;

/// The engine seed for the shared scenario (matches the `iron_condor_naive`
/// golden config).
const PARITY_SEED: u64 = 42;

/// A non-triggering exit-policy horizon, so `on_end` is the sole closer of every
/// leg (matches the golden / `common` scenario).
const PARITY_EXIT_STEPS: usize = 1_000_000;

/// The four Parquet tables of the bundle, in a fixed order.
const TABLE_FILES: [&str; 4] = [
    "fills.parquet",
    "equity_curve.parquet",
    "positions.parquet",
    "greeks_attribution.parquet",
];

/// Register the real `ironcondor` `#[pymodule]` in the embedded interpreter's
/// init table, then initialise the interpreter — exactly once for the whole test
/// binary.
///
/// `append_to_inittab!` **must** run before the interpreter is initialised (it
/// panics otherwise) and only once, so both are guarded by a single [`Once`];
/// every test calls this before `Python::attach`, so a parallel test can never
/// reach `attach` before init has completed. This mirrors the `with_py` helper
/// in `src/python/errors.rs`, extended with the inittab registration an *import*
/// (rather than a direct `to_pyerr` call) needs.
fn ensure_python_ready() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // The `#[pymodule]` companion module is `pub`, so an external test crate
        // can name it for the init-table registration.
        pyo3::append_to_inittab!(ic_module);
        Python::initialize();
    });
}

/// Write the canonical iron-condor chain (the shared golden fixture) to `path`.
fn write_parity_chain(path: &Path) {
    let rows = common::condor_rows(PARITY_STEPS, None);
    if common::write_parquet(path, &rows).is_err() {
        panic!("the canonical parity chain must write");
    }
}

/// The strategy spec the **binding** marshals for the shared scenario — an
/// `IronCondorSpec` whose analytic `f64` fields go through the *same*
/// `Decimal::try_from` conversion as `src/python/config.rs::decimal_from_f64`
/// (so `0.20 → "0.2"`, `0.05 → "0.05"`, `0.0 → "0"`), keeping the manifest /
/// `run_id` byte-identical to the Python path. Every money field is integer
/// cents and matches the `iron_condor_naive` golden legs.
fn parity_strategy_spec() -> StrategySpec {
    let Ok(underlying) = Underlying::new("SPX") else {
        panic!("SPX is a valid ticker");
    };
    let Ok(quantity) = Quantity::new(1) else {
        panic!("1 is a valid quantity");
    };
    let Ok(iv) = Decimal::try_from(0.20_f64) else {
        panic!("0.20 is a representable decimal");
    };
    let Ok(rate) = Decimal::try_from(0.05_f64) else {
        panic!("0.05 is a representable decimal");
    };
    let Ok(dividend) = Decimal::try_from(0.0_f64) else {
        panic!("0.0 is a representable decimal");
    };
    StrategySpec::IronCondor(IronCondorSpec {
        underlying,
        underlying_price: PriceCents::new(500_000),
        short_call_strike: PriceCents::new(510_000),
        short_put_strike: PriceCents::new(490_000),
        long_call_strike: PriceCents::new(520_000),
        long_put_strike: PriceCents::new(480_000),
        expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(common::EXPIRY)),
        implied_volatility: iv,
        risk_free_rate: rate,
        dividend_yield: dividend,
        quantity,
        premium_short_call: PriceCents::new(2_000),
        premium_short_put: PriceCents::new(1_800),
        premium_long_call: PriceCents::new(800),
        premium_long_put: PriceCents::new(700),
        open_fee: PriceCents::new(65),
        close_fee: PriceCents::new(65),
    })
}

/// The Rust-side config for the shared scenario over `chain`, writing to
/// `out_dir`. Reuses `common::condor_config` (seed / capital / fees / naive /
/// limits) and only redirects the operational `output_dir` — the exact config
/// the binding's `to_rust()` produces for the same scenario (pinned struct-level
/// in `src/python/config.rs`).
fn rust_config(chain: &Path, out_dir: &Path) -> BacktestConfig {
    let mut config = common::condor_config(chain, PARITY_SEED);
    config.output_dir = out_dir.to_path_buf();
    config
}

/// Drive the **Rust API** end to end: `run_backtest` (feed → engine → metrics →
/// attribution) then `write_bundle`, returning the finalised bundle directory.
/// This is the exact pair `src/python/run.rs::run` invokes with the GIL
/// released.
fn run_rust_path(chain: &Path, out_dir: &Path) -> PathBuf {
    let config = rust_config(chain, out_dir);
    let spec = parity_strategy_spec();
    let exit = ExitPolicy::TimeSteps(PARITY_EXIT_STEPS);
    let Ok(run) = run_backtest(&config, &spec, exit) else {
        panic!("the Rust parity run must succeed");
    };
    let Ok(dir) = write_bundle(&run, &config, &spec) else {
        panic!("the Rust parity bundle must write");
    };
    dir
}

/// Drive the **Python API** end to end, in-process, through the real registered
/// module: build the config with the chainable builders exactly as the docs/06
/// §4 script does, then `ic.run(cfg)`, and return `bundle.path`.
///
/// The scenario values mirror `run_rust_path` field-for-field so the two
/// bundles are provably built from the same inputs; the only intended
/// difference is the operational `output_dir`.
fn run_python_path(py: Python<'_>, chain: &Path, out_dir: &Path) -> PyResult<PathBuf> {
    let ic = py.import("ironcondor")?;

    // BacktestConfig(seed=42, capital_cents=10_000_000)
    let cfg = ic
        .getattr("BacktestConfig")?
        .call1((PARITY_SEED, 10_000_000_u64))?;

    // .data_parquet(<the shared chain>)
    let cfg = cfg.call_method1("data_parquet", (chain.display().to_string(),))?;

    // .strategy_iron_condor(...) — the full canonical parameter set, by kwargs.
    let strat = PyDict::new(py);
    strat.set_item("underlying", "SPX")?;
    strat.set_item("underlying_price_cents", 500_000_u64)?;
    strat.set_item("short_call_strike_cents", 510_000_u64)?;
    strat.set_item("short_put_strike_cents", 490_000_u64)?;
    strat.set_item("long_call_strike_cents", 520_000_u64)?;
    strat.set_item("long_put_strike_cents", 480_000_u64)?;
    strat.set_item("expiration_ns", common::EXPIRY)?;
    strat.set_item("quantity", 1_u32)?;
    strat.set_item("premium_short_call_cents", 2_000_u64)?;
    strat.set_item("premium_short_put_cents", 1_800_u64)?;
    strat.set_item("premium_long_call_cents", 800_u64)?;
    strat.set_item("premium_long_put_cents", 700_u64)?;
    strat.set_item("implied_volatility", 0.20_f64)?;
    strat.set_item("risk_free_rate", 0.05_f64)?;
    strat.set_item("dividend_yield", 0.0_f64)?;
    strat.set_item("open_fee_cents", 65_u64)?;
    strat.set_item("close_fee_cents", 65_u64)?;
    let cfg = cfg.call_method("strategy_iron_condor", (), Some(&strat))?;

    // .exit_time_steps(...).execution_naive().fees(...).output_dir(...)
    let cfg = cfg.call_method1("exit_time_steps", (PARITY_EXIT_STEPS,))?;
    let cfg = cfg.call_method0("execution_naive")?;
    let fees = PyDict::new(py);
    fees.set_item("per_contract_cents", 65_u64)?;
    fees.set_item("per_order_cents", 100_u64)?;
    let cfg = cfg.call_method("fees", (), Some(&fees))?;
    let cfg = cfg.call_method1("output_dir", (out_dir.display().to_string(),))?;

    // ic.run(cfg) -> Bundle; return bundle.path.
    let bundle = ic.call_method1("run", (cfg,))?;
    let path: String = bundle.getattr("path")?.extract()?;
    Ok(PathBuf::from(path))
}

/// The **marshalled semantic inputs** slice of a bundle manifest — the semantic
/// `config` (with the operational `output_dir` canonicalised), the `strategy`,
/// and the `seed`. If two bundles agree here, their inputs marshalled to the
/// same `BacktestConfig` + strategy + seed. (Struct-level `to_rust()` equality
/// is pinned in `src/python/config.rs`; this is the serialised-boundary proof.)
fn marshalled_inputs(dir: &Path) -> serde_json::Value {
    let manifest = oracle::canonical_manifest(&oracle::read_manifest_json(dir));
    let obj = manifest.as_object().cloned().unwrap_or_default();
    let mut out = serde_json::Map::new();
    for key in ["config", "strategy", "seed"] {
        if let Some(value) = obj.get(key) {
            out.insert((*key).to_string(), value.clone());
        }
    }
    serde_json::Value::Object(out)
}

/// The load-bearing parity gate: the Python API and the Rust API, over the same
/// generated chain and the same marshalled config, produce a bundle that is
/// (a) byte-identical across the four Parquet tables, (b) byte-identical in the
/// manifest after stripping `created_utc` + canonicalising `output_dir`, and
/// (c) logically equal under the single comparison oracle — plus a config-marshal
/// equality assertion on the serialised semantic inputs.
#[test]
fn test_python_and_rust_bundles_are_identical_under_the_oracle() {
    ensure_python_ready();

    let Ok(chain_dir) = tempfile::tempdir() else {
        panic!("a tempdir for the generated chain must create");
    };
    let Ok(out_rust) = tempfile::tempdir() else {
        panic!("a tempdir for the Rust bundle must create");
    };
    let Ok(out_python) = tempfile::tempdir() else {
        panic!("a tempdir for the Python bundle must create");
    };
    let chain = chain_dir.path().join("iron_condor.parquet");
    write_parity_chain(&chain);

    // Path A — the Rust API.
    let dir_rust = run_rust_path(&chain, out_rust.path());

    // Path B — the Python API, in-process through the real registered module.
    let dir_python = Python::attach(|py| match run_python_path(py, &chain, out_python.path()) {
        Ok(dir) => dir,
        Err(err) => {
            let message = err.to_string();
            panic!("the Python parity run must succeed, got: {message}");
        }
    });

    // Same (seed, config, data) ⇒ same run_id (the bundle directory name),
    // independent of the two distinct operational output roots.
    assert_eq!(
        dir_rust.file_name(),
        dir_python.file_name(),
        "the deterministic run_id (directory name) must match across the Rust and Python paths"
    );

    // (a) The four Parquet tables are byte-for-byte identical (they never embed
    // the output path), same environment, one code path (the Rust writer).
    for name in TABLE_FILES {
        let Ok(a) = std::fs::read(dir_rust.join(name)) else {
            panic!("{name} must be readable from the Rust bundle");
        };
        let Ok(b) = std::fs::read(dir_python.join(name)) else {
            panic!("{name} must be readable from the Python bundle");
        };
        assert_eq!(
            a, b,
            "{name} must be byte-identical across the Rust and Python paths"
        );
    }

    // (b) The manifest is byte-identical after stripping `created_utc` (the sole
    // wall-clock field) and canonicalising the operational `output_dir` (the two
    // tempdirs legitimately differ) — exactly the #36 oracle normalisation.
    let manifest_rust = oracle::canonical_manifest(&oracle::read_manifest_json(&dir_rust));
    let manifest_python = oracle::canonical_manifest(&oracle::read_manifest_json(&dir_python));
    assert_eq!(
        manifest_rust, manifest_python,
        "manifest.json must be identical after stripping created_utc + canonicalising output_dir"
    );

    // (c) The single comparison oracle also passes on the decoded content: each
    // table sorted by its pinned key, integer cents exact, the one float column
    // within tolerance, and the manifest as canonical JSON with created_utc
    // excluded. Reuses `tests/oracle` — no forked comparator.
    let limits = ResourceLimits::default();
    let Ok(bundle_rust) = read_bundle(&dir_rust, &limits) else {
        panic!("the Rust bundle must read back through the hardened reader");
    };
    let Ok(bundle_python) = read_bundle(&dir_python, &limits) else {
        panic!("the Python bundle must read back through the hardened reader");
    };
    if let Err(diff) = oracle::compare_bundle_tables(&bundle_rust, &bundle_python) {
        panic!("Python↔Rust bundle table divergence: {diff}");
    }
    if let Err(diff) = oracle::compare_manifest_json(
        &oracle::read_manifest_json(&dir_rust),
        &oracle::read_manifest_json(&dir_python),
    ) {
        panic!("Python↔Rust manifest divergence: {diff}");
    }

    // Config-marshal equality (serialised boundary): the Python config demonstrably
    // marshalled to the same semantic BacktestConfig + strategy + seed as the Rust
    // API — asserted, not assumed. The struct-level `PyBacktestConfig::to_rust()`
    // equality for this shared scenario is pinned in `src/python/config.rs`.
    assert_eq!(
        marshalled_inputs(&dir_rust),
        marshalled_inputs(&dir_python),
        "the Python config must marshal to the same seed / semantic config / strategy as the Rust API"
    );

    // NOTE on the shared golden fixture: this test deliberately does NOT diff the
    // produced bundle against the committed `iron_condor_naive` golden. The
    // canonical scenario (chain generator + config) IS shared with that golden,
    // but the golden's `run_id` / `manifest` — and the `strategy_run_id` column it
    // stamps into `fills.parquet` — are pinned to canonical data-identity
    // constants (`bundle_golden.rs`) that a real run over a freshly generated
    // chain does not reproduce. The golden's frozen numeric content is instead
    // reproduced (excluding that one run-id column) by the pytest golden-table
    // comparison in `python/tests/test_parity.py`. Here the load-bearing claim is
    // Python == Rust for identical inputs, asserted above.
}
