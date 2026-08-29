//! Python bindings (feature `python`).
//!
//! A thin PyO3 layer over the **public Rust API** and nothing more. The Rust
//! core never imports from this module; the dependency arrow points one way
//! ([CLAUDE.md](../../CLAUDE.md) module boundaries,
//! [docs/06 §2](../../docs/06-python-bindings.md)). This scaffold (#38) wires
//! the *shape*, not the API: the `#[pymodule]` entry point plus stub submodules
//! ([`config`], [`run`], [`bundle`], [`errors`]) that #39/#40 fill in.
//!
//! ## What the `python` feature gates
//!
//! The `python` feature gates the **PyO3 dependency and this code**, **not the
//! crate-type**. Cargo resolves `[lib] crate-type` *before* feature
//! unification, so a feature cannot toggle it — toggling the crate-type by
//! feature is **unrepresentable in Cargo**. The crate therefore declares
//! `crate-type = ["rlib", "cdylib"]` **unconditionally**: without `python`
//! the `cdylib` links no PyO3 and exports no Python module (an inert artifact,
//! and the crates.io build carries zero Python dependencies); with `python`
//! that same `cdylib` becomes the importable extension module maturin packages.
//! Do not try to move `crate-type` behind a feature — it will not work.
//!
//! ## `__version__` parity
//!
//! The module reports [`crate_version`] (`env!("CARGO_PKG_VERSION")`) as
//! `ironcondor.__version__`, so the Python surface and the crate share **one**
//! source of truth for the version — parity is structural, never hand-copied
//! ([docs/06 §9](../../docs/06-python-bindings.md)).
//!
//! ## No panic across the FFI boundary (#40)
//!
//! Every fallible boundary entry point ([`run::run`], [`bundle::load_bundle`],
//! the [`bundle::Bundle`] accessors, the fallible [`config`] builders) drives
//! the pure-Rust API — which returns [`Result`]s — and maps every error to its
//! **typed** Python exception through [`errors::to_pyerr`] (the single mapping
//! seam) with `?` / `map_err`, never `.unwrap()` / `.expect()`. Each boundary
//! body is additionally wrapped in [`errors::guard_boundary`], a `catch_unwind`
//! that converts an *unexpected* Rust panic into `ic.EngineError` — so no panic
//! crosses the boundary, whether foreseen (typed) or not (a bug).
//!
//! ## Doc-table parity (docs/06 §5)
//!
//! The mapping is verified against the **actual** [`crate::error::BacktestError`]
//! enum, and the docs/06 §5 table now carries all 15 variants (architect synced
//! it during the #40 review — including `PriceNotTickAligned` → `ic.ConfigError`
//! and `TapeTooLarge` / `Data` → `ic.DataError`, the rows the earlier draft
//! omitted). This exhaustive, wildcard-free `match` is the source of truth; a
//! new variant is a compile error here until it is mapped.
//!
//! ## What is exposed (and what is deferred)
//!
//! #39 fills the working surface: [`config::PyBacktestConfig`] (Python
//! `BacktestConfig`) with integer-cents builders, [`run::run`], and
//! [`bundle::Bundle`] with lazy DataFrame accessors + [`bundle::load_bundle`].
//! The exposed surface is a **Parquet** source and the **iron condor** strategy.
//! A CSV source is still a deferred composition-root dispatch, so no `data_csv`
//! builder exists. A short strangle and an explicit leg set are **not**: since
//! #117 [`crate::run_backtest`] accepts every [`crate::StrategySpec`] kind, so
//! the absence of `strategy_short_strangle` / `strategy_legs` is now a
//! not-yet-wrapped **binding**, not a missing Rust capability. Both execution
//! modes work (#26).

use pyo3::prelude::*;

mod bundle;
mod config;
mod errors;
mod run;

/// The crate version reported to Python as `ironcondor.__version__`.
///
/// Sourced from `env!("CARGO_PKG_VERSION")` so the Python surface and the
/// crate version can never drift.
#[must_use]
#[inline]
pub(crate) fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The `ironcondor` Python extension module.
///
/// The function name matches the crate's `[lib] name`, so PyO3 emits the
/// `PyInit_ironcondor` symbol maturin packages as an importable module. It
/// registers `__version__`, the `BacktestConfig` builder and `Bundle` handle
/// classes, the `run` / `load_bundle` functions (#39), and the typed exception
/// hierarchy rooted at `ic.IronCondorError` (#40).
///
/// # Why `pub`
///
/// The `#[pymodule]` macro derives a hidden companion module from this
/// function's visibility; making the function `pub` therefore also makes that
/// companion `pub`, which is what lets an **embedding** caller register the
/// module in the interpreter's init table via
/// [`pyo3::append_to_inittab!`](pyo3::append_to_inittab) before
/// `Python::initialize`. The determinism-parity integration test
/// (`tests/python_parity.rs`, #42) uses exactly that to drive the *real*
/// registered module in-process. This is a Rust-visibility change only: it does
/// **not** alter what the Python module exposes (the same classes / functions
/// are registered either way), and maturin still packages the identical
/// `PyInit_ironcondor` symbol.
///
/// # Errors
///
/// Returns a [`PyErr`] if registering an attribute, class, or function on the
/// module fails (the interpreter is out of memory or the module object is
/// invalid).
#[pymodule]
pub fn ironcondor(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", crate_version())?;
    module.add_class::<config::PyBacktestConfig>()?;
    module.add_class::<bundle::Bundle>()?;
    module.add_function(wrap_pyfunction!(run::run, module)?)?;
    module.add_function(wrap_pyfunction!(bundle::load_bundle, module)?)?;
    // The `ic.IronCondorError` hierarchy + the internal panic-test hook (#40).
    errors::register(module)?;
    Ok(())
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use super::{crate_version, ironcondor};
    use pyo3::Bound;
    use pyo3::types::PyModule;

    // NOTE: a runtime `import ironcondor` + `__version__` check needs a live
    // Python interpreter; because the shipped extension is built with
    // `pyo3/extension-module` (no libpython link), that assertion lives in
    // `python/tests/test_smoke.py` (pytest) and the CI `python-wheels` job,
    // run against the built wheel. These Rust tests assert what is checkable
    // without an interpreter: version parity and the entry-point signature.

    #[test]
    fn test_crate_version_matches_cargo_pkg_version() {
        assert_eq!(crate_version(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_crate_version_is_non_empty() {
        assert!(
            !crate_version().is_empty(),
            "the crate version reported to Python must be non-empty"
        );
    }

    #[test]
    fn test_pymodule_entry_point_has_expected_signature() {
        // Referencing the `#[pymodule]` entry point as a typed fn item proves
        // the module compiled under `#![forbid(unsafe_code)]` with the exact
        // signature PyO3's init glue expects. It is not called here (that
        // needs the GIL); the runtime init is exercised by the pytest smoke.
        let entry: fn(&Bound<'_, PyModule>) -> pyo3::PyResult<()> = ironcondor;
        let _ = entry;
    }
}
