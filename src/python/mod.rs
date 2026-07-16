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
//! ## No panic across the FFI boundary
//!
//! Later issues map every `BacktestError` to a typed Python exception and wrap
//! the run in `catch_unwind`, so no panic ever crosses the boundary
//! ([docs/06 §5](../../docs/06-python-bindings.md)). This scaffold exposes no
//! fallible call yet.

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
/// `PyInit_ironcondor` symbol maturin packages as an importable module. This
/// scaffold registers only `__version__`; the backtest surface (`run`,
/// `BacktestConfig`, `Bundle`, the exception hierarchy) is added by #39/#40.
///
/// # Errors
///
/// Returns a [`PyErr`] if registering an attribute on the module fails (the
/// interpreter is out of memory or the module object is invalid).
#[pymodule]
fn ironcondor(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", crate_version())?;
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
