//! The `run(config) -> Bundle` entry point.
//!
//! The Python mirror of the Rust run: marshal the config under the GIL, release
//! the GIL for the pure-Rust engine + writer, then re-acquire to build the
//! [`Bundle`] handle to the finalized on-disk directory
//! ([docs/06 §3](../../docs/06-python-bindings.md)).
//!
//! # One write lifecycle, shared with the core
//!
//! `run()` drives the **public Rust API only** — [`crate::run_backtest`] then
//! [`crate::write_bundle`] — the exact same code path (and the same atomic
//! writer, collision check, and `overwrite` semantics) as a Rust
//! `run_backtest` + `write_bundle`. There is no "maybe in memory" variant: the
//! bundle is written to `<output_dir>/<run_id>/` inside `run()` and the returned
//! [`Bundle`] is a handle to that finalized directory
//! ([docs/06 §6](../../docs/06-python-bindings.md)).
//!
//! # GIL discipline
//!
//! Marshalling + validation happen under the GIL (`config.to_rust()`), then
//! `py.detach` releases the GIL around the CPU-bound engine and the bundle
//! I/O, so a batch of runs can be fanned out from Python threads. The strategy
//! is an upstream `optionstratlib` type, not a Python callback, so the loop
//! never re-acquires the GIL mid-run ([docs/06 §3](../../docs/06-python-bindings.md)).
//! No panic crosses the boundary: the engine and writer return [`Result`]s,
//! mapped to a typed Python exception through [`super::errors::to_pyerr`], and
//! the whole body runs inside [`super::errors::guard_boundary`] — a
//! `catch_unwind` **outside** the `detach` region that turns any unexpected
//! panic into `ic.EngineError` with the GIL re-attached.

use std::path::PathBuf;

use pyo3::prelude::*;

use super::bundle::Bundle;
use super::config::PyBacktestConfig;
use super::errors::{guard_boundary, to_pyerr};
use crate::error::BacktestError;
use crate::{run_backtest, write_bundle};

/// Run one backtest end to end and return a [`Bundle`] handle to the finalized
/// on-disk bundle directory (`bundle.path`).
///
/// Marshals `config` under the GIL, then releases the GIL for the pure-Rust
/// engine run **and** the atomic bundle write, exactly as the Rust API does.
///
/// # Errors
///
/// Raises `ic.ConfigError` (⊂ `ValueError`) for a config / input error (no data
/// source or strategy configured, an invalid field), `ic.DataError` (⊂
/// `IOError`) for a feed / conversion / tape failure, `ic.ExecutionError` for a
/// fill failure, `ic.BundleError` (⊂ `IOError`) for a bundle-write failure, and
/// `ic.EngineError` for an arithmetic overflow or an unexpected internal panic.
/// All descend from `ic.IronCondorError`.
#[pyfunction]
pub fn run(py: Python<'_>, config: &PyBacktestConfig) -> PyResult<Bundle> {
    // `guard_boundary` sits OUTSIDE the `detach` region below: a panic in the
    // GIL-released engine unwinds through `detach` (which re-attaches the GIL on
    // its drop guard) and is caught here, in Rust, before any Python re-entry.
    guard_boundary(|| {
        // Marshal + validate under the GIL.
        let (cfg, strategy, exit) = config.to_rust().map_err(|e| to_pyerr(py, e))?;

        // Release the GIL (`detach`, the pyo3 0.29 name for `allow_threads`):
        // the engine is CPU-bound pure Rust with no Python callbacks, and the
        // writer is pure-Rust I/O — the same code path as the Rust API, so the
        // bundle is byte-identical to an equivalent Rust run.
        let dest: PathBuf = py
            .detach(move || -> Result<PathBuf, BacktestError> {
                let run = run_backtest(&cfg, &strategy, exit)?;
                write_bundle(&run, &cfg, &strategy)
            })
            .map_err(|e| to_pyerr(py, e))?;

        Ok(Bundle::from_dir(dest))
    })
}
