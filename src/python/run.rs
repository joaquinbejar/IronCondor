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
//! `py.allow_threads` releases the GIL around the CPU-bound engine and the
//! bundle I/O, so a batch of runs can be fanned out from Python threads. The
//! strategy is an upstream `optionstratlib` type, not a Python callback, so the
//! loop never re-acquires the GIL mid-run ([docs/06 §3](../../docs/06-python-bindings.md)).
//! No panic crosses the boundary: the engine and writer return [`Result`]s,
//! mapped to a Python exception through [`super::errors::to_pyerr`].

use std::path::PathBuf;

use pyo3::prelude::*;

use super::bundle::Bundle;
use super::config::PyBacktestConfig;
use super::errors::to_pyerr;
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
/// Raises `ValueError` for a config / input error (no data source or strategy
/// configured, an invalid field) and `RuntimeError` for a data / execution /
/// bundle failure — the interim mapping until the typed hierarchy lands (#40).
#[pyfunction]
pub fn run(py: Python<'_>, config: &PyBacktestConfig) -> PyResult<Bundle> {
    // Marshal + validate under the GIL.
    let (cfg, strategy, exit) = config.to_rust().map_err(to_pyerr)?;

    // Release the GIL (`detach`, the pyo3 0.29 name for `allow_threads`): the
    // engine is CPU-bound pure Rust with no Python callbacks, and the writer is
    // pure-Rust I/O — the same code path as the Rust API, so the bundle is
    // byte-identical to an equivalent Rust run.
    let dest: PathBuf = py
        .detach(move || -> Result<PathBuf, BacktestError> {
            let run = run_backtest(&cfg, &strategy, exit)?;
            write_bundle(&run, &cfg, &strategy)
        })
        .map_err(to_pyerr)?;

    Ok(Bundle::from_dir(dest))
}
