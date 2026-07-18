//! Temporary `BacktestError` → Python exception mapping — the **#40 seam**.
//!
//! The typed exception hierarchy (`ic.IronCondorError` and its subclasses) plus
//! the `catch_unwind` boundary are #40's territory
//! ([docs/06 §5](../../docs/06-python-bindings.md)). Until then #39 needs *some*
//! mapping so a bad Python input surfaces as a Python exception rather than a
//! Rust `Result` no Python caller can inspect. This module provides that
//! interim mapping and nothing more:
//!
//! - config / input kinds (`Config`, `InvalidQuantity`, `CrossedQuote`,
//!   `PriceNotTickAligned`) → [`PyValueError`] (a wrong argument);
//! - every other kind → [`PyRuntimeError`] (a run / IO / bundle failure).
//!
//! **No panic crosses the FFI boundary**: every binding entry point (`run`,
//! `load_bundle`, the `Bundle` accessors, `to_rust`) drives the pure-Rust API,
//! which returns [`Result`]s, and maps the error through this function with `?`
//! / `map_err` — there is no `.unwrap()` / `.expect()` on the boundary. The
//! richer typed mapping and the `catch_unwind` net land in #40.

use pyo3::PyErr;
use pyo3::exceptions::{PyRuntimeError, PyValueError};

use crate::error::BacktestError;

/// Map a [`BacktestError`] to an interim [`PyErr`] from the error's `Display`.
///
/// This is the #40 seam: config / input errors become [`PyValueError`], every
/// other kind becomes [`PyRuntimeError`]. #40 replaces it with the typed
/// `ic.IronCondorError` hierarchy; the message text is preserved so no
/// information is lost in the interim.
#[must_use]
pub(crate) fn to_pyerr(err: BacktestError) -> PyErr {
    match err {
        BacktestError::Config(_)
        | BacktestError::InvalidQuantity(_)
        | BacktestError::CrossedQuote { .. }
        | BacktestError::PriceNotTickAligned { .. } => PyValueError::new_err(err.to_string()),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}
