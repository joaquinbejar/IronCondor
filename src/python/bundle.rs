//! The `#[pyclass] Bundle` handle and its lazy DataFrame accessors, plus
//! `load_bundle`.
//!
//! A [`Bundle`] is always **backed by an on-disk directory** (`bundle.path`) —
//! the finalized `<output_dir>/<run_id>/` `run()` published, or an existing
//! bundle opened by [`load_bundle`] ([docs/06 §6](../../docs/06-python-bindings.md)).
//! It carries no decoded data: the accessors are **lazy**, reading the on-disk
//! Parquet / JSON on call, so a caller who only wants [`Bundle::metrics`] never
//! decodes the four tables.
//!
//! # DataFrames via pandas (an optional soft dependency)
//!
//! The four table accessors return **pandas DataFrames** built by calling
//! `pandas.read_parquet(<table path>)` on demand — the on-disk format is plain
//! Parquet, so a `polars` / `pyarrow` user can also read `bundle.path` directly
//! and skip the accessors entirely ([ADR-0004](../../docs/adr/0004-parquet-result-bundle.md)).
//! `pandas` (and its `pyarrow` engine) is an **optional extra**
//! (`pip install ironcondor[pandas]`), not a hard wheel dependency, so the core
//! `run()` / `load_bundle` / `metrics()` surface works without it; a missing
//! pandas raises a clear `ImportError` naming the extra.
//!
//! # `metrics()` reads only the manifest
//!
//! [`Bundle::metrics`] parses the manifest JSON `metrics` object into a Python
//! `dict` — it reads `manifest.json` only, never the Parquet tables.
//!
//! # `load_bundle` validates through the hardened reader
//!
//! [`load_bundle`] validates the directory through [`crate::read_bundle`] — the
//! read-back **security gate** (schema tag, manifest fields, table schemas,
//! `row_counts` cross-check, `contract_id` round-trip, resource ceilings) — so a
//! hostile directory fails typed **before** a handle is returned; the accessors
//! then still read lazily from disk.

use std::path::PathBuf;

use pyo3::exceptions::PyImportError;
use pyo3::prelude::*;
use pyo3::types::PyModule;

use super::errors::{guard_boundary, to_pyerr};
use crate::ResourceLimits;
use crate::bundle::read_bundle;
use crate::error::BacktestError;

/// A bundle read-back / copy I/O failure, routed through the single mapping seam
/// so it surfaces as `ic.BundleError` (⊂ `IOError` ⊂ `ic.IronCondorError`) —
/// consistent with `load_bundle`, so `except ic.IronCondorError` around any
/// accessor catches a missing/corrupt manifest or a missing table.
fn bundle_err(py: Python<'_>, message: String) -> PyErr {
    to_pyerr(py, BacktestError::Bundle(message))
}

/// A handle to a finalized on-disk result bundle directory.
///
/// Construct one from [`run()`](super::run::run) or [`load_bundle`]; the
/// accessors read the on-disk Parquet / JSON lazily on each call.
#[pyclass(name = "Bundle", module = "ironcondor")]
pub struct Bundle {
    /// The finalized bundle directory (`<output_dir>/<run_id>/`).
    dir: PathBuf,
}

impl Bundle {
    /// Wrap a finalized bundle directory (used by `run()` and `load_bundle`).
    #[must_use]
    pub(crate) fn from_dir(dir: PathBuf) -> Self {
        Self { dir }
    }
}

#[pymethods]
impl Bundle {
    /// The on-disk bundle directory `run()` published (or `load_bundle` opened).
    #[getter]
    fn path(&self) -> String {
        self.dir.display().to_string()
    }

    /// `fills.parquet` as a `pandas.DataFrame` (columns exactly the frozen
    /// schema). Lazily read on call.
    ///
    /// # Errors
    ///
    /// Raises `ImportError` if pandas is not installed, or `ic.BundleError` (⊂
    /// `IOError`) if the table is missing or unreadable.
    fn fills<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.read_table(py, "fills.parquet")
    }

    /// `equity_curve.parquet` as a `pandas.DataFrame`. Lazily read on call.
    ///
    /// # Errors
    ///
    /// Raises `ImportError` if pandas is not installed, or `ic.BundleError` (⊂
    /// `IOError`) if the table is missing or unreadable.
    fn equity_curve<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.read_table(py, "equity_curve.parquet")
    }

    /// `positions.parquet` as a `pandas.DataFrame`. Lazily read on call.
    ///
    /// # Errors
    ///
    /// Raises `ImportError` if pandas is not installed, or `ic.BundleError` (⊂
    /// `IOError`) if the table is missing or unreadable.
    fn positions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.read_table(py, "positions.parquet")
    }

    /// `greeks_attribution.parquet` as a `pandas.DataFrame`. Lazily read on call.
    ///
    /// # Errors
    ///
    /// Raises `ImportError` if pandas is not installed, or `ic.BundleError` (⊂
    /// `IOError`) if the table is missing or unreadable.
    fn greeks_attribution<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.read_table(py, "greeks_attribution.parquet")
    }

    /// The manifest's `metrics` object as a `dict` (Sharpe, max drawdown,
    /// per-leg, …). Reads `manifest.json` only, never the Parquet tables.
    ///
    /// # Errors
    ///
    /// Raises `ic.BundleError` (⊂ `IOError`) if `manifest.json` is missing,
    /// unreadable, not JSON, or carries no `metrics` object.
    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        guard_boundary(|| {
            let manifest_path = self.dir.join("manifest.json");
            let text = std::fs::read_to_string(&manifest_path).map_err(|e| {
                bundle_err(py, format!("cannot read {}: {e}", manifest_path.display()))
            })?;
            let value: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| bundle_err(py, format!("manifest.json is not valid JSON: {e}")))?;
            let metrics = value
                .get("metrics")
                .ok_or_else(|| bundle_err(py, "manifest.json has no metrics object".to_string()))?;
            let metrics_json = serde_json::to_string(metrics)
                .map_err(|e| bundle_err(py, format!("cannot re-encode manifest metrics: {e}")))?;
            // Parse into native Python objects via the stdlib json module — a
            // metrics object becomes a plain dict.
            let json = py.import("json")?;
            json.call_method1("loads", (metrics_json,))
        })
    }

    /// **Deprecated-by-design copy alias.** `run()` already wrote the bundle;
    /// `write(dir)` only **copies** the finalized directory to `dir` (honouring
    /// `overwrite`) — it never re-runs the engine
    /// ([docs/06 §6](../../docs/06-python-bindings.md)). Returns a [`Bundle`]
    /// handle to the copy.
    ///
    /// # Errors
    ///
    /// Raises `ic.BundleError` (⊂ `IOError`) if `dir` already exists and
    /// `overwrite` is false, or on any copy I/O failure.
    #[pyo3(signature = (dir, overwrite = false))]
    fn write(&self, py: Python<'_>, dir: String, overwrite: bool) -> PyResult<Self> {
        guard_boundary(|| {
            // Emit a DeprecationWarning so callers migrate off the copy alias.
            emit_deprecation(
                py,
                "Bundle.write() is a deprecated copy alias: run() already wrote the bundle at \
                 bundle.path; write(dir) only copies it and never re-runs the engine.",
            )?;

            let dest = PathBuf::from(&dir);
            let dest_exists = dest
                .try_exists()
                .map_err(|e| bundle_err(py, format!("cannot stat {dir}: {e}")))?;
            if dest_exists {
                if !overwrite {
                    return Err(bundle_err(
                        py,
                        format!(
                            "destination {dir} already exists (pass overwrite=True to replace it)"
                        ),
                    ));
                }
                std::fs::remove_dir_all(&dest)
                    .map_err(|e| bundle_err(py, format!("cannot replace {dir}: {e}")))?;
            }
            std::fs::create_dir_all(&dest)
                .map_err(|e| bundle_err(py, format!("cannot create {dir}: {e}")))?;

            // Copy every regular file in the finalized bundle directory (manifest
            // + the four Parquet tables) — a flat directory, no subdirectories.
            let entries = std::fs::read_dir(&self.dir).map_err(|e| {
                bundle_err(
                    py,
                    format!("cannot read bundle {}: {e}", self.dir.display()),
                )
            })?;
            for entry in entries {
                let entry =
                    entry.map_err(|e| bundle_err(py, format!("cannot read bundle entry: {e}")))?;
                let file_type = entry
                    .file_type()
                    .map_err(|e| bundle_err(py, format!("cannot stat bundle entry: {e}")))?;
                if file_type.is_file() {
                    let target = dest.join(entry.file_name());
                    std::fs::copy(entry.path(), &target).map_err(|e| {
                        bundle_err(py, format!("cannot copy {:?}: {e}", entry.file_name()))
                    })?;
                }
            }
            Ok(Self::from_dir(dest))
        })
    }
}

impl Bundle {
    /// Read one bundle table into a `pandas.DataFrame` via `pandas.read_parquet`.
    ///
    /// Shared by the four table accessors; wrapped in [`guard_boundary`] so an
    /// unexpected panic (e.g. from within pandas) becomes `ic.EngineError`
    /// rather than crossing the FFI boundary.
    fn read_table<'py>(&self, py: Python<'py>, file: &str) -> PyResult<Bound<'py, PyAny>> {
        guard_boundary(|| {
            let path = self.dir.join(file);
            if !path.is_file() {
                return Err(bundle_err(
                    py,
                    format!("bundle table {} is missing", path.display()),
                ));
            }
            let path_str = path.to_str().ok_or_else(|| {
                bundle_err(
                    py,
                    format!("bundle path {} is not valid UTF-8", path.display()),
                )
            })?;
            // A missing pandas is a missing *optional dependency*, so it stays an
            // `ImportError` (not `ic.BundleError`) — install the extra, don't
            // treat it as a corrupt bundle.
            let pandas = import_pandas(py)?;
            pandas.call_method1("read_parquet", (path_str,))
        })
    }
}

/// Open an existing on-disk bundle in `dir`, validating it through the hardened
/// read-back gate ([`crate::read_bundle`]) before returning a handle.
///
/// The full validation (schema tag, manifest fields, table schemas, `row_counts`
/// cross-check, `contract_id` round-trip, resource ceilings) runs with the GIL
/// released; on success the accessors still read the tables lazily from disk.
///
/// # Errors
///
/// Raises `ic.BundleError` (⊂ `IOError`) if the directory is not a valid
/// `ironcondor.bundle.v1` bundle (a wrong schema tag, a malformed manifest /
/// table, a non-round-trippable `contract_id`, …) and `ic.DataError` (⊂
/// `IOError`) for a crossed resource ceiling — both descend from
/// `ic.IronCondorError`.
#[pyfunction]
pub fn load_bundle(py: Python<'_>, dir: String) -> PyResult<Bundle> {
    // `guard_boundary` outside the `detach` region: a panic in the GIL-released
    // reader is caught in Rust (GIL re-attached) before any Python re-entry.
    guard_boundary(|| {
        let path = PathBuf::from(&dir);
        let limits = ResourceLimits::default();
        // Validate (decode + check) with the GIL released (`detach`, the pyo3
        // 0.29 name for `allow_threads`) — pure-Rust I/O.
        py.detach(|| read_bundle(&path, &limits))
            .map_err(|e| to_pyerr(py, e))?;
        Ok(Bundle::from_dir(path))
    })
}

/// Import `pandas`, mapping a missing install to a clear `ImportError` naming the
/// optional extra.
fn import_pandas(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    py.import("pandas").map_err(|_| {
        PyImportError::new_err(
            "pandas is required for Bundle DataFrame accessors; install the optional extra with \
             `pip install ironcondor[pandas]` (pulls pandas + pyarrow). The bundle is plain \
             Parquet + JSON at bundle.path, so polars / pyarrow can read it directly instead.",
        )
    })
}

/// Emit a Python `DeprecationWarning` with `message`.
fn emit_deprecation(py: Python<'_>, message: &str) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    let category = py.get_type::<pyo3::exceptions::PyDeprecationWarning>();
    warnings.call_method1("warn", (message, category))?;
    Ok(())
}
