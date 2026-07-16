//! The `BacktestError` → Python-exception mapping and the no-panic FFI net.
//!
//! This module is the **single** place a [`BacktestError`] becomes a Python
//! exception ([docs/06 §5](../../docs/06-python-bindings.md)) — the FFI analogue
//! of the "upstream errors converge in `src/error.rs`, nowhere else" rule. No
//! other binding module maps a [`BacktestError`]; they call [`to_pyerr`].
//!
//! # The typed exception hierarchy
//!
//! Every exception the engine raises descends from a common base
//! [`IronCondorError`] (⊂ `Exception`), so a caller can `except
//! ic.IronCondorError` to catch **everything** the engine raises. Concrete
//! kinds also inherit the closest Python builtin so idiomatic `except` clauses
//! keep working:
//!
//! ```text
//! Exception
//! └── IronCondorError                (ic.IronCondorError — the common base)
//!     ├── ExecutionError             (single base)
//!     ├── StrategyError              (single base)
//!     ├── EngineError                (single base; also the catch_unwind target)
//!     ├── ConfigError    (+ ValueError)   → `except ValueError` also catches it
//!     ├── DataError      (+ OSError)       → `except IOError`/OSError catches it
//!     └── BundleError    (+ OSError)       → `except IOError`/OSError catches it
//! ```
//!
//! `IOError` is a Python-3 alias of `OSError`, so `except IOError` and `except
//! OSError` both catch [`DataError`]/[`BundleError`].
//!
//! # Multiple inheritance in PyO3 0.29
//!
//! PyO3's `create_exception!` and `PyErr::new_type` each take a **single** base
//! type, so they build the base and the single-base subclasses
//! ([`IronCondorError`], [`ExecutionError`], [`StrategyError`], [`EngineError`]).
//! The three double-based classes ([`ConfigError`], [`DataError`],
//! [`BundleError`]) cannot: a Python exception needs `(IronCondorError,
//! ValueError)` / `(IronCondorError, OSError)` as **two** bases. They are built
//! at module init with Python's `type(name, bases, namespace)` builtin — the
//! fully-safe, `#![forbid(unsafe_code)]`-clean path that accepts a bases tuple —
//! and cached in a [`PyOnceLock`] so [`to_pyerr`] can raise them by looking up
//! the cached type object. The C3 linearisation is consistent because both
//! bases descend from `Exception` with the same instance layout.
//!
//! # No panic crosses the FFI boundary
//!
//! Two complementary guarantees:
//!
//! - **Expected failures** — the pure-Rust API returns [`Result`]s; every
//!   binding entry point maps the error with [`to_pyerr`] (`?` / `map_err`),
//!   never `.unwrap()` / `.expect()`.
//! - **Unexpected panics** — every boundary function body is wrapped in
//!   [`guard_boundary`], a `catch_unwind` that converts an unforeseen Rust panic
//!   into [`EngineError`] (so `except ic.IronCondorError` catches it) with the
//!   panic message. The wrapper sits **outside** any `py.detach` region, so a
//!   panic that unwinds out of the GIL-released engine is re-attached to the GIL
//!   by `detach`'s drop guard before it reaches the `catch_unwind` — the PyErr
//!   is constructed with the GIL held.
//!
//! PyO3 0.29's `#[pyfunction]` trampoline **already** catches panics and raises
//! `pyo3_runtime.PanicException`, but that type descends from `BaseException`
//! (like `SystemExit`) — it is deliberately *not* caught by `except Exception`
//! or `except ic.IronCondorError`. [`guard_boundary`] runs **inside** the
//! function, before the trampoline, so the panic becomes our
//! [`EngineError`] (an ordinary `Exception`) that the documented base clause
//! catches. Both nets are honest: the trampoline is the last-resort backstop;
//! [`guard_boundary`] is the contract.
//!
//! # Messages and secrets
//!
//! The raised exception preserves the [`BacktestError`] `Display` message
//! (lowercase, offending value included, per the coding rules). Upstream error
//! types carry **structured context, not raw request bodies**
//! ([docs/07 §9](../../docs/07-performance-and-security.md#9-secrets-handling)),
//! so a preserved message never embeds a simulator credential — this mapping
//! adds no scrubbing because there is nothing to scrub at this layer.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pyo3::PyTypeInfo;
use pyo3::exceptions::{PyException, PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};

use crate::error::BacktestError;

pyo3::create_exception!(
    ironcondor,
    IronCondorError,
    PyException,
    "Base class for every exception the ironcondor engine raises; `except \
     ironcondor.IronCondorError` catches them all."
);
pyo3::create_exception!(
    ironcondor,
    ExecutionError,
    IronCondorError,
    "A fill-model or order-book execution failure (BacktestError::Execution / \
     OrderBook)."
);
pyo3::create_exception!(
    ironcondor,
    StrategyError,
    IronCondorError,
    "A strategy-adapter failure surfaced from optionstratlib \
     (BacktestError::Strategy)."
);
pyo3::create_exception!(
    ironcondor,
    EngineError,
    IronCondorError,
    "An engine-internal failure: checked-arithmetic overflow \
     (BacktestError::ArithmeticOverflow) or an unexpected Rust panic caught at \
     the FFI boundary."
);

/// Docstring for [`ConfigError`], created at module init.
const CONFIG_ERROR_DOC: &str = "An invalid configuration or input value \
    (BacktestError::Config / InvalidQuantity / CrossedQuote / \
    PriceNotTickAligned). Also a ValueError.";
/// Docstring for [`DataError`], created at module init.
const DATA_ERROR_DOC: &str = "A data-source, conversion, tape, or session \
    failure (BacktestError::DataIo / Data / Conversion / DataOutOfOrder / \
    TapeTooLarge / Session). Also an OSError (IOError).";
/// Docstring for [`BundleError`], created at module init.
const BUNDLE_ERROR_DOC: &str = "A result-bundle write or read-back failure \
    (BacktestError::Bundle). Also an OSError (IOError).";

/// `ic.ConfigError` — `(IronCondorError, ValueError)`. Built once via `type()`.
static CONFIG_ERROR: PyOnceLock<Py<PyType>> = PyOnceLock::new();
/// `ic.DataError` — `(IronCondorError, OSError)`. Built once via `type()`.
static DATA_ERROR: PyOnceLock<Py<PyType>> = PyOnceLock::new();
/// `ic.BundleError` — `(IronCondorError, OSError)`. Built once via `type()`.
static BUNDLE_ERROR: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// Build a double-based exception class `name` with bases
/// `(IronCondorError, builtin_base)` and `__module__ = "ironcondor"`.
///
/// Uses Python's `type()` builtin because both `create_exception!` and
/// `PyErr::new_type` accept only a single base; `type()` accepts the bases
/// tuple and is `#![forbid(unsafe_code)]`-clean.
fn build_multi_base<'py>(
    py: Python<'py>,
    name: &str,
    builtin_base: &Bound<'py, PyType>,
    doc: &str,
) -> PyResult<Py<PyType>> {
    let ic_base = py.get_type::<IronCondorError>();
    let bases = PyTuple::new(py, [ic_base, builtin_base.clone()])?;
    let namespace = PyDict::new(py);
    namespace.set_item("__module__", "ironcondor")?;
    namespace.set_item("__doc__", doc)?;
    let type_ctor = py.import("builtins")?.getattr("type")?;
    let class = type_ctor.call1((name, bases, namespace))?;
    Ok(class.cast_into::<PyType>()?.unbind())
}

/// The `ic.ConfigError` type object (lazily built, then cached).
///
/// # Errors
///
/// Returns a [`PyErr`] only if the one-time class construction fails (an
/// out-of-memory interpreter) — never on the steady-state path.
pub(crate) fn config_error_type(py: Python<'_>) -> PyResult<Bound<'_, PyType>> {
    let cached = CONFIG_ERROR.get_or_try_init(py, || {
        build_multi_base(
            py,
            "ConfigError",
            &PyValueError::type_object(py),
            CONFIG_ERROR_DOC,
        )
    })?;
    Ok(cached.bind(py).clone())
}

/// The `ic.DataError` type object (lazily built, then cached).
///
/// # Errors
///
/// Returns a [`PyErr`] only if the one-time class construction fails.
pub(crate) fn data_error_type(py: Python<'_>) -> PyResult<Bound<'_, PyType>> {
    let cached = DATA_ERROR.get_or_try_init(py, || {
        build_multi_base(py, "DataError", &PyOSError::type_object(py), DATA_ERROR_DOC)
    })?;
    Ok(cached.bind(py).clone())
}

/// The `ic.BundleError` type object (lazily built, then cached).
///
/// # Errors
///
/// Returns a [`PyErr`] only if the one-time class construction fails.
pub(crate) fn bundle_error_type(py: Python<'_>) -> PyResult<Bound<'_, PyType>> {
    let cached = BUNDLE_ERROR.get_or_try_init(py, || {
        build_multi_base(
            py,
            "BundleError",
            &PyOSError::type_object(py),
            BUNDLE_ERROR_DOC,
        )
    })?;
    Ok(cached.bind(py).clone())
}

/// Map a [`BacktestError`] to its typed Python exception.
///
/// The `match` is **exhaustive with no wildcard arm**, so a future
/// [`BacktestError`] variant is a compile error until it is mapped here — the
/// mapping gate cannot silently regress. The exception message is the error's
/// `Display` (offending value preserved, no secret leaked).
#[must_use]
pub(crate) fn to_pyerr(py: Python<'_>, err: BacktestError) -> PyErr {
    let message = err.to_string();
    match err {
        // config / input → ConfigError (⊂ ValueError)
        BacktestError::InvalidQuantity(_)
        | BacktestError::CrossedQuote { .. }
        | BacktestError::PriceNotTickAligned { .. }
        | BacktestError::Config(_) => raise(config_error_type(py), message, PyValueError::new_err),
        // data / session → DataError (⊂ OSError/IOError)
        BacktestError::DataIo(_)
        | BacktestError::Data(_)
        | BacktestError::Conversion(_)
        | BacktestError::DataOutOfOrder { .. }
        | BacktestError::TapeTooLarge { .. }
        | BacktestError::Session(_) => raise(data_error_type(py), message, PyOSError::new_err),
        // execution → ExecutionError
        BacktestError::Execution(_) | BacktestError::OrderBook(_) => {
            ExecutionError::new_err(message)
        }
        // strategy adapter → StrategyError
        BacktestError::Strategy(_) => StrategyError::new_err(message),
        // bundle io → BundleError (⊂ OSError/IOError)
        BacktestError::Bundle(_) => raise(bundle_error_type(py), message, PyOSError::new_err),
        // engine-internal → EngineError
        BacktestError::ArithmeticOverflow => EngineError::new_err(message),
    }
}

/// Raise an instance of the double-based `ty`, or — if the impossible one-time
/// class build failed — fall back to the builtin base so `except ValueError` /
/// `except IOError` still catches it (degraded, but never a panic and never a
/// lost class of `except`).
fn raise(ty: PyResult<Bound<'_, PyType>>, message: String, fallback: fn(String) -> PyErr) -> PyErr {
    match ty {
        Ok(ty) => PyErr::from_type(ty, message),
        Err(_) => fallback(message),
    }
}

/// Run a boundary closure, converting any **unexpected** Rust panic into
/// [`EngineError`] instead of letting it unwind across FFI.
///
/// Expected typed errors flow through unchanged (the closure already maps them
/// with [`to_pyerr`]); only a genuine panic is intercepted. Place this at the
/// outermost level of a boundary function so it encloses any `py.detach`
/// region — the panic is caught in Rust, with the GIL re-attached, before any
/// Python re-entry.
pub(crate) fn guard_boundary<F, R>(f: F) -> PyResult<R>
where
    F: FnOnce() -> PyResult<R>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => Err(panic_to_engine_error(payload)),
    }
}

/// Convert a caught panic payload into an [`EngineError`], preserving the panic
/// message where the payload is a `&str` / `String` (the common cases).
#[cold]
fn panic_to_engine_error(payload: Box<dyn Any + Send>) -> PyErr {
    let detail = if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    };
    EngineError::new_err(format!("internal engine panic: {detail}"))
}

/// Register the exception hierarchy (and the internal panic-test hook) on the
/// module.
///
/// # Errors
///
/// Returns a [`PyErr`] if adding a class or function to the module fails, or if
/// building a double-based class fails at init.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    // Base + single-inheritance subclasses (create_exception! Rust types).
    module.add("IronCondorError", py.get_type::<IronCondorError>())?;
    module.add("ExecutionError", py.get_type::<ExecutionError>())?;
    module.add("StrategyError", py.get_type::<StrategyError>())?;
    module.add("EngineError", py.get_type::<EngineError>())?;
    // Double-based subclasses (type()-built, cached).
    module.add("ConfigError", config_error_type(py)?)?;
    module.add("DataError", data_error_type(py)?)?;
    module.add("BundleError", bundle_error_type(py)?)?;
    // The no-panic-across-FFI proof hook (see below).
    module.add_function(wrap_pyfunction!(_panic_for_test, module)?)?;
    Ok(())
}

/// **Internal test hook.** Deliberately panics inside a [`guard_boundary`] so a
/// test can prove an induced Rust panic surfaces as `ic.EngineError` (a
/// catchable `Exception`) rather than aborting the interpreter or leaking
/// PyO3's `BaseException`-derived `PanicException`.
///
/// It exercises the *exact same* [`guard_boundary`] wrapper the real boundary
/// functions use; there is no honest way to force the engine itself to panic on
/// demand (that is the property under test), so a dedicated hook is the truthful
/// mechanism. Underscore-prefixed and `#[doc(hidden)]` to mark it non-public.
///
/// # Errors
///
/// Always returns `ic.EngineError` carrying `message`.
#[doc(hidden)]
#[pyfunction]
pub(crate) fn _panic_for_test(message: String) -> PyResult<()> {
    guard_boundary(move || -> PyResult<()> {
        panic!("{message}");
    })
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use super::{
        EngineError, ExecutionError, IronCondorError, StrategyError, bundle_error_type,
        config_error_type, data_error_type, guard_boundary, to_pyerr,
    };
    use crate::error::BacktestError;
    use pyo3::exceptions::{PyException, PyOSError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::PyType;

    /// One instance of **every** `BacktestError` variant paired with the
    /// exception class name it must map to. Mirrors the exhaustive `to_pyerr`
    /// match; if a variant is added, `to_pyerr` fails to compile (no wildcard).
    fn every_variant() -> Vec<(BacktestError, &'static str)> {
        vec![
            (BacktestError::InvalidQuantity(0), "ConfigError"),
            (
                BacktestError::CrossedQuote { bid: 105, ask: 100 },
                "ConfigError",
            ),
            (
                BacktestError::PriceNotTickAligned {
                    price: 101,
                    tick: 5,
                },
                "ConfigError",
            ),
            (BacktestError::Config("bad capital".into()), "ConfigError"),
            (
                BacktestError::DataIo(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "missing.parquet",
                )),
                "DataError",
            ),
            (BacktestError::Data("truncated footer".into()), "DataError"),
            (BacktestError::Conversion("bad ticker".into()), "DataError"),
            (
                BacktestError::DataOutOfOrder {
                    step: 3,
                    ts: 900,
                    prev: 1_000,
                },
                "DataError",
            ),
            (
                BacktestError::TapeTooLarge {
                    limit: "max_steps",
                    value: 200,
                    cap: 100,
                },
                "DataError",
            ),
            (BacktestError::Session("http 500".into()), "DataError"),
            (
                BacktestError::Execution("oversized close".into()),
                "ExecutionError",
            ),
            (
                BacktestError::OrderBook("no liquidity".into()),
                "ExecutionError",
            ),
            (
                BacktestError::Strategy("upstream reject".into()),
                "StrategyError",
            ),
            (BacktestError::Bundle("write failed".into()), "BundleError"),
            (BacktestError::ArithmeticOverflow, "EngineError"),
        ]
    }

    fn type_name(py: Python<'_>, err: &PyErr) -> String {
        err.get_type(py)
            .name()
            .map(|n| n.to_string())
            .unwrap_or_default()
    }

    #[test]
    fn test_every_variant_maps_to_expected_exception_class() {
        Python::attach(|py| {
            for (err, expected) in every_variant() {
                let py_err = to_pyerr(py, err);
                assert_eq!(
                    type_name(py, &py_err),
                    *expected,
                    "unexpected exception class for a BacktestError variant"
                );
            }
        });
    }

    #[test]
    fn test_every_variant_is_an_ironcondor_error() {
        Python::attach(|py| {
            for (err, _) in every_variant() {
                let py_err = to_pyerr(py, err);
                assert!(
                    py_err.is_instance_of::<IronCondorError>(py),
                    "every engine exception must be an ic.IronCondorError"
                );
            }
        });
    }

    #[test]
    fn test_config_kinds_are_value_errors() {
        Python::attach(|py| {
            let cfg_kinds = [
                BacktestError::InvalidQuantity(0),
                BacktestError::CrossedQuote { bid: 2, ask: 1 },
                BacktestError::PriceNotTickAligned { price: 3, tick: 2 },
                BacktestError::Config("x".into()),
            ];
            for err in cfg_kinds {
                let py_err = to_pyerr(py, err);
                assert!(py_err.is_instance_of::<PyValueError>(py));
                assert!(py_err.is_instance_of::<IronCondorError>(py));
            }
        });
    }

    #[test]
    fn test_data_and_bundle_kinds_are_os_errors() {
        Python::attach(|py| {
            let os_kinds = [
                BacktestError::Data("x".into()),
                BacktestError::Conversion("x".into()),
                BacktestError::DataOutOfOrder {
                    step: 1,
                    ts: 1,
                    prev: 2,
                },
                BacktestError::TapeTooLarge {
                    limit: "max_steps",
                    value: 2,
                    cap: 1,
                },
                BacktestError::Session("x".into()),
                BacktestError::Bundle("x".into()),
            ];
            for err in os_kinds {
                let py_err = to_pyerr(py, err);
                assert!(py_err.is_instance_of::<PyOSError>(py));
                assert!(py_err.is_instance_of::<IronCondorError>(py));
            }
        });
    }

    #[test]
    fn test_single_base_kinds_are_not_value_or_os_errors() {
        Python::attach(|py| {
            let kinds = [
                BacktestError::Execution("x".into()),
                BacktestError::Strategy("x".into()),
                BacktestError::ArithmeticOverflow,
            ];
            for err in kinds {
                let py_err = to_pyerr(py, err);
                assert!(py_err.is_instance_of::<IronCondorError>(py));
                assert!(!py_err.is_instance_of::<PyValueError>(py));
                assert!(!py_err.is_instance_of::<PyOSError>(py));
            }
        });
    }

    #[test]
    fn test_message_is_preserved() {
        Python::attach(|py| {
            let py_err = to_pyerr(py, BacktestError::CrossedQuote { bid: 105, ask: 100 });
            let message = py_err.value(py).to_string();
            assert!(
                message.contains("crossed quote: bid 105 > ask 100"),
                "the offending value must survive the mapping, got {message:?}"
            );
        });
    }

    #[test]
    fn test_config_error_subclass_relationships() {
        Python::attach(|py| {
            let ty: Bound<'_, PyType> = config_error_type(py).expect("config error type builds");
            assert!(ty.is_subclass_of::<IronCondorError>().unwrap());
            assert!(ty.is_subclass_of::<PyValueError>().unwrap());
            assert!(ty.is_subclass_of::<PyException>().unwrap());
        });
    }

    #[test]
    fn test_data_error_subclass_relationships() {
        Python::attach(|py| {
            let ty: Bound<'_, PyType> = data_error_type(py).expect("data error type builds");
            assert!(ty.is_subclass_of::<IronCondorError>().unwrap());
            assert!(ty.is_subclass_of::<PyOSError>().unwrap());
        });
    }

    #[test]
    fn test_bundle_error_subclass_relationships() {
        Python::attach(|py| {
            let ty: Bound<'_, PyType> = bundle_error_type(py).expect("bundle error type builds");
            assert!(ty.is_subclass_of::<IronCondorError>().unwrap());
            assert!(ty.is_subclass_of::<PyOSError>().unwrap());
        });
    }

    #[test]
    fn test_single_base_types_subclass_the_base() {
        Python::attach(|py| {
            assert!(
                py.get_type::<ExecutionError>()
                    .is_subclass_of::<IronCondorError>()
                    .unwrap()
            );
            assert!(
                py.get_type::<StrategyError>()
                    .is_subclass_of::<IronCondorError>()
                    .unwrap()
            );
            assert!(
                py.get_type::<EngineError>()
                    .is_subclass_of::<IronCondorError>()
                    .unwrap()
            );
        });
    }

    #[test]
    fn test_guard_boundary_converts_panic_to_engine_error() {
        Python::attach(|py| {
            let result: PyResult<()> = guard_boundary(|| -> PyResult<()> {
                panic!("boom from rust");
            });
            let py_err = result.expect_err("a panic must surface as an error");
            assert!(
                py_err.is_instance_of::<EngineError>(py),
                "an induced panic must become ic.EngineError"
            );
            assert!(py_err.is_instance_of::<IronCondorError>(py));
            let message = py_err.value(py).to_string();
            assert!(
                message.contains("boom from rust"),
                "the panic message must be preserved, got {message:?}"
            );
        });
    }

    #[test]
    fn test_guard_boundary_passes_ok_through() {
        let result: PyResult<u8> = guard_boundary(|| Ok(42));
        assert!(matches!(result, Ok(42)));
    }
}
