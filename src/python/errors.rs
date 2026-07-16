//! Stub — the `BacktestError` → Python exception mapping.
//!
//! Placeholder for the error surface, filled by #40. It will map every
//! [`crate::error::BacktestError`] variant to a typed Python exception under a
//! common `ic.IronCondorError` base, and a `catch_unwind` at the boundary will
//! convert any unexpected panic into an exception rather than an interpreter
//! crash — no panic ever crosses the FFI boundary
//! ([docs/06 §5](../../docs/06-python-bindings.md)).
