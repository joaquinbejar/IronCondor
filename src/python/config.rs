//! Stub — the `#[pyclass] BacktestConfig` wrapper and its builders.
//!
//! Placeholder for the Python config surface, filled by #39. It will wrap the
//! public [`crate::config::BacktestConfig`] and expose integer-cents builders
//! (`capital_cents`, `strike_cents`, an explicit `seed`) that marshal to the
//! Rust config unchanged ([docs/06 §4](../../docs/06-python-bindings.md)).
