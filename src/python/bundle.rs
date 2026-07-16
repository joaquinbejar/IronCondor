//! Stub — the `#[pyclass] Bundle` handle and its DataFrame accessors.
//!
//! Placeholder for the bundle interop surface, filled by #39. It will wrap a
//! handle to the finalized on-disk bundle directory (`bundle.path`) and expose
//! lazy pandas-DataFrame accessors over the frozen v0.3 Parquet tables
//! ([docs/06 §6](../../docs/06-python-bindings.md)). This scaffold adds no
//! bundle interop and builds only against the frozen schema.
