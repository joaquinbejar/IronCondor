//! Stub — the `run(config) -> Bundle` entry point.
//!
//! Placeholder for the run surface, filled by #39. It will marshal the config
//! under the GIL, release the GIL with `py.allow_threads` around the pure-Rust
//! engine run, and return a `Bundle` handle to the finalized on-disk bundle
//! ([docs/06 §3](../../docs/06-python-bindings.md)).
