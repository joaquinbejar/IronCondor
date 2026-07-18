"""Smoke test for the `ironcondor` extension-module scaffold (#38).

Verifies the built wheel imports and reports a `__version__` that matches the
distribution metadata maturin derived from the crate's `Cargo.toml` version, so
the Rust `env!("CARGO_PKG_VERSION")` and the wheel version are proven to agree
(structural parity, not a hand-copied string).
"""

import importlib.metadata

import ironcondor


def test_import_exposes_version_string() -> None:
    assert isinstance(ironcondor.__version__, str)
    assert ironcondor.__version__, "ironcondor.__version__ must be non-empty"


def test_version_matches_distribution_metadata() -> None:
    dist_version = importlib.metadata.version("ironcondor")
    assert ironcondor.__version__ == dist_version
