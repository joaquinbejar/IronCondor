# Changelog

All notable changes to `ironcondor` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(the full versioning policy lives in the design docs, local until v0.1.0).

## [Unreleased]

### Added

- `BacktestError` (thiserror): the single typed error boundary — every
  documented kind from the domain model, with lowercase messages carrying the
  offending value; upstream errors convert into these kinds in `src/error.rs`
  and nowhere else (#2).
- `BacktestConfig` skeleton with `validate()` as the untrusted-config seam
  (positive capital, per-field `ResourceLimits` hard caps, non-negative
  slippage fraction, no directory-escaping output path), `ResourceLimits`
  with documented defaults and hard caps, `FeeSchedule`, `SlippageModel`,
  `ExecutionMode`, and a minimal `DataSourceSpec` (Csv/Parquet) — all
  `deny_unknown_fields`; money fields are integer cents, never `f64` (#2).

- Crate skeleton: the layered `src/` module tree (`domain`, `engine`,
  `execution`, `data`, `analytics`, `bundle`, `python`, `error`, `config`) as
  compiling placeholders, `#![forbid(unsafe_code)]` + `#![warn(missing_docs)]`
  at the crate root, the four feature flags (`default`, `orderbook`,
  `simulator`, `python`), and the unconditional `rlib` + `cdylib` crate types
  (#1).
- Toolchain and lint policy: `rust-toolchain.toml` pinned to stable 1.97.0,
  `rustfmt.toml`, `clippy.toml`, and a root `Makefile` with the `pre-push`
  gate (#1).
- CI skeleton (`.github/workflows/ci.yml`): `fmt`, `clippy`, `test`, and
  `build-release` jobs on pinned runner and toolchain images with
  cancel-in-progress concurrency (#1).
- Design documentation for the planned backtester (`docs/`): PRD, roadmap,
  competitive analysis, domain model, engine architecture, data layer,
  execution models, analytics/reporting, Python bindings, and ADRs 0001–0006.

## [0.0.1] - 2026-07-12

### Added

- Name-reservation placeholder published to crates.io. No implementation code
  yet; `src/lib.rs` carries crate-level docs only. The roadmap begins at
  v0.1.0.

[Unreleased]: https://github.com/joaquinbejar/IronCondor/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/joaquinbejar/IronCondor/releases/tag/v0.0.1
