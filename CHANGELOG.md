# Changelog

All notable changes to `ironcondor` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(the full versioning policy lives in the design docs, local until v0.1.0).

## [Unreleased]

### Added

- `IronCondor` wired as the single v0.1 strategy: `StrategySpec` /
  `IronCondorSpec` (the manifest's strategy kind + parameters, money in
  integer cents, analytics `Decimal`) and `OptStratAdapter::from_spec`
  constructing the upstream `IronCondor` via its 17-arg `new` and mapping
  `StrategyError` to `BacktestError::Strategy` at the one construction seam.
  The four-leg condor entry emits four `Open` intents in one step, sourcing
  each `decision_mid` from the snapshot quote (never a repriced strategy), so
  the dormant upstream wall-clock reprice stays dormant. `ShortStrangle` is
  not wired (deferred to v0.2) (#11).

- The engine-facing `Strategy` seam (`src/engine/strategy.rs`): the `Strategy`
  trait (`on_start` / `exits` / `on_snapshot` / `on_end`, each appending into a
  caller-owned `&mut Vec<OrderCommand>` so PB-1 is satisfiable by the
  signature), the step-scoped `ChainContext` (snapshot, open inventory,
  pending orders, the sole seeded `ChaCha8Rng`, step), and `OptStratAdapter`
  wrapping any `PositionableStrategy` — the **single** owner of exit-policy
  evaluation, repricing the wrapped strategy and evaluating the configured
  `optionstratlib` `ExitPolicy` via the per-leg `check_exit_policy`, appending
  closing commands only (#10).
- `OpenPosition` and `PendingOrder` domain records — the engine's authoritative
  open-leg inventory and resting-order read models a strategy sees (#10).
- `rand_chacha` dependency (seeded `ChaCha8Rng` — the run's sole randomness
  source; `thread_rng` is never used) (#10).

- The Parquet historical feed (`src/data/historical.rs`, `ParquetFeed`) — the
  single v0.1 release feed. Reads one columnar file, groups rows into
  `ChainSnapshot`s by ascending `step`, funnels each through the single
  conversion boundary, and materialises a validated, strictly-ordered,
  immutable tape at construction; `next` is a pure in-memory read. Enforces
  `max_file_bytes`, `max_decompressed_bytes` (footer-declared **and**
  incremental), `max_steps`, and `max_contracts_per_snapshot` with no
  unbounded read, rejects a non-regular file before hashing, and pins the
  file `sha256` as the tape identity (#9).
- `BacktestError::Data(String)` for undecodable input bytes (truncated or
  corrupt Parquet footer / row group), distinct from `Conversion`, a semantic
  failure on data that decoded cleanly (#9).
- Dependencies for the columnar stack (shared with the future bundle writer
  per ADR-0004): `arrow 59` and `parquet 59` (both `default-features = false`,
  minimal features), `sha2 0.10`; dev-only `tempfile 3` (#9).

- The `DataFeed` seam (`src/data/feed.rs`): a trait exposing exactly
  `next` / `meta` / `tape_meta` (no peek/rewind — no look-ahead by
  construction), the engine-facing `TapeMeta` (`data_identity`, `non_empty`,
  `first_ts`, `final_step`) available before any writer exists so startup can
  derive `run_id`, and the `FeedKind` catalogue classifying each feed by
  feature flag and determinism source. Every feed materialises a validated,
  strictly-ordered, immutable tape before the loop and yields from it
  synchronously — `next` never blocks or `.await`s (#8).
- `DataSourceSpec` gains the feature-gated `Simulator` variant (session,
  base URL, intent-only `data_seed`, tape sha256, simulator version) (#8).

- The single chain-conversion boundary (`src/data/convert.rs`, hot path H3):
  a DTO-independent validation core `raw_quotes_to_snapshot` (strike
  positivity, tick alignment, crossed-quote rejection, the one
  `mid = floor_to_tick((bid + ask) / 2)`, `Days(n)` anchored once on the
  tape's first `ts_0`, NaN/inf-analytic rejection, `BTreeMap` assembly)
  reused by every feed; the simulator-gated `chain_response_to_snapshot`
  where f64 prices die once via banker's rounding; and
  `snapshot_to_option_chain`, the reverse view the strategy adapter
  reprices against (#7).
- Conversion property tests: strike preservation, tick-alignment
  rejection, and `Days(n) ⇒ ts_0 + n·86400e9 ns` UTC-calendar resolution
  independent of snapshot timing (#7).

- Migrated the `OptionStratBacktest` core (reviewed, not copied): the async
  `ApiClient` for OptionChain-Simulator sessions with locally defined wire
  DTOs, the `MarketSimulator` step wrapper, the `PositionableStrategy` bound
  (exactly `Positionable + Strategies + Optimizable`, no `Default`), and the
  `ScenarioType`/`ScenarioParams` data types — all simulator networking
  behind the `simulator` feature, absent from the default build (#6).
- Feature-gated dependencies for the `simulator` feature: `reqwest 0.12`
  (rustls, json), `tokio 1` (rt, macros), `serde_json 1` (#6).

### Fixed

- `MarketSimulator::is_terminated()` now returns `true` when no session
  exists (was `false` in the migrated source — a loop built on it could
  never start safely) (#6).
- `MarketSimulator` per-step session state is derived from the server's
  actual `session_info` step counters (`from_progress`) and the wire state
  string on session creation (`from_wire`) instead of a hardcoded
  `SessionState::InProgress` (#6).

- `SimClock` (`src/engine/clock.rs`): the deterministic, no-wall-clock time
  source the replay loop advances — `advance_to` rejects a duplicate or
  reversed timestamp as `DataOutOfOrder` (gaps preserved, never reordered);
  and the conceptual `Event` model (`Snapshot`/`Decision`/`Fill`)
  documenting the canonical per-step order with exits strictly before
  entries; `clock_monotonic` property test (#5).

- Market-data types: `InstrumentSpec` (validated `> 0` tick size and
  contract multiplier — the single authoritative cents↔ticks / scaling
  source), `QuoteView` (per-contract unit Greeks in native `optionstratlib`
  bases, `Decimal` analytics as the documented exception), and
  `ChainSnapshot` with `BTreeMap` quotes so iteration order is part of the
  determinism contract (#4).
- Execution-record types: lifecycle ids (`PositionId`, `OrderId`,
  `TradeId`), `PositionAction`, `TimeInForce`, `OrderIntent`,
  `OrderCommand`, and the mode-agnostic `Fill` carrying
  `mode: ExecutionMode`; `ExecutionMode` moved to `domain::execution` (#4).
- The `sign_convention` helper module encoding the one truth table:
  `side_sign`, checked `slippage_cents` (positive = adverse, both sides
  tested), and `spread_capture_cents = −Σ slippage` — the single sign flip
  (#4).
- Exact hand-written `Ord` on `ContractKey` (consistent with its exact
  `Eq`/`Hash`) so `BTreeMap` snapshot ordering is deterministic (#4).

- Canonical domain newtypes: `Cents` (signed ledger money, checked
  arithmetic returning `ArithmeticOverflow` — never a silent wrap),
  `PriceCents` with the two `Decimal` boundary helpers (banker's rounding on
  the `Decimal → cents` edge only, lossless reverse), `Quantity` (strictly
  positive), `Ticks`, `SimTime`, `StepIndex` — all `#[serde(transparent)]`
  bare scalars (#3).
- Contract identity: `Underlying` (grammar `^[A-Z0-9._]{1,32}$`),
  `ContractKey` reusing the upstream `ExpirationDate`/`OptionStyle` with
  hand-written **exact** equality/hashing (documented divergence from the
  upstream epsilon-tolerant semantics), and the round-trippable versioned
  `contract_id` (`"v1:{UNDERLYING}:{expiration_ns}:{strike_cents}:{style}"`)
  (#3).
- Property-test suite (`tests/property.rs`): money serde round-trips,
  no-silent-overflow cents arithmetic, `contract_id` round-trip identity,
  deterministic dollar→cents conversion (#3).

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
