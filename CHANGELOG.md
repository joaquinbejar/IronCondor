# Changelog

All notable changes to `ironcondor` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(the full versioning policy lives in the design docs, local until v0.1.0).

## [Unreleased]

### Added

- Supply-chain CI gates from v0.1: the `audit` job (`cargo audit --deny
  warnings`) and the `deny` job (`cargo deny check` — licences, bans, sources,
  advisories), both green on the current dependency set so a new RUSTSEC
  advisory or a disallowed licence fails the build. `deny.toml` encodes an
  **explicit** licence allow-list (MIT, Apache-2.0, BSD-2/3-Clause, BSL-1.0,
  CC0-1.0, Unicode-3.0, 0BSD, Zlib, bzip2-1.0.6) and a single documented
  advisory ignore — RUSTSEC-2024-0436 (`paste` unmaintained, transitive via
  `optionstratlib`'s numeric stack, not a vulnerability); `.cargo/audit.toml`
  mirrors that one ignore. `#![forbid(unsafe_code)]` in the shipped crate is
  intact (#20).

### Changed

- `SECURITY.md` status refreshed: implementation has landed (the v0.1 core
  engine and naive fill model), correcting the stale "no implementation code
  exists yet" framing; the report channel, scope, and disclosure expectation
  are unchanged (#20).

- The zero-steady-state-allocation replay-loop CI gate (`tests/zero_alloc.rs`,
  the `zero-alloc` CI job): a test-only per-thread counting allocator plus a
  sampling-strategy decorator measure the per-step-body (steps b–g) allocation
  delta between a warmup step and the last step over the **real**
  `OptStratAdapter<IronCondor>` and assert it is **zero**; a deliberately
  injected per-step allocation makes the delta non-zero, proving the gate
  bites. A build-failing invariant gate, distinct from the throughput bench
  (#19).

### Changed

- `OptStratAdapter::exits()` no longer rebuilds an `OptionChain` or reprices
  the wrapped strategy every step in v0.1: `underlying` is sourced directly
  from the snapshot scalar (byte-identical to the old
  `chain.underlying_price`), and the reprice — with its transitive upstream
  `Utc::now()` reach — is deferred behind `policy_reads_inner` (false for
  every v0.1 exit policy; re-enabled when a Greek-driven policy is wired).
  Output-preserving (the golden passes unblessed), this closes a wall-clock
  determinism reach on the replay path and removes ~44 heap allocations per
  step. The naive per-step throughput baseline was re-measured accordingly —
  p50 **2354 ns/step** (down from 4172), ≈ 25.5 × 10⁶ steps/min/core — and
  `BENCH.md` supersedes the #18 baseline that baked in the removed dead work
  (#19).

- The `criterion` + `hdrhistogram` bench suite (`benches/`, the `bench-hdr`
  convention) and the first **measured** performance baseline in `BENCH.md`:
  the naive-mode throughput bench (`benches/naive_throughput.rs`) drives the
  full `run_backtest` over a canonical 2048-step × 4-leg iron-condor Parquet
  chain, single strategy / single core, and reports `hdrhistogram`
  p50/p99/p99.9/p99.99 of per-run and per-step latency (not criterion's mean),
  with warmup and an explicit coordinated-omission disclosure (closed-loop
  back-to-back — CO does not apply). `BENCH.md` records the measured baseline,
  the full run-conditions block (CPU/cores/memory/OS/toolchain/`Cargo.lock`
  hash), and an interpretation block versus the docs/07 §3 PB-2 design target.
  This is the v0.1 baseline the #019 zero-alloc gate and the #051
  percentile-regression gate build on. `criterion` / `hdrhistogram` are
  dev-only (kept out of `cargo build`/`cargo test`), carry an audit note in
  `Cargo.toml` (both Apache-2.0 OR MIT), and leave `#![forbid(unsafe_code)]`
  in the crate intact (#18).
- The golden determinism test over the v0.1 artifacts
  (`tests/golden/iron_condor_naive/`): the committed equity curve and minimal
  metrics for the canonical `IronCondor` naive run, the single reusable
  comparison oracle (decode → sort by `step` → integer cents compared
  exactly, analytic floats within the docs/05 §12.5 tolerance), a
  same-environment run-twice byte-identity test, and a `BLESS=1` regeneration
  path so a deliberate engine change re-blesses the artifact in the same
  commit. Scoped to the equity curve + minimal metrics only — the four-table
  bundle and `manifest.json` golden land at v0.3 (#17).
- The `golden` CI job (`cargo test --test golden`); `proptest-regressions/`
  is now tracked so generated regression seeds are committed (#17).

- The v0.1 end-to-end slice is complete: `run_backtest`
  (`src/run.rs`) — a top-level composition root above both engine and
  analytics — ties `ParquetFeed` + `IronCondor` + `NaiveFill` + the ledger +
  metrics into "Parquet chain in, equity curve out", the v0.1 acceptance
  headline (#16).
- Minimal summary metrics (`src/analytics/metrics.rs`): per-step Sharpe,
  volatility, and total return, plus max drawdown as a ratio and a
  peak-to-trough cents magnitude — computed from the ledger's `EquityPoint`
  series and populated into the upstream
  `optionstratlib::backtesting::BacktestResult`
  (`general_performance`, `drawdown_analysis`, and `custom_metrics` for the
  cents magnitude), inventing no parallel result type. Per-Greek attribution,
  `manifest.json`, the four-table bundle, and the full trade/risk statistics
  remain v0.3 — their upstream structs are left defaulted with a doc note
  (#16).

- Mark-to-market ledger enrichment (`src/engine/ledger.rs`): `stale_mark`
  tracking via the engine-owned `PositionMark { position_id, mark, stale }`
  scratch (a held leg absent from a step carries its last-known mark and is
  flagged stale), exposed through `Ledger::position_marks()` for the v0.3
  position rows; and the expiry-settlement rule — a held leg missing at or
  after its own expiry instant is `BacktestError::DataOutOfOrder` (a
  settlement mark is mandatory then), while a merely sparse chain before
  expiry is tolerated by carry-forward. Property tests pin the two distinct
  invariants — cash changes only by fills and fees, and equity reconciles to
  `cash + Σ(mark × quantity × contract_multiplier × side_sign)` every step —
  plus the unclamped drawdown definition at zero and negative equity (#15).

- `BacktestEngine::run<F: DataFeed, X: ExecutionModel, S: Strategy>`
  (`src/engine/backtest.rs`) — the synchronous, single-threaded, monomorphised
  replay loop implementing the normative state machine: startup (materialise
  the tape, execute `on_start` intents against `S0` before step 0), per-step
  (snapshot → mark → exits strictly before entries → naive fills → ledger
  → one `EquityPoint`), and termination (`on_end` inside the final step, legs
  left open flagged `open_at_end`, never a synthetic terminal fill). Lifecycle
  ids are minted from seeded monotonic counters; the only randomness is the
  seeded `ChaCha8Rng`; no wall-clock, no `thread_rng`, no look-ahead. Returns a
  `BacktestRun` carrying the populated `optionstratlib::backtesting::BacktestResult`,
  the `EquityPoint` curve, and the open-at-end legs (#14).
- The minimal mark-to-market `Ledger` (`src/engine/ledger.rs`) — cash moves
  only by fills and fees, positions marked at the snapshot mid with last-known
  carry-forward, `EquityPoint` and unclamped drawdown emitted once per step
  (enriched with the rigorous invariants at #15) — and the `EquityPoint`
  domain record (#14).
- Property tests `no_look_ahead` (perturbing a future snapshot leaves the
  prefix byte-identical) and `same_seed_same_result` (an RNG-consuming strategy
  reproduces exactly), plus an end-to-end `ParquetFeed → run → BacktestResult`
  integration test over the canonical fixture (#14).

### Changed

- `Underlying` is interned as `Arc<str>` instead of `String`, so cloning a
  `ContractKey` (and every `Fill` / `OpenPosition` / `QuoteView` that owns one)
  is a refcount bump rather than a heap allocation — the warm replay-step body
  no longer allocates, the prerequisite for the PB-1 zero-allocation gate. The
  exact `Eq` / `Hash` / `Ord` semantics and the grammar validation are
  unchanged (#14).

- The naive fill model (`src/execution/naive.rs`, `NaiveFill`) — the fast v0.1
  execution mode and criterion throughput baseline. A pure function of the
  snapshot and config with no book, state, or randomness: it fills every
  `Submit` intent single-shot at the full quantity, reference price = the
  quote mid, with the configured `SlippageModel` (`None` / `FixedCents` /
  `SpreadFraction` / `SizeProportional`, all integer-cents and deterministic)
  applied on the adverse side (a buy crosses toward the ask, a sell toward the
  bid), and emits the shared `Fill` shape via the `assemble_fill` seam. A sell
  whose adverse offset exceeds the reference floors at a zero premium (an
  explicit clamp, never `saturating_sub`); `Cancel`/`Replace` produce no fills
  (naive keeps no resting book) (#13).

- The `ExecutionModel` seam (`src/execution/mod.rs`): the command→fill trait
  (`fill` appends into a caller-owned `&mut Vec<Fill>` so PB-1 is satisfiable
  by the signature; `mode()`), and the shared `assemble_fill` helper — the
  single place a `Fill` is stamped with its mode, its signed `slippage` (via
  the `sign_convention` helper, never reinvented), and its `fees` — so both
  fill models are byte-shape identical. The per-order-once fee split is
  expressed by a caller-supplied `FeeCharge {FirstFill, LaterFill}` (the
  domain `Fill` has no `fill_seq`; that ordinal lives on the bundle `FillRow`
  at v0.3) (#12).

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
