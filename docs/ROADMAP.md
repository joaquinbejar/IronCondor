# Roadmap — `ironcondor`

| Field      | Value                                       |
|------------|---------------------------------------------|
| Status     | Living                                      |
| Last edit  | 2026-08-27                                  |

This is a living document. Each version has a tight scope and testable
acceptance criteria. New work that does not fit a version goes to the
[wishlist](#wishlist) or the [anti-roadmap](#anti-roadmap); it never
sneaks into an in-flight version. Scope here pairs one-to-one with
[PRD.md §6](PRD.md#6-scope) — the PRD frames the *what*, this document
sequences it and defines *done*. Per-version issue checklists below are
generated from the milestone tree under [`milestones/`](../milestones/);
each `### Issues` block mirrors that version's `README.md` table, and the
`#N` numbers are the **real, live GitHub issue numbers** (equal to the
local 3-digit id).

## Where we are

**Shipped and public.** `ironcondor` **v0.5.0** is tagged, released on
[crates.io](https://crates.io/crates/ironcondor) and published to
[PyPI](https://pypi.org/project/ironcondor/); the v0.0.1 name-reservation
placeholder is history. Everything from v0.1 to v0.5 is implemented and
merged: the deterministic replay loop, both fill models, the Parquet and
CSV feeds, the simulator feed, P&L attribution by Greek, the frozen
`ironcondor.bundle.v1`, the PyO3 bindings, and the v1.0 hardening gates.
The engine core was **migrated, not rewritten**, from the private
`OptionStratBacktest` project
([ADR-0001](adr/0001-migrate-optionstratbacktest-core.md)).

> **Status 2026-08-27:** All **54** roadmap issues are **closed**, plus the
> post-v0.5 hardening issue #110; 55 in total, delivered as one stacked
> chain of PRs #55–#113 (see the [changelog](#changelog) below). Every
> milestone (v0.1 … v1.0) is closed, with **zero open issues and zero open
> PRs**, and `main` sits on the v0.5.0 tag. What remains is **not
> implementation work**:
>
> - **The v1.0 cut waits on the calendar and on the owner, not on code.**
>   The four public surfaces are frozen behind fail-closed CI snapshots
>   (#49–#53) and the cut itself is scripted (#54,
>   `scripts/release_check.sh`). What is left is the **one-quarter
>   no-breaking-change window**, confirmed per surface at cut time: it is a
>   release-time, user-gated value that [SEMVER.md](SEMVER.md) deliberately
>   does not assert in advance. In practice the four surfaces have been
>   frozen and CI-gated since the 2026-07-19 v0.5.0 release.
> - **Wheel coverage is incomplete on the index.** The v0.5.0 PyPI release
>   carries the macOS `arm64` `cp310-abi3` wheel plus the sdist; the Linux
>   `manylinux` and macOS `x86_64` wheels are **not** published. The wheel
>   matrix failed on the v0.5.0 tag run and the fix (`bd7313d`) landed
>   after it, so the release workflow still owes a green re-run; until
>   then everyone outside macOS-arm64 installs from the sdist and needs a
>   Rust toolchain.

Workflow rules for this repo: they governed the whole chain and still bind
new work. One issue per PR, sequential where a later issue builds on an
earlier one; `Closes #N` in the PR body; the full
[Pre-Submission Checklist](TESTING.md#10-pre-submission-checklist-binding)
per PR; an ADR under [adr/](adr/) for every non-obvious decision. The
result-bundle contract (v0.3 onward) is coordinated with
[ChainView](https://github.com/joaquinbejar/ChainView) in the same week
as any schema change.

## v0.1 — Core engine and naive fills

**Goal.** A deterministic vertical slice through every sub-domain: load
a historical chain, run an `IronCondor` strategy against it, fill naively
with configured slippage and fees, and emit an equity curve. This is the
thin end-to-end pipeline everything later hangs off
([00-design-bootstrap.md §11](00-design-bootstrap.md#11-approach-thin-slice-end-to-end-first)).

Migrated, not rewritten, from `OptionStratBacktest`: the two known
`MarketSimulator` bugs — `is_terminated()` returning `false` when no
session exists, and the hardcoded `SessionState::InProgress` on every
step — are fixed in flight during that migration (#6). The
`ChainResponse` → `OptionChain` conversion (#7) is net-new (it never
existed in `OptionStratBacktest`) and gates the replay loop, so it lands
early. A second strategy (`ShortStrangle`) and the CSV feed are **v0.2**
breadth, not v0.1 gates.

### Issues

- [x] #1 — Bootstrap the crate skeleton, module tree, lints, and CI (M; no dependencies)
- [x] #2 — Define BacktestError boundary and the BacktestConfig skeleton (M; depends on #1)
- [x] #3 — Implement integer-cents money newtypes and ContractKey (M; depends on #1, #2)
- [x] #4 — Add market-data and execution-record domain types (M; depends on #3)
- [x] #5 — Implement SimClock and the loop event model (M; depends on #3)
- [x] #6 — Migrate the OptionStratBacktest core and fix the two MarketSimulator bugs (L; depends on #2, #4)
- [x] #7 — Build the ChainResponse to OptionChain conversion layer (L; depends on #3, #4)
- [x] #8 — Define the DataFeed trait and feed-catalogue seam (M; depends on #4)
- [x] #9 — Implement the Parquet historical feed (the v0.1 release feed) (L; depends on #7, #8)
- [x] #10 — Add the Strategy trait and the optionstratlib adapter (M; depends on #6)
- [x] #11 — Wire IronCondor as the single v0.1 strategy (M; depends on #10)
- [x] #12 — Define the ExecutionModel trait and shared FillReport shape (M; depends on #4)
- [x] #13 — Implement the naive fill model (mid/spread + slippage + fees) (M; depends on #12)
- [x] #14 — Implement BacktestEngine::run, the normative replay state machine (L; depends on #5, #7, #10, #12)
- [x] #15 — Add the mark-to-market ledger and minimal equity curve (M; depends on #14)
- [x] #16 — Emit the equity curve plus minimal metrics (M; depends on #15, #11, #13)
- [x] #17 — Add the golden determinism test over the v0.1 artifacts (M; depends on #16)
- [x] #18 — Stand up the bench suite and record the naive-throughput baseline in BENCH.md (M; depends on #16)
- [x] #19 — Enforce the zero-steady-state-allocation replay-loop gate in CI (M; depends on #14, #18)
- [x] #20 — Wire cargo audit and cargo deny into CI and land SECURITY.md (S; depends on #1)
- [x] #21 — Harden the Parquet feed against malformed and hostile input (M; depends on #9)

Full per-issue specs: `milestones/v0.1-core-engine-naive-fills/` (local).

**Acceptance (the v0.1 thin slice — one strategy, one feed, one mode).**
- An `IronCondor` backtest over a **Parquet** chain dataset produces an
  equity curve end to end.
- **Determinism is asserted over the v0.1 artifacts only — the equity
  curve and the minimal metrics.** Same `(seed, config, data)` in one
  environment ⇒ a byte-identical equity curve (and identical minimal
  metrics); across environments ⇒ logically equivalent under the comparison
  oracle — a golden determinism test proves it in CI
  ([02-engine-architecture.md §7](02-engine-architecture.md#7-determinism-and-reproducibility)).
  The **full four-table result bundle + `manifest.json`** and their
  determinism land with the bundle writer at **v0.3** (#34/#36); v0.1 does
  not claim them.
- Naive fills are recorded with slippage-vs-mid and fees.
- `BENCH.md` carries a measured naive-mode throughput baseline (p50 / p99 /
  p99.9 via `hdrhistogram`); the bench suite exists **before any performance
  claim** appears in the docs.
- **Security / perf gates:** `cargo audit` + `cargo deny` green in CI; the
  Parquet feed rejects a malformed input with a typed error and a bounded
  resource ceiling (no panic / hang / OOM); the zero-alloc replay-loop gate
  passes.

(A second strategy `ShortStrangle` and the CSV feed are **v0.2** breadth,
carried on the same slice — see v0.2 below — not v0.1 release gates.)

## v0.2 — Realistic fills via option-chain-orderbook

**Goal.** The differentiator: route orders through a real options
matching engine so fills carry queue position, per-strike liquidity, and
market impact — switchable against v0.1's naive mode by one config field
([ADR-0002](adr/0002-order-book-level-fill-simulation.md)).

Queue position and market impact are **emergent** properties of routing
through the seeded book, not configured knobs. Latency is deliberately
**not** an issue here: v0.x is same-snapshot execution, and latency is a
post-1.0 candidate that would need sub-snapshot market state to have any
behavioural effect
([04-execution-models.md §8](04-execution-models.md#8-latency-deferred-same-snapshot-execution)).
This milestone also carries the two pieces of v0.1-deferred breadth — the
CSV feed (#27) and `ShortStrangle` as a second strategy (#28) — proving
the feed and strategy seams generalise without engine changes.

### Issues

- [x] #22 — Build the `option-chain-orderbook` adapter (L; depends on #12, #7)
- [x] #23 — Seed each strike's book from the chain snapshot (M; depends on #22)
- [x] #24 — Model queue position and market impact (L; depends on #23)
- [x] #25 — Implement normative between-snapshot book transitions (M; depends on #23)
- [x] #26 — Add the naive/realistic mode switch and cross-mode parity test (M; depends on #13, #24, #25)
- [x] #27 — Add the CSV historical feed (v0.2 breadth) (M; depends on #7, #8)
- [x] #28 — Add `ShortStrangle` as a second strategy (v0.2 breadth) (S; depends on #10)
- [x] #29 — Record realistic-mode overhead and gate the naive baseline in CI (M; depends on #26, #18, #19)

Full per-issue specs: `milestones/v0.2-realistic-fills-orderbook/` (local).

**Acceptance.**
- The same strategy code runs unchanged under both modes.
- Realistic fills reflect queue position and per-strike depth, not mid.
- `FillReport` is byte-shape-identical across modes (analytics can't tell
  which mode produced it).
- `BENCH.md` records the realistic-mode overhead ratio vs naive on a
  fixed scenario (measured, with run conditions).
- **Perf gate:** the naive-mode throughput bench from v0.1 stays within its
  tracked tolerance — a regression fails CI ([07 §6](07-performance-and-security.md#6-regression-gates-in-ci-before-v10)).

## v0.3 — P&L attribution by Greek and the result bundle

**Goal.** Decompose the equity curve into *why*, and freeze the portable
artifact both ChainView and notebooks consume.

The mark-to-market ledger enrichment (#30) gates both attribution and
metrics; the writer (#34) and the schema freeze (#36) turn each run into
the portable `ironcondor.bundle.v1` artifact — the first cross-repo wire
contract, governed by [SEMVER.md](SEMVER.md), so post-freeze changes are
SemVer-relevant and ChainView-coordinated.

### Issues

- [x] #30 — Enrich the mark-to-market ledger for per-step attribution (M; depends on #15)
- [x] #31 — Implement P&L attribution by Greek with an exact residual (L; depends on #30)
- [x] #32 — Populate summary metrics into the upstream backtesting types (M; depends on #30)
- [x] #33 — Define the result-bundle record types and manifest.json schema (M; depends on #31, #32)
- [x] #34 — Implement the result-bundle writer with atomic writes (L; depends on #33)
- [x] #35 — Harden bundle read-back against malformed and hostile bundles (M; depends on #34)
- [x] #36 — Freeze the bundle schema with golden round-trip tests and SemVer (M; depends on #34, #35)
- [x] #37 — Bench the bundle writer for linear time and bounded memory (M; depends on #34, #18)

Full per-issue specs: `milestones/v0.3-attribution-result-bundle/` (local).

**Acceptance.**
- Attribution components + residual sum **exactly** to each step's
  **mark-to-market P&L (`step_pnl = equity_n − equity_{n-1}`)** in integer
  cents — the reconciliation identity, which always holds; the residual is
  reported, never hidden, and exceeding its **advisory** threshold warns but
  does not fail or invalidate the run
  ([05 §3.1](05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
- Summary metrics populate the upstream result types.
- A written bundle round-trips (write → read → equal) in a golden test.
- The bundle schema is documented and frozen; ChainView's design docs
  describe it identically.
- **Security gate:** bundle **read-back** validates the `schema` tag,
  required fields, and referenced-input hashes with resource ceilings and
  typed errors — a malformed or hostile bundle fails cleanly, never panics
  ([07 §8](07-performance-and-security.md#8-untrusted-input-hardening)).
- **Perf gate:** the bundle writer is O(rows) single-pass; a write bench
  over growing runs shows linear time and bounded peak memory
  ([07 §3](07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).

## v0.4 — PyO3 bindings and PyPI

**Goal.** The same engine for Python quants: `pip install ironcondor`,
run a backtest, load a bundle as DataFrames — no Rust toolchain.

PyO3 is a thin layer over the public Rust API — the "Python quant" is a
first-class user of the *same* engine, not a second-class wrapper. This
milestone opens **only against the frozen v0.3 bundle schema** (#36); it
consumes `ironcondor.bundle.v1`, it does not move it. PyPI name
registration (#41) must precede the first wheel publish.

### Issues

- [x] #38 — Scaffold the feature-gated PyO3 module and maturin packaging (M; depends on #36)
- [x] #39 — Expose the Python API to define, run, and load backtests (L; depends on #38)
- [x] #40 — Map `BacktestError` to Python exceptions with no panic across FFI (M; depends on #39)
- [x] #41 — Register the PyPI name and wire Linux+macOS wheel CI (M; depends on #38)
- [x] #42 — Prove Python/Rust bundle parity (M; depends on #39, #40, #36)
- [x] #43 — Measure per-call PyO3 overhead vs the batch path (S; depends on #39, #18)

Full per-issue specs: `milestones/v0.4-pyo3-pypi/` (local).

**Acceptance.**
- Wheels install on Linux + macOS from PyPI (Windows is a wishlist item,
  see [PRD Q-6](PRD.md#8-open-questions)). *Delivered with a caveat: the
  wheel matrix and the trusted-publishing workflow are wired, but the
  v0.5.0 index release carries only the macOS `arm64` wheel plus the sdist;
  the Linux and macOS `x86_64` wheels await a green release run (see
  [Where we are](#where-we-are)).*
- A Python backtest produces a bundle identical in content to the Rust
  one for the same inputs.
- No panic can cross the PyO3 boundary — errors surface as Python
  exceptions. This is a **security gate**, not only ergonomics: a panic
  unwinding across FFI is a host-interpreter integrity issue
  ([07 §11](07-performance-and-security.md#11-pyo3-boundary-as-a-security-surface)).
- **Perf note:** per-call PyO3 overhead vs the batch path is measured and
  documented in `BENCH.md` so users know when to batch
  ([07 §3](07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).

## v0.5 — Synthetic data and ChainView replay integration

**Goal.** No historical data? Generate it. And close the loop with the
renderer.

Scenario sweeps fan out **parallel across runs, never within one run**.
Same-seed **synthetic** reproducibility stays blocked on an upstream
simulator seed channel (see the blocked acceptance criterion below); the
ChainView compatibility proof (#48) rides the **frozen** v0.3 schema
(#36).

### Issues

- [x] #44 — Add the simulator-feature reqwest client and DTOs (L; depends on #8, #7)
- [x] #45 — Materialise simulator sessions to a validated tape (M; depends on #44)
- [x] #46 — Implement scenario generation and Monte-Carlo sweeps (L; depends on #45)
- [x] #47 — Run reproducible batch sweeps over file feeds (M; depends on #46, #9)
- [x] #48 — Prove ChainView replay compatibility from shared fixtures (M; depends on #36, #46)

Full per-issue specs: `milestones/v0.5-synthetic-chainview/` (local).

**Acceptance.**
- A synthetic session drives a full backtest with no historical input;
  the run is materialised to a validated tape and is self-consistent
  end to end.
- **Partly delivered (upstream-gated): same-seed reproducibility of a
  synthetic session.** The seed channel now exists upstream:
  OptionChain-Simulator v0.1.0 accepts a walk `seed` on
  `CreateSessionRequest`, and #44/#45 wire it end to end. `data_seed` is
  sent as `session.seed`, the effective seed is recorded, and the tape is
  identified by the `sha256` of its materialised bytes. **Tape identity is
  still not claimed reproducible:** upstream stamps every `ChainResponse`
  with a wall-clock timestamp, so the same seed repeats the *walk*, not the
  tape hash. The closing assertion is the `SIM_LIVE` #45 test, expected to
  fail until upstream serves deterministic timestamps
  ([03-data-layer.md §6](03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
- A batch sweep over **file** feeds runs N scenarios reproducibly,
  parallel across runs (file feeds are content-addressed and repeatable).
- A `ironcondor` bundle renders in `chainview` from shared fixtures with
  zero conversion.

## v1.0 — Stability commitment

Promote to 1.0 once each public surface has shipped without a breaking
change for one quarter:

- **Result bundle:** `manifest.json` fields + Parquet table schemas +
  the `"ironcondor.bundle.v1"` tag stable for one quarter; the primary
  cross-repo contract with ChainView.
- **Rust public API:** the re-exports from `src/lib.rs`
  (`BacktestEngine`, `BacktestConfig`, the `Strategy`/`DataFeed`/
  `ExecutionModel` seams) SemVer-stable for one quarter.
- **Python API:** the PyO3 surface stable for one quarter; wheels build
  green on Linux + macOS.
- **Config surface:** `BacktestConfig` fields + any env vars stable.
- **Determinism:** the same-seed-same-bundle guarantee proven by a
  golden suite that has caught at least one real regression.
- **Performance:** hot-path regression gates wired into CI against the
  `BENCH.md` baselines; a merge that regresses a tracked percentile beyond
  tolerance fails ([07 §6](07-performance-and-security.md#6-regression-gates-in-ci-before-v10)).
- **Security:** fuzz targets for the CSV / Parquet / bundle-read-back
  parsers landed and green — malformed bytes yield a typed error or a valid
  parse, never a panic / hang / OOM; adversarial fixtures committed
  ([07 §13](07-performance-and-security.md#13-fuzzing-before-v10)). Supply-chain
  gates (`cargo audit` / `cargo deny`) green throughout.

v1.0 hardens and gates surfaces already built across v0.1 → v0.4 — no new
engine capability lands in this milestone.

### Issues

- [x] #49 — Audit and freeze the public surfaces for SemVer 1.0 (M; depends on #36, #39)
- [x] #50 — Harden the determinism golden suite to catch real regressions (M; depends on #36, #17)
- [x] #51 — Wire hot-path regression gates into CI against BENCH.md baselines (M; depends on #29, #37, #43)
- [x] #52 — Land fuzz targets for the CSV, Parquet, and bundle-read-back parsers (L; depends on #27, #9, #35)
- [x] #53 — Prove secrets never leak and supply-chain gates stay green (S; depends on #20, #40)
- [x] #54 — Execute the v1.0 acceptance checklist and release cut (M; depends on #49, #50, #51, #52, #53)

Full per-issue specs: `milestones/v1.0-stability/` (local).

**Status.** Every gate above is landed and green as of v0.5.0: the surface
snapshots fail CI on drift, the determinism golden suite covers the full
four-table bundle, the hot-path gates H1–H5 run against the `BENCH.md`
baselines, the parser fuzz targets are committed, and the acceptance
checklist is scripted. The cut itself is held only by the one-quarter
stability window (see [Where we are](#where-we-are)).

The mechanics of the cut live in
[RELEASE-PROCESS.md §11](RELEASE-PROCESS.md); the surface definitions
live in [SEMVER.md](SEMVER.md).

## Post-v0.5 — unscheduled hardening (merged)

Work filed after the milestone tree and landed outside it:

- [x] #110 — Realistic-mode resting-order lifecycle: refresh-fill identity,
  `Cancel`/`Replace`, book eviction, and batch error-descriptor caching
  (L; merged in [#112](https://github.com/joaquinbejar/IronCondor/pull/112),
  shipped in v0.5.0; IOC-only runs stay byte-identical and all goldens
  unchanged)

## Post-1.0 — deferred by decision

Explicitly out of v0.x and v1.0, on the roadmap only after 1.0:

- **Reinforcement-learning layer.** Agent training on top of the engine
  is deferred to post-1.0 ([ADR-0006](adr/0006-rl-layer-deferred.md)).
  None of OptionStratBacktest's empty `rl` module migrates. The engine
  is deterministic, seedable, and step-driven precisely so the RL layer
  can attach later without a rewrite — but no RL code ships before then.
- **Distributed runs.** Multi-machine parameter sweeps are post-1.0;
  v0.x targets a single machine. The across-runs parallelism from v0.5
  is the on-ramp.

## Dependency notes

- **#7 (chain conversion) gates the loop (#14) and the orderbook adapter
  (#22).** Nothing routes or reprices until the DTO becomes an
  `OptionChain`. Land it early.
- **#22 gates #23–#26** — seeding, queue position, market impact, and the
  between-snapshot book transition all sit on the adapter (latency is
  deferred and carries no issue).
- **The v0.3 bundle freeze (#34/#36) gates #38–#39 and #48** — the Python
  API loads bundles and ChainView renders them, so both depend on a stable
  schema. Do not open v0.4 against an unfrozen bundle.
- **PyPI name registration (#41) preceded the first wheel publish**, as
  required (see [PRD Q-5](PRD.md#8-open-questions)). The name is registered
  and `ironcondor` 0.5.0 is on the index; the outstanding piece is wheel
  breadth, not the name (see [Where we are](#where-we-are)).
- **Benchmark obligations.** The naive baseline (#18) and the realistic
  overhead ratio (#29) carry criterion + hdrhistogram measurements. No
  performance claim ships without numbers in `BENCH.md`
  ([TESTING.md §11](TESTING.md#11-benchmarks)). The **bench suite lands
  before any performance claim** appears in the docs; the **regression
  gate** (#51) wraps it before v1.0.
- **Security-gate sequencing.** `cargo audit` + `cargo deny` are wired at
  **v0.1** (#20), not late; untrusted-input hardening lands with each feed
  and the bundle reader (#21, #27, #35); **parser fuzzing (#52) lands
  before v1.0**. The full posture and threat model live in
  [07-performance-and-security.md](07-performance-and-security.md)
  ([ADR-0007](adr/0007-production-grade-performance-and-security.md)).

## Anti-roadmap

What `ironcondor` explicitly will **not** become:

- A live-trading system or broker order router. The backtester ends at
  the bundle.
- A charting or terminal UI —
  [ChainView](https://github.com/joaquinbejar/ChainView) owns rendering.
- An exchange simulator as a service —
  [fauxchange](https://github.com/joaquinbejar/fauxchange) owns that.
- A general-purpose equities / futures / spot backtester. `ironcondor`
  is options-native by design; spot lives in vectorbt / hftbacktest.
- A market-data vendor. Users bring their own history or generate
  synthetic chains; data licensing is theirs.
- A reimplementation of pricing, Greeks, strategies, or matching — those
  are upstream ([optionstratlib](https://github.com/joaquinbejar/OptionStratLib),
  [option-chain-orderbook](https://github.com/joaquinbejar/Option-Chain-OrderBook)).
- A custom columnar format. We ride Parquet / Arrow.

## Wishlist

Ideas worth tracking but not scheduled:

- American-style early exercise / assignment modeling in backtests
  (heavy — see [PRD Q-4](PRD.md#8-open-questions)).
- Windows wheels alongside Linux + macOS.
- Portfolio-level, multi-strategy backtests (several strategies, one
  capital pool).
- Intraday tick-level chain replay if real per-strike options depth
  becomes available.
- A first-party pandas / polars bundle reader in the Python package.
- An optopsy-compatible convenience API to ease migration.
- Distributed parameter sweeps (post-1.0; on the deferred list above).

## Changelog

Merged work is recorded here: one row per merged PR (real date, issue
number, PR link, one-line summary), appended as each issue closes and its
checkbox is ticked above. The whole v0.1 → v0.5 chain landed as PRs
#55–#113; per-release notes with the detail live in
[CHANGELOG.md](../CHANGELOG.md).

| Date | Issue | PR | Summary |
|------|-------|----|---------|
| 2026-07-18 | #1 | [#55](https://github.com/joaquinbejar/IronCondor/pull/55) | Bootstrap the crate skeleton, module tree, lints, and CI |
| 2026-07-18 | #2 | [#56](https://github.com/joaquinbejar/IronCondor/pull/56) | Add BacktestError boundary and BacktestConfig skeleton |
| 2026-07-18 | #3 | [#57](https://github.com/joaquinbejar/IronCondor/pull/57) | Add integer-cents money newtypes and ContractKey |
| 2026-07-18 | #4 | [#58](https://github.com/joaquinbejar/IronCondor/pull/58) | Add market-data and execution-record domain types |
| 2026-07-18 | #5 | [#59](https://github.com/joaquinbejar/IronCondor/pull/59) | Implement SimClock and the loop event model |
| 2026-07-18 | #6 | [#60](https://github.com/joaquinbejar/IronCondor/pull/60) | Migrate OptionStratBacktest core and fix the two MarketSimulator bugs |
| 2026-07-18 | #7 | [#61](https://github.com/joaquinbejar/IronCondor/pull/61) | Add the ChainResponse to OptionChain conversion layer |
| 2026-07-18 | #8 | [#62](https://github.com/joaquinbejar/IronCondor/pull/62) | Define the DataFeed trait and feed-catalogue seam |
| 2026-07-18 | #9 | [#63](https://github.com/joaquinbejar/IronCondor/pull/63) | Implement the Parquet historical feed |
| 2026-07-18 | #10 | [#64](https://github.com/joaquinbejar/IronCondor/pull/64) | Add the Strategy trait and the optionstratlib adapter |
| 2026-07-18 | #11 | [#65](https://github.com/joaquinbejar/IronCondor/pull/65) | Wire IronCondor as the single v0.1 strategy |
| 2026-07-18 | #12 | [#66](https://github.com/joaquinbejar/IronCondor/pull/66) | Define the ExecutionModel trait and shared FillReport shape |
| 2026-07-18 | #13 | [#67](https://github.com/joaquinbejar/IronCondor/pull/67) | Implement the naive fill model |
| 2026-07-18 | #14 | [#68](https://github.com/joaquinbejar/IronCondor/pull/68) | Implement BacktestEngine::run, the normative replay state machine |
| 2026-07-18 | #15 | [#69](https://github.com/joaquinbejar/IronCondor/pull/69) | Add the mark-to-market ledger and minimal equity curve |
| 2026-07-18 | #16 | [#70](https://github.com/joaquinbejar/IronCondor/pull/70) | Emit the equity curve plus minimal metrics |
| 2026-07-18 | #17 | [#71](https://github.com/joaquinbejar/IronCondor/pull/71) | Add the golden determinism test over the v0.1 artifacts |
| 2026-07-18 | #18 | [#72](https://github.com/joaquinbejar/IronCondor/pull/72) | Stand up the bench suite and record the naive-throughput baseline in BENCH.md |
| 2026-07-18 | #19 | [#73](https://github.com/joaquinbejar/IronCondor/pull/73) | Enforce the zero-steady-state-allocation replay-loop gate in CI |
| 2026-07-18 | #20 | [#74](https://github.com/joaquinbejar/IronCondor/pull/74) | Wire cargo audit and cargo deny into CI and land SECURITY.md |
| 2026-07-18 | #21 | [#75](https://github.com/joaquinbejar/IronCondor/pull/75) | Harden the Parquet feed against malformed and hostile input |
| 2026-07-18 | #22 | [#76](https://github.com/joaquinbejar/IronCondor/pull/76) | Build the option-chain-orderbook adapter for realistic fills |
| 2026-07-18 | #23 | [#77](https://github.com/joaquinbejar/IronCondor/pull/77) | Seed each strike's order book from the chain snapshot |
| 2026-07-18 | #24 | [#78](https://github.com/joaquinbejar/IronCondor/pull/78) | Model queue position and market impact in realistic fills |
| 2026-07-18 | #25 | [#79](https://github.com/joaquinbejar/IronCondor/pull/79) | Rebuild seeded liquidity from every snapshot in realistic fills |
| 2026-07-18 | #26 | [#80](https://github.com/joaquinbejar/IronCondor/pull/80) | Add the naive/realistic mode switch and cross-mode parity test |
| 2026-07-18 | #27 | [#81](https://github.com/joaquinbejar/IronCondor/pull/81) | Add the CSV historical DataFeed (v0.2 breadth) |
| 2026-07-18 | #28 | [#82](https://github.com/joaquinbejar/IronCondor/pull/82) | Wire ShortStrangle as a second strategy through the adapter |
| 2026-07-18 | #29 | [#83](https://github.com/joaquinbejar/IronCondor/pull/83) | Record realistic-mode overhead and gate the naive baseline in CI |
| 2026-07-18 | #30 | [#84](https://github.com/joaquinbejar/IronCondor/pull/84) | Enrich the mark-to-market ledger for per-step attribution |
| 2026-07-18 | #31 | [#85](https://github.com/joaquinbejar/IronCondor/pull/85) | Implement P&L attribution by Greek with an exact residual |
| 2026-07-18 | #32 | [#86](https://github.com/joaquinbejar/IronCondor/pull/86) | Populate summary metrics into the upstream backtesting types |
| 2026-07-18 | #33 | [#87](https://github.com/joaquinbejar/IronCondor/pull/87) | Define the result-bundle record types and manifest.json schema |
| 2026-07-18 | #34 | [#88](https://github.com/joaquinbejar/IronCondor/pull/88) | Implement the result-bundle writer with atomic writes |
| 2026-07-18 | #35 | [#89](https://github.com/joaquinbejar/IronCondor/pull/89) | Harden bundle read-back against malformed and hostile bundles |
| 2026-07-18 | — | [#90](https://github.com/joaquinbejar/IronCondor/pull/90) | Correlate realistic multi-level fills into one order with fill_seq (prerequisite for #36) |
| 2026-07-18 | #36 | [#91](https://github.com/joaquinbejar/IronCondor/pull/91) | Freeze the bundle schema with golden round-trip tests and SemVer |
| 2026-07-18 | #37 | [#92](https://github.com/joaquinbejar/IronCondor/pull/92) | Bench the bundle writer for linear time and bounded memory |
| 2026-07-18 | #38 | [#93](https://github.com/joaquinbejar/IronCondor/pull/93) | Scaffold the feature-gated PyO3 module and maturin packaging |
| 2026-07-18 | #39 | [#94](https://github.com/joaquinbejar/IronCondor/pull/94) | Expose the Python API to define, run, and load backtests |
| 2026-07-18 | #40 | [#95](https://github.com/joaquinbejar/IronCondor/pull/95) | Map BacktestError to Python exceptions with no panic across FFI |
| 2026-07-18 | #41 | [#96](https://github.com/joaquinbejar/IronCondor/pull/96) | Register the PyPI name and wire Linux+macOS wheel CI |
| 2026-07-18 | #42 | [#97](https://github.com/joaquinbejar/IronCondor/pull/97) | Prove Python/Rust bundle parity |
| 2026-07-18 | #43 | [#98](https://github.com/joaquinbejar/IronCondor/pull/98) | Measure per-call PyO3 overhead vs the batch path |
| 2026-07-18 | #44 | [#99](https://github.com/joaquinbejar/IronCondor/pull/99) | Add the simulator-feature reqwest client and DTOs |
| 2026-07-18 | #45 | [#100](https://github.com/joaquinbejar/IronCondor/pull/100) | Materialise simulator sessions to a validated tape |
| 2026-07-18 | #46 | [#101](https://github.com/joaquinbejar/IronCondor/pull/101) | Implement scenario generation and Monte-Carlo sweeps |
| 2026-07-18 | #47 | [#102](https://github.com/joaquinbejar/IronCondor/pull/102) | Run reproducible batch sweeps over file feeds |
| 2026-07-18 | #48 | [#103](https://github.com/joaquinbejar/IronCondor/pull/103) | Prove ChainView replay compatibility from shared fixtures |
| 2026-07-18 | #49 | [#104](https://github.com/joaquinbejar/IronCondor/pull/104) | Freeze the four public surfaces with fail-closed CI gates |
| 2026-07-18 | #50 | [#105](https://github.com/joaquinbejar/IronCondor/pull/105) | Extend the determinism golden suite to the full four-table bundle |
| 2026-07-18 | #51 | [#106](https://github.com/joaquinbejar/IronCondor/pull/106) | Wire hot-path regression gates H1-H5 against BENCH.md baselines |
| 2026-07-18 | #52 | [#107](https://github.com/joaquinbejar/IronCondor/pull/107) | Land parser fuzz targets; fix two fuzzer-found Parquet panics |
| 2026-07-18 | #53 | [#108](https://github.com/joaquinbejar/IronCondor/pull/108) | Prove credentials never leak; reaffirm supply-chain gates |
| 2026-07-18 | #54 | [#109](https://github.com/joaquinbejar/IronCondor/pull/109) | Automate the v1.0 acceptance checklist and release-cut gates |
| 2026-07-18 | — | [#111](https://github.com/joaquinbejar/IronCondor/pull/111) | Close 24 verified stack-review findings in one pass (review response) |
| 2026-07-19 | #110 | [#112](https://github.com/joaquinbejar/IronCondor/pull/112) | Make resting GTC orders first-class in realistic mode |
| 2026-07-19 | — | [#113](https://github.com/joaquinbejar/IronCondor/pull/113) | Release v0.5.0 |
