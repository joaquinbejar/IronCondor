# Roadmap — `ironcondor`

| Field      | Value                                       |
|------------|---------------------------------------------|
| Status     | Living                                      |
| Last edit  | 2026-07-12                                  |

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

Pre-code. The published `ironcondor` v0.0.1 on crates.io is a
name-reservation placeholder; nothing described below is implemented.
The `docs/` set is **under active review** — bootstrap, six numbered
design docs, the ADRs, and the specs are maintained locally during the
design phase and are the working source of truth until the first code
lands. The engine core will be **migrated, not rewritten**, from the
private `OptionStratBacktest` project
([ADR-0001](adr/0001-migrate-optionstratbacktest-core.md)).

> **Status 2026-07-12:** Design **under active review**, not frozen — the
> v0.1-relevant reviewer recommendations are being cleared before the
> design set is declared stable for v0.1. **The issues are filed and
> live.** All **54** issues for this repo are open on
> [github.com/joaquinbejar/IronCondor](https://github.com/joaquinbejar/IronCondor)
> — one GitHub issue per `milestones/**/NNN-*.md` spec, with the GitHub
> number equal to the local 3-digit id (`001` → #1, verified zero drift) —
> part of **167** issues across the three sibling repos (IronCondor,
> [ChainView](https://github.com/joaquinbejar/ChainView),
> [fauxchange](https://github.com/joaquinbejar/fauxchange)). Milestones
> v0.1 … v1.0, labels, and the assignee are set. **No implementation code
> exists yet** — the crates.io v0.0.1 is a placeholder, and every `#N`
> below is a live issue number, **not** a planned allocation. The first
> milestone is the v0.1 vertical slice: one strategy, one feed, one fill
> mode, end to end.

Workflow rules for this path: one issue per PR, sequential where a
later issue builds on an earlier one; `Closes #N` in the PR body; the
full [Pre-Submission Checklist](TESTING.md#10-pre-submission-checklist-binding)
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

- [ ] #1 — Bootstrap the crate skeleton, module tree, lints, and CI (M; no dependencies)
- [ ] #2 — Define BacktestError boundary and the BacktestConfig skeleton (M; depends on #1)
- [ ] #3 — Implement integer-cents money newtypes and ContractKey (M; depends on #1, #2)
- [ ] #4 — Add market-data and execution-record domain types (M; depends on #3)
- [ ] #5 — Implement SimClock and the loop event model (M; depends on #3)
- [ ] #6 — Migrate the OptionStratBacktest core and fix the two MarketSimulator bugs (L; depends on #2, #4)
- [ ] #7 — Build the ChainResponse to OptionChain conversion layer (L; depends on #3, #4)
- [ ] #8 — Define the DataFeed trait and feed-catalogue seam (M; depends on #4)
- [ ] #9 — Implement the Parquet historical feed (the v0.1 release feed) (L; depends on #7, #8)
- [ ] #10 — Add the Strategy trait and the optionstratlib adapter (M; depends on #6)
- [ ] #11 — Wire IronCondor as the single v0.1 strategy (M; depends on #10)
- [ ] #12 — Define the ExecutionModel trait and shared FillReport shape (M; depends on #4)
- [ ] #13 — Implement the naive fill model (mid/spread + slippage + fees) (M; depends on #12)
- [ ] #14 — Implement BacktestEngine::run, the normative replay state machine (L; depends on #5, #7, #10, #12)
- [ ] #15 — Add the mark-to-market ledger and minimal equity curve (M; depends on #14)
- [ ] #16 — Emit the equity curve plus minimal metrics (M; depends on #15, #11, #13)
- [ ] #17 — Add the golden determinism test over the v0.1 artifacts (M; depends on #16)
- [ ] #18 — Stand up the bench suite and record the naive-throughput baseline in BENCH.md (M; depends on #16)
- [ ] #19 — Enforce the zero-steady-state-allocation replay-loop gate in CI (M; depends on #14, #18)
- [ ] #20 — Wire cargo audit and cargo deny into CI and land SECURITY.md (S; depends on #1)
- [ ] #21 — Harden the Parquet feed against malformed and hostile input (M; depends on #9)

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

- [ ] #22 — Build the `option-chain-orderbook` adapter (L; depends on #12, #7)
- [ ] #23 — Seed each strike's book from the chain snapshot (M; depends on #22)
- [ ] #24 — Model queue position and market impact (L; depends on #23)
- [ ] #25 — Implement normative between-snapshot book transitions (M; depends on #23)
- [ ] #26 — Add the naive/realistic mode switch and cross-mode parity test (M; depends on #13, #24, #25)
- [ ] #27 — Add the CSV historical feed (v0.2 breadth) (M; depends on #7, #8)
- [ ] #28 — Add `ShortStrangle` as a second strategy (v0.2 breadth) (S; depends on #10)
- [ ] #29 — Record realistic-mode overhead and gate the naive baseline in CI (M; depends on #26, #18, #19)

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

- [ ] #30 — Enrich the mark-to-market ledger for per-step attribution (M; depends on #15)
- [ ] #31 — Implement P&L attribution by Greek with an exact residual (L; depends on #30)
- [ ] #32 — Populate summary metrics into the upstream backtesting types (M; depends on #30)
- [ ] #33 — Define the result-bundle record types and manifest.json schema (M; depends on #31, #32)
- [ ] #34 — Implement the result-bundle writer with atomic writes (L; depends on #33)
- [ ] #35 — Harden bundle read-back against malformed and hostile bundles (M; depends on #34)
- [ ] #36 — Freeze the bundle schema with golden round-trip tests and SemVer (M; depends on #34, #35)
- [ ] #37 — Bench the bundle writer for linear time and bounded memory (M; depends on #34, #18)

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

- [ ] #38 — Scaffold the feature-gated PyO3 module and maturin packaging (M; depends on #36)
- [ ] #39 — Expose the Python API to define, run, and load backtests (L; depends on #38)
- [ ] #40 — Map `BacktestError` to Python exceptions with no panic across FFI (M; depends on #39)
- [ ] #41 — Register the PyPI name and wire Linux+macOS wheel CI (M; depends on #38)
- [ ] #42 — Prove Python/Rust bundle parity (M; depends on #39, #40, #36)
- [ ] #43 — Measure per-call PyO3 overhead vs the batch path (S; depends on #39, #18)

Full per-issue specs: `milestones/v0.4-pyo3-pypi/` (local).

**Acceptance.**
- Wheels install on Linux + macOS from PyPI (Windows is a wishlist item,
  see [PRD Q-6](PRD.md#8-open-questions)).
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

- [ ] #44 — Add the simulator-feature reqwest client and DTOs (L; depends on #8, #7)
- [ ] #45 — Materialise simulator sessions to a validated tape (M; depends on #44)
- [ ] #46 — Implement scenario generation and Monte-Carlo sweeps (L; depends on #45)
- [ ] #47 — Run reproducible batch sweeps over file feeds (M; depends on #46, #9)
- [ ] #48 — Prove ChainView replay compatibility from shared fixtures (M; depends on #36, #46)

Full per-issue specs: `milestones/v0.5-synthetic-chainview/` (local).

**Acceptance.**
- A synthetic session drives a full backtest with no historical input;
  the run is materialised to a validated tape and is self-consistent
  end to end.
- **Blocked criterion (upstream-gated): same-seed reproducibility of a
  synthetic session.** OptionChain-Simulator has **no seed channel**
  today (`CreateSessionRequest` carries none; the server walk is unseeded,
  verified 2026-07-12) — so a synthetic run is not repeatable across
  sessions. This criterion is gated on the upstream **feature request:
  add a `seed` to `CreateSessionRequest`**; until it lands, v0.5 ships
  without the same-seed synthetic guarantee and the docs say so plainly
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

- [ ] #49 — Audit and freeze the public surfaces for SemVer 1.0 (M; depends on #36, #39)
- [ ] #50 — Harden the determinism golden suite to catch real regressions (M; depends on #36, #17)
- [ ] #51 — Wire hot-path regression gates into CI against BENCH.md baselines (M; depends on #29, #37, #43)
- [ ] #52 — Land fuzz targets for the CSV, Parquet, and bundle-read-back parsers (L; depends on #27, #9, #35)
- [ ] #53 — Prove secrets never leak and supply-chain gates stay green (S; depends on #20, #40)
- [ ] #54 — Execute the v1.0 acceptance checklist and release cut (M; depends on #49, #50, #51, #52, #53)

Full per-issue specs: `milestones/v1.0-stability/` (local).

The mechanics of the cut live in
[RELEASE-PROCESS.md §11](RELEASE-PROCESS.md); the surface definitions
live in [SEMVER.md](SEMVER.md).

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
- **PyPI name registration (#41) must precede the first wheel publish.**
  The name is unregistered today; register it against the earliest wheel
  that builds green (see [PRD Q-5](PRD.md#8-open-questions)).
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

Merged work is recorded here — `/implement-roadmap` **Step 7** appends one
row per merged PR (real date, issue number, PR link, one-line summary)
after each issue closes and its checkbox is ticked above. It starts empty:
no issue has been implemented yet (code does not exist).

| Date | Issue | PR | Summary |
|------|-------|----|---------|
| —    | —     | —  | Empty — the first row lands with the first merged PR (`/implement-roadmap` Step 7). |
