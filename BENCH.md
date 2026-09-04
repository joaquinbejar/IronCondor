# BENCH.md — measured performance baselines

This file records the project's **measured** hot-path numbers. The rule is
absolute: **no number is written here before it is measured**
([docs/07 §5](docs/07-performance-and-security.md#5-benchmark-methodology),
[docs/TESTING.md §11](docs/TESTING.md#11-benchmarks)). Every entry is the raw
output of a `criterion` + `hdrhistogram` bench under `benches/`, reported as
`hdrhistogram` percentiles (p50 / p99 / p99.9 / p99.99), **not** criterion's
mean — a mean hides the tail a production sweep actually feels (the `bench-hdr`
convention). An entry without its run-conditions block is not a result.

Reproduce every entry with:

```bash
cargo bench --bench <name>
```

---

## H1 / PB-2 — Naive-mode throughput (v0.1 baseline)

- **Bench:** `benches/naive_throughput.rs` (`cargo bench --bench naive_throughput`)
- **Date measured:** 2026-07-16
- **What it measures:** the **full** [`run_backtest`] call — Parquet feed open +
  parse → replay loop → naive fills → mark-to-market ledger → summary metrics —
  over a canonical iron-condor chain, single strategy, single core, back to back.
- **Workload / scale:** a canonical Parquet chain of **2048 steps × 4 condor
  legs = 8192 quote rows**; the four legs (short call 510000 / long call 520000 /
  short put 490000 / long put 480000, tick-aligned to 5c) are held open across
  the whole run (a non-triggering `ExitPolicy::TimeSteps` so every step evaluates
  the exit seam and re-marks the four legs — the representative naive-mode
  per-step workload). Fixed seed (`42`); the tape stays strictly before expiry so
  every step marks in the normal pre-expiry regime.
- **Samples:** **2123** recorded runs (criterion warmup + measurement, all at
  steady state after an explicit 32-run warmup).

> **Supersedes the #18 baseline.** The first PB-2 baseline (p50 **4172 ns/step**,
> 8544.3 µs/run) was measured **with a now-removed dead per-step reprice**: the
> `IronCondor` adapter's `exits()` rebuilt an `OptionChain` via
> `snapshot_to_option_chain` every step (~44 alloc events/step) whose only
> consumed output was `underlying = chain.underlying_price` — derivable directly
> from `snapshot.underlying_price`. The #19 FOLD-THE-FIX verdict defers that
> reprice (output-preserving; the golden passed unblessed), so the numbers below
> are re-measured on the real path without the dead work. The old number is
> **not** a valid PB-2 baseline and is retained here only as the superseded
> reference.

### Measured percentiles

Per-run latency is the raw measured quantity (each timed iteration is one full
`run_backtest` over the 2048-step chain). Per-step is that latency divided by the
fixed 2048 steps — a fair per-step figure because every run processes exactly the
same step count.

| Metric | p50 | p99 | p99.9 | p99.99 |
|--------|-----|-----|-------|--------|
| **Per-run latency** | 4821.0 µs (4.82 ms) | 5185.5 µs (5.19 ms) | 7110.7 µs (7.11 ms) | 9666.6 µs (9.67 ms) |
| **Per-step latency** | **2354 ns/step** | 2532 ns/step | 3472 ns/step | — |

- **Throughput (from the p50 per-run latency):** **≈ 424,800 steps/sec/core =
  ≈ 25.49 × 10⁶ steps/min/core.**
- **Per-step cost (p50):** ≈ **2.35 µs/step** (four legs re-marked per step, no
  per-step chain rebuild).
- **criterion cross-check** (mean/median, the harness's own estimate, for
  sanity): time `[4.8068 ms 4.8268 ms 4.8481 ms]`,
  throughput `[422.43 Kelem/s 424.30 Kelem/s 426.07 Kelem/s]` — consistent with
  the hdrhistogram p50, which validates the measurement. criterion also reports
  the change vs the superseded baseline as **−43.7%** ("Performance has
  improved"), i.e. the dead reprice was ~44% of the old per-step cost.

**Coordinated-omission disclosure.** This is a **closed-loop** throughput bench:
each run starts immediately after the previous one finishes, with no external
arrival schedule. Coordinated omission (a slow response masking the lateness of
requests that *should* have been issued on a fixed cadence) **does not apply** —
there is no expected inter-arrival interval to correct against, so the histogram
records raw back-to-back run latency (`Histogram::record`, not `record_correct`).

**Tail-resolution caveat (honest).** With 2123 recorded runs, p50, p99 and p99.9
are well resolved (>1000 samples beyond p99). p99.99 still needs ~10⁴ samples to
be distinct, so at this sample count it collapses to the observed max
(9666.6 µs) — treat it as "≥ p99.9", not an independently resolved quantile.

### Run conditions

| Field | Value |
|-------|-------|
| CPU | Apple M4 Max |
| Cores | 16 (16 physical) |
| Memory | 64 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| Toolchain | rustc 1.97.0 (2d8144b78 2026-07-07) |
| Build profile | `bench` (optimized, `cargo bench`) |
| Bench harness | `criterion` 0.5.1, `harness = false` |
| Percentiles | `hdrhistogram` 7.5.4 (3 significant figures) |
| `Cargo.lock` sha256 | `7dc4105a15aefa459f720cee5cb442fbd4e504fbcc1563c765cae6c38aa7535e` (the lockfile has since advanced to `e7a2d27…` via #27's `csv` edge; PB-3's freshly re-measured naive denominator, 2186 vs 2354 ns/step here, cross-validates this baseline within noise, so it is not re-measured) |

### Interpretation

- **What the number means.** At the p50, one full naive backtest over a
  2048-step, 4-leg iron-condor chain — Parquet parse, 2048 re-marks of the four
  held legs, naive fills, and the mark-to-market ledger — costs **≈ 4.82 ms**,
  i.e. **≈ 2.35 µs per step** and **≈ 425k steps/sec/core** on the reference
  machine. Post-#19 the exit seam sources `underlying` directly from the snapshot
  scalar and performs **no** per-step `OptionChain` rebuild, so the per-step cost
  is now the ledger re-mark of the four held legs plus the naive fill path — the
  ~44-alloc/step chain rebuild that dominated the superseded baseline is gone
  ([docs/07 §3 PB-1](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
- **Scale.** 2048 steps × 4 legs; the number is per single core (no cross-run
  parallelism — Monte-Carlo parallelism is across runs, not within one).
- **Did it meet the DESIGN TARGET?** PB-2's DESIGN TARGET is "naive-mode
  throughput **on the order of 1 × 10⁶ steps/min per core**", deliberately
  order-of-magnitude
  ([docs/07 §3](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
  Measured: **≈ 25.5 × 10⁶ steps/min/core** — about **25×** the target, i.e. the
  baseline **meets (comfortably exceeds) the order-of-magnitude target** (it is
  faster, not slower). No regression to explain.
- **This becomes the fixed budget.** Per docs/07 §3, the real PB-2 budget is
  **fixed at v0.1 from the `criterion` baseline**, not from the
  order-of-magnitude sanity target. This re-measured entry — with the dead
  per-step reprice removed (#19) — **is** that baseline; the superseded #18
  number (which baked in the dead work) is not.

### Downstream

This is the **v0.1 naive-throughput baseline** that later gates build on:

- **#019 — zero-alloc replay-loop gate.** A separate hard CI invariant
  (per-step allocations must not grow with step count); this bench is the
  throughput companion to it, not the alloc gate itself.
- **#51 — hot-path regression gate (v1.0).** This naive baseline is protected
  **portably** by the realistic/naive overhead **ratio floor** (18×) in
  `scripts/bench_gate.sh`: a naive-mode regression slows naive, collapses the
  ratio toward 1, and trips the floor — see the §H2 regression-gate tolerance
  table for the band. The gate compares the dimensionless ratio against the
  committed band, not an absolute number, so it tracks its own hardware; a merge
  that regresses it beyond the band fails
  ([docs/07 §6](docs/07-performance-and-security.md#6-regression-gates-in-ci-before-v10)).

---

## H2 / PB-3 — Realistic-mode overhead ratio (v0.2 A/B, #29)

- **Bench:** `benches/realistic_overhead.rs`
  (`cargo bench --features orderbook --bench realistic_overhead`)
- **Date measured:** 2026-07-16
- **What it measures:** the **same fixed condor scenario** run through
  [`run_backtest`] in **both** execution modes, back to back in **one process** —
  naive (mid + configured slippage) and realistic (orders routed through the
  seeded `option-chain-orderbook` matching engine: per-step book refresh, queue
  position, per-strike depth, market impact) — and the **dimensionless
  realistic / naive overhead ratio** of the per-step latency. Both modes share
  the identical `run_backtest` wrapper (Parquet open, ledger re-mark, metrics),
  differing **only** in `config.mode`, so the ratio isolates the fill model.
- **Workload / scale:** the **same** canonical Parquet chain PB-2 uses —
  **2048 steps × 4 condor legs = 8192 quote rows**, the four legs held open
  across the whole run (non-triggering `ExitPolicy::TimeSteps`), fixed seed
  (`42`), tape strictly before expiry. Realistic mode additionally seeds each
  strike's book from the **default** `LiquidityProfile` (quoted-size touch,
  `L = 5` deeper levels per side, `r = 0.5` geometric decay) and reseeds it every
  step (#25).
- **Samples:** naive **16 173** recorded runs; realistic **431** recorded runs
  (each after an explicit 32-run per-mode warmup, `--measurement-time 60`).

### Measured percentiles

Per-run latency is the raw measured quantity (one full `run_backtest` over the
2048-step chain); per-step is that latency divided by the fixed 2048 steps.

| Mode (per-step latency) | p50 | p99 | p99.9 |
|-------------------------|-----|-----|-------|
| **naive** (denominator) | **2186 ns/step** | 2498 ns/step | 2940 ns/step |
| **realistic** (numerator) | **78 976 ns/step** (≈ 79.0 µs) | 86 528 ns/step | 91 136 ns/step |

| Mode (per-run latency) | p50 | p99 | p99.9 | p99.99 |
|------------------------|-----|-----|-------|--------|
| **naive** | 4476.9 µs | 5115.9 µs | 6021.1 µs | 8359.9 µs |
| **realistic** | 161.74 ms | 177.21 ms | 186.65 ms | — |

- **Overhead ratio (realistic / naive), the PB-3 deliverable:**
  **p50 = 36.13× · p99 = 34.64×.** The per-run and per-step ratios are equal (the
  step count is the same fixed tape), so the ratio is scale-free.
- **Naive per-step (2186 ns) is consistent with the committed PB-2 baseline**
  (2354 ns/step, same machine) — within run-to-run noise, which cross-validates
  the A/B's denominator.
- **Throughput:** naive ≈ **27.45 × 10⁶ steps/min/core**; realistic ≈
  **0.76 × 10⁶ steps/min/core** on the reference machine.

**Coordinated-omission disclosure.** Both modes are measured as a **closed-loop**
throughput bench: each run starts immediately after the previous one finishes,
with no external arrival schedule. Coordinated omission **does not apply** — there
is no expected inter-arrival interval to correct against, so each histogram
records raw back-to-back run latency (`Histogram::record`, not `record_correct`).

**Tail-resolution caveat (honest).** The realistic histogram has **431** recorded
runs (realistic mode is ~36× slower, so fewer fit the measurement window). p50 and
p99 are well resolved; p99.9 rests on ~1 sample beyond p99 and p99.99 collapses to
the observed max — treat the realistic p99.9 as "≥ p99", not an independently
resolved quantile. The naive tail is well resolved (16 173 runs). The **ratio** is
taken at p50 and p99, where both histograms are solid.

### Run conditions

| Field | Value |
|-------|-------|
| CPU | Apple M4 Max |
| Cores | 16 (16 physical) |
| Memory | 64 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| Toolchain | rustc 1.97.0 (2d8144b78 2026-07-07) |
| Build profile | `bench` (optimized, `cargo bench`) |
| Feature | `orderbook` (realistic mode + `option-chain-orderbook` matching) |
| Bench harness | `criterion` 0.5.1, `harness = false` |
| Percentiles | `hdrhistogram` 7.5.4 (3 significant figures) |
| `Cargo.lock` sha256 | `e7a2d27de97489cb934d772d0cf67e19ce636174e13b64a1e510ef8a94eeab7a` |

### Interpretation

- **What the number means.** On the fixed 2048-step, 4-leg canonical condor
  scenario, realistic mode costs **≈ 36× the naive per-step latency at the p50**
  (≈ 79 µs/step vs ≈ 2.2 µs/step). That overhead is the **honest cost of
  order-book-level fills**: every step cancels the previous snapshot's seeded
  depth and reseeds a fresh ladder (touch + `L = 5` deeper levels × 2 sides × 4
  legs ≈ 48 resting orders/step) through the real matching engine, then routes the
  strategy's marketable intents against that book — queue position, per-strike
  depth, and market impact emerge from the matching, not from a configured offset.
  PB-3's DESIGN TARGET is deliberately **"a bounded, documented ratio vs naive,
  no absolute promise"** ([docs/07 §3](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite));
  **36× measured is that bound, recorded — not asserted.**
- **The ratio is the meaningful, portable quantity.** Because both modes share the
  identical `run_backtest` cost and differ only in the fill model, a uniform
  hardware clock-speed factor cancels in the ratio. So `36×` is a property of the
  two fill models, not of the M4 Max — it is expected to hold (within a wide band)
  on other hardware, which is exactly why the CI regression gate below tracks the
  **ratio**, not an absolute nanosecond count.
- **Scale.** 2048 steps × 4 legs; single core (Monte-Carlo parallelism is across
  runs, not within one).

### Per-step allocation — realistic mode is gated at a measured budget

The naive warm step allocates **0 events/step** and IS gated at zero (PB-1
below). The **realistic warm step allocates by construction** — this is an
honest, measured cost, not an oversight — so since #127 it is gated at a
**measured ceiling** rather than at zero:

- **Measured (2026-09-04, re-measured on the then-current lockfile:
  `option-chain-orderbook` 0.10.0 / `orderbook-rs` 0.12.1 / `pricelevel` 0.9.1;
  the #128 bump to `option-chain-orderbook` 0.11.0 republished a byte-identical
  `src/` tree and moved neither `orderbook-rs` nor `pricelevel`, so the count
  carries over unchanged — re-measured four runs on each side, **30 976–30 992
  events after the bump** against **30 982–30 985 before** it, both inside the
  interval recorded below).**
  On the 4-leg condor at seed 42, realistic mode allocates **≈ 564 allocation
  events per warm step**: the steady-state tail delta over the 55 warm steps
  `K = 8 .. LAST = 63` was **30 969–31 019 events observed over 24 runs**
  (31 019 / 55 ≈ 564.0/step) — measured with the same counting-`GlobalAlloc`
  harness as `tests/zero_alloc.rs`, driving `RealisticFill` instead of
  `NaiveFill`. The earlier figure recorded here (31 005 events, 2026-07) falls
  inside that interval.
- **Measurement platform.** macOS 26.6.2, Apple silicon (arm64), rustc 1.97.0
  (2d8144b78 2026-07-07), `test` profile. CI runs the gate on `ubuntu-24.04`,
  which has **not** been measured separately: allocation-event counts are
  determined by the code path rather than by clock speed, and the 25 % headroom
  is expected to cover any platform difference, but that expectation is an
  argument, not a measurement. **This is the open gap in this entry**: the
  `ubuntu-24.04` count should be recorded here once observed. The gate prints
  nothing on success, so obtaining it needs a one-off run on that image with a
  temporary print (or a deliberately failing assert) rather than a green CI run.
- **Not reproducible run to run.** Unlike the naive gate's exact zero, the
  realistic allocation *count* moves between runs, at both scales, and the two
  scales were recorded separately. **Per step:** the full 63-step sample series
  was captured in three processes, and **all 55 warm steps differ** across them,
  by a median of **6** and at most **14** events out of ≈ 560 (worst case step 12
  at 583 / 587 / 573). **Per tail:** those same three runs summed to 30 975 /
  30 982 / 30 991, and the 24-run range above spans 50 events — a **≈ 0.2 %
  spread**, far narrower than independent per-step noise would give, so the
  per-step deviations largely cancel rather than accumulate. It is **not**
  per-process seeding:
  three identical back-to-back runs inside a *single* process gave 30 988 /
  30 980 / 30 984, so address-space layout and per-process hash keying are ruled
  out and the cause is process-global carried state — `crossbeam-epoch`'s global
  collector and its deferred-reclamation bags, whose allocation *pattern* depends
  on when reclamation fires. (Node height in `crossbeam-skiplist` 0.1.3 is *not*
  a contributor: `random_height` draws from a per-list seed initialised to a
  constant, and `Node::alloc` is one allocation whatever the height.) **The run's
  results are unaffected** — the `iron_condor_realistic` golden bundle is
  byte-frozen and passes — only the allocation-event count moves. This is why the
  gate is a ceiling with headroom and never an equality.
- **Scope: a gated warm step is a book REFRESH with no fills.** The condor opens
  once at `on_start` and the exit policy never triggers, so the run's only fills
  land at step 0 (four opens) and at step 63 after the last sample (four `on_end`
  closes) — both outside the measured window. Every warm step issues zero orders
  of its own, so the ≈ 564 events are the per-step liquidity reseed alone and
  **per-fill allocation in realistic mode is not gated here**.
- **Why.** The per-step book refresh (#25) calls
  `OptionOrderBook::add_limit_order_full` for every reseed level, and each call
  returns an **owned `TradeResult` whose `symbol` `String` heap-allocates upstream
  even when nothing crosses** (the #25 P2-01 finding — see the
  `resting_seed_ids` doc comment in `src/execution/realistic.rs`). With ≈ 48
  reseed orders + 48 cancels per step across the four legs — a count that follows
  from the fixture, `4 quotes × 2 sides × (1 + depth_levels)` with
  `LiquidityProfile::depth_levels` defaulting to 5, and a ladder that stops early
  if a level's size rounds to zero — that upstream
  per-call capture cost — the symbol `String` (~48–96 events) **plus the matching
  engine's internal per-order allocations**, which are the larger share — accounts
  for the ≈ 564 events. `ironcondor`'s own tracking state (`resting_seed_ids` /
  `seed_plan`) is cleared **in place** and does not contribute steady-state
  allocation; the residual is entirely upstream capture-API cost.
- **The gate (`tests/zero_alloc.rs`, module `realistic`).** Budget
  **705 events/warm step** = the measured 564 + 25 % headroom, rounded up — the
  same factor-with-headroom discipline as `scripts/bench_gate.sh`. The
  measurement sits at ~80 % of the ceiling. Because the count is a function of
  the fixture, **changing `common::condor_rows`' quote universe or the default
  `LiquidityProfile` invalidates the budget** — it must then be re-measured, not
  rescaled. Note too that `K = 8` is shared with the naive gate and sits inside
  the book's own warm-up head (≈ 662 events at step 1, a ≈ 555 plateau only from
  about step 37), so 564 is the **window average**, conservative for a ceiling.
  A second assertion pins **linearity**,
  **one-sided**: the delta over the last 27 warm steps may exceed the delta over
  the first 27 by at most **2 %**, and a *smaller* second window is never a leak
  so it is not bounded at all. The direction matters, because the measured second
  window runs **2.47 %–2.77 % below** the first (a warm-up decay from ≈ 660
  events/step early to a ≈ 555 plateau); a two-sided check would let a leak first
  cancel that decay, needing ≈ 7.7 % of growth before firing, and would report a
  false "leak" wherever the decay runs longer. One-sided, the tolerance only has
  to absorb the ≤ 0.25 % inter-process jitter of a single window, so 2 % is 8×
  that jitter and still fires on ≈ 11 extra events per step. Two negative tests
  confirm both assertions bite: a constant 400 allocations per step (≈ 2.8× the
  headroom) breaks the ceiling, and an allocation count that **grows with the
  step index** (8 per index, a leak's shape) drives the second window 5635–5645
  events above the first against a 399–400 tolerance, ~14× the margin.
- **Consequence.** Realistic mode is **gated as a ceiling, not as zero**: gating
  it at zero would make the gate a lie (it would fail, or force a false
  invariant), and leaving it ungated let an upstream regression pass unseen. The
  headroom absorbs upstream patch-level churn, never a leak — if the gate fails,
  find the new allocation rather than raise the budget. The overhead ratio above
  **already reflects** this allocation cost: it is priced into the 36× number.
  The residual is a candidate for a future **upstream symbol-borrowing
  optimisation** (a capture API that borrows the book's symbol rather than
  cloning it into each `TradeResult`); when that lands, the ratio here is the
  before-number to measure against.

### Downstream — the naive-throughput / overhead regression gate (this issue)

CI job **`bench-gate`** (`scripts/bench_gate.sh`, `.github/workflows/ci.yml`)
protects the naive baseline and the realistic overhead **portably**:

- **It gates the dimensionless ratio, not an absolute number.** The committed
  baseline here is Apple M4 Max, but CI runs on GitHub Linux runners — an
  absolute-ns compare would false-positive every run. The **ratio is
  hardware-portable** (a uniform clock factor cancels; see the interpretation
  above), so the gate compares the CI-measured p50 ratio against a committed band
  **[18×, 72×]** — a factor of two each way around the 36× baseline, wide enough
  that ordinary CI variance (a handful of samples) cannot trip it, tight enough to
  catch a **gross (~2×) disproportionate regression**: a **naive** regression
  slows naive and drops the ratio toward 1 → trips the **18× floor** (this is the
  headline naive-throughput protection); a **realistic** blow-up raises the ratio
  → trips the **72× ceiling**.
- **Coarse absolute backstop.** A secondary check fails if naive per-step p50
  exceeds **100 000 ns** (≈ 45× the M4 baseline) — far above any plausible CI
  hardware, so it never false-positives, but it catches the one case the ratio is
  blind to: a regression that slows **both** modes by the same factor (shared
  infrastructure), which leaves the ratio unchanged.
- **Honest scoping.** A truly robust *absolute* throughput gate is not feasible on
  shared, noisy CI runners without a per-run calibration baseline; gating the
  portable ratio + a coarse backstop is the defensible choice, and it is
  documented as such. The gate runs the bench with **reduced** criterion samples
  (`--sample-size 10 --measurement-time 5`) so the job is short — full-fidelity
  numbers are this local measurement, the gate only needs enough samples to catch
  a gross regression (the reduced-sample ratio measured 34.1× here, well inside
  the band). Verified locally: the gate PASSES on the current baseline and FAILS
  (exit 1) on a forced gross ratio (99× or 9×) or a forced 200 µs/step naive.
- **Relationship to #051.** This was the first live percentile-tracked regression
  gate; **#51 extended the same machinery** to the remaining hot paths
  (conversion §H3, bundle writer §H4, PyO3 §H5) against their `BENCH.md`
  baselines. The bands below are each of those gates' committed tolerances.

### Regression-gate tolerance (#51)

| Gated quantity | Baseline | Band | Trips on |
|----------------|----------|------|----------|
| realistic / naive overhead ratio p50 | 36.13× | **[18×, 72×]** | a ~2× disproportionate naive (floor) or realistic (ceiling) regression |
| naive per-step p50 (coarse backstop) | 2186 ns | **≤ 100 000 ns** | a catastrophic absolute naive regression the ratio is blind to (the SOLE absolute-number gate, #29) |

`scripts/bench_gate.sh`, CI job `bench-gate`. The ratio is dimensionless and
hardware-portable (a uniform clock factor cancels); the coarse ceiling is the
one documented absolute backstop, set ~45× above the M4 baseline so CI hardware
never approaches it.

---

### Re-measurement 2026-09-04 — the dependency refresh (#125) is performance-neutral

The refresh that moved `optionstratlib` 0.18 -> 0.21 (with `rand` 0.10,
`positive` 0.6, `rust_decimal` 1.43, `arrow`/`parquet` 59.3, and `reqwest` 0.13
beneath it) touches crates that sit on the per-step path — `Positive` and
`Decimal` arithmetic in the exit seam and the ledger — so it carries the
measurement the hot-path rule demands. Two things were measured, in this order,
on the same machine within one quiet session (no concurrent builds), using
`scripts/bench_gate.sh` (sample-size 10, measurement-time 5 s):

| Run (back to back) | naive p50 | realistic / naive p50 |
|---|---|---|
| `main` @ `cc6b73a` (pre-bump) | 3088 ns/step | 29.97x |
| branch @ `92442a8` (post-bump) | 3178 ns/step | 29.08x |
| `main` again (order control) | 3140 ns/step | 28.92x |

- **Branch vs `main`: +2.9 % / -1.7 % on naive per-step, -3 % on the ratio** —
  inside `main`'s own run-to-run scatter (3088 vs 3140 ns, +1.7 %). The bump
  does not move the hot path.
- **The absolute naive figure today (~3.1 us/step) is ~40 % above the
  2026-07-16 baseline (2186-2354 ns) — on `main` too**, i.e. before the bump.
  That is the machine on the day (host state; the baseline was taken on rustc
  1.97.0 / macOS 26.5.2 on 2026-07-16), not the code, and it is exactly the
  case the gate's **dimensionless** overhead band exists for: the ratio
  (~29x, band [18x, 72x]) is unaffected by a uniform clock factor, and the
  absolute naive figure is held only by the coarse 100 000 ns backstop.
- A first `release_check.sh` pass measured **53.07x** with other `cargo`
  builds running concurrently. That number is contention, not a regression —
  the three quiet runs above are the evidence — and it is recorded here so a
  future reader who sees it in a log does not mistake it for a baseline.
- H3 / H4 / H5 on the branch, same session: conversion per-contract ratio
  **1.135x** (ceiling 4x), writer per-row ratio **0.262x** (ceiling 2x), PyO3
  marshal ratio **15.82x** (band [8x, 32x]). All within band; all gates PASS.

The committed baselines above are **not** re-blessed by this note: they remain
the reference numbers, and the July hardware/OS state they were taken on is
part of their provenance. What this note pins is that the refresh is neutral
against that reference under an A/B on identical conditions.

---

## H3 / PB-4 — Conversion-boundary scaling (v1.0 baseline, #51)

- **Bench:** `benches/conversion.rs` (`cargo bench --bench conversion`)
- **Date measured:** 2026-07-17
- **What it measures:** [`raw_quotes_to_snapshot`] — the DTO-independent
  validation core in `src/data/convert.rs` that **every feed reuses** (there is
  no second validation path), timed over a single snapshot of **growing contract
  count**. One O(contracts) pass: per quote it rejects a non-positive /
  non-tick-aligned strike, a non-tick bid/ask, a crossed quote, and a NaN /
  infinite analytic; derives the one deterministic `mid`; resolves the expiry
  once; and inserts into a `BTreeMap<ContractKey, QuoteView>`. This is the
  per-snapshot boundary work a feed pays at tape materialisation — hot path
  **H3**. The `snapshot_to_option_chain` sibling (the deferred-reprice path, off
  the per-step loop) is the same O(contracts) complexity and is not separately
  gated.
- **Workload / scale:** one snapshot at **512 / 2 048 / 8 192 contracts** — a
  **16×** sweep — each contract a distinct tick-aligned strike
  (`100 000 + i·500` cents on the 5 c grid), one style, one expiry, a constant
  tick-aligned `bid ≤ ask`, and finite analytics (so every quote validates and
  every `ContractKey` is unique). The range spans below and well above a
  realistic option chain, wide enough to expose super-linearity.
- **Samples:** **61 008 / 14 295 / 3 573** recorded conversions (criterion
  warmup + measurement at `--sample-size 100 --measurement-time 10`, all at
  steady state after an explicit 32-conversion per-size warmup).

### Measured percentiles

Per-conversion latency is the raw measured quantity (each timed iteration is one
full `raw_quotes_to_snapshot` over the size's contracts); `ns/contract` is the
p50 divided by the contract count — a fair per-contract figure because every
recorded conversion at a size processes exactly that many contracts.

| contracts | samples | p50 | p99 | p99.9 | **ns/contract (p50)** |
|-----------|---------|-----|-----|-------|------------------------|
| 512       | 61 008  | 225.53 µs | 250.75 µs | 292.10 µs | 440.5 |
| 2 048     | 14 295  | 953.86 µs | 1141.76 µs | 1173.50 µs | 465.75 |
| 8 192     | 3 573   | 4061.18 µs | 4325.38 µs | 4501.50 µs | **495.75** |

- **Scaling verdict — LINEAR (PB-4: MET).** Contracts grew **16×** (512 →
  8 192); the per-contract cost rose only **1.13×** (440.5 → 495.75 ns/contract)
  — a mild `log(contracts)` `BTreeMap`-assembly factor (≈ 1.44× worst case over
  16×), **not** the ~16× a quadratic core would show. The conversion is
  **O(contracts) single-pass**, converging to **≈ 2.0 M contracts/sec** on the
  reference machine.
- **The gated quantity is the cost ratio.** The dimensionless per-contract cost
  ratio (largest / smallest = **1.125×**) is what the CI linearity gate tracks —
  it is a within-run ratio, so a uniform hardware clock factor cancels and the
  gate is portable to Linux CI.

**Coordinated-omission disclosure.** A **closed-loop** micro-bench — each
conversion starts immediately after the previous finishes, no external arrival
schedule — so coordinated omission **does not apply**; the histogram records raw
back-to-back latency (`Histogram::record`, not `record_correct`).

**Tail-resolution caveat (honest).** The 512- and 2 048-contract sizes are richly
resolved (61 k / 14 k samples). The 8 192-contract size has **3 573** recorded
conversions (it is ~18× slower, so fewer fit the window): p50 and p99 are solid,
p99.9 rests on ~3 samples beyond p99 — treat it as "≥ p99", not an independently
resolved quantile. The **cost ratio** is taken at p50, where every size is solid.

### Run conditions

| Field | Value |
|-------|-------|
| CPU | Apple M4 Max |
| Cores | 16 (16 physical) |
| Memory | 64 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| Toolchain | rustc 1.97.0 (2d8144b78 2026-07-07) |
| Build profile | `bench` (optimized, `cargo bench`) |
| Bench harness | `criterion` 0.5.1, `harness = false`, `--sample-size 100` |
| Percentiles | `hdrhistogram` 7.5.4 (3 significant figures) |
| `Cargo.lock` sha256 | `52e6e87afd51d878df63c046c2177946677ea92e5b59e55d1ed602063fabcb7b` |

### Interpretation

- **What the number means.** Converting one snapshot of `N` contracts costs
  **≈ 496 ns per contract** at steady state on the reference machine — a single
  O(contracts) validation pass with a `BTreeMap` assembly, no re-conversion. At
  the p50 an 8 192-contract chain converts in **≈ 4.06 ms**.
- **Did it meet PB-4?** PB-4's measurement clause is *"O(contracts) single pass
  per snapshot; assert linear scaling across chain sizes"*
  ([docs/07 §3](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
  **Met — measured, not asserted:** the per-contract cost is ~flat (1.13× over
  16× contracts), so the pass is linear (with the expected mild `log n` map
  factor), not quadratic.
- **Scale.** Single snapshot, single core; the conversion runs once per snapshot
  at materialisation, before the replay loop (the loop sees only the validated
  `ChainSnapshot`, never a raw DTO).

### Regression-gate tolerance (#51)

| Gated quantity | Baseline | Ceiling | Trips on |
|----------------|----------|---------|----------|
| per-contract cost ratio (largest / smallest, 16× sweep) | 1.125× | **≤ 4.0×** | a quadratic-class super-linear regression — a quadratic core shows ≈ 16× |

`scripts/linearity_gate.sh`, CI job `bench-gate`. The ceiling is a **shape
bound**, not an absolute performance number, and it catches super-linear SHAPE,
not magnitude: it separates the linear / `n·log n` regime (≈ 1.1–1.9×) from the
quadratic one (≈ 16×), anchored to the O(contracts) scaling shape above. A uniform
constant-factor slowdown that preserves the ratio passes silently (that case is
the H1/H2 absolute backstop's job, §H2). **Scope:** the gate covers
`raw_quotes_to_snapshot` (the per-snapshot validation core) only, **not**
`snapshot_to_option_chain` (the deferred-reprice sibling, off the per-step loop).
The gated quantity is a within-run dimensionless ratio, so a uniform clock factor
cancels and the gate is portable to Linux CI.

### Downstream

This is the **v1.0 conversion baseline** the #51 linearity gate builds on — the
first PB-4 measurement (previously a DESIGN TARGET pending its bench). The gate
does not record new baselines; it wraps this one
([docs/07 §6](docs/07-performance-and-security.md#6-regression-gates-in-ci-before-v10)).

---

## H1 / PB-1 — Zero-steady-state-allocation replay-loop gate, naive mode (invariant, #19)

This is **not a throughput measurement** — it is an **invariant gate** and is
recorded here only to distinguish it from the PB-2 baseline above. It gates like
a correctness test, not a benchmark: a regression **fails the build**
([docs/07 §4/§6](docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop),
[docs/TESTING.md §11.1](docs/TESTING.md#111-performance-regression-gates)).

- **Gate:** `tests/zero_alloc.rs` (`cargo test --test zero_alloc`); CI job
  `zero-alloc`, separate from the `naive_throughput` bench (PB-2 above) and the
  `golden` job (#17). The same job also runs the file under `--features
  orderbook`, which adds the realistic-mode ceiling gate (#127, PB-3 above).
- **What it measures.** Allocation **events** (each `alloc` / `alloc_zeroed` /
  `realloc`) inside the engine's **per-step body** must **not grow with step
  count**. A test-only counting `GlobalAlloc` (per-thread counter, immune to
  `cargo test` parallelism) is sampled at a fixed per-step phase by a
  `SamplingStrategy` decorator — the start of `on_snapshot`, into a pre-sized
  buffer, so recording is itself allocation-free. The gated quantity is the
  delta between the sample at warmup step **`K = 8`** and the last step
  (**`63`**) of a fixed **64-step** canonical condor run.
- **Measurement boundary (explicit).** Only the per-step body — steps (b)–(g)
  of [docs/02 §3.2](docs/02-engine-architecture.md#32-per-step-for-each-snapshot-s_n-on-the-tape-in-order).
  **Startup** (feed materialisation, `run_id`, writer construction, the one-time
  `on_start` sizing of `cmds` / `fills` / `equity_curve` / ledger-scratch) and
  **termination** (`finalize` + Parquet encode) are **outside** it and may
  allocate — the warmup-to-last delta cancels them. `K = 8` is well past the
  single condor entry (at `on_start`, before step 0) and the first ledger mark
  (step 0), so the tail measures only warm bodies.

### Measured (dev machine, reference below)

| Scenario (64-step canonical condor) | Per-step alloc events | Tail delta `K=8 → 63` | Verdict |
|-------------------------------------|-----------------------|-----------------------|---------|
| **Real `OptStratAdapter<IronCondor>` per-step body** (alloc-free feed) — THE gate | **0** | **0** | **gate PASSES** |
| Deliberate `Vec::with_capacity(64)` in the step body (negative) | 1 | 55 | correctly **> 0** (gate would fail) |
| `ParquetFeed::next` snapshot clone (data step (a)) | 1 (steady constant) | 55 | data-layer, outside the (b)–(g) boundary |

- **Real v0.1 strategy body = 0.** Post-#19 the gate drives the **real**
  `OptStratAdapter<IronCondor>` (not a probe) over the alloc-free feed, and its
  per-step body allocates nothing per warm step: `cmds` / `fills` /
  `equity_curve` are sized once and cleared in place, the ledger's `marks`
  `BTreeMap` / `position_marks` scratch grow only during warmup, `ContractKey`
  clones are `Arc<str>` refcount bumps, and `exits()` now sources `underlying`
  from the snapshot scalar with **no** per-step `OptionChain` rebuild. This is
  what the gate enforces.
- **The #19 fix folded the exit seam into the zero.** The old `IronCondor::exits()`
  chain rebuild (previously 44 events/step) was **dead work** — its only consumed
  output, `chain.underlying_price`, is `snapshot.underlying_price` — so it is
  deferred until a Greek-driven exit policy is wired. The exit seam now allocates
  **0/step** and sits **inside** the gated body; the FOLD-THE-FIX change is
  output-preserving (the golden passed unblessed).
- **One per-step cost still sits *outside* the gated engine body.**
  `ParquetFeed::next`'s snapshot clone (1 event/step) is a **data-layer** cost at
  step (a); it is a **steady-state constant** and asserted by the diagnostic test
  in `tests/zero_alloc.rs` so a shift to *growing* per-step allocation would be
  caught.
- **The gate bites.** Injecting one `Vec` allocation into the step body yields a
  non-zero tail delta (55 = one event per tail step) — the negative test proves
  a real regression fails the gate.
- **The zero applies to the naive warm step only.** This gate asserts **0
  events/step** for the **naive** step body. The **realistic** warm step cannot be
  zero — it allocates ≈ 564 events/step through the upstream
  `add_limit_order_full` capture API (the #25 P2-01 finding) — so since #127 it is
  gated in the **same file** at a **measured ceiling plus a linearity assertion**
  rather than at zero (`cargo test --features orderbook --test zero_alloc`, run by
  the same `zero-alloc` CI job). Numbers, the jitter caveat and the budget
  derivation live in **PB-3 above** — see its "Per-step allocation" block.

### Run conditions

| Field | Value |
|-------|-------|
| CPU | Apple M4 Max |
| Cores | 16 (16 physical) |
| Memory | 64 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| Toolchain | rustc 1.97.0 (2d8144b78 2026-07-07) |
| Build profile | `test` (debug; the counter is exact regardless of profile) |
| Allocator | test-only counting `GlobalAlloc` over `System` (per-thread event counter) |

The counts are **exact allocation-event counts**, not timings, so they are
machine-independent (the reference block records where they were last observed,
not a hardware-sensitive quantity). CI runs the gate on `ubuntu-24.04`.

---

## H4 / PB-5 — Bundle-writer scaling (v0.3 baseline, #37)

- **Bench:** `benches/bundle_write.rs`
  (`cargo bench --bench bundle_write`) for the **time** numbers; companion
  `tests/bundle_write_pb5.rs`
  (`cargo test --release --test bundle_write_pb5 -- --ignored --nocapture`) for
  the **peak-memory** numbers and the linear-not-quadratic assertions.
- **Date measured:** 2026-07-16
- **What it measures:** [`write_bundle`] alone — the encode of the four Parquet
  tables (`fills` / `equity_curve` / `positions` / `greeks_attribution`, chunked
  into 8192-row row-group batches with the pinned SNAPPY codec) plus the
  canonical-JSON `manifest.json`, published atomically (temp dir + rename) — over
  [`BacktestRun`]s of **growing length**. The run is synthesised in **setup**
  (un-timed); only the write is timed, so this isolates hot path **H4** from the
  replay loop (H1/PB-2) and the fill models (H2/PB-3). `overwrite = true`, so each
  timed iteration republishes the **same** `run_id` directory in place.
- **Workload / scale:** the realistic four-condor-leg shape — `positions.parquet`
  gets `4 · steps` rows (the dominant table), `equity_curve` and
  `greeks_attribution` get `steps` rows each, `fills` a small constant (4), so
  **total rows ≈ `6 · steps`**. Step counts are chosen so the dominant
  `positions` table lands at **1 024 / 8 192 / 32 768 / 131 072** rows — i.e.
  **1/8×, 1×, 4×, 16×** one 8192-row batch — so the streaming path is exercised
  across the row-group boundary (from below one batch to sixteen), not assumed.
  All monetary columns are integer cents; the sole `f64` (`drawdown`) is finite.
- **Samples:** 30 criterion samples per size (`--sample-size 30`), i.e. **2790 /
  1395 / 465 / 120** timed `write_bundle` iterations recorded into the size's
  `hdrhistogram` (warmup + measurement, all at steady state after an explicit
  8-write per-size warmup) — the same record-every-iteration convention as PB-2.

### Measured — write time (criterion, `bench` profile)

Per-write wall-time is the raw measured quantity; `rows/sec` and `ns/row` are
derived from the p50 against the total rows written.

| steps  | positions rows | total rows | p50 | p99 | rows/sec | **ns/row (p50)** |
|--------|----------------|------------|-----|-----|----------|------------------|
| 256    | 1 024          | 1 540      | 1.685 ms | 9.232 ms | 0.91 M | 1093.8 |
| 2 048  | 8 192          | 12 292     | 3.740 ms | 6.046 ms | 3.29 M | 304.2 |
| 8 192  | 32 768         | 49 156     | 11.805 ms | 23.069 ms | 4.16 M | 240.2 |
| 32 768 | 131 072        | 196 612    | 45.810 ms | 56.951 ms | 4.29 M | **233.0** |

- **Scaling verdict — LINEAR (PB-5 time: MET).** Rows grew **128×** (1 540 →
  196 612); the per-row cost **fell** to **0.21×** (1093.8 → 233.0 ns/row) as the
  fixed per-write overhead (`run_id` SHA-256, manifest JSON, four file
  opens/closes, atomic rename) amortised over more rows, then **converged flat**
  from 8 k rows up (240.2 → 233.0 ns/row). A least-squares reading of the three
  largest points is `time ≈ 0.47 ms + ~231 ns/row` — a straight line. A
  **quadratic** writer's per-row cost would have *risen* ~128×; it fell, so the
  encode is **O(rows) single-pass**, converging to **≈ 4.29 M rows/sec** on the
  reference machine.
- **criterion cross-check** (its own median estimate, for sanity): 1.963 ms /
  3.859 ms / 11.987 ms / 47.006 ms — consistent with the hdrhistogram p50s, which
  validates the measurement.

### Measured — peak memory (counting `GlobalAlloc`, `tests/bundle_write_pb5.rs`)

The writer's **incremental** peak allocation high-water — outstanding bytes over
the baseline captured **after** the run is already resident, so it is the
writer's own footprint, not the run's. Byte counts are build-profile
independent. Sizes here extend past **1 M rows** in the `positions` table to test
whether the peak flattens (a hard row-group bound) or keeps growing.

| steps   | total rows | peak | **bytes/row** |
|---------|------------|------|---------------|
| 256     | 1 540      | 1 079 KiB | 717.7 |
| 2 048   | 12 292     | 4 078 KiB | 339.8 |
| 8 192   | 49 156     | 11 547 KiB | 240.6 |
| 32 768  | 196 612    | 33 346 KiB | 173.7 |
| 131 072 | 786 436    | 133 378 KiB | 173.7 |
| 262 144 | 1 572 868  | 266 754 KiB | 173.7 |

- **Peak grows exactly LINEARLY with rows (not quadratically).** From 196 k rows
  up the `bytes/row` is **constant at 173.7 B/row** — `4×` rows (196 612 →
  786 436) gives `4.0×` peak, `2×` rows (786 436 → 1 572 868) gives `2.0×` peak.
  The high `bytes/row` at tiny sizes (717.7 at 1 540 rows) is the fixed writer
  overhead (Arrow schema, per-table batch buffers, Parquet state) amortising out.
- **Honest reading — O(rows), NOT flat/row-group-bounded.** The peak does **not**
  flatten even at 1.57 M rows (> one 1 M-row Parquet row group), because
  `write_bundle` sorts each table into a fully-materialised `Vec` of wire rows
  (`Vec<FillRow>` / `Vec<PositionRow>`, and index vectors for equity/greeks)
  **before** streaming the row-group batches. That sort buffer is **O(rows)** and
  dominates the peak. The per-batch Arrow encode buffer **is** bounded by
  `WRITE_BATCH_ROWS = 8192`, but it is not what the peak measures. So the writer
  is **linear-memory**, which is the correct bar for a **termination-phase**
  component (outside the PB-1 zero-alloc boundary — free to allocate O(rows));
  it is **not** literally "peak bounded by the row-group buffer".

### PB-5 assertion test

`tests/bundle_write_pb5.rs` carries the machine-checkable evidence:

- `pb5_writer_scales_linearly_not_quadratically` (**CI-light**, in the default
  test run): sizes 512 / 2 048 / 8 192 steps (a **16×** row range), debug build.
  It asserts the per-row **time** ratio (measured **0.66×**) and per-row **peak
  memory** ratio (measured **0.48×**) both stay `≤ 4×` — a quadratic writer over
  16× rows would show ~16×, so the band cleanly separates linear from quadratic.
  Runs in ≈ 0.6 s.
- `pb5_writer_full_size_record` (**`#[ignore]`d**, heavy — builds multi-million-
  row runs, ~2 s in release, ~260 MiB peak): the 256 … 262 144-step record above,
  run manually to refresh this entry.

The scaling ratio is build-profile independent, so the CI-light assertion is a
valid regression guard regardless of the `test` (debug) profile; the absolute
optimised numbers are the criterion bench's job.

**Coordinated-omission disclosure.** This is a **closed-loop** throughput bench —
each `write_bundle` starts immediately after the previous finishes, with no
external arrival schedule. Coordinated omission **does not apply**; the histogram
records raw back-to-back write latency (`Histogram::record`, not
`record_correct`).

**Warmup.** An explicit **8-write** un-recorded warmup precedes each size's
measured phase, on top of criterion's own 3 s warmup.

**Tail-resolution caveat (honest).** At the smallest size (1 540 rows, ≈ 1.7 ms
write) the p99 (9.23 ms) is dominated by filesystem jitter on the atomic
rename / prior-bundle removal — the write is so fast the OS tail swamps it. p50
(the reported headline) is well resolved at every size; treat the small-size p99
as "OS-jitter-bounded", not an encode cost. The larger sizes have tight p99/p50
(≈ 1.24× at 196 612 rows).

### Run conditions

| Field | Value |
|-------|-------|
| CPU | Apple M4 Max |
| Cores | 16 (16 physical) |
| Memory | 64 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| Toolchain | rustc 1.97.0 (2d8144b78 2026-07-07) |
| Build profile (time) | `bench` (optimized, `cargo bench`) |
| Build profile (memory) | `release` (byte counts are profile-independent) |
| Bench harness | `criterion` 0.5.1, `harness = false`, `--sample-size 30` |
| Percentiles | `hdrhistogram` 7.5.4 (3 significant figures) |
| Memory instrument | test-only counting `GlobalAlloc` over `System` (outstanding-bytes high-water) |
| `Cargo.lock` sha256 | `95e2a25afe8e4322e3a4bfcfdfc1f590af579d175f4db4e033e7e6d3a2e4ab0b` |

### Interpretation

- **What the numbers mean.** Encoding a full result bundle costs **≈ 233 ns per
  row** at steady state (≈ **4.29 M rows/sec**) and **≈ 174 bytes of peak working
  memory per row**, both on the reference machine. A 32 768-step, four-leg run
  (196 612 rows across the four tables) writes in **≈ 46 ms** with a **≈ 33 MiB**
  writer peak.
- **Did it meet PB-5?** PB-5's measurement clause is *"assert write time and peak
  RSS grow **linearly, not quadratically**"*
  ([docs/07 §3](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
  **Both do — measured, not asserted:** write time converges to a flat
  231–233 ns/row (per-row ratio 0.21× over 128× rows), and peak memory to a flat
  173.7 bytes/row (exactly linear from 196 k to 1.57 M rows). **On that clause,
  PB-5 is MET.**
- **The honest gap on the stricter wording.** The budget *prose* also says "peak
  memory bounded by the **row-group buffer**, not the total run length". The
  current writer does **not** satisfy that literal reading: it sorts each table
  into a fully-materialised `Vec` of wire rows before streaming, so its peak is
  **O(rows)** (it did not flatten past a 1 M-row row group). This is a **linear**,
  not quadratic, footprint — appropriate for the **termination-phase** writer,
  which is deliberately **outside** the PB-1 zero-alloc boundary and free to
  allocate O(rows) — but it means the "row-group-bounded" phrase describes only
  the **per-batch Arrow encode buffer** (bounded by `WRITE_BATCH_ROWS = 8192`),
  **not** the total peak. Making the total peak row-group-bounded would require a
  streaming external sort or a pre-sorted-input contract in the writer (#34), a
  bundle-writer change out of scope for this measurement issue; it is flagged here
  honestly rather than hidden.
- **Scale.** Single core, single strategy; the write is termination-phase, run
  once per backtest (Monte-Carlo parallelism is across runs, not within one).

### Regression-gate tolerance (#51)

| Gated quantity | Baseline | Ceiling | Trips on |
|----------------|----------|---------|----------|
| per-row cost ratio (largest / smallest, 128× row sweep) | ≈ 0.14–0.25× (healthy; cost *falls* with amortisation) | **≤ 2.0×** | a quadratic-class super-linear writer — **effective trip ≈ O(rows^1.5) and worse**; a true O(rows²) writer shows ≈ 128× |

`scripts/linearity_gate.sh`, CI job `bench-gate`. The ceiling is a **shape
bound**, not an absolute performance number, and it catches super-linear SHAPE,
not magnitude: a uniform constant-factor slowdown that preserves the ratio passes
silently here (that case is the H1/H2 absolute backstop's job, §H2). **Honest
tightness caveat:** because the healthy per-row cost *falls* with amortisation
(ratio ≈ 0.2×, far below 1.0), the 2.0× ceiling **reads tighter than it
enforces** — it trips only at roughly **O(rows^1.5) and worse**, a quadratic-class
regression, not at an `n·log n` drift. The gated quantity is a within-run
dimensionless ratio, so a uniform clock factor cancels and the gate is portable to
Linux CI.

### Downstream

This is the **v0.3 bundle-writer baseline** the #51 linearity gate builds on. The
gate wraps the per-row cost ratio above (it does not record a new baseline)
([docs/07 §6](docs/07-performance-and-security.md#6-regression-gates-in-ci-before-v10)).

---

## H5 / PB-6 — PyO3 per-call overhead vs the batch path (v0.4 baseline, #43)

Two complementary measurements, one on each side of the boundary:

- **Bench (Rust side, marshalling cost):** `benches/pyo3_marshal.rs`
  (`PYO3_PYTHON=python3.12 cargo bench --features python --bench pyo3_marshal`)
  — `criterion` + `hdrhistogram`, driving the **real** registered `#[pymodule]`
  in an embedded interpreter (`append_to_inittab!` + `Python::initialize`, the
  same in-process mechanism as `tests/python_parity.rs`).
- **Script (Python side, per-call vs batch):** `python/benches/pyo3_overhead.py`
  — a **standalone, manually-run** script (deliberately **not** a `pytest`: the
  filename is not `test_*` and it lives under `python/benches/`, so the <30 s test
  gate never collects it). Run it against a **release** module:
  ```bash
  # from python/, into a 3.12 venv with maturin + pyarrow
  maturin develop --release --features "python orderbook simulator"
  python python/benches/pyo3_overhead.py           # --steps/--iters/--batch/--threads
  ```
- **Date measured:** 2026-07-16

### What each measures — the honest isolation

- **`marshal_full_config` (Rust):** building one fully-populated `BacktestConfig`
  through the chainable builders — `BacktestConfig(seed, capital)` +
  `.data_parquet` + `.strategy_iron_condor` + `.exit_time_steps` +
  `.execution_naive` + `.fees` + `.output_dir` — i.e. the **marshal-in path**, the
  seven Python→Rust trampoline crossings a caller performs *before* `ic.run()`
  reaches the engine. **INSIDE** the timed region: the seven crossings, PyO3
  argument extraction of every field (`int`→`u64`, `str`→`String`, `float`→`f64`,
  the 17-key kwargs dict → the typed `IronCondorSpec` args), the Rust construction
  each builder does — notably `strategy_iron_condor`'s `Underlying::new` grammar
  check, `Quantity::new`, three `Decimal::try_from` conversions and its
  `guard_boundary` `catch_unwind` — and the `#[pyclass]` allocation of the new
  config. **OUTSIDE:** the input kwargs / fees `PyDict`s (pre-built once and
  reused, so the timed cost is the *marshalling*, not Python-side dict
  construction), the GIL acquire (held across the whole measurement — the
  marshal-in path runs under the GIL in a real caller too, the GIL being *released*
  only later inside `run()` around the engine), and the entire engine + writer.
- **`marshal_single_call` (Rust):** one `.seed(u64)` builder crossing — the
  **fixed per-boundary-call floor**: a single trampoline + one `u64` extract + the
  `PyRefMut` self-return. Each timed sample runs an inner batch of **512**
  crossings and records the **mean** per crossing (to lift the signal above
  `Instant::now` resolution), so its percentiles are batch-mean percentiles.
- **Documented gap (Rust):** `PyBacktestConfig::to_rust()` — the final marshal of
  the wrapper into the real `BacktestConfig` + `StrategySpec` + `ExitPolicy` plus
  `validate()` — is `pub(crate)` and **not reachable via the Python object
  protocol**; it runs *inside* `run()` immediately before the (GIL-released)
  engine, so it cannot be timed in isolation from the engine on the public
  surface. `marshal_full_config` measures the builder marshal-in a caller pays
  explicitly; the `to_rust()` finalisation is a bounded one-time extra folded into
  `run()` (the same field copies + one `validate()` the builders already
  exercise). The bench does **not** widen the binding's public surface to reach it.
- **`ic.run()` single-call + batch (Python):** end-to-end wall time of one
  `ic.run(cfg)` over a small fixed scenario (marshal-in under the GIL → the
  **GIL-released** engine + bundle writer → the handle-out), then a fixed batch of
  runs fanned across `ThreadPoolExecutor` worker threads for `N = 1, 2, 4, 8, 12,
  16`. Because `run()` releases the GIL for the whole engine + writer
  (`py.detach`, `src/python/run.rs`), the N Rust engines execute concurrently, so
  the per-run wall time falls as N grows — the across-run parallelism the engine
  is built for ([docs/06 §3](docs/06-python-bindings.md#3-gil-strategy),
  [docs/02 §9](docs/02-engine-architecture.md#9-scenario-orchestration)). Only
  `ic.run(cfg)` is inside the single-call timer; the config build is excluded (it
  is the `marshal_full_config` quantity above).

- **Workload / scale:** the same canonical four-leg iron condor as PB-2/PB-3
  (short call 510000 / long call 520000 / short put 490000 / long put 480000,
  tick-aligned to 5c, mids 2000/800/1800/700, seed `42`, non-triggering
  `ExitPolicy::TimeSteps`). The Rust marshalling benches marshal that config with
  **no engine run** (a dummy chain path that is never opened). The Python script
  runs a **512-step** tape (4 legs, naive mode) per `ic.run()` — small enough to
  keep the script quick, large enough that the GIL-released engine + writer give
  the batch fan-out real work to overlap.
- **Samples:** Rust `marshal_full_config` **8 022 203** recorded marshals; Rust
  `marshal_single_call` **257 321** recorded batch-means (each the mean over 512
  crossings). Python single-call **400** sequential `ic.run()` calls after a
  30-run warmup; each batch point is **one** batch-wall sample (96 runs) after a
  2-batch warmup.

### Measured — Rust marshalling cost (`criterion` `bench` profile, hdrhistogram)

| Rust boundary quantity | p50 | p99 | p99.9 | p99.99 | max |
|------------------------|-----|-----|-------|--------|-----|
| **`marshal_full_config`** (7 crossings, full strategy marshal) | **1250 ns (1.25 µs)** | 1584 ns | 1708 ns | 3667 ns | 61 183 ns |
| **`marshal_single_call`** (one `.seed(u64)` crossing, batch-mean) | **78 ns** | 94 ns | 107 ns | 191 ns | 2375 ns |

- **Per-crossing:** the trivial-crossing **floor is ≈ 78 ns** (trampoline + one
  `u64` extract + self-return, GIL already held). The full 7-crossing config
  marshal averages **≈ 179 ns/crossing** (1250 / 7) — higher than the floor
  because it is dominated by the one heavy `strategy_iron_condor` crossing (17-arg
  extract + `Underlying`/`Quantity`/three `Decimal::try_from` + `catch_unwind`),
  not because a crossing is intrinsically expensive.
- **criterion cross-check** (its own mean estimate, for sanity):
  `marshal_full_config` `[1.2473 µs 1.2507 µs 1.2538 µs]`; `marshal_single_call`
  `[40.287 µs 40.408 µs 40.543 µs]` **per 512-crossing batch** ⇒ 78.7–79.2
  ns/crossing — both consistent with the hdrhistogram p50s, which validates the
  measurement.

### Measured — Python `ic.run()` per-call + batch (release module, warm)

Single-call wall time (512-step scenario, closed-loop, 400 samples after warmup):

| Python `ic.run()` (512 steps) | p50 | p90 | p99 | p99.9 | min | max |
|-------------------------------|-----|-----|-----|-------|-----|-----|
| **Wall / call** | **2.99 ms** | 3.10 ms | 3.28 ms | 3.36 ms | 2.88 ms | 3.36 ms |

Batch amortisation — a fixed batch of **96** runs fanned across N worker threads,
per-run = batch-wall / 96, speedup = per-run(1) / per-run(N):

| Threads N | batch | batch wall | per-run | speedup |
|-----------|-------|-----------|---------|---------|
| 1 | 96 | 290.7 ms | 3.028 ms | 1.00× |
| 2 | 96 | 187.8 ms | 1.957 ms | 1.55× |
| 4 | 96 | 84.0 ms | 0.875 ms | 3.46× |
| **8** | 96 | 53.4 ms | **0.557 ms** | **5.44×** |
| 12 | 96 | 57.6 ms | 0.600 ms | 5.05× |
| 16 | 96 | 58.5 ms | 0.610 ms | 4.97× |

- **The batch amortises the per-run wall via across-run parallelism:** from **3.03
  ms/run at N=1** down to **0.557 ms/run at N=8** (5.44×), i.e. throughput lifts
  from **≈ 330 runs/sec to ≈ 1795 runs/sec** on the reference box. The curve
  **saturates at ≈ N=8** and slightly *regresses* at 12/16 — past 8 concurrent
  runs the **bundle writer's filesystem I/O** (each run atomically writes four
  Parquet tables + a manifest via a temp-dir + rename) is the ceiling, **not** the
  PyO3 boundary and **not** the GIL (which is released for the whole run). A second
  independent pass (`--batch 64`) reproduced the shape: 1.00× / 1.81× / 3.55× /
  5.14× for N = 1/2/4/8.

### The boundary is a fraction of a percent of a run

- **Marshal-in vs a full run:** the full config marshal-in **p50 ≈ 1.25 µs** is
  **≈ 0.04 %** of a single 512-step `ic.run()` (**≈ 2.99 ms**) — about **1 part in
  2400**. The per-crossing floor (**≈ 78 ns**) is smaller still.
- **Where the 2.99 ms actually goes:** the engine (H1/PB-2 scaled to 512 steps ≈
  512 × 2.35 µs/step ≈ **1.2 ms**) plus the bundle writer (H4/PB-5, ≈ **1.6–1.8
  ms** for the ~3080-row four-table bundle) account for essentially all of it; the
  PyO3 boundary is the ~0.04 % remainder. **The marshalling never dominates a
  run** — it is already amortised into insignificance by the engine + writer cost
  of even **one** run.

**Coordinated-omission disclosure.** Every part is **closed-loop** — each marshal
/ each `ic.run()` / each batch starts immediately after the previous finishes,
with no external arrival schedule — so coordinated omission **does not apply**;
the histograms record raw back-to-back latency (`record`, not `record_correct`).

**Warmup.** Rust: an explicit 64-marshal un-recorded warmup on top of criterion's
own. Python: a 30-run warmup before the single-call phase and a 2-batch warmup
before the amortisation sweep; all single-call figures are steady-state.

**Tail-resolution / sampling caveat (honest).** The Rust histograms are richly
resolved (8.0 M / 257 k samples). The Python **single-call** percentiles rest on
400 samples — p50/p90/p99 are solid, p99.9 is the observed max (treat as
"≥ p99"). Each **batch** point is a **single** batch-wall sample (not a
percentile), so the individual per-run numbers carry ≈ 10–15 % run-to-run noise
(compare the N=2 point across the two passes: 1.55× vs 1.81×); the **curve shape**
— strong amortisation through ~N=8, plateau/regression after — is the stable,
reported result, not any single cell.

### Run conditions

| Field | Value |
|-------|-------|
| CPU | Apple M4 Max |
| Cores | 16 (16 physical) |
| Memory | 64 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| Toolchain | rustc 1.97.0 (2d8144b78 2026-07-07) |
| Python | 3.12.13 (Homebrew) |
| Rust bench profile | `bench` (optimized, `cargo bench`, `PYO3_PYTHON=python3.12`) |
| Python module build | `maturin develop --release --features "python orderbook simulator"` (optimized) |
| PyO3 | 0.29.0 (`abi3-py310`) |
| Bench harness | `criterion` 0.5.1, `harness = false`; Python percentiles are a pure sorted-array reduction (no numpy) |
| Percentiles | `hdrhistogram` 7.5.4 (3 significant figures) on the Rust side |
| `Cargo.lock` sha256 | `52e6e87afd51d878df63c046c2177946677ea92e5b59e55d1ed602063fabcb7b` |

### Interpretation

- **What the numbers mean.** One PyO3 boundary crossing costs **≈ 78 ns** and
  marshalling a **complete** `BacktestConfig` costs **≈ 1.25 µs** on the reference
  machine — **≈ 0.04 %** of even a small 512-step `ic.run()`. The Python cost of a
  run is the engine + writer, not the boundary. Fanning a batch of independent
  runs across GIL-releasing threads amortises the per-run **wall** time **5.4×**
  (to ≈ 0.56 ms/run at 8 threads), saturating near the core count where the
  writer's filesystem I/O — not the boundary — becomes the ceiling.
- **Did it meet PB-6?** PB-6's DESIGN TARGET is **"per-call overhead documented and
  bounded; the batch path amortises it"**
  ([docs/07 §3](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
  **MET, and in the stronger sense:** the per-call overhead is not merely bounded,
  it is **negligible** (~0.04 % of a run) *before* any batching; and the batch path
  **does** amortise the per-run wall (5.4× at N=8) via the across-run parallelism
  the GIL-release enables. Recorded, not asserted.
- **When to batch (the plain guidance).** Because the boundary is never the
  bottleneck, batching is **not** about hiding marshalling — it is about running
  **independent** runs (parameter sweeps, Monte-Carlo grids) **concurrently**. Fan
  them across a `ThreadPoolExecutor`; the win appears from **N = 2** and is strong
  through **≈ N = 8** (5.4× here), then plateaus. Rule of thumb on this hardware:
  batch whenever you have ≥ 2 independent runs, size the pool near the physical
  core count, and expect I/O-bound saturation around 8 — a single run needs no
  batching, and no batch width recovers a cost the boundary never charged.
- **Scale / honest single-machine scope.** All figures are one Apple M4 Max. The
  **absolute** per-run wall and the **saturation thread count** are
  hardware/filesystem-specific (they will differ on Linux CI runners and on
  spinning vs NVMe storage); the **portable takeaways** are the per-crossing floor
  (~78 ns), the full-marshal cost (~1.25 µs), and the **boundary fraction of a run
  (~0.04 %)**, which a uniform clock-speed factor largely preserves.

### Regression-gate tolerance (#51)

| Gated quantity | Baseline | Band | Trips on |
|----------------|----------|------|----------|
| full-config / single-crossing marshal ratio p50 | 16.03× (1250 ns / 78 ns) | **[8×, 32×]** | a ~2× disproportionate regression in the config/strategy marshal path (ceiling) or an inflated crossing floor (floor) |

`scripts/pyo3_gate.sh`, CI job `bench-gate` (built with the `python` feature under
`PYO3_PYTHON`). The gate tracks the **dimensionless full/single ratio**, not an
absolute nanosecond count: both numbers are measured in the same embedded
interpreter, same process, so a uniform clock factor cancels and the gate is
portable to Linux CI. It catches a **disproportionate** marshal-path regression
(SHAPE), not magnitude — a uniform slowdown of *both* the full and single crossing
preserves the ratio and passes silently (as with H3/H4, the uniform-slowdown case
is the H1/H2 absolute backstop's job, §H2). The band is a factor of two each way
around the committed 16× baseline — noise-safe with reduced CI samples, tight
enough to catch a ~2× disproportionate marshal-path regression.
`marshal_single_call` is the batch-mean per-crossing floor (each sample the mean
over 512 crossings), so the ratio is stable even at reduced sample counts
(measured 15.7–16.4× reduced-sample here).

### Downstream

This is the **v0.4 PyO3-boundary baseline** the #51 marshal-ratio gate builds on.
The gate wraps the full/single ratio above (it does not record a new baseline)
([docs/07 §6](docs/07-performance-and-security.md#6-regression-gates-in-ci-before-v10)).

[`run_backtest`]: src/run.rs
[`write_bundle`]: src/bundle/writer.rs
[`BacktestRun`]: src/engine/backtest.rs
[`ic.run()`]: src/python/run.rs
