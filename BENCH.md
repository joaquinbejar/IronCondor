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
- **#051 — percentile-regression gate (before v1.0).** CI will compare a tracked
  hot-path percentile against **this committed baseline** (not an absolute
  number, so it tracks its own hardware). A merge that regresses the tracked
  percentile beyond the stated tolerance fails
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

### Per-step allocation — realistic mode is NOT under the zero-alloc gate

The naive warm step allocates **0 events/step** and IS gated (PB-1 below). The
**realistic warm step is intentionally NOT gated**, and this is an honest,
measured cost, not an oversight:

- **Measured:** on the 4-leg condor, realistic mode allocates **≈ 564 allocation
  events per warm step** (headline derivation: steady-state tail delta 31 005
  events over 55 warm steps ⇒ 31 005 / 55 ≈ 564/step — measured with the same
  counting-`GlobalAlloc` technique as `tests/zero_alloc.rs`, driving
  `RealisticFill` instead of `NaiveFill`).
- **Why.** The per-step book refresh (#25) calls
  `OptionOrderBook::add_limit_order_full` for every reseed level, and each call
  returns an **owned `TradeResult` whose `symbol` `String` heap-allocates upstream
  even when nothing crosses** (the #25 P2-01 finding — see the
  `resting_seed_ids` doc comment in `src/execution/realistic.rs`). With ≈ 48
  reseed orders + 48 cancels per step across the four legs, that upstream
  per-call capture cost — the symbol `String` (~48–96 events) **plus the matching
  engine's internal per-order allocations**, which are the larger share — accounts
  for the ≈ 564 events. `ironcondor`'s own tracking state (`resting_seed_ids` /
  `seed_plan`) is cleared **in place** and does not contribute steady-state
  allocation; the residual is entirely upstream capture-API cost.
- **Consequence.** Adding realistic mode to the PB-1 zero-alloc gate would make
  the gate a **lie** (it would fail, or force a false invariant), so the gate
  stays naive-only. The overhead ratio above **already reflects** this allocation
  cost — it is priced into the 36× number. The residual is a candidate for a
  future **upstream symbol-borrowing optimisation** (a capture API that borrows
  the book's symbol rather than cloning it into each `TradeResult`); when that
  lands, the ratio here is the before-number to measure against.

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
- **Relationship to #051.** This is the first live percentile-tracked regression
  gate; #051 extends the same machinery to the remaining hot paths (conversion,
  bundle writer, PyO3) against their `BENCH.md` baselines.

---

## H1 / PB-1 — Zero-steady-state-allocation replay-loop gate (invariant, #19)

This is **not a throughput measurement** — it is an **invariant gate** and is
recorded here only to distinguish it from the PB-2 baseline above. It gates like
a correctness test, not a benchmark: a regression **fails the build**
([docs/07 §4/§6](docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop),
[docs/TESTING.md §11.1](docs/TESTING.md#111-performance-regression-gates)).

- **Gate:** `tests/zero_alloc.rs` (`cargo test --test zero_alloc`); CI job
  `zero-alloc`, separate from the `naive_throughput` bench (PB-2 above) and the
  `golden` job (#17).
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
- **Naive-mode only, by design.** This gate covers the **naive** warm step (0
  events/step). The **realistic** warm step is intentionally **out of scope**: it
  allocates ≈ 564 events/step from the upstream `add_limit_order_full` capture API
  (the #25 P2-01 finding), so gating it to zero would be a lie. That cost is
  measured, documented, and priced into the overhead ratio in **PB-3 above** —
  see its "Per-step allocation" block.

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

[`run_backtest`]: src/run.rs
