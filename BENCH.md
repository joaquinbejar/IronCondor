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

### Downstream

This is the **v0.3 bundle-writer baseline** the future gate builds on:

- **#051 — percentile-regression gate (before v1.0).** CI will compare a tracked
  writer percentile (or the per-row cost / bytes-per-row) against **this committed
  baseline**. This issue lands the **measurement**; #051 wraps it in the gate
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

### Downstream

This is the **v0.4 PyO3-boundary baseline** the future gate builds on:

- **#051 — percentile-regression gate (before v1.0).** CI will compare a tracked
  boundary percentile (the `marshal_full_config` p50, or the per-crossing floor)
  against **this committed baseline**, not an absolute number, so it tracks its own
  hardware. This issue lands the **measurement**; #051 wraps it in the gate
  ([docs/07 §6](docs/07-performance-and-security.md#6-regression-gates-in-ci-before-v10)).

[`run_backtest`]: src/run.rs
[`write_bundle`]: src/bundle/writer.rs
[`BacktestRun`]: src/engine/backtest.rs
[`ic.run()`]: src/python/run.rs
