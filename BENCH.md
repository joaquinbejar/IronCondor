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
| `Cargo.lock` sha256 | `7dc4105a15aefa459f720cee5cb442fbd4e504fbcc1563c765cae6c38aa7535e` |

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
