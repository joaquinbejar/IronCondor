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
  the whole run (a non-triggering `ExitPolicy::TimeSteps` so every step reprices
  the four legs — the representative naive-mode per-step workload). Fixed seed
  (`42`); the tape stays strictly before expiry so every step reprices in the
  normal pre-expiry regime.
- **Samples:** **1111** recorded runs (criterion warmup + measurement, all at
  steady state after an explicit 32-run warmup).

### Measured percentiles

Per-run latency is the raw measured quantity (each timed iteration is one full
`run_backtest` over the 2048-step chain). Per-step is that latency divided by the
fixed 2048 steps — a fair per-step figure because every run processes exactly the
same step count.

| Metric | p50 | p99 | p99.9 | p99.99 |
|--------|-----|-----|-------|--------|
| **Per-run latency** | 8544.3 µs (8.54 ms) | 9158.7 µs (9.16 ms) | 17727.5 µs (17.73 ms) | 21430.3 µs (21.43 ms) |
| **Per-step latency** | **4172 ns/step** | 4472 ns/step | 8656 ns/step | — |

- **Throughput (from the p50 per-run latency):** **≈ 239,700 steps/sec/core =
  ≈ 14.38 × 10⁶ steps/min/core.**
- **Per-step cost (p50):** ≈ **4.17 µs/step** (four legs repriced per step).
- **criterion cross-check** (mean/median, the harness's own estimate, for
  sanity): time `[8.5531 ms 8.5700 ms 8.5876 ms]`,
  throughput `[238.48 Kelem/s 238.97 Kelem/s 239.45 Kelem/s]` — consistent with
  the hdrhistogram p50, which validates the measurement.

**Coordinated-omission disclosure.** This is a **closed-loop** throughput bench:
each run starts immediately after the previous one finishes, with no external
arrival schedule. Coordinated omission (a slow response masking the lateness of
requests that *should* have been issued on a fixed cadence) **does not apply** —
there is no expected inter-arrival interval to correct against, so the histogram
records raw back-to-back run latency (`Histogram::record`, not `record_correct`).

**Tail-resolution caveat (honest).** With 1111 recorded runs, p50 and p99 are
well resolved and p99.9 sits at the resolution edge (~1000 samples). p99.99
needs ~10⁴ samples to be distinct, so at this sample count it collapses to the
observed max (21430.3 µs) — treat it as "≥ p99.9", not an independently resolved
quantile.

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
  2048-step, 4-leg iron-condor chain — Parquet parse, 2048 repricings of the four
  held legs, naive fills, and the mark-to-market ledger — costs **≈ 8.5 ms**, i.e.
  **≈ 4.17 µs per step** and **≈ 240k steps/sec/core** on the reference machine.
  The per-step cost is dominated by the per-step chain rebuild the exit seam
  performs to reprice the open legs (`snapshot_to_option_chain`) — the one
  documented per-step allocation the naive path makes today
  ([docs/07 §3 PB-1](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
- **Scale.** 2048 steps × 4 legs; the number is per single core (no cross-run
  parallelism — Monte-Carlo parallelism is across runs, not within one).
- **Did it meet the DESIGN TARGET?** PB-2's DESIGN TARGET is "naive-mode
  throughput **on the order of 1 × 10⁶ steps/min per core**", deliberately
  order-of-magnitude
  ([docs/07 §3](docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
  Measured: **≈ 14.4 × 10⁶ steps/min/core** — about **14×** the target, i.e. the
  baseline **meets (comfortably exceeds) the order-of-magnitude target** (it is
  faster, not slower). No regression to explain.
- **This becomes the fixed budget.** Per docs/07 §3, the real PB-2 budget is
  **fixed at v0.1 from this first `criterion` baseline**, not from the
  order-of-magnitude sanity target. This entry **is** that baseline.

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

[`run_backtest`]: src/run.rs
