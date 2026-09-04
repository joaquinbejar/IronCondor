# Hot-path regression gates — fail-closed evidence (#51)

This file records the **fail-closed** evidence for the v1.0 hot-path regression
gates (issue #51, [docs/07 §6](../docs/07-performance-and-security.md),
[docs/TESTING.md §11.1](../docs/TESTING.md)). Every gate is proven to (a) PASS on
the committed baseline and (b) FAIL when the tracked quantity crosses its
tolerance. All numbers below are the raw output of an actual run on the machine
in the run-conditions block — no number is written before it is measured.

The gates and their scripts:

| Hot path | Gate script | CI step (job `bench-gate`) | Gated quantity | Band / ceiling |
|----------|-------------|----------------------------|----------------|----------------|
| H1 replay loop + H2 fill models | `scripts/bench_gate.sh` (#29) | H1+H2 ratio gate | realistic/naive overhead ratio p50 | **[18×, 72×]** + coarse naive backstop ≤ 100 000 ns |
| H3 conversion | `scripts/linearity_gate.sh` | H3+H4 linearity gate | per-contract cost ratio (16× sweep) | **≤ 4.0×** |
| H4 bundle writer | `scripts/linearity_gate.sh` | H3+H4 linearity gate | per-row cost ratio (128× sweep) | **≤ 2.0×** |
| H5 PyO3 boundary | `scripts/pyo3_gate.sh` | H5 marshal-ratio gate | full/single marshal ratio p50 | **[8×, 32×]** |
| PB-1 zero-alloc | `cargo test --test zero_alloc` | PB-1 reaffirmation | naive per-step alloc-event delta warmup→last | **must be 0** (canonical gate = `zero-alloc` job) |
| PB-3 realistic allocation | `cargo test --features orderbook --test zero_alloc` | — (canonical gate = `zero-alloc` job) | realistic per-warm-step alloc-event count, and its growth across the tail | **ceiling 705 events/warm step** + **one-sided linearity ≤ 2 %** |

**Every gate is baseline-relative — no absolute-number threshold** except the one
documented `#29` coarse naive ceiling (a catastrophic-only backstop, ~45× the M4
baseline, never approached on CI hardware). Each gated quantity is a
**dimensionless within-run ratio**, so a uniform hardware clock-speed factor
cancels and the gate is portable from the Apple M4 Max baselines in `BENCH.md` to
the Linux CI runners.

### Honest scope — what these gates do and don't catch

The **H3 / H4 / H5** gates catch a **super-linear / disproportionate SHAPE**
regression, **not a magnitude** one. A uniform constant-factor slowdown that
**preserves the ratio** passes silently — that is by design (it is what keeps the
ratio hardware-portable), and it is the **H1/H2 coarse absolute naive backstop**
(the sole absolute number, `scripts/bench_gate.sh`), **not** the linearity/ratio
gates, that guards the uniform-slowdown case. So "one gate per hot path H1–H5"
means each path has a gate against its baseline, **not** that every possible
regression on that path is caught. Two further honesty notes carried into the
tolerances (BENCH.md):

- **H3** gates the `raw_quotes_to_snapshot` validation core only (the per-snapshot
  materialisation cost), **not** `snapshot_to_option_chain` (the deferred-reprice
  sibling, off the per-step loop).
- **H4**'s 2.0× ceiling reads **tighter than it enforces**: the healthy ratio is
  ≈ 0.2× (per-row cost falls with amortisation), so the ceiling trips only at
  roughly **O(rows^1.5) and worse** — a quadratic-class regression, not an
  `n·log n` drift.

## Run conditions

| Field | Value |
|-------|-------|
| CPU | Apple M4 Max |
| Cores | 16 (16 physical) |
| Memory | 64 GiB |
| OS | macOS 26.5.2 (build 25F84) |
| Toolchain | rustc 1.97.0 (2d8144b78 2026-07-07) |
| Python | 3.12.13 (Homebrew), `PYO3_PYTHON=python3.12` |
| `Cargo.lock` sha256 | `52e6e87afd51d878df63c046c2177946677ea92e5b59e55d1ed602063fabcb7b` |
| Date | 2026-07-17 |

## How the fail-closed cases were produced

Two techniques, both honest about what they exercise:

- **Override self-test** — every gate reads its tracked quantity from an
  `*_OVERRIDE` env var when set, bypassing the bench, so the parse + compare +
  exit-code path is proven to fail closed on a tripping value without a slow
  bench run. This is the same mechanism the committed `#29` gate documents
  (`BENCH_GATE_RATIO_OVERRIDE`).
- **Real bench-driven scratch** — for the three gates this issue adds/extends
  (H3, H4, H5) a temporary artificial cost was injected **into the bench timed
  region only** (never production code — `src/data/convert.rs`,
  `src/bundle/writer.rs`, `src/python/*` were untouched), the gate was run
  end-to-end against the genuinely-regressed measurement, and the scratch was
  **reverted immediately** (`benches/bundle_write.rs` is byte-identical to HEAD;
  no `SCRATCH` marker remains in any bench). This proves the full
  bench → parse → compare → fail path, not just the compare.

---

## H1 + H2 — `scripts/bench_gate.sh` (realistic/naive overhead ratio)

Baseline PASS is the committed `#29` gate (BENCH.md §H2): the reduced-sample p50
ratio (~34×) sits inside `[18×, 72×]`. Fail-closed via override (the ratio gate
already carries committed real-regression evidence in BENCH.md §H2):

```
$ BENCH_GATE_RATIO_OVERRIDE=99  … bench_gate.sh   # realistic blow-up
bench-gate: FAIL — overhead ratio 99x > ceiling 72.0x.          exit=1
$ BENCH_GATE_RATIO_OVERRIDE=9   … bench_gate.sh   # naive regression
bench-gate: FAIL — overhead ratio 9x < floor 18.0x.             exit=1
$ BENCH_GATE_NAIVE_NS_OVERRIDE=200000 … bench_gate.sh  # coarse backstop
bench-gate: FAIL — naive per-step 200000 ns > coarse ceiling 100000.0 ns.  exit=1
$ BENCH_GATE_RATIO_OVERRIDE=36 BENCH_GATE_NAIVE_NS_OVERRIDE=2186 … bench_gate.sh
bench-gate: PASS — within the committed band.                   exit=0
```

A **uniform** slowdown of both modes does not move the ratio by design (that is
what makes it portable); the coarse naive backstop above is the documented catch
for that one case.

---

## H3 — conversion linearity (`scripts/linearity_gate.sh`)

**Baseline PASS** (real bench, current code):

```
PB4_CONVERT_COST_RATIO=1.1402
linearity-gate: conversion per-contract cost ratio = 1.1402x  (ceiling 4.0x)
linearity-gate: PASS — both hot paths scale linearly (within the committed shape bands).
```

**Fail-closed, real super-linear scratch** — an artificial `O(contracts²)` cost
in the `benches/conversion.rs` timed region (`quad = contracts² × 8`), reverted
after capture:

```
PB4_CONVERT_COST_RATIO=10.7114
linearity-gate: conversion per-contract cost ratio = 10.7114x  (ceiling 4.0x)
linearity-gate: FAIL — conversion per-contract cost ratio 10.7114x > ceiling 4.0x.
            H3 conversion regressed super-linearly (per-contract cost rising with contract count).
exit=1
```

Override control (writer branch held in-band): a forced conversion ratio of `16`
FAILs, `1.126` PASSes.

---

## H4 — bundle-writer linearity (`scripts/linearity_gate.sh`)

**Baseline PASS** (real bench, current code): per-row cost ratio ≈ 0.14–0.25×
(the cost *falls* with amortisation — see BENCH.md §H4), well under the 2.0×
ceiling.

**Fail-closed, real super-linear scratch** — an artificial `O(rows²)` cost in the
`benches/bundle_write.rs` timed region (`quad = total² × 16`, with `STEP_SIZES`
and `WARMUP_WRITES` shrunk in the same scratch purely to keep the quadratic run
fast), reverted after capture:

```
PB5_PER_ROW_COST_RATIO=9.8399
linearity-gate: writer per-row cost ratio       = 9.8399x  (ceiling 2.0x)
linearity-gate: FAIL — writer per-row cost ratio 9.8399x > ceiling 2.0x.
            H4 bundle writer regressed super-linearly (per-row cost rising with row count).
exit=1
```

Override control: a forced writer ratio of `9` FAILs, `0.25` PASSes.

---

## H5 — PyO3 marshal ratio (`scripts/pyo3_gate.sh`)

**Baseline PASS** (real bench, current code):

```
MARSHAL_FULL_NS_P50=1292   MARSHAL_SINGLE_NS_P50=79   MARSHAL_RATIO_P50=16.3544
pyo3-gate: measured full/single marshal ratio p50 = 16.3544x  |  band = [8.0x, 32.0x]
pyo3-gate: PASS — within the committed band.
```

**Fail-closed, real disproportionate scratch** — a fixed extra cost in the
`marshal_full_config` timed region **only** (not `marshal_single_call`) in
`benches/pyo3_marshal.rs`, reverted after capture:

```
MARSHAL_FULL_NS_P50=3959   MARSHAL_SINGLE_NS_P50=79   MARSHAL_RATIO_P50=50.1139
pyo3-gate: measured full/single marshal ratio p50 = 50.1139x  |  band = [8.0x, 32.0x]
pyo3-gate: FAIL — full/single marshal ratio 50.1139x > ceiling 32.0x.
            The config/strategy marshal path regressed disproportionately to the crossing floor.
exit=1
```

Override control: a forced ratio of `99` FAILs (ceiling), `3` FAILs (floor),
`15.69` PASSes.

---

## PB-1 — zero-alloc replay-loop gate (reaffirmation)

Not re-implemented here. The canonical hard gate is the separate `zero-alloc` CI
job (`cargo test --test zero_alloc`, **4/4 with default features**; **8/8 under
`--features orderbook`**, which adds the realistic-mode section below — including
the two negative probes). The "must be 0" quantity is **naive mode only**: it is
the negative test that a single injected `Vec` allocation in the step body yields
a non-zero tail delta (BENCH.md §H1/PB-1). The `bench-gate` job **reaffirms** the
naive invocation by re-running the same test in the perf-gate stage, so the
zero-steady-state-allocation invariant fails the build alongside the
percentile/linearity gates. Baseline: 4 passed (default), 8 passed (`orderbook`).

---

## PB-3 — realistic-mode per-step allocation gate (#127)

Not re-implemented here either: it lives in the same `tests/zero_alloc.rs`, behind
`#[cfg(feature = "orderbook")]`, and the canonical gate is the `zero-alloc` CI
job's second invocation, `cargo test --features orderbook --test zero_alloc`.

Realistic fills route every order through the upstream matching engine, so the
warm step allocates **by construction** and **cannot** be gated at zero. It is
gated at a **measured ceiling** instead — 705 allocation events per warm step, the
measured ≈ 564 plus 25 % headroom — together with a **one-sided linearity** check
that the second half of the warm window exceeds the first by at most 2 %. The
check is one-sided because a shrinking second window is never a leak; see
BENCH.md §H2/PB-3 "Per-step allocation" for the measurement and the derivation.

**Fail-closed evidence — two negative probes, one per assertion:**

| Probe | Injected per step | Observed | Assertion it breaks |
|-------|-------------------|----------|---------------------|
| `BadStepMany` | constant 400 allocations (≈ 2.8× the headroom) | tail delta far above the 38 775 ceiling | the ceiling |
| `BadStepGrowing` | `8 × step_index` allocations (a leak's shape) | second window 5635–5645 above the first vs a 399–400 tolerance, ~14× | the one-sided linearity check |

Baseline: **8 passed** (`cargo test --features orderbook --test zero_alloc`) —
the 4 naive tests plus the realistic ceiling, the realistic linearity check, and
the two negative probes above.
