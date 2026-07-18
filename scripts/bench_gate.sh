#!/usr/bin/env bash
#
# bench_gate.sh — the naive-throughput / realistic-overhead regression gate
# (issue #29, docs/07 §6, docs/TESTING.md §11.1).
#
# WHAT IT GATES, AND WHY IT IS HARDWARE-PORTABLE
# ----------------------------------------------
# The committed BENCH.md baselines are Apple M4 Max, but CI runs on GitHub Linux
# runners — different, noisier hardware. An absolute-nanoseconds comparison
# against the M4 numbers would false-positive on every CI run. So the PRIMARY
# gated quantity is the DIMENSIONLESS realistic/naive overhead RATIO measured by
# `benches/realistic_overhead.rs`.
#
# The ratio is portable because both modes run the SAME `run_backtest` over the
# SAME fixed tape in the SAME process, differing only in the fill model. Their
# shared cost (Parquet open, ledger re-mark, metrics) is identical, so a uniform
# clock scaling factor `f` from slower hardware cancels:
#
#     ratio = (f·shared + f·realistic_fill) / (f·shared + f·naive_fill)
#           = (shared + realistic_fill) / (shared + naive_fill)   [f cancels]
#
# The ratio therefore only moves when the RELATIVE per-step cost of one fill
# model changes — i.e. a real algorithmic regression in naive or realistic mode —
# not when the runner is merely slow. That is exactly what we want to gate.
#
# WHAT IT CATCHES
# ---------------
#   * A naive-mode throughput regression: naive gets slower, the ratio DROPS
#     toward 1 → trips RATIO_LOW. (This is the headline "naive-throughput gate":
#     the naive baseline that funds fast iteration is protected.)
#   * A realistic-mode blow-up: realistic gets slower, the ratio RISES → trips
#     RATIO_HIGH.
# A COARSE absolute naive ceiling (NAIVE_CEILING_NS) is a secondary backstop for
# the one case the ratio is blind to: a regression that slows BOTH modes by the
# same factor (shared infrastructure) keeps the ratio constant. The ceiling is
# set far above any plausible CI hardware (~45x the M4 per-step number), so it
# only trips on a catastrophic absolute regression — never on CI slowness.
#
# HONEST SCOPING (per issue #29): a truly robust ABSOLUTE throughput gate is not
# feasible on shared, noisy CI runners without a per-run calibration baseline, so
# we gate the portable ratio plus a coarse absolute backstop and say so plainly.
# Gross = a ~2x disproportionate change; the band is a factor-of-two each way
# around the committed baseline, wide enough that ordinary CI variance (a handful
# of samples) cannot trip it.
#
# USAGE
#   scripts/bench_gate.sh                 # run the bench, gate the parsed ratio
#   BENCH_GATE_RATIO_OVERRIDE=99 \
#     scripts/bench_gate.sh               # self-test: force a value, prove it fails
#   BENCH_GATE_NAIVE_NS_OVERRIDE=200000 \
#     scripts/bench_gate.sh               # self-test the absolute backstop
#
# Exit 0 = within band (gate passes); exit 1 = regression (gate fails the build).

set -euo pipefail

# --- committed baseline + tolerances (BENCH.md PB-3, Apple M4 Max) -----------
# Measured overhead ratio p50 = 36.13x (naive per-step 2186 ns, realistic
# 78976 ns). The band is a factor of two each way — noise-safe with reduced CI
# samples, and trips on a ~2x disproportionate regression in either mode.
readonly RATIO_LOW="18.0"    # naive-regression floor  (ratio drops toward 1)
readonly RATIO_HIGH="72.0"   # realistic-blowup ceiling (ratio climbs)
# Coarse absolute backstop: naive per-step must stay under ~45x the M4 baseline.
# On CI (a few x slower than M4) this is never approached; it only trips on a
# catastrophic absolute regression the ratio would miss.
readonly NAIVE_CEILING_NS="100000.0"   # 100 us/step

# --- reduced criterion settings so the CI run is short -----------------------
# Full-fidelity numbers come from the local BENCH.md measurement; the gate just
# needs enough samples to estimate the ratio and catch a gross regression.
SAMPLE_SIZE="${BENCH_GATE_SAMPLE_SIZE:-10}"
MEAS_TIME="${BENCH_GATE_MEAS_TIME:-5}"
WARMUP_TIME="${BENCH_GATE_WARMUP_TIME:-1}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# --- obtain the measured quantities ------------------------------------------
ratio_p50="${BENCH_GATE_RATIO_OVERRIDE:-}"
naive_ns="${BENCH_GATE_NAIVE_NS_OVERRIDE:-}"

if [[ -z "$ratio_p50" || -z "$naive_ns" ]]; then
    echo "bench-gate: running realistic_overhead (sample-size=$SAMPLE_SIZE, measurement-time=${MEAS_TIME}s) ..."
    out="$(cargo bench --features orderbook --bench realistic_overhead -- \
        --sample-size "$SAMPLE_SIZE" \
        --measurement-time "$MEAS_TIME" \
        --warm-up-time "$WARMUP_TIME" 2>&1)"
    echo "$out" | grep -E '^(RATIO_P50|RATIO_P99|NAIVE_PER_STEP_NS_P50|REALISTIC_PER_STEP_NS_P50)=' || true

    parsed_ratio="$(echo "$out" | grep -E '^RATIO_P50=' | tail -1 | cut -d= -f2)"
    parsed_naive="$(echo "$out" | grep -E '^NAIVE_PER_STEP_NS_P50=' | tail -1 | cut -d= -f2)"
    [[ -z "$ratio_p50" ]] && ratio_p50="$parsed_ratio"
    [[ -z "$naive_ns" ]] && naive_ns="$parsed_naive"
fi

if [[ -z "$ratio_p50" || -z "$naive_ns" ]]; then
    echo "bench-gate: FAILED to parse the bench output (no RATIO_P50 / NAIVE_PER_STEP_NS_P50 line)." >&2
    exit 1
fi

# --- gate --------------------------------------------------------------------
echo "bench-gate: measured overhead ratio p50 = ${ratio_p50}x  |  naive per-step p50 = ${naive_ns} ns"
echo "bench-gate: band = [${RATIO_LOW}x, ${RATIO_HIGH}x]  |  naive ceiling = ${NAIVE_CEILING_NS} ns"

status=0

awk -v v="$ratio_p50" -v lo="$RATIO_LOW" 'BEGIN { exit !(v+0 >= lo+0) }' || {
    echo "bench-gate: FAIL — overhead ratio ${ratio_p50}x < floor ${RATIO_LOW}x." >&2
    echo "            Naive mode regressed (slower), collapsing the ratio toward 1." >&2
    status=1
}
awk -v v="$ratio_p50" -v hi="$RATIO_HIGH" 'BEGIN { exit !(v+0 <= hi+0) }' || {
    echo "bench-gate: FAIL — overhead ratio ${ratio_p50}x > ceiling ${RATIO_HIGH}x." >&2
    echo "            Realistic mode regressed (slower) disproportionately to naive." >&2
    status=1
}
awk -v v="$naive_ns" -v cap="$NAIVE_CEILING_NS" 'BEGIN { exit !(v+0 <= cap+0) }' || {
    echo "bench-gate: FAIL — naive per-step ${naive_ns} ns > coarse ceiling ${NAIVE_CEILING_NS} ns." >&2
    echo "            Catastrophic absolute naive regression (or wrong hardware class)." >&2
    status=1
}

if [[ "$status" -eq 0 ]]; then
    echo "bench-gate: PASS — within the committed band."
fi
exit "$status"
