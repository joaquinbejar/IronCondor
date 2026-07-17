#!/usr/bin/env bash
#
# pyo3_gate.sh — the PyO3 boundary (H5/PB-6) marshalling regression gate
# (issue #51, docs/07 §6, docs/TESTING.md §11.1).
#
# WHAT IT GATES, AND WHY IT IS HARDWARE-PORTABLE
# ----------------------------------------------
# The committed BENCH.md §H5 baseline is Apple M4 Max, but CI runs on Linux
# runners, so — exactly like `scripts/bench_gate.sh` — the gated quantity is
# DIMENSIONLESS: the ratio of the full-config marshal-in cost to the trivial
# single-crossing floor, both measured by `benches/pyo3_marshal.rs` in the SAME
# embedded interpreter, SAME process:
#
#     ratio = (f · marshal_full_config) / (f · marshal_single_call)   [f cancels]
#
# `marshal_full_config` is the seven Python->Rust crossings a caller pays to build
# a BacktestConfig (dominated by the one heavy `strategy_iron_condor` crossing:
# 17-arg extract + Underlying/Quantity/three Decimal conversions + catch_unwind).
# `marshal_single_call` is one `.seed(u64)` crossing — the fixed per-boundary
# floor. Their ratio only moves when the heavy marshal regresses RELATIVE to the
# floor — a real boundary regression — not when the runner is merely slow.
#
# WHAT IT CATCHES
# ---------------
#   * A regression in the config/strategy marshal path (extra allocations, a
#     heavier conversion, a widened argument extract) raises the ratio toward the
#     RATIO_HIGH ceiling.
#   * A regression that somehow inflates the trivial crossing floor lowers the
#     ratio toward the RATIO_LOW floor.
# The blind spot is identical to bench_gate.sh's: a uniform slowdown of BOTH the
# full and single crossing (e.g. an interpreter-wide trampoline regression) leaves
# the ratio unchanged. That is acceptable — PB-6 already establishes the boundary
# is ≈ 0.04 % of a run, so a uniform boundary slowdown is not a hot-path concern;
# the ratio guards the SHAPE of the marshal path, which is what H5 owns.
#
# WHY NOT AN ABSOLUTE NANOSECOND GATE
# -----------------------------------
# An absolute `marshal_full_config` ns ceiling would false-positive on slower CI
# hardware, so it is deliberately avoided (the no-absolute-number rule, AC #4).
# The band is a factor of two each way around the committed ≈ 16x baseline (BENCH.md
# §H5: 1250 ns / 78 ns ≈ 16.03x), wide enough that reduced-sample CI variance
# cannot trip it, tight enough to catch a ~2x disproportionate marshal regression.
#
# USAGE
#   PYO3_PYTHON=python3.12 scripts/pyo3_gate.sh          # run the bench, gate the ratio
#   PYO3_GATE_RATIO_OVERRIDE=99 \
#     PYO3_PYTHON=python3.12 scripts/pyo3_gate.sh         # self-test: force a value, prove it fails
#
# Exit 0 = within band (gate passes); exit 1 = regression (gate fails the build).

set -euo pipefail

# --- committed baseline + band (BENCH.md §H5, Apple M4 Max) ------------------
# Measured full/single marshal ratio p50 ≈ 16.03x (full 1250 ns, single 78 ns).
# The band is a factor of two each way — noise-safe with reduced CI samples, and
# trips on a ~2x disproportionate regression in the config/strategy marshal path.
readonly RATIO_LOW="8.0"     # marshal-path speedup floor / floor-inflation guard
readonly RATIO_HIGH="32.0"   # config/strategy marshal-blowup ceiling

# --- reduced criterion settings so the CI run is short -----------------------
SAMPLE_SIZE="${PYO3_GATE_SAMPLE_SIZE:-20}"
MEAS_TIME="${PYO3_GATE_MEAS_TIME:-3}"
WARMUP_TIME="${PYO3_GATE_WARMUP_TIME:-1}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# --- obtain the measured ratio -----------------------------------------------
ratio_p50="${PYO3_GATE_RATIO_OVERRIDE:-}"

if [[ -z "$ratio_p50" ]]; then
    : "${PYO3_PYTHON:?pyo3-gate needs PYO3_PYTHON set to a >= 3.10 interpreter (e.g. python3.12)}"
    echo "pyo3-gate: running pyo3_marshal (sample-size=$SAMPLE_SIZE, measurement-time=${MEAS_TIME}s) ..."
    out="$(cargo bench --features python --bench pyo3_marshal -- \
        --sample-size "$SAMPLE_SIZE" \
        --measurement-time "$MEAS_TIME" \
        --warm-up-time "$WARMUP_TIME" 2>&1)"
    echo "$out" | grep -E '^(MARSHAL_)' || true
    ratio_p50="$(echo "$out" | grep -E '^MARSHAL_RATIO_P50=' | tail -1 | cut -d= -f2)"
fi

if [[ -z "$ratio_p50" ]]; then
    echo "pyo3-gate: FAILED to parse the bench output (no MARSHAL_RATIO_P50 line)." >&2
    exit 1
fi

# --- reject a non-finite / non-positive ratio BEFORE the awk compare ---------
# mawk (the default awk on Debian/Ubuntu CI) parses "inf" / "NaN" / a stray
# non-number as 0. The band here is two-sided so an `inf` would trip the floor
# (fail closed), but a guard is still required for defence in depth (the bench
# prints `inf` when the single-crossing floor measures 0) and to keep the failure
# message honest. Require a positive finite decimal.
require_positive_finite() {
    local name="$1" val="$2"
    local re='^[0-9]+(\.[0-9]+)?$'
    if [[ ! "$val" =~ $re ]]; then
        echo "pyo3-gate: FAIL — $name parsed as non-numeric/non-finite '$val' (guarding against a fail-open compare)." >&2
        exit 1
    fi
    awk -v v="$val" 'BEGIN { exit !(v+0 > 0) }' || {
        echo "pyo3-gate: FAIL — $name parsed as non-positive '$val'." >&2
        exit 1
    }
}

require_positive_finite "full/single marshal ratio" "$ratio_p50"

# --- gate --------------------------------------------------------------------
echo "pyo3-gate: measured full/single marshal ratio p50 = ${ratio_p50}x  |  band = [${RATIO_LOW}x, ${RATIO_HIGH}x]"

status=0

awk -v v="$ratio_p50" -v lo="$RATIO_LOW" 'BEGIN { exit !(v+0 >= lo+0) }' || {
    echo "pyo3-gate: FAIL — full/single marshal ratio ${ratio_p50}x < floor ${RATIO_LOW}x." >&2
    echo "            The single-crossing floor inflated (or the full marshal collapsed) disproportionately." >&2
    status=1
}
awk -v v="$ratio_p50" -v hi="$RATIO_HIGH" 'BEGIN { exit !(v+0 <= hi+0) }' || {
    echo "pyo3-gate: FAIL — full/single marshal ratio ${ratio_p50}x > ceiling ${RATIO_HIGH}x." >&2
    echo "            The config/strategy marshal path regressed disproportionately to the crossing floor." >&2
    status=1
}

if [[ "$status" -eq 0 ]]; then
    echo "pyo3-gate: PASS — within the committed band."
fi
exit "$status"
