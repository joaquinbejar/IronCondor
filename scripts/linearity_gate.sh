#!/usr/bin/env bash
#
# linearity_gate.sh — the conversion (H3/PB-4) and bundle-writer (H4/PB-5)
# LINEARITY regression gates (issue #51, docs/07 §6, docs/TESTING.md §11.1).
#
# WHAT IT GATES, AND WHY IT IS HARDWARE-PORTABLE
# ----------------------------------------------
# The committed BENCH.md baselines are Apple M4 Max, but CI runs on GitHub Linux
# runners. An absolute-nanoseconds comparison against the M4 numbers would
# false-positive on every CI run, so — exactly like `scripts/bench_gate.sh` — the
# gated quantity is DIMENSIONLESS: a within-run **cost ratio** that a uniform
# clock-speed factor cancels.
#
#   * H3 conversion (`benches/conversion.rs`): the per-CONTRACT cost of
#     `raw_quotes_to_snapshot` at the largest snapshot divided by the per-contract
#     cost at the smallest. Both are measured in the same process over the same
#     16x contract sweep, so the runner's clock speed `f` cancels:
#
#         cost_ratio = (f · cost_per_contract @ large) / (f · cost_per_contract @ small)
#
#     For an O(contracts) single pass the per-contract cost is ~flat, so the
#     ratio hovers near 1 (a mild BTreeMap log(n) factor aside). An
#     O(contracts^2) regression makes the per-contract cost rise ~with the
#     contract growth (16x here), so the ratio blows up toward 16x — caught.
#
#   * H4 writer (`benches/bundle_write.rs`): the per-ROW cost of `write_bundle`
#     at the largest run divided by the per-row cost at the smallest, over a 128x
#     row sweep straddling the 8192-row Parquet batch. Same portability argument.
#     An O(rows) single-pass encode keeps the per-row cost ~flat (it FALLS as the
#     fixed per-write overhead amortises), so the ratio is < 1; an O(rows^2)
#     writer's per-row cost rises ~with the row growth (128x) — caught.
#
# WHAT THESE GATES DO AND DON'T CATCH (honest scope)
# --------------------------------------------------
# The ceilings below are quadratic-class SHAPE bounds, not performance bounds:
# they catch a SUPER-LINEAR (rising-per-unit-cost) regression, not a magnitude
# one. A uniform constant-factor slowdown that PRESERVES the per-unit cost RATIO
# passes silently here — that is by design (it is what keeps the gate
# hardware-portable), and it is the H1/H2 coarse absolute backstop
# (scripts/bench_gate.sh), NOT these linearity gates, that guards the
# uniform-slowdown case. The ceilings are baseline-relative (anchored to the O(n)
# scaling SHAPE in BENCH.md PB-4 / PB-5) and portable (the gated quantity is a
# within-run dimensionless ratio). This is the mechanism docs/07 §6 /
# docs/TESTING.md §11.1 mandate for a linearity assertion ("grow linearly, not
# quadratically").
#
#   * Conversion ceiling 4.0x — healthy ratio ≈ 1.13x over 16x contracts (near 1,
#     the O(contracts) regime; BENCH.md §H3). A quadratic core shows ≈ 16x; an
#     n·log n worst case ≈ 1.9x. 4.0x sits above the linear regime and far below
#     the quadratic one, so CI variance cannot trip it but a super-linear
#     regression does.
#   * Writer ceiling 2.0x — HEALTHY ratio ≈ 0.14–0.25x over 128x rows: the per-row
#     cost FALLS as fixed overhead amortises (BENCH.md §H4). Because that healthy
#     baseline sits so far below 1.0, the 2.0x ceiling reads TIGHTER than it
#     enforces: it trips only at roughly O(rows^1.5) and worse — a quadratic-class
#     regression (a true O(rows^2) writer shows ≈ 128x) — NOT at an n·log n drift.
#     Documented honestly rather than overstated.
#
# H3 gates the raw_quotes_to_snapshot validation core only (the per-snapshot
# materialisation cost); snapshot_to_option_chain (the deferred-reprice sibling,
# off the per-step loop) is the same complexity but is not separately gated.
#
# USAGE
#   scripts/linearity_gate.sh                          # run both benches, gate both ratios
#   LINEARITY_GATE_CONVERT_OVERRIDE=20 \
#     scripts/linearity_gate.sh                        # self-test: force a value, prove it fails
#   LINEARITY_GATE_WRITER_OVERRIDE=9 \
#     scripts/linearity_gate.sh                        # self-test the writer ceiling
#
# Exit 0 = both ratios within their linear band (gate passes); exit 1 = a
# super-linear regression (gate fails the build).

set -euo pipefail

# --- committed linearity ceilings (BENCH.md §H3 / §H4) -----------------------
readonly CONVERT_CEILING="4.0"   # per-contract cost ratio over the 16x contract sweep
readonly WRITER_CEILING="2.0"    # per-row cost ratio over the 128x row sweep

# --- shortened criterion measurement so the CI run is short ------------------
# NOTE: the benches fix their OWN sample count programmatically
# (`group.sample_size`: conversion 50, bundle_write 30), which criterion 0.5
# honours OVER any `--sample-size` CLI flag — so passing one here would be DEAD
# (verified: a prebuilt bench run with `--sample-size 20` still collected 50).
# The stage-time reduction therefore comes from the shortened `--measurement-time`
# below; the sample count (and thus the shape estimate) is the bench's committed
# setting, and full-fidelity numbers are the local BENCH.md measurement.
CONVERT_MEAS_TIME="${LINEARITY_GATE_CONVERT_MEAS:-3}"
WRITER_MEAS_TIME="${LINEARITY_GATE_WRITER_MEAS:-3}"
WARMUP_TIME="${LINEARITY_GATE_WARMUP:-1}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# --- obtain the measured ratios ----------------------------------------------
convert_ratio="${LINEARITY_GATE_CONVERT_OVERRIDE:-}"
writer_ratio="${LINEARITY_GATE_WRITER_OVERRIDE:-}"

if [[ -z "$convert_ratio" ]]; then
    echo "linearity-gate: running conversion (H3/PB-4, measurement-time=${CONVERT_MEAS_TIME}s; sample count fixed by the bench) ..."
    convert_out="$(cargo bench --bench conversion -- \
        --measurement-time "$CONVERT_MEAS_TIME" \
        --warm-up-time "$WARMUP_TIME" 2>&1)"
    echo "$convert_out" | grep -E '^(PB4_|verdict:)' || true
    convert_ratio="$(echo "$convert_out" | grep -E '^PB4_CONVERT_COST_RATIO=' | tail -1 | cut -d= -f2)"
fi

if [[ -z "$writer_ratio" ]]; then
    echo "linearity-gate: running bundle_write (H4/PB-5, measurement-time=${WRITER_MEAS_TIME}s; sample count fixed by the bench) ..."
    writer_out="$(cargo bench --bench bundle_write -- \
        --measurement-time "$WRITER_MEAS_TIME" \
        --warm-up-time "$WARMUP_TIME" 2>&1)"
    echo "$writer_out" | grep -E '^(PB5_|verdict:)' || true
    writer_ratio="$(echo "$writer_out" | grep -E '^PB5_PER_ROW_COST_RATIO=' | tail -1 | cut -d= -f2)"
fi

if [[ -z "$convert_ratio" || -z "$writer_ratio" ]]; then
    echo "linearity-gate: FAILED to parse a bench ratio (no PB4_CONVERT_COST_RATIO / PB5_PER_ROW_COST_RATIO line)." >&2
    exit 1
fi

# --- reject a non-finite / non-positive ratio BEFORE the awk compare ---------
# mawk (the default awk on Debian/Ubuntu CI) parses "inf" / "NaN" / a stray
# non-number as 0, so a one-sided `<= ceiling` compare would PASS a garbage value
# — a fail-OPEN. The benches print `inf` when a size's cost is 0 (a broken
# measurement), so this is a real path. Require a positive finite decimal first.
require_positive_finite() {
    local name="$1" val="$2"
    local re='^[0-9]+(\.[0-9]+)?$'
    if [[ ! "$val" =~ $re ]]; then
        echo "linearity-gate: FAIL — $name parsed as non-numeric/non-finite '$val' (guarding against a fail-open compare)." >&2
        exit 1
    fi
    awk -v v="$val" 'BEGIN { exit !(v+0 > 0) }' || {
        echo "linearity-gate: FAIL — $name parsed as non-positive '$val'." >&2
        exit 1
    }
}

require_positive_finite "conversion per-contract cost ratio" "$convert_ratio"
require_positive_finite "writer per-row cost ratio" "$writer_ratio"

# --- gate --------------------------------------------------------------------
echo "linearity-gate: conversion per-contract cost ratio = ${convert_ratio}x  (ceiling ${CONVERT_CEILING}x)"
echo "linearity-gate: writer per-row cost ratio       = ${writer_ratio}x  (ceiling ${WRITER_CEILING}x)"

status=0

awk -v v="$convert_ratio" -v cap="$CONVERT_CEILING" 'BEGIN { exit !(v+0 <= cap+0) }' || {
    echo "linearity-gate: FAIL — conversion per-contract cost ratio ${convert_ratio}x > ceiling ${CONVERT_CEILING}x." >&2
    echo "            H3 conversion regressed super-linearly (per-contract cost rising with contract count)." >&2
    status=1
}
awk -v v="$writer_ratio" -v cap="$WRITER_CEILING" 'BEGIN { exit !(v+0 <= cap+0) }' || {
    echo "linearity-gate: FAIL — writer per-row cost ratio ${writer_ratio}x > ceiling ${WRITER_CEILING}x." >&2
    echo "            H4 bundle writer regressed super-linearly (per-row cost rising with row count)." >&2
    status=1
}

if [[ "$status" -eq 0 ]]; then
    echo "linearity-gate: PASS — both hot paths scale linearly (within the committed shape bands)."
fi
exit "$status"
