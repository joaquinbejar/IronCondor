#!/usr/bin/env bash
#
# release_check.sh — the ONE runnable, fail-closed pre-release + v1.0 acceptance
# gate (issue #54, docs/RELEASE-PROCESS.md §1 + §13, docs/SEMVER.md
# §"v1.0 commitments").
#
# WHAT THIS IS
# -----------
# A single script the operator runs AT CUT TIME to prove, in one pass, that the
# mechanical pre-release checks (RELEASE-PROCESS §1) and the one-off v1.0
# acceptance gates (RELEASE-PROCESS §13 — the surface, determinism, performance,
# fuzz, and security gates shipped across #49–#53) are all green on the release
# commit. It prints a machine-readable PASS/FAIL/SKIP per check and a summary,
# and exits non-zero if ANY check FAILED.
#
# WHAT THIS IS NOT — the cut itself is user/time-gated and NOT automated here.
# ---------------------------------------------------------------------------
# This script NEVER bumps the version, locks the CHANGELOG, creates a tag,
# pushes, or publishes to crates.io / PyPI. Those are RELEASE-PROCESS §2–§9,
# owner-approved and deliberately manual (CLAUDE.md "never publish without
# approval"). The items it CANNOT verify — the one-quarter stability window, the
# publish approval, and the wheels-green-on-the-release-commit status — are
# printed as an explicit MANUAL CHECKLIST at the end. `cargo publish --dry-run`
# is packaging validation only; it uploads nothing.
#
# FAIL-CLOSED, WITH A NAMED SKIP CLASS
# ------------------------------------
# The default posture is FAIL-CLOSED: a check that needs a tool which is absent
# is a FAIL, so "all green" cannot be reached by a missing prerequisite. The
# ONLY exceptions are three checks explicitly in the SKIP-WITH-NOTICE class,
# because their tooling is an optional, out-of-band prerequisite (nightly /
# cargo-fuzz, maturin, gh + network), and CI proves them independently:
#
#   * maturin wheel-build sanity      — SKIP if `maturin` is absent.
#   * gh milestone open-issue check   — SKIP if `gh` is absent or errors (offline/auth).
#   * fuzz corpus replay              — SKIP if a nightly toolchain OR cargo-fuzz is absent.
#
# A SKIP is NEVER silent: it prints WHY, is counted separately, and the final
# verdict is `PASS_WITH_SKIPS` (not `PASS`) whenever any check skipped — so
# "all green" always means "everything that RAN, ran green, and nothing that
# MUST run was skipped". Every other check is FAIL-CLOSED: cargo (the pinned
# stable toolchain), cargo-audit, and cargo-deny absent = FAIL.
#
# SECTIONS (repeatable `--section NAME`; default = all)
# -----------------------------------------------------
#   mechanical   RELEASE-PROCESS §1 — fmt, clippy, test, build --release,
#                publish --dry-run, [Unreleased] non-empty, maturin*, gh milestone*.
#   surface      §13 — the SemVer surface-freeze gates (Rust re-exports + config pin).
#   determinism  §13 — the golden bundle suite + REGRESSION-EVIDENCE.md + PB-1 zero-alloc.
#   performance  §13 — the hot-path regression gates (bench_gate + linearity_gate
#                + pyo3_gate). SLOW: runs real criterion benches (several minutes).
#   fuzz         §13 — the parser fuzz corpus replay (-runs=0). SLOW: ASan build.*
#   security     §13 — the adversarial-input suite + cargo audit + cargo deny.
#   (* SKIP-WITH-NOTICE members live in mechanical / fuzz.)
#
# USAGE
#   scripts/release_check.sh                       # run every section
#   scripts/release_check.sh --section mechanical  # §1 pre-release only
#   scripts/release_check.sh --section mechanical --section surface --section security
#   scripts/release_check.sh --list                # print the check inventory, run nothing
#   scripts/release_check.sh --help
#
# Machine-readable: every check prints one `RESULT <STATUS> <section.id> <desc>`
# line (grep `^RESULT `); the run ends with `RC_PASS=.. RC_FAIL=.. RC_SKIP=..`
# and `RELEASE_CHECK_RESULT=PASS|PASS_WITH_SKIPS|FAIL`.
#
# Exit 0 = no FAILs (green, possibly with skips); exit 1 = at least one FAIL
# (or a bad invocation). No number, gate, or state is faked: a check either RAN
# and reported, or SKIPPED loudly.

set -euo pipefail

# ─── static configuration ────────────────────────────────────────────────────
readonly MILESTONE="v1.0"                                  # RELEASE-PROCESS §1 open-issue check
readonly WHEEL_FEATURES="python orderbook simulator"       # the shipped-wheel feature set (docs/06 §7)
readonly ALL_SECTIONS=(mechanical surface determinism performance fuzz security)

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# ─── result plumbing ─────────────────────────────────────────────────────────
pass=0
fail=0
skip=0
declare -a RESULT_LINES=()

# emit STATUS section.id description  — record + print one machine-readable line.
emit() {
    local status="$1" id="$2" desc="$3"
    RESULT_LINES+=("$(printf 'RESULT %-4s %-26s %s' "$status" "$id" "$desc")")
    printf 'RESULT %-4s %-26s %s\n' "$status" "$id" "$desc"
    case "$status" in
        PASS) pass=$((pass + 1)) ;;
        FAIL) fail=$((fail + 1)) ;;
        SKIP) skip=$((skip + 1)) ;;
    esac
}

hr() { printf '%s\n' "────────────────────────────────────────────────────────────────────────"; }

# banner "section" — announce a section.
banner() {
    echo
    hr
    printf '  SECTION: %s\n' "$1"
    hr
}

# ─── section selection ───────────────────────────────────────────────────────
declare -a SELECTED=()
LIST_ONLY=0

want() {
    # want NAME → true if NAME is selected (empty selection = all sections).
    local name="$1" s
    if [[ ${#SELECTED[@]} -eq 0 ]]; then return 0; fi
    for s in "${SELECTED[@]}"; do [[ "$s" == "$name" ]] && return 0; done
    return 1
}

valid_section() {
    local name="$1" s
    for s in "${ALL_SECTIONS[@]}"; do [[ "$s" == "$name" ]] && return 0; done
    return 1
}

# ─── generic runner: run a command, classify by its exit code (fail-closed) ───
# run_check section.id "description" cmd args...
run_check() {
    local id="$1" desc="$2"
    shift 2
    echo
    echo "── [$id] $desc"
    echo "   \$ $*"
    if "$@"; then
        emit PASS "$id" "$desc"
    else
        emit FAIL "$id" "$desc"
    fi
}

# ─── python interpreter detection (for the pyo3-building checks) ──────────────
# The `python` feature is compiled by `clippy --all-features`, `test
# --all-features`, and the H5 pyo3 gate; pyo3-build-config needs an interpreter
# at or above the abi3-py310 floor. Honour a pre-set PYO3_PYTHON, else pick the
# highest 3.1x on PATH. If none is found we do NOT skip — those checks stay
# FAIL-CLOSED and cargo reports the missing-interpreter error itself.
detect_python() {
    if [[ -n "${PYO3_PYTHON:-}" ]]; then
        printf '%s' "$PYO3_PYTHON"
        return 0
    fi
    local c
    for c in python3.13 python3.12 python3.11 python3.10; do
        if command -v "$c" >/dev/null 2>&1; then
            command -v "$c"
            return 0
        fi
    done
    return 1
}

# ─── section: mechanical (RELEASE-PROCESS §1) ────────────────────────────────
section_mechanical() {
    want mechanical || return 0
    banner "mechanical — RELEASE-PROCESS §1 pre-release checks"

    run_check mechanical.fmt "cargo fmt --all --check" \
        cargo fmt --all --check

    run_check mechanical.clippy "cargo clippy --all-targets --all-features -- -D warnings" \
        cargo clippy --all-targets --all-features -- -D warnings

    run_check mechanical.test "cargo test --all-features" \
        cargo test --all-features

    run_check mechanical.build_release "cargo build --release" \
        cargo build --release

    # `--allow-dirty`: this script validates PACKAGING (does it build, are deps
    # published, is the file list sane), not git hygiene — the working tree is
    # routinely dirty on a working branch. Git cleanliness at the tagged commit
    # is a separate operator concern, in the MANUAL CHECKLIST below. No upload.
    run_check mechanical.publish_dryrun "cargo publish --dry-run (packaging only, no upload)" \
        cargo publish --dry-run --allow-dirty

    # [Unreleased] non-empty — the §-abort rule (SEMVER §"v1.0 commitments").
    # DELEGATED to scripts/check-changelog.sh (Mode A, no args) so the rule lives
    # in ONE place — the same script the `changelog-check` CI job and
    # RELEASE-PROCESS §3 use. It exits 0 (non-empty) / 1 (empty or missing).
    run_check mechanical.changelog_unreleased "CHANGELOG [Unreleased] non-empty — the §-abort rule (delegates to check-changelog.sh)" \
        ./scripts/check-changelog.sh

    # maturin wheel-build sanity — SKIP-WITH-NOTICE if maturin is absent.
    echo
    echo "── [mechanical.maturin] maturin wheel-build sanity (shipped feature set)"
    if command -v maturin >/dev/null 2>&1; then
        local out
        out="$(mktemp -d)"
        if ( cd python && maturin build --release --features "$WHEEL_FEATURES" --out "$out" ); then
            emit PASS mechanical.maturin "maturin build --release --features \"$WHEEL_FEATURES\""
        else
            emit FAIL mechanical.maturin "maturin build --release --features \"$WHEEL_FEATURES\" failed"
        fi
        rm -rf "$out"
    else
        emit SKIP mechanical.maturin "maturin absent — SKIP (wheels are proven on the python-wheels CI job; install maturin>=1.7,<2.0 to run locally)"
    fi

    # gh milestone open-issue check — SKIP-WITH-NOTICE if gh is absent or errors.
    echo
    echo "── [mechanical.gh_milestone] gh issue list --milestone \"$MILESTONE\" --state open (must be empty)"
    if command -v gh >/dev/null 2>&1; then
        local ghout
        if ghout="$(gh issue list --milestone "$MILESTONE" --state open 2>&1)"; then
            if [[ -z "${ghout//[[:space:]]/}" ]]; then
                emit PASS mechanical.gh_milestone "milestone \"$MILESTONE\" has no open issues"
            else
                echo "$ghout"
                emit FAIL mechanical.gh_milestone "milestone \"$MILESTONE\" still has OPEN issues (see above) — the milestone must be closed"
            fi
        else
            echo "$ghout"
            emit SKIP mechanical.gh_milestone "gh could not query (offline / unauthenticated / milestone missing) — SKIP; verify the milestone is empty on GitHub before the cut"
        fi
    else
        emit SKIP mechanical.gh_milestone "gh absent — SKIP; verify \"$MILESTONE\" has zero open issues on GitHub before the cut"
    fi
}

# ─── section: surface (§13 — SemVer surface-freeze gates, #49) ────────────────
section_surface() {
    want surface || return 0
    banner "surface — SemVer surface-freeze gates (#49)"

    run_check surface.rust_api "cargo test --test surface (Rust re-export surface + env-var pin)" \
        cargo test --test surface

    run_check surface.config_pin "cargo test --lib test_config_serialized_field_set_is_pinned" \
        cargo test --lib test_config_serialized_field_set_is_pinned
}

# ─── section: determinism (§13 — golden suite + PB-1, #50/#36/#19) ────────────
section_determinism() {
    want determinism || return 0
    banner "determinism — golden bundle suite + PB-1 zero-alloc (#50, #36)"

    run_check determinism.golden "cargo test --test golden" \
        cargo test --test golden
    run_check determinism.golden_ob "cargo test --test golden --features orderbook" \
        cargo test --test golden --features orderbook
    run_check determinism.bundle_golden "cargo test --test bundle_golden" \
        cargo test --test bundle_golden
    run_check determinism.bundle_golden_ob "cargo test --test bundle_golden --features orderbook" \
        cargo test --test bundle_golden --features orderbook

    # The v1.0 "caught at least one real regression" evidence must exist.
    echo
    echo "── [determinism.regression_evidence] tests/golden/REGRESSION-EVIDENCE.md exists and is non-empty"
    if [[ -s tests/golden/REGRESSION-EVIDENCE.md ]]; then
        emit PASS determinism.regression_evidence "REGRESSION-EVIDENCE.md present (the caught-regression proof, #50)"
    else
        emit FAIL determinism.regression_evidence "tests/golden/REGRESSION-EVIDENCE.md missing/empty — the caught-regression proof is required (§13)"
    fi

    # PB-1 zero-alloc hard gate (reaffirmation; canonical gate = the zero-alloc CI job).
    run_check determinism.zero_alloc "cargo test --test zero_alloc (PB-1 zero steady-state allocation)" \
        cargo test --test zero_alloc
}

# ─── section: performance (§13 — hot-path regression gates, #51/#29) ──────────
section_performance() {
    want performance || return 0
    banner "performance — hot-path regression gates (#51). SLOW: real criterion benches"
    echo "  NOTE (honest runtime): each gate runs a real criterion bench with a"
    echo "  shortened measurement-time; end-to-end this section is typically SEVERAL"
    echo "  MINUTES (plus a cold compile). The gates are baseline-relative,"
    echo "  dimensionless ratios (BENCH.md), portable off the M4 baselines."

    run_check performance.bench_gate "scripts/bench_gate.sh (H1+H2 realistic/naive overhead ratio)" \
        ./scripts/bench_gate.sh

    run_check performance.linearity_gate "scripts/linearity_gate.sh (H3 conversion + H4 writer linearity)" \
        ./scripts/linearity_gate.sh

    # The H5 pyo3 gate needs PYO3_PYTHON; it was exported in main() when detected.
    run_check performance.pyo3_gate "scripts/pyo3_gate.sh (H5 PyO3 marshal ratio)" \
        ./scripts/pyo3_gate.sh
}

# ─── section: fuzz (§13 — parser fuzz corpus replay, #52) ─────────────────────
section_fuzz() {
    want fuzz || return 0
    banner "fuzz — parser corpus replay -runs=0 (#52). SLOW: ASan build on first run"

    # SKIP-WITH-NOTICE: needs a nightly toolchain AND cargo-fuzz. The repo pins
    # stable 1.97.0 (rust-toolchain.toml); CI installs nightly in-job only.
    local have_nightly=0 have_fuzz=0
    if cargo +nightly --version >/dev/null 2>&1; then have_nightly=1; fi
    if command -v cargo-fuzz >/dev/null 2>&1 || cargo fuzz --version >/dev/null 2>&1; then have_fuzz=1; fi

    if [[ "$have_nightly" -ne 1 || "$have_fuzz" -ne 1 ]]; then
        local why=""
        [[ "$have_nightly" -ne 1 ]] && why="nightly toolchain absent"
        [[ "$have_fuzz" -ne 1 ]] && why="${why:+$why; }cargo-fuzz absent"
        emit SKIP fuzz.corpus_replay "$why — SKIP (the fuzz-smoke CI job proves this; install nightly + cargo-fuzz 0.13.2 to run locally)"
        return 0
    fi

    echo
    echo "── materialising the seed corpus (fuzz/seed_corpus.sh, stable toolchain)"
    if ! bash fuzz/seed_corpus.sh; then
        emit FAIL fuzz.corpus_replay "fuzz/seed_corpus.sh failed to materialise the seed corpus"
        return 0
    fi

    local t rc=0
    for t in fuzz_csv_feed fuzz_parquet_feed fuzz_bundle_readback; do
        echo
        echo "── [fuzz.$t] replay corpus with zero mutation (-runs=0)"
        if cargo +nightly fuzz run "$t" -- -runs=0 -seed=1 -rss_limit_mb=2048 -timeout=10 -malloc_limit_mb=2048; then
            echo "   $t: clean"
        else
            echo "   $t: CRASH/hang/OOM on the committed corpus" >&2
            rc=1
        fi
    done
    if [[ "$rc" -eq 0 ]]; then
        emit PASS fuzz.corpus_replay "all three targets replayed the committed corpus clean (no panic/hang/OOM)"
    else
        emit FAIL fuzz.corpus_replay "a fuzz target crashed/hung/OOMed on the committed corpus (see above)"
    fi
}

# ─── section: security (§13 — adversarial suite + supply chain, #21/#20/#53) ──
section_security() {
    want security || return 0
    banner "security — adversarial-input suite + supply chain (#21, #53)"

    run_check security.suite "cargo test --test security (malformed input → typed error, no panic/hang/OOM)" \
        cargo test --test security

    # cargo audit / cargo deny are FAIL-CLOSED: a security gate that cannot run
    # is a FAIL, not a pass. (Contrast the SKIP-class maturin/gh/fuzz above.)
    run_check security.audit "cargo audit --deny warnings" \
        cargo audit --deny warnings

    run_check security.deny "cargo deny --all-features check" \
        cargo deny --all-features check
}

# ─── inventory (for --list / --help) ─────────────────────────────────────────
print_inventory() {
    cat <<'EOF'
Check inventory (id — class — description):

  mechanical  (RELEASE-PROCESS §1)
    mechanical.fmt                  FAIL-CLOSED   cargo fmt --all --check
    mechanical.clippy               FAIL-CLOSED   cargo clippy --all-targets --all-features -- -D warnings
    mechanical.test                 FAIL-CLOSED   cargo test --all-features
    mechanical.build_release        FAIL-CLOSED   cargo build --release
    mechanical.publish_dryrun       FAIL-CLOSED   cargo publish --dry-run (packaging only, no upload)
    mechanical.changelog_unreleased FAIL-CLOSED   CHANGELOG [Unreleased] non-empty via check-changelog.sh (§-abort rule)
    mechanical.maturin              SKIP-NOTICE   maturin wheel-build sanity (SKIP if maturin absent)
    mechanical.gh_milestone         SKIP-NOTICE   gh milestone open-issue check (SKIP if gh absent/offline)

  surface  (§13, #49)
    surface.rust_api                FAIL-CLOSED   cargo test --test surface
    surface.config_pin              FAIL-CLOSED   cargo test --lib test_config_serialized_field_set_is_pinned

  determinism  (§13, #50 / #36 / #19)
    determinism.golden              FAIL-CLOSED   cargo test --test golden
    determinism.golden_ob           FAIL-CLOSED   cargo test --test golden --features orderbook
    determinism.bundle_golden       FAIL-CLOSED   cargo test --test bundle_golden
    determinism.bundle_golden_ob    FAIL-CLOSED   cargo test --test bundle_golden --features orderbook
    determinism.regression_evidence FAIL-CLOSED   tests/golden/REGRESSION-EVIDENCE.md exists + non-empty
    determinism.zero_alloc          FAIL-CLOSED   cargo test --test zero_alloc (PB-1)

  performance  (§13, #51 — SLOW)
    performance.bench_gate          FAIL-CLOSED   scripts/bench_gate.sh (H1+H2)
    performance.linearity_gate      FAIL-CLOSED   scripts/linearity_gate.sh (H3+H4)
    performance.pyo3_gate           FAIL-CLOSED   scripts/pyo3_gate.sh (H5)

  fuzz  (§13, #52 — SLOW)
    fuzz.corpus_replay              SKIP-NOTICE   corpus replay -runs=0 (SKIP if nightly/cargo-fuzz absent)

  security  (§13, #21 / #53)
    security.suite                  FAIL-CLOSED   cargo test --test security
    security.audit                  FAIL-CLOSED   cargo audit --deny warnings
    security.deny                   FAIL-CLOSED   cargo deny --all-features check

FAIL-CLOSED = a needed tool absent ⇒ FAIL. SKIP-NOTICE = the three documented
optional-tooling checks; a SKIP is printed loudly and downgrades the verdict to
PASS_WITH_SKIPS. Nothing else may skip.
EOF
}

print_manual_checklist() {
    echo
    hr
    echo "  TIME-GATED / USER-GATED — this script CANNOT verify these; do them by hand"
    hr
    cat <<EOF
  [ ] One-quarter stability window ELAPSED for EACH frozen surface (result
      bundle, Rust API, Python API, config). The window starts at the v1.0 CUT
      DATE (docs/SEMVER.md §"v1.0 commitments"); it is a release-time value, not
      assertable today. Confirm the elapsed quarter per surface at cut time.
  [ ] Explicit PUBLISH APPROVAL obtained from the owner before ANY cargo publish
      or PyPI upload (CLAUDE.md; RELEASE-PROCESS §6/§7). No dry-run in this script
      uploads; the real publish is owner-gated.
  [ ] Wheels GREEN on the RELEASE COMMIT's CI (python-wheels job: Linux + macOS).
      This script's maturin check (if run) is a local sanity build, not the
      matrix CI proof.
  [ ] Working tree CLEAN at the tagged commit — this script runs publish
      --dry-run with --allow-dirty, so it does NOT gate git cleanliness.
  [ ] Both registry credentials / OIDC LIVE before pushing the tag (the §7.1
      preflight that shrinks the half-published window).

  MECHANICAL CUT STEPS — RELEASE-PROCESS §2–§9, run ONLY after all of the above:
  [ ] §2 bump [package].version → 1.0.0        [ ] §3 lock CHANGELOG ([1.0.0] - date + fresh [Unreleased])
  [ ] §4 commit + annotated tag                [ ] §5 push branch, THEN tag
  [ ] §6 cargo publish (crates.io, approved)   [ ] §7 PyPI wheels (approved, OIDC)
  [ ] §8 GitHub Release                        [ ] §9 post-release sanity (both registries)
  A half-published release is reconciled by the recover-forward playbook
  (RELEASE-PROCESS §7.1) / rolled forward (§11), NEVER by editing a published
  version or force-pushing a tag.
EOF
}

# ─── argument parsing ────────────────────────────────────────────────────────
usage() {
    sed -n '2,60p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --section)
            shift
            [[ $# -gt 0 ]] || { echo "release-check: --section needs a value" >&2; exit 1; }
            if ! valid_section "$1"; then
                echo "release-check: unknown section '$1' (valid: ${ALL_SECTIONS[*]})" >&2
                exit 1
            fi
            SELECTED+=("$1")
            ;;
        --section=*)
            val="${1#--section=}"
            if ! valid_section "$val"; then
                echo "release-check: unknown section '$val' (valid: ${ALL_SECTIONS[*]})" >&2
                exit 1
            fi
            SELECTED+=("$val")
            ;;
        --list)
            LIST_ONLY=1
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "release-check: unknown argument '$1' (try --help)" >&2
            exit 1
            ;;
    esac
    shift
done

if [[ "$LIST_ONLY" -eq 1 ]]; then
    print_inventory
    exit 0
fi

# ─── run ─────────────────────────────────────────────────────────────────────
echo "release-check: ironcondor v1.0 pre-release + acceptance gate (issue #54)"
echo "release-check: repo = $repo_root"
if [[ ${#SELECTED[@]} -gt 0 ]]; then
    echo "release-check: sections = ${SELECTED[*]}"
else
    echo "release-check: sections = ${ALL_SECTIONS[*]} (all)"
fi

# Export PYO3_PYTHON for the python-feature checks (clippy/test all-features, H5).
if py="$(detect_python)"; then
    export PYO3_PYTHON="$py"
    echo "release-check: PYO3_PYTHON = $PYO3_PYTHON"
else
    echo "release-check: WARNING — no python3.1x on PATH; the python-feature checks (clippy/test --all-features, pyo3 gate) may FAIL (fail-closed)." >&2
fi

section_mechanical
section_surface
section_determinism
section_performance
section_fuzz
section_security

# ─── summary ─────────────────────────────────────────────────────────────────
echo
hr
echo "  SUMMARY"
hr
printf '%s\n' "${RESULT_LINES[@]}"
echo
printf 'RC_PASS=%d RC_FAIL=%d RC_SKIP=%d\n' "$pass" "$fail" "$skip"

verdict="PASS"
if [[ "$fail" -gt 0 ]]; then
    verdict="FAIL"
elif [[ "$skip" -gt 0 ]]; then
    verdict="PASS_WITH_SKIPS"
fi
echo "RELEASE_CHECK_RESULT=$verdict"

case "$verdict" in
    PASS)
        echo "release-check: every selected check RAN and PASSED. Proceed to the MANUAL CHECKLIST below." ;;
    PASS_WITH_SKIPS)
        echo "release-check: no failures, but $skip check(s) SKIPPED (tool absent/offline). This is NOT a clean all-green:" >&2
        echo "release-check: install the missing tooling (see the SKIP lines) and re-run before the cut, or confirm each on CI." >&2 ;;
    FAIL)
        echo "release-check: $fail check(s) FAILED — the release is BLOCKED. Fix and re-run." >&2 ;;
esac

print_manual_checklist

[[ "$fail" -eq 0 ]] && exit 0 || exit 1
