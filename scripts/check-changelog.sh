#!/usr/bin/env bash
#
# check-changelog.sh — the CHANGELOG discipline gate (docs/SEMVER.md §"CHANGELOG
# discipline" / §"CI enforcement", docs/RELEASE-PROCESS.md §3). One script, two
# modes, both fail-closed:
#
#   MODE A — non-empty assertion (the §-abort rule)
#     $ scripts/check-changelog.sh
#     Asserts the working-tree CHANGELOG has a `## [Unreleased]` section that is
#     present AND non-empty — at least one real entry, not just the canonical
#     `### …` subheadings. This is the release-cut §-abort rule
#     (docs/SEMVER.md §"v1.0 commitments": "[Unreleased] is never empty at a
#     release tag — if it is, the release process aborts"). `release_check.sh`
#     delegates its `mechanical.changelog_unreleased` check here, so the rule
#     lives in ONE place.
#
#   MODE B — per-PR diff check (the `changelog-check` CI job)
#     $ scripts/check-changelog.sh <base> [head]        # head defaults to HEAD
#     Implements docs/SEMVER.md §"CI enforcement": (1) SKIP if $PR_TITLE starts
#     with chore:/refactor:/test:/docs:/ci:/bench: OR contains `[skip changelog]`;
#     (2) otherwise the diff of CHANGELOG.md between <base> and <head> must add at
#     least one line UNDER the `## [Unreleased]` section of the head file;
#     (3) fail closed. `head` == `HEAD` (the default) diffs against the WORKING
#     TREE so an as-yet-uncommitted rename (RELEASE-PROCESS §3) is seen; any other
#     `head` is a committed ref. Matches the doc's `check-changelog.sh main HEAD`.
#
# OUTPUT — a single machine-readable line, consistent with release_check.sh:
#     check-changelog: PASS — <why>
#     check-changelog: SKIP — <why>          (Mode B title-skip only)
#     check-changelog: FAIL — <why>
# Exit 0 = PASS or SKIP; exit 1 = FAIL (or a bad invocation).
#
# No toolchain, no network, no new dependency — awk + git only.

set -euo pipefail

readonly CHANGELOG="CHANGELOG.md"
# The internal-only prefixes that exempt a PR from a CHANGELOG entry
# (docs/SEMVER.md §"CHANGELOG discipline").
readonly SKIP_PREFIX_RE='^(chore|refactor|test|docs|ci|bench):'
readonly SKIP_TOKEN='[skip changelog]'

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

usage() {
    sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

pass() { echo "check-changelog: PASS — $1"; exit 0; }
skip() { echo "check-changelog: SKIP — $1"; exit 0; }
fail() { echo "check-changelog: FAIL — $1" >&2; exit 1; }

# ─── shared readers (operate on stdin) ───────────────────────────────────────

# unreleased_status  — read a CHANGELOG on stdin; exit 0 = present & non-empty,
# 1 = present but empty, 2 = section missing. A line counts as content unless it
# is a `### …` subheading or blank.
unreleased_status() {
    awk '
        /^## \[Unreleased\]/ { seen = 1; inblock = 1; next }
        inblock && /^## \[/  { inblock = 0 }
        inblock && /^###/    { next }
        inblock && NF        { content = 1 }
        END {
            if (!seen)   { exit 2 }
            exit(content ? 0 : 1)
        }
    '
}

# unreleased_range  — read a CHANGELOG on stdin; print "START END" (1-based,
# inclusive) of the `## [Unreleased]` section (header line through the line
# before the next `## [` header, or EOF). Exit 1 if the section is absent.
unreleased_range() {
    awk '
        /^## \[Unreleased\]/ { start = NR; inblock = 1; next }
        inblock && /^## \[/  { end = NR - 1; inblock = 0 }
        END {
            if (!start) { exit 1 }
            if (inblock) { end = NR }
            print start, end
        }
    '
}

# ─── MODE A — non-empty assertion (§-abort rule) ─────────────────────────────
mode_a_nonempty() {
    [[ -f "$CHANGELOG" ]] || fail "$CHANGELOG not found (run from the repo root)"
    local rc=0
    unreleased_status <"$CHANGELOG" || rc=$?
    case "$rc" in
        0) pass "CHANGELOG [Unreleased] is present and non-empty (the §-abort rule)" ;;
        1) fail "CHANGELOG [Unreleased] is EMPTY — only subheadings, no entry; the release process aborts (SEMVER §\"v1.0 commitments\")" ;;
        2) fail "CHANGELOG has no '## [Unreleased]' section (SEMVER §\"CHANGELOG discipline\")" ;;
        *) fail "could not read the CHANGELOG [Unreleased] section (awk exit $rc)" ;;
    esac
}

# ─── MODE B — per-PR diff check (changelog-check CI job) ──────────────────────
mode_b_diff() {
    local base="$1" head="${2:-HEAD}"

    # (1) title-based skip — an internal-only PR needs no CHANGELOG entry.
    local title="${PR_TITLE:-}"
    if [[ -n "$title" ]]; then
        if [[ "$title" =~ $SKIP_PREFIX_RE ]]; then
            skip "PR title '$title' is an internal-only change (chore:/refactor:/test:/docs:/ci:/bench:) — CHANGELOG entry not required"
        fi
        if [[ "$title" == *"$SKIP_TOKEN"* ]]; then
            skip "PR title carries the '$SKIP_TOKEN' override"
        fi
    fi

    # Resolve the head CHANGELOG content + the diff. head==HEAD ⇒ working tree,
    # so an uncommitted RELEASE-PROCESS §3 rename is visible; else a committed ref.
    local head_content diff_out range
    if [[ "$head" == "HEAD" ]]; then
        [[ -f "$CHANGELOG" ]] || fail "$CHANGELOG not found (run from the repo root)"
        head_content="$(cat "$CHANGELOG")"
        diff_out="$(git diff "$base" -- "$CHANGELOG" 2>/dev/null)" \
            || fail "git diff against base '$base' failed — is '$base' a fetched ref? (CI needs fetch-depth: 0)"
    else
        head_content="$(git show "$head:$CHANGELOG" 2>/dev/null)" \
            || fail "cannot read $CHANGELOG at head ref '$head'"
        diff_out="$(git diff "$base" "$head" -- "$CHANGELOG" 2>/dev/null)" \
            || fail "git diff '$base' '$head' failed — are both refs fetched?"
    fi

    range="$(printf '%s\n' "$head_content" | unreleased_range)" \
        || fail "the head $CHANGELOG has no '## [Unreleased]' section"
    # Split "START END" with parameter expansion (portable; avoids a here-string
    # `read`, which wedges on bash 3.2's default /bin/bash).
    local u_start="${range%% *}" u_end="${range##* }"

    # No diff at all ⇒ this PR did not touch the CHANGELOG ⇒ needs an entry.
    # `git diff` prints an empty string when there are no changes, so a plain
    # emptiness test suffices — and a whitespace-stripping `${x//…}` over a large
    # multi-line diff is pathologically slow on bash 3.2's /bin/bash.
    if [[ -z "$diff_out" ]]; then
        fail "this PR adds no CHANGELOG entry (no change to $CHANGELOG between '$base' and '$head'); add an [Unreleased] entry, or mark the PR chore:/refactor:/…/[skip changelog]"
    fi

    # (2) the diff must ADD ≥1 line whose NEW-file line number lands inside the
    # head [Unreleased] range. We track each hunk's +new line counter and test
    # every added ('+', not '+++') line for membership in [u_start, u_end].
    if printf '%s\n' "$diff_out" | awk -v s="$u_start" -v e="$u_end" '
        /^@@/ {
            if (match($0, /\+[0-9]+/)) { nl = substr($0, RSTART + 1, RLENGTH - 1) + 0 }
            inhunk = 1
            next
        }
        !inhunk    { next }
        /^\+\+\+/  { next }
        /^\+/      { if (nl >= s && nl <= e) { found = 1 }; nl++; next }
        /^-/       { next }
        /^\\/      { next }   # "\ No newline at end of file"
        { nl++ }              # context line (leading space)
        END { exit(found ? 0 : 1) }
    '; then
        pass "the diff adds at least one line under [Unreleased] (base '$base' → head '$head')"
    else
        fail "the diff touches $CHANGELOG but adds no line under [Unreleased] — a user-visible PR must add an [Unreleased] entry (SEMVER §\"CI enforcement\"); or mark it chore:/refactor:/…/[skip changelog]"
    fi
}

# ─── dispatch ────────────────────────────────────────────────────────────────
case "${1:-}" in
    -h | --help)
        usage
        exit 0
        ;;
    "")
        mode_a_nonempty
        ;;
    *)
        mode_b_diff "$@"
        ;;
esac
