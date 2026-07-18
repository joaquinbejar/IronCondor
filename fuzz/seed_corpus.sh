#!/usr/bin/env bash
#
# Materialise the fuzz seed corpus (#52) into fuzz/corpus/<target>/ from the
# committed adversarial generators (tests/fixtures/adversarial/mod.rs).
#
# The seeds are DERIVED, never committed: the generators are the permanent,
# reviewable regressions (the repo "no committed binary blob" convention, see
# fuzz/README.md and docs/TESTING.md §12.1). This script runs the ignored
# `materialise_seed_corpus` test in the MAIN crate — on the repo's pinned stable
# toolchain (rust-toolchain.toml), not nightly — which writes the seeds. Run it
# before `cargo +nightly fuzz run <target>` (CI does this in the fuzz-smoke job).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

# The single ignored test writes fuzz/corpus/* (relative to CARGO_MANIFEST_DIR).
# `--ignored` runs it; `--nocapture` surfaces the per-target seed counts.
cargo test --test seed_corpus -- --ignored --nocapture
