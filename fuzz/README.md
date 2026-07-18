# IronCondor parser fuzz targets (#52)

Fuzz targets for the crate's three untrusted-byte parser surfaces, proving the
v1.0 security invariant: **for any input bytes, the only outcomes are a typed
`BacktestError` or a valid parse — never a panic, an unbounded loop, or an OOM**
([docs/07 §13](../docs/07-performance-and-security.md#13-fuzzing-before-v10),
[docs/TESTING.md §12.2](../docs/TESTING.md#122-fuzz-targets-before-v10)).

| Target | Real parser driven | Input mapping |
|--------|--------------------|---------------|
| `fuzz_csv_feed` | `CsvFeed::open` + full drain | bytes → one `step_000.csv` in a fresh tempdir |
| `fuzz_parquet_feed` | `ParquetFeed::open` + full drain | bytes → one `chain.parquet` in a fresh tempdir |
| `fuzz_bundle_readback` | `read_bundle` | bytes → 5 bundle files via a length-prefixed frame |

Each target drives the **real public parser** — no reimplementation, no internal
shortcut. It writes the fuzzer bytes where the path-based parser expects them and
calls the public entry point. The "only `Ok` or `Err(BacktestError)`" half of the
invariant is enforced **by the return type** (`open` / `next` / `read_bundle` all
return `Result<_, BacktestError>`, so no third outcome is representable); the
fuzzer proves the **no-panic / no-hang / no-OOM** half, backed by a tight
`ResourceLimits` in every target plus the CI `-rss_limit_mb` / `-timeout` /
`-malloc_limit_mb` ceilings.

## Tight `ResourceLimits`

`ironcondor_fuzz::tight_limits()` (one source of truth, `src/lib.rs`) sets every
ceiling to MiB-/thousands-scale instead of the GiB / hundred-million defaults, so
a decompression bomb or a huge declared count is cut off fast and the fuzzer
explores the **validation** logic rather than huge allocations. Each bound stays
generous enough that the well-formed seeds still parse `Ok`.

## Ok-path reachability (confirmed)

A fuzz target that only ever errors is weak. Each target's seed corpus includes a
**well-formed** seed (`well_formed` for CSV/Parquet, `well_formed` for the bundle)
that parses `Ok` under `tight_limits()`:

- CSV / Parquet — the well-formed seed is a self-contained valid file → `Ok`.
- Bundle — the well-formed seed decodes back to a valid `ironcondor.bundle.v1`;
  its referenced input is simply skipped as unreachable at read-back, so
  `read_bundle` returns `Ok(ValidatedBundle)`.

Because libFuzzer replays every corpus seed at startup, a smoke run with the
corpus present executes the `Ok` path on every invocation, and the coverage
counters reflect it. This is how `Ok`-path reachability is confirmed locally and
in CI.

## Bundle framing (why hand-rolled, not `#[derive(Arbitrary)]`)

`read_bundle` needs a directory (`manifest.json` + four Parquet tables), so the
bundle target splits the fuzzer bytes into five files. It uses a hand-rolled
length-prefixed frame — five `[u32 LE length][length bytes]` sections in
`BUNDLE_FILES` order (`ironcondor_fuzz::split_bundle_sections`) — rather than an
`#[derive(Arbitrary)]` split, for one reason: a **well-formed seed must round-trip
deterministically**. An `Arbitrary`-derived split has an implementation-defined
byte encoding (lengths consumed from the end of the buffer, version-dependent),
which makes a hand-authored `Ok`-path seed fragile. The hand-rolled frame has an
exact inverse encoder (`frame_bundle` in `../tests/seed_corpus.rs`), so the
well-formed bundle seed decodes back to exactly its five files. The decoder is
**total** (a short/oversized length is clamped to the remaining bytes, never
panics, never writes more than `data.len()`), so mutated inputs keep reaching the
reader instead of bouncing off a strict frame.

## Seed corpus: derived, never committed

Consistent with the established repo convention — the adversarial fixtures are
**source-form deterministic generators, not committed binary blobs**
([tests/fixtures/adversarial/mod.rs](../tests/fixtures/adversarial/mod.rs) module
docs, [docs/TESTING.md §12.1](../docs/TESTING.md#121-adversarial-input-fixtures)) —
the seed corpus is **materialised from those generators at fuzz-time**, not
committed. `corpus/` is gitignored. The permanent, reviewable regressions are the
generators; the materialised bytes are disposable.

`fuzz/seed_corpus.sh` runs the ignored `materialise_seed_corpus` test
([tests/seed_corpus.rs](../tests/seed_corpus.rs)) on the repo's pinned **stable**
toolchain — the `fuzz/` crate is a separate workspace and cannot import the test
crate, so this ignored test is the bridge — writing one seed per generator into
`corpus/<target>/`. Run it before `cargo +nightly fuzz run`.

## Running locally

```bash
# One-time: materialise the seed corpus from the committed generators.
bash fuzz/seed_corpus.sh

# The exact CI gate — a deterministic corpus REPLAY (no exploration).
cargo +nightly fuzz run fuzz_parquet_feed -- -runs=0 -seed=1 -rss_limit_mb=2048 -timeout=10 -malloc_limit_mb=2048

# A local EXPLORING campaign to discover NEW panics (unpinned, longer).
cargo +nightly fuzz run fuzz_csv_feed        -- -runs=5000 -max_total_time=120 -rss_limit_mb=2048 -timeout=10 -malloc_limit_mb=2048
cargo +nightly fuzz run fuzz_parquet_feed    -- -runs=5000 -max_total_time=120 -rss_limit_mb=2048 -timeout=10 -malloc_limit_mb=2048
cargo +nightly fuzz run fuzz_bundle_readback -- -runs=5000 -max_total_time=120 -rss_limit_mb=2048 -timeout=10 -malloc_limit_mb=2048
```

CI replays the corpus (`-runs=0`) over all three targets in the `fuzz-smoke` job
(`.github/workflows/ci.yml`), with the nightly toolchain installed in-job only
(the repo `rust-toolchain.toml` stays pinned to stable).

## Throughput note (honest)

The parsers are **path-based**, so each iteration writes the input into a
per-iteration tempdir before parsing. That filesystem round-trip caps throughput
(single-digit-thousands exec/s, not the millions a pure in-memory target reaches).
That is an accepted trade for driving the *real* parser unmodified; it is a smoke,
not a saturation campaign.

## Parser panics found + the arrow/parquet backstop

This harness found two real panics on crafted Parquet input, both reachable via
`ParquetFeed::open` and `read_bundle`:

1. **arrow-ipc embedded-schema** — a malformed `ARROW:schema` IPC flatbuffer in
   the Parquet footer panicked `arrow-ipc` (`unimplemented!("Type NONE")`).
   Fixed by `ArrowReaderOptions::with_skip_arrow_metadata(true)` at both read
   sites (the schema is derived from the Parquet schema, which we already
   validate column-for-column).
2. **parquet metadata `byte_range` assert** — a negative column-chunk offset
   trips `assert!(col_start >= 0 && col_len >= 0)` during row-group decode.

The second is one of a **class** of Parquet metadata panics arrow-rs is still
hardening upstream (arrow-rs #9840, #9868, #5382), several still present in
parquet **59.1.0 — the latest published**, so no dep bump can close them yet.
It is closed by **two complementary layers**:

- **A pre-decode prevention guard.** At both read sites, the row-group metadata
  loop rejects a negative column-chunk `compressed_size` / `data_page_offset` /
  `dictionary_page_offset` before the decode loop — exactly what `byte_range()`
  asserts — so the panic **never fires**. This is what keeps the fuzz targets
  green: **cargo-fuzz builds `panic=abort`**, where a caught panic still aborts
  the process, so a fuzz target only stays green if the panic is *prevented*,
  not merely caught.
- **A narrowly-scoped `catch_unwind` backstop.** It wraps **only** the upstream
  builder-construction and row-group-decode calls (our own validation stays
  outside the wrap), logs a contained panic (`tracing::warn!`), and maps it to a
  typed `BacktestError` (`Data` / `Bundle`). In the production **unwind** build
  this contains the *residual, still-unhardened* members of the class — the ones
  no prevention guard exists for yet.

### The smoke is a deterministic green-on-known gate

The CI `fuzz-smoke` runs each target at **`-runs=0`**: libFuzzer **replays the
materialised corpus** (every seed + committed regression) with **zero mutation
or exploration**, then exits. The set of tested inputs is therefore exactly the
committed corpus, and the pass/fail is **provably flake-free** — it cannot vary
run-to-run. (An exploring smoke, `-runs=N`, is *not* bit-deterministic even with
`-seed=1`: libFuzzer under AddressSanitizer perturbs the coverage-guided path
slightly, so a long-enough exploring run could wander into an as-yet-unhardened
member of the parquet metadata-panic class on some runs but not others — a
nondeterministic red that would block a stacked-PR chain. `-runs=0` removes that
by not exploring at all.) `-seed=1` is harmless at `-runs=0` (there is no
mutation to seed) and is kept for clarity.

**Finding new class members is a deliberate LOCAL activity**, not CI's job: run
a long campaign with an unpinned seed (`cargo +nightly fuzz run <target> --
-runs=1000000`, no `-seed` — like the ~40k-run run that found the `byte_range`
case). A newly-found panic is minimised, turned into a **prevention guard** (so
the `panic=abort` build stays green) plus a committed source-form regression,
and only then does it enter the corpus — at which point the deterministic
`-runs=0` replay covers it too.

**Future dep-bump path:** when an arrow-rs release lands the metadata-panic
fixes above and a long local campaign stays clean with the `catch_unwind`
removed, drop the backstop (and, per member, the prevention guards) and rely on
upstream's typed errors. Track it against the crate bump; keep the two committed
regression fixtures (`tests/fixtures/adversarial/malformed_arrow_schema.rs`,
`malformed_parquet_byte_range.rs`) either way.

## A fuzzer-found crash

Minimise it (`cargo +nightly fuzz tmin <target> <artifact>`), then add the
minimised input as a **source-form generator** in
`tests/fixtures/adversarial/mod.rs` plus a `tests/security.rs` regression — the
same permanent-regression path every adversarial fixture already follows. Fixing
the underlying crate bug is out of scope for the harness itself.

## `unsafe`

The `fuzz_target!` macro expands to a `#[no_mangle]` `extern "C"` entrypoint whose
glue is `unsafe` — the ONLY tolerated `unsafe` here, exactly the same exemption
the PyO3 macro glue gets. No hand-written `unsafe` appears in this crate, and the
`ironcondor` crate keeps `#![forbid(unsafe_code)]`.
