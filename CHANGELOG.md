# Changelog

All notable changes to `ironcondor` are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(the full versioning policy lives in the design docs, local until v0.1.0).

## [Unreleased]

### Security

- **`RUSTSEC-2026-0235` (`rkyv` 0.7.46) suppressed with a documented note.**
  The advisory (insufficient archive validation causing out-of-bounds reads,
  fixed in rkyv 0.8.17) reaches `Cargo.lock` only as an **optional**
  dependency of `rust_decimal` 1.42.1; no feature combination of this crate
  enables it, so rkyv is never compiled into the crate, the wheel, or the test
  binaries (`cargo tree -i rkyv --all-features --target all` prints nothing).
  `rust_decimal` 1.42.1 is the latest release and still declares rkyv 0.7, so
  there is no upgrade path; the ignore is mirrored in `.cargo/audit.toml` and
  `deny.toml` and is dropped as soon as upstream moves to rkyv >= 0.8.17. No
  dependency, lockfile entry, or compiled code changed.

## [0.5.0] - 2026-07-19

### Added

- **Resting-order (GTC) lifecycle in realistic mode + batch error caching
  (#110).** A GTC limit that fills partially or not at all is now a first-class
  working order: the engine pre-mints order ids and passes them through the
  execution seam (`ExecutionModel::fill` gains `submit_ids`), a pending-order
  registry tracks each resting order across steps, and refresh-generated fills
  are correlated back through the new `carry_fills()` sidecar
  (`CarryGroup { order_id, fill_count }` describing the e1 prefix) — a carried
  open pushes a new inventory leg under the order's original trade with a
  continuing `fill_seq`, a carried close reduces its target leg with the reason
  captured at scheduling. `Cancel`/`Replace` are live: validated against the
  step-start registry (an id never pending is a typed error; one consumed by
  the same step's refresh is a benign no-op), resolved through the new
  domain-`OrderId` → book-id bridge, with `Replace` as cancel-plus-fresh-submit
  under a new identity. `ChainContext::pending` is now populated, so
  `assert_owned` enforces ownership and `close_all` reconciles against resting
  closes (an unfilled resting close leaves the leg honestly `open_at_end`; a
  pending open at end of data is dropped, never phantom-filled). Books whose
  contract left the snapshot universe are evicted unless they hold a live
  strategy order (bounded per-refresh work under a rolling universe); a taker
  intent crossing the strategy's own resting order fails closed (typed error)
  until maker-side e2 capture is designed. Batch shared-tape materialisation
  failures are cached per path as an error descriptor, so every run sharing a
  bad path records the identical typed error without re-parsing it.
  IOC-only runs derive byte-identical ids and bundles — all goldens unchanged.

### Fixed

- **24 stack-review findings closed in one pass (PRs #55–#109 review response).**
  The engine/analytics corrections change golden **values** (schema untouched;
  re-blessed in the same commit, the sanctioned path):
  - **Attribution cash scale**: per-fill slippage now scales by
    `contract_multiplier` like every other cash flow, so `spread_capture` is on
    the same basis as the P&L it explains; premium aggregates
    (`net_premium_cents`, `return_on_premium`, `premium_capture`,
    `CapitalUtilization`) carry the multiplier too.
  - **Closed-leg attribution**: per-step Greek attribution now samples
    beginning-of-step holdings, so a leg closed this step contributes its final
    interval to θ/Δ/V instead of falling into the residual; the exact
    attribution identity is unchanged.
  - **Terminal-step duplicate close**: an exit policy firing on the final step
    no longer collides with `on_end`'s close-all (the run previously aborted);
    close-all reconciles against already-scheduled closes.
  - **Correctness hardening**: leg selection matches the full contract identity
    (expiration included) in multi-expiry snapshots; stale-quote exit decisions
    read the ledger's carried mark, not the entry premium; the execution seam's
    fill-correlation mode is now explicit (`fill_groups() -> Option<…>`), so a
    surplus realistic fill is a typed error, never silently attached to the
    wrong order; `SimClock` rejects step regressions in release builds;
    `ExitPolicy::TimeSteps` records a truthful exit reason instead of
    `Expiration`; the golden oracle rejects fractional-cent metrics instead of
    truncating.
  - **Boundary validation**: `Quantity`, `OrderIntent` (marketable ⇒ IOC),
    `InstrumentSpec`, and `RunId` (64-hex; `from_hex` now fallible) validate on
    deserialization; `TapeMeta` enforces the 0-based consecutive step contract;
    the bundle reader verifies `strategy_run_id == run_id` and post-sort key
    uniqueness / step contiguity / cross-table consistency; the bundle writer's
    overwrite is move-aside (a complete bundle survives any single failure);
    Parquet/CSV ingestion hashes and parses one open handle (no TOCTOU);
    scenario expansion gains a hard `MAX_RUNS` ceiling, rejects an explicit
    seed with `count > 1`, and gates walk shocks to `StressTest`; the Python
    `Bundle.metrics()` read is bounded and `Bundle.write()` rejects
    source == destination before deleting anything.

### Security

- **Simulator responses are capped while streaming** (review fix): every
  response body is read chunked into a bounded buffer and aborted mid-stream
  past the ceiling, so a hostile simulator can no longer force an OOM before
  the post-parse limits ran. **URL userinfo redaction fails closed** for
  scheme-less authority-like values (`user:secret@host` without `://` now
  collapses to `[redacted-url]`). The release workflow's publish job
  additionally requires the repository variable `PYPI_PUBLISH_ENABLED == 'true'`
  (fail-closed kill-switch complementing the environment-reviewers gate).

- **Captured-log credential test proving a simulator credential never leaks, and
  the supply-chain / no-panic-across-FFI controls reaffirmed at the 1.0 cut
  (#53).** The current `simulator` surface (frozen at #49) has **no auth-token
  field**; the only channel a credential can take is URL userinfo
  (`http://user:token@host`), which `reqwest` turns into a transport
  `Authorization` header. A new `tests/secrets_log.rs` (feature `simulator`)
  drives a dummy credential through a full materialise → provenance → manifest
  path **and** a failure path, capturing every `tracing` event, the resulting
  manifest provenance, the derived `run_id`, and the returned error, and asserts
  the credential substring appears **nowhere**.
  - **One real leak found and fixed.** `SimulatorSourceSpec.base_url` was
    recorded **verbatim** in the manifest (both the top-level `data_source` and
    the nested `config.data_source`) and in the `run_id` preimage, so a
    userinfo-embedded credential would have been written to disk. Closed with a
    custom `Serialize` on `SimulatorSourceSpec` that **redacts URL userinfo at
    serialisation** — the single chokepoint through which every recorded copy
    passes — while the in-memory value the transport client authenticates with
    keeps the credential. Redaction is a no-op for every credential-free URL, so
    all existing goldens/fixtures are byte-unchanged; two configs differing only
    in the embedded credential now derive the **identical** `run_id`. The
    manifest records only data-source **identity** (host URL + tape `sha256`),
    never a credential ([docs/07 §9](docs/07-performance-and-security.md#9-secrets-handling)).
  - **No leak on the error/log path (verified, no fix needed).** `reqwest`
    0.12.28 already redacts URL userinfo from its error `Display`, and the crate
    never embeds `base_url` in an error string or a `tracing` field — so
    `BacktestError::Session` messages and the bounded-retry warnings carry
    structured context, not a credential.
  - **Supply-chain gates reaffirmed green at the cut.** `cargo audit --deny
    warnings` and `cargo deny --all-features check` both pass; the **only**
    suppressed advisory is `RUSTSEC-2024-0436` (`paste 1.0.15` **unmaintained** —
    a notice, not a vulnerability; transitive through the upstream numeric
    stack), documented identically in `.cargo/audit.toml` and `deny.toml`. No
    suppression masks an unfixed advisory; the duplicate-version entries are
    surfaced as non-failing warnings by design. No new runtime dependency (the
    test's HTTP responder and `tracing`-capture subscriber are zero-dep).
  - **`#![forbid(unsafe_code)]` intact** and the **no-panic-across-FFI** control
    (#40) stays asserted by `python/tests/test_errors.py`
    (`test_induced_panic_surfaces_as_engine_error`,
    `test_induced_panic_is_caught_by_the_base_clause_and_interpreter_survives`) —
    the third leg of the posture.
- **Fuzz targets landed for the three untrusted-byte parser surfaces — CSV
  feed, Parquet feed, and bundle read-back (#52).** A `fuzz/` `cargo-fuzz`
  subcrate (its own workspace, so the main `Cargo.lock` and `cargo deny` graph
  are untouched) with three libFuzzer targets driving the real public parsers
  (`CsvFeed::open`, `ParquetFeed::open`, `read_bundle`) under tight
  `ResourceLimits`, asserting the v1.0 invariant: **for any input bytes, the
  only outcomes are a typed `BacktestError` or a valid parse — never a panic,
  hang, or OOM**. The seed corpus is materialised at fuzz-time from the
  committed adversarial generators (source-form, never a committed binary), and
  a `fuzz-smoke` CI job runs a short smoke over each target (nightly toolchain
  installed in-job only; the repo stays pinned to stable 1.97.0).
- **Two real parser panics found by the fuzzer and closed (#52).** Both were
  reachable through the Parquet read path (`ParquetFeed::open` and
  `read_bundle`) on crafted input, and both are now committed as minimised,
  source-form regression fixtures with `tests/security.rs` assertions:
  1. **arrow-ipc embedded-schema panic** — a Parquet footer may embed an
     `ARROW:schema` IPC flatbuffer whose parse panics inside `arrow-ipc`
     (`unimplemented!("Type NONE not supported")` in `get_data_type`). Fixed by
     opening the reader with
     `ArrowReaderOptions::new().with_skip_arrow_metadata(true)` at both call
     sites, so the schema is derived from the Parquet schema itself — which the
     feed/bundle column validation already checks — and the embedded flatbuffer
     is never parsed.
  2. **parquet metadata `byte_range` assert** — a crafted negative column-chunk
     offset trips `assert!(col_start >= 0 && col_len >= 0)` in
     `ColumnChunkMetaData::byte_range()` during row-group decode.
- **Untrusted-Parquet hardening: a prevention guard + a `catch_unwind`
  backstop (#52).** The `byte_range` assert is one of a *class* of Parquet
  metadata panics arrow-rs is still hardening upstream (arrow-rs
  [#9840](http://www.mail-archive.com/commits@arrow.apache.org/msg61636.html),
  [#9868](http://www.mail-archive.com/commits@arrow.apache.org/msg61779.html),
  [#5382](https://www.mail-archive.com/commits@arrow.apache.org/msg38005.html)),
  several still present in parquet 59.1.0 (the latest published, so no dep bump
  is available yet). Two complementary layers close it at the two Parquet read
  sites (`src/data/historical.rs`, `src/bundle/reader.rs`):
  - a **pre-decode prevention guard** that rejects a negative column-chunk
    `compressed_size` / `data_page_offset` / `dictionary_page_offset` before the
    decode loop, so the `byte_range` panic never fires. This is what keeps the
    **fuzz** targets green: `cargo-fuzz` builds `panic=abort`, where a *caught*
    panic still aborts — a fuzz target only stays green if the panic is
    *prevented*, not merely caught.
  - a narrowly-scoped `std::panic::catch_unwind` **backstop** wrapping **only**
    the upstream builder-construction and row-group-decode calls (our own
    validation stays outside the wrap, so a bug in this crate still panics
    loudly); a contained panic is logged (`tracing::warn!`) and mapped to a
    typed `BacktestError` (`Data` for the feed, `Bundle` for read-back). In the
    production unwind build this contains the *residual*, still-unhardened
    members of the class that no prevention guard exists for yet.

  The `fuzz-smoke` job is a **deterministic green-on-known gate**: it runs each
  target at `-runs=0`, replaying the materialised corpus (every seed + committed
  regression) with **zero mutation/exploration**, so the pass/fail is provably
  flake-free (an exploring run is not bit-deterministic under AddressSanitizer
  and could nondeterministically red a stacked-PR chain). Discovery of new class
  members is a deliberate **local** campaign that becomes a new prevention guard
  + regression. The future dep-bump path (drop both layers once a hardened
  arrow-rs release ships and a long local campaign stays clean) is documented in
  `fuzz/README.md`.

### Added

- **The v1.0 pre-release + acceptance pass is one runnable, fail-closed script,
  `scripts/release_check.sh` (#54).** It runs the [RELEASE-PROCESS.md §1]
  mechanical checks (`cargo fmt --all --check`, `clippy --all-targets
  --all-features -- -D warnings`, `test --all-features`, `build --release`,
  `cargo publish --dry-run` — packaging validation only, no upload — and the
  `[Unreleased]` non-empty §-abort rule) and the §13 acceptance gates shipped
  across #49–#53 (the surface-freeze snapshots, the golden determinism suite +
  `tests/golden/REGRESSION-EVIDENCE.md`, the PB-1 zero-alloc gate, the H1–H5
  hot-path regression gates, the parser fuzz corpus replay, and the
  adversarial-input + `cargo audit` + `cargo deny` supply-chain gates), grouped
  into `--section`-selectable stages. Each check prints a machine-readable
  `RESULT <PASS|FAIL|SKIP> <section.id> …` line and the run ends in a
  `RELEASE_CHECK_RESULT=PASS|PASS_WITH_SKIPS|FAIL` verdict (exit non-zero on any
  FAIL). The default posture is **fail-closed** — a check whose tool is absent
  FAILs, so "all green" cannot be reached by a missing prerequisite — with
  exactly three documented **skip-with-notice** checks whose tooling is optional
  and CI proves independently (maturin wheel-build sanity, the `gh` milestone
  open-issue check, the nightly + `cargo-fuzz` corpus replay); a skip is printed
  loudly and downgrades the verdict to `PASS_WITH_SKIPS`, never a silent pass.
  The cut itself stays **user/time-gated and is NOT automated**: the script
  never bumps the version, locks the CHANGELOG, tags, pushes, or publishes, and
  prints the items it cannot verify (the one-quarter stability window, the
  explicit publish approval, and wheels-green-on-the-release-commit) as a manual
  checklist. `docs/RELEASE-PROCESS.md` §13 documents the gate; no production
  code changed and no new dependency was added.
- **Hot-path percentile/linearity regression gates for every tracked path
  H1–H5 are wired into CI (#51).** The `BENCH.md` baselines (H1/H2 #29, H4 #37,
  H5 #43) are now wrapped in gates that fail a merge which regresses a tracked
  quantity beyond its committed tolerance. **Every gate is baseline-relative —
  no absolute-number threshold** except the one documented `#29` coarse naive
  backstop; each gated quantity is a **dimensionless within-run ratio**, so a
  uniform hardware clock factor cancels and the gate is portable from the Apple
  M4 Max baselines to the Linux CI runners:
  - a new **conversion bench** `benches/conversion.rs` (H3/PB-4, previously a
    DESIGN TARGET with no bench) times `raw_quotes_to_snapshot` over a 16×
    contract sweep and emits the per-contract cost ratio; its baseline
    (per-contract cost ratio 1.13×, LINEAR) is recorded in `BENCH.md` §H3;
  - `scripts/linearity_gate.sh` gates the H3 conversion (per-contract cost
    ratio ≤ 4.0×) and H4 bundle-writer (per-row cost ratio ≤ 2.0×) **linearity**
    — a super-linear O(n²) regression fails;
  - `scripts/pyo3_gate.sh` gates the H5 PyO3 boundary (full/single marshal ratio
    band [8×, 32×]); `benches/pyo3_marshal.rs` gains the machine-readable
    `MARSHAL_*` lines it parses;
  - the existing `scripts/bench_gate.sh` (#29) continues to gate H1+H2 via the
    realistic/naive overhead ratio band [18×, 72×] plus the coarse naive
    backstop;
  - the `bench-gate` CI job now runs all four gate steps plus a **PB-1 zero-alloc
    reaffirmation** (`cargo test --test zero_alloc`, reference — the canonical
    hard gate stays the separate `zero-alloc` job);
  - `BENCH.md` records the per-gate tolerance + run conditions next to each
    baseline, and `scripts/GATE-EVIDENCE.md` captures the fail-closed evidence
    (each gate proven to FAIL on a real bench-driven super-linear/disproportionate
    scratch change, reverted, plus override self-tests). No production code
    (`src/data/convert.rs`, `src/bundle/writer.rs`, `src/python/*`) changed.
- **The determinism golden suite is hardened to the full bundle for every named
  scenario (#50).** The frozen four-table + `manifest.json` bundle golden (#36)
  covered only `iron_condor_naive`; it now covers **every** named golden scenario
  under the single comparison oracle (`tests/oracle/mod.rs`) plus a
  same-environment run-twice byte-identical assertion:
  - `tests/bundle_golden.rs` gains full-bundle goldens for `short_strangle_naive`
    (naive `ShortStrangle`) and `iron_condor_realistic` (realistic fills, feature
    `orderbook`), each with committed `expected/` trees (manifest + four Parquet
    tables, regenerate with `BLESS=1 cargo test --test bundle_golden`), plus a
    mode-pair test proving the naive/realistic bundles share a manifest key shape
    while their table values diverge;
  - the `golden` CI job now runs the extended suite (`--test golden` and
    `--test bundle_golden`, each with and without `--features orderbook`);
  - the v1.0 acceptance line "caught at least one real regression" is now
    evidence, not prose: `tests/golden/REGRESSION-EVIDENCE.md` records a real
    wall-clock determinism leak introduced on scratch and caught by the
    run-twice **byte layer** (the logical golden missed it because the affected
    field lives in the opaque `metrics` object the cross-environment oracle
    strips), then reverted. The frozen `iron_condor_naive` bundle and the
    conformance fixture are untouched.
- **The four public surfaces are frozen for SemVer 1.0 (#49).** Each surface —
  the result bundle, the `src/lib.rs` Rust re-exports, the PyO3 module surface,
  and the `BacktestConfig` + env-var configuration surface — now has a named
  source of truth and a CI diff gate that fails a PR changing the surface without
  updating its committed snapshot in the same diff:
  - the **Rust re-export surface** is snapshotted in
    `tests/surface/rust-public-api.txt` and diffed by the new `surface` CI job
    (`tests/surface.rs`, regenerate with `BLESS=1 cargo test --test surface`);
  - the **PyO3 runtime names** are pinned in
    `python/tests/expected_public_names.txt`, checked against the built wheel by
    a new step in the `python-wheels` job and by `python/tests/test_surface.py`
    (`python/ironcondor.pyi` remains the human-readable snapshot);
  - the **`BacktestConfig` serialized field set** and the **runtime env-var set**
    (`API_URL`) are pinned by `test_config_serialized_field_set_is_pinned` and
    `test_runtime_env_vars_are_pinned`;
  - the **result-bundle schema** stays gated by the v0.3 golden + conformance
    round-trip (#36) — no new bundle code, its failure is wired to the freeze.
  The v1.0 commitments, the one-quarter stability window (start = the v1.0 cut
  date), and the deferred CLI surface are recorded in `docs/SEMVER.md`. A robust
  stable-toolchain committed-snapshot gate was chosen over `cargo-public-api` /
  `cargo-semver-checks`, both of which require nightly rustdoc JSON (and the
  latter is directional under `0.x` semantics); rationale in `docs/SEMVER.md`.
- **The `ironcondor.bundle.v1` wire contract is FROZEN (#36).** The result-bundle
  schema is now a **versioned wire contract**, not a proposal: the tag
  `"ironcondor.bundle.v1"` is the consumer's primary version pin, and any
  post-freeze change to a `manifest.json` field, a Parquet column
  name/type/nullability, a sort/unique key, or an identifier grammar **bumps the
  tag** (`v1` → `v2`, a major SemVer event), is ChainView-coordinated in the same
  week, regenerates the goldens, and adds a `CHANGELOG.md` entry
  (`docs/SEMVER.md#result-bundle-versioning`, `docs/05-analytics-and-reporting.md`
  §12 now marked FROZEN). The freeze is pinned by:
  - a **golden bundle** (`tests/golden/iron_condor_naive/expected/`:
    `manifest.json` + the four Parquet tables) asserted **write → read → equal**
    under the single comparison oracle — decode, sort by each table's pinned key,
    integer cents compared **exactly**, `drawdown` within the fixed tolerance,
    canonical-JSON manifest with `created_utc` excluded — plus a same-environment
    **run-twice byte-identical** test (four tables byte-for-byte, manifest after
    stripping `created_utc`); a value-changing engine / attribution / schema
    change that leaves the golden untouched fails CI (`tests/bundle_golden.rs`,
    `tests/oracle/mod.rs` extended with the four-table + canonical-manifest
    oracle).
  - the **shared conformance fixture** (`tests/fixtures/conformance/`) — a
    realistic thin-book four-leg condor whose legs share one `trade_id`, with one
    leg closed mid-run (a non-null `exit_reason`), three legs left `open_at_end`,
    and multi-level realistic fills exercising the `(step, order_id, fill_seq)`
    unique key — asserting **every cell** of the `docs/05` §12 producer-side
    contract matrix, loaded identically by ChainView's tests
    (`tests/conformance.rs`; the producer/regeneration side is feature-gated
    `orderbook`, the matrix guard runs on the default build via `read_bundle`).

### Changed

- **Bundle build identity is `code_version + lockfile_sha256`, with no
  per-commit git sha (#36).** `docs/01` §10, `docs/05` §6, and `src/bundle/schema.rs`
  had promised `code_version` = "crate version + git short sha", while the writer
  only ever set `env!("CARGO_PKG_VERSION")`. Aligned the contract to the code: a
  per-commit git sha in the `run_id` build identity would change the golden's
  `run_id` (its directory name) and manifest **every commit**, so a frozen golden
  could never exist. Build identity is now documented as crate version +
  `Cargo.lock` sha256 (both stable across commits); git provenance, if ever
  wanted, would be a manifest-only field excluded from the `run_id` and byte
  comparison, like `created_utc`.
- **The bundle writer builds `FillRow`/`PositionRow` at encode time (#36).**
  `src/bundle/writer.rs` (termination-phase, free to allocate) now constructs the
  flat wire rows (`FillRow`/`PositionRow`, the reader's decode target) from the
  in-loop collector carriers (`FillRecord`/`PositionSnapshot`) and sorts them
  through the pinned `fill_sort_key`/`position_sort_key` helpers, unifying the
  wire-row representation (write = encode-time build, read = decode target) and
  removing the previous inline-sort duplication. The produced Parquet bytes are
  **unchanged** (proven byte-identical against the goldens).

### Fixed

- **Attribution mis-scale proptests no longer flake on a rounds-to-zero input
  (#36).** `theta_term_uses_daily_greek_and_day_delta` and
  `vega_term_uses_per_point_greek_and_pp_delta` (`tests/property.rs`) guard the
  `prop_assert_ne!(correct, mis)` discriminator with `prop_assume!(correct != 0)`:
  a term that rounds to `0` cents cannot distinguish the mis-scale (both round to
  `0`), so the degenerate case was a false failure. The primary value-equality
  assertion still runs for every case (including degenerate ones), and the guard
  still bites under the ×365 / ×100 / N² mis-scale, so the mis-scaling intent is
  intact; no failing seed is committed to `proptest-regressions/`.

- Result-bundle record types + `manifest.json` schema — the typed shape of the
  frozen `ironcondor.bundle.v1` contract ChainView consumes (#33). The two
  remaining bundle rows land next to the existing pair in `src/domain/result.rs`:
  `FillRow` (one executed fill, the wire projection of `Fill` plus the
  `trade_id`/`position_id`/`order_id`/`fill_seq` lifecycle ids) and `PositionRow`
  (a leg's per-step state); `exit_reason` is the **only** nullable column in the
  whole bundle, every money column is integer cents, and the only float column is
  `drawdown` on `EquityPoint`. `src/bundle/schema.rs` adds the schema tag
  constant `BUNDLE_SCHEMA` (`"ironcondor.bundle.v1"`), the pinned per-table sort
  keys (`fills` by `(step, order_id, fill_seq)` unique; `positions` by
  `(step, position_id)`; `equity_curve`/`greeks_attribution` by `step`), the
  `RunId` hex-string newtype with a deterministic `RunId::derive` over the
  reproducibility tuple (seed + semantic config + strategy + tape identity +
  build identity — **excluding** the operational `overwrite`/output-path
  controls), the `RowCounts` per-table integrity struct, and the single
  serialization-source `Manifest` carrying exactly the docs/05 §6 fields (no
  `currency` field — USD is fixed by the tag). Types only; the Parquet encoding
  (#34), read-back (#35), and the golden freeze (#36) follow.

- Queue position and market impact in realistic mode (`src/execution/realistic.rs`,
  feature `orderbook`) — both **emergent** from routing through the seeded book,
  not configured knobs. A marketable order larger than the touch walks the
  seeded ladder, producing one `Fill` per level at progressively worse prices
  (the realised price feeds `Fill.price`, the gap vs the fixed `decision_mid`
  is `Fill.slippage`, positive = adverse); a resting strategy limit fills only
  behind the seeded depth (price-time priority), so a thinly seeded strike
  leaves a partial or zero fill — realistic mode can fill less than the full
  intent. `SlippageModel` has no effect in realistic mode (tested); fees match
  naive exactly. The `iron_condor_realistic` golden is committed alongside the
  naive golden (equity curve + minimal metrics, same comparison oracle,
  same-seed byte-identical), and is honestly worse than the naive golden by the
  spread crossed on entry and exit (#24).

### Fixed

- Realistic-mode close routing: `ob_side` no longer double-flips the book side
  on a `Close`. The strategy's `close_command` already flips a leg to its trade
  side, so a buy-to-close of a short leg was being routed to the bid (a
  favourable fill with a dishonest slippage sign). It now crosses the ask
  (adverse), matching the naive interpretation of `intent.side` and the
  sign-convention truth table — closes cross the spread adversely like opens
  (#24).

- Per-strike book seeding for realistic mode (`src/execution/liquidity.rs`,
  feature `orderbook`): each snapshot seeds every leaf book with a multi-level
  ladder — a touch level at the quoted bid/ask sized from the configured
  `LiquidityProfile`, plus up to `L` deeper levels stepping one
  `tick_size_cents` away from mid, with geometric size decay
  `round(touch_size × rⁱ)` (computed in `Decimal`, half-to-even, terminating
  when a level rounds to zero). Every price is tick-aligned by construction;
  all seed `OrderId`s come from the disjoint seeded-maker range; orders submit
  in a fixed order (ascending `contract_id`, bid before ask) so the seeded
  book is byte-identical across runs. Seeding runs once before the strategy's
  intents, so a strategy order queues behind the seeded depth (#23).
- `BacktestConfig.liquidity_profile` (`LiquidityProfile` — touch-size function
  `QuotedSize`/`Flat`, depth `L` default 5, decay `r` default 0.5, validated),
  recorded in the run config so a seeded book is reproducible from the
  manifest (#23).

- The `option-chain-orderbook` adapter for realistic fills
  (`src/execution/realistic.rs`, `RealisticFill`, feature `orderbook`) — the
  foundation of realistic mode. It navigates to the leaf `OptionOrderBook`,
  mints deterministic `OrderId`s from seeded `Id::Sequential` counters with
  disjoint strategy/seeded-maker ranges, scales integer-cents prices to the
  book's `u128` ticks via `InstrumentSpec.tick_size_cents` (a non-aligned
  price is `PriceNotTickAligned`), maps `optionstratlib::Side` + the
  `PositionAction` to the book's Buy/Sell (a close-long is a Sell), routes a
  `Submit` through `add_limit_order_full`, and emits one shared `Fill` per
  price level (byte-shape-identical to the naive model, `mode = Realistic`)
  with the `FeeCharge::FirstFill`/`LaterFill` fee split. A marketable intent
  becomes a tick-aligned aggressive limit off the touch capped at
  `marketable_cap_ticks`, with the unfilled remainder cancelled, not chased.
  Entirely feature-gated — the default build carries no orderbook dependency
  (#22).
- `BacktestConfig.marketable_cap_ticks` (serde default 10, validated `> 0`):
  the tick cap for converting a marketable intent to an aggressive limit (#22).

- Adversarial-input hardening for the Parquet feed (`tests/security.rs`, the
  `security` CI job): 11 committed deterministic adversarial-fixture
  generators (crossed quote, negative strike, NaN analytic, out-of-order and
  duplicate timestamps, oversized steps/contracts, decompression bomb,
  truncated footer, corrupt row group, over-total-bytes) each drive
  `ParquetFeed::open` and assert the documented typed `BacktestError` with a
  bounded resource ceiling — never a panic, hang, or OOM. Closes the v0.1
  untrusted-input security gate for the release feed (#21).
- The `max_total_bytes` materialised-tape ceiling in the Parquet feed: a
  per-snapshot in-memory footprint estimate (O(entries), checked arithmetic)
  accumulated before each `tape.push`, so a hostile tape is cut off with
  `BacktestError::TapeTooLarge { limit: "max_total_bytes", .. }` before memory
  grows unbounded — the one feed ceiling #9 had left unenforced (#21).

- Supply-chain CI gates from v0.1: the `audit` job (`cargo audit --deny
  warnings`) and the `deny` job (`cargo deny check` — licences, bans, sources,
  advisories), both green on the current dependency set so a new RUSTSEC
  advisory or a disallowed licence fails the build. `deny.toml` encodes an
  **explicit** licence allow-list (MIT, Apache-2.0, BSD-2/3-Clause, BSL-1.0,
  CC0-1.0, Unicode-3.0, 0BSD, Zlib, bzip2-1.0.6) and a single documented
  advisory ignore — RUSTSEC-2024-0436 (`paste` unmaintained, transitive via
  `optionstratlib`'s numeric stack, not a vulnerability); `.cargo/audit.toml`
  mirrors that one ignore. `#![forbid(unsafe_code)]` in the shipped crate is
  intact (#20).

### Changed

- `SECURITY.md` status refreshed: implementation has landed (the v0.1 core
  engine and naive fill model), correcting the stale "no implementation code
  exists yet" framing; the report channel, scope, and disclosure expectation
  are unchanged (#20).

- The zero-steady-state-allocation replay-loop CI gate (`tests/zero_alloc.rs`,
  the `zero-alloc` CI job): a test-only per-thread counting allocator plus a
  sampling-strategy decorator measure the per-step-body (steps b–g) allocation
  delta between a warmup step and the last step over the **real**
  `OptStratAdapter<IronCondor>` and assert it is **zero**; a deliberately
  injected per-step allocation makes the delta non-zero, proving the gate
  bites. A build-failing invariant gate, distinct from the throughput bench
  (#19).

### Changed

- `OptStratAdapter::exits()` no longer rebuilds an `OptionChain` or reprices
  the wrapped strategy every step in v0.1: `underlying` is sourced directly
  from the snapshot scalar (byte-identical to the old
  `chain.underlying_price`), and the reprice — with its transitive upstream
  `Utc::now()` reach — is deferred behind `policy_reads_inner` (false for
  every v0.1 exit policy; re-enabled when a Greek-driven policy is wired).
  Output-preserving (the golden passes unblessed), this closes a wall-clock
  determinism reach on the replay path and removes ~44 heap allocations per
  step. The naive per-step throughput baseline was re-measured accordingly —
  p50 **2354 ns/step** (down from 4172), ≈ 25.5 × 10⁶ steps/min/core — and
  `BENCH.md` supersedes the #18 baseline that baked in the removed dead work
  (#19).

- The `criterion` + `hdrhistogram` bench suite (`benches/`, the `bench-hdr`
  convention) and the first **measured** performance baseline in `BENCH.md`:
  the naive-mode throughput bench (`benches/naive_throughput.rs`) drives the
  full `run_backtest` over a canonical 2048-step × 4-leg iron-condor Parquet
  chain, single strategy / single core, and reports `hdrhistogram`
  p50/p99/p99.9/p99.99 of per-run and per-step latency (not criterion's mean),
  with warmup and an explicit coordinated-omission disclosure (closed-loop
  back-to-back — CO does not apply). `BENCH.md` records the measured baseline,
  the full run-conditions block (CPU/cores/memory/OS/toolchain/`Cargo.lock`
  hash), and an interpretation block versus the docs/07 §3 PB-2 design target.
  This is the v0.1 baseline the #019 zero-alloc gate and the #051
  percentile-regression gate build on. `criterion` / `hdrhistogram` are
  dev-only (kept out of `cargo build`/`cargo test`), carry an audit note in
  `Cargo.toml` (both Apache-2.0 OR MIT), and leave `#![forbid(unsafe_code)]`
  in the crate intact (#18).
- The golden determinism test over the v0.1 artifacts
  (`tests/golden/iron_condor_naive/`): the committed equity curve and minimal
  metrics for the canonical `IronCondor` naive run, the single reusable
  comparison oracle (decode → sort by `step` → integer cents compared
  exactly, analytic floats within the docs/05 §12.5 tolerance), a
  same-environment run-twice byte-identity test, and a `BLESS=1` regeneration
  path so a deliberate engine change re-blesses the artifact in the same
  commit. Scoped to the equity curve + minimal metrics only — the four-table
  bundle and `manifest.json` golden land at v0.3 (#17).
- The `golden` CI job (`cargo test --test golden`); `proptest-regressions/`
  is now tracked so generated regression seeds are committed (#17).

- The v0.1 end-to-end slice is complete: `run_backtest`
  (`src/run.rs`) — a top-level composition root above both engine and
  analytics — ties `ParquetFeed` + `IronCondor` + `NaiveFill` + the ledger +
  metrics into "Parquet chain in, equity curve out", the v0.1 acceptance
  headline (#16).
- Minimal summary metrics (`src/analytics/metrics.rs`): per-step Sharpe,
  volatility, and total return, plus max drawdown as a ratio and a
  peak-to-trough cents magnitude — computed from the ledger's `EquityPoint`
  series and populated into the upstream
  `optionstratlib::backtesting::BacktestResult`
  (`general_performance`, `drawdown_analysis`, and `custom_metrics` for the
  cents magnitude), inventing no parallel result type. Per-Greek attribution,
  `manifest.json`, the four-table bundle, and the full trade/risk statistics
  remain v0.3 — their upstream structs are left defaulted with a doc note
  (#16).

- Mark-to-market ledger enrichment (`src/engine/ledger.rs`): `stale_mark`
  tracking via the engine-owned `PositionMark { position_id, mark, stale }`
  scratch (a held leg absent from a step carries its last-known mark and is
  flagged stale), exposed through `Ledger::position_marks()` for the v0.3
  position rows; and the expiry-settlement rule — a held leg missing at or
  after its own expiry instant is `BacktestError::DataOutOfOrder` (a
  settlement mark is mandatory then), while a merely sparse chain before
  expiry is tolerated by carry-forward. Property tests pin the two distinct
  invariants — cash changes only by fills and fees, and equity reconciles to
  `cash + Σ(mark × quantity × contract_multiplier × side_sign)` every step —
  plus the unclamped drawdown definition at zero and negative equity (#15).

- `BacktestEngine::run<F: DataFeed, X: ExecutionModel, S: Strategy>`
  (`src/engine/backtest.rs`) — the synchronous, single-threaded, monomorphised
  replay loop implementing the normative state machine: startup (materialise
  the tape, execute `on_start` intents against `S0` before step 0), per-step
  (snapshot → mark → exits strictly before entries → naive fills → ledger
  → one `EquityPoint`), and termination (`on_end` inside the final step, legs
  left open flagged `open_at_end`, never a synthetic terminal fill). Lifecycle
  ids are minted from seeded monotonic counters; the only randomness is the
  seeded `ChaCha8Rng`; no wall-clock, no `thread_rng`, no look-ahead. Returns a
  `BacktestRun` carrying the populated `optionstratlib::backtesting::BacktestResult`,
  the `EquityPoint` curve, and the open-at-end legs (#14).
- The minimal mark-to-market `Ledger` (`src/engine/ledger.rs`) — cash moves
  only by fills and fees, positions marked at the snapshot mid with last-known
  carry-forward, `EquityPoint` and unclamped drawdown emitted once per step
  (enriched with the rigorous invariants at #15) — and the `EquityPoint`
  domain record (#14).
- Property tests `no_look_ahead` (perturbing a future snapshot leaves the
  prefix byte-identical) and `same_seed_same_result` (an RNG-consuming strategy
  reproduces exactly), plus an end-to-end `ParquetFeed → run → BacktestResult`
  integration test over the canonical fixture (#14).

### Changed

- `Underlying` is interned as `Arc<str>` instead of `String`, so cloning a
  `ContractKey` (and every `Fill` / `OpenPosition` / `QuoteView` that owns one)
  is a refcount bump rather than a heap allocation — the warm replay-step body
  no longer allocates, the prerequisite for the PB-1 zero-allocation gate. The
  exact `Eq` / `Hash` / `Ord` semantics and the grammar validation are
  unchanged (#14).

- The naive fill model (`src/execution/naive.rs`, `NaiveFill`) — the fast v0.1
  execution mode and criterion throughput baseline. A pure function of the
  snapshot and config with no book, state, or randomness: it fills every
  `Submit` intent single-shot at the full quantity, reference price = the
  quote mid, with the configured `SlippageModel` (`None` / `FixedCents` /
  `SpreadFraction` / `SizeProportional`, all integer-cents and deterministic)
  applied on the adverse side (a buy crosses toward the ask, a sell toward the
  bid), and emits the shared `Fill` shape via the `assemble_fill` seam. A sell
  whose adverse offset exceeds the reference floors at a zero premium (an
  explicit clamp, never `saturating_sub`); `Cancel`/`Replace` produce no fills
  (naive keeps no resting book) (#13).

- The `ExecutionModel` seam (`src/execution/mod.rs`): the command→fill trait
  (`fill` appends into a caller-owned `&mut Vec<Fill>` so PB-1 is satisfiable
  by the signature; `mode()`), and the shared `assemble_fill` helper — the
  single place a `Fill` is stamped with its mode, its signed `slippage` (via
  the `sign_convention` helper, never reinvented), and its `fees` — so both
  fill models are byte-shape identical. The per-order-once fee split is
  expressed by a caller-supplied `FeeCharge {FirstFill, LaterFill}` (the
  domain `Fill` has no `fill_seq`; that ordinal lives on the bundle `FillRow`
  at v0.3) (#12).

- `IronCondor` wired as the single v0.1 strategy: `StrategySpec` /
  `IronCondorSpec` (the manifest's strategy kind + parameters, money in
  integer cents, analytics `Decimal`) and `OptStratAdapter::from_spec`
  constructing the upstream `IronCondor` via its 17-arg `new` and mapping
  `StrategyError` to `BacktestError::Strategy` at the one construction seam.
  The four-leg condor entry emits four `Open` intents in one step, sourcing
  each `decision_mid` from the snapshot quote (never a repriced strategy), so
  the dormant upstream wall-clock reprice stays dormant. `ShortStrangle` is
  not wired (deferred to v0.2) (#11).

- The engine-facing `Strategy` seam (`src/engine/strategy.rs`): the `Strategy`
  trait (`on_start` / `exits` / `on_snapshot` / `on_end`, each appending into a
  caller-owned `&mut Vec<OrderCommand>` so PB-1 is satisfiable by the
  signature), the step-scoped `ChainContext` (snapshot, open inventory,
  pending orders, the sole seeded `ChaCha8Rng`, step), and `OptStratAdapter`
  wrapping any `PositionableStrategy` — the **single** owner of exit-policy
  evaluation, repricing the wrapped strategy and evaluating the configured
  `optionstratlib` `ExitPolicy` via the per-leg `check_exit_policy`, appending
  closing commands only (#10).
- `OpenPosition` and `PendingOrder` domain records — the engine's authoritative
  open-leg inventory and resting-order read models a strategy sees (#10).
- `rand_chacha` dependency (seeded `ChaCha8Rng` — the run's sole randomness
  source; `thread_rng` is never used) (#10).

- The Parquet historical feed (`src/data/historical.rs`, `ParquetFeed`) — the
  single v0.1 release feed. Reads one columnar file, groups rows into
  `ChainSnapshot`s by ascending `step`, funnels each through the single
  conversion boundary, and materialises a validated, strictly-ordered,
  immutable tape at construction; `next` is a pure in-memory read. Enforces
  `max_file_bytes`, `max_decompressed_bytes` (footer-declared **and**
  incremental), `max_steps`, and `max_contracts_per_snapshot` with no
  unbounded read, rejects a non-regular file before hashing, and pins the
  file `sha256` as the tape identity (#9).
- `BacktestError::Data(String)` for undecodable input bytes (truncated or
  corrupt Parquet footer / row group), distinct from `Conversion`, a semantic
  failure on data that decoded cleanly (#9).
- Dependencies for the columnar stack (shared with the future bundle writer
  per ADR-0004): `arrow 59` and `parquet 59` (both `default-features = false`,
  minimal features), `sha2 0.10`; dev-only `tempfile 3` (#9).

- The `DataFeed` seam (`src/data/feed.rs`): a trait exposing exactly
  `next` / `meta` / `tape_meta` (no peek/rewind — no look-ahead by
  construction), the engine-facing `TapeMeta` (`data_identity`, `non_empty`,
  `first_ts`, `final_step`) available before any writer exists so startup can
  derive `run_id`, and the `FeedKind` catalogue classifying each feed by
  feature flag and determinism source. Every feed materialises a validated,
  strictly-ordered, immutable tape before the loop and yields from it
  synchronously — `next` never blocks or `.await`s (#8).
- `DataSourceSpec` gains the feature-gated `Simulator` variant (session,
  base URL, intent-only `data_seed`, tape sha256, simulator version) (#8).

- The single chain-conversion boundary (`src/data/convert.rs`, hot path H3):
  a DTO-independent validation core `raw_quotes_to_snapshot` (strike
  positivity, tick alignment, crossed-quote rejection, the one
  `mid = floor_to_tick((bid + ask) / 2)`, `Days(n)` anchored once on the
  tape's first `ts_0`, NaN/inf-analytic rejection, `BTreeMap` assembly)
  reused by every feed; the simulator-gated `chain_response_to_snapshot`
  where f64 prices die once via banker's rounding; and
  `snapshot_to_option_chain`, the reverse view the strategy adapter
  reprices against (#7).
- Conversion property tests: strike preservation, tick-alignment
  rejection, and `Days(n) ⇒ ts_0 + n·86400e9 ns` UTC-calendar resolution
  independent of snapshot timing (#7).

- Migrated the `OptionStratBacktest` core (reviewed, not copied): the async
  `ApiClient` for OptionChain-Simulator sessions with locally defined wire
  DTOs, the `MarketSimulator` step wrapper, the `PositionableStrategy` bound
  (exactly `Positionable + Strategies + Optimizable`, no `Default`), and the
  `ScenarioType`/`ScenarioParams` data types — all simulator networking
  behind the `simulator` feature, absent from the default build (#6).
- Feature-gated dependencies for the `simulator` feature: `reqwest 0.12`
  (rustls, json), `tokio 1` (rt, macros), `serde_json 1` (#6).

### Fixed

- `MarketSimulator::is_terminated()` now returns `true` when no session
  exists (was `false` in the migrated source — a loop built on it could
  never start safely) (#6).
- `MarketSimulator` per-step session state is derived from the server's
  actual `session_info` step counters (`from_progress`) and the wire state
  string on session creation (`from_wire`) instead of a hardcoded
  `SessionState::InProgress` (#6).

- `SimClock` (`src/engine/clock.rs`): the deterministic, no-wall-clock time
  source the replay loop advances — `advance_to` rejects a duplicate or
  reversed timestamp as `DataOutOfOrder` (gaps preserved, never reordered);
  and the conceptual `Event` model (`Snapshot`/`Decision`/`Fill`)
  documenting the canonical per-step order with exits strictly before
  entries; `clock_monotonic` property test (#5).

- Market-data types: `InstrumentSpec` (validated `> 0` tick size and
  contract multiplier — the single authoritative cents↔ticks / scaling
  source), `QuoteView` (per-contract unit Greeks in native `optionstratlib`
  bases, `Decimal` analytics as the documented exception), and
  `ChainSnapshot` with `BTreeMap` quotes so iteration order is part of the
  determinism contract (#4).
- Execution-record types: lifecycle ids (`PositionId`, `OrderId`,
  `TradeId`), `PositionAction`, `TimeInForce`, `OrderIntent`,
  `OrderCommand`, and the mode-agnostic `Fill` carrying
  `mode: ExecutionMode`; `ExecutionMode` moved to `domain::execution` (#4).
- The `sign_convention` helper module encoding the one truth table:
  `side_sign`, checked `slippage_cents` (positive = adverse, both sides
  tested), and `spread_capture_cents = −Σ slippage` — the single sign flip
  (#4).
- Exact hand-written `Ord` on `ContractKey` (consistent with its exact
  `Eq`/`Hash`) so `BTreeMap` snapshot ordering is deterministic (#4).

- Canonical domain newtypes: `Cents` (signed ledger money, checked
  arithmetic returning `ArithmeticOverflow` — never a silent wrap),
  `PriceCents` with the two `Decimal` boundary helpers (banker's rounding on
  the `Decimal → cents` edge only, lossless reverse), `Quantity` (strictly
  positive), `Ticks`, `SimTime`, `StepIndex` — all `#[serde(transparent)]`
  bare scalars (#3).
- Contract identity: `Underlying` (grammar `^[A-Z0-9._]{1,32}$`),
  `ContractKey` reusing the upstream `ExpirationDate`/`OptionStyle` with
  hand-written **exact** equality/hashing (documented divergence from the
  upstream epsilon-tolerant semantics), and the round-trippable versioned
  `contract_id` (`"v1:{UNDERLYING}:{expiration_ns}:{strike_cents}:{style}"`)
  (#3).
- Property-test suite (`tests/property.rs`): money serde round-trips,
  no-silent-overflow cents arithmetic, `contract_id` round-trip identity,
  deterministic dollar→cents conversion (#3).

- `BacktestError` (thiserror): the single typed error boundary — every
  documented kind from the domain model, with lowercase messages carrying the
  offending value; upstream errors convert into these kinds in `src/error.rs`
  and nowhere else (#2).
- `BacktestConfig` skeleton with `validate()` as the untrusted-config seam
  (positive capital, per-field `ResourceLimits` hard caps, non-negative
  slippage fraction, no directory-escaping output path), `ResourceLimits`
  with documented defaults and hard caps, `FeeSchedule`, `SlippageModel`,
  `ExecutionMode`, and a minimal `DataSourceSpec` (Csv/Parquet) — all
  `deny_unknown_fields`; money fields are integer cents, never `f64` (#2).

- Crate skeleton: the layered `src/` module tree (`domain`, `engine`,
  `execution`, `data`, `analytics`, `bundle`, `python`, `error`, `config`) as
  compiling placeholders, `#![forbid(unsafe_code)]` + `#![warn(missing_docs)]`
  at the crate root, the four feature flags (`default`, `orderbook`,
  `simulator`, `python`), and the unconditional `rlib` + `cdylib` crate types
  (#1).
- Toolchain and lint policy: `rust-toolchain.toml` pinned to stable 1.97.0,
  `rustfmt.toml`, `clippy.toml`, and a root `Makefile` with the `pre-push`
  gate (#1).
- CI skeleton (`.github/workflows/ci.yml`): `fmt`, `clippy`, `test`, and
  `build-release` jobs on pinned runner and toolchain images with
  cancel-in-progress concurrency (#1).
- Design documentation for the planned backtester (`docs/`): PRD, roadmap,
  competitive analysis, domain model, engine architecture, data layer,
  execution models, analytics/reporting, Python bindings, and ADRs 0001–0006.

## [0.0.1] - 2026-07-12

### Added

- Name-reservation placeholder published to crates.io. No implementation code
  yet; `src/lib.rs` carries crate-level docs only. The roadmap begins at
  v0.1.0.

[Unreleased]: https://github.com/joaquinbejar/IronCondor/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/joaquinbejar/IronCondor/releases/tag/v0.0.1
