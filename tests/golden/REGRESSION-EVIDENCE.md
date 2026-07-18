# Determinism golden suite — caught-regression evidence (#50)

The v1.0 stability line requires that the determinism golden suite has **caught at
least one real regression** — a determinism suite that has never failed is a
claim, not a control ([milestone
050](../../milestones/v1.0-stability/050-determinism-golden-suite-hardening.md),
[docs/02 §7](../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
This file is that evidence: a real determinism regression was introduced on the
working tree (never committed), run against the extended suite, caught, and
reverted.

- **Date:** 2026-07-17
- **Suite:** `cargo test --test bundle_golden` (+ `--features orderbook`) — the
  #50 extension of the frozen `ironcondor.bundle.v1` goldens to every named
  scenario, under the single comparison oracle (`tests/oracle/mod.rs`) plus the
  same-environment run-twice byte-identical assertion.
- **src/ diff after the experiment:** none (the scratch change was reverted with
  `git checkout -- src/engine/backtest.rs`; this file is test-tree only).

## The regression (scratch, reverted)

Class: an **unstripped wall-clock read reaching a result** — the exact violation
the determinism contract forbids ("No wall clock in the loop … time is `SimTime`
from the feed", [docs/02
§7](../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)). It
mirrors a real bug this codebase carried and removed (the per-step reprice
`Utc::now()` leak, resolved at #19), so it is not a contrived failure.

The per-step fill/close timestamp `ts_ns` in `apply_step_fills`
(`src/engine/backtest.rs`) was changed from the deterministic feed time to the
host wall clock:

```diff
     let multiplier = snapshot.spec.contract_multiplier;
-    let ts_ns = snapshot.ts.value();
+    let ts_ns = std::time::SystemTime::now()
+        .duration_since(std::time::UNIX_EPOCH)
+        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
+        .unwrap_or_else(|_| snapshot.ts.value());
```

That `ts_ns` feeds the trade log's open/close timestamps, so the derived
`trade_statistics.average_holding_period` (a `metrics` field in the manifest)
becomes a function of when the run executed — non-deterministic across two runs
of the same `(seed, config, data)`.

## What caught it — the **byte layer**, not the logical layer

Running the extended suite (default features) with the scratch change:

```text
test test_bundle_golden_iron_condor_naive_write_read_equal ... ok
test test_bundle_golden_short_strangle_naive_write_read_equal ... ok
test test_bundle_run_twice_short_strangle_is_byte_identical ... FAILED
test test_bundle_run_twice_is_byte_identical ... FAILED

thread 'test_bundle_run_twice_is_byte_identical' panicked at tests/bundle_golden.rs:413:5:
assertion `left == right` failed: manifest.json must be identical after stripping created_utc + output_dir
  left:  … "trade_statistics": Object { … "average_holding_period": Number(6.712962962962962e-10) … }
  right: … "trade_statistics": Object { … "average_holding_period": Number(6.597222222222222e-10) … }
```

Under `--features orderbook`, the realistic scenario's run-twice test failed the
same way, so **all three** scenarios caught it:

```text
test test_bundle_run_twice_is_byte_identical ... FAILED
test test_bundle_run_twice_short_strangle_is_byte_identical ... FAILED
test realistic::test_bundle_run_twice_realistic_is_byte_identical ... FAILED
test result: FAILED. 16 passed; 3 failed
```

Every other assertion — the golden `write → read → equal` compares against the
committed `expected/` bundles, and the mode-pair schema test — **passed**.

### Which oracle layer, and why it matters

- **Byte layer (same-environment run-twice) — CAUGHT it.** The run-twice test
  compares `manifest.json` byte-for-byte after stripping only `created_utc`, so
  it keeps the `metrics` object. The two runs' `average_holding_period` differed
  in their low bits, so the assertion failed. This is precisely the property the
  contract assigns to the run-twice test ("a run-twice golden test guards it in
  CI", [docs/02
  §7](../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
- **Logical layer (cross-environment golden compare) — MISSED it, by design.**
  The comparison oracle deliberately treats `metrics` as a **versioned, opaque**
  object and strips it before comparing ([docs/05
  §12.5](../../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause),
  `oracle::strip_opaque_metrics`), because some of its fields serialise as bare
  `f64` that must not be compared for exact equality across environments. The
  four Parquet tables' own `ts_ns` columns are sourced separately and were
  unaffected, so `compare_bundle_tables` and `compare_manifest_json` both passed.

This is the load-bearing finding: a wall-clock determinism leak into a
`metrics`-only field is **invisible to the logical golden** and is caught **only
by the same-environment byte layer**. It is direct evidence that the run-twice
byte assertion is not redundant with the golden compare — it is the sole guard
for the opaque region of the manifest, which is exactly why the determinism
contract mandates both.

## Revert + green

The scratch change was reverted (`git checkout -- src/engine/backtest.rs`) and
the suite is green again:

```text
cargo test --test bundle_golden                     → 16 passed; 0 failed
cargo test --test bundle_golden --features orderbook → 19 passed; 0 failed
```
