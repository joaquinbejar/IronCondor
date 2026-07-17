# Shared conformance fixture — `ironcondor.bundle.v1`

This directory is the **single shared conformance bundle** for the IronCondor ↔
[ChainView](https://github.com/joaquinbejar/ChainView) replay contract. It is
produced by the **real bundle writer** (a realistic thin-book iron-condor run,
`tests/conformance.rs`, feature `orderbook`) and frozen at the v0.3 schema
freeze (#36). It is `manifest.json` plus the four Parquet tables:

```
manifest.json               # run metadata + metrics + row_counts
fills.parquet               # 10 rows — 4 legs opened (multi-level) + 1 close
equity_curve.parquet        #  5 rows — one per step, 0..4
positions.parquet           # 18 rows — per-step leg marks + 1 terminal row
greeks_attribution.parquet  #  5 rows — one per step, 0..4
```

It exercises the full trade lifecycle the contract matrix pins
([docs/05 §12.6](../../../docs/05-analytics-and-reporting.md#126-shared-conformance-fixture)):
a four-leg condor sharing one `trade_id`, the short put (`490000:P`) closed
mid-run with a non-null `exit_reason` (`manual_close`), the other three legs
left `open_at_end`, and multi-level realistic fills exercising the
`(step, order_id, fill_seq)` unique key.

## What consumes it here (IronCondor, producer side)

- `tests/conformance.rs` — asserts every cell of the
  [docs/05 §12](../../../docs/05-analytics-and-reporting.md#12-producer-side-contract-matrix-ironcondorbundlev1)
  matrix (manifest fields, table shapes, identifier grammar, lifecycle).
- `tests/chainview_replay.rs` (#48) — the **zero-conversion reconstruction
  proof**: reads this bundle through `read_bundle` **only** (no engine /
  analytics / conversion imports) and reconstructs each ChainView surface —
  equity curve, attribution (the cross-table reconciliation identity), per-trade
  drill-down (total joins), and payoff inputs — **from the bundle alone**.

Both regenerate identically with:

```
BLESS=1 cargo test --features orderbook --test conformance
```

## Copy-loadable into ChainView (consumer side — user action)

ChainView already ships a bundle **reader** (`src/replay/{mod,tables,validate}.rs`)
that parses `ironcondor.bundle.v1`, but has **no committed copy of this fixture
yet** (its replay integration/goldens milestone is where the identical bytes
land). This directory is **copy-loadable unchanged** into ChainView's documented
fixture path:

```
ChainView/tests/fixtures/bundle/valid/
```

Copy these five files there verbatim (no conversion). ChainView's replay tests
should then assert, against the identical bytes:

1. `BundleReader::open` accepts the `schema` tag `ironcondor.bundle.v1`.
2. `row_counts` cross-checks the decoded table lengths
   (`{fills:10, equity_curve:5, positions:18, greeks_attribution:5}`).
3. The equity valuation identity and the **cross-table attribution
   reconciliation** hold per step (`θ+Δ+V+spread−fees+residual == step_pnl`,
   `step_pnl₀` against `initial_capital`).
4. `run_id` is an opaque string key and `fills.strategy_run_id == manifest.run_id`.
5. The near-boundary float oracle produces identical pass/fail on both sides.

The ChainView-side **load + render** verification (equity curve, attribution
panel, drill-down, payoff diagram) is the outstanding **user action** to run
once that reader path is wired; IronCondor never renders (anti-roadmap).

## Do not hand-edit

Any change to these bytes is a schema event: it bumps the `schema` tag, updates
`docs/SEMVER.md` + `docs/05-analytics-and-reporting.md`, regenerates the golden,
and is mirrored into ChainView the same week. Regenerate via `BLESS=1`, never by
hand.
