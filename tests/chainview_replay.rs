//! **#48 — ChainView replay compatibility: the zero-conversion reconstruction
//! proof.** This test consumes the committed shared conformance fixture
//! ([`tests/fixtures/conformance/`](../tests/fixtures/conformance), #36) **exactly
//! as a renderer would** — through [`read_bundle`](ironcondor::read_bundle) only —
//! and proves that every ChainView replay-mode surface is reconstructable **from
//! the bundle alone**, with **no conversion step**:
//!
//! - **equity curve** — a plot-ready `(step, ts_ns, equity_cents, drawdown)`
//!   series read straight from `equity_curve.parquet`
//!   ([docs/05 §8](../docs/05-analytics-and-reporting.md#8-equity_curveparquet));
//! - **attribution** — the per-step decomposition in `greeks_attribution.parquet`
//!   reconciles to the equity-curve step delta **cross-table, in integer cents**
//!   ([docs/05 §3.1](../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)),
//!   which is what lets ChainView render the attribution panel without recomputing;
//! - **per-trade drill-down** — grouping `fills` + `positions` by
//!   `trade_id`/`position_id` reconstructs each leg's lifecycle (entry fills →
//!   per-step marks → terminal row / `exit_reason`, or `open_at_end`), and every
//!   join is **total** (no orphan rows)
//!   ([docs/05 §9](../docs/05-analytics-and-reporting.md#9-positionsparquet));
//! - **payoff diagram inputs** — each leg's `(style, strike_cents, side, quantity,
//!   entry premium)` recovers from `positions` + `fills` alone, so the condor
//!   payoff is computable without the engine.
//!
//! # Renderer stand-in — the import list is the enforcement
//!
//! This binary imports **only** [`read_bundle`](ironcondor::read_bundle), the
//! reader's [`ResourceLimits`]/[`ValidatedBundle`] gate, and the **frozen wire row
//! types** ([`FillRow`], [`PositionRow`], [`EquityPoint`],
//! [`GreeksAttributionRow`]). It imports **nothing** from `engine`, `analytics`,
//! `execution`, or the data-conversion layer, and it never re-runs pricing,
//! attribution, or metrics — it reads the four Parquet tables and the manifest and
//! reconstructs each surface from those bytes, the same read a ChainView
//! (`joaquinbejar/ChainView`) `BundleReader` performs. If a future edit needs an
//! engine/analytics import to make an assertion pass, that assertion was not
//! reconstructable from the bundle and the change is wrong.
//!
//! # Cross-repo status (honest, today)
//!
//! ChainView already ships a bundle **reader** (`src/replay/{mod,tables,validate}.rs`,
//! its issues #29–#31) that parses `ironcondor.bundle.v1`, but it has **no
//! committed copy of this shared fixture yet** — its replay
//! integration/goldens milestone (ChainView #37 (render goldens; the compat freeze is its #56)) is where the identical bytes
//! land. So "ChainView loads the identical fixture" today means: this fixture is
//! **copy-loadable** into ChainView's documented `tests/fixtures/bundle/valid/`
//! path unchanged, and the ChainView-side load + render verification is a **user
//! action** to run when that reader path is wired (see the PR body and
//! [`tests/fixtures/conformance/README.md`](../tests/fixtures/conformance/README.md)).
//! This binary is the **producer-side** half of that proof; ironcondor never
//! renders (anti-roadmap).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ironcondor::{
    EquityPoint, FillRow, GreeksAttributionRow, PositionRow, ResourceLimits, ValidatedBundle,
    read_bundle,
};

/// The `trade_id` every leg of the single condor trade shares (the fixture's one
/// trade group, [docs/05 §12.4](../docs/05-analytics-and-reporting.md#124-identifier-grammars-producer-defined-delimiter-safe)).
const CONDOR_TRADE_ID: u64 = 1;

/// The step at which the short put is closed (a `ManualClose`).
const CLOSE_STEP: u32 = 2;

/// The per-leg contract count the fixture opens (a 5-lot condor).
const LEG_LOTS: u32 = 5;

/// Tolerance for the sole float column (`drawdown`) — the fixed cross-environment
/// float discipline ([docs/05 §12.5](../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause));
/// a peak marks `drawdown == 0.0` exactly, so a tiny positive slack keeps `+0.0`
/// inside the `≤ 0` contract without an exact float compare.
const DRAWDOWN_TOL: f64 = 1e-9;

/// The four condor legs as `(strike_cents, style, side)` — the payoff-input set a
/// renderer must recover from the bundle alone.
const EXPECTED_LEGS: [(u64, &str, &str); 4] = [
    (510_000, "call", "short"), // short call — body
    (520_000, "call", "long"),  // long call — wing
    (490_000, "put", "short"),  // short put — body (closed mid-run)
    (480_000, "put", "long"),   // long put — wing
];

/// Absolute path to the committed shared conformance fixture directory.
fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/conformance")
}

/// Read the committed conformance fixture through the reader security gate — the
/// **same** read a ChainView renderer performs (no engine, no conversion).
fn read_fixture() -> ValidatedBundle {
    let Ok(bundle) = read_bundle(fixture_dir(), &ResourceLimits::default()) else {
        panic!(
            "the committed conformance fixture must read back through read_bundle \
             (regenerate with: BLESS=1 cargo test --features orderbook --test conformance)"
        );
    };
    bundle
}

/// Parse a canonical `contract_id` back into `(strike_cents, style_char)` the way
/// a renderer does — split on the `:` delimiter of
/// `"v1:{UNDERLYING}:{expiration_ns}:{strike_cents}:{style}"`, `style ∈ {C, P}`
/// ([docs/05 §12.4](../docs/05-analytics-and-reporting.md#124-identifier-grammars-producer-defined-delimiter-safe)).
/// `UNDERLYING` is colon-free by grammar, so the split has exactly five fields.
fn parse_contract_id(contract_id: &str) -> Option<(u64, char)> {
    let (_expiration_ns, strike_cents, style_char) = parse_contract_id_parts(contract_id)?;
    Some((strike_cents, style_char))
}

/// The same parse, keeping the `expiration_ns` field too — what a renderer needs
/// to group a multi-expiration position (a diagonal, a calendar) by expiry.
fn parse_contract_id_parts(contract_id: &str) -> Option<(i64, u64, char)> {
    let mut parts = contract_id.split(':');
    if parts.next()? != "v1" {
        return None;
    }
    let _underlying = parts.next()?;
    let expiration_ns = parts.next()?.parse::<i64>().ok()?;
    let strike_cents = parts.next()?.parse::<u64>().ok()?;
    let style = parts.next()?;
    if parts.next().is_some() {
        return None; // more than five fields ⇒ not the v1 grammar
    }
    let style_char = match style {
        "C" => 'C',
        "P" => 'P',
        _ => return None,
    };
    Some((expiration_ns, strike_cents, style_char))
}

/// The lower-case style string a `C`/`P` id char maps to (`fills.style` grammar).
fn style_str(style_char: char) -> Option<&'static str> {
    match style_char {
        'C' => Some("call"),
        'P' => Some("put"),
        _ => None,
    }
}

/// The opposite side string — a close fill flips the leg's opening side.
fn opposite_side(side: &str) -> &'static str {
    match side {
        "long" => "short",
        "short" => "long",
        _ => "?",
    }
}

/// The exact left-hand side of the reconciliation identity for one attribution
/// row — `theta + delta + vega + spread_capture − fees + residual`, in checked
/// integer cents ([docs/05 §3.1](../docs/05-analytics-and-reporting.md#31-reconciliation-identity-vs-model-quality--two-separate-things)).
fn attribution_sum(row: &GreeksAttributionRow) -> Option<i64> {
    row.theta_pnl_cents
        .checked_add(row.delta_pnl_cents)?
        .checked_add(row.vega_pnl_cents)?
        .checked_add(row.spread_capture_cents)?
        .checked_sub(row.fees_cents)?
        .checked_add(row.residual_cents)
}

// ---------------------------------------------------------------------------
// Surface 1 — the equity curve, plot-ready from `equity_curve.parquet` alone.
// ---------------------------------------------------------------------------

/// ChainView's equity-curve panel reads `(step, ts_ns, equity_cents, drawdown)`
/// straight from `equity_curve.parquet` — no gaps, monotone steps and time, and
/// the valuation identity holds per row. All reconstructed from the bundle alone.
#[test]
fn test_equity_curve_is_plot_ready_from_the_bundle_alone() {
    let bundle = read_fixture();
    let curve = &bundle.equity_curve;

    // Consumer role: the plotted length equals the manifest's declared count
    // ([docs/05 §12.5](../docs/05-analytics-and-reporting.md#125-equality-oracle-and-the-metrics-clause)).
    // Cross the u64 count into usize with a checked conversion, never an `as` cast.
    let Ok(equity_len) = u64::try_from(curve.len()) else {
        panic!("equity-curve length fits u64");
    };
    assert_eq!(
        equity_len, bundle.manifest.row_counts.equity_curve,
        "the equity series length equals row_counts.equity_curve (no gaps)"
    );
    assert!(!curve.is_empty(), "the fixture has at least one step");

    // A plot-ready series a renderer can hand straight to a chart widget.
    let series: Vec<(u32, i64, i64, f64)> = curve
        .iter()
        .map(|p| (p.step, p.ts_ns, p.equity_cents, p.drawdown))
        .collect();

    let mut prev_ts: Option<i64> = None;
    for (i, point) in curve.iter().enumerate() {
        // Steps are a contiguous 0..N (monotone, no gap) — the x-axis.
        let Ok(expected_step) = u32::try_from(i) else {
            panic!("step index fits u32");
        };
        assert_eq!(
            point.step, expected_step,
            "equity_curve is a contiguous 0..N by step (no gaps)"
        );
        // Time is strictly increasing — a monotone replay timeline.
        if let Some(prev) = prev_ts {
            assert!(
                point.ts_ns > prev,
                "ts_ns is strictly increasing across steps (step {})",
                point.step
            );
        }
        prev_ts = Some(point.ts_ns);
        // The valuation identity is reconstructable per row: equity = cash + value.
        let Some(equity) = point.cash_cents.checked_add(point.position_value_cents) else {
            panic!(
                "cash + position_value must not overflow at step {}",
                point.step
            );
        };
        assert_eq!(
            point.equity_cents, equity,
            "equity_cents == cash_cents + position_value_cents (step {})",
            point.step
        );
        // The sole float column: finite and ≤ 0, never clamped, plottable as-is.
        assert!(
            point.drawdown.is_finite(),
            "drawdown is finite at step {} (a NaN/Inf is a load error)",
            point.step
        );
        assert!(
            point.drawdown <= DRAWDOWN_TOL,
            "drawdown is ≤ 0 at step {} (a peak is 0.0)",
            point.step
        );
    }

    assert_eq!(
        series.len(),
        curve.len(),
        "the plot-ready series covers every step"
    );
}

// ---------------------------------------------------------------------------
// Surface 2 — attribution reconciles to the equity step delta, cross-table.
// ---------------------------------------------------------------------------

/// ChainView's attribution panel renders the per-step Greek decomposition. The
/// proof that it needs **no recompute** is the cross-table identity: for every
/// step, `theta + delta + vega + spread_capture − fees + residual` (from
/// `greeks_attribution.parquet`) equals the equity-curve step delta (from
/// `equity_curve.parquet`), in exact integer cents — step 0 against
/// `initial_capital` from the manifest, later steps against the previous equity.
#[test]
fn test_attribution_reconciles_to_the_equity_step_delta_per_row() {
    let bundle = read_fixture();
    let curve = &bundle.equity_curve;
    let greeks = &bundle.greeks_attribution;

    // One attribution row per equity step, aligned 0..N.
    assert_eq!(
        greeks.len(),
        curve.len(),
        "greeks_attribution has one row per equity-curve step"
    );
    let Ok(greeks_len) = u64::try_from(greeks.len()) else {
        panic!("attribution length fits u64");
    };
    assert_eq!(
        greeks_len, bundle.manifest.row_counts.greeks_attribution,
        "the attribution length equals row_counts.greeks_attribution"
    );

    // Step 0's baseline is the manifest's initial_capital — from the bundle alone.
    let Ok(initial_capital) = i64::try_from(bundle.manifest.config.initial_capital) else {
        panic!("initial_capital fits i64 cents");
    };

    for (i, row) in greeks.iter().enumerate() {
        let Some(point) = curve.get(i) else {
            panic!("every attribution row has its equity twin at index {i}");
        };
        assert_eq!(row.step, point.step, "attribution/equity steps align");
        // Cross-table: the two tables agree on this step's timestamp.
        assert_eq!(
            row.ts_ns, point.ts_ns,
            "attribution and equity share ts_ns at step {}",
            row.step
        );
        // The subtracted-fees term is always a non-negative magnitude.
        assert!(
            row.fees_cents >= 0,
            "fees are a non-negative magnitude at step {}",
            row.step
        );

        // step_pnl = equity_n − equity_{n-1}; for step 0, equity_0 − initial_capital.
        let prev_equity = if i == 0 {
            initial_capital
        } else {
            let Some(prev) = curve.get(i - 1) else {
                panic!("a non-zero step has a predecessor at index {}", i - 1);
            };
            prev.equity_cents
        };
        let Some(step_pnl) = point.equity_cents.checked_sub(prev_equity) else {
            panic!("step_pnl must not overflow at step {}", row.step);
        };
        let Some(decomposition) = attribution_sum(row) else {
            panic!(
                "the attribution terms must not overflow at step {}",
                row.step
            );
        };
        // The reconciliation identity, cross-table, in exact integer cents.
        assert_eq!(
            decomposition, step_pnl,
            "attribution reconciles to the equity step delta at step {} \
             (θ+Δ+V+spread−fees+residual == step_pnl)",
            row.step
        );
    }
}

// ---------------------------------------------------------------------------
// Surface 3 — per-trade drill-down: totally joined fills + positions.
// ---------------------------------------------------------------------------

/// ChainView's per-trade drill-down groups `fills` + `positions` by
/// `trade_id`/`position_id` and reconstructs each leg's lifecycle. The proof it
/// needs is that the joins are **total** — every fill's position exists, every
/// leg has an entry fill and a contiguous run of per-step marks, every closed leg
/// has a matching close fill on its terminal step, and every `open_at_end` leg
/// has no close — all reconstructed from the bundle alone.
#[test]
fn test_per_trade_drilldown_joins_are_total() {
    let bundle = read_fixture();
    let run_id = &bundle.manifest.run_id;

    // Every row shares the one condor trade_id, and every fill's join key
    // (strategy_run_id) equals the manifest run_id — the referential check a
    // consumer runs before it uses run_id as a key.
    for f in &bundle.fills {
        assert_eq!(
            f.trade_id, CONDOR_TRADE_ID,
            "every fill shares the trade_id"
        );
        assert_eq!(
            &f.strategy_run_id, run_id,
            "every fill's strategy_run_id == manifest.run_id"
        );
    }
    for p in &bundle.positions {
        assert_eq!(
            p.trade_id, CONDOR_TRADE_ID,
            "every position shares the trade_id"
        );
    }

    // Group by leg (BTreeMap ⇒ deterministic iteration; read_bundle already
    // sorted rows, so each leg's slices stay in step order).
    let mut leg_positions: BTreeMap<u64, Vec<&PositionRow>> = BTreeMap::new();
    for p in &bundle.positions {
        leg_positions.entry(p.position_id).or_default().push(p);
    }
    let mut leg_fills: BTreeMap<u64, Vec<&FillRow>> = BTreeMap::new();
    for f in &bundle.fills {
        leg_fills.entry(f.position_id).or_default().push(f);
    }

    // The condor has exactly four legs.
    assert_eq!(
        leg_positions.len(),
        4,
        "the condor drill-down has four legs (four position_ids)"
    );

    // No orphan fills: every fill's position exists in `positions`.
    for position_id in leg_fills.keys() {
        assert!(
            leg_positions.contains_key(position_id),
            "fill position_id {position_id} has a matching position (no orphan fill)"
        );
    }

    let mut closed_legs = 0usize;
    let mut open_at_end_legs = 0usize;

    for (position_id, rows) in &leg_positions {
        let Some(first) = rows.first() else {
            panic!("leg {position_id} has at least one row");
        };
        let Some(last) = rows.last() else {
            panic!("leg {position_id} has a last row");
        };
        let leg_side = first.side.as_str();
        let entry_step = first.step;

        // Side and contract_id are stable across the leg's whole lifetime.
        for row in rows {
            assert_eq!(row.side, first.side, "leg {position_id} side is stable");
            assert_eq!(
                row.contract_id, first.contract_id,
                "leg {position_id} contract_id is stable"
            );
            assert_eq!(
                row.trade_id, CONDOR_TRADE_ID,
                "leg {position_id} row shares the trade_id"
            );
        }

        // Per-step marks are a contiguous run [entry_step, last_step] — one mark
        // per step, no gap, so a renderer can draw the leg's mark timeline.
        let steps: Vec<u32> = rows.iter().map(|r| r.step).collect();
        for pair in steps.windows(2) {
            let [a, b] = pair else {
                panic!("windows(2) yields pairs");
            };
            let Some(next) = a.checked_add(1) else {
                panic!("leg {position_id} step increment must not overflow");
            };
            assert_eq!(
                *b, next,
                "leg {position_id} per-step marks are contiguous (no gap)"
            );
        }

        // The leg has ≥ 1 entry fill at its entry step, all on the leg's own side.
        let Some(fills) = leg_fills.get(position_id) else {
            panic!("leg {position_id} has at least one entry fill (no orphan position)");
        };
        let entry_fills: Vec<&&FillRow> = fills.iter().filter(|f| f.step == entry_step).collect();
        assert!(
            !entry_fills.is_empty(),
            "leg {position_id} has an entry fill at its entry step {entry_step}"
        );
        for f in &entry_fills {
            assert_eq!(
                f.side, leg_side,
                "leg {position_id} entry fill opens on the leg side"
            );
            assert_eq!(
                f.contract_id, first.contract_id,
                "leg {position_id} entry fill joins to the leg contract_id"
            );
        }

        // Terminal vs open_at_end — exactly one of the two closes the lifecycle.
        let is_closed = last.exit_reason.is_some();
        if is_closed {
            closed_legs += 1;
            assert_eq!(
                last.exit_reason.as_deref(),
                Some("manual_close"),
                "the closed leg carries the ManualClose exit_reason"
            );
            assert_eq!(last.step, CLOSE_STEP, "the close lands on its step");
            assert!(!last.open_at_end, "a closed leg is not open_at_end");
            // exit_reason is null on every non-terminal row of the leg.
            for row in &rows[..rows.len().saturating_sub(1)] {
                assert!(
                    row.exit_reason.is_none(),
                    "leg {position_id} is null exit_reason while open"
                );
            }
            // A matching close fill exists on the terminal step, flipping the side.
            let close_fills: Vec<&&FillRow> =
                fills.iter().filter(|f| f.step == last.step).collect();
            assert!(
                !close_fills.is_empty(),
                "closed leg {position_id} has a matching close fill on its terminal step"
            );
            for f in &close_fills {
                assert_eq!(
                    f.side,
                    opposite_side(leg_side),
                    "leg {position_id} close fill flips the opening side"
                );
            }
        } else {
            open_at_end_legs += 1;
            assert!(
                last.open_at_end,
                "an un-closed leg's last row is open_at_end"
            );
            // No exit_reason anywhere, and every fill is an entry fill (no close).
            for row in rows {
                assert!(
                    row.exit_reason.is_none(),
                    "open_at_end leg {position_id} never carries an exit_reason"
                );
            }
            for f in fills {
                assert_eq!(
                    f.step, entry_step,
                    "open_at_end leg {position_id} has only entry fills"
                );
                assert_eq!(
                    f.side, leg_side,
                    "open_at_end leg {position_id} fills stay on the leg side"
                );
            }
        }
    }

    // The lifecycle the fixture pins: ≥ 1 closed leg, ≥ 1 open_at_end leg.
    assert!(
        closed_legs >= 1,
        "at least one leg is closed with an exit_reason"
    );
    assert!(
        open_at_end_legs >= 1,
        "at least one leg is left open_at_end"
    );
    assert_eq!(
        closed_legs + open_at_end_legs,
        4,
        "every leg ends either closed or open_at_end"
    );
}

// ---------------------------------------------------------------------------
// Surface 4 — payoff diagram inputs recover the four condor legs exactly.
// ---------------------------------------------------------------------------

/// ChainView's payoff diagram needs each leg's `(style, strike_cents, side,
/// quantity, entry premium)`. The proof is that those recover **exactly** from
/// `positions` + `fills` alone — the condor payoff is computable without the
/// engine — and the recovered structure is a well-formed iron condor (two calls,
/// two puts, long wings outside short body).
#[test]
fn test_payoff_inputs_reconstruct_the_four_condor_legs() {
    let bundle = read_fixture();

    // Group positions per leg; take each leg's entry (first) row for its payoff
    // inputs (avg_price_cents is the average entry premium).
    let mut leg_positions: BTreeMap<u64, Vec<&PositionRow>> = BTreeMap::new();
    for p in &bundle.positions {
        leg_positions.entry(p.position_id).or_default().push(p);
    }
    let mut leg_fills: BTreeMap<u64, Vec<&FillRow>> = BTreeMap::new();
    for f in &bundle.fills {
        leg_fills.entry(f.position_id).or_default().push(f);
    }

    // Reconstructed payoff-input tuples: (strike_cents, style, side).
    let mut recovered: Vec<(u64, String, String)> = Vec::new();
    let mut calls = 0usize;
    let mut puts = 0usize;
    let mut shorts = 0usize;
    let mut longs = 0usize;
    let mut short_call_strike: Option<u64> = None;
    let mut long_call_strike: Option<u64> = None;
    let mut short_put_strike: Option<u64> = None;
    let mut long_put_strike: Option<u64> = None;

    for (position_id, rows) in &leg_positions {
        let Some(entry) = rows.first() else {
            panic!("leg {position_id} has an entry row");
        };
        // Parse the leg identity from its contract_id (renderer-style).
        let Some((strike_cents, style_char)) = parse_contract_id(&entry.contract_id) else {
            panic!("leg {position_id} contract_id parses to (strike, style)");
        };
        let Some(style) = style_str(style_char) else {
            panic!("leg {position_id} style char is C|P");
        };

        // The payoff inputs are present and well-formed.
        assert_eq!(
            entry.quantity, LEG_LOTS,
            "leg {position_id} carries the 5-lot quantity"
        );
        assert!(
            entry.avg_price_cents > 0,
            "leg {position_id} has a positive entry premium (avg_price_cents)"
        );

        // Cross-check the parsed identity against the fills' explicit
        // strike_cents / style columns — the id round-trips to the row columns.
        let Some(fills) = leg_fills.get(position_id) else {
            panic!("leg {position_id} has fills to cross-check");
        };
        for f in fills {
            assert_eq!(
                f.strike_cents, strike_cents,
                "leg {position_id} fill strike matches its contract_id"
            );
            assert_eq!(
                f.style, style,
                "leg {position_id} fill style matches its contract_id"
            );
        }

        recovered.push((strike_cents, style.to_string(), entry.side.clone()));

        match (style, entry.side.as_str()) {
            ("call", "short") => {
                calls += 1;
                shorts += 1;
                short_call_strike = Some(strike_cents);
            }
            ("call", "long") => {
                calls += 1;
                longs += 1;
                long_call_strike = Some(strike_cents);
            }
            ("put", "short") => {
                puts += 1;
                shorts += 1;
                short_put_strike = Some(strike_cents);
            }
            ("put", "long") => {
                puts += 1;
                longs += 1;
                long_put_strike = Some(strike_cents);
            }
            other => panic!("unexpected leg shape {other:?}"),
        }
    }

    // The four legs recover exactly (order-independent set equality).
    let mut recovered_sorted = recovered.clone();
    recovered_sorted.sort_unstable();
    let mut expected: Vec<(u64, String, String)> = EXPECTED_LEGS
        .iter()
        .map(|(strike, style, side)| (*strike, (*style).to_string(), (*side).to_string()))
        .collect();
    expected.sort_unstable();
    assert_eq!(
        recovered_sorted, expected,
        "the four condor legs recover exactly from positions + fills"
    );

    // The recovered structure is a well-formed iron condor payoff.
    assert_eq!(calls, 2, "two call legs");
    assert_eq!(puts, 2, "two put legs");
    assert_eq!(shorts, 2, "two short (body) legs");
    assert_eq!(longs, 2, "two long (wing) legs");
    let (Some(sc), Some(lc), Some(sp), Some(lp)) = (
        short_call_strike,
        long_call_strike,
        short_put_strike,
        long_put_strike,
    ) else {
        panic!("all four condor legs are present");
    };
    assert!(
        sc < lc,
        "the long call wing is above the short call body ({sc} < {lc})"
    );
    assert!(
        sp > lp,
        "the long put wing is below the short put body ({sp} > {lp})"
    );
}

// ---------------------------------------------------------------------------
// A small guard on the contract_id renderer-parser used by the surfaces above.
// ---------------------------------------------------------------------------

/// The renderer-style `contract_id` parser accepts the v1 grammar and rejects
/// malformed / wrong-arity / bad-style ids, so a surface never joins on a bad key.
#[test]
fn test_contract_id_parser_accepts_v1_and_rejects_malformed() {
    let ok = parse_contract_id("v1:SPX:1752883200000000000:510000:C");
    assert_eq!(ok, Some((510_000, 'C')), "the v1 grammar parses");
    let ok_put = parse_contract_id("v1:BRK.B:1:480000:P");
    assert_eq!(ok_put, Some((480_000, 'P')), "dotted underlying parses");

    assert!(
        parse_contract_id("v2:SPX:1:1:C").is_none(),
        "a wrong format prefix is rejected"
    );
    assert!(
        parse_contract_id("v1:SPX:1:1:C:extra").is_none(),
        "extra trailing field is rejected"
    );
    assert!(
        parse_contract_id("v1:SPX:1:notanumber:C").is_none(),
        "a non-numeric strike is rejected"
    );
    assert!(
        parse_contract_id("v1:SPX:1:1:X").is_none(),
        "a non-C/P style is rejected"
    );
    assert!(
        parse_contract_id("v1:SPX:1:1").is_none(),
        "a truncated id is rejected"
    );
}

/// Compile-time-visible guard that the surfaces only ever touch the wire row
/// types (no engine/analytics type appears in a signature here). This function
/// is never called; it exists so a reviewer sees the whole reconstruction
/// vocabulary is `{FillRow, PositionRow, EquityPoint, GreeksAttributionRow}`.
#[allow(dead_code)]
fn wire_types_only(_f: &FillRow, _p: &PositionRow, _e: &EquityPoint, _g: &GreeksAttributionRow) {}

// ---------------------------------------------------------------------------
// Surface 5 — the same surfaces over a MULTI-EXPIRATION leg-set bundle (#117).
// ---------------------------------------------------------------------------

/// The replay surfaces over a `StrategySpec::Legs` bundle: the position shape no
/// named strategy spec can describe — four legs across **two** expirations.
///
/// The point of this module is that ChainView needs **nothing new** for it: the
/// same [`read_bundle`] call, the same four tables, the same wire row types. The
/// bundle read here is the committed `legs_multi_expiry_naive` golden, so the
/// frozen bytes and the replay contract are proven against one artifact.
mod legs {
    use std::collections::BTreeMap;

    use ironcondor::{PositionRow, ResourceLimits, ValidatedBundle, read_bundle};

    use super::{DRAWDOWN_TOL, attribution_sum, parse_contract_id_parts, style_str};

    /// The four legs of the golden leg set as `(strike_cents, style, side)` and
    /// the expiry bucket (`0` = near, `1` = far) each sits in.
    const EXPECTED_LEGS: [(u64, &str, &str, usize); 4] = [
        (510_000, "call", "short", 0), // near-expiry body
        (490_000, "put", "short", 0),  // near-expiry body
        (520_000, "call", "long", 1),  // far-expiry wing
        (480_000, "put", "long", 1),   // far-expiry wing
    ];

    /// Read the committed multi-expiration leg-set golden bundle — the same read
    /// a ChainView renderer performs (no engine, no conversion).
    fn read_legs_bundle() -> ValidatedBundle {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/legs_multi_expiry_naive/expected");
        let Ok(bundle) = read_bundle(dir, &ResourceLimits::default()) else {
            panic!(
                "the committed leg-set golden must read back through read_bundle \
                 (regenerate with: BLESS=1 cargo test --test bundle_golden)"
            );
        };
        bundle
    }

    #[test]
    fn test_legs_bundle_equity_curve_is_plot_ready() {
        let bundle = read_legs_bundle();
        let curve = &bundle.equity_curve;
        let Ok(len) = u64::try_from(curve.len()) else {
            panic!("equity-curve length fits u64");
        };
        assert_eq!(len, bundle.manifest.row_counts.equity_curve);
        assert!(!curve.is_empty(), "the leg-set golden has steps");

        let mut prev_ts: Option<i64> = None;
        for (i, point) in curve.iter().enumerate() {
            let Ok(expected_step) = u32::try_from(i) else {
                panic!("step index fits u32");
            };
            assert_eq!(point.step, expected_step, "contiguous 0..N by step");
            if let Some(prev) = prev_ts {
                assert!(point.ts_ns > prev, "ts_ns strictly increases");
            }
            prev_ts = Some(point.ts_ns);
            let Some(equity) = point.cash_cents.checked_add(point.position_value_cents) else {
                panic!(
                    "cash + position_value must not overflow at step {}",
                    point.step
                );
            };
            assert_eq!(
                point.equity_cents, equity,
                "equity == cash + position value"
            );
            assert!(point.drawdown.is_finite(), "drawdown is finite");
            assert!(point.drawdown <= DRAWDOWN_TOL, "drawdown is <= 0");
        }
    }

    #[test]
    fn test_legs_bundle_attribution_reconciles_per_row() {
        let bundle = read_legs_bundle();
        let curve = &bundle.equity_curve;
        let greeks = &bundle.greeks_attribution;
        assert_eq!(greeks.len(), curve.len(), "one attribution row per step");

        let Ok(initial_capital) = i64::try_from(bundle.manifest.config.initial_capital) else {
            panic!("initial_capital fits i64 cents");
        };
        for (i, row) in greeks.iter().enumerate() {
            let Some(point) = curve.get(i) else {
                panic!("every attribution row has its equity twin at index {i}");
            };
            assert_eq!(row.step, point.step, "attribution/equity steps align");
            assert_eq!(row.ts_ns, point.ts_ns, "the two tables share ts_ns");
            let prev_equity = if i == 0 {
                initial_capital
            } else {
                let Some(prev) = curve.get(i - 1) else {
                    panic!("a non-zero step has a predecessor at index {}", i - 1);
                };
                prev.equity_cents
            };
            let Some(step_pnl) = point.equity_cents.checked_sub(prev_equity) else {
                panic!("step_pnl must not overflow at step {}", row.step);
            };
            let Some(decomposition) = attribution_sum(row) else {
                panic!(
                    "the attribution terms must not overflow at step {}",
                    row.step
                );
            };
            assert_eq!(
                decomposition, step_pnl,
                "attribution reconciles to the equity step delta at step {}",
                row.step
            );
        }
    }

    #[test]
    fn test_legs_bundle_joins_are_total() {
        let bundle = read_legs_bundle();
        let run_id = &bundle.manifest.run_id;
        let mut leg_rows: BTreeMap<u64, Vec<&PositionRow>> = BTreeMap::new();
        for p in &bundle.positions {
            leg_rows.entry(p.position_id).or_default().push(p);
        }
        assert_eq!(leg_rows.len(), EXPECTED_LEGS.len(), "one group per leg");

        for f in &bundle.fills {
            assert_eq!(
                f.strategy_run_id,
                run_id.as_str(),
                "every fill joins the manifest run_id"
            );
            assert!(
                leg_rows.contains_key(&f.position_id),
                "every fill's position exists in positions.parquet"
            );
        }
    }

    #[test]
    fn test_legs_bundle_payoff_inputs_recover_four_legs_at_two_expirations() {
        // The claim the variant exists for: a renderer recovers each leg's
        // identity — INCLUDING its own expiration — from the bundle alone, and
        // the recovered set spans two distinct expiries.
        let bundle = read_legs_bundle();
        let mut leg_rows: BTreeMap<u64, Vec<&PositionRow>> = BTreeMap::new();
        for p in &bundle.positions {
            leg_rows.entry(p.position_id).or_default().push(p);
        }

        let mut recovered: Vec<(i64, u64, String, String)> = Vec::new();
        for (position_id, rows) in &leg_rows {
            let Some(entry) = rows.first() else {
                panic!("leg {position_id} has an entry row");
            };
            let Some((expiration_ns, strike_cents, style_char)) =
                parse_contract_id_parts(&entry.contract_id)
            else {
                panic!("leg {position_id} contract_id parses to (expiry, strike, style)");
            };
            let Some(style) = style_str(style_char) else {
                panic!("leg {position_id} style char is C|P");
            };
            assert!(
                entry.avg_price_cents > 0,
                "leg {position_id} has a positive entry premium"
            );
            recovered.push((
                expiration_ns,
                strike_cents,
                style.to_string(),
                entry.side.clone(),
            ));
        }

        // Two distinct expiries, two legs each — the shape a single
        // strategy-level `expiration` cannot describe.
        let mut expiries: Vec<i64> = recovered.iter().map(|(e, _, _, _)| *e).collect();
        expiries.sort_unstable();
        expiries.dedup();
        assert_eq!(
            expiries.len(),
            2,
            "the leg set spans two distinct expirations"
        );

        for (strike, style, side, bucket) in EXPECTED_LEGS {
            let Some(expected_expiry) = expiries.get(bucket) else {
                panic!("expiry bucket {bucket} exists");
            };
            assert!(
                recovered.iter().any(|(e, s, st, sd)| e == expected_expiry
                    && *s == strike
                    && st == style
                    && sd == side),
                "the {side} {style} at {strike} is recovered in expiry bucket {bucket}"
            );
        }
    }

    #[test]
    fn test_legs_bundle_manifest_declares_the_legs_kind() {
        // A renderer that groups by strategy kind reads it from the manifest —
        // the leg set is a first-class kind on the wire, not a special case.
        let bundle = read_legs_bundle();
        let Ok(json) = serde_json::to_value(&bundle.manifest.strategy) else {
            panic!("the manifest strategy serialises");
        };
        let Some(obj) = json.as_object() else {
            panic!("the strategy record is a JSON object");
        };
        assert!(obj.contains_key("Legs"), "the manifest carries a leg set");
    }
}
