//! Bundle-writer scaling bench — hot path **H4**/PB-5 (issue #37).
//!
//! Times [`ironcondor::write_bundle`] — the encode of the four Parquet tables
//! (`fills` / `equity_curve` / `positions` / `greeks_attribution`) plus the
//! `manifest.json`, published atomically — over [`BacktestRun`]s of **growing
//! length**, to validate PB-5: the writer is an **O(rows) single-pass** encode
//! whose wall-time grows **linearly** (not quadratically) with the number of
//! rows written
//! ([docs/07 §3](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
//!
//! This bench lands the **time** measurement; the companion peak-memory
//! measurement + the PB-5 linear/bounded assertions live in
//! `tests/bundle_write_pb5.rs` (a counting `GlobalAlloc` measures the writer's
//! peak allocation high-water — a byte quantity that is build-profile
//! independent, so it does not belong in this optimized timing bench). The CI
//! regression gate that wraps both is v1.0 work (#51), out of scope here.
//!
//! # Why build the run directly (not `run_backtest`)
//!
//! PB-5 governs the **writer**, not the replay loop (that is PB-2, measured in
//! `naive_throughput.rs`). To isolate the writer this bench synthesises a
//! [`BacktestRun`] of the requested length in **setup** (un-timed) and times
//! **only** [`write_bundle`]. The run carries the realistic four-condor-leg
//! shape: `positions.parquet` gets `4 · steps` rows (the dominant streaming
//! table), `equity_curve` / `greeks_attribution` get `steps` rows each, and
//! `fills` a small constant — so total rows ≈ `6 · steps`.
//!
//! # Sizes — straddling the row-group boundary
//!
//! The writer encodes each table in fixed-size [`WRITE_BATCH_ROWS`]` = 8192`
//! Arrow row-group batches. The step counts below are chosen so the dominant
//! `positions` table lands at `1 024 / 8 192 / 32 768 / 131 072` rows — i.e.
//! **1/8×, 1×, 4×, and 16×** one row-group batch — so the streaming path is
//! actually exercised across the boundary (from below one batch to sixteen),
//! not assumed.
//!
//! | steps  | positions rows | total rows |
//! |--------|----------------|------------|
//! | 256    | 1 024          | ~1.5 k     |
//! | 2 048  | 8 192          | ~12 k      |
//! | 8 192  | 32 768         | ~49 k      |
//! | 32 768 | 131 072        | ~197 k     |
//!
//! # Bench-hdr convention, warmup, coordinated omission
//!
//! Each size records its per-write wall-time into an [`hdrhistogram::Histogram`]
//! and reports the p50 / p99 / p99.9 write latency, the derived **rows/sec** and
//! **per-row cost**, plus a cross-size **linear-scaling verdict** (per-row cost
//! stays ~flat for O(rows); it would rise for O(rows²))
//! ([docs/07 §5](../docs/07-performance-and-security.md#5-benchmark-methodology)).
//! An explicit [`WARMUP_WRITES`] un-recorded warmup precedes each size's
//! measured phase, on top of criterion's own. Coordinated omission **does not
//! apply**: this is a closed-loop, back-to-back throughput bench with no
//! external arrival schedule, so the histogram records raw write latency
//! (`record`, not `record_correct`).
//!
//! # Determinism note
//!
//! [`write_bundle`] is deterministic given the run (pinned sort keys, pinned
//! Parquet codec, no wall clock except the manifest's `created_utc` provenance
//! field). The wall-clock timing lives entirely in this bench harness — it never
//! changes the bundle bytes.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use std::hint::black_box;

use ironcondor::{
    AttributionSubstrate, BacktestConfig, BacktestRun, Cents, ContractKey, DataSourceSpec,
    EquityPoint, ExecutionMode, FeeSchedule, Fill, FillRecord, GreeksAttributionRow,
    IronCondorSpec, LiquidityProfile, PositionSnapshot, PriceCents, Quantity, ResourceLimits,
    SimTime, SlippageModel, StepIndex, StrategySpec, Underlying,
};
use optionstratlib::backtesting::BacktestResult;
use optionstratlib::{ExpirationDate, OptionStyle, Side};
use rust_decimal::Decimal;

/// Step counts for the growing runs — chosen so the dominant `positions` table
/// (4 legs/step) lands at 1 024 / 8 192 / 32 768 / 131 072 rows, straddling the
/// 8 192-row Parquet batch boundary (see the module docs).
const STEP_SIZES: [u32; 4] = [256, 2_048, 8_192, 32_768];

/// The four iron-condor legs written per step into `positions.parquet`.
const LEGS: usize = 4;

/// Un-recorded warmup writes before each size's measured phase.
const WARMUP_WRITES: usize = 8;

/// Tape anchor `ts_0` (ns since epoch, UTC) — matches the shared fixtures.
const TS0: i64 = 1_750_291_200_000_000_000;

/// Per-step timestamp advance (60 s) — strictly increasing.
const DT_NS: i64 = 60_000_000_000;

/// The legs' shared absolute expiry: `ts_0 + 30 days`.
const EXPIRY: i64 = TS0 + 30 * 86_400_000_000_000;

/// Total rows a `steps`-length run writes across the four tables:
/// `equity(steps) + greeks(steps) + positions(4·steps) + fills(4)`.
fn total_rows(steps: u32) -> u64 {
    u64::from(steps) * (2 + LEGS as u64) + LEGS as u64
}

/// The four condor legs as `(strike_cents, style, side)` — short call / long
/// call / short put / long put, the shape the golden fixtures use.
fn legs() -> [(u64, OptionStyle, Side); LEGS] {
    [
        (510_000, OptionStyle::Call, Side::Short),
        (520_000, OptionStyle::Call, Side::Long),
        (490_000, OptionStyle::Put, Side::Short),
        (480_000, OptionStyle::Put, Side::Long),
    ]
}

/// A resolved (`DateTime`) contract key for one leg — resolvable so the writer
/// can derive its wire `contract_id` (a `Days` key would fail the encode).
fn leg_key(strike_cents: u64, style: OptionStyle) -> ContractKey {
    ContractKey {
        underlying: Underlying::new("SPX").expect("SPX is a valid underlying"),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        strike: PriceCents::new(strike_cents),
        style,
    }
}

/// Build a `steps`-length [`BacktestRun`] with the four-condor-leg shape — the
/// writer's input, synthesised in **setup** (un-timed). All monetary fields are
/// integer cents; the sole `f64` (`drawdown`) is finite so the writer's
/// finiteness guard passes.
fn build_run(steps: u32) -> BacktestRun {
    let n = steps as usize;
    let leg_specs = legs();

    let mut equity_curve = Vec::with_capacity(n);
    let mut greeks_attribution = Vec::with_capacity(n);
    let mut positions = Vec::with_capacity(n * LEGS);

    for step in 0..steps {
        let ts = TS0 + i64::from(step) * DT_NS;
        equity_curve.push(EquityPoint::new(
            step,
            ts,
            10_020_000 - i64::from(step),
            -20_000,
            10_000_000 - i64::from(step),
            0.0,
        ));
        greeks_attribution.push(GreeksAttributionRow::new(
            step, ts, 12, -7, 3, 40, 65, 1_935,
        ));
        let open_at_end = step == steps - 1;
        for (leg_id, (strike_cents, style, side)) in leg_specs.into_iter().enumerate() {
            positions.push(PositionSnapshot {
                step,
                ts_ns: ts,
                position_id: leg_id as u64,
                trade_id: 1,
                contract: leg_key(strike_cents, style),
                side,
                quantity: 1,
                avg_price_cents: strike_cents,
                mark_cents: strike_cents,
                unrealized_cents: 500,
                stale_mark: false,
                exit_reason: None,
                open_at_end,
            });
        }
    }

    // One opening fill per leg at step 0 — a small constant table.
    let mut fills = Vec::with_capacity(LEGS);
    for (order, (strike_cents, style, side)) in leg_specs.into_iter().enumerate() {
        fills.push(FillRecord {
            fill: Fill {
                ts: SimTime::new(TS0),
                step: StepIndex::new(0),
                contract: leg_key(strike_cents, style),
                side,
                quantity: Quantity::new(1).expect("1 is a valid quantity"),
                price: PriceCents::new(strike_cents),
                fees: Cents::new(65),
                slippage: Cents::new(0),
                mode: ExecutionMode::Naive,
            },
            trade_id: 1,
            position_id: order as u64,
            order_id: order as u64,
            fill_seq: 0,
        });
    }

    BacktestRun {
        result: BacktestResult::default(),
        equity_curve,
        open_at_end: Vec::new(),
        trade_log: Vec::new(),
        attribution_substrate: AttributionSubstrate::default(),
        greeks_attribution,
        fills,
        positions,
        data_source: DataSourceSpec::Parquet {
            path: "chains/spx.parquet".to_string(),
            sha256: "bench-tape".to_string(),
        },
        data_identity: "bench-tape".to_string(),
    }
}

/// The iron-condor strategy spec the manifest records (leg strikes match the
/// synthesised legs).
fn strategy_spec() -> StrategySpec {
    StrategySpec::IronCondor(IronCondorSpec {
        underlying: Underlying::new("SPX").expect("SPX is a valid underlying"),
        underlying_price: PriceCents::new(500_000),
        short_call_strike: PriceCents::new(510_000),
        short_put_strike: PriceCents::new(490_000),
        long_call_strike: PriceCents::new(520_000),
        long_put_strike: PriceCents::new(480_000),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        implied_volatility: Decimal::new(20, 2),
        risk_free_rate: Decimal::new(5, 2),
        dividend_yield: Decimal::ZERO,
        quantity: Quantity::new(1).expect("1 is a valid quantity"),
        premium_short_call: PriceCents::new(2_000),
        premium_short_put: PriceCents::new(1_800),
        premium_long_call: PriceCents::new(800),
        premium_long_put: PriceCents::new(700),
        open_fee: PriceCents::new(65),
        close_fee: PriceCents::new(65),
    })
}

/// A `BacktestConfig` writing into `output_dir` with `overwrite = true`, so each
/// timed iteration republishes the SAME `run_id` directory in place (the sort
/// keys, codec, and derived `run_id` are all fixed — only the wall-clock timing
/// varies).
fn bench_config(output_dir: &Path) -> BacktestConfig {
    BacktestConfig {
        data_source: DataSourceSpec::Parquet {
            path: "chains/spx.parquet".to_string(),
            sha256: "bench-tape".to_string(),
        },
        mode: ExecutionMode::Naive,
        seed: 42,
        initial_capital: 10_000_000,
        fees: FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        slippage: SlippageModel::None,
        marketable_cap_ticks: 10,
        liquidity_profile: LiquidityProfile::default(),
        limits: ResourceLimits::default(),
        output_dir: output_dir.to_path_buf(),
        overwrite: true,
    }
}

/// The p50 write latency in ns as `f64` (saturating on the histogram's `u64`).
fn p_ns(hist: &Histogram<u64>, quantile: f64) -> f64 {
    let v = hist.value_at_quantile(quantile);
    u32::try_from(v).map_or(f64::from(u32::MAX), f64::from)
}

/// Print the per-size summary + the cross-size linear-scaling verdict.
fn report(rows: &[(u32, u64, Histogram<u64>)]) {
    println!("\n=== bundle_write — PB-5 scaling (bench-hdr, docs/07 §5) ===");
    println!("coordinated omission: N/A (closed-loop back-to-back; no arrival schedule)");
    println!(
        "{:>8} {:>10} {:>10} {:>10} {:>10} {:>14} {:>12}",
        "steps", "pos_rows", "total", "p50_us", "p99_us", "rows/sec", "ns/row"
    );

    let mut per_row_costs: Vec<(u64, f64)> = Vec::with_capacity(rows.len());
    for (steps, total, hist) in rows {
        let p50 = p_ns(hist, 0.50);
        let p99 = p_ns(hist, 0.99);
        let total_f = *total as f64;
        let rows_per_sec = if p50 == 0.0 {
            f64::INFINITY
        } else {
            total_f * 1_000_000_000.0 / p50
        };
        let ns_per_row = if *total == 0 { 0.0 } else { p50 / total_f };
        per_row_costs.push((*total, ns_per_row));
        println!(
            "{:>8} {:>10} {:>10} {:>10.1} {:>10.1} {:>14.0} {:>12.2}",
            steps,
            u64::from(*steps) * LEGS as u64,
            total,
            p50 / 1_000.0,
            p99 / 1_000.0,
            rows_per_sec,
            ns_per_row,
        );
    }

    // Linear-scaling verdict: for an O(rows) single-pass writer the per-row cost
    // is ~flat across sizes; an O(rows^2) writer's per-row cost rises with size.
    if let (Some(first), Some(last)) = (per_row_costs.first(), per_row_costs.last()) {
        let (small_rows, small_cost) = *first;
        let (large_rows, large_cost) = *last;
        let row_growth = large_rows as f64 / small_rows.max(1) as f64;
        let cost_ratio = if small_cost == 0.0 {
            f64::INFINITY
        } else {
            large_cost / small_cost
        };
        println!(
            "\nrows grew {row_growth:.0}x (smallest -> largest); per-row cost ratio = {cost_ratio:.2}x"
        );
        println!(
            "verdict: {} (per-row cost ~flat => O(rows) linear; rising => O(rows^2))",
            if cost_ratio <= 2.0 {
                "LINEAR (PB-5 time: MET)"
            } else {
                "NON-LINEAR (investigate)"
            }
        );
        // Machine-readable lines a future CI gate (#51) / BENCH.md can parse.
        println!("PB5_PER_ROW_NS_SMALL={small_cost:.4}");
        println!("PB5_PER_ROW_NS_LARGE={large_cost:.4}");
        println!("PB5_PER_ROW_COST_RATIO={cost_ratio:.4}");
    }
    println!("===========================================================\n");
}

/// The bundle-writer scaling benchmark.
fn bench_bundle_write(c: &mut Criterion) {
    let strategy = strategy_spec();

    let mut group = c.benchmark_group("bundle_write");
    // The largest write dominates runtime; keep the sample budget modest.
    group.sample_size(30);

    let mut recorded: Vec<(u32, u64, Histogram<u64>)> = Vec::with_capacity(STEP_SIZES.len());
    for steps in STEP_SIZES {
        // --- setup (un-timed): build the run + a fresh output dir ------------
        let run = build_run(steps);
        let dir = tempfile::tempdir().expect("create a tempdir for the bundle");
        let config = bench_config(dir.path());
        let total = total_rows(steps);

        // Explicit warmup — un-recorded, on top of criterion's own.
        for _ in 0..WARMUP_WRITES {
            let path = write_bundle_checked(&run, &config, &strategy);
            let _ = black_box(path);
        }

        let mut hist = Histogram::<u64>::new(3).expect("build the write-latency histogram");
        group.throughput(Throughput::Elements(total));
        group.bench_with_input(BenchmarkId::new("write_bundle", total), &total, |b, _| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let path = write_bundle_checked(&run, &config, &strategy);
                    let dt = start.elapsed();
                    let _ = black_box(path);
                    elapsed += dt;
                    // Closed-loop: raw latency (no coordinated-omission correction).
                    let ns = u64::try_from(dt.as_nanos()).unwrap_or(u64::MAX);
                    hist.record(ns).ok();
                }
                elapsed
            });
        });

        recorded.push((steps, total, hist));
        // `dir` drops here (after this size's measured phase) — the bundle is
        // cleaned; the run is dropped with it.
        drop(dir);
    }
    group.finish();

    report(&recorded);
}

/// Write the bundle and return the published path, panicking on error (a bench
/// helper — the writer is exercised for real, so an encode failure is a bug).
fn write_bundle_checked(
    run: &BacktestRun,
    config: &BacktestConfig,
    strategy: &StrategySpec,
) -> PathBuf {
    ironcondor::write_bundle(run, config, strategy).expect("write_bundle succeeds in the bench")
}

criterion_group!(benches, bench_bundle_write);
criterion_main!(benches);
