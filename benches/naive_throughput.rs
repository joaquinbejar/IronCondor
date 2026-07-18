//! Naive-mode throughput baseline — hot path **H1**/PB-2 (issue #18).
//!
//! Measures the **full** [`ironcondor::run_backtest`] over a canonical Parquet
//! iron-condor chain: Parquet feed open + parse → replay loop → naive fills →
//! mark-to-market ledger → summary metrics, single strategy, single core. This
//! is the project's **first published number**; its measured p50 / p99 / p99.9
//! are recorded in `BENCH.md` and become the v0.1 baseline the #019 zero-alloc
//! gate and the #051 percentile-regression gate build on
//! ([docs/07 §3/§5/§6](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
//!
//! # Bench-hdr convention
//!
//! Latency-shaped paths report **percentiles via `hdrhistogram`
//! (p50 / p99 / p99.9 / p99.99), not criterion's default mean** — a mean hides
//! the tail a production sweep actually feels
//! ([docs/07 §5](../docs/07-performance-and-security.md#5-benchmark-methodology)).
//! `criterion` drives warmup + sample scheduling; each timed iteration's latency
//! is recorded into an [`hdrhistogram::Histogram`] and the percentiles are
//! printed here.
//!
//! # Coordinated omission — disclosure
//!
//! This is a **closed-loop throughput** bench: each run starts immediately after
//! the previous one finishes, with no external arrival schedule. Coordinated
//! omission (a slow response masking the lateness of requests that *should* have
//! been issued on a fixed cadence) therefore **does not apply** — there is no
//! expected inter-arrival interval to correct against, so the histogram records
//! raw back-to-back run latency without a corrected-interval replay
//! (`record`, not `record_correct`). The reported quantiles are the true
//! distribution of end-to-end run latency at saturation.
//!
//! # Warmup
//!
//! An explicit warmup of [`WARMUP_RUNS`] un-recorded runs precedes the criterion
//! measurement (cache, branch predictor, allocator arenas), on top of
//! criterion's own warmup phase. Every histogram observation is therefore taken
//! at steady state.
//!
//! # Determinism note
//!
//! The engine under measurement is deterministic (fixed [`SEED`], the sole
//! seeded `ChaCha8Rng`, no wall clock in the loop). The wall-clock timing here is
//! inherent to *benchmarking* and lives entirely in this bench harness — it never
//! enters the engine, so `(seed, config, data)` stays byte-reproducible.

use std::cell::RefCell;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;

use ironcondor::{
    BacktestConfig, DataSourceSpec, ExecutionMode, FeeSchedule, IronCondorSpec, PriceCents,
    Quantity, ResourceLimits, SlippageModel, StrategySpec, Underlying, run_backtest,
};
use optionstratlib::ExpirationDate;
use optionstratlib::simulation::ExitPolicy;
use rust_decimal::Decimal;

/// The run's fixed seed — the engine's sole randomness source is seeded from it.
const SEED: u64 = 42;

/// Number of snapshots on the canonical bench tape (one per step). Each snapshot
/// carries the four iron-condor legs, so the chain is `STEPS * 4` quote rows.
const STEPS: u32 = 2048;

/// Tape anchor `ts_0` (ns since epoch, UTC) — matches the shared test fixture.
const TS0: i64 = 1_750_291_200_000_000_000;

/// Nanoseconds in one 86 400 s calendar day.
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Per-step timestamp advance (60 s). Strictly increasing and, over `STEPS`,
/// well inside the 30-day expiry window so every step reprices in the normal
/// pre-expiry regime (see the const guard below).
const DT_NS: i64 = 60_000_000_000;

/// The four condor legs' shared absolute expiry: `ts_0 + 30 days`.
const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;

/// Compile-time guard: the last snapshot's timestamp stays strictly before the
/// shared expiry, so no step falls into the past-expiry (zero-time-value)
/// regime.
const _: () = assert!(
    TS0 + (STEPS as i64 - 1) * DT_NS < EXPIRY,
    "bench tape must stay strictly before expiry"
);

/// Un-recorded warmup runs before the measured phase.
const WARMUP_RUNS: usize = 32;

/// One quote row: `(step, ts, strike_cents, style, bid_cents, ask_cents)`.
type Row = (i32, i64, i64, &'static str, i64, i64);

/// The canonical Parquet schema for the historical feed, in column order.
///
/// Replicated from `tests/common` — `benches/` is a separate compilation target
/// that cannot import the test-only module, and the fixture is small.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("underlying", DataType::Utf8, false),
        Field::new("underlying_price", DataType::Int64, false),
        Field::new("tick_size", DataType::Int64, false),
        Field::new("contract_multiplier", DataType::Int32, false),
        Field::new("expiration", DataType::Int64, false),
        Field::new("strike", DataType::Int64, false),
        Field::new("style", DataType::Utf8, false),
        Field::new("bid", DataType::Int64, false),
        Field::new("ask", DataType::Int64, false),
        Field::new("bid_size", DataType::Int32, false),
        Field::new("ask_size", DataType::Int32, false),
        Field::new("implied_volatility", DataType::Float64, false),
        Field::new("delta", DataType::Float64, false),
        Field::new("gamma", DataType::Float64, false),
        Field::new("theta", DataType::Float64, false),
        Field::new("vega", DataType::Float64, false),
    ]))
}

/// Build `STEPS` snapshots of the four iron-condor legs (short call 510000,
/// long call 520000, short put 490000, long put 480000), tick-aligned to 5c with
/// mids 2000/800/1800/700 — the same shape as the shared `condor_rows` fixture,
/// but with a 60 s step so the whole tape stays before expiry.
fn condor_rows() -> Vec<Row> {
    // (strike, style, bid, ask) — mids are 2000/800/1800/700.
    let legs: [(i64, &'static str, i64, i64); 4] = [
        (510_000, "call", 1_995, 2_005),
        (520_000, "call", 795, 805),
        (490_000, "put", 1_795, 1_805),
        (480_000, "put", 695, 705),
    ];
    let mut rows = Vec::with_capacity(STEPS as usize * legs.len());
    for step in 0..STEPS {
        let step_i = i32::try_from(step).unwrap_or(i32::MAX);
        let ts = TS0 + i64::from(step) * DT_NS;
        for (strike, style, bid, ask) in legs {
            rows.push((step_i, ts, strike, style, bid, ask));
        }
    }
    rows
}

/// Encode the rows as one Parquet batch and write them to `path`.
fn write_parquet(path: &Path, rows: &[Row]) -> Result<(), String> {
    let step: Vec<i32> = rows.iter().map(|r| r.0).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let strike: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let style: Vec<&str> = rows.iter().map(|r| r.3).collect();
    let bid: Vec<i64> = rows.iter().map(|r| r.4).collect();
    let ask: Vec<i64> = rows.iter().map(|r| r.5).collect();
    let n = rows.len();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(step)) as ArrayRef,
        Arc::new(Int64Array::from(ts)),
        Arc::new(StringArray::from(vec!["SPX"; n])),
        Arc::new(Int64Array::from(vec![500_000i64; n])),
        Arc::new(Int64Array::from(vec![5i64; n])),
        Arc::new(Int32Array::from(vec![100i32; n])),
        Arc::new(Int64Array::from(vec![EXPIRY; n])),
        Arc::new(Int64Array::from(strike)),
        Arc::new(StringArray::from(style)),
        Arc::new(Int64Array::from(bid)),
        Arc::new(Int64Array::from(ask)),
        Arc::new(Int32Array::from(vec![50i32; n])),
        Arc::new(Int32Array::from(vec![50i32; n])),
        Arc::new(Float64Array::from(vec![0.2f64; n])),
        Arc::new(Float64Array::from(vec![0.3f64; n])),
        Arc::new(Float64Array::from(vec![0.01f64; n])),
        Arc::new(Float64Array::from(vec![-0.05f64; n])),
        Arc::new(Float64Array::from(vec![0.1f64; n])),
    ];

    let batch = RecordBatch::try_new(schema(), columns).map_err(|e| e.to_string())?;
    let file = std::fs::File::create(path).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema(), None).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}

/// The iron-condor spec whose four leg strikes and premiums match the fixture.
fn iron_condor_spec() -> StrategySpec {
    let underlying = Underlying::new("SPX").expect("SPX is a valid underlying");
    let quantity = Quantity::new(1).expect("1 is a valid quantity");
    StrategySpec::IronCondor(IronCondorSpec {
        underlying,
        underlying_price: PriceCents::new(500_000),
        short_call_strike: PriceCents::new(510_000),
        short_put_strike: PriceCents::new(490_000),
        long_call_strike: PriceCents::new(520_000),
        long_put_strike: PriceCents::new(480_000),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        implied_volatility: Decimal::new(20, 2),
        risk_free_rate: Decimal::new(5, 2),
        dividend_yield: Decimal::ZERO,
        quantity,
        premium_short_call: PriceCents::new(2_000),
        premium_short_put: PriceCents::new(1_800),
        premium_long_call: PriceCents::new(800),
        premium_long_put: PriceCents::new(700),
        open_fee: PriceCents::new(65),
        close_fee: PriceCents::new(65),
    })
}

/// A valid naive `BacktestConfig` over the fixture at `path`.
fn condor_config(path: &Path) -> BacktestConfig {
    BacktestConfig {
        data_source: DataSourceSpec::Parquet {
            path: path.display().to_string(),
            sha256: String::new(),
        },
        mode: ExecutionMode::Naive,
        seed: SEED,
        initial_capital: 10_000_000,
        fees: FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        slippage: SlippageModel::None,
        limits: ResourceLimits::default(),
        output_dir: "runs/out".into(),
        overwrite: false,
    }
}

/// The exit policy: a non-triggering `TimeSteps` so `on_end` is the sole closer
/// and every step reprices the four held legs — the representative naive-mode
/// per-step workload.
fn exit_policy() -> ExitPolicy {
    ExitPolicy::TimeSteps(1_000_000)
}

/// Print the hdrhistogram percentiles of per-run latency, plus the derived
/// per-step cost and steps-per-second / steps-per-minute figures.
fn report(hist: &Histogram<u64>) {
    let steps = f64::from(STEPS);
    let p = |q: f64| hist.value_at_quantile(q);
    let per_step = |q: f64| f64::from(u32::try_from(p(q)).unwrap_or(u32::MAX)) / steps;

    let us = |ns: u64| f64::from(u32::try_from(ns).unwrap_or(u32::MAX)) / 1_000.0;
    // steps/sec derived from the p50 run latency (the typical run).
    let p50_ns = p(0.50);
    let steps_per_sec = if p50_ns == 0 {
        f64::INFINITY
    } else {
        steps * 1_000_000_000.0 / f64::from(u32::try_from(p50_ns).unwrap_or(u32::MAX))
    };

    println!("\n=== naive_throughput — hdrhistogram (bench-hdr, docs/07 §5) ===");
    println!("chain: {STEPS} steps x 4 legs = {} quote rows", STEPS * 4);
    println!("recorded runs: {}", hist.len());
    println!("coordinated omission: N/A (closed-loop back-to-back; no arrival schedule)");
    println!(
        "per-run latency  p50={:.1}us  p99={:.1}us  p99.9={:.1}us  p99.99={:.1}us  max={:.1}us",
        us(p(0.50)),
        us(p(0.99)),
        us(p(0.999)),
        us(p(0.9999)),
        us(hist.max()),
    );
    println!(
        "per-step latency p50={:.1}ns  p99={:.1}ns  p99.9={:.1}ns",
        per_step(0.50),
        per_step(0.99),
        per_step(0.999),
    );
    println!(
        "throughput (from p50): {steps_per_sec:.0} steps/sec  =  {:.2}e6 steps/min",
        steps_per_sec * 60.0 / 1_000_000.0,
    );
    println!("===============================================================\n");
}

/// The naive-throughput benchmark.
fn bench_naive_throughput(c: &mut Criterion) {
    // --- setup (outside the measured region) ---------------------------------
    let dir = tempfile::tempdir().expect("create a tempdir for the bench fixture");
    let path = dir.path().join("condor_chain.parquet");
    write_parquet(&path, &condor_rows()).expect("write the canonical bench fixture");
    let config = condor_config(&path);
    let spec = iron_condor_spec();

    // Explicit warmup — un-recorded, on top of criterion's own warmup phase.
    for _ in 0..WARMUP_RUNS {
        let run = run_backtest(&config, &spec, exit_policy()).expect("warmup run succeeds");
        let _ = black_box(run);
    }

    // Per-run latencies land here; 3 significant figures, auto-resizing.
    let hist = RefCell::new(Histogram::<u64>::new(3).expect("build the latency histogram"));

    let mut group = c.benchmark_group("naive_throughput");
    // Each iteration processes STEPS steps → criterion also reports steps/sec.
    group.throughput(Throughput::Elements(u64::from(STEPS)));
    group.bench_function("run_backtest", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let run = run_backtest(&config, &spec, exit_policy())
                    .expect("measured run_backtest succeeds");
                let elapsed = start.elapsed();
                let _ = black_box(run);
                total += elapsed;
                // Closed-loop: record raw latency (no coordinated-omission
                // correction — see the module disclosure).
                let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
                hist.borrow_mut().record(ns).ok();
            }
            total
        });
    });
    group.finish();

    report(&hist.borrow());
}

criterion_group!(benches, bench_naive_throughput);
criterion_main!(benches);
