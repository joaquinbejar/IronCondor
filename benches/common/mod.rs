//! Shared bench scenario: the canonical iron-condor Parquet chain and the
//! knobs both the naive-throughput baseline (`naive_throughput.rs`, PB-2) and
//! the realistic-vs-naive overhead A/B (`realistic_overhead.rs`, PB-3) drive.
//!
//! `benches/` is a separate compilation target that cannot import the test-only
//! `tests/common` module, and the two benches must run the **same fixed
//! scenario** to make the overhead ratio meaningful — so the scenario lives here
//! once and both benches `mod common;` it (the same pattern
//! `tests/common/mod.rs` uses across the test binaries). Each bench uses a subset
//! of this module, so `dead_code` is allowed.
//!
//! # `bench-hdr` convention
//!
//! Latency-shaped paths report **percentiles via `hdrhistogram`
//! (p50 / p99 / p99.9 / p99.99), not criterion's default mean** — a mean hides
//! the tail a production sweep actually feels
//! ([docs/07 §5](../../docs/07-performance-and-security.md#5-benchmark-methodology)).
//! `criterion` drives warmup + sample scheduling; each timed iteration's latency
//! is recorded into an [`hdrhistogram::Histogram`] and the percentiles are
//! printed by [`report`].
//!
//! # Determinism note
//!
//! The engine under measurement is deterministic (fixed [`SEED`], the sole
//! seeded `ChaCha8Rng`, no wall clock in the loop). The wall-clock timing lives
//! entirely in the bench harness — it never enters the engine, so
//! `(seed, config, data)` stays byte-reproducible.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use hdrhistogram::Histogram;
use parquet::arrow::ArrowWriter;

use ironcondor::{
    BacktestConfig, DataSourceSpec, ExecutionMode, FeeSchedule, IronCondorSpec, LiquidityProfile,
    PriceCents, Quantity, ResourceLimits, SlippageModel, StrategySpec, Underlying,
};
use optionstratlib::ExpirationDate;
use optionstratlib::simulation::ExitPolicy;
use rust_decimal::Decimal;

/// The run's fixed seed — the engine's sole randomness source is seeded from it.
pub const SEED: u64 = 42;

/// Number of snapshots on the canonical bench tape (one per step). Each snapshot
/// carries the four iron-condor legs, so the chain is `STEPS * 4` quote rows.
pub const STEPS: u32 = 2048;

/// Tape anchor `ts_0` (ns since epoch, UTC) — matches the shared test fixture.
pub const TS0: i64 = 1_750_291_200_000_000_000;

/// Nanoseconds in one 86 400 s calendar day.
pub const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Per-step timestamp advance (60 s). Strictly increasing and, over `STEPS`,
/// well inside the 30-day expiry window so every step reprices in the normal
/// pre-expiry regime (see the const guard below).
pub const DT_NS: i64 = 60_000_000_000;

/// The four condor legs' shared absolute expiry: `ts_0 + 30 days`.
pub const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;

/// Compile-time guard: the last snapshot's timestamp stays strictly before the
/// shared expiry, so no step falls into the past-expiry (zero-time-value)
/// regime.
const _: () = assert!(
    TS0 + (STEPS as i64 - 1) * DT_NS < EXPIRY,
    "bench tape must stay strictly before expiry"
);

/// Un-recorded warmup runs before the measured phase.
pub const WARMUP_RUNS: usize = 32;

/// One quote row: `(step, ts, strike_cents, style, bid_cents, ask_cents)`.
pub type Row = (i32, i64, i64, &'static str, i64, i64);

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
pub fn condor_rows() -> Vec<Row> {
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
pub fn write_parquet(path: &Path, rows: &[Row]) -> Result<(), String> {
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
pub fn iron_condor_spec() -> StrategySpec {
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

/// A valid `BacktestConfig` over the fixture at `path`, in `mode`.
///
/// Everything but `mode` is identical across the two modes — same seed, capital,
/// fees, marketable cap, and the **default** [`LiquidityProfile`] (quoted-size
/// touch, `L = 5` deeper levels, `r = 0.5` decay) realistic mode seeds each book
/// from. `slippage` is `None` (naive mode's reference is the raw mid; realistic
/// mode ignores it — slippage is emergent from the seeded book). So the A/B
/// isolates the fill model: the two runs differ **only** in `config.mode`.
pub fn condor_config(path: &Path, mode: ExecutionMode) -> BacktestConfig {
    BacktestConfig {
        data_source: DataSourceSpec::Parquet {
            path: path.display().to_string(),
            sha256: String::new(),
        },
        mode,
        seed: SEED,
        initial_capital: 10_000_000,
        fees: FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        slippage: SlippageModel::None,
        marketable_cap_ticks: 10,
        liquidity_profile: LiquidityProfile::default(),
        limits: ResourceLimits::default(),
        output_dir: "runs/out".into(),
        overwrite: false,
    }
}

/// The exit policy: a non-triggering `TimeSteps` so `on_end` is the sole closer
/// and every step reprices the four held legs — the representative per-step
/// workload for both modes.
pub fn exit_policy() -> ExitPolicy {
    ExitPolicy::TimeSteps(1_000_000)
}

/// The per-step p50/p99/p99.9 latency in nanoseconds, derived from a per-run
/// histogram over the fixed [`STEPS`]-step tape. Per-step is fair because every
/// recorded run processes exactly the same step count.
#[must_use]
pub fn per_step_ns(hist: &Histogram<u64>, quantile: f64) -> f64 {
    let steps = f64::from(STEPS);
    let run_ns = hist.value_at_quantile(quantile);
    f64::from(u32::try_from(run_ns).unwrap_or(u32::MAX)) / steps
}

/// Print the hdrhistogram percentiles of per-run latency for one labelled mode,
/// plus the derived per-step cost and steps-per-second / steps-per-minute
/// figures. Shared by the naive baseline and the A/B overhead bench so both
/// report identically (the `bench-hdr` convention, docs/07 §5).
pub fn report(label: &str, hist: &Histogram<u64>) {
    let p = |q: f64| hist.value_at_quantile(q);
    let us = |ns: u64| f64::from(u32::try_from(ns).unwrap_or(u32::MAX)) / 1_000.0;
    let steps = f64::from(STEPS);
    // steps/sec derived from the p50 run latency (the typical run).
    let p50_ns = p(0.50);
    let steps_per_sec = if p50_ns == 0 {
        f64::INFINITY
    } else {
        steps * 1_000_000_000.0 / f64::from(u32::try_from(p50_ns).unwrap_or(u32::MAX))
    };

    println!("\n=== {label} — hdrhistogram (bench-hdr, docs/07 §5) ===");
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
        per_step_ns(hist, 0.50),
        per_step_ns(hist, 0.99),
        per_step_ns(hist, 0.999),
    );
    println!(
        "throughput (from p50): {steps_per_sec:.0} steps/sec  =  {:.2}e6 steps/min",
        steps_per_sec * 60.0 / 1_000_000.0,
    );
    println!("===============================================================\n");
}
