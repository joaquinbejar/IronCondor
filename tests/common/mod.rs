//! Shared test scaffolding: a programmatic iron-condor-shaped Parquet chain
//! and a helper that drives `ParquetFeed → BacktestEngine::run` end to end.
//!
//! Each `tests/` binary uses a subset of this module, so `dead_code` is allowed.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use ironcondor::{
    BacktestConfig, BacktestEngine, BacktestRun, DataSourceSpec, ExecutionMode, FeeSchedule,
    IronCondorSpec, NaiveFill, OptStratAdapter, ParquetFeed, PriceCents, Quantity, ResourceLimits,
    SlippageModel, StrategySpec, Underlying,
};
use optionstratlib::ExpirationDate;
use optionstratlib::simulation::ExitPolicy;
use optionstratlib::strategies::IronCondor;
use rust_decimal::Decimal;

/// The tape anchor `ts_0` (ns since epoch, UTC).
pub const TS0: i64 = 1_750_291_200_000_000_000;
/// Nanoseconds in one 86 400 s calendar day.
pub const NANOS_PER_DAY: i64 = 86_400_000_000_000;
/// The four condor legs' absolute expiry: `ts_0 + 30 days`.
pub const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;

/// One quote row: `(step, ts, strike_cents, style, bid_cents, ask_cents)`.
pub type Row = (i32, i64, i64, &'static str, i64, i64);

/// A single-row override applied to a built tape (used to perturb a future
/// snapshot in the no-look-ahead test).
#[derive(Debug, Clone, Copy)]
pub struct Perturb {
    /// The step whose quote to overwrite.
    pub step: i32,
    /// The strike (cents) of the quote to overwrite.
    pub strike: i64,
    /// The style (`"call"` | `"put"`) of the quote to overwrite.
    pub style: &'static str,
    /// The replacement bid (cents, tick-aligned).
    pub bid: i64,
    /// The replacement ask (cents, tick-aligned).
    pub ask: i64,
}

/// The canonical Parquet schema for the historical feed, in column order.
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

/// Build `steps` snapshots of the four iron-condor legs (short call 510000,
/// long call 520000, short put 490000, long put 480000), tick-aligned to 5c,
/// with mids 2000/800/1800/700 (matching [`iron_condor_spec`]); `perturb`
/// optionally overwrites one quote.
pub fn condor_rows(steps: u32, perturb: Option<Perturb>) -> Vec<Row> {
    // (strike, style, bid, ask) — mids are 2000/800/1800/700.
    let legs: [(i64, &'static str, i64, i64); 4] = [
        (510_000, "call", 1_995, 2_005),
        (520_000, "call", 795, 805),
        (490_000, "put", 1_795, 1_805),
        (480_000, "put", 695, 705),
    ];
    let mut rows = Vec::new();
    for step in 0..steps {
        let step_i = i32::try_from(step).unwrap_or(i32::MAX);
        let ts = TS0 + i64::from(step) * NANOS_PER_DAY;
        for (strike, style, bid, ask) in legs {
            let (bid, ask) = match perturb {
                Some(p) if p.step == step_i && p.strike == strike && p.style == style => {
                    (p.bid, p.ask)
                }
                _ => (bid, ask),
            };
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

/// A `PriceCents` helper for the spec.
fn cents(value: u64) -> PriceCents {
    PriceCents::new(value)
}

/// The iron-condor spec whose four leg strikes match [`condor_rows`].
pub fn iron_condor_spec() -> StrategySpec {
    let Ok(underlying) = Underlying::new("SPX") else {
        panic!("SPX is valid");
    };
    let Ok(quantity) = Quantity::new(1) else {
        panic!("1 is a valid quantity");
    };
    StrategySpec::IronCondor(IronCondorSpec {
        underlying,
        underlying_price: cents(500_000),
        short_call_strike: cents(510_000),
        short_put_strike: cents(490_000),
        long_call_strike: cents(520_000),
        long_put_strike: cents(480_000),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        implied_volatility: Decimal::new(20, 2),
        risk_free_rate: Decimal::new(5, 2),
        dividend_yield: Decimal::ZERO,
        quantity,
        premium_short_call: cents(2_000),
        premium_short_put: cents(1_800),
        premium_long_call: cents(800),
        premium_long_put: cents(700),
        open_fee: cents(65),
        close_fee: cents(65),
    })
}

/// A valid `BacktestConfig` for a naive run over `path`, with the given seed.
pub fn condor_config(path: &Path, seed: u64) -> BacktestConfig {
    BacktestConfig {
        data_source: DataSourceSpec::Parquet {
            path: path.display().to_string(),
            sha256: String::new(),
        },
        mode: ExecutionMode::Naive,
        seed,
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

/// Open `path` as a `ParquetFeed`, wrap the iron condor with a non-triggering
/// exit policy (so `on_end` is the sole closer), and run to completion.
pub fn run_condor(path: &Path, seed: u64) -> Result<BacktestRun, String> {
    let config = condor_config(path, seed);
    let feed = ParquetFeed::open(path, &ResourceLimits::default()).map_err(|e| e.to_string())?;
    // TimeSteps(1_000_000) never fires on a small tape, so exits stays empty and
    // on_end performs the single clean close of every leg at the terminal step.
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let adapter = OptStratAdapter::<IronCondor>::from_spec(&iron_condor_spec(), exit)
        .map_err(|e| e.to_string())?;
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    BacktestEngine::run(&config, feed, execution, adapter, "iron_condor").map_err(|e| e.to_string())
}
