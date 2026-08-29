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
    BacktestConfig, BacktestEngine, BacktestRun, CsvFeed, DataSourceSpec, ExecutionMode,
    FeeSchedule, IronCondorSpec, LegSetSpec, LegSetStrategy, LegSpec, NaiveFill, OptStratAdapter,
    ParquetFeed, PriceCents, Quantity, ResourceLimits, ShortStrangleSpec, SlippageModel,
    StrategySpec, Underlying,
};
use optionstratlib::simulation::ExitPolicy;
use optionstratlib::strategies::{IronCondor, ShortStrangle};
use optionstratlib::{ExpirationDate, OptionStyle, Side};
use rust_decimal::Decimal;

/// The tape anchor `ts_0` (ns since epoch, UTC).
pub const TS0: i64 = 1_750_291_200_000_000_000;
/// Nanoseconds in one 86 400 s calendar day.
pub const NANOS_PER_DAY: i64 = 86_400_000_000_000;
/// The four condor legs' absolute expiry: `ts_0 + 30 days`.
pub const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;
/// The FAR expiry of the multi-expiration leg-set tape: `ts_0 + 60 days`.
pub const FAR_EXPIRY: i64 = TS0 + 60 * NANOS_PER_DAY;

/// One quote row: `(step, ts, strike_cents, style, bid_cents, ask_cents)` — all
/// at the single [`EXPIRY`].
pub type Row = (i32, i64, i64, &'static str, i64, i64);

/// One quote row carrying its **own** expiry:
/// `(step, ts, expiration_ns, strike_cents, style, bid_cents, ask_cents)` — the
/// multi-expiration tape a [`StrategySpec::Legs`] leg set is quoted by.
pub type ExpiryRow = (i32, i64, i64, i64, &'static str, i64, i64);

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

/// Build `steps` snapshots of the two short-strangle legs (short call 520000,
/// short put 480000), tick-aligned to 5c, with flat mids 800/700 (matching
/// [`short_strangle_spec`]). A deliberately small, flat, two-leg analogue of
/// [`condor_rows`] for the `short_strangle_naive` golden (#28).
pub fn strangle_rows(steps: u32) -> Vec<Row> {
    // (strike, style, bid, ask) — mids are 800/700.
    let legs: [(i64, &'static str, i64, i64); 2] =
        [(520_000, "call", 795, 805), (480_000, "put", 695, 705)];
    let mut rows = Vec::new();
    for step in 0..steps {
        let step_i = i32::try_from(step).unwrap_or(i32::MAX);
        let ts = TS0 + i64::from(step) * NANOS_PER_DAY;
        for (strike, style, bid, ask) in legs {
            rows.push((step_i, ts, strike, style, bid, ask));
        }
    }
    rows
}

/// Build `steps` snapshots of a four-leg set spanning TWO expirations: the two
/// body legs (short call 510000 / short put 490000) at [`EXPIRY`], the two wings
/// (long call 520000 / long put 480000) a month later at [`FAR_EXPIRY`]. The
/// shape no named strategy spec can describe, and the tape [`legs_spec`] is
/// quoted by.
pub fn legs_rows(steps: u32) -> Vec<ExpiryRow> {
    // (expiration, strike, style, bid, ask) — mids are 2000/1800/800/700.
    let legs: [(i64, i64, &'static str, i64, i64); 4] = [
        (EXPIRY, 510_000, "call", 1_995, 2_005),
        (EXPIRY, 490_000, "put", 1_795, 1_805),
        (FAR_EXPIRY, 520_000, "call", 795, 805),
        (FAR_EXPIRY, 480_000, "put", 695, 705),
    ];
    let mut rows = Vec::new();
    for step in 0..steps {
        let step_i = i32::try_from(step).unwrap_or(i32::MAX);
        let ts = TS0 + i64::from(step) * NANOS_PER_DAY;
        for (expiration, strike, style, bid, ask) in legs {
            rows.push((step_i, ts, expiration, strike, style, bid, ask));
        }
    }
    rows
}

/// Encode the rows as one Parquet batch and write them to `path` — every quote
/// at the single [`EXPIRY`]. A thin wrapper over [`write_parquet_multi_expiry`],
/// so the single- and multi-expiry tapes share one encoder (and the existing
/// tapes keep their exact bytes).
pub fn write_parquet(path: &Path, rows: &[Row]) -> Result<(), String> {
    let rows: Vec<ExpiryRow> = rows
        .iter()
        .map(|&(step, ts, strike, style, bid, ask)| (step, ts, EXPIRY, strike, style, bid, ask))
        .collect();
    write_parquet_multi_expiry(path, &rows)
}

/// Encode rows that carry their **own** per-row expiry as one Parquet batch and
/// write them to `path`.
pub fn write_parquet_multi_expiry(path: &Path, rows: &[ExpiryRow]) -> Result<(), String> {
    let step: Vec<i32> = rows.iter().map(|r| r.0).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let expiration: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let strike: Vec<i64> = rows.iter().map(|r| r.3).collect();
    let style: Vec<&str> = rows.iter().map(|r| r.4).collect();
    let bid: Vec<i64> = rows.iter().map(|r| r.5).collect();
    let ask: Vec<i64> = rows.iter().map(|r| r.6).collect();
    let n = rows.len();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(step)) as ArrayRef,
        Arc::new(Int64Array::from(ts)),
        Arc::new(StringArray::from(vec!["SPX"; n])),
        Arc::new(Int64Array::from(vec![500_000i64; n])),
        Arc::new(Int64Array::from(vec![5i64; n])),
        Arc::new(Int32Array::from(vec![100i32; n])),
        Arc::new(Int64Array::from(expiration)),
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

/// The canonical CSV header for the historical feed, matching [`schema`] and
/// the integer-cents [`write_parquet`] values column-for-column.
pub const CSV_HEADER: &str = "ts,underlying,underlying_price,tick_size,contract_multiplier,\
    expiration,strike,style,bid,ask,bid_size,ask_size,implied_volatility,delta,gamma,theta,vega";

/// Write a directory of per-step CSV chain files from canonical `rows`, one file
/// per distinct step (`step_{step:05}.csv`, so the name sort matches the step
/// order), with the SAME snapshot-level values [`write_parquet`] encodes (SPX /
/// 500000 spot / 5c tick / 100x / [`EXPIRY`] / size 50 / iv 0.2 / greeks). This
/// lets the same logical chain be built as both CSV and Parquet for a
/// feed-parity assertion.
pub fn write_csv_dir(dir: &Path, rows: &[Row]) -> Result<(), String> {
    use std::collections::BTreeMap;

    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let mut by_step: BTreeMap<i32, Vec<Row>> = BTreeMap::new();
    for row in rows {
        by_step.entry(row.0).or_default().push(*row);
    }
    for (step, step_rows) in by_step {
        let mut out = String::from(CSV_HEADER);
        for (_step, ts, strike, style, bid, ask) in step_rows {
            out.push('\n');
            out.push_str(&format!(
                "{ts},SPX,500000,5,100,{EXPIRY},{strike},{style},{bid},{ask},50,50,0.2,0.3,0.01,-0.05,0.1"
            ));
        }
        let name = format!("step_{step:05}.csv");
        std::fs::write(dir.join(name), out).map_err(|e| e.to_string())?;
    }
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

/// The short-strangle spec whose two leg strikes match [`strangle_rows`]:
/// short call 520000, short put 480000 (both OTM around a 500000 spot). Money
/// fields are integer cents; rates/vol are `Decimal`. The strategy-level premia
/// and per-leg fees do not reach the engine ledger (the engine marks from
/// snapshot mids and charges `config.fees`), so they are set to clean values.
pub fn short_strangle_spec() -> StrategySpec {
    let Ok(underlying) = Underlying::new("SPX") else {
        panic!("SPX is valid");
    };
    let Ok(quantity) = Quantity::new(1) else {
        panic!("1 is a valid quantity");
    };
    StrategySpec::ShortStrangle(ShortStrangleSpec {
        underlying,
        underlying_price: cents(500_000),
        call_strike: cents(520_000),
        put_strike: cents(480_000),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        call_implied_volatility: Decimal::new(20, 2),
        put_implied_volatility: Decimal::new(20, 2),
        risk_free_rate: Decimal::new(5, 2),
        dividend_yield: Decimal::ZERO,
        quantity,
        premium_short_call: cents(800),
        premium_short_put: cents(700),
        open_fee_short_call: cents(65),
        close_fee_short_call: cents(65),
        open_fee_short_put: cents(65),
        close_fee_short_put: cents(65),
    })
}

/// The **leg-set** spec whose four legs match [`legs_rows`]: a short call /
/// short put body at [`EXPIRY`] and a long call / long put pair of wings at
/// [`FAR_EXPIRY`] — a position spanning TWO expirations, which neither
/// [`iron_condor_spec`] nor [`short_strangle_spec`] can express (both carry a
/// single strategy-level expiration).
///
/// The legs are deliberately written in a NON-canonical order (a wing first), so
/// any test that hashes or writes this spec exercises the canonicalisation.
pub fn legs_spec() -> StrategySpec {
    let Ok(underlying) = Underlying::new("SPX") else {
        panic!("SPX is valid");
    };
    StrategySpec::Legs(LegSetSpec {
        underlying,
        underlying_price: cents(500_000),
        legs: vec![
            leg(520_000, OptionStyle::Call, Side::Long, FAR_EXPIRY),
            leg(510_000, OptionStyle::Call, Side::Short, EXPIRY),
            leg(480_000, OptionStyle::Put, Side::Long, FAR_EXPIRY),
            leg(490_000, OptionStyle::Put, Side::Short, EXPIRY),
        ],
        risk_free_rate: Decimal::new(5, 2),
        dividend_yield: Decimal::ZERO,
    })
}

/// One leg of [`legs_spec`]: a single contract at its own expiry.
pub fn leg(strike: u64, style: OptionStyle, side: Side, expiration_ns: i64) -> LegSpec {
    let Ok(quantity) = Quantity::new(1) else {
        panic!("1 is a valid quantity");
    };
    LegSpec {
        side,
        style,
        strike: cents(strike),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(expiration_ns)),
        quantity,
        implied_volatility: Decimal::new(20, 2),
    }
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
        marketable_cap_ticks: 10,
        liquidity_profile: ironcondor::LiquidityProfile::default(),
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

/// Open `path` as a `ParquetFeed`, wrap the iron condor with the given
/// `exit` policy, and run to completion — the exit-policy-parameterised
/// analogue of [`run_condor`], used by the F11 terminal-step regression (an exit
/// policy firing on the final step must not double-close via `on_end`).
pub fn run_condor_with_exit(
    path: &Path,
    seed: u64,
    exit: ExitPolicy,
) -> Result<BacktestRun, String> {
    let config = condor_config(path, seed);
    let feed = ParquetFeed::open(path, &ResourceLimits::default()).map_err(|e| e.to_string())?;
    let adapter = OptStratAdapter::<IronCondor>::from_spec(&iron_condor_spec(), exit)
        .map_err(|e| e.to_string())?;
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    BacktestEngine::run(&config, feed, execution, adapter, "iron_condor").map_err(|e| e.to_string())
}

/// Open `dir` as a [`CsvFeed`] (records a `DataSourceSpec::Csv` provenance),
/// wrap the iron condor with a non-triggering exit policy, and run to
/// completion — the CSV analogue of [`run_condor`].
pub fn run_condor_csv(dir: &Path, seed: u64) -> Result<BacktestRun, String> {
    let mut config = condor_config(dir, seed);
    config.data_source = DataSourceSpec::Csv {
        path: dir.display().to_string(),
        sha256: String::new(),
    };
    let feed = CsvFeed::open(dir, &ResourceLimits::default()).map_err(|e| e.to_string())?;
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let adapter = OptStratAdapter::<IronCondor>::from_spec(&iron_condor_spec(), exit)
        .map_err(|e| e.to_string())?;
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    BacktestEngine::run(&config, feed, execution, adapter, "iron_condor").map_err(|e| e.to_string())
}

/// Open `path` as a `ParquetFeed`, wrap a [`ShortStrangle`] (the v0.2 second
/// strategy) built via `from_spec` with a non-triggering exit policy, and run to
/// completion — the strangle analogue of [`run_condor`], driving the **same**
/// generic adapter and engine loop. `strategy_name` is `"short_strangle"`.
pub fn run_strangle(path: &Path, seed: u64) -> Result<BacktestRun, String> {
    let config = condor_config(path, seed);
    let feed = ParquetFeed::open(path, &ResourceLimits::default()).map_err(|e| e.to_string())?;
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let adapter = OptStratAdapter::<ShortStrangle>::from_spec(&short_strangle_spec(), exit)
        .map_err(|e| e.to_string())?;
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    BacktestEngine::run(&config, feed, execution, adapter, "short_strangle")
        .map_err(|e| e.to_string())
}

/// Open `path` as a `ParquetFeed`, drive the [`legs_spec`] leg set through
/// [`LegSetStrategy`] with a non-triggering exit policy, and run to completion —
/// the leg-set analogue of [`run_condor`], driving the **same** engine loop
/// without an upstream adapter. `strategy_name` is `"legs"`.
pub fn run_legs(path: &Path, seed: u64) -> Result<BacktestRun, String> {
    let config = condor_config(path, seed);
    let feed = ParquetFeed::open(path, &ResourceLimits::default()).map_err(|e| e.to_string())?;
    let exit = ExitPolicy::TimeSteps(1_000_000);
    let strategy = LegSetStrategy::from_spec(&legs_spec(), exit).map_err(|e| e.to_string())?;
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    BacktestEngine::run(&config, feed, execution, strategy, "legs").map_err(|e| e.to_string())
}
