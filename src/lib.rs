//! **IronCondor** is a high-performance backtester for options-trading
//! strategies with **order-book-level fill simulation**, written in Rust.
//!
//! It is the deterministic replay engine *around* an upstream options stack,
//! not a re-implementation of it. Pricing, Greeks, multi-leg strategies, and
//! exit policies come from
//! [`optionstratlib`](https://crates.io/crates/optionstratlib); order matching
//! comes from
//! [`option-chain-orderbook`](https://crates.io/crates/option-chain-orderbook)
//! and [`orderbook-rs`](https://github.com/joaquinbejar/OrderBook-rs);
//! synthetic option chains come from
//! [OptionChain-Simulator](https://github.com/joaquinbejar/OptionChain-Simulator).
//! This crate contributes the replay loop, the dual fill models, P&L
//! attribution by Greek, the result bundle, and the Python bindings.
//!
//! ## Why order-book-level fills
//!
//! Most backtesters fill option orders at mid or bid/ask with a fixed slippage
//! assumption. IronCondor adds a **realistic** mode that routes every order
//! through a real options matching engine — queue position, partial fills,
//! multi-level book walks, and a resting GTC lifecycle — so fill risk is an
//! emergent property of the book rather than a guess. A fast **naive** mode
//! (mid/spread plus configurable slippage) stays available for quick iteration,
//! and both modes emit the identical fill-report shape, so downstream analytics
//! is mode-agnostic.
//!
//! ## Key properties
//!
//! - **Deterministic replay.** For a fixed
//!   `(seed, config, data, crate version, Rust toolchain, lockfile)` the four
//!   Parquet tables are byte-identical and the manifest is identical minus its
//!   one wall-clock provenance field. Across environments the guarantee is
//!   *logical equivalence* under a documented normalization (canonical row
//!   ordering, canonical JSON). No wall clock, no unseeded RNG, and no
//!   map-iteration order reaches a result.
//! - **Money as integer cents.** Every execution and result boundary carries
//!   money as integer cents; order-book prices are `u128` ticks. `f64` is
//!   confined to the upstream pricing/Greeks kernel and the documented
//!   derived-analytics columns.
//! - **Dual execution modes.** `naive` (mid/spread plus slippage) and
//!   `realistic` (a real order book: queue position, partial fills, multi-level
//!   walks, resting GTC orders), selected once from config with no per-step
//!   dynamic dispatch.
//! - **P&L attribution by Greek.** Each step's mark-to-market change is
//!   decomposed into theta, delta, vega, and spread capture minus fees, closed
//!   by an **exact** integer-cents residual: for every step,
//!   `theta + delta + vega + spread − fees + residual` equals the equity change
//!   by construction. A large residual is an advisory model-quality signal,
//!   never a run failure.
//! - **A frozen result bundle.** Every run publishes an `ironcondor.bundle.v1`
//!   directory (a manifest plus four Parquet tables) consumed by
//!   [ChainView](https://github.com/joaquinbejar/ChainView).
//! - **Hardened against untrusted input.** The engine parses untrusted files
//!   and runs inside CI, so every external input pairs a validation with a
//!   resource ceiling and a typed error — no panic, hang, or OOM on malformed
//!   input. Parsers are fuzzed, the crate is `#![forbid(unsafe_code)]`, errors
//!   are the typed `BacktestError`, and none cross the Python boundary as a
//!   panic.
//! - **Performance as an acceptance criterion.** The replay loop holds zero
//!   steady-state allocation, and the hot paths (loop, fill models, conversion,
//!   bundle writer, PyO3 boundary) carry CI-gated budgets measured with
//!   `criterion` and `hdrhistogram`. As a recorded baseline (see `BENCH.md`), a
//!   full `run_backtest` over a 2048-step, four-leg iron-condor Parquet chain
//!   runs at a p50 of about 2.35 µs/step (about 425k steps/sec/core) in naive
//!   mode on an Apple M4 Max — a measurement, not a guarantee.
//!
//! ## Feature flags
//!
//! | Feature | Default | What it adds |
//! |---------|:-------:|--------------|
//! | *(none)* | yes | Replay engine, naive execution, Parquet/CSV historical feeds, and the result bundle. |
//! | `orderbook` | | Realistic, liquidity-aware fills routed through `option-chain-orderbook`. |
//! | `simulator` | | Synthetic chain sessions from OptionChain-Simulator over HTTP. |
//! | `python` | | PyO3 bindings, built as a wheel with maturin (PyPI publication planned). |
//!
//! ## Quick start (Rust)
//!
//! Drive one backtest end to end — a Parquet chain in, an equity curve out:
//!
//! ```rust,no_run
//! use ironcondor::{
//!     BacktestConfig, DataSourceSpec, ExecutionMode, FeeSchedule, IronCondorSpec,
//!     LiquidityProfile, PriceCents, Quantity, ResourceLimits, SlippageModel,
//!     StrategySpec, Underlying, run_backtest,
//! };
//! use optionstratlib::ExpirationDate;
//! use optionstratlib::simulation::ExitPolicy;
//! use rust_decimal::Decimal;
//!
//! fn main() -> Result<(), ironcondor::BacktestError> {
//!     let config = BacktestConfig {
//!         data_source: DataSourceSpec::Parquet {
//!             path: "chains/spx.parquet".to_string(),
//!             sha256: String::new(),
//!         },
//!         mode: ExecutionMode::Naive,
//!         seed: 42,
//!         initial_capital: 10_000_000, // $100,000, in cents
//!         fees: FeeSchedule { per_contract_cents: 65, per_order_cents: 100 },
//!         slippage: SlippageModel::None,
//!         marketable_cap_ticks: 10,
//!         liquidity_profile: LiquidityProfile::default(),
//!         limits: ResourceLimits::default(),
//!         output_dir: "runs".into(),
//!         overwrite: false,
//!     };
//!
//!     let strategy = StrategySpec::IronCondor(IronCondorSpec {
//!         underlying: Underlying::new("SPX")?,
//!         underlying_price: PriceCents::new(500_000),
//!         short_call_strike: PriceCents::new(510_000),
//!         short_put_strike: PriceCents::new(490_000),
//!         long_call_strike: PriceCents::new(520_000),
//!         long_put_strike: PriceCents::new(480_000),
//!         expiration: ExpirationDate::DateTime(
//!             chrono::DateTime::from_timestamp_nanos(1_750_291_200_000_000_000),
//!         ),
//!         implied_volatility: Decimal::new(20, 2), // 0.20
//!         risk_free_rate: Decimal::new(5, 2),      // 0.05
//!         dividend_yield: Decimal::ZERO,
//!         quantity: Quantity::new(1)?,
//!         premium_short_call: PriceCents::new(2_000),
//!         premium_short_put: PriceCents::new(1_800),
//!         premium_long_call: PriceCents::new(800),
//!         premium_long_put: PriceCents::new(700),
//!         open_fee: PriceCents::new(65),
//!         close_fee: PriceCents::new(65),
//!     });
//!
//!     // A non-triggering exit so the run marks every step and closes at the end.
//!     let run = run_backtest(&config, &strategy, ExitPolicy::TimeSteps(1_000_000))?;
//!     println!(
//!         "{}: {} equity points",
//!         run.result.strategy_name,
//!         run.equity_curve.len(),
//!     );
//!     Ok(())
//! }
//! ```
//!
//! ## Quick start (Python)
//!
//! The `python` feature builds a PyO3 extension module. Wheels are **not yet on
//! PyPI**; build one locally with [maturin](https://www.maturin.rs):
//!
//! ```bash
//! maturin develop --release --features python,orderbook,simulator
//! ```
//!
//! ```python
//! import ironcondor as ic
//!
//! config = (
//!     ic.BacktestConfig(seed=42, capital_cents=10_000_000)
//!     .data_parquet("chains/spx.parquet")
//!     .strategy_iron_condor(
//!         underlying="SPX",
//!         underlying_price_cents=500_000,
//!         short_call_strike_cents=510_000,
//!         short_put_strike_cents=490_000,
//!         long_call_strike_cents=520_000,
//!         long_put_strike_cents=480_000,
//!         expiration_ns=1_750_291_200_000_000_000,
//!         quantity=1,
//!         premium_short_call_cents=2_000,
//!         premium_short_put_cents=1_800,
//!         premium_long_call_cents=800,
//!         premium_long_put_cents=700,
//!         implied_volatility=0.20,
//!         risk_free_rate=0.05,
//!         dividend_yield=0.0,
//!         open_fee_cents=65,
//!         close_fee_cents=65,
//!     )
//!     .execution_naive()
//!     .fees(per_contract_cents=65, per_order_cents=100)
//!     .exit_time_steps(1_000_000)
//!     .output_dir("runs")
//! )
//!
//! bundle = ic.run(config)          # publishes an ironcondor.bundle.v1 directory
//! print(bundle.metrics())          # summary metrics as a dict
//! equity = bundle.equity_curve()   # a pandas DataFrame with integer-cents columns
//! ```
//!
//! ## The result bundle
//!
//! Every run publishes an `ironcondor.bundle.v1` directory: a `manifest.json`
//! (run metadata, strategy, config, data source, code version) plus four Parquet
//! tables — `fills.parquet`, `equity_curve.parquet`, `positions.parquet`, and
//! `greeks_attribution.parquet`. Writes are atomic (temp file plus rename). The
//! schema tag is frozen and its lineage is coordinated with
//! [ChainView](https://github.com/joaquinbejar/ChainView), which consumes the
//! bundle in replay mode — so a schema change is a deliberate SemVer event, not
//! an accident.
//!
//! ## Status and versioning
//!
//! `0.5.0` completes the v0.1–v0.5 roadmap — the engine, both fill models, the
//! full analytics and result bundle, and the Python bindings — with the v1.0
//! stability gates wired: the Rust public surface, the configuration surface,
//! and the bundle schema are each pinned by a committed snapshot that fails CI
//! on drift. Under SemVer `0.x`, breaking changes may still land in minor
//! releases; the `1.0` cut follows the documented one-quarter stability window.
//! Documentation states present-tense claims only for behaviour that exists,
//! and no benchmark number is written before it is measured.
//!
//! ## Ecosystem
//!
//! Part of a family of Rust crates for options-trading infrastructure:
//! [OptionStratLib](https://github.com/joaquinbejar/OptionStratLib) ·
//! [Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook) ·
//! [OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) ·
//! [OptionChain-Simulator](https://github.com/joaquinbejar/OptionChain-Simulator) ·
//! [ChainView](https://github.com/joaquinbejar/ChainView)
//!
//! ## Contact
//!
//! Joaquin Bejar — <jb@taunais.com>

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod analytics;
pub mod batch;
pub mod bundle;
pub mod config;
pub mod data;
pub mod domain;
pub mod engine;
pub mod error;
pub mod execution;
#[cfg(feature = "python")]
pub mod python;
pub mod run;

pub use analytics::Metrics;
pub use batch::{
    BATCH_INDEX_SCHEMA, BatchIndex, BatchRunEntry, BatchRunOutcome, SimulatorMaterialisation,
    run_scenario_batch,
};
pub use bundle::{
    BUNDLE_SCHEMA, EQUITY_CURVE_SORT_COLUMNS, FILLS_SORT_COLUMNS, GREEKS_ATTRIBUTION_SORT_COLUMNS,
    Manifest, POSITIONS_SORT_COLUMNS, RowCounts, RunId, ValidatedBundle, ValidatedManifest,
    equity_sort_key, fill_sort_key, greeks_sort_key, position_sort_key, read_bundle, write_bundle,
};
pub use config::{
    BacktestConfig, FeeSchedule, LiquidityProfile, ResourceLimits, SlippageModel, TouchSize,
};
pub use data::{
    CsvFeed, DataFeed, DataSourceSpec, FeedKind, ParquetFeed, RawQuote, SnapshotMeta, TapeMeta,
    feed_catalogue, raw_quotes_to_snapshot, snapshot_to_option_chain,
};
pub use domain::{
    Cents, ChainSnapshot, ContractKey, EquityPoint, ExecutionMode, Fill, FillRow,
    GreeksAttributionRow, InstrumentSpec, IronCondorSpec, OpenPosition, OrderCommand, OrderId,
    OrderIntent, PendingOrder, PositionAction, PositionId, PositionRow, PriceCents, Quantity,
    QuoteView, ShortStrangleSpec, SimTime, StepIndex, StrategySpec, Ticks, TimeInForce, TradeId,
    Underlying,
};
pub use engine::{
    AttributionSubstrate, BacktestEngine, BacktestRun, ChainContext, ConfigOverride, Event,
    FillRecord, Ledger, LegAttributionSample, OptStratAdapter, PositionMark, PositionSnapshot,
    PositionableStrategy, ScenarioParams, ScenarioType, SimClock, StepAttributionScalars, Strategy,
    UnitGreeks, WalkPreset, child_data_seed, child_seed, expand,
};
pub use error::BacktestError;
#[cfg(feature = "orderbook")]
pub use execution::RealisticFill;
pub use execution::{ExecutionModel, FillGroup, NaiveFill};
pub use run::run_backtest;

#[cfg(feature = "simulator")]
pub use data::SimulatorSourceSpec;
/// The migrated OptionChain-Simulator session surface (feature `simulator`):
/// the async [`data::simulator::ApiClient`], the bug-fixed
/// [`data::simulator::MarketSimulator`] step wrapper, the local wire DTOs,
/// and the materialised-tape [`data::simulator::SimulatorFeed`] (#45).
#[cfg(feature = "simulator")]
pub use data::chain_response_to_snapshot;
#[cfg(feature = "simulator")]
pub use data::simulator::SimulatorFeed;
#[cfg(feature = "simulator")]
pub use data::simulator::{
    ApiClient, ChainResponse, CreateSessionRequest, ErrorResponse, MarketSimulator, MarketState,
    OptionContractResponse, OptionPriceResponse, SessionInfoResponse, SessionParametersResponse,
    SessionResponse, SessionState, UpdateSessionRequest,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_builds() {}
}
