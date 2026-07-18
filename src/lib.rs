//! > 🚧 **Early development** — the architecture scaffold exists, but no
//! > engine functionality is implemented yet. APIs are not stable.
//!
//! **IronCondor** is a high-performance backtesting engine for options
//! trading strategies, written in Rust.
//!
//! ## Why another backtester?
//!
//! Every existing backtester fills options orders at mid or bid/ask with a
//! fixed slippage assumption. IronCondor is designed to simulate execution
//! **at the order book level** — queue position, per-strike liquidity, and
//! real market impact — using a full options matching engine.
//!
//! ## Planned features
//!
//! - **Order-book-level fills**: route orders through
//!   [Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook)
//!   instead of assuming mid-price fills. A fast `naive` mode (mid/spread) is
//!   also available for quick iteration.
//! - **Multi-leg strategies**: spreads, condors, butterflies and custom
//!   strategies via [OptionStratLib](https://github.com/joaquinbejar/OptionStratLib).
//! - **P&L attribution by Greek**: decompose returns into theta, delta, vega
//!   and spread capture.
//! - **Synthetic data**: no historical chains? Generate realistic ones with
//!   [OptionChain-Simulator](https://github.com/joaquinbejar/OptionChain-Simulator).
//! - **Python bindings**: first-class Python API via PyO3, published on PyPI.
//!
//! ## Architecture
//!
//! The crate is a single package layered as follows (each module is a
//! compiling placeholder until its roadmap issue lands):
//!
//! - [`domain`] — canonical types: integer-cents money, market-data and
//!   execution records.
//! - [`engine`] — the deterministic replay loop, strategy seam, simulation
//!   clock, scenarios, and ledger.
//! - [`execution`] — fill models: naive (mid/spread + slippage) and realistic
//!   (order-book matching, feature `orderbook`).
//! - [`data`] — chain feeds: historical Parquet/CSV loaders and the
//!   OptionChain-Simulator client + materialised-tape feed
//!   (feature `simulator`).
//! - [`analytics`] — P&L attribution by Greek and summary metrics.
//! - [`bundle`] — the result-bundle writer (manifest + Parquet tables).
//! - `python` — PyO3 bindings (feature `python`).
//! - [`error`] / [`config`] — the shared typed-error and configuration
//!   boundary.
//!
//! ## Feature flags
//!
//! - `default` — engine + naive execution + historical feeds.
//! - `orderbook` — realistic, liquidity-aware execution.
//! - `simulator` — synthetic chain sessions over HTTP.
//! - `python` — PyO3 bindings (the `cdylib` is inert without it).
//!
//! ## Ecosystem
//!
//! Part of a family of Rust crates for options trading infrastructure:
//! [OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) ·
//! [OptionStratLib](https://github.com/joaquinbejar/OptionStratLib) ·
//! [PriceLevel](https://github.com/joaquinbejar/PriceLevel) ·
//! [Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook)
//!
//! ## Documentation
//!
//! The full design documentation (PRD, roadmap, domain model, engine
//! architecture, execution models, analytics/reporting, Python bindings, and
//! ADRs) is maintained locally during the design phase and will be published
//! with the first implementation release (v0.1.0).
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
