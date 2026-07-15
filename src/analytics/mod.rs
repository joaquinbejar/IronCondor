//! Post-run analytics.
//!
//! Analytics **consumes** the engine's output — never the `engine` module —
//! and populates the `optionstratlib::backtesting` result types in place; it
//! invents no parallel result types
//! ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
//!
//! - [`metrics`] — the minimal v0.1 summary from the ledger's `EquityPoint`
//!   series (per-step Sharpe, volatility, total return, max drawdown as a ratio
//!   and a cents magnitude), populated into [`optionstratlib::backtesting::BacktestResult`]
//!   (issue #16).
//! - P&L attribution by Greek with an exact residual and the fuller metric
//!   slice land at v0.3 (issues #30–#32).

pub mod metrics;

pub use metrics::populate;
