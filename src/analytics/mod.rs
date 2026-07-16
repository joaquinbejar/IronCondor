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
//! - [`attribution`] — P&L attribution by Greek with an **exact** integer-cents
//!   residual (issue #31): the post-run pass that decomposes each step's
//!   mark-to-market `step_pnl` from the engine's owned attribution substrate
//!   ([`crate::engine::substrate`]) into one [`crate::domain::GreeksAttributionRow`]
//!   per step ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
//! - The fuller metric slice lands at v0.3 (issue #32).

pub mod attribution;
pub mod metrics;

pub use attribution::attribute;
pub use metrics::populate;
