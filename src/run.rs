//! The v0.1 end-to-end entry: **Parquet in, equity curve out**.
//!
//! [`run_backtest`] is the thin composition root that ties the v0.1 vertical
//! slice together — the Parquet historical feed (#9), the `IronCondor` strategy
//! adapter (#11), the naive fill model (#13), the replay loop + mark-to-market
//! ledger (#14/#15), and the minimal summary metrics (#16) — into one call.
//!
//! # Layering: why this lives above both `engine` and `analytics`
//!
//! `analytics` **consumes** engine output; the engine must therefore **not**
//! import `analytics` (that would invert the `analytics → engine output`
//! dependency, [CLAUDE.md](../../CLAUDE.md) Module Boundaries). So the "run,
//! then compute metrics" orchestration cannot live inside
//! [`crate::engine::BacktestEngine::run`]. It lives **here**, at the crate top
//! level — a composition root that sits *above* both layers, calls
//! [`crate::engine::BacktestEngine::run`] first, then
//! [`crate::analytics::metrics::populate`] on its output. The engine stays
//! analytics-free; analytics stays engine-free; this module depends on both.
//!
//! # Determinism
//!
//! `run_backtest` adds no wall clock and no RNG of its own — the engine owns
//! the sole seeded RNG and the metrics are a pure function of the equity
//! series, so `(seed, config, data)` is byte-reproducible
//! ([docs/02 §7](../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use optionstratlib::simulation::ExitPolicy;
use optionstratlib::strategies::IronCondor;

use crate::analytics::metrics;
use crate::config::BacktestConfig;
use crate::data::{DataSourceSpec, ParquetFeed};
use crate::domain::StrategySpec;
use crate::engine::{BacktestEngine, BacktestRun, OptStratAdapter};
use crate::error::BacktestError;
use crate::execution::NaiveFill;

/// Run one v0.1 naive backtest end to end and return the populated
/// [`BacktestRun`] — the ordered `equity_curve` plus the upstream
/// [`optionstratlib::backtesting::BacktestResult`] with the minimal summary
/// metrics filled in ([`metrics::populate`]).
///
/// The feed and execution model are built **from `config`**: a
/// [`DataSourceSpec::Parquet`] source opens a [`ParquetFeed`] under
/// `config.limits`, and the naive [`NaiveFill`] takes `config.slippage` and
/// `config.fees`. `strategy_spec` (the single v0.1 [`StrategySpec::IronCondor`]
/// kind) is wrapped with `exit` through the strategy adapter.
///
/// The primary v0.1 artifact is the ordered `run.equity_curve`
/// (`Vec<EquityPoint>`, integer cents + the one drawdown float); the result
/// bundle (`manifest.json` + Parquet tables) is v0.3 and is **not** written
/// here.
///
/// # Errors
///
/// - [`BacktestError::Config`] if the config fails [`BacktestConfig::validate`],
///   or the data source is not a Parquet feed (the sole v0.1 feed), or the
///   initial capital exceeds the `i64` cents range.
/// - [`BacktestError::Data`] / [`BacktestError::DataIo`] if the Parquet feed
///   cannot be opened or its tape is malformed.
/// - [`BacktestError::Strategy`] / [`BacktestError::Conversion`] if the strategy
///   spec cannot be constructed.
/// - Any [`BacktestError`] the replay loop or the metrics pass raises
///   (including [`BacktestError::ArithmeticOverflow`]).
pub fn run_backtest(
    config: &BacktestConfig,
    strategy_spec: &StrategySpec,
    exit: ExitPolicy,
) -> Result<BacktestRun, BacktestError> {
    config.validate()?;

    // Build the feed from the config's data source — Parquet is the sole v0.1
    // feed (CSV is v0.2, the simulator feed v0.5).
    let path = match &config.data_source {
        DataSourceSpec::Parquet { path, .. } => path.clone(),
        other => {
            return Err(BacktestError::Config(format!(
                "run_backtest supports only a parquet data source in v0.1, got {other:?}"
            )));
        }
    };
    let feed = ParquetFeed::open(&path, &config.limits)?;

    // Naive execution + the iron-condor strategy adapter, both from config/spec.
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    let adapter = OptStratAdapter::<IronCondor>::from_spec(strategy_spec, exit)?;
    let strategy_name = strategy_spec.kind();

    // Engine first (produces the equity curve + minimally-populated result)…
    let mut run = BacktestEngine::run(config, feed, execution, adapter, strategy_name)?;

    // …then analytics consumes that output to fill the summary metrics.
    let initial_capital_cents = i64::try_from(config.initial_capital).map_err(|_| {
        BacktestError::Config("initial capital exceeds the i64 cents range".to_string())
    })?;
    metrics::populate(&mut run.result, &run.equity_curve, initial_capital_cents)?;

    Ok(run)
}
