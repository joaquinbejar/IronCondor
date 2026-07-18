//! The end-to-end entry: **Parquet in, equity curve out**.
//!
//! [`run_backtest`] is the thin composition root that ties the vertical
//! slice together — the Parquet historical feed (#9), the `IronCondor` strategy
//! adapter (#11), the config-selected fill model (naive #13 / realistic #24,
//! selected by `config.mode` at startup #26), the replay loop + mark-to-market
//! ledger (#14/#15), and the minimal summary metrics (#16) — into one call.
//!
//! # Layering: why this lives above both `engine` and `analytics`
//!
//! `analytics` **consumes** engine output; the engine must therefore **not**
//! import `analytics` (that would invert the `analytics → engine output`
//! dependency, [CLAUDE.md](../../CLAUDE.md) Module Boundaries). So the "run,
//! then compute analytics" orchestration cannot live inside
//! [`crate::engine::BacktestEngine::run`]. It lives **here**, at the crate top
//! level — a composition root that sits *above* both layers, calls
//! [`crate::engine::BacktestEngine::run`] first, then
//! [`crate::analytics::metrics::populate`] and
//! [`crate::analytics::attribution::attribute`] on its output (the P&L
//! attribution by Greek, #31, reads the engine's owned attribution substrate
//! and fills `run.greeks_attribution`). The engine stays analytics-free;
//! analytics stays engine-loop-free; this module depends on both.
//!
//! # Determinism
//!
//! `run_backtest` adds no wall clock and no RNG of its own — the engine owns
//! the sole seeded RNG and the metrics are a pure function of the equity
//! series, so `(seed, config, data)` is byte-reproducible
//! ([docs/02 §7](../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use optionstratlib::simulation::ExitPolicy;
use optionstratlib::strategies::IronCondor;

use crate::analytics::{attribution, metrics};
use crate::config::BacktestConfig;
use crate::data::{DataSourceSpec, ParquetFeed};
use crate::domain::{ExecutionMode, StrategySpec};
use crate::engine::{BacktestEngine, BacktestRun, OptStratAdapter};
use crate::error::BacktestError;
use crate::execution::NaiveFill;
#[cfg(feature = "orderbook")]
use crate::execution::RealisticFill;

/// Run one backtest end to end and return the populated [`BacktestRun`] — the
/// ordered `equity_curve` plus the upstream
/// [`optionstratlib::backtesting::BacktestResult`] with the minimal summary
/// metrics filled in ([`metrics::populate`]).
///
/// The feed and execution model are built **from `config`**: a
/// [`DataSourceSpec::Parquet`] source opens a [`ParquetFeed`] under
/// `config.limits`, and `config.mode` selects the [`crate::execution::ExecutionModel`]
/// once at startup ([docs/04 §2](../../docs/04-execution-models.md#2-the-executionmodel-trait-and-the-shared-fill-report)):
///
/// - [`ExecutionMode::Naive`] builds a [`NaiveFill`] from `config.slippage` and
///   `config.fees`;
/// - [`ExecutionMode::Realistic`] (feature `orderbook`) builds a `RealisticFill`
///   from `config.fees`, `config.marketable_cap_ticks`, `config.seed`, and
///   `config.liquidity_profile`.
///
/// The selected model fixes the concrete `X: ExecutionModel` of the
/// monomorphised [`BacktestEngine::run`], so the loop has **no per-step `dyn`
/// dispatch**; the strategy runs unchanged under either mode (the mode is
/// configuration, not a code path the strategy sees). This composition root
/// wires the [`StrategySpec::IronCondor`] kind, wrapping `strategy_spec` with
/// `exit` through `OptStratAdapter::<IronCondor>::from_spec`; a
/// [`StrategySpec::ShortStrangle`] (v0.2 breadth, #28) is a typed
/// [`BacktestError::Strategy`] here rather than being wired into `run_backtest`
/// (it is driven directly through the same generic adapter and engine — see
/// `OptStratAdapter::<ShortStrangle>::from_spec` and the `short_strangle_naive`
/// golden).
///
/// The primary artifact is the ordered `run.equity_curve`
/// (`Vec<EquityPoint>`, integer cents + the one drawdown float); the result
/// bundle (`manifest.json` + Parquet tables) is v0.3 and is **not** written
/// here.
///
/// # Errors
///
/// - [`BacktestError::Config`] if the config fails [`BacktestConfig::validate`],
///   or the data source is not a Parquet feed (the sole v0.1 feed), or
///   `config.mode` is [`ExecutionMode::Realistic`] but the crate was built
///   without the `orderbook` feature (realistic execution is unavailable), or
///   the initial capital exceeds the `i64` cents range.
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

    // The iron-condor strategy adapter (mode-agnostic — the strategy never sees
    // which fill model runs). `feed` and `adapter` are built once and moved into
    // whichever mutually-exclusive `config.mode` arm executes below.
    let adapter = OptStratAdapter::<IronCondor>::from_spec(strategy_spec, exit)?;
    let strategy_name = strategy_spec.kind();

    // Config-driven fill-model dispatch (#26): `config.mode` selects the concrete
    // `ExecutionModel` ONCE, here at startup, so the loop stays monomorphised
    // over `X: ExecutionModel` — no per-step `dyn`. Both arms return the SAME
    // `BacktestRun` type, so analytics downstream is mode-agnostic.
    let mut run = match config.mode {
        ExecutionMode::Naive => {
            let execution = NaiveFill::new(config.slippage.clone(), config.fees);
            BacktestEngine::run(config, feed, execution, adapter, strategy_name)?
        }
        // Realistic mode is behind the `orderbook` feature: the matching engine
        // and the `RealisticFill` adapter only compile with it. Built from the
        // realistic knobs (fees, marketable cap, seed, liquidity profile);
        // `config.slippage` is deliberately unused (realistic slippage is
        // emergent from the book, docs/04 §4).
        #[cfg(feature = "orderbook")]
        ExecutionMode::Realistic => {
            let execution = RealisticFill::with_liquidity_profile(
                config.fees,
                config.marketable_cap_ticks,
                config.seed,
                config.liquidity_profile,
            );
            BacktestEngine::run(config, feed, execution, adapter, strategy_name)?
        }
        // Without the feature, realistic mode is a typed config error rather than
        // a silent fallback to naive (which would misreport fill risk).
        #[cfg(not(feature = "orderbook"))]
        ExecutionMode::Realistic => {
            return Err(BacktestError::Config(
                "realistic execution mode requires the `orderbook` feature".to_string(),
            ));
        }
    };

    // …then analytics consumes that output to fill the summary metrics.
    let initial_capital_cents = i64::try_from(config.initial_capital).map_err(|_| {
        BacktestError::Config("initial capital exceeds the i64 cents range".to_string())
    })?;
    metrics::populate(&mut run.result, &run.equity_curve, initial_capital_cents)?;

    // …and the per-step P&L attribution by Greek, decomposed from the engine's
    // owned attribution substrate ([`crate::engine::substrate`]) into one
    // `GreeksAttributionRow` per step, with an exact integer-cents residual
    // (#31). The engine leaves `run.greeks_attribution` empty; analytics fills
    // it here — the same layering as `metrics::populate` above.
    run.greeks_attribution = attribution::attribute(&run.attribution_substrate)?;

    Ok(run)
}
