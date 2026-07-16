//! The `#[pyclass] BacktestConfig` wrapper and its chainable builders.
//!
//! A thin, integer-cents surface over the public [`crate::BacktestConfig`]
//! ([docs/06 §4](../../docs/06-python-bindings.md)). Each builder mutates the
//! wrapper **in place and returns it** for fluent chaining
//! (`ic.BacktestConfig(...).data_parquet(...).strategy_iron_condor(...)`); the
//! wrapper carries no business logic — [`BacktestConfig::to_rust`] marshals the
//! accumulated state into the real Rust config + strategy + exit policy and runs
//! [`crate::BacktestConfig::validate`] **once, at the boundary**.
//!
//! # Money is integer cents ([ADR-0003](../../docs/adr/0003-money-as-integer-cents.md))
//!
//! Every money argument names its unit in cents (`capital_cents`,
//! `strike_cents`, `premium_*_cents`, `*_fee_cents`, `slippage_cents`) so a
//! Python caller never guesses units and no `f64` money crosses the boundary.
//! The **only** floats accepted are the documented analytic exception — the
//! per-leg implied volatility and the two rates, and the `ExitPolicy`
//! percentages — which convert to `Decimal` at the boundary.
//!
//! # What is deliberately NOT exposed (the deferred composition-root dispatch)
//!
//! [`crate::run_backtest`] currently wires only a **Parquet** data source and
//! the **iron condor** strategy; a CSV source or a short strangle spec returns a
//! typed error there (the dispatch matrix is deferred). So this wrapper exposes
//! only [`BacktestConfig::data_parquet`] and
//! [`BacktestConfig::strategy_iron_condor`] — no `data_csv`, no
//! `strategy_short_strangle` — to keep the Python surface to what genuinely runs
//! end to end. Both execution modes **do** work
//! ([`BacktestConfig::execution_naive`] / [`BacktestConfig::execution_realistic`],
//! #26), so both are exposed; realistic mode needs a wheel built with the
//! `orderbook` feature or [`crate::run_backtest`] returns a typed config error.

use std::path::PathBuf;

use chrono::DateTime;
use optionstratlib::ExpirationDate;
use optionstratlib::simulation::ExitPolicy;
use pyo3::prelude::*;
use rust_decimal::Decimal;

use super::errors::{guard_boundary, to_pyerr};
use crate::BacktestConfig as RustBacktestConfig;
use crate::config::{FeeSchedule, LiquidityProfile, ResourceLimits, SlippageModel};
use crate::data::DataSourceSpec;
use crate::domain::{
    ExecutionMode, IronCondorSpec, PriceCents, Quantity, StrategySpec, Underlying,
};
use crate::error::BacktestError;

/// The default bundle output directory when the caller sets none — operational
/// only (excluded from the `run_id` hash), so a default never changes a run.
const DEFAULT_OUTPUT_DIR: &str = "ironcondor_runs";

/// The default marketable price cap in ticks (realistic mode), matching the Rust
/// config default ([docs/04 §5.2](../../docs/04-execution-models.md)).
const DEFAULT_MARKETABLE_CAP_TICKS: u32 = 10;

/// The Python `ironcondor.BacktestConfig` — an integer-cents builder over the
/// public [`crate::BacktestConfig`].
///
/// Constructed with an explicit `seed` and starting `capital_cents`, then
/// configured through chainable builder methods. Required inputs (a data source
/// and a strategy) are validated at [`Self::to_rust`], which errors clearly if
/// either is missing.
#[pyclass(name = "BacktestConfig", module = "ironcondor")]
pub struct PyBacktestConfig {
    /// Engine RNG seed (determinism).
    seed: u64,
    /// Starting capital in integer cents (`> 0`).
    capital_cents: u64,
    /// The Parquet chain path, once `data_parquet` is called.
    data_parquet_path: Option<String>,
    /// The strategy spec, once `strategy_iron_condor` is called.
    strategy: Option<StrategySpec>,
    /// The execution mode (`Naive` default; `Realistic` via `execution_realistic`).
    mode: ExecutionMode,
    /// The naive-mode slippage model (ignored by realistic mode).
    slippage: SlippageModel,
    /// Broker/exchange fees in integer cents.
    fees: FeeSchedule,
    /// The exit policy (`Expiration` default — hold to expiry).
    exit: ExitPolicy,
    /// Realistic-mode marketable price cap in ticks.
    marketable_cap_ticks: u32,
    /// Realistic-mode book-seeding profile (default).
    liquidity_profile: LiquidityProfile,
    /// Untrusted-input resource ceilings (default).
    limits: ResourceLimits,
    /// Where the result bundle is written; defaults to [`DEFAULT_OUTPUT_DIR`].
    output_dir: Option<PathBuf>,
    /// Whether an existing same-`run_id` bundle directory may be replaced.
    overwrite: bool,
}

#[pymethods]
impl PyBacktestConfig {
    /// Create a config with an explicit `seed` and starting `capital_cents`.
    ///
    /// `capital_cents` is integer cents and must be `> 0` (checked at
    /// [`Self::to_rust`]). Defaults: `seed = 0`, `capital_cents = 1_000_000`
    /// (`$10,000`), execution mode naive, no fees, no slippage, hold-to-expiry
    /// exit.
    #[new]
    #[pyo3(signature = (seed = 0, capital_cents = 1_000_000))]
    #[must_use]
    fn new(seed: u64, capital_cents: u64) -> Self {
        Self {
            seed,
            capital_cents,
            data_parquet_path: None,
            strategy: None,
            mode: ExecutionMode::Naive,
            slippage: SlippageModel::None,
            fees: FeeSchedule {
                per_contract_cents: 0,
                per_order_cents: 0,
            },
            exit: ExitPolicy::Expiration,
            marketable_cap_ticks: DEFAULT_MARKETABLE_CAP_TICKS,
            liquidity_profile: LiquidityProfile::default(),
            limits: ResourceLimits::default(),
            output_dir: None,
            overwrite: false,
        }
    }

    /// Set the engine RNG seed (determinism).
    fn seed<'py>(mut slf: PyRefMut<'py, Self>, seed: u64) -> PyRefMut<'py, Self> {
        slf.seed = seed;
        slf
    }

    /// Set the starting capital in integer cents (`> 0`).
    fn capital_cents<'py>(mut slf: PyRefMut<'py, Self>, capital_cents: u64) -> PyRefMut<'py, Self> {
        slf.capital_cents = capital_cents;
        slf
    }

    /// Use a single-file Parquet chain at `path` as the data source.
    ///
    /// The only source [`crate::run_backtest`] wires today (a CSV source is a
    /// deferred dispatch and is intentionally not exposed here).
    fn data_parquet<'py>(mut slf: PyRefMut<'py, Self>, path: String) -> PyRefMut<'py, Self> {
        slf.data_parquet_path = Some(path);
        slf
    }

    /// Define the run's iron condor strategy from the full `IronCondorSpec`
    /// parameter set (money in integer cents, the expiry as ns since the Unix
    /// epoch, the analytic vol/rate fields as decimal fractions).
    ///
    /// # Errors
    ///
    /// Raises `ic.DataError` if the underlying ticker violates its grammar (an
    /// upstream conversion check), and `ic.ConfigError` (⊂ `ValueError`) if the
    /// quantity is zero or a vol/rate field is not a representable decimal.
    #[pyo3(signature = (
        underlying,
        underlying_price_cents,
        short_call_strike_cents,
        short_put_strike_cents,
        long_call_strike_cents,
        long_put_strike_cents,
        expiration_ns,
        quantity,
        premium_short_call_cents,
        premium_short_put_cents,
        premium_long_call_cents,
        premium_long_put_cents,
        implied_volatility = 0.20,
        risk_free_rate = 0.05,
        dividend_yield = 0.0,
        open_fee_cents = 0,
        close_fee_cents = 0,
    ))]
    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per IronCondorSpec construction field (the full v0.1 parameter set); the builder centralises the marshalling in one place"
    )]
    fn strategy_iron_condor<'py>(
        mut slf: PyRefMut<'py, Self>,
        underlying: String,
        underlying_price_cents: u64,
        short_call_strike_cents: u64,
        short_put_strike_cents: u64,
        long_call_strike_cents: u64,
        long_put_strike_cents: u64,
        expiration_ns: i64,
        quantity: u32,
        premium_short_call_cents: u64,
        premium_short_put_cents: u64,
        premium_long_call_cents: u64,
        premium_long_put_cents: u64,
        implied_volatility: f64,
        risk_free_rate: f64,
        dividend_yield: f64,
        open_fee_cents: u64,
        close_fee_cents: u64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        guard_boundary(move || {
            let underlying = Underlying::new(underlying).map_err(|e| to_pyerr(py, e))?;
            let quantity = Quantity::new(quantity).map_err(|e| to_pyerr(py, e))?;
            let spec = IronCondorSpec {
                underlying,
                underlying_price: PriceCents::new(underlying_price_cents),
                short_call_strike: PriceCents::new(short_call_strike_cents),
                short_put_strike: PriceCents::new(short_put_strike_cents),
                long_call_strike: PriceCents::new(long_call_strike_cents),
                long_put_strike: PriceCents::new(long_put_strike_cents),
                expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(expiration_ns)),
                implied_volatility: decimal_from_f64(implied_volatility, "implied_volatility")
                    .map_err(|e| to_pyerr(py, e))?,
                risk_free_rate: decimal_from_f64(risk_free_rate, "risk_free_rate")
                    .map_err(|e| to_pyerr(py, e))?,
                dividend_yield: decimal_from_f64(dividend_yield, "dividend_yield")
                    .map_err(|e| to_pyerr(py, e))?,
                quantity,
                premium_short_call: PriceCents::new(premium_short_call_cents),
                premium_short_put: PriceCents::new(premium_short_put_cents),
                premium_long_call: PriceCents::new(premium_long_call_cents),
                premium_long_put: PriceCents::new(premium_long_put_cents),
                open_fee: PriceCents::new(open_fee_cents),
                close_fee: PriceCents::new(close_fee_cents),
            };
            slf.strategy = Some(StrategySpec::IronCondor(spec));
            Ok(slf)
        })
    }

    /// Select naive execution (mid/spread) with an optional flat adverse
    /// slippage in integer cents; omit `slippage_cents` for no slippage.
    #[pyo3(signature = (slippage_cents = None))]
    fn execution_naive<'py>(
        mut slf: PyRefMut<'py, Self>,
        slippage_cents: Option<u64>,
    ) -> PyRefMut<'py, Self> {
        slf.mode = ExecutionMode::Naive;
        slf.slippage = match slippage_cents {
            Some(cents) => SlippageModel::FixedCents { cents },
            None => SlippageModel::None,
        };
        slf
    }

    /// Select realistic execution (order-book matching). Requires a wheel built
    /// with the `orderbook` feature; otherwise `run()` raises a config error.
    /// Realistic slippage is emergent from the book, so any configured
    /// `slippage` is ignored.
    fn execution_realistic<'py>(mut slf: PyRefMut<'py, Self>) -> PyRefMut<'py, Self> {
        slf.mode = ExecutionMode::Realistic;
        slf
    }

    /// Set broker/exchange fees in integer cents (per contract and per order).
    #[pyo3(signature = (per_contract_cents = 0, per_order_cents = 0))]
    fn fees<'py>(
        mut slf: PyRefMut<'py, Self>,
        per_contract_cents: u64,
        per_order_cents: u64,
    ) -> PyRefMut<'py, Self> {
        slf.fees = FeeSchedule {
            per_contract_cents,
            per_order_cents,
        };
        slf
    }

    /// Exit when profit reaches `percent` of the initial premium
    /// (`optionstratlib::ExitPolicy::ProfitPercent`, e.g. `0.5` for 50%).
    ///
    /// # Errors
    ///
    /// Raises `ic.ConfigError` (⊂ `ValueError`) if `percent` is not a
    /// representable decimal.
    fn exit_profit_percent<'py>(
        mut slf: PyRefMut<'py, Self>,
        percent: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        guard_boundary(move || {
            let value = decimal_from_f64(percent, "profit percent").map_err(|e| to_pyerr(py, e))?;
            slf.exit = ExitPolicy::ProfitPercent(value);
            Ok(slf)
        })
    }

    /// Exit when loss reaches `percent` of the initial premium
    /// (`optionstratlib::ExitPolicy::LossPercent`).
    ///
    /// # Errors
    ///
    /// Raises `ic.ConfigError` (⊂ `ValueError`) if `percent` is not a
    /// representable decimal.
    fn exit_loss_percent<'py>(
        mut slf: PyRefMut<'py, Self>,
        percent: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        guard_boundary(move || {
            let value = decimal_from_f64(percent, "loss percent").map_err(|e| to_pyerr(py, e))?;
            slf.exit = ExitPolicy::LossPercent(value);
            Ok(slf)
        })
    }

    /// Exit after `steps` time steps (`optionstratlib::ExitPolicy::TimeSteps`).
    fn exit_time_steps<'py>(mut slf: PyRefMut<'py, Self>, steps: usize) -> PyRefMut<'py, Self> {
        slf.exit = ExitPolicy::TimeSteps(steps);
        slf
    }

    /// Hold every leg to expiration (`optionstratlib::ExitPolicy::Expiration`,
    /// the default).
    fn exit_expiration<'py>(mut slf: PyRefMut<'py, Self>) -> PyRefMut<'py, Self> {
        slf.exit = ExitPolicy::Expiration;
        slf
    }

    /// Set the directory the result bundle is written under (operational —
    /// excluded from the `run_id` hash).
    fn output_dir<'py>(mut slf: PyRefMut<'py, Self>, path: String) -> PyRefMut<'py, Self> {
        slf.output_dir = Some(PathBuf::from(path));
        slf
    }

    /// Allow replacing an existing bundle directory for the same `run_id`
    /// (operational — excluded from the `run_id` hash).
    #[pyo3(signature = (overwrite = true))]
    fn overwrite<'py>(mut slf: PyRefMut<'py, Self>, overwrite: bool) -> PyRefMut<'py, Self> {
        slf.overwrite = overwrite;
        slf
    }
}

impl PyBacktestConfig {
    /// Marshal the accumulated state into the real Rust config + strategy + exit
    /// policy, running [`crate::BacktestConfig::validate`] at the boundary.
    ///
    /// This is the single Rust ↔ Python config seam: money stays integer cents,
    /// the seed and every field map one-for-one, so a Python run marshals to the
    /// **same** `BacktestConfig` an equivalent Rust run builds (determinism
    /// parity, [docs/06 §9](../../docs/06-python-bindings.md); the byte-for-byte
    /// parity test is #42).
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Config`] if no data source or strategy has been
    /// configured, or if [`crate::BacktestConfig::validate`] rejects a field.
    pub(crate) fn to_rust(
        &self,
    ) -> Result<(RustBacktestConfig, StrategySpec, ExitPolicy), BacktestError> {
        let data_source = match &self.data_parquet_path {
            Some(path) => DataSourceSpec::Parquet {
                path: path.clone(),
                sha256: String::new(),
            },
            None => {
                return Err(BacktestError::Config(
                    "no data source configured: call .data_parquet(path)".to_string(),
                ));
            }
        };
        let strategy = self.strategy.clone().ok_or_else(|| {
            BacktestError::Config(
                "no strategy configured: call .strategy_iron_condor(...)".to_string(),
            )
        })?;
        let output_dir = self
            .output_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_DIR));

        let config = RustBacktestConfig {
            data_source,
            mode: self.mode,
            seed: self.seed,
            initial_capital: self.capital_cents,
            fees: self.fees,
            slippage: self.slippage.clone(),
            marketable_cap_ticks: self.marketable_cap_ticks,
            liquidity_profile: self.liquidity_profile,
            limits: self.limits,
            output_dir,
            overwrite: self.overwrite,
        };
        config.validate()?;
        Ok((config, strategy, self.exit.clone()))
    }
}

/// Convert an analytic `f64` (vol / rate / percent) to `Decimal` at the
/// boundary — the documented exception to integer-cents money.
///
/// Returns a [`BacktestError::Config`] (mapped to `ic.ConfigError` ⊂
/// `ValueError` by [`to_pyerr`]) so a bad analytic field flows through the
/// single error seam like every other input error, instead of a bare
/// `ValueError` that `except ic.IronCondorError` would miss.
///
/// # Errors
///
/// Returns [`BacktestError::Config`] if `value` is `NaN` / `±∞` or otherwise
/// not a representable decimal.
fn decimal_from_f64(value: f64, field: &str) -> Result<Decimal, BacktestError> {
    Decimal::try_from(value).map_err(|_| {
        BacktestError::Config(format!("{field} {value} is not a representable decimal"))
    })
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use optionstratlib::simulation::ExitPolicy;
    use rust_decimal::Decimal;

    use super::{DEFAULT_OUTPUT_DIR, PyBacktestConfig};
    use crate::config::{
        BacktestConfig, FeeSchedule, LiquidityProfile, ResourceLimits, SlippageModel,
    };
    use crate::data::DataSourceSpec;
    use crate::domain::{ExecutionMode, StrategySpec};

    /// The tape anchor `ts_0` and the `ts_0 + 30 days` expiry, matching
    /// `tests/common` and the `iron_condor_naive` golden.
    const TS0_NS: i64 = 1_750_291_200_000_000_000;
    /// The four condor legs' absolute expiry (`ts_0 + 30 days`).
    const EXPIRY_NS: i64 = TS0_NS + 30 * 86_400_000_000_000;

    /// A directly-constructed wrapper (bypassing the GIL-bound builders) so
    /// `to_rust` marshalling is unit-testable without a live interpreter.
    fn base() -> PyBacktestConfig {
        PyBacktestConfig {
            seed: 7,
            capital_cents: 10_000_000,
            data_parquet_path: Some("chains/spx.parquet".to_string()),
            strategy: Some(strategy()),
            mode: ExecutionMode::Naive,
            slippage: SlippageModel::None,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            exit: ExitPolicy::TimeSteps(1_000_000),
            marketable_cap_ticks: 10,
            liquidity_profile: LiquidityProfile::default(),
            limits: ResourceLimits::default(),
            output_dir: None,
            overwrite: false,
        }
    }

    fn strategy() -> StrategySpec {
        use chrono::DateTime;
        use optionstratlib::ExpirationDate;

        use crate::domain::{IronCondorSpec, PriceCents, Quantity, Underlying};

        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        let Ok(quantity) = Quantity::new(1) else {
            panic!("1 is a valid quantity");
        };
        StrategySpec::IronCondor(IronCondorSpec {
            underlying,
            underlying_price: PriceCents::new(500_000),
            short_call_strike: PriceCents::new(510_000),
            short_put_strike: PriceCents::new(490_000),
            long_call_strike: PriceCents::new(520_000),
            long_put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                1_750_291_200_000_000_000,
            )),
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

    #[test]
    fn test_to_rust_marshals_every_field_and_preserves_cents() {
        let cfg = base();
        let Ok((rust, strat, exit)) = cfg.to_rust() else {
            panic!("a complete config must marshal");
        };
        // Cents preserved exactly (no f64 money).
        assert_eq!(rust.initial_capital, 10_000_000);
        assert_eq!(rust.seed, 7);
        assert_eq!(rust.fees.per_contract_cents, 65);
        assert_eq!(rust.fees.per_order_cents, 100);
        assert_eq!(rust.mode, ExecutionMode::Naive);
        // The Parquet source marshals with an empty (unpinned) sha256.
        assert!(matches!(
            rust.data_source,
            DataSourceSpec::Parquet { ref path, ref sha256 }
                if path == "chains/spx.parquet" && sha256.is_empty()
        ));
        // The strategy round-trips unchanged; the exit policy is carried through.
        assert_eq!(strat, strategy());
        assert_eq!(exit, ExitPolicy::TimeSteps(1_000_000));
        // The default output dir is applied when the caller sets none.
        assert_eq!(
            rust.output_dir,
            std::path::PathBuf::from(DEFAULT_OUTPUT_DIR)
        );
        // The marshalled config passes its own validation.
        assert!(rust.validate().is_ok());
    }

    #[test]
    fn test_to_rust_errors_when_data_source_missing() {
        let mut cfg = base();
        cfg.data_parquet_path = None;
        assert!(matches!(
            cfg.to_rust(),
            Err(crate::error::BacktestError::Config(msg)) if msg.contains("data source")
        ));
    }

    #[test]
    fn test_to_rust_errors_when_strategy_missing() {
        let mut cfg = base();
        cfg.strategy = None;
        assert!(matches!(
            cfg.to_rust(),
            Err(crate::error::BacktestError::Config(msg)) if msg.contains("strategy")
        ));
    }

    #[test]
    fn test_to_rust_propagates_config_validation_error() {
        // Zero capital is rejected by BacktestConfig::validate at the boundary.
        let mut cfg = base();
        cfg.capital_cents = 0;
        assert!(matches!(
            cfg.to_rust(),
            Err(crate::error::BacktestError::Config(msg)) if msg.contains("initial capital")
        ));
    }

    #[test]
    fn test_to_rust_carries_realistic_mode() {
        let mut cfg = base();
        cfg.mode = ExecutionMode::Realistic;
        let Ok((rust, _, _)) = cfg.to_rust() else {
            panic!("realistic-mode config marshals (the feature gate is checked in run_backtest)");
        };
        assert_eq!(rust.mode, ExecutionMode::Realistic);
    }

    /// The iron-condor spec the **binding** marshals for the shared #42 parity
    /// scenario. The analytic fields go through the *same* `Decimal::try_from`
    /// conversion as [`super::decimal_from_f64`] (so `0.20 → "0.2"`), which is
    /// value-equal to but serialises differently from the golden's
    /// `Decimal::new(20, 2)`; the parity manifest / `run_id` are byte-stable
    /// precisely because both sides pick the `try_from` scale.
    fn parity_strategy() -> StrategySpec {
        use chrono::DateTime;
        use optionstratlib::ExpirationDate;

        use crate::domain::{IronCondorSpec, PriceCents, Quantity, Underlying};

        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        let Ok(quantity) = Quantity::new(1) else {
            panic!("1 is a valid quantity");
        };
        let Ok(iv) = Decimal::try_from(0.20_f64) else {
            panic!("0.20 is a representable decimal");
        };
        let Ok(rate) = Decimal::try_from(0.05_f64) else {
            panic!("0.05 is a representable decimal");
        };
        let Ok(dividend) = Decimal::try_from(0.0_f64) else {
            panic!("0.0 is a representable decimal");
        };
        StrategySpec::IronCondor(IronCondorSpec {
            underlying,
            underlying_price: PriceCents::new(500_000),
            short_call_strike: PriceCents::new(510_000),
            short_put_strike: PriceCents::new(490_000),
            long_call_strike: PriceCents::new(520_000),
            long_put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXPIRY_NS)),
            implied_volatility: iv,
            risk_free_rate: rate,
            dividend_yield: dividend,
            quantity,
            premium_short_call: PriceCents::new(2_000),
            premium_short_put: PriceCents::new(1_800),
            premium_long_call: PriceCents::new(800),
            premium_long_put: PriceCents::new(700),
            open_fee: PriceCents::new(65),
            close_fee: PriceCents::new(65),
        })
    }

    /// **Config-marshal equality for the shared #42 parity scenario.** The Python
    /// `BacktestConfig` for the canonical iron-condor run marshals to *exactly*
    /// the Rust `BacktestConfig` + `StrategySpec` + `ExitPolicy` the Rust API
    /// consumes (`tests/python_parity.rs::run_rust_path`). This is the struct-level
    /// proof that "the same inputs" reach both surfaces — the load-bearing
    /// precondition for the byte-identical bundle the integration parity test
    /// asserts. Money stays integer cents throughout; the analytic fields are the
    /// documented `Decimal` exception, converted identically on both sides.
    #[test]
    fn test_to_rust_matches_the_shared_parity_scenario() {
        let cfg = PyBacktestConfig {
            seed: 42,
            capital_cents: 10_000_000,
            data_parquet_path: Some("iron_condor.parquet".to_string()),
            strategy: Some(parity_strategy()),
            mode: ExecutionMode::Naive,
            slippage: SlippageModel::None,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            exit: ExitPolicy::TimeSteps(1_000_000),
            marketable_cap_ticks: 10,
            liquidity_profile: LiquidityProfile::default(),
            limits: ResourceLimits::default(),
            output_dir: Some(std::path::PathBuf::from("runs/out")),
            overwrite: false,
        };

        // The exact Rust config `tests/python_parity.rs::run_rust_path` builds
        // (`common::condor_config` with a redirected output_dir).
        let expected = BacktestConfig {
            data_source: DataSourceSpec::Parquet {
                path: "iron_condor.parquet".to_string(),
                sha256: String::new(),
            },
            mode: ExecutionMode::Naive,
            seed: 42,
            initial_capital: 10_000_000,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            slippage: SlippageModel::None,
            marketable_cap_ticks: 10,
            liquidity_profile: LiquidityProfile::default(),
            limits: ResourceLimits::default(),
            output_dir: std::path::PathBuf::from("runs/out"),
            overwrite: false,
        };

        let Ok((rust, strat, exit)) = cfg.to_rust() else {
            panic!("the shared parity config must marshal");
        };
        assert_eq!(
            rust, expected,
            "the marshalled BacktestConfig must equal the Rust parity config field-for-field"
        );
        assert_eq!(
            strat,
            parity_strategy(),
            "the marshalled strategy must equal the Rust parity spec (identical integer cents + decimals)"
        );
        assert_eq!(
            exit,
            ExitPolicy::TimeSteps(1_000_000),
            "the exit policy must carry through unchanged"
        );
    }
}
