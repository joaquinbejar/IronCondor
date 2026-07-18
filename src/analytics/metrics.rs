//! Summary metrics from the mark-to-market equity series and the per-trade log.
//!
//! Analytics **consumes** the engine's output — the ordered
//! [`crate::domain::EquityPoint`] series the ledger emits
//! ([docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger))
//! and the owned [`ClosedTrade`] log the loop collected
//! ([`crate::engine::tradelog`]) — and **populates the result structs that
//! already exist upstream** in `optionstratlib::backtesting`; it never invents
//! parallel result types
//! ([ADR-0001](../../../docs/adr/0001-migrate-optionstratbacktest-core.md),
//! [docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics),
//! [specs/optionstratlib.md §6](../../../docs/specs/optionstratlib.md#6-backtesting-result-and-metric-types)).
//! It imports `domain`, `error`, `optionstratlib`, and the engine's **output**
//! types ([`ClosedTrade`], the same way [`crate::analytics::attribution`]
//! consumes the attribution substrate) — **never** the `engine` loop — so the
//! layering (`analytics → engine output`) is not inverted.
//!
//! # What [`populate`] fills (and the mapping to upstream structs)
//!
//! Following the metric-family → struct mapping in
//! [docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics),
//! [`populate`] fills, in place, on the upstream [`BacktestResult`]:
//!
//! - **[`GeneralPerformanceMetrics`]** — `total_return`; per-step `sharpe_ratio`,
//!   `sortino_ratio`, `volatility`, `downside_deviation` from the equity
//!   returns; and the win/loss slice (`win_rate`, `profit_factor`, `avg_gain`,
//!   `avg_loss`, `gain_loss_ratio`) from the trade log.
//! - **[`TradeStatistics`]** — per-leg realised counts, average/median trade
//!   return, largest win/loss, holding periods, and the long/short/call/put
//!   composition. A condor's short strikes and long wings are attributed
//!   **separately** by side (also surfaced in `custom_metrics`, below).
//! - **[`DrawdownAnalysis`]** / **[`DrawdownEvent`]** — the worst drawdown from
//!   the ledger's running-peak series, both as a ratio (`max_drawdown`,
//!   `magnitude`) and a cents magnitude (`custom_metrics`), plus its
//!   start/bottom/recovery dates and the underwater span.
//! - **[`CapitalUtilization`]** — premium received / paid / net from the trade
//!   log's entry legs.
//! - **[`OptionsSpecificMetrics`]** — the call/put and long/short composition
//!   ratios, `return_on_premium`, and `premium_capture`.
//! - **[`AdvancedRiskMetrics`]** — `max_consecutive_losses`, the Ulcer index,
//!   and the Pain index from the return / drawdown series.
//!
//! ## Sharpe & the risk-adjusted family live in `GeneralPerformanceMetrics`
//!
//! The docs/05 §4 table names *"Risk-adjusted → `AdvancedRiskMetrics` (Sharpe,
//! Sortino, …)"*, but the **actual upstream `AdvancedRiskMetrics` struct carries
//! no Sharpe/Sortino field** — those fields live on `GeneralPerformanceMetrics`
//! (`sharpe_ratio`, `sortino_ratio`, `calmar_ratio`). The rule to *populate the
//! real upstream field, never a parallel one* wins over the doc table's looser
//! grouping, so Sharpe/Sortino are written to `general_performance` and
//! `AdvancedRiskMetrics` is filled with its genuine tail-risk fields.
//!
//! ## Deliberately left `Default` (not fabricated)
//!
//! Fields that require inputs the run does **not** produce are left `Default`
//! rather than guessed: `annualized_return` / `calmar_ratio` (no annualization
//! factor is pinned by docs/05); the average Greek exposures on
//! `OptionsSpecificMetrics` (they need the per-step Greek series — the
//! attribution substrate — which this pass does not thread); the margin /
//! capital-in-use fields on `CapitalUtilization` (margin is not modelled); and
//! `AdvancedRiskMetrics`' VaR / Expected-Shortfall / tail-ratio (no confidence
//! method is pinned). `BacktestResult.trades` (`Vec<TradeRecord>`) is **left
//! empty**: a `TradeRecord` requires a `Position`, whose `Default` reads
//! `Utc::now()` and whose full construction needs entry-time market data
//! (IV, rate) the trade log does not carry — populating it would break
//! determinism or fabricate data, so the per-trade record is the owned
//! [`ClosedTrade`] log and the wire `TradeRecord`/`positions.parquet` assembly
//! is the bundle writer's concern (#33).
//!
//! # Money and floats
//!
//! IronCondor's **own** money stays integer cents — the drawdown cents magnitude
//! and the per-leg realised-P&L split are written into `custom_metrics` as
//! integer-valued `Decimal`s. The upstream struct `Decimal` money fields follow
//! the upstream convention (**dollars**, as the engine already sets
//! `initial_capital` / `final_capital`); ratios (Sharpe, drawdown, win rate) are
//! the documented analytic-float exception, guarded for `NaN`/`Inf` before they
//! enter a field.
//!
//! # Determinism
//!
//! Every metric is a pure function of `(equity series, trade log, open legs,
//! initial capital)` — no wall clock, no RNG, no `HashMap` iteration reaching an
//! ordering — so the same inputs always yield the same output
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use std::collections::BTreeMap;

use chrono::DateTime;
use optionstratlib::Side;
use optionstratlib::backtesting::{
    AdvancedRiskMetrics, BacktestResult, CapitalUtilization, DrawdownAnalysis, DrawdownEvent,
    GeneralPerformanceMetrics, OptionsSpecificMetrics, TradeStatistics,
};
use optionstratlib::prelude::Positive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{EquityPoint, OpenPosition};
use crate::engine::ClosedTrade;
use crate::error::BacktestError;

/// Nanoseconds in one 86 400 s calendar day (UTC) — the day convention holding
/// periods and drawdown durations share with expiry resolution
/// ([docs/01 §5.1](../../../docs/01-domain-model.md#51-expiration-resolves-to-one-absolute-instant)).
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// `custom_metrics` key for the signed worst-drawdown ratio (`≤ 0`).
pub const MAX_DRAWDOWN_RATIO_KEY: &str = "max_drawdown_ratio";
/// `custom_metrics` key for the max peak-to-trough decline in integer cents.
pub const MAX_DRAWDOWN_CENTS_KEY: &str = "max_drawdown_cents";
/// `custom_metrics` key for the net entry premium (received − paid) in cents.
pub const NET_PREMIUM_CENTS_KEY: &str = "net_premium_cents";
/// `custom_metrics` key for the total realised P&L across all closes, in cents.
pub const REALIZED_PNL_CENTS_KEY: &str = "realized_pnl_cents";
/// `custom_metrics` key for the realised P&L of the **short** legs (a condor's
/// short strikes), in integer cents.
pub const SHORT_LEGS_REALIZED_CENTS_KEY: &str = "short_legs_realized_cents";
/// `custom_metrics` key for the realised P&L of the **long** legs (a condor's
/// long wings), in integer cents.
pub const LONG_LEGS_REALIZED_CENTS_KEY: &str = "long_legs_realized_cents";

/// The single **manifest metrics projection** — the one record #33 serialises
/// into `manifest.metrics` ([docs/05 §6](../../../docs/05-analytics-and-reporting.md#6-manifestjson)).
///
/// It is a **thin projection** that bundles the populated
/// `optionstratlib::backtesting` **summary** structs (plus the IronCondor
/// integer-cents extras in `custom_metrics`), **not a parallel result type**: it
/// reinvents no metric and defines no second `BacktestResult` (ADR-0001). It
/// deliberately **excludes** the per-step / per-trade detail that lives in the
/// Parquet tables (`BacktestResult.trades`, `time_series`) and the Monte-Carlo
/// slice — the manifest carries the run-level metric structs only
/// ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
///
/// `custom_metrics` is a **[`BTreeMap`]** (sorted keys) so the projection
/// serialises deterministically — the upstream `BacktestResult.custom_metrics`
/// is a `HashMap`, whose iteration order must never reach the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    /// Core performance + risk-adjusted metrics (returns, Sharpe, Sortino,
    /// win/loss).
    pub general_performance: GeneralPerformanceMetrics,
    /// Options-specific composition and premium-return ratios.
    pub options_metrics: OptionsSpecificMetrics,
    /// Per-leg realised-trade statistics.
    pub trade_statistics: TradeStatistics,
    /// Drawdown analysis (ratio + dated worst event).
    pub drawdown_analysis: DrawdownAnalysis,
    /// Capital / premium utilisation.
    pub capital_utilization: CapitalUtilization,
    /// Tail-risk metrics (consecutive losses, Ulcer / Pain).
    pub risk_metrics: AdvancedRiskMetrics,
    /// IronCondor integer-cents extras (drawdown cents, per-leg realised split,
    /// net premium), in deterministic (sorted) key order.
    pub custom_metrics: BTreeMap<String, Decimal>,
}

impl Metrics {
    /// Project the manifest metrics out of a [`populate`]d [`BacktestResult`],
    /// cloning the summary sub-structs and re-keying `custom_metrics` into a
    /// sorted [`BTreeMap`] so the projection serialises deterministically.
    #[must_use]
    pub fn from_result(result: &BacktestResult) -> Self {
        Self {
            general_performance: result.general_performance.clone(),
            options_metrics: result.options_metrics.clone(),
            trade_statistics: result.trade_statistics.clone(),
            drawdown_analysis: result.drawdown_analysis.clone(),
            capital_utilization: result.capital_utilization.clone(),
            risk_metrics: result.risk_metrics.clone().unwrap_or_default(),
            custom_metrics: result
                .custom_metrics
                .iter()
                .map(|(key, value)| (key.clone(), *value))
                .collect(),
        }
    }
}

/// Populate the summary metrics into `result` in place, from the ledger's
/// ordered per-step `equity_curve`, the run's `initial_capital_cents`, the
/// per-leg `trade_log`, and the legs left `open_at_end`.
///
/// Every family listed in the [module docs](self) is filled with what the run
/// genuinely produces; fields needing inputs the run does not carry are left
/// `Default` (never fabricated). This **populates the upstream struct** — there
/// is no parallel IronCondor result type; the manifest projection is
/// [`Metrics::from_result`].
///
/// `initial_capital_cents` is the ledger's step-`−1` baseline (the value the
/// running drawdown peak is seeded with,
/// [docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)),
/// so `total_return` and the drawdown cents magnitude share the run's baseline.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] if the checked integer-cents
/// arithmetic (the peak-to-trough drawdown magnitude, or a realised-P&L
/// aggregate) overflows `i64`.
pub fn populate(
    result: &mut BacktestResult,
    equity_curve: &[EquityPoint],
    initial_capital_cents: i64,
    trade_log: &[ClosedTrade],
    open_at_end: &[OpenPosition],
) -> Result<(), BacktestError> {
    // --- returns → Sharpe / Sortino / volatility / downside deviation --------
    let returns = step_returns(equity_curve);
    populate_return_metrics(&mut result.general_performance, &returns);

    // --- total return: (final − initial) / initial, from integer cents -------
    let final_cents = equity_curve
        .last()
        .map_or(initial_capital_cents, |point| point.equity_cents);
    if initial_capital_cents != 0 {
        let delta = final_cents
            .checked_sub(initial_capital_cents)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if let Some(total_return) =
            Decimal::from(delta).checked_div(Decimal::from(initial_capital_cents))
        {
            result.general_performance.total_return = total_return;
        }
    }

    // --- win/loss slice + per-leg trade statistics from the trade log --------
    let realised = RealisedStats::from_log(trade_log)?;
    realised.populate_win_loss(&mut result.general_performance);
    result.trade_statistics = realised.trade_statistics(trade_log);
    result.options_metrics = realised.options_metrics();
    result.capital_utilization = realised.capital_utilization();

    // --- drawdown: full dated analysis, ratio + cents magnitude --------------
    result.drawdown_analysis = build_drawdown_analysis(equity_curve, initial_capital_cents)?;

    // --- advanced (tail) risk from the return / drawdown series --------------
    result.risk_metrics = Some(build_advanced_risk(equity_curve, &returns));

    // --- IronCondor integer-cents extras into custom_metrics (sorted-safe) ---
    let worst_ratio = worst_drawdown_ratio(equity_curve);
    if let Some(ratio) = Decimal::from_f64_retain(worst_ratio) {
        result
            .custom_metrics
            .insert(MAX_DRAWDOWN_RATIO_KEY.to_string(), ratio);
    }
    let drawdown_cents = max_drawdown_cents(equity_curve, initial_capital_cents)?;
    result.custom_metrics.insert(
        MAX_DRAWDOWN_CENTS_KEY.to_string(),
        Decimal::from(drawdown_cents),
    );
    result.custom_metrics.insert(
        REALIZED_PNL_CENTS_KEY.to_string(),
        Decimal::from(realised.total_cents),
    );
    result.custom_metrics.insert(
        SHORT_LEGS_REALIZED_CENTS_KEY.to_string(),
        Decimal::from(realised.short_cents),
    );
    result.custom_metrics.insert(
        LONG_LEGS_REALIZED_CENTS_KEY.to_string(),
        Decimal::from(realised.long_cents),
    );
    result.custom_metrics.insert(
        NET_PREMIUM_CENTS_KEY.to_string(),
        Decimal::from(net_premium_cents(trade_log, open_at_end)?),
    );

    Ok(())
}

/// Fill the per-step return metrics on `general` from the equity returns:
/// `volatility`, `sharpe_ratio`, `downside_deviation`, `sortino_ratio`.
fn populate_return_metrics(general: &mut GeneralPerformanceMetrics, returns: &[f64]) {
    let Some(mean) = mean(returns) else {
        return;
    };
    if let Some(stddev) = population_stddev(returns, mean) {
        if let Some(stddev_dec) = Decimal::from_f64_retain(stddev)
            && let Ok(volatility) = Positive::new_decimal(stddev_dec)
        {
            general.volatility = Some(volatility);
        }
        if let Some(sharpe) = ratio(mean, stddev) {
            general.sharpe_ratio = Decimal::from_f64_retain(sharpe);
        }
    }
    if let Some(downside) = downside_deviation(returns) {
        if let Some(downside_dec) = Decimal::from_f64_retain(downside)
            && let Ok(downside_pos) = Positive::new_decimal(downside_dec)
        {
            general.downside_deviation = Some(downside_pos);
        }
        if let Some(sortino) = ratio(mean, downside) {
            general.sortino_ratio = Decimal::from_f64_retain(sortino);
        }
    }
}

/// The realised-P&L aggregates over the trade log — the shared basis for the
/// win/loss, trade-statistics, options, and capital families. All money is
/// integer cents; the per-side split attributes a condor's short strikes and
/// long wings separately ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
struct RealisedStats {
    winners: usize,
    losers: usize,
    break_even: usize,
    gross_profit_cents: i128,
    gross_loss_cents: i128,
    total_cents: i64,
    short_cents: i64,
    long_cents: i64,
    long_trades: usize,
    short_trades: usize,
    call_trades: usize,
    put_trades: usize,
    /// Entry premium **received** on short legs (`Σ entry × quantity`), cents.
    entry_received_cents: i128,
    /// Entry premium **paid** on long legs (`Σ entry × quantity`), cents.
    entry_paid_cents: i128,
}

impl RealisedStats {
    /// Aggregate the trade log into the realised statistics.
    ///
    /// # Errors
    ///
    /// [`BacktestError::ArithmeticOverflow`] if a cents aggregate overflows.
    fn from_log(trade_log: &[ClosedTrade]) -> Result<Self, BacktestError> {
        let mut s = Self {
            winners: 0,
            losers: 0,
            break_even: 0,
            gross_profit_cents: 0,
            gross_loss_cents: 0,
            total_cents: 0,
            short_cents: 0,
            long_cents: 0,
            long_trades: 0,
            short_trades: 0,
            call_trades: 0,
            put_trades: 0,
            entry_received_cents: 0,
            entry_paid_cents: 0,
        };
        for trade in trade_log {
            let pnl = trade.realized_pnl.value();
            // Cash basis: `entry_premium × quantity × contract_multiplier`, the
            // same scaling `realized_pnl` carries — otherwise the premium
            // aggregates that feed CapitalUtilization / return_on_premium /
            // premium_capture are underweighted by the multiplier (F23).
            let premium = i128::from(trade.entry_premium.value())
                .checked_mul(i128::from(trade.quantity.value()))
                .and_then(|p| p.checked_mul(i128::from(trade.contract_multiplier)))
                .ok_or(BacktestError::ArithmeticOverflow)?;
            match pnl.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    s.winners += 1;
                    s.gross_profit_cents = s
                        .gross_profit_cents
                        .checked_add(i128::from(pnl))
                        .ok_or(BacktestError::ArithmeticOverflow)?;
                }
                std::cmp::Ordering::Less => {
                    s.losers += 1;
                    s.gross_loss_cents = s
                        .gross_loss_cents
                        .checked_add(i128::from(pnl))
                        .ok_or(BacktestError::ArithmeticOverflow)?;
                }
                std::cmp::Ordering::Equal => s.break_even += 1,
            }
            s.total_cents = s
                .total_cents
                .checked_add(pnl)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            match trade.side {
                Side::Short => {
                    s.short_trades += 1;
                    s.short_cents = s
                        .short_cents
                        .checked_add(pnl)
                        .ok_or(BacktestError::ArithmeticOverflow)?;
                    s.entry_received_cents = s
                        .entry_received_cents
                        .checked_add(premium)
                        .ok_or(BacktestError::ArithmeticOverflow)?;
                }
                Side::Long => {
                    s.long_trades += 1;
                    s.long_cents = s
                        .long_cents
                        .checked_add(pnl)
                        .ok_or(BacktestError::ArithmeticOverflow)?;
                    s.entry_paid_cents = s
                        .entry_paid_cents
                        .checked_add(premium)
                        .ok_or(BacktestError::ArithmeticOverflow)?;
                }
            }
            match trade.contract.style {
                optionstratlib::OptionStyle::Call => s.call_trades += 1,
                optionstratlib::OptionStyle::Put => s.put_trades += 1,
            }
        }
        Ok(s)
    }

    /// Total number of realised leg closes.
    const fn number_of_trades(&self) -> usize {
        self.winners + self.losers + self.break_even
    }

    /// Fill the win/loss slice of [`GeneralPerformanceMetrics`].
    fn populate_win_loss(&self, general: &mut GeneralPerformanceMetrics) {
        let n = self.number_of_trades();
        if n == 0 {
            return;
        }
        general.win_rate = checked_ratio(Decimal::from(self.winners), Decimal::from(n));
        // profit factor = gross profit / |gross loss| (None with no losses).
        if self.gross_loss_cents != 0 {
            general.profit_factor = checked_ratio(
                Decimal::from(self.gross_profit_cents),
                Decimal::from(self.gross_loss_cents.abs()),
            );
        }
        if self.winners > 0 {
            let avg = checked_ratio(
                Decimal::from(self.gross_profit_cents),
                Decimal::from(self.winners as i128),
            );
            general.avg_gain = avg.map(cents_to_dollars_dec);
        }
        if self.losers > 0 {
            let avg = checked_ratio(
                Decimal::from(self.gross_loss_cents),
                Decimal::from(self.losers as i128),
            );
            general.avg_loss = avg.map(cents_to_dollars_dec);
        }
        if let (Some(gain), Some(loss)) = (general.avg_gain, general.avg_loss)
            && !loss.is_zero()
        {
            general.gain_loss_ratio = gain.checked_div(loss.abs());
        }
    }

    /// Build the per-leg [`TradeStatistics`].
    fn trade_statistics(&self, trade_log: &[ClosedTrade]) -> TradeStatistics {
        let mut pnls_dollars: Vec<Decimal> = trade_log
            .iter()
            .map(|t| cents_to_dollars(t.realized_pnl.value()))
            .collect();
        pnls_dollars.sort();
        let mut holding_days: Vec<Positive> = trade_log
            .iter()
            .map(|t| days_positive(t.exit_ts.saturating_sub(t.entry_ts)))
            .collect();
        holding_days.sort();

        TradeStatistics {
            number_of_trades: self.number_of_trades(),
            winners: self.winners,
            losers: self.losers,
            break_even: self.break_even,
            average_trade_return: mean_decimal(&pnls_dollars).unwrap_or(Decimal::ZERO),
            median_trade_return: median_decimal(&pnls_dollars).unwrap_or(Decimal::ZERO),
            largest_win: pnls_dollars
                .last()
                .copied()
                .filter(|d| d.is_sign_positive()),
            largest_loss: pnls_dollars
                .first()
                .copied()
                .filter(|d| d.is_sign_negative()),
            average_holding_period: mean_positive(&holding_days),
            median_holding_period: median_positive(&holding_days),
            min_holding_period: holding_days.first().copied().unwrap_or(Positive::ZERO),
            max_holding_period: holding_days.last().copied().unwrap_or(Positive::ZERO),
            long_trades: self.long_trades,
            short_trades: self.short_trades,
            call_trades: self.call_trades,
            put_trades: self.put_trades,
            // Each ClosedTrade is a single leg; the multi-leg grouping is the
            // trade_id, not a per-leg "spread trade" — so this stays 0.
            spread_trades: 0,
        }
    }

    /// Build the [`OptionsSpecificMetrics`] composition + premium-return ratios.
    ///
    /// `return_on_premium = total realised P&L / net premium` and
    /// `premium_capture = total realised P&L / premium received` (relevant to
    /// premium-selling strategies) are ratios over the realised trades; the
    /// average Greek exposures are left `Default` (they need the per-step Greek
    /// series this pass does not thread — see the [module docs](self)).
    fn options_metrics(&self) -> OptionsSpecificMetrics {
        let n = self.number_of_trades();
        let mut m = OptionsSpecificMetrics::default();
        if n == 0 {
            return m;
        }
        let denom = Decimal::from(n);
        m.calls_percentage = checked_ratio(Decimal::from(self.call_trades), denom);
        m.puts_percentage = checked_ratio(Decimal::from(self.put_trades), denom);
        m.long_percentage = checked_ratio(Decimal::from(self.long_trades), denom);
        m.short_percentage = checked_ratio(Decimal::from(self.short_trades), denom);
        let net = self.entry_received_cents - self.entry_paid_cents;
        if net != 0 {
            m.return_on_premium =
                checked_ratio(Decimal::from(self.total_cents), Decimal::from(net));
        }
        if self.entry_received_cents > 0 {
            m.premium_capture = checked_ratio(
                Decimal::from(self.total_cents),
                Decimal::from(self.entry_received_cents),
            );
        }
        m
    }

    /// Build the [`CapitalUtilization`] premium flows from the entry legs. The
    /// margin / capital-in-use fields are left `Default` (margin is not modelled
    /// — see the [module docs](self)).
    fn capital_utilization(&self) -> CapitalUtilization {
        let net = self.entry_received_cents - self.entry_paid_cents;
        CapitalUtilization {
            total_premium_received: cents_to_dollars_i128(self.entry_received_cents),
            total_premium_paid: cents_to_dollars_i128(self.entry_paid_cents),
            net_premium: cents_to_dollars_i128(net),
            ..CapitalUtilization::default()
        }
    }
}

/// The net entry premium in integer cents: `Σ short entry − Σ long entry` over
/// **all** legs the run opened (both the closed legs in `trade_log` and any left
/// `open_at_end`), each weighted by its contract count.
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if the premium sum overflows `i64`.
fn net_premium_cents(
    trade_log: &[ClosedTrade],
    open_at_end: &[OpenPosition],
) -> Result<i64, BacktestError> {
    let mut net: i128 = 0;
    // Closed legs carry their own multiplier; scale premium to the cash basis so
    // net premium matches the cash-scaled realised P&L (F23).
    for trade in trade_log {
        let premium = premium_cash_cents(
            trade.entry_premium.value(),
            trade.quantity.value(),
            trade.contract_multiplier,
        )?;
        net = net_add(net, trade.side, premium)?;
    }
    // Legs still open at feed end are `OpenPosition`s, which do not carry the
    // multiplier. A single-underlying run shares one multiplier across every
    // leg, so reuse the run's (taken from any closed leg). With no closed legs
    // there is no realised P&L for net premium to be inconsistent with, so an
    // unweighted (per-contract) premium is the documented degenerate fallback.
    let run_multiplier = trade_log.first().map_or(1, |t| t.contract_multiplier);
    for leg in open_at_end {
        let premium = premium_cash_cents(
            leg.entry_premium.value(),
            leg.quantity.value(),
            run_multiplier,
        )?;
        net = net_add(net, leg.side, premium)?;
    }
    i64::try_from(net).map_err(|_| BacktestError::ArithmeticOverflow)
}

/// Premium on the cash basis: `entry_premium_cents × quantity ×
/// contract_multiplier`, in `i128` cents (checked).
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if the product overflows `i128`.
fn premium_cash_cents(
    entry_premium_cents: u64,
    quantity: u32,
    contract_multiplier: u32,
) -> Result<i128, BacktestError> {
    i128::from(entry_premium_cents)
        .checked_mul(i128::from(quantity))
        .and_then(|p| p.checked_mul(i128::from(contract_multiplier)))
        .ok_or(BacktestError::ArithmeticOverflow)
}

/// Add a leg's premium to the running net with its side sign: a short leg
/// **received** premium (`+`), a long leg **paid** it (`−`).
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if the running sum overflows `i128`.
fn net_add(net: i128, side: Side, premium: i128) -> Result<i128, BacktestError> {
    match side {
        Side::Short => net.checked_add(premium),
        Side::Long => net.checked_sub(premium),
    }
    .ok_or(BacktestError::ArithmeticOverflow)
}

/// Build the [`AdvancedRiskMetrics`] tail-risk slice from the return and
/// drawdown series: the longest consecutive-loss run, the Ulcer index, and the
/// Pain index. VaR / Expected Shortfall / tail ratio are left `None` (no
/// confidence method is pinned — not fabricated).
fn build_advanced_risk(equity_curve: &[EquityPoint], returns: &[f64]) -> AdvancedRiskMetrics {
    let mut m = AdvancedRiskMetrics {
        max_consecutive_losses: max_consecutive_losses(returns),
        ..AdvancedRiskMetrics::default()
    };
    // Ulcer index = sqrt(mean(drawdown_ratio^2)); Pain index = mean(|drawdown|).
    if !equity_curve.is_empty() {
        let n = equity_curve.len() as f64;
        let sum_sq: f64 = equity_curve.iter().map(|p| p.drawdown * p.drawdown).sum();
        let sum_abs: f64 = equity_curve.iter().map(|p| p.drawdown.abs()).sum();
        let ulcer = (sum_sq / n).sqrt();
        let pain = sum_abs / n;
        m.ulcer_index = Decimal::from_f64_retain(ulcer);
        m.pain_index = Decimal::from_f64_retain(pain);
    }
    m
}

/// The longest run of consecutive **negative** per-step returns.
fn max_consecutive_losses(returns: &[f64]) -> usize {
    let mut max = 0usize;
    let mut run = 0usize;
    for &r in returns {
        if r < 0.0 {
            run += 1;
            if run > max {
                max = run;
            }
        } else {
            run = 0;
        }
    }
    max
}

/// Build the full [`DrawdownAnalysis`] from the equity/drawdown series: the
/// `max_drawdown` magnitude, the dated worst [`DrawdownEvent`], its duration and
/// recovery, and the underwater span.
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if the cents drawdown magnitude
/// overflows `i64`.
fn build_drawdown_analysis(
    equity_curve: &[EquityPoint],
    initial_capital_cents: i64,
) -> Result<DrawdownAnalysis, BacktestError> {
    let mut analysis = DrawdownAnalysis::default();
    let worst_ratio = worst_drawdown_ratio(equity_curve);
    if let Some(magnitude) = Decimal::from_f64_retain(-worst_ratio) {
        analysis.max_drawdown = magnitude;
        analysis.avg_drawdown = magnitude; // one recorded event (see below)
    }
    // Confirm the cents magnitude is computable (kept as the run-level check).
    let _cents = max_drawdown_cents(equity_curve, initial_capital_cents)?;

    if equity_curve.is_empty() {
        return Ok(analysis);
    }

    // Locate the trough (worst drawdown) and its surrounding peak / recovery.
    let bottom_idx = equity_curve
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| a.drawdown.total_cmp(&b.drawdown))
        .map_or(0, |(idx, _)| idx);
    // The peak that preceded the trough: last at-peak (drawdown ≈ 0) step ≤ bottom.
    let start_idx = (0..=bottom_idx)
        .rev()
        .find(|&i| {
            equity_curve
                .get(i)
                .is_some_and(|p| p.drawdown >= -f64::EPSILON)
        })
        .unwrap_or(0);
    // Recovery: first at-peak step after the trough (if any).
    let recovery_idx = (bottom_idx + 1..equity_curve.len()).find(|&i| {
        equity_curve
            .get(i)
            .is_some_and(|p| p.drawdown >= -f64::EPSILON)
    });

    let start_ts = equity_curve.get(start_idx).map_or(0, |p| p.ts_ns);
    let bottom_ts = equity_curve.get(bottom_idx).map_or(0, |p| p.ts_ns);
    let first_ts = equity_curve.first().map_or(0, |p| p.ts_ns);
    let last_ts = equity_curve.last().map_or(0, |p| p.ts_ns);

    let duration = days_positive(bottom_ts.saturating_sub(start_ts));
    let recovery_duration = recovery_idx.map(|i| {
        days_positive(
            equity_curve
                .get(i)
                .map_or(0, |p| p.ts_ns)
                .saturating_sub(bottom_ts),
        )
    });

    // Only record an event when there is a real drawdown (worst_ratio < 0).
    if worst_ratio < 0.0 {
        analysis.drawdowns = vec![DrawdownEvent {
            start_date: naive_from_ns(start_ts),
            bottom_date: naive_from_ns(bottom_ts),
            recovery_date: recovery_idx
                .map(|i| naive_from_ns(equity_curve.get(i).map_or(0, |p| p.ts_ns))),
            magnitude: analysis.max_drawdown,
            duration,
            recovery_duration,
        }];
        analysis.max_drawdown_duration = duration;
        analysis.recovery_duration = recovery_duration;
        analysis.time_to_max_drawdown = days_positive(start_ts.saturating_sub(first_ts));
        analysis.avg_recovery_time = recovery_duration;
    }

    // Time spent underwater: sum the day-gaps into every below-peak step.
    let mut underwater_ns: i64 = 0;
    for pair in equity_curve.windows(2) {
        let [prev, curr] = pair else { continue };
        if curr.drawdown < 0.0 {
            underwater_ns = underwater_ns.saturating_add(curr.ts_ns.saturating_sub(prev.ts_ns));
        }
    }
    analysis.total_underwater_days = days_positive(underwater_ns);
    let total_ns = last_ts.saturating_sub(first_ts);
    if total_ns > 0
        && let Some(pct) = Decimal::from(underwater_ns).checked_div(Decimal::from(total_ns))
    {
        analysis.underwater_percentage = pct;
    }

    Ok(analysis)
}

// --- pure numeric helpers ----------------------------------------------------

/// The per-step equity returns between **consecutive** equity points:
/// `r = (equity_n − equity_{n-1}) / equity_{n-1}` ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
///
/// A pair whose prior equity is **zero** is undefined (division by zero) and is
/// **omitted**; a non-finite result is likewise skipped. Fewer than two points
/// yields an empty series.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "per-step returns are the documented analytic float exception (docs/05 §4); equity cents cast to f64 for the ratio"
)]
fn step_returns(equity_curve: &[EquityPoint]) -> Vec<f64> {
    let mut returns = Vec::with_capacity(equity_curve.len());
    for pair in equity_curve.windows(2) {
        let [prev, curr] = pair else { continue };
        let prev_cents = prev.equity_cents;
        if prev_cents == 0 {
            continue;
        }
        let ret = (curr.equity_cents as f64 - prev_cents as f64) / prev_cents as f64;
        if ret.is_finite() {
            returns.push(ret);
        }
    }
    returns
}

/// The arithmetic mean of `values`, or `None` for an empty slice / non-finite
/// sum.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "the count is cast to f64 to average the analytic-float returns"
)]
fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let sum: f64 = values.iter().sum();
    let mean = sum / values.len() as f64;
    mean.is_finite().then_some(mean)
}

/// The **population** standard deviation (divide by `N`) of `values` about
/// `mean`, or `None` for an empty slice / non-finite result.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "the count is cast to f64 to normalise the analytic-float variance"
)]
fn population_stddev(values: &[f64], mean: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    let stddev = variance.sqrt();
    stddev.is_finite().then_some(stddev)
}

/// The **downside deviation** about a zero minimum-acceptable return:
/// `sqrt( (1/N) Σ min(0, r)^2 )` — the volatility of the negative returns only.
/// `None` for an empty slice / non-finite result; `Some(0)` when no return is
/// negative.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "the count is cast to f64 to normalise the analytic-float downside variance"
)]
fn downside_deviation(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let variance = values
        .iter()
        .map(|&value| {
            let down = value.min(0.0);
            down * down
        })
        .sum::<f64>()
        / values.len() as f64;
    let dev = variance.sqrt();
    dev.is_finite().then_some(dev)
}

/// A risk-adjusted ratio `mean / denominator`, or `None` when the denominator is
/// zero (or the quotient is non-finite) — the shared shape of Sharpe and
/// Sortino. Not annualized (docs/05 §4 pins these to per-step returns).
#[must_use]
fn ratio(mean: f64, denominator: f64) -> Option<f64> {
    if denominator <= 0.0 {
        return None;
    }
    let value = mean / denominator;
    value.is_finite().then_some(value)
}

/// The worst (minimum) drawdown ratio in the series (`≤ 0`), `0` for an empty
/// series.
#[must_use]
fn worst_drawdown_ratio(equity_curve: &[EquityPoint]) -> f64 {
    equity_curve
        .iter()
        .map(|point| point.drawdown)
        .fold(0.0_f64, f64::min)
}

/// The maximum peak-to-trough equity decline in **integer cents**, running peak
/// seeded at `initial_capital_cents` (the ledger's step-`−1` baseline).
///
/// # Errors
///
/// [`BacktestError::ArithmeticOverflow`] if `peak − equity` overflows `i64`.
#[must_use = "the computed drawdown magnitude must be used"]
fn max_drawdown_cents(
    equity_curve: &[EquityPoint],
    initial_capital_cents: i64,
) -> Result<i64, BacktestError> {
    let mut peak = initial_capital_cents;
    let mut max_decline: i64 = 0;
    for point in equity_curve {
        if point.equity_cents > peak {
            peak = point.equity_cents;
        }
        let decline = peak
            .checked_sub(point.equity_cents)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if decline > max_decline {
            max_decline = decline;
        }
    }
    Ok(max_decline)
}

/// `num / den` as a `Decimal`, or `None` when `den` is zero.
#[must_use]
fn checked_ratio(num: Decimal, den: Decimal) -> Option<Decimal> {
    if den.is_zero() {
        return None;
    }
    num.checked_div(den)
}

/// Integer cents → `Decimal` dollars (scale 2) — the upstream money convention.
#[must_use]
fn cents_to_dollars(cents: i64) -> Decimal {
    Decimal::from_i128_with_scale(i128::from(cents), 2)
}

/// `i128` cents → `Decimal` dollars (scale 2).
#[must_use]
fn cents_to_dollars_i128(cents: i128) -> Decimal {
    Decimal::from_i128_with_scale(cents, 2)
}

/// A cents-denominated `Decimal` (e.g. an average of integer cents) → dollars.
#[must_use]
fn cents_to_dollars_dec(cents: Decimal) -> Decimal {
    cents
        .checked_div(Decimal::ONE_HUNDRED)
        .unwrap_or(Decimal::ZERO)
}

/// The arithmetic mean of a slice of `Decimal`s, or `None` when empty.
#[must_use]
fn mean_decimal(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let sum: Decimal = values.iter().copied().sum();
    sum.checked_div(Decimal::from(values.len()))
}

/// The median of a **pre-sorted** slice of `Decimal`s, or `None` when empty.
#[must_use]
fn median_decimal(sorted: &[Decimal]) -> Option<Decimal> {
    let n = sorted.len();
    if n == 0 {
        return None;
    }
    if n % 2 == 1 {
        sorted.get(n / 2).copied()
    } else {
        let (Some(a), Some(b)) = (sorted.get(n / 2 - 1), sorted.get(n / 2)) else {
            return None;
        };
        a.checked_add(*b)?.checked_div(Decimal::from(2))
    }
}

/// The arithmetic mean of a slice of [`Positive`] durations (`0` when empty).
#[must_use]
fn mean_positive(values: &[Positive]) -> Positive {
    if values.is_empty() {
        return Positive::ZERO;
    }
    let sum: Decimal = values.iter().map(|p| p.to_dec()).sum();
    sum.checked_div(Decimal::from(values.len()))
        .and_then(|avg| Positive::new_decimal(avg).ok())
        .unwrap_or(Positive::ZERO)
}

/// The median of a **pre-sorted** slice of [`Positive`] durations (`0` when
/// empty).
#[must_use]
fn median_positive(sorted: &[Positive]) -> Positive {
    let n = sorted.len();
    if n == 0 {
        return Positive::ZERO;
    }
    if n % 2 == 1 {
        return sorted.get(n / 2).copied().unwrap_or(Positive::ZERO);
    }
    let (Some(a), Some(b)) = (sorted.get(n / 2 - 1), sorted.get(n / 2)) else {
        return Positive::ZERO;
    };
    a.to_dec()
        .checked_add(b.to_dec())
        .and_then(|s| s.checked_div(Decimal::from(2)))
        .and_then(|avg| Positive::new_decimal(avg).ok())
        .unwrap_or(Positive::ZERO)
}

/// A non-negative day count from a nanosecond span, clamped to `0` for a
/// non-positive span (never a panic).
#[must_use]
fn days_positive(nanos: i64) -> Positive {
    if nanos <= 0 {
        return Positive::ZERO;
    }
    Decimal::from(nanos)
        .checked_div(Decimal::from(NANOS_PER_DAY))
        .and_then(|days| Positive::new_decimal(days).ok())
        .unwrap_or(Positive::ZERO)
}

/// A UTC [`chrono::NaiveDateTime`] from a nanosecond epoch instant — the wire
/// time of a [`DrawdownEvent`]. Falls back to the epoch on an out-of-range ns.
#[must_use]
fn naive_from_ns(ns: i64) -> chrono::NaiveDateTime {
    DateTime::from_timestamp_nanos(ns).naive_utc()
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::backtesting::{BacktestResult, ExitReason};
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rust_decimal::Decimal;
    use rust_decimal::prelude::ToPrimitive;

    use super::{
        LONG_LEGS_REALIZED_CENTS_KEY, MAX_DRAWDOWN_CENTS_KEY, MAX_DRAWDOWN_RATIO_KEY, Metrics,
        NET_PREMIUM_CENTS_KEY, REALIZED_PNL_CENTS_KEY, SHORT_LEGS_REALIZED_CENTS_KEY,
        max_consecutive_losses, max_drawdown_cents, mean, populate, population_stddev, ratio,
        step_returns,
    };
    use crate::domain::{
        Cents, ContractKey, EquityPoint, PositionId, PriceCents, Quantity, TradeId, Underlying,
    };
    use crate::engine::ClosedTrade;

    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    const TOL: f64 = 1e-9;

    fn point(step: u32, equity_cents: i64, drawdown: f64) -> EquityPoint {
        EquityPoint::new(
            step,
            TS0 + i64::from(step) * NANOS_PER_DAY,
            equity_cents,
            0,
            equity_cents,
            drawdown,
        )
    }

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < TOL
    }

    fn dec_to_f64(d: Decimal) -> f64 {
        d.to_f64().unwrap_or(f64::NAN)
    }

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        u
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is valid");
        };
        q
    }

    fn key(strike: u64, style: OptionStyle) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                TS0 + 30 * NANOS_PER_DAY,
            )),
            strike: PriceCents::new(strike),
            style,
        }
    }

    /// One closed leg with an explicit realised P&L, side, and style.
    #[allow(clippy::too_many_arguments)]
    fn closed(
        position_id: u64,
        strike: u64,
        style: OptionStyle,
        side: Side,
        entry: u64,
        exit: u64,
        realized: i64,
        exit_reason: ExitReason,
    ) -> ClosedTrade {
        ClosedTrade {
            trade_id: TradeId::new(1),
            position_id: PositionId::new(position_id),
            contract: key(strike, style),
            side,
            quantity: qty(1),
            contract_multiplier: 100,
            entry_premium: PriceCents::new(entry),
            exit_price: PriceCents::new(exit),
            close_fees: Cents::new(0),
            close_slippage: Cents::new(0),
            realized_pnl: Cents::new(realized),
            entry_ts: TS0,
            exit_ts: TS0 + 7 * NANOS_PER_DAY,
            exit_reason,
        }
    }

    // --- Sharpe / returns (hand-computed) ------------------------------------

    #[test]
    fn test_sharpe_hand_built_series_matches_population_formula() {
        // returns [1.0, -0.5]: mean 0.25, population stddev 0.75, Sharpe 1/3.
        let returns = [1.0_f64, -0.5];
        let Some(m) = mean(&returns) else {
            panic!("mean defined");
        };
        assert!(approx(m, 0.25));
        let Some(sd) = population_stddev(&returns, m) else {
            panic!("stddev defined");
        };
        assert!(approx(sd, 0.75));
        let Some(sh) = ratio(m, sd) else {
            panic!("Sharpe defined");
        };
        assert!(approx(sh, 1.0 / 3.0));
    }

    #[test]
    fn test_step_returns_zero_prior_equity_is_omitted() {
        let curve = [point(0, 0, 0.0), point(1, 100, 0.0), point(2, 200, 0.0)];
        let returns = step_returns(&curve);
        assert_eq!(returns.len(), 1);
        assert!(matches!(returns.first(), Some(r) if approx(*r, 1.0)));
    }

    #[test]
    fn test_max_consecutive_losses_counts_the_longest_run() {
        // signs: - - + - - - + : longest negative run is 3.
        let returns = [-0.1, -0.2, 0.3, -0.1, -0.1, -0.1, 0.5];
        assert_eq!(max_consecutive_losses(&returns), 3);
    }

    // --- max drawdown (cents, hand-computed ledger) --------------------------

    #[test]
    fn test_max_drawdown_cents_matches_hand_computed_ledger() {
        // initial 10_000; equity 10_000 → 9_500 → 9_000 → 9_800.
        // running peak stays 10_000 → worst decline 1_000c.
        let curve = [
            point(0, 10_000, 0.0),
            point(1, 9_500, -0.05),
            point(2, 9_000, -0.10),
            point(3, 9_800, -0.02),
        ];
        let Ok(decline) = max_drawdown_cents(&curve, 10_000) else {
            panic!("checked cents arithmetic succeeds");
        };
        assert_eq!(decline, 1_000);
    }

    // --- per-leg realised P&L split on a condor fixture ----------------------

    #[test]
    fn test_per_leg_realised_split_reports_short_and_long_separately() {
        // A condor: short call +300, short put +200 (short strikes), long call
        // −150, long put −50 (long wings). total = 300. short = 500, long = −200.
        let curve = [point(0, 1_000_000, 0.0), point(1, 1_000_300, 0.0)];
        let log = [
            closed(
                1,
                510_000,
                OptionStyle::Call,
                Side::Short,
                2_000,
                1_970,
                300,
                ExitReason::Expiration,
            ),
            closed(
                2,
                490_000,
                OptionStyle::Put,
                Side::Short,
                1_800,
                1_780,
                200,
                ExitReason::Expiration,
            ),
            closed(
                3,
                520_000,
                OptionStyle::Call,
                Side::Long,
                800,
                785,
                -150,
                ExitReason::Expiration,
            ),
            closed(
                4,
                480_000,
                OptionStyle::Put,
                Side::Long,
                700,
                695,
                -50,
                ExitReason::Expiration,
            ),
        ];
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &curve, 1_000_000, &log, &[]) else {
            panic!("populate succeeds");
        };
        // Per-leg split in custom_metrics (integer cents).
        assert!(matches!(
            result.custom_metrics.get(SHORT_LEGS_REALIZED_CENTS_KEY),
            Some(v) if *v == Decimal::from(500)
        ));
        assert!(matches!(
            result.custom_metrics.get(LONG_LEGS_REALIZED_CENTS_KEY),
            Some(v) if *v == Decimal::from(-200)
        ));
        assert!(matches!(
            result.custom_metrics.get(REALIZED_PNL_CENTS_KEY),
            Some(v) if *v == Decimal::from(300)
        ));
        // TradeStatistics: 4 legs, 2 winners (shorts), 2 losers (longs).
        let stats = &result.trade_statistics;
        assert_eq!(stats.number_of_trades, 4);
        assert_eq!(stats.winners, 2);
        assert_eq!(stats.losers, 2);
        assert_eq!(stats.short_trades, 2);
        assert_eq!(stats.long_trades, 2);
        assert_eq!(stats.call_trades, 2);
        assert_eq!(stats.put_trades, 2);
        // Net premium on the cash basis: (shorts 2000+1800 − longs 800+700) ×
        // 100 multiplier = 230_000c (F23 — scaled like realised P&L).
        assert!(matches!(
            result.custom_metrics.get(NET_PREMIUM_CENTS_KEY),
            Some(v) if *v == Decimal::from(230_000)
        ));
        // win_rate = 2/4 = 0.5.
        assert!(matches!(
            result.general_performance.win_rate,
            Some(w) if approx(dec_to_f64(w), 0.5)
        ));
    }

    #[test]
    fn test_exit_reason_propagates_from_the_trade_log_into_statistics() {
        // A StopLoss close is a losing trade; TradeStatistics counts it as a
        // loser and the trade log carries the ExitReason unchanged.
        let curve = [point(0, 1_000_000, 0.0), point(1, 999_500, -0.0005)];
        let log = [closed(
            1,
            510_000,
            OptionStyle::Call,
            Side::Short,
            2_000,
            2_500,
            -500,
            ExitReason::StopLoss,
        )];
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &curve, 1_000_000, &log, &[]) else {
            panic!("populate succeeds");
        };
        assert_eq!(result.trade_statistics.losers, 1);
        assert_eq!(result.trade_statistics.winners, 0);
        // The exit reason is preserved on the source log record.
        assert!(matches!(log.first(), Some(t) if t.exit_reason == ExitReason::StopLoss));
    }

    // --- drawdown analysis event ---------------------------------------------

    #[test]
    fn test_drawdown_analysis_records_the_worst_event_dated() {
        // equity 1_000_000 → 990_000 (dd −0.01) → 1_010_000 (recovered).
        let curve = [
            point(0, 1_000_000, 0.0),
            point(1, 990_000, -0.01),
            point(2, 1_010_000, 0.0),
        ];
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &curve, 1_000_000, &[], &[]) else {
            panic!("populate succeeds");
        };
        let dd = &result.drawdown_analysis;
        assert!(approx(dec_to_f64(dd.max_drawdown), 0.01));
        assert_eq!(dd.drawdowns.len(), 1, "one dated worst event");
        let Some(event) = dd.drawdowns.first() else {
            panic!("one event");
        };
        // start at step 0 (the peak), bottom at step 1, recovered at step 2.
        assert!(event.recovery_date.is_some(), "recovery detected");
        assert!(
            approx(dec_to_f64(event.duration.to_dec()), 1.0),
            "1 day peak→trough"
        );
    }

    // --- Metrics projection --------------------------------------------------

    #[test]
    fn test_metrics_projection_bundles_upstream_structs_deterministically() {
        let curve = [point(0, 1_000_000, 0.0), point(1, 1_000_300, 0.0)];
        let log = [closed(
            1,
            510_000,
            OptionStyle::Call,
            Side::Short,
            2_000,
            1_970,
            300,
            ExitReason::Expiration,
        )];
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &curve, 1_000_000, &log, &[]) else {
            panic!("populate succeeds");
        };
        let metrics = Metrics::from_result(&result);
        // The custom_metrics are a sorted BTreeMap (deterministic serialisation).
        let keys: Vec<&String> = metrics.custom_metrics.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "custom_metrics keys are in sorted order");
        // Two serialisations of the same result are byte-identical.
        let (Ok(a), Ok(b)) = (
            serde_json::to_string(&metrics),
            serde_json::to_string(&Metrics::from_result(&result)),
        ) else {
            panic!("Metrics serialises");
        };
        assert_eq!(a, b, "the projection serialises deterministically");
        assert_eq!(metrics.trade_statistics.number_of_trades, 1);
    }

    #[test]
    fn test_populate_is_deterministic_for_same_inputs() {
        let curve = [
            point(0, 1_000_000, 0.0),
            point(1, 1_050_000, 0.0),
            point(2, 990_000, -0.057_142_857),
        ];
        let log = [closed(
            1,
            510_000,
            OptionStyle::Call,
            Side::Short,
            2_000,
            1_500,
            500,
            ExitReason::TargetReached,
        )];
        let mut a = BacktestResult::default();
        let mut b = BacktestResult::default();
        let Ok(()) = populate(&mut a, &curve, 1_000_000, &log, &[]) else {
            panic!("populate a");
        };
        let Ok(()) = populate(&mut b, &curve, 1_000_000, &log, &[]) else {
            panic!("populate b");
        };
        let (Ok(ja), Ok(jb)) = (
            serde_json::to_string(&Metrics::from_result(&a)),
            serde_json::to_string(&Metrics::from_result(&b)),
        ) else {
            panic!("serialise");
        };
        assert_eq!(ja, jb, "same inputs ⇒ byte-identical metrics projection");
    }

    #[test]
    fn test_populate_empty_curve_and_log_leaves_metrics_at_baseline() {
        let mut result = BacktestResult::default();
        let Ok(()) = populate(&mut result, &[], 1_000_000, &[], &[]) else {
            panic!("populate is no-op-safe on empty inputs");
        };
        assert!(result.general_performance.sharpe_ratio.is_none());
        assert!(result.general_performance.total_return.is_zero());
        assert_eq!(result.trade_statistics.number_of_trades, 0);
        assert!(matches!(
            result.custom_metrics.get(MAX_DRAWDOWN_CENTS_KEY),
            Some(v) if v.is_zero()
        ));
        assert!(matches!(
            result.custom_metrics.get(MAX_DRAWDOWN_RATIO_KEY),
            Some(v) if v.is_zero()
        ));
    }
}
