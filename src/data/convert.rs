//! The single `ChainResponse` / `OptionChain` conversion boundary (hot path
//! **H3**).
//!
//! This module is the **only** place a raw feed row becomes a validated
//! [`ChainSnapshot`], and the only place a `ChainSnapshot` becomes an
//! `optionstratlib::chains::OptionChain`. Every determinism-critical
//! validation happens here, **once**, in a single O(contracts) pass per
//! snapshot ([docs/03 §7](../../../docs/03-data-layer.md#7-chainresponse--optionchain-conversion)).
//!
//! # Where `f64` dies
//!
//! `f64` is confined to the feed edge. Prices arrive as `f64` **dollars**
//! only from the simulator DTO; they are rounded to integer [`PriceCents`]
//! **once** with the fixed banker's-rounding policy
//! ([`PriceCents::from_decimal_dollars`], [ADR-0003](../../../docs/adr/0003-money-as-integer-cents.md)),
//! rejecting NaN / infinite / negative first. Analytic fields (IV, Greeks)
//! arrive as `f64` from **both** the simulator DTO and the Parquet feed;
//! they convert to `Decimal` in one place (the validation core), rejecting
//! NaN / infinite first. After conversion no downstream code sees a float
//! price.
//!
//! # DTO-facing vs DTO-independent split
//!
//! - [`raw_quotes_to_snapshot`] is the **DTO-independent validation core**
//!   (unconditional). It takes already-cents money plus `f64` analytics and
//!   performs every shared rule: strike positivity, tick alignment, crossed
//!   quote, the one `mid = floor_to_tick((bid + ask) / 2)` rule, relative
//!   expiry anchoring, analytic NaN rejection, and `BTreeMap` assembly. The
//!   Parquet feed (issue #9) reads integer-cents money directly and reuses
//!   this same core — there is **no second validation path**.
//! - [`chain_response_to_snapshot`] is the **DTO-facing** entry (feature
//!   `simulator`). It performs only the `f64`-dollar price death and the DTO
//!   field/timestamp parsing the simulator wire shape needs, then delegates
//!   to the core. Because the simulator's price fields are behind the
//!   `simulator` feature, this function is gated with it; the core is not.
//!
//! # Relative expiry
//!
//! An [`ExpirationDate::Days(n)`] is resolved to one absolute
//! `expiration_ns` against the tape's **first** timestamp `ts_0`
//! (`TapeMeta.first_ts`), **not** the current snapshot's `ts`, so a
//! contract's identity is fixed for its whole lifecycle
//! ([docs/01 §5.1](../../../docs/01-domain-model.md#51-expiration-resolves-to-one-absolute-instant)).
//! A [`ExpirationDate::DateTime`] passes through. The resolved
//! `ContractKey.expiration` is **always** the `DateTime` form.

use std::collections::BTreeMap;

use optionstratlib::chains::OptionChain;
use optionstratlib::prelude::Positive;
use optionstratlib::{ExpirationDate, OptionStyle};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};

use crate::domain::contract::{ContractKey, Underlying};
use crate::domain::market::{ChainSnapshot, InstrumentSpec, QuoteView};
use crate::domain::money::{PriceCents, Quantity};
use crate::domain::time::{SimTime, StepIndex};
use crate::error::BacktestError;

#[cfg(feature = "simulator")]
use crate::data::simulator::{ChainResponse, OptionPriceResponse};

/// Nanoseconds in one calendar day of exactly 86 400 s (the one deterministic
/// day convention, UTC — no exchange calendar, no DST).
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

// ---------------------------------------------------------------------------
// Feed-agnostic inputs to the validation core
// ---------------------------------------------------------------------------

/// The snapshot-level inputs the validation core needs that are **not**
/// per-contract.
///
/// A feed supplies these once per snapshot. `tick_size_cents` and
/// `contract_multiplier` are carried **raw** (not as a pre-built
/// [`InstrumentSpec`]) so the core is the single place that rejects a
/// zero/missing value — no silent default reaches a snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotMeta {
    /// This snapshot's market timestamp.
    pub ts: SimTime,
    /// The 0-based step ordinal.
    pub step: StepIndex,
    /// The tape's first timestamp `ts_0` — the anchor for `Days(n)` expiry
    /// resolution (`TapeMeta.first_ts`). For step 0 this equals `ts`.
    pub anchor_ts: SimTime,
    /// The canonical underlying ticker.
    pub underlying: Underlying,
    /// The underlying price in integer cents (need not be tick-aligned — the
    /// spot rarely sits on the option tick grid).
    pub underlying_price: PriceCents,
    /// Minimum price increment in integer cents; the core rejects `0`.
    pub tick_size_cents: PriceCents,
    /// Contracts → underlying units; the core rejects `0`.
    pub contract_multiplier: u32,
}

/// One contract's quote as a feed presents it to the validation core.
///
/// Money is already integer cents (rounded once at the `f64` edge for the
/// simulator, read directly for a file feed); analytics are still `f64` so
/// the core is the single place that rejects NaN / infinite analytics and
/// converts them to `Decimal`.
#[derive(Debug, Clone)]
pub struct RawQuote {
    /// The contract's expiry — a relative `Days(n)` or an absolute
    /// `DateTime`. Resolved once against [`SnapshotMeta::anchor_ts`].
    pub expiration: ExpirationDate,
    /// The strike in integer cents; the core rejects `0` and non-tick values.
    pub strike: PriceCents,
    /// Call or put.
    pub style: OptionStyle,
    /// Best bid in integer cents.
    pub bid: PriceCents,
    /// Best ask in integer cents.
    pub ask: PriceCents,
    /// Size at the best bid.
    pub bid_size: Quantity,
    /// Size at the best ask.
    pub ask_size: Quantity,
    /// Implied volatility (analytic `f64`; NaN / infinite rejected).
    pub implied_volatility: f64,
    /// Per-contract unit delta (analytic `f64`).
    pub delta: f64,
    /// Per-contract unit gamma (analytic `f64`).
    pub gamma: f64,
    /// Per-contract unit theta, per day (analytic `f64`).
    pub theta: f64,
    /// Per-contract unit vega, per percentage point of IV (analytic `f64`).
    pub vega: f64,
}

// ---------------------------------------------------------------------------
// f64 edge helpers (the only place a float price/analytic is validated)
// ---------------------------------------------------------------------------

/// Round one `f64` **dollar** price to integer [`PriceCents`] at the feed
/// edge: reject NaN / infinite first, then round once with banker's rounding
/// (and reject a negative amount) via [`PriceCents::from_decimal_dollars`].
///
/// This is the single `Decimal`-from-`f64` price path; it is deterministic
/// (the same `f64` always yields the same cents).
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] for a NaN / infinite / negative
/// value, or one not representable as a `Decimal` or outside the `u64` cent
/// range.
#[must_use = "the converted price must be used"]
#[cfg_attr(not(feature = "simulator"), allow(dead_code))]
pub(crate) fn dollars_f64_to_price_cents(
    field: &str,
    value: f64,
) -> Result<PriceCents, BacktestError> {
    if !value.is_finite() {
        return Err(BacktestError::Conversion(format!(
            "non-finite {field} price {value}"
        )));
    }
    let dollars = Decimal::from_f64_retain(value).ok_or_else(|| {
        BacktestError::Conversion(format!(
            "{field} price {value} is not representable as a decimal"
        ))
    })?;
    PriceCents::from_decimal_dollars(dollars)
}

/// Convert one analytic `f64` (IV or a Greek) to `Decimal` at the feed edge:
/// reject NaN / infinite first, then retain the exact `f64` value as a
/// `Decimal` (deterministic — the same `f64` always yields the same
/// `Decimal`).
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] for a NaN / infinite value or one
/// not representable as a `Decimal`.
#[must_use = "the converted analytic must be used"]
fn analytic_f64_to_decimal(field: &str, value: f64) -> Result<Decimal, BacktestError> {
    if !value.is_finite() {
        return Err(BacktestError::Conversion(format!(
            "non-finite {field} analytic {value}"
        )));
    }
    Decimal::from_f64_retain(value).ok_or_else(|| {
        BacktestError::Conversion(format!(
            "{field} analytic {value} is not representable as a decimal"
        ))
    })
}

// ---------------------------------------------------------------------------
// Deterministic integer / expiry helpers
// ---------------------------------------------------------------------------

/// Floor `value` down to the nearest multiple of `tick` (integer math).
///
/// `tick` must be `> 0` — the caller guarantees it by building an
/// [`InstrumentSpec`] first, which rejects a zero tick.
#[inline]
#[must_use]
const fn floor_to_tick(value: u64, tick: u64) -> u64 {
    value - value % tick
}

/// Resolve an [`ExpirationDate`] to its absolute `DateTime` form.
///
/// - `Days(n)` → `anchor_ts + round(n × 86_400 × 1e9)` ns, with banker's
///   rounding for a fractional `n` (the one deterministic day convention:
///   calendar days of exactly 86 400 s, UTC).
/// - `DateTime(t)` passes through unchanged.
///
/// The returned value is **always** the `DateTime` variant.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] when the day-to-ns scaling
/// or the anchor addition overflows the `i64` nanosecond range.
#[must_use = "the resolved expiration must be used"]
fn resolve_expiration(
    expiration: &ExpirationDate,
    anchor_ts: SimTime,
) -> Result<ExpirationDate, BacktestError> {
    match expiration {
        ExpirationDate::DateTime(instant) => Ok(ExpirationDate::DateTime(*instant)),
        ExpirationDate::Days(days) => {
            let offset = days
                .to_dec()
                .checked_mul(Decimal::from(NANOS_PER_DAY))
                .ok_or(BacktestError::ArithmeticOverflow)?
                .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
                .to_i128()
                .ok_or(BacktestError::ArithmeticOverflow)?;
            let expiration_ns = i128::from(anchor_ts.value())
                .checked_add(offset)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            let expiration_ns =
                i64::try_from(expiration_ns).map_err(|_| BacktestError::ArithmeticOverflow)?;
            Ok(ExpirationDate::DateTime(
                chrono::DateTime::from_timestamp_nanos(expiration_ns),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// The DTO-independent validation core (unconditional; reused by every feed)
// ---------------------------------------------------------------------------

/// Validate feed-agnostic raw quotes into a [`ChainSnapshot`] — the single
/// validation core every feed shares.
///
/// One O(contracts) pass: for each quote it rejects a non-positive or
/// non-tick-aligned strike, a non-tick-aligned bid/ask, a crossed quote, and
/// a NaN / infinite analytic; computes the one `mid = floor_to_tick((bid +
/// ask) / 2)`; resolves the expiry once against `meta.anchor_ts`; and inserts
/// into a `BTreeMap` keyed by [`ContractKey`] (so iteration order is fixed
/// regardless of the feed's array order). The `InstrumentSpec` is built here,
/// so a zero tick size or contract multiplier is rejected in this path.
///
/// # Errors
///
/// - [`BacktestError::Conversion`] — zero tick / multiplier, non-positive
///   strike, NaN / infinite analytic, or a duplicate contract key.
/// - [`BacktestError::PriceNotTickAligned`] — a bid / ask / strike not a
///   multiple of `tick_size_cents`.
/// - [`BacktestError::CrossedQuote`] — `bid > ask` after rounding.
/// - [`BacktestError::ArithmeticOverflow`] — the mid sum or expiry scaling
///   overflows.
#[must_use = "the converted snapshot must be used"]
pub fn raw_quotes_to_snapshot(
    meta: &SnapshotMeta,
    quotes: &[RawQuote],
) -> Result<ChainSnapshot, BacktestError> {
    // Build (and thereby validate `> 0`) the instrument spec once.
    let spec = InstrumentSpec::new(meta.tick_size_cents, meta.contract_multiplier)?;
    let tick = spec.tick_size_cents.value();

    let mut map: BTreeMap<ContractKey, QuoteView> = BTreeMap::new();

    for raw in quotes {
        let strike = raw.strike.value();
        if strike == 0 {
            return Err(BacktestError::Conversion(
                "non-positive strike 0".to_string(),
            ));
        }
        // Tick alignment on every strike / bid / ask.
        check_tick_aligned(strike, tick)?;
        let bid = raw.bid.value();
        let ask = raw.ask.value();
        check_tick_aligned(bid, tick)?;
        check_tick_aligned(ask, tick)?;

        // Crossed quote (validated on the converted cents).
        if bid > ask {
            return Err(BacktestError::CrossedQuote { bid, ask });
        }

        // The one deterministic midpoint: floor the half-cent, then floor to
        // the tick grid.
        let sum = bid
            .checked_add(ask)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let mid = floor_to_tick(sum / 2, tick);
        debug_assert!(bid <= mid && mid <= ask, "mid must sit within the spread");

        // Analytics: NaN / infinite rejected once, here.
        let implied_volatility =
            analytic_f64_to_decimal("implied_volatility", raw.implied_volatility)?;
        let delta = analytic_f64_to_decimal("delta", raw.delta)?;
        let gamma = analytic_f64_to_decimal("gamma", raw.gamma)?;
        let theta = analytic_f64_to_decimal("theta", raw.theta)?;
        let vega = analytic_f64_to_decimal("vega", raw.vega)?;

        // Relative expiry anchored once on ts_0; always stored as DateTime.
        let expiration = resolve_expiration(&raw.expiration, meta.anchor_ts)?;

        let contract = ContractKey {
            underlying: meta.underlying.clone(),
            expiration,
            strike: raw.strike,
            style: raw.style,
        };
        let view = QuoteView {
            contract: contract.clone(),
            bid: raw.bid,
            ask: raw.ask,
            mid: PriceCents::new(mid),
            bid_size: raw.bid_size,
            ask_size: raw.ask_size,
            implied_volatility,
            delta,
            gamma,
            theta,
            vega,
        };
        if map.insert(contract, view).is_some() {
            return Err(BacktestError::Conversion(format!(
                "duplicate contract at strike {strike} for the same expiry and style"
            )));
        }
    }

    Ok(ChainSnapshot {
        ts: meta.ts,
        step: meta.step,
        underlying: meta.underlying.clone(),
        underlying_price: meta.underlying_price,
        spec,
        quotes: map,
    })
}

/// Reject a price that is not an exact multiple of `tick`.
#[inline]
fn check_tick_aligned(price: u64, tick: u64) -> Result<(), BacktestError> {
    if price.is_multiple_of(tick) {
        Ok(())
    } else {
        Err(BacktestError::PriceNotTickAligned { price, tick })
    }
}

// ---------------------------------------------------------------------------
// DTO-facing entry (feature `simulator`)
// ---------------------------------------------------------------------------

/// Convert one simulator [`ChainResponse`] into a validated [`ChainSnapshot`]
/// — the single boundary; the engine never sees a raw DTO.
///
/// The simulator wire shape carries neither the tick size / contract
/// multiplier, the tape anchor `ts_0`, nor per-quote sizes, so the caller (the
/// feed materialiser) supplies them: `spec` from config, `anchor_ts` from
/// `TapeMeta.first_ts`, and a single `quote_size` used for both sides (the DTO
/// exposes no depth). This function performs only the `f64`-dollar price death
/// and the DTO timestamp / expiry parsing, then delegates every shared rule to
/// [`raw_quotes_to_snapshot`]. The DTO's own `mid` field is deliberately
/// ignored — `mid` is derived once by the core, never read from the feed.
///
/// The DTO exposes no theta / vega (and delta / gamma are optional); absent
/// analytics default to `0` here — computing them from IV upstream is deferred
/// ([docs/03 §4](../../../docs/03-data-layer.md#4-historical-csv-schema)).
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] for an invalid underlying ticker, an
/// unparseable timestamp / expiry, a NaN / infinite / negative price, a
/// missing bid / ask / IV, or (via the core) any snapshot-level validation
/// failure; and the tick-alignment / crossed-quote / overflow variants the
/// core raises.
#[cfg(feature = "simulator")]
#[must_use = "the converted snapshot must be used"]
pub fn chain_response_to_snapshot(
    resp: &ChainResponse,
    step: StepIndex,
    spec: InstrumentSpec,
    anchor_ts: SimTime,
    quote_size: Quantity,
) -> Result<ChainSnapshot, BacktestError> {
    use chrono::{DateTime, Utc};

    let underlying = Underlying::new(resp.underlying.as_str())?;
    let ts_dt = DateTime::parse_from_rfc3339(&resp.timestamp).map_err(|e| {
        BacktestError::Conversion(format!(
            "unparseable snapshot timestamp {:?}: {e}",
            resp.timestamp
        ))
    })?;
    let ts_ns = ts_dt.timestamp_nanos_opt().ok_or_else(|| {
        BacktestError::Conversion(format!(
            "snapshot timestamp {:?} is outside the nanosecond range",
            resp.timestamp
        ))
    })?;
    let underlying_price = dollars_f64_to_price_cents("underlying", resp.price)?;

    // Two raw quotes (call + put) per DTO contract row. `saturating_mul` is
    // deliberate and exempt from the no-saturating rule: this is a Vec
    // capacity HINT, not money/counter arithmetic — an (impossible) usize
    // overflow may only under-reserve, never mis-count — and clippy's
    // `manual_saturating_arithmetic` mandates this exact form.
    let mut raw: Vec<RawQuote> = Vec::with_capacity(resp.contracts.len().saturating_mul(2));
    for contract in &resp.contracts {
        let strike = dollars_f64_to_price_cents("strike", contract.strike)?;
        let exp_dt = DateTime::parse_from_rfc3339(&contract.expiration).map_err(|e| {
            BacktestError::Conversion(format!(
                "unparseable contract expiration {:?}: {e}",
                contract.expiration
            ))
        })?;
        let expiration = ExpirationDate::DateTime(exp_dt.with_timezone(&Utc));
        let iv = contract.implied_volatility.ok_or_else(|| {
            BacktestError::Conversion(format!(
                "missing implied_volatility for the contract at strike {}",
                strike.value()
            ))
        })?;
        let gamma = contract.gamma.unwrap_or(0.0);

        raw.push(side_to_raw_quote(
            expiration,
            strike,
            OptionStyle::Call,
            &contract.call,
            iv,
            gamma,
            quote_size,
        )?);
        raw.push(side_to_raw_quote(
            expiration,
            strike,
            OptionStyle::Put,
            &contract.put,
            iv,
            gamma,
            quote_size,
        )?);
    }

    let meta = SnapshotMeta {
        ts: SimTime::new(ts_ns),
        step,
        anchor_ts,
        underlying,
        underlying_price,
        tick_size_cents: spec.tick_size_cents,
        contract_multiplier: spec.contract_multiplier,
    };
    raw_quotes_to_snapshot(&meta, &raw)
}

/// Build one [`RawQuote`] from a DTO side, converting the `f64`-dollar bid /
/// ask to cents and defaulting the absent theta / vega to `0`.
#[cfg(feature = "simulator")]
fn side_to_raw_quote(
    expiration: ExpirationDate,
    strike: PriceCents,
    style: OptionStyle,
    side: &OptionPriceResponse,
    implied_volatility: f64,
    gamma: f64,
    size: Quantity,
) -> Result<RawQuote, BacktestError> {
    let bid = side.bid.ok_or_else(|| {
        BacktestError::Conversion(format!(
            "missing bid for {style:?} at strike {}",
            strike.value()
        ))
    })?;
    let ask = side.ask.ok_or_else(|| {
        BacktestError::Conversion(format!(
            "missing ask for {style:?} at strike {}",
            strike.value()
        ))
    })?;
    Ok(RawQuote {
        expiration,
        strike,
        style,
        bid: dollars_f64_to_price_cents("bid", bid)?,
        ask: dollars_f64_to_price_cents("ask", ask)?,
        bid_size: size,
        ask_size: size,
        implied_volatility,
        delta: side.delta.unwrap_or(0.0),
        gamma,
        theta: 0.0,
        vega: 0.0,
    })
}

// ---------------------------------------------------------------------------
// Reverse view: ChainSnapshot -> OptionChain (the strategy adapter's input)
// ---------------------------------------------------------------------------

/// The per-strike accumulator the reverse conversion folds call + put into.
#[derive(Default)]
struct StrikeAccumulator {
    call_bid: Option<Positive>,
    call_ask: Option<Positive>,
    put_bid: Option<Positive>,
    put_ask: Option<Positive>,
    implied_volatility: Option<Positive>,
    delta_call: Option<Decimal>,
    delta_put: Option<Decimal>,
    gamma: Option<Decimal>,
}

/// Build the strategy-facing `optionstratlib::chains::OptionChain` the adapter
/// reprices against each step — the reverse of [`raw_quotes_to_snapshot`].
///
/// The snapshot's integer-cents money converts back to `Positive` dollars
/// (lossless), each strike's call and put fold into one `add_option` call, and
/// the chain's single `expiration_date` is the quotes' shared resolved expiry.
/// A snapshot mixing more than one expiry cannot be one `OptionChain` and is
/// rejected — the v0.1 single-expiration strategy model
/// ([docs/02 §4.1](../../../docs/02-engine-architecture.md#41-the-optionstratlib-adapter)).
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] when a money field is not a valid
/// `Positive`, when the quotes span more than one expiry, or when the built
/// chain drops a strike (an upstream parse of the expiration string failed).
/// Upstream `optionstratlib` / `Positive` errors are mapped to
/// `Conversion` at this seam.
#[must_use = "the converted option chain must be used"]
pub fn snapshot_to_option_chain(snap: &ChainSnapshot) -> Result<OptionChain, BacktestError> {
    let underlying_price = positive_from_price("underlying", snap.underlying_price)?;

    // Fold call + put per strike, and pin the single shared expiration.
    let mut strikes: BTreeMap<u64, StrikeAccumulator> = BTreeMap::new();
    let mut expiration_ns: Option<i64> = None;
    for view in snap.quotes.values() {
        let this_ns = view.contract.expiration_ns()?;
        match expiration_ns {
            Some(existing) if existing != this_ns => {
                return Err(BacktestError::Conversion(format!(
                    "snapshot mixes expiries {existing} and {this_ns}; one option chain carries one expiry"
                )));
            }
            Some(_) => {}
            None => expiration_ns = Some(this_ns),
        }

        let bid = positive_from_price("bid", view.bid)?;
        let ask = positive_from_price("ask", view.ask)?;
        let iv = Positive::new_decimal(view.implied_volatility).map_err(|e| {
            BacktestError::Conversion(format!(
                "implied volatility {} is not a valid positive: {e}",
                view.implied_volatility
            ))
        })?;
        let acc = strikes.entry(view.contract.strike.value()).or_default();
        acc.implied_volatility.get_or_insert(iv);
        match view.contract.style {
            OptionStyle::Call => {
                acc.call_bid = Some(bid);
                acc.call_ask = Some(ask);
                acc.delta_call = Some(view.delta);
                acc.gamma.get_or_insert(view.gamma);
            }
            OptionStyle::Put => {
                acc.put_bid = Some(bid);
                acc.put_ask = Some(ask);
                acc.delta_put = Some(view.delta);
                acc.gamma.get_or_insert(view.gamma);
            }
        }
    }

    let expiration_date = match expiration_ns {
        Some(ns) => chrono::DateTime::from_timestamp_nanos(ns).to_rfc3339(),
        // Empty snapshot: a well-formed but option-less chain at the epoch.
        None => chrono::DateTime::from_timestamp_nanos(0).to_rfc3339(),
    };

    let mut chain = OptionChain::new(
        snap.underlying.as_str(),
        underlying_price,
        expiration_date,
        None,
        None,
    );

    let expected = strikes.len();
    for (strike_cents, acc) in strikes {
        let strike = positive_from_price("strike", PriceCents::new(strike_cents))?;
        let iv = acc.implied_volatility.unwrap_or(Positive::ZERO);
        chain.add_option(
            strike,
            acc.call_bid,
            acc.call_ask,
            acc.put_bid,
            acc.put_ask,
            iv,
            acc.delta_call,
            acc.delta_put,
            acc.gamma,
            None,
            None,
            None,
        );
    }

    // `add_option` silently drops a strike if the chain's expiration string
    // fails to parse; guard against that so the reverse view is complete.
    if chain.options.len() != expected {
        return Err(BacktestError::Conversion(format!(
            "option chain built {} of {expected} strikes; an expiration parse failed",
            chain.options.len()
        )));
    }

    Ok(chain)
}

/// Convert integer-cents money to a `Positive` dollar amount (lossless),
/// mapping the upstream `Positive` error to [`BacktestError::Conversion`].
///
/// `pub(crate)` so the strategy adapter's exit seam can source `underlying`
/// from `snap.underlying_price` through the **same** derivation this module
/// uses for `chain.underlying_price` (keeping the two byte-identical), without
/// rebuilding the whole `OptionChain`.
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] if `price` is not a valid non-negative
/// `Positive` dollar amount (unreachable for a well-formed [`PriceCents`], but
/// propagated rather than unwrapped).
#[inline]
pub(crate) fn positive_from_price(
    field: &str,
    price: PriceCents,
) -> Result<Positive, BacktestError> {
    Positive::new_decimal(price.to_decimal_dollars()).map_err(|e| {
        BacktestError::Conversion(format!(
            "{field} {} is not a valid positive dollar amount: {e}",
            price.value()
        ))
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::prelude::Positive;
    use optionstratlib::{ExpirationDate, OptionStyle};

    use super::{
        RawQuote, SnapshotMeta, dollars_f64_to_price_cents, raw_quotes_to_snapshot,
        snapshot_to_option_chain,
    };
    use crate::domain::money::{PriceCents, Quantity};
    use crate::domain::time::{SimTime, StepIndex};
    use crate::domain::{ContractKey, Underlying};
    use crate::error::BacktestError;

    const TS0: i64 = 1_750_291_200_000_000_000;

    fn qty(n: u32) -> Quantity {
        match Quantity::new(n) {
            Ok(q) => q,
            Err(e) => panic!("{n} must be a valid quantity: {e}"),
        }
    }

    fn underlying() -> Underlying {
        match Underlying::new("SPX") {
            Ok(u) => u,
            Err(e) => panic!("SPX must be valid: {e}"),
        }
    }

    fn days(n: f64) -> ExpirationDate {
        match Positive::new(n) {
            Ok(p) => ExpirationDate::Days(p),
            Err(e) => panic!("{n} must be a valid positive: {e}"),
        }
    }

    /// A meta with a 5-cent tick and a 100x multiplier anchored at `TS0`.
    fn meta(step: u32) -> SnapshotMeta {
        SnapshotMeta {
            ts: SimTime::new(TS0 + i64::from(step) * 86_400_000_000_000),
            step: StepIndex::new(step),
            anchor_ts: SimTime::new(TS0),
            underlying: underlying(),
            underlying_price: PriceCents::new(510_000),
            tick_size_cents: PriceCents::new(5),
            contract_multiplier: 100,
        }
    }

    /// A tick-aligned raw call quote at `strike`, `bid`, `ask` (all cents).
    fn raw(
        expiration: ExpirationDate,
        strike: u64,
        bid: u64,
        ask: u64,
        style: OptionStyle,
    ) -> RawQuote {
        RawQuote {
            expiration,
            strike: PriceCents::new(strike),
            style,
            bid: PriceCents::new(bid),
            ask: PriceCents::new(ask),
            bid_size: qty(10),
            ask_size: qty(10),
            implied_volatility: 0.2,
            delta: 0.5,
            gamma: 0.01,
            theta: -0.05,
            vega: 0.1,
        }
    }

    fn abs_expiry() -> ExpirationDate {
        ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
            TS0 + 30 * 86_400_000_000_000,
        ))
    }

    #[test]
    fn test_convert_chain_rejects_negative_strike() {
        // A negative strike dies at the f64 price edge (the only place a
        // signed dollar amount exists before it becomes unsigned cents).
        assert!(matches!(
            dollars_f64_to_price_cents("strike", -100.0),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_dollars_f64_to_price_cents_rejects_nan_and_inf_price_conversion_error() {
        assert!(matches!(
            dollars_f64_to_price_cents("bid", f64::NAN),
            Err(BacktestError::Conversion(_))
        ));
        assert!(matches!(
            dollars_f64_to_price_cents("ask", f64::INFINITY),
            Err(BacktestError::Conversion(_))
        ));
        // A finite dollar value rounds once to cents.
        assert!(matches!(
            dollars_f64_to_price_cents("bid", 1.23),
            Ok(p) if p.value() == 123
        ));
    }

    #[test]
    fn test_convert_chain_rejects_zero_strike_conversion_error() {
        let quotes = [raw(abs_expiry(), 0, 100, 110, OptionStyle::Call)];
        assert!(matches!(
            raw_quotes_to_snapshot(&meta(0), &quotes),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_convert_rejects_crossed_quote() {
        // bid 110 > ask 105 (both tick-aligned) is crossed.
        let quotes = [raw(abs_expiry(), 510_000, 110, 105, OptionStyle::Call)];
        assert!(matches!(
            raw_quotes_to_snapshot(&meta(0), &quotes),
            Err(BacktestError::CrossedQuote { bid: 110, ask: 105 })
        ));
    }

    #[test]
    fn test_convert_rejects_non_tick_aligned_price() {
        // ask 107 is not a multiple of the 5-cent tick.
        let quotes = [raw(abs_expiry(), 510_000, 100, 107, OptionStyle::Call)];
        assert!(matches!(
            raw_quotes_to_snapshot(&meta(0), &quotes),
            Err(BacktestError::PriceNotTickAligned {
                price: 107,
                tick: 5
            })
        ));
        // A non-tick strike is rejected too.
        let quotes = [raw(abs_expiry(), 510_001, 100, 110, OptionStyle::Call)];
        assert!(matches!(
            raw_quotes_to_snapshot(&meta(0), &quotes),
            Err(BacktestError::PriceNotTickAligned {
                price: 510_001,
                tick: 5
            })
        ));
    }

    #[test]
    fn test_mid_is_floor_to_tick() {
        // bid 100, ask 105 (odd-cent spread): (100+105)/2 = 102 floored,
        // then floored to the 5-cent grid = 100.
        let quotes = [raw(abs_expiry(), 510_000, 100, 105, OptionStyle::Call)];
        let snap = match raw_quotes_to_snapshot(&meta(0), &quotes) {
            Ok(s) => s,
            Err(e) => panic!("conversion must succeed: {e}"),
        };
        let view = match snap.quotes.values().next() {
            Some(v) => v,
            None => panic!("one quote must be present"),
        };
        assert_eq!(view.mid.value(), 100);
        assert!(view.bid.value() <= view.mid.value() && view.mid.value() <= view.ask.value());
    }

    #[test]
    fn test_convert_rejects_nan_iv_and_greek_conversion_error() {
        let mut q = raw(abs_expiry(), 510_000, 100, 110, OptionStyle::Call);
        q.implied_volatility = f64::NAN;
        assert!(matches!(
            raw_quotes_to_snapshot(&meta(0), &[q]),
            Err(BacktestError::Conversion(_))
        ));
        let mut q = raw(abs_expiry(), 510_000, 100, 110, OptionStyle::Call);
        q.vega = f64::INFINITY;
        assert!(matches!(
            raw_quotes_to_snapshot(&meta(0), &[q]),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_convert_rejects_zero_tick_and_zero_multiplier_conversion_error() {
        let quotes = [raw(abs_expiry(), 510_000, 100, 110, OptionStyle::Call)];
        let mut m = meta(0);
        m.tick_size_cents = PriceCents::new(0);
        assert!(matches!(
            raw_quotes_to_snapshot(&m, &quotes),
            Err(BacktestError::Conversion(_))
        ));
        let mut m = meta(0);
        m.contract_multiplier = 0;
        assert!(matches!(
            raw_quotes_to_snapshot(&m, &quotes),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_convert_happy_path_orders_quotes_and_keeps_analytics_decimal() {
        // Three strikes inserted out of order; the BTreeMap fixes the order.
        let quotes = [
            raw(abs_expiry(), 520_000, 90, 100, OptionStyle::Call),
            raw(abs_expiry(), 500_000, 200, 210, OptionStyle::Call),
            raw(abs_expiry(), 510_000, 140, 150, OptionStyle::Call),
        ];
        let snap = match raw_quotes_to_snapshot(&meta(0), &quotes) {
            Ok(s) => s,
            Err(e) => panic!("conversion must succeed: {e}"),
        };
        let strikes: Vec<u64> = snap.quotes.keys().map(|k| k.strike.value()).collect();
        assert_eq!(strikes, vec![500_000, 510_000, 520_000]);
        assert_eq!(snap.spec.tick_size_cents.value(), 5);
        assert_eq!(snap.spec.contract_multiplier, 100);
        // The strike-500000 quote: mid = floor_to_tick((200+210)/2=205, 5)=205.
        let first = match snap.quotes.values().next() {
            Some(v) => v,
            None => panic!("quotes present"),
        };
        assert_eq!(first.mid.value(), 205);
        // Analytics stay Decimal via the deterministic `from_f64_retain` path.
        assert_eq!(
            Some(first.implied_volatility),
            rust_decimal::Decimal::from_f64_retain(0.2)
        );
    }

    #[test]
    fn test_days_expiration_anchors_on_ts0_not_snapshot_ts() {
        // Same Days(30) contract seen at step 0 and step 5 (different `ts`),
        // both anchored on the same ts_0, must resolve to the SAME identity.
        let q0 = raw(days(30.0), 510_000, 100, 110, OptionStyle::Call);
        let q5 = raw(days(30.0), 510_000, 100, 110, OptionStyle::Call);
        let s0 = match raw_quotes_to_snapshot(&meta(0), &[q0]) {
            Ok(s) => s,
            Err(e) => panic!("step 0 conversion must succeed: {e}"),
        };
        let s5 = match raw_quotes_to_snapshot(&meta(5), &[q5]) {
            Ok(s) => s,
            Err(e) => panic!("step 5 conversion must succeed: {e}"),
        };
        let k0 = match s0.quotes.keys().next() {
            Some(k) => k,
            None => panic!("step 0 quote present"),
        };
        let k5 = match s5.quotes.keys().next() {
            Some(k) => k,
            None => panic!("step 5 quote present"),
        };
        // Identity is fixed: same resolved expiration_ns and same key.
        assert_eq!(k0, k5);
        let expected = TS0 + 30 * 86_400_000_000_000;
        assert!(matches!(k0.expiration_ns(), Ok(ns) if ns == expected));
    }

    #[test]
    fn test_days_expiration_fractional_rounds_half_to_even() {
        // Days(0.5) = 43_200_000_000_000 ns past ts_0 (exact half-day; no
        // rounding needed, integer result).
        let q = raw(days(0.5), 510_000, 100, 110, OptionStyle::Call);
        let snap = match raw_quotes_to_snapshot(&meta(0), &[q]) {
            Ok(s) => s,
            Err(e) => panic!("conversion must succeed: {e}"),
        };
        let key = match snap.quotes.keys().next() {
            Some(k) => k,
            None => panic!("quote present"),
        };
        let expected = TS0 + 43_200_000_000_000;
        assert!(matches!(key.expiration_ns(), Ok(ns) if ns == expected));
    }

    #[test]
    fn test_snapshot_to_option_chain_happy_path_round_trips_strikes() {
        let quotes = [
            raw(abs_expiry(), 500_000, 200, 210, OptionStyle::Call),
            raw(abs_expiry(), 500_000, 180, 190, OptionStyle::Put),
            raw(abs_expiry(), 510_000, 140, 150, OptionStyle::Call),
        ];
        let snap = match raw_quotes_to_snapshot(&meta(0), &quotes) {
            Ok(s) => s,
            Err(e) => panic!("conversion must succeed: {e}"),
        };
        let chain = match snapshot_to_option_chain(&snap) {
            Ok(c) => c,
            Err(e) => panic!("reverse conversion must succeed: {e}"),
        };
        assert_eq!(chain.symbol, "SPX");
        // Two distinct strikes (the call+put at 500000 fold into one).
        assert_eq!(chain.options.len(), 2);
        assert_eq!(
            chain.underlying_price,
            Positive::new(5100.0).unwrap_or(Positive::ZERO)
        );
    }

    #[test]
    fn test_snapshot_to_option_chain_rejects_mixed_expiries() {
        // Two contracts with different absolute expiries cannot be one chain.
        let far = ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
            TS0 + 60 * 86_400_000_000_000,
        ));
        let mut snap = match raw_quotes_to_snapshot(
            &meta(0),
            &[raw(abs_expiry(), 510_000, 100, 110, OptionStyle::Call)],
        ) {
            Ok(s) => s,
            Err(e) => panic!("conversion must succeed: {e}"),
        };
        // Inject a second contract at a different expiry directly.
        let key = ContractKey {
            underlying: underlying(),
            expiration: far,
            strike: PriceCents::new(520_000),
            style: OptionStyle::Call,
        };
        if let Some(view) = snap.quotes.values().next().cloned() {
            let mut view = view;
            view.contract = key.clone();
            snap.quotes.insert(key, view);
        }
        assert!(matches!(
            snapshot_to_option_chain(&snap),
            Err(BacktestError::Conversion(_))
        ));
    }
}

#[cfg(all(test, feature = "simulator"))]
mod dto_tests {
    use super::chain_response_to_snapshot;
    use crate::data::simulator::{
        ChainResponse, OptionContractResponse, OptionPriceResponse, SessionInfoResponse,
    };
    use crate::domain::market::InstrumentSpec;
    use crate::domain::money::{PriceCents, Quantity};
    use crate::domain::time::{SimTime, StepIndex};
    use crate::error::BacktestError;

    const TS0: i64 = 1_750_291_200_000_000_000;

    fn spec() -> InstrumentSpec {
        match InstrumentSpec::new(PriceCents::new(5), 100) {
            Ok(s) => s,
            Err(e) => panic!("spec must build: {e}"),
        }
    }

    fn size() -> Quantity {
        match Quantity::new(10) {
            Ok(q) => q,
            Err(e) => panic!("size must be valid: {e}"),
        }
    }

    fn response(strike: f64) -> ChainResponse {
        ChainResponse {
            underlying: "SPX".to_string(),
            timestamp: "2025-06-19T00:00:00Z".to_string(),
            price: 5100.0,
            contracts: vec![OptionContractResponse {
                strike,
                expiration: "2025-07-19T00:00:00Z".to_string(),
                call: OptionPriceResponse {
                    bid: Some(1.40),
                    ask: Some(1.50),
                    mid: Some(1.45),
                    delta: Some(0.30),
                },
                put: OptionPriceResponse {
                    bid: Some(1.20),
                    ask: Some(1.30),
                    mid: Some(1.25),
                    delta: Some(-0.30),
                },
                implied_volatility: Some(0.19),
                gamma: Some(0.001),
            }],
            session_info: SessionInfoResponse::default(),
        }
    }

    #[test]
    fn test_chain_response_to_snapshot_happy_path_prices_are_cents() {
        let resp = response(5100.0);
        let snap = match chain_response_to_snapshot(
            &resp,
            StepIndex::new(0),
            spec(),
            SimTime::new(TS0),
            size(),
        ) {
            Ok(s) => s,
            Err(e) => panic!("dto conversion must succeed: {e}"),
        };
        // One DTO contract row becomes a call and a put quote.
        assert_eq!(snap.quotes.len(), 2);
        assert_eq!(snap.underlying_price.value(), 510_000);
        // The DTO's own mid is ignored; the core recomputes it.
        let call_mid = snap
            .quotes
            .values()
            .find(|v| matches!(v.contract.style, optionstratlib::OptionStyle::Call))
            .map(|v| v.mid.value());
        // (140 + 150) / 2 = 145 floored to the 5-cent grid = 145.
        assert_eq!(call_mid, Some(145));
    }

    #[test]
    fn test_chain_response_to_snapshot_rejects_negative_strike() {
        let resp = response(-5100.0);
        assert!(matches!(
            chain_response_to_snapshot(&resp, StepIndex::new(0), spec(), SimTime::new(TS0), size(),),
            Err(BacktestError::Conversion(_))
        ));
    }
}
