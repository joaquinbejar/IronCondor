//! Market-data types: the per-step [`ChainSnapshot`] and its parts.
//!
//! One `ChainSnapshot` per step is the engine's unit of market data. Quote
//! validation (crossed quotes, tick alignment, the one `mid` computation)
//! happens once at ingest, in the data conversion boundary — these types
//! carry the validated values.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::contract::{ContractKey, Underlying};
use crate::domain::money::{PriceCents, Quantity};
use crate::domain::time::{SimTime, StepIndex};
use crate::error::BacktestError;

/// The instrument's price grid and contract size — the single authoritative
/// source for cents↔ticks scaling and cash/Greek scaling.
///
/// Carried by the feed schema and validated at ingest: a zero tick size or
/// contract multiplier is rejected, never defaulted — a wrong multiplier
/// mis-scales cash by a factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstrumentSpec {
    /// Minimum price increment in integer cents; `> 0`.
    pub tick_size_cents: PriceCents,
    /// Contracts → underlying units (e.g. `100`); `> 0`.
    pub contract_multiplier: u32,
}

impl InstrumentSpec {
    /// Build a validated spec.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] when `tick_size_cents` or
    /// `contract_multiplier` is zero — no silent default.
    #[must_use = "the validated instrument spec must be used"]
    pub fn new(
        tick_size_cents: PriceCents,
        contract_multiplier: u32,
    ) -> Result<Self, BacktestError> {
        if tick_size_cents.value() == 0 {
            return Err(BacktestError::Conversion(
                "instrument tick_size_cents must be > 0, got 0".to_string(),
            ));
        }
        if contract_multiplier == 0 {
            return Err(BacktestError::Conversion(
                "instrument contract_multiplier must be > 0, got 0".to_string(),
            ));
        }
        Ok(Self {
            tick_size_cents,
            contract_multiplier,
        })
    }
}

/// One contract's quoted state inside a snapshot.
///
/// `mid` holds the value precomputed once at ingest as
/// `floor_to_tick((bid + ask) / 2)` — nothing recomputes a midpoint
/// downstream. The Greeks are per-contract **unit** sensitivities in native
/// `optionstratlib` bases (delta per $1 underlying, theta per **day**, vega
/// per one **percentage point** of IV), weighted once by
/// `quantity × contract_multiplier` in attribution — never re-weighted.
/// `Decimal` fields are derived analytics, the documented exception to the
/// integer-cents rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteView {
    /// The quoted contract.
    pub contract: ContractKey,
    /// Best bid in integer cents.
    pub bid: PriceCents,
    /// Best ask in integer cents.
    pub ask: PriceCents,
    /// The one precomputed midpoint (see type docs).
    pub mid: PriceCents,
    /// Size at the best bid, in contracts.
    pub bid_size: Quantity,
    /// Size at the best ask, in contracts.
    pub ask_size: Quantity,
    /// Implied volatility — analytic `Decimal`, not cents.
    pub implied_volatility: Decimal,
    /// Per-contract unit delta, per $1 of underlying.
    pub delta: Decimal,
    /// Per-contract unit gamma.
    pub gamma: Decimal,
    /// Per-contract unit theta, per day.
    pub theta: Decimal,
    /// Per-contract unit vega, per percentage point of IV.
    pub vega: Decimal,
}

/// The engine's unit of market data: one validated chain snapshot per step.
///
/// `quotes` is a `BTreeMap`, not a `HashMap` — iteration order is part of
/// the determinism contract, so the map backing a snapshot has a stable
/// order regardless of insertion order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainSnapshot {
    /// The snapshot's market timestamp.
    pub ts: SimTime,
    /// The 0-based step ordinal in the run.
    pub step: StepIndex,
    /// The underlying ticker.
    pub underlying: Underlying,
    /// The underlying price in integer cents.
    pub underlying_price: PriceCents,
    /// Tick size + contract multiplier for this underlying.
    pub spec: InstrumentSpec,
    /// The quoted contracts, in stable key order.
    pub quotes: BTreeMap<ContractKey, QuoteView>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle};
    use rust_decimal_macros::dec;

    use super::{InstrumentSpec, QuoteView};
    use crate::domain::contract::{ContractKey, Underlying};
    use crate::domain::money::{PriceCents, Quantity};
    use crate::error::BacktestError;

    #[test]
    fn test_instrument_spec_rejects_zero_tick_size_conversion_error() {
        let spec = InstrumentSpec::new(PriceCents::new(0), 100);
        assert!(matches!(spec, Err(BacktestError::Conversion(_))));
    }

    #[test]
    fn test_instrument_spec_rejects_zero_multiplier_conversion_error() {
        let spec = InstrumentSpec::new(PriceCents::new(5), 0);
        assert!(matches!(spec, Err(BacktestError::Conversion(_))));
    }

    #[test]
    fn test_instrument_spec_accepts_valid_values() {
        let spec = InstrumentSpec::new(PriceCents::new(5), 100);
        assert!(
            matches!(spec, Ok(s) if s.tick_size_cents.value() == 5 && s.contract_multiplier == 100)
        );
    }

    fn key(strike_cents: u64) -> ContractKey {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        ContractKey {
            underlying,
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                1_750_291_200_000_000_000,
            )),
            strike: PriceCents::new(strike_cents),
            style: OptionStyle::Call,
        }
    }

    fn quote(strike_cents: u64) -> QuoteView {
        let Ok(qty) = Quantity::new(10) else {
            panic!("10 is a valid quantity");
        };
        QuoteView {
            contract: key(strike_cents),
            bid: PriceCents::new(100),
            ask: PriceCents::new(110),
            mid: PriceCents::new(105),
            bid_size: qty,
            ask_size: qty,
            implied_volatility: dec!(0.2),
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    #[test]
    fn test_chain_snapshot_quotes_iterate_in_key_order_regardless_of_insertion() {
        let mut quotes = BTreeMap::new();
        for strike in [520_000u64, 500_000, 510_000] {
            quotes.insert(key(strike), quote(strike));
        }
        let strikes: Vec<u64> = quotes.keys().map(|k| k.strike.value()).collect();
        assert_eq!(strikes, vec![500_000, 510_000, 520_000]);
    }
}
