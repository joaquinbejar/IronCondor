//! The strategy specification recorded for a run.
//!
//! [`StrategySpec`] is the canonical "strategy kind + construction parameters"
//! record the bundle manifest serialises and the `run_id` derivation hashes
//! ([01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
//! v0.1 supports exactly one kind — [`StrategySpec::IronCondor`], the strategy
//! the crate is named for; a `ShortStrangle` variant is v0.2 breadth
//! ([ROADMAP.md](../../../docs/ROADMAP.md)), not a v0.1 gate.
//!
//! # Placement (domain, not engine)
//!
//! The type lives in `domain` because the domain `Manifest`
//! ([01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest))
//! references it as `strategy: StrategySpec`, and `domain` is the lowest layer
//! — it cannot import `engine`, where the `StrategySpec → optionstratlib`
//! construction path lives ([`crate::engine::OptStratAdapter::from_spec`],
//! `src/engine/strategy.rs`). Keeping the pure serialisable record here and the
//! `IronCondor::new` call in the engine seam preserves the layering.
//!
//! # Money vs. analytics
//!
//! Money-valued fields — the underlying price, the four strikes, the four
//! per-leg premia, and the open/close fees — are integer cents ([`PriceCents`])
//! and cross into `optionstratlib` as `Positive` dollars at the construction
//! seam ([ADR-0003](../../../docs/adr/0003-money-as-integer-cents.md)). The
//! rate/volatility fields are `Decimal`, the documented analytic exception to
//! integer-cents money — matching [`crate::domain::QuoteView`].

use optionstratlib::ExpirationDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{PriceCents, Quantity, Underlying};

/// The strategy kind and its construction parameters for a single run.
///
/// The enum discriminant is the strategy *kind*; its payload carries the
/// *parameters*. v0.1 wires only [`StrategySpec::IronCondor`]; a
/// `ShortStrangle` variant lands in v0.2 as a new arm, so the shape is already
/// forward-compatible.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StrategySpec {
    /// A four-leg iron condor — the single v0.1 strategy.
    IronCondor(IronCondorSpec),
}

impl StrategySpec {
    /// The strategy kind as a stable lowercase tag (manifest/log field).
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::IronCondor(_) => "iron_condor",
        }
    }
}

/// Construction parameters for `optionstratlib::strategies::IronCondor`,
/// mirroring its 17-argument `new`
/// ([specs/optionstratlib.md §3](../../../docs/specs/optionstratlib.md#3-strategy-types-and-traits)).
///
/// This is the **v0.1-minimal** parameter set: exactly the `IronCondor::new`
/// arguments, no more. Money fields are integer cents; volatility and the two
/// rates are `Decimal` (the analytic exception) and are validated into
/// `Positive` at the construction seam, so a negative volatility or yield
/// surfaces as [`crate::error::BacktestError::Strategy`], never a panic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IronCondorSpec {
    /// The canonical underlying ticker.
    pub underlying: Underlying,
    /// Underlying price in integer cents.
    pub underlying_price: PriceCents,
    /// Short-call strike in integer cents.
    pub short_call_strike: PriceCents,
    /// Short-put strike in integer cents.
    pub short_put_strike: PriceCents,
    /// Long-call strike in integer cents.
    pub long_call_strike: PriceCents,
    /// Long-put strike in integer cents.
    pub long_put_strike: PriceCents,
    /// Contract expiry — reused from `optionstratlib`. A resolved
    /// (`DateTime`) expiry is expected; a relative `Days(n)` is resolved once
    /// at tape materialisation ([01 §5](../../../docs/01-domain-model.md#5-contract-identity)).
    pub expiration: ExpirationDate,
    /// Implied volatility as a decimal fraction (e.g. `0.20`) — analytic
    /// `Decimal`, validated non-negative at construction.
    pub implied_volatility: Decimal,
    /// Risk-free rate as a decimal fraction — analytic `Decimal`, passed
    /// through to `optionstratlib` unchanged.
    pub risk_free_rate: Decimal,
    /// Dividend yield as a decimal fraction — analytic `Decimal`, validated
    /// non-negative at construction.
    pub dividend_yield: Decimal,
    /// Contract count per leg (strictly positive).
    pub quantity: Quantity,
    /// Short-call premium in integer cents.
    pub premium_short_call: PriceCents,
    /// Short-put premium in integer cents.
    pub premium_short_put: PriceCents,
    /// Long-call premium in integer cents.
    pub premium_long_call: PriceCents,
    /// Long-put premium in integer cents.
    pub premium_long_put: PriceCents,
    /// Per-contract open fee in integer cents.
    pub open_fee: PriceCents,
    /// Per-contract close fee in integer cents.
    pub close_fee: PriceCents,
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::ExpirationDate;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{IronCondorSpec, StrategySpec};
    use crate::domain::{PriceCents, Quantity, Underlying};

    fn spec() -> StrategySpec {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
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
            implied_volatility: dec!(0.20),
            risk_free_rate: dec!(0.05),
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
    fn test_strategy_spec_kind_is_iron_condor() {
        assert_eq!(spec().kind(), "iron_condor");
    }

    #[test]
    fn test_strategy_spec_serde_round_trips() {
        // The manifest serialises this record verbatim; a round-trip guards
        // that wiring for the four money fields, the two rates, and the expiry.
        let original = spec();
        let Ok(json) = serde_json::to_string(&original) else {
            panic!("strategy spec serialises");
        };
        let Ok(decoded) = serde_json::from_str::<StrategySpec>(&json) else {
            panic!("strategy spec deserialises");
        };
        assert_eq!(original, decoded);
    }
}
