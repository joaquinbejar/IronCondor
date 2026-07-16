//! The strategy specification recorded for a run.
//!
//! [`StrategySpec`] is the canonical "strategy kind + construction parameters"
//! record the bundle manifest serialises and the `run_id` derivation hashes
//! ([01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
//! v0.1 shipped exactly one kind — [`StrategySpec::IronCondor`], the strategy
//! the crate is named for; v0.2 adds [`StrategySpec::ShortStrangle`] as a second
//! arm ([ROADMAP.md](../../../docs/ROADMAP.md)). Both are pure serialisable
//! parameter records; the `StrategySpec → optionstratlib` construction path (and
//! the choice of which concrete adapter type to build) lives in the engine seam.
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
/// *parameters*. v0.1 wired only [`StrategySpec::IronCondor`]; v0.2 adds
/// [`StrategySpec::ShortStrangle`] as a second arm — the second strategy driven
/// through the **unchanged** generic `OptStratAdapter`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StrategySpec {
    /// A four-leg iron condor — the strategy the crate is named for.
    IronCondor(IronCondorSpec),
    /// A two-leg short strangle (short OTM call + short OTM put) — the v0.2
    /// breadth strategy proving the seam generalises beyond `IronCondor`.
    ShortStrangle(ShortStrangleSpec),
}

impl StrategySpec {
    /// The strategy kind as a stable lowercase tag (manifest/log field).
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::IronCondor(_) => "iron_condor",
            Self::ShortStrangle(_) => "short_strangle",
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

/// Construction parameters for `optionstratlib::strategies::ShortStrangle`,
/// mirroring its 16-argument `new`
/// ([specs/optionstratlib.md §3](../../../docs/specs/optionstratlib.md#3-strategy-types-and-traits)).
///
/// A short strangle is a **two-leg** strategy — a short out-of-the-money call
/// and a short out-of-the-money put — so this is a smaller parameter set than
/// [`IronCondorSpec`]: two strikes and two premiums rather than four. Two
/// differences from the condor spec follow the upstream constructor exactly:
/// the call and put legs each take their **own** implied volatility
/// (`call_implied_volatility` / `put_implied_volatility`), and the open/close
/// fees are **per leg** (`open_fee_short_call` / `close_fee_short_call` /
/// `open_fee_short_put` / `close_fee_short_put`).
///
/// Money fields are integer cents; volatility and the two rates are `Decimal`
/// (the analytic exception) and are validated into `Positive` at the
/// construction seam, so a negative volatility or yield surfaces as
/// [`crate::error::BacktestError::Strategy`], never a panic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShortStrangleSpec {
    /// The canonical underlying ticker.
    pub underlying: Underlying,
    /// Underlying price in integer cents.
    pub underlying_price: PriceCents,
    /// Short-call strike in integer cents (out-of-the-money, above the spot).
    pub call_strike: PriceCents,
    /// Short-put strike in integer cents (out-of-the-money, below the spot).
    pub put_strike: PriceCents,
    /// Contract expiry — reused from `optionstratlib`. A resolved
    /// (`DateTime`) expiry is expected; a relative `Days(n)` is resolved once
    /// at tape materialisation ([01 §5](../../../docs/01-domain-model.md#5-contract-identity)).
    pub expiration: ExpirationDate,
    /// Short-call implied volatility as a decimal fraction (e.g. `0.20`) —
    /// analytic `Decimal`, validated non-negative at construction.
    pub call_implied_volatility: Decimal,
    /// Short-put implied volatility as a decimal fraction — analytic `Decimal`,
    /// validated non-negative at construction.
    pub put_implied_volatility: Decimal,
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
    /// Per-contract open fee for the short call in integer cents.
    pub open_fee_short_call: PriceCents,
    /// Per-contract close fee for the short call in integer cents.
    pub close_fee_short_call: PriceCents,
    /// Per-contract open fee for the short put in integer cents.
    pub open_fee_short_put: PriceCents,
    /// Per-contract close fee for the short put in integer cents.
    pub close_fee_short_put: PriceCents,
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::ExpirationDate;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{IronCondorSpec, ShortStrangleSpec, StrategySpec};
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

    /// A short strangle spec whose two leg strikes sit OTM around a 5000 spot.
    fn strangle_spec() -> StrategySpec {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        let Ok(quantity) = Quantity::new(1) else {
            panic!("1 is a valid quantity");
        };
        StrategySpec::ShortStrangle(ShortStrangleSpec {
            underlying,
            underlying_price: PriceCents::new(500_000),
            call_strike: PriceCents::new(520_000),
            put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                1_750_291_200_000_000_000,
            )),
            call_implied_volatility: dec!(0.20),
            put_implied_volatility: dec!(0.20),
            risk_free_rate: dec!(0.05),
            dividend_yield: Decimal::ZERO,
            quantity,
            premium_short_call: PriceCents::new(800),
            premium_short_put: PriceCents::new(700),
            open_fee_short_call: PriceCents::new(65),
            close_fee_short_call: PriceCents::new(65),
            open_fee_short_put: PriceCents::new(65),
            close_fee_short_put: PriceCents::new(65),
        })
    }

    #[test]
    fn test_strategy_spec_kind_is_iron_condor() {
        assert_eq!(spec().kind(), "iron_condor");
    }

    #[test]
    fn test_strategy_spec_kind_is_short_strangle() {
        assert_eq!(strangle_spec().kind(), "short_strangle");
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

    #[test]
    fn test_short_strangle_spec_serde_round_trips() {
        // The second-strategy arm serialises/deserialises verbatim too: two
        // strikes, two per-leg IVs, two premiums, and four per-leg fees.
        let original = strangle_spec();
        let Ok(json) = serde_json::to_string(&original) else {
            panic!("short strangle spec serialises");
        };
        let Ok(decoded) = serde_json::from_str::<StrategySpec>(&json) else {
            panic!("short strangle spec deserialises");
        };
        assert_eq!(original, decoded);
    }
}
