//! The strategy specification recorded for a run.
//!
//! [`StrategySpec`] is the canonical "strategy kind + construction parameters"
//! record the bundle manifest serialises and the `run_id` derivation hashes
//! ([01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
//! v0.1 shipped exactly one kind — [`StrategySpec::IronCondor`], the strategy
//! the crate is named for; v0.2 adds [`StrategySpec::ShortStrangle`] as a second
//! arm ([ROADMAP.md](../../../docs/ROADMAP.md)); [`StrategySpec::Legs`] adds the
//! shape no named constructor covers — an **explicit leg set** whose expiration
//! is **per leg**, so a diagonal, a calendar, or a condor with wings in a
//! further week is expressible. All three are pure serialisable parameter
//! records; the `StrategySpec → strategy` construction path (and the choice of
//! which concrete adapter or strategy type to build) lives in the engine seam.
//!
//! # Canonical leg order
//!
//! A leg set is a *set*, but it serialises as an ordered `Vec`, so the same
//! position written with its legs in a different input order would otherwise
//! hash to a different `run_id`. [`StrategySpec::canonical`] fixes one total
//! order — `(expiration, strike, style, side, quantity, implied_volatility)` —
//! and every hash of a spec (`run_id`, `batch_id`) and the manifest record
//! itself go through it. So does the **engine**: `LegSetStrategy` sorts the legs
//! it opens by the same rule, because ids are minted in submission order — were
//! the engine to run the caller's order instead, one `run_id` would name two
//! different `fills`/`positions` byte-sets. Between them, leg order is not
//! observable anywhere. The expiration comparison is the crate's single rule,
//! shared with [`crate::domain::ContractKey`]'s `Ord` (`Days` before
//! `DateTime`, exact values within a variant).
//!
//! **Known coupling.** The `style` and `side` positions of that key use
//! `optionstratlib`'s derived `Ord` (`Call < Put`, `Long < Short`), so an
//! upstream variant reorder would move every leg-set `run_id` and invalidate
//! every frozen leg-set golden. The lockfile pin makes that a controlled break
//! rather than a silent one — `lockfile_sha256` is part of the build identity
//! hashed into the `run_id`, so such a bump already regenerates the goldens.
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

use std::cmp::Ordering;

use optionstratlib::{ExpirationDate, OptionStyle, Side};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::contract::expiration_exact_cmp;
use crate::domain::{PriceCents, Quantity, Underlying};

/// The strategy kind and its construction parameters for a single run.
///
/// The enum discriminant is the strategy *kind*; its payload carries the
/// *parameters*. v0.1 wired only [`StrategySpec::IronCondor`]; v0.2 adds
/// [`StrategySpec::ShortStrangle`] as a second arm — the second strategy driven
/// through the **unchanged** generic `OptStratAdapter`; [`StrategySpec::Legs`]
/// adds an explicit leg set, the shape no named upstream constructor covers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StrategySpec {
    /// A four-leg iron condor — the strategy the crate is named for.
    IronCondor(IronCondorSpec),
    /// A two-leg short strangle (short OTM call + short OTM put) — the v0.2
    /// breadth strategy proving the seam generalises beyond `IronCondor`.
    ShortStrangle(ShortStrangleSpec),
    /// An explicit leg set with a **per-leg** expiration — the shape no named
    /// constructor covers (a diagonal, a calendar, a condor whose wings sit in a
    /// further week).
    Legs(LegSetSpec),
}

impl StrategySpec {
    /// The strategy kind as a stable lowercase tag (manifest/log field).
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::IronCondor(_) => "iron_condor",
            Self::ShortStrangle(_) => "short_strangle",
            Self::Legs(_) => "legs",
        }
    }

    /// This spec in **canonical form** — the form every hash of a strategy
    /// (`run_id`, `batch_id`) and the bundle manifest record.
    ///
    /// For the two named kinds it is a plain clone: their parameters are fixed
    /// fields in declaration order, so they are already canonical. For
    /// [`Self::Legs`] the legs are sorted by [`LegSpec::canonical_cmp`], so two
    /// identical positions written in a different leg order produce the same
    /// bytes — and therefore the same `run_id`. The engine applies the same
    /// ordering to the legs it opens (`LegSetStrategy`), so the run the id names
    /// is the run that executes
    /// ([01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
    /// Idempotent: `s.canonical().canonical() == s.canonical()`.
    #[must_use = "the canonical spec must be used"]
    pub fn canonical(&self) -> Self {
        match self {
            Self::IronCondor(_) | Self::ShortStrangle(_) => self.clone(),
            Self::Legs(spec) => {
                let mut spec = spec.clone();
                spec.legs.sort_by(LegSpec::canonical_cmp);
                Self::Legs(spec)
            }
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

/// An **explicit leg set** — the position shape no named `optionstratlib`
/// constructor covers, with the expiration carried **per leg**.
///
/// [`IronCondorSpec`] and [`ShortStrangleSpec`] mirror a specific upstream
/// constructor and share one strategy-level `expiration`, so a position whose
/// legs sit in different expiries — a diagonal, a calendar, a condor with wings
/// in a further week — cannot be described by either regardless of how many legs
/// the struct has. This record describes it directly: an underlying, its price,
/// and the legs themselves.
///
/// # Not an upstream strategy
///
/// There is no `optionstratlib` object to build from a leg set, so this spec is
/// **not** wrapped by `OptStratAdapter` (whose entry reads
/// `Positionable::get_positions` on a preconstructed strategy). The engine seam
/// drives it through `crate::engine::LegSetStrategy`, which opens the listed legs
/// at the first snapshot and holds them. The premia are therefore absent by
/// design — a leg's entry price is the snapshot quote the fill model executes
/// against, never a spec field.
///
/// # Money vs. analytics
///
/// `underlying_price` and each leg's `strike` are integer cents; the two rates
/// here and each leg's `implied_volatility` are `Decimal`, the documented
/// analytic exception. `dividend_yield` and each leg's `implied_volatility` are
/// validated non-negative at the construction seam
/// (`LegSetStrategy::from_spec`) — a typed error, never a panic.
/// `risk_free_rate` is deliberately **unconstrained**: a negative risk-free rate
/// is a real market state, and the named specs pass theirs through unvalidated
/// for the same reason.
///
/// # Recorded and hashed, not read
///
/// The v0.1 engine reads only `underlying` and `legs`: entry prices come from
/// the snapshot quotes, so `underlying_price`, the two rates and the per-leg
/// `implied_volatility` never reach a fill. They are still recorded in the
/// manifest **and hashed into the `run_id`**, and that is deliberate — dropping
/// them from the hash would let two specs differing only in implied volatility
/// share one `<run_id>/` directory and write two different manifests into it.
/// The visible consequence is the benign one: two runs differing only in a
/// descriptive field behave identically but land in different directories. They
/// are also the handoff for a Greek-driven exit policy
/// (`ExitPolicy::DeltaThreshold`), which will need exactly these values to
/// price.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegSetSpec {
    /// The canonical underlying ticker shared by every leg.
    pub underlying: Underlying,
    /// Underlying price in integer cents.
    pub underlying_price: PriceCents,
    /// The legs, in **canonical order** once the spec has been through
    /// [`StrategySpec::canonical`] (which every hash and the manifest apply).
    pub legs: Vec<LegSpec>,
    /// Risk-free rate as a decimal fraction — analytic `Decimal`.
    pub risk_free_rate: Decimal,
    /// Dividend yield as a decimal fraction — analytic `Decimal`, validated
    /// non-negative at construction.
    pub dividend_yield: Decimal,
}

/// One leg of a [`LegSetSpec`]: a contract identity (strike, style, its **own**
/// expiration), the side and size traded, and the leg's implied volatility.
///
/// The expiration is per leg — that is the whole point of the variant. The
/// underlying is not repeated here: every leg of a set shares the
/// [`LegSetSpec::underlying`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegSpec {
    /// Long or short — reused from `optionstratlib`.
    pub side: Side,
    /// Call or put — reused from `optionstratlib`.
    pub style: OptionStyle,
    /// Strike in integer cents.
    pub strike: PriceCents,
    /// This leg's contract expiry — reused from `optionstratlib`. A resolved
    /// (`DateTime`) expiry is **required**: this record can still *represent* a
    /// relative `Days(n)` (it serialises and orders like any other), but the
    /// strategy refuses to run one — `LegSetStrategy::from_spec` returns a typed
    /// error, because tape materialisation resolves the **chain's** quotes, not
    /// a spec's legs, so a relative leg could never match a chain key
    /// ([01 §5](../../../docs/01-domain-model.md#5-contract-identity)).
    /// Resolving it against the tape anchor instead is issue #120.
    pub expiration: ExpirationDate,
    /// Contract count for this leg (strictly positive).
    pub quantity: Quantity,
    /// This leg's implied volatility as a decimal fraction (e.g. `0.20`) —
    /// analytic `Decimal`, validated non-negative at construction.
    pub implied_volatility: Decimal,
}

impl LegSpec {
    /// The canonical leg ordering: `(expiration, strike, style, side, quantity,
    /// implied_volatility)`, with a final textual tiebreak.
    ///
    /// **Total over the leg's serialised bytes**, which is the property that
    /// matters: the stable sort in [`StrategySpec::canonical`] may only leave
    /// two legs in input order when they serialise identically, so leg order
    /// cannot reach the `run_id` or the manifest. Value equality alone is not
    /// enough for that, because `Decimal` compares **scale-insensitively**:
    /// `0.20` and `0.200` are equal numbers that serialise as different strings.
    /// The last two tiebreaks order by the written form instead, so a scale
    /// difference orders rather than ties. They are reached only when every
    /// value field already matched, so the two small allocations never occur on
    /// an ordinary sort.
    ///
    /// The `Decimal` tiebreak matches serde's output exactly **because
    /// `rust_decimal`'s `serde-float` / `serde-arbitrary-precision` are off**,
    /// which is what makes its wire form its `Display` form. Note that
    /// `cargo tree -e features -i rust_decimal --all-features` DOES show
    /// `serde-with-float` (pulled by `option-chain-orderbook`); that is harmless
    /// and does not make this note stale — the implication runs one way only
    /// (`serde-float = ["serde-with-float"]`, not the reverse) and the default
    /// `Serialize` is gated `#[cfg(not(feature = "serde-float"))]`, so the string
    /// form still wins. What *enforces* it is not the manifest — feature
    /// unification could override that — but the **frozen goldens**: a flip to
    /// the float form changes the manifest bytes and fails `bundle_golden`. The [`ExpirationDate`] tiebreak is
    /// scale-carrying rather than byte-exact — its `DateTime` arm is unreachable
    /// anyway, since two instants equal under `Ord` are the same instant and
    /// serialise identically, so only the `Days` arm can decide anything.
    ///
    /// The expiration comparison is the crate's single rule, shared with
    /// [`crate::domain::ContractKey`]'s `Ord`: `Days` before `DateTime`, exact
    /// values within a variant.
    #[must_use]
    pub fn canonical_cmp(&self, other: &Self) -> Ordering {
        expiration_exact_cmp(&self.expiration, &other.expiration)
            .then_with(|| self.strike.cmp(&other.strike))
            .then_with(|| self.style.cmp(&other.style))
            .then_with(|| self.side.cmp(&other.side))
            .then_with(|| self.quantity.cmp(&other.quantity))
            .then_with(|| self.implied_volatility.cmp(&other.implied_volatility))
            // Value-equal but byte-different: `Decimal` ignores scale, and a
            // `Days(n)` expiration carries one too. Order by the written form so
            // a tie means "serialises identically", not merely "compares equal".
            .then_with(|| {
                self.implied_volatility
                    .to_string()
                    .cmp(&other.implied_volatility.to_string())
            })
            .then_with(|| {
                expiration_text(&self.expiration).cmp(&expiration_text(&other.expiration))
            })
    }
}

/// The written form of an expiration — the scale-carrying text serde emits for a
/// relative `Days(n)`, used as [`LegSpec::canonical_cmp`]'s last tiebreak so the
/// comparator is total over bytes rather than over values.
fn expiration_text(expiration: &ExpirationDate) -> String {
    match expiration {
        ExpirationDate::Days(days) => format!("d{}", days.to_dec()),
        ExpirationDate::DateTime(instant) => format!("t{}", instant.to_rfc3339()),
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{IronCondorSpec, LegSetSpec, LegSpec, ShortStrangleSpec, StrategySpec};
    use crate::domain::{PriceCents, Quantity, Underlying};

    /// The tape anchor + one calendar day in nanoseconds, for the two expiries
    /// the leg-set fixtures straddle.
    const TS0: i64 = 1_750_291_200_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    /// The near expiry (`ts_0 + 30 days`) and the far one (`ts_0 + 60 days`).
    const NEAR_EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;
    const FAR_EXPIRY: i64 = TS0 + 60 * NANOS_PER_DAY;

    fn expiry(ns: i64) -> ExpirationDate {
        ExpirationDate::DateTime(DateTime::from_timestamp_nanos(ns))
    }

    fn leg(strike: u64, style: OptionStyle, side: Side, expiration_ns: i64) -> LegSpec {
        let Ok(quantity) = Quantity::new(1) else {
            panic!("1 is a valid quantity");
        };
        LegSpec {
            side,
            style,
            strike: PriceCents::new(strike),
            expiration: expiry(expiration_ns),
            quantity,
            implied_volatility: dec!(0.20),
        }
    }

    /// A four-leg set across TWO expiries — the shape the named specs cannot
    /// describe: a near-week body and a far-week pair of wings.
    fn leg_set(legs: Vec<LegSpec>) -> StrategySpec {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        StrategySpec::Legs(LegSetSpec {
            underlying,
            underlying_price: PriceCents::new(500_000),
            legs,
            risk_free_rate: dec!(0.05),
            dividend_yield: Decimal::ZERO,
        })
    }

    /// The canonical four legs in an arbitrary (non-canonical) input order.
    fn unsorted_legs() -> Vec<LegSpec> {
        vec![
            leg(520_000, OptionStyle::Call, Side::Long, FAR_EXPIRY),
            leg(490_000, OptionStyle::Put, Side::Short, NEAR_EXPIRY),
            leg(480_000, OptionStyle::Put, Side::Long, FAR_EXPIRY),
            leg(510_000, OptionStyle::Call, Side::Short, NEAR_EXPIRY),
        ]
    }

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
    fn test_strategy_spec_kind_is_legs() {
        assert_eq!(leg_set(unsorted_legs()).kind(), "legs");
    }

    #[test]
    fn test_leg_set_spec_serde_round_trips() {
        // The manifest serialises a leg set verbatim: four legs, TWO distinct
        // expiries, per-leg side/style/strike/quantity/IV, plus the two rates.
        let original = leg_set(unsorted_legs()).canonical();
        let Ok(json) = serde_json::to_string(&original) else {
            panic!("leg set spec serialises");
        };
        let Ok(decoded) = serde_json::from_str::<StrategySpec>(&json) else {
            panic!("leg set spec deserialises");
        };
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_leg_set_canonical_sorts_by_expiration_then_strike() {
        let StrategySpec::Legs(spec) = leg_set(unsorted_legs()).canonical() else {
            panic!("the canonical form of a leg set is a leg set");
        };
        let order: Vec<(i64, u64)> = spec
            .legs
            .iter()
            .map(|l| {
                let ExpirationDate::DateTime(dt) = l.expiration else {
                    panic!("the fixture legs carry resolved expiries");
                };
                (
                    dt.timestamp_nanos_opt().unwrap_or_default(),
                    l.strike.value(),
                )
            })
            .collect();
        // Near expiry first (both its strikes ascending), then the far expiry.
        assert_eq!(
            order,
            vec![
                (NEAR_EXPIRY, 490_000),
                (NEAR_EXPIRY, 510_000),
                (FAR_EXPIRY, 480_000),
                (FAR_EXPIRY, 520_000),
            ]
        );
    }

    #[test]
    fn test_canonical_orders_scale_differing_decimals_rather_than_tying() {
        // `Decimal` compares scale-insensitively, so `0.20` and `0.200` are equal
        // NUMBERS that serialise as different strings. If the comparator tied on
        // them, the stable sort would leave two byte-different legs in input
        // order and the same position would hash to two `run_id`s.
        let mut a = leg(510_000, OptionStyle::Call, Side::Short, NEAR_EXPIRY);
        let mut b = a.clone();
        a.implied_volatility = Decimal::new(20, 2);
        b.implied_volatility = Decimal::new(200, 3);
        assert_eq!(
            a.implied_volatility, b.implied_volatility,
            "equal as numbers"
        );
        assert_ne!(
            a.implied_volatility.to_string(),
            b.implied_volatility.to_string(),
            "different as bytes"
        );
        assert_ne!(
            a.canonical_cmp(&b),
            std::cmp::Ordering::Equal,
            "a byte difference must order, not tie"
        );

        // …so both input orders canonicalise to the same bytes.
        let Ok(forward) = serde_json::to_string(&leg_set(vec![a.clone(), b.clone()]).canonical())
        else {
            panic!("the leg set serialises");
        };
        let Ok(reverse) = serde_json::to_string(&leg_set(vec![b, a]).canonical()) else {
            panic!("the permuted leg set serialises");
        };
        assert_eq!(forward, reverse);
    }

    #[test]
    fn test_canonical_orders_scale_differing_relative_expirations() {
        // The same property for an unresolved `Days(n)`: `Days(30)` and
        // `Days(30.0)` are equal values with different written forms.
        let Ok(thirty) = optionstratlib::prelude::Positive::new_decimal(Decimal::new(30, 0)) else {
            panic!("30 is a valid positive");
        };
        let Ok(thirty_scaled) =
            optionstratlib::prelude::Positive::new_decimal(Decimal::new(300, 1))
        else {
            panic!("30.0 is a valid positive");
        };
        let mut a = leg(510_000, OptionStyle::Call, Side::Short, NEAR_EXPIRY);
        let mut b = a.clone();
        a.expiration = ExpirationDate::Days(thirty);
        b.expiration = ExpirationDate::Days(thirty_scaled);
        assert_ne!(
            a.canonical_cmp(&b),
            std::cmp::Ordering::Equal,
            "a byte-different expiration must order, not tie"
        );
    }

    #[test]
    fn test_leg_set_canonical_is_idempotent() {
        let once = leg_set(unsorted_legs()).canonical();
        let twice = once.canonical();
        assert_eq!(once, twice);
    }

    #[test]
    fn test_leg_set_canonical_is_input_order_independent() {
        // The `run_id` hashes the canonical spec, so the SAME position written
        // with its legs in a different order must serialise to the same bytes.
        let mut permuted = unsorted_legs();
        permuted.reverse();
        let Ok(a) = serde_json::to_string(&leg_set(unsorted_legs()).canonical()) else {
            panic!("the canonical leg set serialises");
        };
        let Ok(b) = serde_json::to_string(&leg_set(permuted).canonical()) else {
            panic!("the permuted canonical leg set serialises");
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_canonical_leaves_the_named_kinds_untouched() {
        // The two named specs are already canonical (fixed fields, declaration
        // order), so `canonical` must be an exact clone — the frozen goldens for
        // both kinds depend on it.
        assert_eq!(spec().canonical(), spec());
        assert_eq!(strangle_spec().canonical(), strangle_spec());
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
