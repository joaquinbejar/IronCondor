//! The strategy seam over `optionstratlib`.
//!
//! This module owns the [`PositionableStrategy`] trait bound — the family of
//! upstream multi-leg strategies (`IronCondor`, `ShortStrangle`, spreads, …)
//! the engine's `OptStratAdapter` can wrap. The adapter itself (and the
//! engine-facing `Strategy` trait it implements) lands with issue #10; this
//! file is only the bound plus its blanket impl, migrated from
//! OptionStratBacktest's trait of the same name
//! ([docs/02 §4.1](../../../docs/02-engine-architecture.md#41-the-optionstratlib-adapter)).

use optionstratlib::strategies::Strategies;
use optionstratlib::strategies::base::{Optimizable, Positionable};

/// The upstream trait family an engine strategy adapter can wrap.
///
/// Composed of **exactly** three `optionstratlib` traits —
/// [`Positionable`] (position inventory), [`Strategies`] (the
/// repricing/valuation surface), and [`Optimizable`] (chain-driven strike
/// optimisation) — and **no** `Default` bound. The migrated OptionStratBacktest
/// trait carried a `Default` supertrait; it is dropped here because the adapter
/// wraps an **already-constructed** strategy instance by value and never calls
/// `S::default()`. Requiring `Default` would reject valid upstream strategies
/// whose constructors take required arguments (e.g. `IronCondor::new` takes
/// sixteen) for no benefit
/// ([docs/02 §4.1](../../../docs/02-engine-architecture.md#41-the-optionstratlib-adapter)).
///
/// A blanket impl covers every type satisfying the three bounds, so upstream
/// strategies opt in automatically — verified against
/// `optionstratlib::strategies::{IronCondor, ShortStrangle}`, both of which
/// implement all three (the [`Optimizable`] open question is thereby resolved
/// in the affirmative for `IronCondor`).
pub trait PositionableStrategy: Positionable + Strategies + Optimizable {}

// Blanket impl. Its bound is *identical* to the trait's supertrait list, so
// the two are structurally locked together: were a `+ Default` (or any fourth
// bound) added to `PositionableStrategy` without adding it here, this impl
// would fail to compile — a compile-time guard that the bound is exactly the
// documented triple.
impl<S: Positionable + Strategies + Optimizable> PositionableStrategy for S {}

#[cfg(test)]
mod tests {
    use super::PositionableStrategy;
    use optionstratlib::strategies::{IronCondor, ShortStrangle};

    /// Instantiable only for a type satisfying the full bound; referencing it
    /// with a concrete strategy forces the compiler to prove the bound holds.
    fn assert_positionable_strategy<S: PositionableStrategy>() {}

    #[test]
    fn test_positionable_strategy_composes_three_bounds_short_strangle() {
        // `ShortStrangle` implements `Positionable + Strategies + Optimizable`.
        // Taking the monomorphised fn item as a value type-checks the bound
        // without constructing an instance; if the bound required `Default`
        // (or anything ShortStrangle lacks) this would not compile.
        let _bound_holds: fn() = assert_positionable_strategy::<ShortStrangle>;
    }

    #[test]
    fn test_positionable_strategy_composes_three_bounds_iron_condor() {
        // `IronCondor` (the v0.1 strategy) also satisfies the full triple —
        // this self-guards the docs/02 §4.1 affirmative resolution in-repo.
        let _bound_holds: fn() = assert_positionable_strategy::<IronCondor>;
    }
}
