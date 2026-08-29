//! The strategy seam over `optionstratlib`.
//!
//! This module owns the [`PositionableStrategy`] trait bound — the family of
//! upstream multi-leg strategies (`IronCondor`, `ShortStrangle`, spreads, …) —
//! plus the engine-facing [`Strategy`] trait the replay loop drives, the
//! [`ChainContext`] a strategy reads, and the [`OptStratAdapter`] that wraps an
//! upstream strategy behind [`Strategy`]
//! ([docs/02 §4/§4.1](../../../docs/02-engine-architecture.md#4-the-strategy-trait)).
//!
//! # Two ways into the seam
//!
//! - [`OptStratAdapter`] wraps a **preconstructed** `optionstratlib` strategy
//!   and opens the legs `Positionable::get_positions` reports — the path for the
//!   named kinds ([`StrategySpec::IronCondor`], [`StrategySpec::ShortStrangle`]).
//! - [`LegSetStrategy`] drives a [`StrategySpec::Legs`] **explicit leg set**,
//!   whose expiration is per leg, so there is no upstream object to wrap. It
//!   opens the listed legs at the first snapshot and holds them.
//!
//! Both are the same seam: they share the entry quote matching
//! (`select_leg_quote`), the exit decision (`evaluate_exits`), the terminal
//! flatten (`close_all`), and the recorded reason (`exit_policy_to_reason`).
//! The adapter adds exactly one thing — the gated reprice of its wrapped
//! `inner` — and nothing else differs.
//!
//! # The seam contract
//!
//! Every [`Strategy`] callback **appends** its intents into a caller-owned
//! `out: &mut Vec<OrderCommand>` and returns `Result<(), BacktestError>`; none
//! returns an owned `Vec`. The engine owns that buffer and clears it in place
//! before each callback, so a warm step allocates nothing on the strategy seam
//! — the zero-allocation gate (PB-1,
//! [docs/07 §4](../../../docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop))
//! is satisfiable by the signature itself.
//!
//! # Exit ownership
//!
//! The [`OptStratAdapter`] is the **single** owner of exit-policy evaluation:
//! its [`Strategy::exits`] evaluates the configured
//! [`optionstratlib::simulation::ExitPolicy`] via
//! [`optionstratlib::simulation::check_exit_policy`] over snapshot-derived
//! scalars, appending **closing** commands only. The engine never calls
//! `check_exit_policy` itself. In v0.1 the exit decision reads no repriced
//! `inner` state, so `exits` does **not** rebuild an `OptionChain` per step;
//! the reprice of `inner` is deferred until a Greek-driven policy is wired (see
//! [`Strategy::exits`] and `policy_reads_inner`), a zero-alloc/step and
//! wall-clock-free step path
//! ([docs/02 §4.1](../../../docs/02-engine-architecture.md#41-the-optionstratlib-adapter)).

use rand_chacha::ChaCha8Rng;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use optionstratlib::backtesting::ExitReason;
use optionstratlib::prelude::Positive;
use optionstratlib::simulation::{ExitPolicy, check_exit_policy};
use optionstratlib::strategies::base::{Optimizable, Positionable};
use optionstratlib::strategies::{IronCondor, ShortStrangle, Strategies};
use optionstratlib::{ExpirationDate, OptionStyle, Side};

use crate::data::convert::{positive_from_price, resolve_expiration, snapshot_to_option_chain};
use crate::domain::{
    ChainSnapshot, ContractKey, IronCondorSpec, LegSetSpec, LegSpec, OpenPosition, OrderCommand,
    OrderId, OrderIntent, PendingOrder, PositionAction, PriceCents, Quantity, QuoteView,
    ShortStrangleSpec, SimTime, StepIndex, StrategySpec, TimeInForce, Underlying,
};
use crate::error::BacktestError;

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
/// seventeen) for no benefit
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

/// The step-scoped view a [`Strategy`] reads: the current snapshot, the
/// engine's open-leg inventory and resting orders, the seeded RNG, and the
/// step index ([docs/02 §4](../../../docs/02-engine-architecture.md#4-the-strategy-trait)).
///
/// The RNG is held as `&mut ChaCha8Rng` because it is the run's **sole**
/// randomness source and a strategy that draws from it needs an exclusive
/// borrow; that is why the callbacks that may draw ([`Strategy::on_start`],
/// [`Strategy::on_snapshot`], [`Strategy::on_end`]) take `&mut ChainContext`,
/// while the read-only [`Strategy::exits`] takes `&ChainContext`. `thread_rng`
/// is never used — determinism is byte-exact for a fixed seed
/// ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
pub struct ChainContext<'a> {
    /// The current step's validated chain snapshot (the only market state a
    /// step may read — no look-ahead to `S_{n+1}`).
    pub snapshot: &'a ChainSnapshot,
    /// Legs currently open, with their stable [`crate::domain::PositionId`]
    /// handles — the authoritative inventory exit evaluation drives off.
    pub open: &'a [OpenPosition],
    /// The strategy's own resting orders, addressable for cancel/replace by
    /// their stable [`OrderId`] handles.
    pub pending: &'a [PendingOrder],
    /// The ledger's last-known per-contract mark (carried forward from the most
    /// recently settled step), keyed by [`ContractKey`]. A stale-quote exit
    /// decision reads this carry-forward value instead of snapping back to the
    /// entry premium (see [`Strategy::exits`] / `current_premium`). Empty before
    /// the first settle (step 0), where a still-unmarked leg falls back to its
    /// entry premium. Borrowed by reference — no per-step allocation (PB-1).
    pub marks: &'a std::collections::BTreeMap<ContractKey, PriceCents>,
    /// The run's sole randomness source, seeded from `config.seed`.
    pub rng: &'a mut ChaCha8Rng,
    /// The 0-based ordinal of the current step.
    pub step: StepIndex,
}

/// The engine-facing strategy seam the replay loop drives.
///
/// A strategy *decides*; it does not price or execute. Each callback appends
/// [`OrderCommand`]s into a caller-owned `out` buffer and returns
/// `Result<(), BacktestError>` — see the [module docs](self) for the
/// append-into-buffer contract that makes PB-1 satisfiable by the signature.
///
/// The loop calls the callbacks in a fixed intra-step order: [`Self::on_start`]
/// once before step 0, then per step [`Self::exits`] (closes) strictly before
/// [`Self::on_snapshot`] (entries) into the same buffer, and [`Self::on_end`]
/// once within the final step
/// ([docs/02 §3](../../../docs/02-engine-architecture.md#3-the-replay-loop-normative-state-machine)).
pub trait Strategy {
    /// Called once before the first step, with the first snapshot visible.
    /// Appends opening intents only.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError`] if the strategy cannot form its opening
    /// intents (e.g. a leg has no matching snapshot quote).
    fn on_start(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError>;

    /// Exit-policy evaluation — **this** seam owns it. Called each step before
    /// [`Self::on_snapshot`]; appends **closing** commands only. The default
    /// appends nothing; [`OptStratAdapter`] overrides it to evaluate its
    /// configured [`ExitPolicy`]. The engine never evaluates exits itself.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError`] if repricing the wrapped strategy or resolving
    /// a leg's exit state fails.
    fn exits(
        &mut self,
        ctx: &ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        let _ = (ctx, out);
        Ok(())
    }

    /// Called once per step, after repricing and exit evaluation. Appends
    /// entry/adjustment commands.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError`] if the strategy cannot form its intents.
    fn on_snapshot(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError>;

    /// Called once within the final step, after the feed is exhausted. Appends
    /// **closing** commands only — a `Submit(Open)` here is rejected downstream
    /// by the loop (issue 014).
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError`] if forming the closing intents fails.
    fn on_end(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError>;

    /// The [`ExitReason`] to record on the trade log for closes this strategy
    /// emitted from its **[`Self::exits`] (exit-policy)** phase — the reason the
    /// applied [`ExitPolicy`] gives ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
    ///
    /// The loop consults this **only** when the exits phase produced closes (a
    /// real policy trigger), never on a warm step, so it is off the PB-1 step
    /// path. The default is a generic policy close; [`OptStratAdapter`] overrides
    /// it to map its configured [`ExitPolicy`] to a specific reason.
    #[must_use]
    fn exit_reason(&self) -> ExitReason {
        ExitReason::Other("exit_policy".to_string())
    }
}

/// Number of nanoseconds in one 86 400 s calendar day — the fixed divisor for
/// days-to-expiration (the naive session clock, [docs/02 §8](../../../docs/02-engine-architecture.md#8-clock-model)).
const NANOS_PER_DAY: i128 = 86_400_000_000_000;

/// The adapter that drives any upstream [`PositionableStrategy`] through the
/// engine's [`Strategy`] seam, owning exit-policy evaluation
/// ([docs/02 §4.1](../../../docs/02-engine-architecture.md#41-the-optionstratlib-adapter)).
///
/// `new` takes an **already-constructed** strategy by value (there is no
/// `Default` bound), so a strategy with required constructor arguments — e.g.
/// `IronCondor::new`, which takes seventeen — wraps unchanged. Entry runs once,
/// guarded by `entered`; exit evaluation applies the configured [`ExitPolicy`]
/// over snapshot-derived scalars. In v0.1 no policy reads `inner`'s repriced
/// state, so `exits` skips the per-step `OptionChain` rebuild and the reprice of
/// `inner` (deferred to [`Self::reprice_inner`] until a Greek-driven policy is
/// wired, gated by `policy_reads_inner`).
///
/// # `IronCondor` `Optimizable` bound (resolved)
///
/// The bound is the full `Positionable + Strategies + Optimizable` triple: both
/// `IronCondor` and `ShortStrangle` satisfy `Optimizable` upstream, so no
/// narrowing is needed ([docs/02 §4.1](../../../docs/02-engine-architecture.md#41-the-optionstratlib-adapter)).
pub struct OptStratAdapter<S: PositionableStrategy> {
    inner: S,
    entered: bool,
    exit: ExitPolicy,
}

impl<S: PositionableStrategy> OptStratAdapter<S> {
    /// Wrap an already-constructed upstream strategy with the exit policy the
    /// adapter evaluates each step.
    #[must_use]
    pub const fn new(inner: S, exit: ExitPolicy) -> Self {
        Self {
            inner,
            entered: false,
            exit,
        }
    }

    /// `true` once the opening intents have been emitted (entry is one-shot).
    #[must_use]
    pub const fn has_entered(&self) -> bool {
        self.entered
    }

    /// Validate and emit a `Cancel` against one of the strategy's **own**
    /// resting orders, then append it to `out`.
    ///
    /// Ownership is exactly membership in [`ChainContext::pending`] — seeded
    /// liquidity is never addressable here. This is the guard behind the
    /// pending-order-control rule ([docs/02 §4](../../../docs/02-engine-architecture.md#4-the-strategy-trait)):
    /// a foreign handle is rejected rather than routed.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] if `order_id` is not one of the
    /// strategy's resting orders.
    pub fn request_cancel(
        &self,
        ctx: &ChainContext,
        order_id: OrderId,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        Self::assert_owned(ctx, order_id)?;
        out.push(OrderCommand::Cancel(order_id));
        Ok(())
    }

    /// Validate and emit a `Replace` of one of the strategy's **own** resting
    /// orders, then append it to `out`.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] if `order_id` is not one of the
    /// strategy's resting orders.
    pub fn request_replace(
        &self,
        ctx: &ChainContext,
        order_id: OrderId,
        replacement: OrderIntent,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        Self::assert_owned(ctx, order_id)?;
        out.push(OrderCommand::Replace {
            order_id,
            replacement,
        });
        Ok(())
    }

    /// Reject an `OrderId` that is not one of the strategy's resting orders.
    fn assert_owned(ctx: &ChainContext, order_id: OrderId) -> Result<(), BacktestError> {
        if ctx.pending.iter().any(|p| p.order_id == order_id) {
            Ok(())
        } else {
            Err(BacktestError::Execution(format!(
                "order {} is not a resting strategy order; a strategy may only cancel or replace its own orders",
                order_id.value()
            )))
        }
    }

    /// Build the opening intents (one `Submit(Open)` per upstream leg), matched
    /// to `snapshot` quotes by the leg's **full contract identity** (underlying,
    /// expiration, strike, style). Guarded by `entered`, so it is a no-op after
    /// the first successful entry.
    ///
    /// # Expiration matching
    ///
    /// A leg is matched by strike+style when the snapshot quotes exactly one such
    /// contract (the single-expiry case — byte-identical to the earlier match,
    /// and it works whether the leg's `expiration_date` is relative `Days` or a
    /// resolved `DateTime`). When several **expirations** quote the same
    /// strike/style (a multi-expiry snapshot), the leg is disambiguated by its
    /// OWN expiration through the exact [`ContractKey`] identity — never
    /// whichever expiry sorts first in the map. A leg whose (unresolved `Days`)
    /// expiration cannot be matched exactly in that ambiguous case is a
    /// [`BacktestError::Execution`] rather than a silent wrong-expiry pick.
    fn open_entries(
        &mut self,
        snapshot: &ChainSnapshot,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        if self.entered {
            return Ok(());
        }
        let legs = self
            .inner
            .get_positions()
            .map_err(|e| BacktestError::Strategy(format!("get_positions failed: {e}")))?;
        let leg_count = legs.len();
        for leg in legs {
            let opt = &leg.option;
            let strike = PriceCents::from_decimal_dollars(opt.strike_price.to_dec())?;
            let style = opt.option_style;
            let quote = select_leg_quote(snapshot, strike, style, opt.expiration_date)?;
            out.push(OrderCommand::Submit(OrderIntent {
                contract: quote.contract.clone(),
                action: PositionAction::Open,
                side: opt.side,
                quantity: quantity_from_positive(opt.quantity)?,
                limit: None,
                tif: TimeInForce::Ioc,
                decision_mid: quote.mid,
            }));
        }
        if leg_count > 0 {
            self.entered = true;
        }
        Ok(())
    }

    /// Reprice `inner` from `snapshot` so a Greek-driven exit policy can read
    /// the wrapped strategy's updated state.
    ///
    /// **v0.1 never calls this.** [`Strategy::exits`] gates it behind
    /// [`policy_reads_inner`], which is `false` for every v0.1 policy, so the
    /// per-step `OptionChain` rebuild here — and the upstream `Utc::now()`
    /// expiry check it reaches — stays OFF the v0.1 step path. It exists so that
    /// wiring a Greek-driven policy (e.g. a supported [`ExitPolicy::DeltaThreshold`],
    /// #11/#14) re-enables the reprice by flipping that gate `true`, at which
    /// point a reprice failure must PROPAGATE (it would change which legs
    /// trigger) — which this method does via `?`, unlike the v0.1 drop.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] if the snapshot cannot be converted
    /// to an `OptionChain`, and [`BacktestError::Strategy`] if the upstream
    /// reprice of `inner` (underlying price or implied volatility) is rejected.
    fn reprice_inner(&mut self, snapshot: &ChainSnapshot) -> Result<(), BacktestError> {
        let chain = snapshot_to_option_chain(snapshot)?;
        self.inner
            .set_underlying_price(&chain.underlying_price)
            .map_err(|e| BacktestError::Strategy(format!("reprice underlying failed: {e}")))?;
        let iv = *chain
            .get_atm_implied_volatility()
            .map_err(|e| BacktestError::Strategy(format!("atm implied volatility failed: {e}")))?;
        self.inner.set_implied_volatility(&iv).map_err(|e| {
            BacktestError::Strategy(format!("reprice implied volatility failed: {e}"))
        })?;
        Ok(())
    }
}

impl OptStratAdapter<IronCondor> {
    /// Build an iron-condor adapter from a [`StrategySpec`] and the
    /// [`ExitPolicy`] the adapter evaluates each step.
    ///
    /// This is the `StrategySpec → optionstratlib` construction path for the
    /// iron condor: it converts the spec's integer-cents money into `Positive`
    /// dollars, calls the 17-argument `IronCondor::new`
    /// ([specs/optionstratlib.md §3](../../../docs/specs/optionstratlib.md#3-strategy-types-and-traits)),
    /// and wraps the result in [`OptStratAdapter::new`]. It is **kind-checked**:
    /// this constructor builds an `OptStratAdapter<IronCondor>`, so a
    /// [`StrategySpec::ShortStrangle`] is a caller error and returns
    /// [`BacktestError::Strategy`] pointing at
    /// [`OptStratAdapter::<ShortStrangle>::from_spec`] — never a silent wrong
    /// build. The match stays exhaustive, so a future third strategy forces this
    /// arm to be revisited.
    ///
    /// # Determinism
    ///
    /// The construction is a pure function of the spec — no wall-clock and no
    /// RNG on this path. (`IronCondor::new` timestamps its internal `Position`s
    /// with `Utc::now()`, but that date is never read by [`Strategy::exits`]
    /// or the entry path, which source decisions from snapshot scalars and leg
    /// definitions only — so it cannot reach a result. See the reprice
    /// invariant in [`Strategy::exits`].)
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Strategy`] when the upstream constructor
    /// rejects the parameters, a volatility/yield is not a valid `Positive`, or
    /// the spec is not an [`StrategySpec::IronCondor`], and
    /// [`BacktestError::Conversion`] when a cents → `Positive` money conversion
    /// fails.
    pub fn from_spec(spec: &StrategySpec, exit: ExitPolicy) -> Result<Self, BacktestError> {
        match spec {
            StrategySpec::IronCondor(inner) => Ok(Self::new(build_iron_condor(inner)?, exit)),
            StrategySpec::ShortStrangle(_) => Err(BacktestError::Strategy(format!(
                "OptStratAdapter::<IronCondor>::from_spec received a {} spec; \
                 build it with OptStratAdapter::<ShortStrangle>::from_spec",
                spec.kind()
            ))),
            StrategySpec::Legs(_) => Err(BacktestError::Strategy(format!(
                "OptStratAdapter::<IronCondor>::from_spec received a {} spec; \
                 an explicit leg set has no upstream strategy object — \
                 build it with LegSetStrategy::from_spec",
                spec.kind()
            ))),
        }
    }
}

impl OptStratAdapter<ShortStrangle> {
    /// Build a short-strangle adapter from a [`StrategySpec`] and the
    /// [`ExitPolicy`] the adapter evaluates each step.
    ///
    /// The **v0.2 second strategy**, wired through the **unchanged** generic
    /// [`OptStratAdapter`] and [`Strategy`] impl — the whole point of #28. It
    /// mirrors [`OptStratAdapter::<IronCondor>::from_spec`]: convert the spec's
    /// integer-cents money into `Positive` dollars, call the 16-argument
    /// `ShortStrangle::new` (a two-leg short OTM call + short OTM put, with a
    /// per-leg IV and per-leg open/close fees,
    /// [specs/optionstratlib.md §3](../../../docs/specs/optionstratlib.md#3-strategy-types-and-traits)),
    /// and wrap the result in [`OptStratAdapter::new`]. It is kind-checked: a
    /// [`StrategySpec::IronCondor`] returns [`BacktestError::Strategy`] pointing
    /// at [`OptStratAdapter::<IronCondor>::from_spec`].
    ///
    /// # Determinism
    ///
    /// A pure function of the spec — no wall-clock, no RNG. (`ShortStrangle::new`
    /// timestamps its `Position`s with `Utc::now()`, but that date is never read
    /// by [`Strategy::exits`] or the entry path, exactly as for the iron condor.)
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Strategy`] when the upstream constructor
    /// rejects the parameters, a volatility/yield is not a valid `Positive`, or
    /// the spec is not an [`StrategySpec::ShortStrangle`], and
    /// [`BacktestError::Conversion`] when a cents → `Positive` money conversion
    /// fails.
    pub fn from_spec(spec: &StrategySpec, exit: ExitPolicy) -> Result<Self, BacktestError> {
        match spec {
            StrategySpec::ShortStrangle(inner) => Ok(Self::new(build_short_strangle(inner)?, exit)),
            StrategySpec::IronCondor(_) => Err(BacktestError::Strategy(format!(
                "OptStratAdapter::<ShortStrangle>::from_spec received a {} spec; \
                 build it with OptStratAdapter::<IronCondor>::from_spec",
                spec.kind()
            ))),
            StrategySpec::Legs(_) => Err(BacktestError::Strategy(format!(
                "OptStratAdapter::<ShortStrangle>::from_spec received a {} spec; \
                 an explicit leg set has no upstream strategy object — \
                 build it with LegSetStrategy::from_spec",
                spec.kind()
            ))),
        }
    }
}

/// Convert an integer-cents money value into `optionstratlib`'s `Positive`
/// dollars at the strategy-construction seam.
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] if the cents value is not a valid
/// non-negative `Positive` dollar amount (unreachable for a well-formed
/// [`PriceCents`], but propagated rather than unwrapped).
fn positive_dollars(price: PriceCents) -> Result<Positive, BacktestError> {
    Positive::new_decimal(price.to_decimal_dollars()).map_err(|e| {
        BacktestError::Conversion(format!(
            "price {} cents is not a valid positive dollar amount: {e}",
            price.value()
        ))
    })
}

/// Validate an analytic `Decimal` (volatility, dividend yield) into a
/// `Positive` at the strategy-construction seam. `field` is the fully-qualified
/// field name (e.g. `"iron condor implied volatility"`), so the message is
/// strategy-agnostic and reusable across strategy constructors.
///
/// # Errors
///
/// Returns [`BacktestError::Strategy`] if `value` is negative (a `Positive`
/// wraps a non-negative `Decimal`).
fn positive_rate(value: Decimal, field: &str) -> Result<Positive, BacktestError> {
    Positive::new_decimal(value)
        .map_err(|e| BacktestError::Strategy(format!("{field} {value} must be non-negative: {e}")))
}

/// Build an `optionstratlib::strategies::IronCondor` from its
/// [`IronCondorSpec`], converting integer-cents money into `Positive` dollars
/// and mapping the upstream `StrategyError` into [`BacktestError::Strategy`] —
/// the one strategy-construction conversion place.
///
/// # Errors
///
/// Returns [`BacktestError::Strategy`] if `IronCondor::new` rejects the
/// parameters or a volatility/yield/quantity is not a valid `Positive`, and
/// [`BacktestError::Conversion`] if a cents → `Positive` money conversion
/// fails.
fn build_iron_condor(spec: &IronCondorSpec) -> Result<IronCondor, BacktestError> {
    let quantity = Positive::new_decimal(Decimal::from(spec.quantity.value())).map_err(|e| {
        BacktestError::Strategy(format!(
            "iron condor quantity {} is invalid: {e}",
            spec.quantity.value()
        ))
    })?;
    IronCondor::new(
        spec.underlying.as_str().to_string(),
        positive_dollars(spec.underlying_price)?,
        positive_dollars(spec.short_call_strike)?,
        positive_dollars(spec.short_put_strike)?,
        positive_dollars(spec.long_call_strike)?,
        positive_dollars(spec.long_put_strike)?,
        spec.expiration,
        positive_rate(spec.implied_volatility, "iron condor implied volatility")?,
        spec.risk_free_rate,
        positive_rate(spec.dividend_yield, "iron condor dividend yield")?,
        quantity,
        positive_dollars(spec.premium_short_call)?,
        positive_dollars(spec.premium_short_put)?,
        positive_dollars(spec.premium_long_call)?,
        positive_dollars(spec.premium_long_put)?,
        positive_dollars(spec.open_fee)?,
        positive_dollars(spec.close_fee)?,
    )
    .map_err(|e| BacktestError::Strategy(format!("iron condor construction rejected: {e}")))
}

/// Build an `optionstratlib::strategies::ShortStrangle` from its
/// [`ShortStrangleSpec`], converting integer-cents money into `Positive`
/// dollars and mapping the upstream `StrategyError` into
/// [`BacktestError::Strategy`] — the short-strangle analogue of
/// [`build_iron_condor`], and the one short-strangle construction conversion
/// place.
///
/// The argument order and units follow `ShortStrangle::new` **exactly**: a
/// short OTM call + short OTM put, with a per-leg implied volatility and per-leg
/// open/close fees.
///
/// # Errors
///
/// Returns [`BacktestError::Strategy`] if `ShortStrangle::new` rejects the
/// parameters or a volatility/yield/quantity is not a valid `Positive`, and
/// [`BacktestError::Conversion`] if a cents → `Positive` money conversion fails.
fn build_short_strangle(spec: &ShortStrangleSpec) -> Result<ShortStrangle, BacktestError> {
    let quantity = Positive::new_decimal(Decimal::from(spec.quantity.value())).map_err(|e| {
        BacktestError::Strategy(format!(
            "short strangle quantity {} is invalid: {e}",
            spec.quantity.value()
        ))
    })?;
    ShortStrangle::new(
        spec.underlying.as_str().to_string(),
        positive_dollars(spec.underlying_price)?,
        positive_dollars(spec.call_strike)?,
        positive_dollars(spec.put_strike)?,
        spec.expiration,
        positive_rate(
            spec.call_implied_volatility,
            "short strangle call implied volatility",
        )?,
        positive_rate(
            spec.put_implied_volatility,
            "short strangle put implied volatility",
        )?,
        spec.risk_free_rate,
        positive_rate(spec.dividend_yield, "short strangle dividend yield")?,
        quantity,
        positive_dollars(spec.premium_short_call)?,
        positive_dollars(spec.premium_short_put)?,
        positive_dollars(spec.open_fee_short_call)?,
        positive_dollars(spec.close_fee_short_call)?,
        positive_dollars(spec.open_fee_short_put)?,
        positive_dollars(spec.close_fee_short_put)?,
    )
    .map_err(|e| BacktestError::Strategy(format!("short strangle construction rejected: {e}")))
}

impl<S: PositionableStrategy> Strategy for OptStratAdapter<S> {
    fn on_start(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        let snapshot = ctx.snapshot;
        self.open_entries(snapshot, out)
    }

    fn exits(
        &mut self,
        ctx: &ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        if ctx.open.is_empty() {
            return Ok(());
        }
        // Reprice `inner` ONLY when the configured policy reads its repriced
        // state. No v0.1 policy does — `policy_reads_inner` is `false` for all
        // of them (`ExitPolicy::DeltaThreshold` is unsupported and never
        // triggers) — so the reprice, and its upstream `Utc::now()` wall-clock
        // reach, is SKIPPED in v0.1. This is output-preserving: the dropped
        // reprice never fed the deterministic exit decision, and skipping it
        // also removes a wall-clock read (a determinism win).
        //
        // INVARIANT (revisit when `ExitPolicy::DeltaThreshold` is wired,
        // #11/#14): the instant a Greek-driven policy reads `inner`'s repriced
        // state, `policy_reads_inner` returns `true` for it, the reprice runs,
        // and a reprice failure PROPAGATES (it would change which legs trigger)
        // rather than being dropped.
        if policy_reads_inner(&self.exit) {
            self.reprice_inner(ctx.snapshot)?;
        }
        // The per-leg decision itself is the seam-wide rule, shared verbatim
        // with [`LegSetStrategy`]: this adapter owns only the `inner` reprice
        // above, which the policy gate keeps off the v0.1 step path.
        evaluate_exits(&self.exit, ctx, out)
    }

    fn on_snapshot(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        let snapshot = ctx.snapshot;
        self.open_entries(snapshot, out)
    }

    fn on_end(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        // Closing commands only — the loop rejects a Submit(Open) from on_end.
        // close_all skips legs the exit phase already closed this step (F11)
        // and quantity covered by resting (GTC) closes still working (#110).
        close_all(ctx.open, ctx.pending, ctx.snapshot, out)
    }

    fn exit_reason(&self) -> ExitReason {
        exit_policy_to_reason(&self.exit)
    }
}

/// The strategy that drives a [`StrategySpec::Legs`] explicit leg set: open the
/// listed legs at the first snapshot and hold them, evaluating the configured
/// [`ExitPolicy`] each step exactly as [`OptStratAdapter`] does.
///
/// # Why this is not an [`OptStratAdapter`]
///
/// The adapter's entry reads `Positionable::get_positions` on a
/// **preconstructed** `optionstratlib` strategy. A leg set has no such object —
/// that is the point of the variant: it describes positions with a **per-leg**
/// expiration (a diagonal, a calendar, a condor with wings in a further week)
/// that no named upstream constructor covers. So the `Legs` kind goes through
/// the same [`Strategy`] seam without the adapter, reusing the seam's shared
/// pieces verbatim — `select_leg_quote` for entry matching, `evaluate_exits`
/// for the exit decision, `close_all` for the terminal flatten, and
/// `exit_policy_to_reason` for the recorded reason.
///
/// # Determinism
///
/// Entry iterates the legs in **canonical** order — [`Self::new`] sorts them by
/// [`LegSpec::canonical_cmp`], the same order [`StrategySpec::canonical`] gives
/// the `run_id` hash and the manifest — resolves any relative expiry against the
/// tape anchor, and matches each leg by exact [`ContractKey`] identity.
///
/// The canonical order is taken over the spec **as written**, before resolution
/// (`Days` sorts before `DateTime`), which keeps it a pure function of the spec
/// — the thing the `run_id` hashed. A relative spec and the resolved spec naming
/// the same position are therefore *different specs* with different `run_id`s,
/// which is correct: the manifest records what it hashed. That is what makes one `run_id` name one byte-set:
/// the engine mints `order_id` / `position_id` / `trade_id` in submission order,
/// so running the caller's arbitrary leg order would put a permuted
/// `fills`/`positions` table under an identical `run_id` and manifest. No wall
/// clock, no RNG, no map-iteration-order dependence. After entry,
/// [`Strategy::on_snapshot`] returns immediately, so a warm step allocates
/// nothing on this seam (PB-1).
pub struct LegSetStrategy {
    /// The legs to open, in **canonical** order — sorted by
    /// [`LegSpec::canonical_cmp`] at construction, never taken on trust from the
    /// caller's spec (see [`Self::new`]).
    legs: Vec<LegSpec>,
    /// The underlying every leg shares — the [`ContractKey`] prefix.
    underlying: Underlying,
    /// `true` once the opening intents have been emitted (entry is one-shot).
    entered: bool,
    /// The exit policy evaluated each step.
    exit: ExitPolicy,
}

impl LegSetStrategy {
    /// Build a leg-set strategy from a [`StrategySpec`] and the [`ExitPolicy`]
    /// it evaluates each step.
    ///
    /// This is the `StrategySpec::Legs → engine` construction path, the
    /// counterpart of the two `OptStratAdapter::from_spec` constructors: it
    /// validates the spec's analytic fields and takes the legs in the order the
    /// spec carries them. It is **kind-checked** — an [`StrategySpec::IronCondor`]
    /// or [`StrategySpec::ShortStrangle`] returns [`BacktestError::Strategy`]
    /// pointing at the right constructor, never a silent wrong build — and the
    /// match stays exhaustive, so a future fourth kind forces this arm to be
    /// revisited.
    ///
    /// # Determinism
    ///
    /// A pure function of the spec: no wall clock, no RNG.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Strategy`] when the spec is not a
    /// [`StrategySpec::Legs`], when its leg set is empty (a position with no
    /// legs is not a position), or when the dividend yield or any leg's implied
    /// volatility is negative (the analytic fields are validated here, at the
    /// construction seam, exactly as the named specs' are). A relative `Days(n)`
    /// expiry is **accepted** and resolved at entry (#120).
    pub fn from_spec(spec: &StrategySpec, exit: ExitPolicy) -> Result<Self, BacktestError> {
        match spec {
            StrategySpec::Legs(inner) => Self::new(inner, exit),
            StrategySpec::IronCondor(_) | StrategySpec::ShortStrangle(_) => {
                Err(BacktestError::Strategy(format!(
                    "LegSetStrategy::from_spec received a {} spec; \
                     build it with OptStratAdapter::from_spec",
                    spec.kind()
                )))
            }
        }
    }

    /// Build the strategy from an already-narrowed [`LegSetSpec`] — the
    /// validation body of [`Self::from_spec`].
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Strategy`] for an empty leg set or a negative
    /// dividend yield / per-leg implied volatility.
    fn new(spec: &LegSetSpec, exit: ExitPolicy) -> Result<Self, BacktestError> {
        if spec.legs.is_empty() {
            return Err(BacktestError::Strategy(
                "a leg set must carry at least one leg".to_string(),
            ));
        }
        // The analytic Decimals are validated here rather than at deserialisation
        // so a hostile manifest surfaces as a typed error at construction, the
        // same seam the named specs validate at (`positive_rate`).
        let _ = positive_rate(spec.dividend_yield, "leg set dividend yield")?;
        for leg in &spec.legs {
            let _ = positive_rate(leg.implied_volatility, "leg set implied volatility")?;
        }
        // Canonicalise HERE, at the one place every entry path funnels through
        // (`run_spec_with_feed`, and a caller building the strategy directly).
        // The `run_id` and the manifest already record the canonical spec, so
        // the ORDER THE ENGINE RUNS must be that same order: entry emits one
        // `Submit` per leg in this vector's order, and the engine mints
        // `order_id` / `position_id` / `trade_id` from monotonic counters in
        // submission order. Storing the caller's order instead would let one
        // `run_id` name two different `fills`/`positions` byte-sets — and under
        // realistic fills the arrival order reaches the book, so the divergence
        // would not even be confined to labels.
        let mut legs = spec.legs.clone();
        legs.sort_by(LegSpec::canonical_cmp);
        Ok(Self {
            legs,
            underlying: spec.underlying.clone(),
            entered: false,
            exit,
        })
    }

    /// `true` once the opening intents have been emitted (entry is one-shot).
    #[must_use]
    pub const fn has_entered(&self) -> bool {
        self.entered
    }

    /// Build the opening intents (one `Submit(Open)` per leg, in the canonical
    /// order [`Self::new`] stored), matched to `snapshot` quotes by the leg's
    /// **full contract identity** (underlying, expiration, strike, style), with
    /// a relative expiry resolved against the tape anchor first — see
    /// [`select_leg_set_quote`] for both, and for why a leg set demands the
    /// exact expiry rather than the adapter's single-candidate fallback. Guarded
    /// by `entered`, so it is a no-op after the first successful entry.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when the spec's underlying is not
    /// the snapshot's, or when a leg has no matching snapshot quote at its own
    /// (resolved) expiration, and
    /// [`BacktestError::Conversion`] / [`BacktestError::ArithmeticOverflow`] if a
    /// relative expiry cannot be resolved.
    fn open_entries(
        &mut self,
        snapshot: &ChainSnapshot,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        if self.entered {
            return Ok(());
        }
        // The underlying is checked once: a leg set is quoted by ONE chain, so a
        // spec written against another underlying is a configuration error, not
        // a per-leg "no quote" mismatch.
        if snapshot.underlying != self.underlying {
            return Err(BacktestError::Execution(format!(
                "leg set underlying {} is not the snapshot underlying {}",
                self.underlying.as_str(),
                snapshot.underlying.as_str()
            )));
        }
        for leg in &self.legs {
            let quote = select_leg_set_quote(snapshot, leg)?;
            out.push(OrderCommand::Submit(OrderIntent {
                contract: quote.contract.clone(),
                action: PositionAction::Open,
                side: leg.side,
                quantity: leg.quantity,
                limit: None,
                tif: TimeInForce::Ioc,
                decision_mid: quote.mid,
            }));
        }
        self.entered = true;
        Ok(())
    }
}

impl Strategy for LegSetStrategy {
    fn on_start(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        self.open_entries(ctx.snapshot, out)
    }

    fn exits(
        &mut self,
        ctx: &ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        if ctx.open.is_empty() {
            return Ok(());
        }
        // No `inner` to reprice — a leg set has no upstream strategy object — so
        // this is the shared per-leg decision and nothing else.
        evaluate_exits(&self.exit, ctx, out)
    }

    /// Nothing. Entry is **`on_start` only** for a leg set, which is what makes
    /// the anchor claim structural instead of a comment: the one snapshot a
    /// relative expiry resolves against is the one the loop opens with. Were
    /// entry retried here, a `Days(30)` leg reaching this at step k would
    /// resolve to `ts_k + 30d` — usually a typed miss, but on a chain quoting
    /// several tenors it lands on a REAL contract and silently opens a position
    /// the spec never named. `on_start` either enters or propagates, so this is
    /// not a behaviour change under [`crate::BacktestEngine::run`]; it removes
    /// the reach of a driver that would call `on_snapshot` alone.
    fn on_snapshot(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        let _ = (ctx, out);
        Ok(())
    }

    fn on_end(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        close_all(ctx.open, ctx.pending, ctx.snapshot, out)
    }

    fn exit_reason(&self) -> ExitReason {
        exit_policy_to_reason(&self.exit)
    }
}

/// Evaluate the configured [`ExitPolicy`] over the open inventory and append a
/// **closing** command for every triggered leg — the seam's single exit
/// decision, shared by [`OptStratAdapter`] and [`LegSetStrategy`].
///
/// `underlying` is the ONLY repricing output the v0.1 exit DECISION reads:
/// `check_exit_policy` (docs/specs/optionstratlib.md §9) consumes
/// snapshot-derived scalars only and never reads a wrapped strategy's repriced
/// Greeks. It is sourced directly from the snapshot scalar through the SAME
/// derivation `convert.rs` uses for `chain.underlying_price`
/// ([`positive_from_price`]), so the value is byte-identical to the old
/// `snapshot_to_option_chain(ctx.snapshot)?.underlying_price` — without
/// rebuilding the whole `OptionChain` each step (~44 alloc events/step, #18/#19)
/// or reaching the upstream `Utc::now()` expiry-check wall-clock. A conversion
/// failure is a genuine data error and propagates.
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] if the snapshot's underlying price is
/// not a valid `Positive`, or if a leg's expiry is unresolved, and
/// [`BacktestError::ArithmeticOverflow`] if the days-to-expiry scaling overflows.
fn evaluate_exits(
    exit: &ExitPolicy,
    ctx: &ChainContext,
    out: &mut Vec<OrderCommand>,
) -> Result<(), BacktestError> {
    let underlying = positive_from_price("underlying", ctx.snapshot.underlying_price)?;
    let step = ctx.step.value() as usize;
    let snapshot = ctx.snapshot;
    for leg in ctx.open {
        let days_left = days_to_expiry(&leg.contract, snapshot.ts)?;
        let is_long = matches!(leg.side, Side::Long);
        let initial = leg.entry_premium.to_decimal_dollars();
        let current = current_premium(snapshot, ctx.marks, leg);
        if policy_triggered(exit, initial, current, step, days_left, underlying, is_long) {
            out.push(close_command(snapshot, leg));
        }
    }
    Ok(())
}

/// Append a `Close` for every open leg **not already fully scheduled for close**
/// by an earlier phase this step (the exit policy, or an in-step adjustment),
/// flattening a partially-closed leg for its remaining size — the seam's single
/// terminal flatten, shared by [`OptStratAdapter`] and [`LegSetStrategy`].
///
/// On the terminal step the exit phase and `on_end` append into the **same**
/// buffer. A leg the exit policy already closed must not be closed again:
/// `apply_step_fills` removes a fully-closed leg from the inventory, and a
/// second `Close` of the now-absent leg aborts the run in `reduce_leg` (F11).
/// This reconciles against the commands already in `out` so each leg is closed
/// exactly once.
///
/// It scans the already-appended commands rather than allocating a scratch set:
/// `on_end` runs once, at the terminal step (off the warm-step path), so the
/// `O(legs × commands)` scan allocates nothing (PB-1).
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] if the scheduled-quantity sum
/// or the remainder overflows, and [`BacktestError::InvalidQuantity`] never (the
/// remainder is guarded strictly positive before it becomes a `Quantity`).
fn close_all(
    open: &[OpenPosition],
    pending: &[PendingOrder],
    snapshot: &ChainSnapshot,
    out: &mut Vec<OrderCommand>,
) -> Result<(), BacktestError> {
    for leg in open {
        let open_qty = leg.quantity.value();
        let mut scheduled = scheduled_close_qty(out, leg.position_id)?;
        // A resting (GTC) close is also scheduled coverage (#110): if it fills
        // in this final step's refresh it closes the leg itself, and
        // double-closing here would overshoot in reduce_leg. A pending remainder
        // that never fills leaves the leg honestly open_at_end.
        for pend in pending {
            if let PositionAction::Close(pid) = pend.intent.action
                && pid == leg.position_id
            {
                scheduled = scheduled
                    .checked_add(pend.intent.quantity.value())
                    .ok_or(BacktestError::ArithmeticOverflow)?;
            }
        }
        if scheduled >= open_qty {
            // Already fully closed this step — appending another close would
            // duplicate it and abort the run in reduce_leg.
            continue;
        }
        // `scheduled < open_qty`, so the remainder is strictly positive.
        let remaining = open_qty
            .checked_sub(scheduled)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let quantity = Quantity::new(remaining)?;
        out.push(close_command_qty(snapshot, leg, quantity));
    }
    Ok(())
}

/// Map an applied [`ExitPolicy`] to the [`ExitReason`] recorded on a
/// policy-triggered leg close ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
///
/// Profit-side policies map to [`ExitReason::TargetReached`], loss-side to
/// [`ExitReason::StopLoss`], and genuine expiration policies
/// ([`ExitPolicy::Expiration`], [`ExitPolicy::DaysToExpiration`]) to
/// [`ExitReason::Expiration`]. A step-count hold
/// ([`ExitPolicy::TimeSteps`]) is **not** an options expiry — it maps to the
/// truthful [`ExitReason::Other`]`("time_steps")` (upstream has no dedicated
/// holding-period variant, so recording `Expiration` would misattribute it). An
/// ambiguous or composite policy (whose specific triggering leaf the engine
/// cannot disambiguate) maps to a descriptive [`ExitReason::Other`] rather than
/// a fabricated specific reason. The mapping is a **pure** function of the
/// policy — no wall clock, no RNG — so it is deterministic.
#[must_use]
fn exit_policy_to_reason(policy: &ExitPolicy) -> ExitReason {
    match policy {
        ExitPolicy::ProfitPercent(_) => ExitReason::TargetReached,
        ExitPolicy::LossPercent(_) => ExitReason::StopLoss,
        ExitPolicy::Expiration | ExitPolicy::DaysToExpiration(_) => ExitReason::Expiration,
        // A step-count hold is a holding-period exit, not a contract expiry:
        // record it honestly rather than as Expiration.
        ExitPolicy::TimeSteps(_) => ExitReason::Other("time_steps".to_string()),
        ExitPolicy::MinPrice(_) => ExitReason::StopLoss,
        ExitPolicy::MaxPrice(_) | ExitPolicy::FixedPrice(_) => ExitReason::TargetReached,
        // A composite (or any variant whose triggering leaf is not
        // disambiguable) records a descriptive Other rather than guessing one.
        other => ExitReason::Other(other.to_string()),
    }
}

/// Round a `Positive` contract count to a strictly-positive [`Quantity`].
///
/// # Errors
///
/// Returns [`BacktestError::Strategy`] if the value is not a representable
/// `u32`, and [`BacktestError::InvalidQuantity`] if it rounds to zero.
fn quantity_from_positive(value: Positive) -> Result<Quantity, BacktestError> {
    let count = value.to_dec().round().to_u32().ok_or_else(|| {
        BacktestError::Strategy(format!(
            "leg quantity {value} is not a valid u32 contract count"
        ))
    })?;
    Quantity::new(count)
}

/// Select the snapshot quote for a strategy leg by its **full contract
/// identity** (underlying, expiration, strike, style).
///
/// When exactly one quote matches the leg's strike+style — the single-expiry
/// case — that quote is returned directly, byte-identical to the earlier
/// strike+style match and agnostic to whether the leg's `expiration` is a
/// relative `Days` or a resolved `DateTime`. When several **expirations** quote
/// the same strike/style, the leg is disambiguated by its own `expiration`
/// through the exact [`ContractKey`] identity (the per-underlying key is
/// `snapshot.underlying`, which every quote in the snapshot shares), never
/// whichever expiry sorts first in the map.
///
/// # Errors
///
/// Returns [`BacktestError::Execution`] when no quote matches the leg's
/// strike/style, or — in a multi-expiry snapshot — none matches its full
/// identity (e.g. an unresolved `Days` expiration against resolved quotes).
fn select_leg_quote(
    snapshot: &ChainSnapshot,
    strike: PriceCents,
    style: OptionStyle,
    expiration: ExpirationDate,
) -> Result<&QuoteView, BacktestError> {
    let mut candidates = snapshot
        .quotes
        .values()
        .filter(|q| q.contract.strike == strike && q.contract.style == style);
    let first = candidates.next().ok_or_else(|| {
        BacktestError::Execution(format!(
            "no snapshot quote for leg strike {} style {style:?}",
            strike.value()
        ))
    })?;
    if candidates.next().is_none() {
        // Exactly one strike+style quote: single-expiry — the historical match.
        return Ok(first);
    }
    // Several expirations quote this strike/style: match the leg's OWN
    // expiration exactly (never the first in map order).
    let contract = ContractKey {
        underlying: snapshot.underlying.clone(),
        expiration,
        strike,
        style,
    };
    snapshot.quotes.get(&contract).ok_or_else(|| {
        BacktestError::Execution(format!(
            "no snapshot quote for leg strike {} style {style:?} at expiration \
             {expiration:?} in a multi-expiry snapshot",
            strike.value()
        ))
    })
}

/// Select the snapshot quote for one [`LegSpec`] of an explicit leg set.
///
/// A leg set exists to express a **per-leg** expiration, so its legs are matched
/// on their exact [`ContractKey`] identity and nothing else: if the chain does
/// not quote that contract, that is a typed error, **not** a licence to fill on
/// whatever else happens to sit at the same strike and style. This is the one
/// place a leg set deliberately diverges from [`select_leg_quote`], whose
/// single-candidate fallback exists for the named specs — there a mismatch
/// cannot arise, because every leg shares the one strategy-level expiration the
/// tape was chosen for. Here it can: a mis-specified calendar leg would
/// otherwise open silently against the wrong week and the bundle would record a
/// contract the spec never asked for.
///
/// A leg's expiry is **resolved first**, against the snapshot's own timestamp,
/// through the crate's single implementation of that rule
/// ([`resolve_expiration`], [01 §5.1](../../../docs/01-domain-model.md#51-expiration-resolves-to-one-absolute-instant)).
/// A relative `Days(n)` therefore means what it says — `n` days past the tape
/// anchor — instead of being ignored or unmatchable, and a leg already written
/// as a `DateTime` passes through untouched. There is one matching mode either
/// way: exact identity, or a typed error.
///
/// # The anchor
///
/// Correctness depends on entry being **one-shot at step 0**: `snapshot` here is
/// the first snapshot, so its `ts` is the tape anchor `ts_0` that
/// [`crate::data::convert`] resolves the chain's own quotes against. That holds
/// because [`Strategy::on_start`] runs with the first snapshot and either enters
/// or propagates, and `entered` makes every later call a no-op — but it is
/// invisible at this call site, so it is stated here.
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] / [`BacktestError::ArithmeticOverflow`]
/// if the relative expiry cannot be resolved, and [`BacktestError::Execution`]
/// when no snapshot quote carries the leg's resolved identity.
fn select_leg_set_quote<'a>(
    snapshot: &'a ChainSnapshot,
    leg: &LegSpec,
) -> Result<&'a QuoteView, BacktestError> {
    let expiration = resolve_expiration(&leg.expiration, snapshot.ts)?;
    let contract = ContractKey {
        underlying: snapshot.underlying.clone(),
        expiration,
        strike: leg.strike,
        style: leg.style,
    };
    snapshot.quotes.get(&contract).ok_or_else(|| {
        BacktestError::Execution(format!(
            "no snapshot quote for leg set leg strike {} style {:?} at expiration \
             {:?} (resolved from {:?}); a leg set matches its own expiration exactly",
            leg.strike.value(),
            leg.style,
            expiration,
            leg.expiration
        ))
    })
}

/// The current mark of a leg in dollars: the snapshot mid of its contract, the
/// last-known carried-forward mark when the contract is absent this step
/// (`stale_mark`), or the leg's entry premium only when it was never marked
/// ([docs/01 §6](../../../docs/01-domain-model.md#6-market-data)).
#[must_use]
fn current_premium(
    snapshot: &ChainSnapshot,
    marks: &std::collections::BTreeMap<ContractKey, PriceCents>,
    leg: &OpenPosition,
) -> Decimal {
    // Quote present this step → its mid. Absent (a stale quote) → the ledger's
    // last-known carried-forward mark, so the exit decision sees the position's
    // prior movement rather than snapping back to the entry premium. Entry
    // premium only when the contract has never been marked (no mark exists yet).
    let cents = snapshot
        .quotes
        .get(&leg.contract)
        .map(|q| q.mid)
        .or_else(|| marks.get(&leg.contract).copied())
        .unwrap_or(leg.entry_premium);
    cents.to_decimal_dollars()
}

/// Build the closing `Submit` for one open leg's **full** open quantity: the
/// opposite trade side flattens it, priced against the snapshot mid (falling back
/// to entry premium if the contract is stale).
#[must_use]
fn close_command(snapshot: &ChainSnapshot, leg: &OpenPosition) -> OrderCommand {
    close_command_qty(snapshot, leg, leg.quantity)
}

/// Build the closing `Submit` for a specific `quantity` of one open leg — the
/// opposite trade side flattens it, priced against the snapshot mid (falling back
/// to entry premium if the contract is stale). Used to flatten the **remaining**
/// size of a partially-closed leg from `close_all`.
#[must_use]
fn close_command_qty(
    snapshot: &ChainSnapshot,
    leg: &OpenPosition,
    quantity: Quantity,
) -> OrderCommand {
    let decision_mid = snapshot
        .quotes
        .get(&leg.contract)
        .map_or(leg.entry_premium, |q| q.mid);
    OrderCommand::Submit(OrderIntent {
        contract: leg.contract.clone(),
        action: PositionAction::Close(leg.position_id),
        side: flip_side(leg.side),
        quantity,
        limit: None,
        tif: TimeInForce::Ioc,
        decision_mid,
    })
}

/// Total contracts already scheduled for close of `position_id` in `out` — the
/// checked sum over every `Submit(Close(position_id))` already appended this
/// step. Lets [`OptStratAdapter::close_all`] flatten only a leg's remaining size
/// (or skip a fully-closed leg) so the terminal step never double-closes (F11).
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] if the scheduled-quantity sum
/// overflows `u32`.
fn scheduled_close_qty(
    out: &[OrderCommand],
    position_id: crate::domain::PositionId,
) -> Result<u32, BacktestError> {
    let mut total: u32 = 0;
    for cmd in out {
        if let OrderCommand::Submit(OrderIntent {
            action: PositionAction::Close(pid),
            quantity,
            ..
        }) = cmd
            && *pid == position_id
        {
            total = total
                .checked_add(quantity.value())
                .ok_or(BacktestError::ArithmeticOverflow)?;
        }
    }
    Ok(total)
}

/// The trade side that flattens a leg opened on `side` (`Long` closed by a
/// sell, `Short` by a buy).
#[must_use]
const fn flip_side(side: Side) -> Side {
    match side {
        Side::Long => Side::Short,
        Side::Short => Side::Long,
    }
}

/// Days remaining from `ts` to the leg's absolute expiry, clamped to zero once
/// expired. Uses the fixed 86 400 s day (the naive session clock).
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] if the contract expiry is unresolved,
/// and [`BacktestError::ArithmeticOverflow`] if the day scaling overflows.
fn days_to_expiry(contract: &ContractKey, ts: SimTime) -> Result<Positive, BacktestError> {
    let expiration_ns = contract.expiration_ns()?;
    let diff = i128::from(expiration_ns) - i128::from(ts.value());
    if diff <= 0 {
        return Ok(Positive::ZERO);
    }
    let days = Decimal::from_i128_with_scale(diff, 0)
        .checked_div(Decimal::from_i128_with_scale(NANOS_PER_DAY, 0))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    Positive::new_decimal(days)
        .map_err(|e| BacktestError::Strategy(format!("days-to-expiry {days} is not positive: {e}")))
}

/// Whether the configured [`ExitPolicy`] reads the wrapped strategy's repriced
/// `inner` state (its Greeks), which would require a per-step reprice of `inner`.
///
/// **`false` for every v0.1 policy.** The v0.1 exit decision consumes
/// snapshot-derived scalars only via [`check_exit_policy`], and
/// [`ExitPolicy::DeltaThreshold`] is unsupported (it never triggers, so it reads
/// nothing). Gating the reprice on this keeps the v0.1 step path free of the
/// per-step `OptionChain` rebuild — and its upstream `Utc::now()` reach — while
/// leaving the door open for a future Greek-driven policy: wiring a supported
/// `DeltaThreshold` (reading `inner`'s per-leg delta, #11/#14) flips this to
/// `true` for that policy, re-enabling [`OptStratAdapter::reprice_inner`] in
/// [`Strategy::exits`].
#[must_use]
fn policy_reads_inner(policy: &ExitPolicy) -> bool {
    match policy {
        // Composites read `inner` iff any child does.
        ExitPolicy::And(policies) | ExitPolicy::Or(policies) => {
            policies.iter().any(policy_reads_inner)
        }
        // No v0.1 leaf reads `inner`. `DeltaThreshold` is unsupported today
        // (never triggers); the future supported variant returns `true` here.
        _ => false,
    }
}

/// Evaluate the configured [`ExitPolicy`] for one leg.
///
/// Delegates scalar leaves to [`check_exit_policy`] and composes `And`/`Or`
/// itself so it can inject the two variants the upstream checker returns `None`
/// for: [`ExitPolicy::Expiration`] triggers at zero days left (the naive
/// hold-to-expiry rule), and [`ExitPolicy::DeltaThreshold`] is **unsupported in
/// v0.1** — it never triggers (per-leg delta wiring is deferred; documented so
/// the no-op is intentional, not silent).
#[must_use]
fn policy_triggered(
    policy: &ExitPolicy,
    initial_premium: Decimal,
    current_premium: Decimal,
    step_num: usize,
    days_left: Positive,
    underlying_price: Positive,
    is_long: bool,
) -> bool {
    match policy {
        ExitPolicy::And(policies) => policies.iter().all(|p| {
            policy_triggered(
                p,
                initial_premium,
                current_premium,
                step_num,
                days_left,
                underlying_price,
                is_long,
            )
        }),
        ExitPolicy::Or(policies) => policies.iter().any(|p| {
            policy_triggered(
                p,
                initial_premium,
                current_premium,
                step_num,
                days_left,
                underlying_price,
                is_long,
            )
        }),
        ExitPolicy::Expiration => days_left.to_dec().is_zero(),
        ExitPolicy::DeltaThreshold(_) => false,
        leaf => check_exit_policy(
            leaf,
            initial_premium,
            current_premium,
            step_num,
            days_left,
            underlying_price,
            is_long,
        )
        .is_some(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use rand_chacha::ChaCha8Rng;
    use rand_chacha::rand_core::SeedableRng;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use optionstratlib::prelude::Positive;
    use optionstratlib::simulation::ExitPolicy;
    use optionstratlib::strategies::{IronCondor, ShortStrangle};
    use optionstratlib::{ExpirationDate, OptionStyle, Side};

    use super::{
        ChainContext, LegSetStrategy, OptStratAdapter, PositionableStrategy, Strategy,
        policy_reads_inner, policy_triggered,
    };
    use crate::domain::{
        ChainSnapshot, ContractKey, InstrumentSpec, IronCondorSpec, LegSetSpec, LegSpec,
        OpenPosition, OrderCommand, OrderId, OrderIntent, PendingOrder, PositionAction, PositionId,
        PriceCents, Quantity, QuoteView, ShortStrangleSpec, SimTime, StepIndex, StrategySpec,
        Underlying,
    };
    use crate::error::BacktestError;

    /// A shared empty last-known-marks map for context fixtures that do not
    /// exercise stale-quote carry-forward (F9); the stale-quote test builds its
    /// own populated map.
    static NO_MARKS: BTreeMap<ContractKey, PriceCents> = BTreeMap::new();

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

    // --- fixtures -----------------------------------------------------------

    /// A resolved expiry 30 calendar days after the snapshot timestamp.
    const TS_NS: i64 = 1_750_291_200_000_000_000;
    const EXP_NS: i64 = TS_NS + 30 * super::NANOS_PER_DAY as i64;

    fn pos(value: Decimal) -> Positive {
        let Ok(p) = Positive::new_decimal(value) else {
            panic!("{value} is a valid positive");
        };
        p
    }

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        u
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    fn key(strike_cents: u64, style: OptionStyle) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXP_NS)),
            strike: PriceCents::new(strike_cents),
            style,
        }
    }

    fn quote(strike_cents: u64, style: OptionStyle, mid_cents: u64) -> QuoteView {
        // Fixtures always pass `mid_cents >= 10`, so the spread stays positive
        // without `saturating_*` (banned by the rules, even in tests).
        debug_assert!(mid_cents >= 10, "quote fixtures use a mid of at least 10c");
        QuoteView {
            contract: key(strike_cents, style),
            bid: PriceCents::new(mid_cents - 10),
            ask: PriceCents::new(mid_cents + 10),
            mid: PriceCents::new(mid_cents),
            bid_size: qty(50),
            ask_size: qty(50),
            implied_volatility: dec!(0.20),
            delta: dec!(0.30),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.10),
        }
    }

    /// A contract key at an explicit expiration (ns) — the multi-expiry fixture
    /// for the full-identity match test.
    fn key_at(strike_cents: u64, style: OptionStyle, exp_ns: i64) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(exp_ns)),
            strike: PriceCents::new(strike_cents),
            style,
        }
    }

    /// A quote at an explicit expiration (ns) — the multi-expiry fixture.
    fn quote_at(strike_cents: u64, style: OptionStyle, mid_cents: u64, exp_ns: i64) -> QuoteView {
        debug_assert!(mid_cents >= 10, "quote fixtures use a mid of at least 10c");
        QuoteView {
            contract: key_at(strike_cents, style, exp_ns),
            bid: PriceCents::new(mid_cents - 10),
            ask: PriceCents::new(mid_cents + 10),
            mid: PriceCents::new(mid_cents),
            bid_size: qty(50),
            ask_size: qty(50),
            implied_volatility: dec!(0.20),
            delta: dec!(0.30),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.10),
        }
    }

    /// Snapshot carrying the four Iron Condor legs (short/long call, short/long
    /// put) at the strikes [`iron_condor`] is built with.
    fn snapshot(step: u32) -> ChainSnapshot {
        let mut quotes = BTreeMap::new();
        for (strike, style, mid) in [
            (490_000u64, OptionStyle::Put, 1_800u64),
            (480_000, OptionStyle::Put, 700),
            (510_000, OptionStyle::Call, 2_000),
            (520_000, OptionStyle::Call, 800),
        ] {
            let q = quote(strike, style, mid);
            quotes.insert(q.contract.clone(), q);
        }
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("valid instrument spec");
        };
        ChainSnapshot {
            ts: SimTime::new(TS_NS),
            step: StepIndex::new(step),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes,
        }
    }

    /// A real `IronCondor` at strikes 4800/4900/5100/5200, underlying 5000.
    fn iron_condor() -> IronCondor {
        let Ok(condor) = IronCondor::new(
            "SPX".to_string(),
            pos(dec!(5000)),
            pos(dec!(5100)), // short call
            pos(dec!(4900)), // short put
            pos(dec!(5200)), // long call
            pos(dec!(4800)), // long put
            ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXP_NS)),
            pos(dec!(0.20)),
            dec!(0.05),
            Positive::ZERO,
            pos(dec!(1)),
            pos(dec!(20)), // premium short call
            pos(dec!(18)), // premium short put
            pos(dec!(8)),  // premium long call
            pos(dec!(7)),  // premium long put
            pos(dec!(0.65)),
            pos(dec!(0.65)),
        ) else {
            panic!("valid iron condor construction");
        };
        condor
    }

    fn adapter(exit: ExitPolicy) -> OptStratAdapter<IronCondor> {
        OptStratAdapter::new(iron_condor(), exit)
    }

    /// A [`StrategySpec`] whose IronCondor strikes match the [`snapshot`]
    /// quotes (cents): short call 5100, short put 4900, long call 5200, long
    /// put 4800; money fields are integer cents, rates/vol are `Decimal`.
    fn iron_condor_spec() -> StrategySpec {
        StrategySpec::IronCondor(IronCondorSpec {
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            short_call_strike: PriceCents::new(510_000),
            short_put_strike: PriceCents::new(490_000),
            long_call_strike: PriceCents::new(520_000),
            long_put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXP_NS)),
            implied_volatility: dec!(0.20),
            risk_free_rate: dec!(0.05),
            dividend_yield: Decimal::ZERO,
            quantity: qty(1),
            premium_short_call: PriceCents::new(2_000),
            premium_short_put: PriceCents::new(1_800),
            premium_long_call: PriceCents::new(800),
            premium_long_put: PriceCents::new(700),
            open_fee: PriceCents::new(65),
            close_fee: PriceCents::new(65),
        })
    }

    /// Build the adapter through the `StrategySpec → IronCondor` seam.
    fn adapter_from_spec(exit: ExitPolicy) -> Result<OptStratAdapter<IronCondor>, BacktestError> {
        OptStratAdapter::<IronCondor>::from_spec(&iron_condor_spec(), exit)
    }

    /// The four open legs the engine would hold after entry, with matching
    /// contracts and per-contract entry premia (cents).
    fn open_legs() -> Vec<OpenPosition> {
        vec![
            OpenPosition {
                position_id: PositionId::new(1),
                contract: key(510_000, OptionStyle::Call),
                side: Side::Short,
                quantity: qty(1),
                entry_premium: PriceCents::new(2_000),
            },
            OpenPosition {
                position_id: PositionId::new(2),
                contract: key(520_000, OptionStyle::Call),
                side: Side::Long,
                quantity: qty(1),
                entry_premium: PriceCents::new(800),
            },
            OpenPosition {
                position_id: PositionId::new(3),
                contract: key(490_000, OptionStyle::Put),
                side: Side::Short,
                quantity: qty(1),
                entry_premium: PriceCents::new(1_800),
            },
            OpenPosition {
                position_id: PositionId::new(4),
                contract: key(480_000, OptionStyle::Put),
                side: Side::Long,
                quantity: qty(1),
                entry_premium: PriceCents::new(700),
            },
        ]
    }

    // --- ShortStrangle fixtures (the v0.2 second strategy, #28) -------------

    /// A real `ShortStrangle`: short call 5200, short put 4800, underlying 5000
    /// — both OTM legs quoted in the shared [`snapshot`] fixture.
    fn short_strangle() -> ShortStrangle {
        let Ok(strangle) = ShortStrangle::new(
            "SPX".to_string(),
            pos(dec!(5000)),
            pos(dec!(5200)), // short call strike (OTM, above spot)
            pos(dec!(4800)), // short put strike (OTM, below spot)
            ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXP_NS)),
            pos(dec!(0.20)), // call implied volatility
            pos(dec!(0.20)), // put implied volatility
            dec!(0.05),
            Positive::ZERO,
            pos(dec!(1)),
            pos(dec!(8)),    // premium short call
            pos(dec!(7)),    // premium short put
            pos(dec!(0.65)), // open fee short call
            pos(dec!(0.65)), // close fee short call
            pos(dec!(0.65)), // open fee short put
            pos(dec!(0.65)), // close fee short put
        ) else {
            panic!("valid short strangle construction");
        };
        strangle
    }

    fn strangle_adapter(exit: ExitPolicy) -> OptStratAdapter<ShortStrangle> {
        OptStratAdapter::new(short_strangle(), exit)
    }

    /// A [`StrategySpec`] whose ShortStrangle strikes match the [`snapshot`]
    /// quotes (cents): short call 5200, short put 4800.
    fn short_strangle_spec() -> StrategySpec {
        StrategySpec::ShortStrangle(ShortStrangleSpec {
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            call_strike: PriceCents::new(520_000),
            put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXP_NS)),
            call_implied_volatility: dec!(0.20),
            put_implied_volatility: dec!(0.20),
            risk_free_rate: dec!(0.05),
            dividend_yield: Decimal::ZERO,
            quantity: qty(1),
            premium_short_call: PriceCents::new(800),
            premium_short_put: PriceCents::new(700),
            open_fee_short_call: PriceCents::new(65),
            close_fee_short_call: PriceCents::new(65),
            open_fee_short_put: PriceCents::new(65),
            close_fee_short_put: PriceCents::new(65),
        })
    }

    /// Build the short-strangle adapter through the `StrategySpec → ShortStrangle`
    /// seam.
    fn strangle_adapter_from_spec(
        exit: ExitPolicy,
    ) -> Result<OptStratAdapter<ShortStrangle>, BacktestError> {
        OptStratAdapter::<ShortStrangle>::from_spec(&short_strangle_spec(), exit)
    }

    /// The two open legs the engine would hold after a short-strangle entry,
    /// both short, with matching contracts and per-contract entry premia (cents).
    fn strangle_open_legs() -> Vec<OpenPosition> {
        vec![
            OpenPosition {
                position_id: PositionId::new(1),
                contract: key(520_000, OptionStyle::Call),
                side: Side::Short,
                quantity: qty(1),
                entry_premium: PriceCents::new(800),
            },
            OpenPosition {
                position_id: PositionId::new(2),
                contract: key(480_000, OptionStyle::Put),
                side: Side::Short,
                quantity: qty(1),
                entry_premium: PriceCents::new(700),
            },
        ]
    }

    fn is_open(cmd: &OrderCommand) -> bool {
        matches!(
            cmd,
            OrderCommand::Submit(OrderIntent {
                action: PositionAction::Open,
                ..
            })
        )
    }

    fn is_close(cmd: &OrderCommand) -> bool {
        matches!(
            cmd,
            OrderCommand::Submit(OrderIntent {
                action: PositionAction::Close(_),
                ..
            })
        )
    }

    // --- entry --------------------------------------------------------------

    #[test]
    fn test_on_snapshot_first_call_opens_four_condor_legs() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let mut adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 4, "one open per condor leg");
        assert!(out.iter().all(is_open), "on_snapshot appends opens only");
        assert!(adapter.has_entered());
    }

    #[test]
    fn test_on_snapshot_second_call_is_noop_via_entered_guard() {
        let mut rng = ChaCha8Rng::seed_from_u64(2);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let mut adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        out.clear();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert!(out.is_empty(), "entered flag prevents a second entry");
    }

    // --- ShortStrangle through the SAME seam (issue #28) --------------------

    #[test]
    fn test_short_strangle_on_snapshot_opens_two_legs_through_same_seam() {
        // A ShortStrangle wrapped in the SAME generic OptStratAdapter, driven
        // through the SAME Strategy::on_snapshot seam as IronCondor — no
        // strategy-specific branch. A two-leg strategy emits two opens.
        let mut rng = ChaCha8Rng::seed_from_u64(30);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let mut adapter = strangle_adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 2, "one open per strangle leg (short call + put)");
        assert!(out.iter().all(is_open), "on_snapshot appends opens only");
        assert!(adapter.has_entered());
    }

    #[test]
    fn test_short_strangle_second_on_snapshot_is_noop_via_entered_guard() {
        // The one-shot entry guard is the generic adapter's, not IronCondor's —
        // it holds identically for the strangle.
        let mut rng = ChaCha8Rng::seed_from_u64(31);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let mut adapter = strangle_adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        out.clear();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert!(out.is_empty(), "entered flag prevents a second entry");
    }

    #[test]
    fn test_short_strangle_exits_emits_two_closes_when_policy_triggers() {
        // Exit-policy evaluation routes through the adapter's OWNED exits(),
        // identically to IronCondor: one close per open leg when it fires.
        let mut rng = ChaCha8Rng::seed_from_u64(32);
        let snap = snapshot(7);
        let legs = strangle_open_legs();
        let ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(7),
        };
        let mut adapter = strangle_adapter(ExitPolicy::TimeSteps(0));
        let mut out = Vec::new();
        assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 2, "one close per open leg when the policy fires");
        assert!(out.iter().all(is_close), "exits appends closes only");
        // The closing side flattens each short leg: it is bought back.
        assert!(out.iter().all(|c| matches!(
            c,
            OrderCommand::Submit(OrderIntent {
                side: Side::Long,
                action: PositionAction::Close(_),
                ..
            })
        )));
    }

    #[test]
    fn test_short_strangle_on_end_appends_only_closes() {
        let mut rng = ChaCha8Rng::seed_from_u64(33);
        let snap = snapshot(11);
        let legs = strangle_open_legs();
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(11),
        };
        let mut adapter = strangle_adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_end(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(is_close), "on_end closes only");
        assert!(!out.iter().any(is_open), "on_end never opens");
    }

    #[test]
    fn test_short_strangle_satisfies_positionable_strategy_bound() {
        // The full triple (Positionable + Strategies + Optimizable, no Default)
        // holds for ShortStrangle upstream — the same bound IronCondor meets.
        // Referencing the monomorphised guard fn proves the bound at compile
        // time; building the adapter through from_spec proves the concrete
        // construction path accepts it.
        let _bound_holds: fn() = assert_positionable_strategy::<ShortStrangle>;
        assert!(strangle_adapter_from_spec(ExitPolicy::Expiration).is_ok());
    }

    #[test]
    fn test_short_strangle_from_spec_entry_emits_two_open_intents() {
        let mut rng = ChaCha8Rng::seed_from_u64(34);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let Ok(mut adapter) = strangle_adapter_from_spec(ExitPolicy::Expiration) else {
            panic!("the short strangle spec builds a valid adapter");
        };
        let mut out = Vec::new();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 2, "the two strangle legs emit two opens");
        assert!(out.iter().all(is_open), "entry appends opens only");
        assert!(adapter.has_entered());
        // Dormancy guard: each Open's decision_mid is the SNAPSHOT quote mid,
        // never a repriced inner premium — so no wall-clock reprice reaches it.
        for cmd in &out {
            let OrderCommand::Submit(intent) = cmd else {
                panic!("entry emits Submit intents");
            };
            let Some(quote) = snap.quotes.get(&intent.contract) else {
                panic!("each entry leg matches a snapshot quote");
            };
            assert_eq!(intent.decision_mid, quote.mid);
            assert_eq!(intent.side, Side::Short, "both strangle legs are short");
        }
    }

    #[test]
    fn test_from_spec_rejects_mismatched_strategy_kind() {
        // Each concrete from_spec is kind-checked: the ShortStrangle constructor
        // refuses an IronCondor spec and vice versa — a typed BacktestError,
        // never a silent wrong build. (No generic-adapter change: the rejection
        // lives in the per-concrete-type from_spec.)
        let strangle_from_condor = OptStratAdapter::<ShortStrangle>::from_spec(
            &iron_condor_spec(),
            ExitPolicy::Expiration,
        );
        assert!(matches!(
            strangle_from_condor,
            Err(BacktestError::Strategy(_))
        ));
        let condor_from_strangle = OptStratAdapter::<IronCondor>::from_spec(
            &short_strangle_spec(),
            ExitPolicy::Expiration,
        );
        assert!(matches!(
            condor_from_strangle,
            Err(BacktestError::Strategy(_))
        ));
    }

    // --- StrategySpec -> IronCondor construction seam (issue #11) -----------

    #[test]
    fn test_iron_condor_satisfies_positionable_strategy_bound() {
        // The full triple (Positionable + Strategies + Optimizable, no Default)
        // holds for IronCondor — the Optimizable open question is resolved
        // affirmative (docs/02 §4.1). Referencing the monomorphised guard fn
        // proves the bound at compile time; building the adapter through
        // from_spec proves the concrete construction path accepts it.
        let _bound_holds: fn() = assert_positionable_strategy::<IronCondor>;
        assert!(adapter_from_spec(ExitPolicy::Expiration).is_ok());
    }

    #[test]
    fn test_iron_condor_entry_emits_four_open_intents() {
        let mut rng = ChaCha8Rng::seed_from_u64(20);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let Ok(mut adapter) = adapter_from_spec(ExitPolicy::Expiration) else {
            panic!("the iron condor spec builds a valid adapter");
        };
        let mut out = Vec::new();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert_eq!(
            out.len(),
            4,
            "the four condor legs emit four opens in one step"
        );
        assert!(out.iter().all(is_open), "entry appends opens only");
        assert!(adapter.has_entered());
        // Dormancy guard: each Open's decision_mid is the SNAPSHOT quote mid,
        // never a repriced inner premium — so no wall-clock reprice reaches the
        // emitted intent.
        for cmd in &out {
            let OrderCommand::Submit(intent) = cmd else {
                panic!("entry emits Submit intents");
            };
            let Some(quote) = snap.quotes.get(&intent.contract) else {
                panic!("each entry leg matches a snapshot quote");
            };
            assert_eq!(intent.decision_mid, quote.mid);
        }
    }

    #[test]
    fn test_open_entries_picks_the_legs_expiration_in_a_multi_expiry_snapshot() {
        // A snapshot quotes each condor strike/style at TWO expirations: the
        // leg's own EXP_NS and a decoy later one. open_entries must match the
        // FULL contract identity and emit opens at EXP_NS — never the decoy that
        // merely shares strike+style (and would win a strike+style-only match).
        const EXP2_NS: i64 = EXP_NS + 7 * super::NANOS_PER_DAY as i64;
        let mut quotes = BTreeMap::new();
        for (strike, style, mid) in [
            (490_000u64, OptionStyle::Put, 1_800u64),
            (480_000, OptionStyle::Put, 700),
            (510_000, OptionStyle::Call, 2_000),
            (520_000, OptionStyle::Call, 800),
        ] {
            // Insert the decoy expiration FIRST (it sorts earlier in the map's
            // key order for a strike+style-only `find`), then the leg's own.
            for exp_ns in [EXP_NS, EXP2_NS] {
                let q = quote_at(strike, style, mid, exp_ns);
                quotes.insert(q.contract.clone(), q);
            }
        }
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("valid instrument spec");
        };
        let snap = ChainSnapshot {
            ts: SimTime::new(TS_NS),
            step: StepIndex::new(0),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes,
        };
        let mut rng = ChaCha8Rng::seed_from_u64(50);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        // `iron_condor()` (and `adapter`) is built at EXP_NS.
        let mut adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 4, "the four condor legs emit four opens");
        for cmd in &out {
            let OrderCommand::Submit(intent) = cmd else {
                panic!("entry emits Submit intents");
            };
            assert_eq!(
                intent.contract.expiration_ns().ok(),
                Some(EXP_NS),
                "each leg targets its own expiration, never the decoy EXP2_NS"
            );
        }
    }

    #[test]
    fn test_iron_condor_from_spec_exits_emits_closes_when_policy_triggers() {
        let mut rng = ChaCha8Rng::seed_from_u64(21);
        let snap = snapshot(7);
        let legs = open_legs();
        let ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(7),
        };
        // TimeSteps(0) always triggers; v0.1 exits decides from snapshot
        // scalars only (no per-step inner reprice — `policy_reads_inner` false).
        let Ok(mut adapter) = adapter_from_spec(ExitPolicy::TimeSteps(0)) else {
            panic!("the iron condor spec builds a valid adapter");
        };
        let mut out = Vec::new();
        assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 4, "one close per open leg when the policy fires");
        assert!(out.iter().all(is_close), "exits appends closes only");
    }

    #[test]
    fn test_strategy_error_maps_to_backtest_error() {
        // IronCondor::new is effectively infallible for valid `Positive`
        // inputs, so the reliable upstream rejection is a parameter the
        // `Positive` domain refuses: a negative implied volatility. It surfaces
        // as BacktestError::Strategy at the construction seam, never a panic.
        let StrategySpec::IronCondor(mut inner) = iron_condor_spec() else {
            panic!("iron_condor_spec builds an IronCondor spec");
        };
        inner.implied_volatility = dec!(-0.20);
        let spec = StrategySpec::IronCondor(inner);
        let result = OptStratAdapter::<IronCondor>::from_spec(&spec, ExitPolicy::Expiration);
        assert!(matches!(result, Err(BacktestError::Strategy(_))));
    }

    #[test]
    fn test_on_start_opens_then_on_snapshot_is_noop() {
        let mut rng = ChaCha8Rng::seed_from_u64(3);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let mut adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_start(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 4);
        out.clear();
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));
        assert!(
            out.is_empty(),
            "on_start already entered; on_snapshot no-ops"
        );
    }

    // --- exits --------------------------------------------------------------

    #[test]
    fn test_exits_appends_only_closes_when_policy_triggers() {
        let mut rng = ChaCha8Rng::seed_from_u64(4);
        let snap = snapshot(7);
        let legs = open_legs();
        let ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(7),
        };
        // TimeSteps(0) always triggers (step 7 >= 0).
        let mut adapter = adapter(ExitPolicy::TimeSteps(0));
        let mut out = Vec::new();
        assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 4, "one close per open leg");
        assert!(out.iter().all(is_close), "exits appends closes only");
        // The closing side flattens the leg: a short leg is bought back.
        let short_call_close = out.iter().find_map(|c| match c {
            OrderCommand::Submit(i) if i.action == PositionAction::Close(PositionId::new(1)) => {
                Some(i.side)
            }
            _ => None,
        });
        assert!(matches!(short_call_close, Some(Side::Long)));
    }

    #[test]
    fn test_exits_appends_nothing_when_policy_not_triggered() {
        let mut rng = ChaCha8Rng::seed_from_u64(5);
        let snap = snapshot(0);
        let legs = open_legs();
        let ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        // TimeSteps(1000) not reached at step 0; prices sit far from any target.
        let mut adapter = adapter(ExitPolicy::TimeSteps(1000));
        let mut out = Vec::new();
        assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        assert!(out.is_empty(), "no leg triggers; nothing appended");
    }

    #[test]
    fn test_exits_empty_open_is_noop() {
        let mut rng = ChaCha8Rng::seed_from_u64(6);
        let snap = snapshot(3);
        let ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(3),
        };
        let mut adapter = adapter(ExitPolicy::TimeSteps(0));
        let mut out = Vec::new();
        assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        assert!(out.is_empty(), "no open legs: nothing to reprice or close");
    }

    #[test]
    fn test_exits_stale_quote_reads_carried_mark_not_entry_premium() {
        // A leg whose contract is ABSENT this step must have its exit decision
        // read the ledger's carried last-known mark, not snap back to the entry
        // premium. Short leg, entry 2000c; carried mark 500c is a 75% premium
        // decay, so ProfitPercent(0.5) fires — whereas the entry-premium fallback
        // (0% decay) would not.
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("valid instrument spec");
        };
        // The leg's contract is NOT quoted this step (a stale quote).
        let snap = ChainSnapshot {
            ts: SimTime::new(TS_NS),
            step: StepIndex::new(5),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes: BTreeMap::new(),
        };
        let legs = vec![OpenPosition {
            position_id: PositionId::new(1),
            contract: key(510_000, OptionStyle::Call),
            side: Side::Short,
            quantity: qty(1),
            entry_premium: PriceCents::new(2_000),
        }];
        // The ledger carries a last-known mark of 500c for that contract.
        let mut carried = BTreeMap::new();
        carried.insert(key(510_000, OptionStyle::Call), PriceCents::new(500));

        // With the carried mark, ProfitPercent(0.5) fires (75% decay > 50%).
        let mut rng = ChaCha8Rng::seed_from_u64(60);
        let ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &carried,
            rng: &mut rng,
            step: StepIndex::new(5),
        };
        let mut marked_adapter = adapter(ExitPolicy::ProfitPercent(dec!(0.5)));
        let mut out = Vec::new();
        assert!(matches!(marked_adapter.exits(&ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 1, "the carried 75% profit fires the exit");
        assert!(out.iter().all(is_close), "exits appends closes only");

        // Control: with NO carried mark the decision falls back to the entry
        // premium (0% decay) and the same policy does NOT fire.
        let mut rng2 = ChaCha8Rng::seed_from_u64(61);
        let ctx_no_mark = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng2,
            step: StepIndex::new(5),
        };
        let mut unmarked_adapter = adapter(ExitPolicy::ProfitPercent(dec!(0.5)));
        let mut out2 = Vec::new();
        assert!(matches!(
            unmarked_adapter.exits(&ctx_no_mark, &mut out2),
            Ok(())
        ));
        assert!(
            out2.is_empty(),
            "no carried mark → entry premium → no profit → no close"
        );
    }

    // --- combined ordering: closes strictly before entries ------------------

    #[test]
    fn test_exits_then_on_snapshot_orders_closes_before_entries() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let snap = snapshot(9);
        let legs = open_legs();
        // Deliberately not-yet-entered while holding open legs, to force both
        // closes (exits) and entries (on_snapshot) into ONE buffer and assert
        // the loop's exits-before-on_snapshot ordering.
        let mut adapter = adapter(ExitPolicy::TimeSteps(0));
        let mut out = Vec::new();
        {
            let ctx = ChainContext {
                snapshot: &snap,
                open: &legs,
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(9),
            };
            assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        }
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(9),
        };
        assert!(matches!(adapter.on_snapshot(&mut ctx, &mut out), Ok(())));

        let first_open = out.iter().position(is_open);
        let last_close = out.iter().rposition(is_close);
        assert!(matches!((last_close, first_open), (Some(lc), Some(fo)) if lc < fo));
        assert_eq!(out.iter().filter(|&c| is_close(c)).count(), 4);
        assert_eq!(out.iter().filter(|&c| is_open(c)).count(), 4);
    }

    // --- on_end -------------------------------------------------------------

    #[test]
    fn test_on_end_appends_only_closes() {
        let mut rng = ChaCha8Rng::seed_from_u64(8);
        let snap = snapshot(11);
        let legs = open_legs();
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(11),
        };
        let mut adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(adapter.on_end(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 4);
        assert!(out.iter().all(is_close), "on_end closes only");
        assert!(!out.iter().any(is_open), "on_end never opens");
    }

    #[test]
    fn test_on_end_after_exits_produces_one_close_per_leg() {
        // On the terminal step BOTH the exit phase (policy fires) and on_end
        // append into the SAME buffer. on_end must skip legs the policy already
        // closed, so each leg gets exactly ONE close — a duplicate would abort
        // the run in reduce_leg once apply_step_fills removed the leg (F11).
        let mut rng = ChaCha8Rng::seed_from_u64(70);
        let snap = snapshot(9);
        let legs = open_legs();
        // TimeSteps(0) fires at step 9 ⇒ the exit phase closes all four legs.
        let mut adapter = adapter(ExitPolicy::TimeSteps(0));
        let mut out = Vec::new();
        {
            let ctx = ChainContext {
                snapshot: &snap,
                open: &legs,
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(9),
            };
            assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        }
        assert_eq!(out.len(), 4, "the policy closes all four legs");
        // on_end into the SAME buffer must NOT re-close them.
        {
            let mut ctx = ChainContext {
                snapshot: &snap,
                open: &legs,
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(9),
            };
            assert!(matches!(adapter.on_end(&mut ctx, &mut out), Ok(())));
        }
        assert_eq!(out.len(), 4, "on_end appended no duplicate closes");
        // Exactly one close per position_id (1..=4).
        for pid in [1u64, 2, 3, 4] {
            let count = out
                .iter()
                .filter(|c| {
                    matches!(
                        c,
                        OrderCommand::Submit(OrderIntent {
                            action: PositionAction::Close(p),
                            ..
                        }) if p.value() == pid
                    )
                })
                .count();
            assert_eq!(count, 1, "exactly one close for position {pid}");
        }
    }

    #[test]
    fn test_on_end_closes_a_leg_the_exit_phase_left_open() {
        // A leg the exit phase did NOT close is still flattened by on_end at the
        // terminal step — the reconciliation skips only already-scheduled legs.
        let mut rng = ChaCha8Rng::seed_from_u64(71);
        let snap = snapshot(9);
        let legs = open_legs();
        // A non-firing policy: the exit phase appends nothing.
        let mut adapter = adapter(ExitPolicy::TimeSteps(1_000));
        let mut out = Vec::new();
        {
            let ctx = ChainContext {
                snapshot: &snap,
                open: &legs,
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(9),
            };
            assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        }
        assert!(out.is_empty(), "the policy did not fire");
        {
            let mut ctx = ChainContext {
                snapshot: &snap,
                open: &legs,
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(9),
            };
            assert!(matches!(adapter.on_end(&mut ctx, &mut out), Ok(())));
        }
        assert_eq!(out.len(), 4, "on_end flattens every still-open leg");
        assert!(out.iter().all(is_close));
    }

    // --- pending-order control ---------------------------------------------

    #[test]
    fn test_request_cancel_foreign_order_is_execution_error() {
        let mut rng = ChaCha8Rng::seed_from_u64(9);
        let snap = snapshot(0);
        let ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[], // no resting strategy orders
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        let result = adapter.request_cancel(&ctx, OrderId::new(999), &mut out);
        assert!(matches!(result, Err(BacktestError::Execution(_))));
        assert!(out.is_empty(), "a foreign cancel emits nothing");
    }

    #[test]
    fn test_request_cancel_owned_order_emits_cancel() {
        let mut rng = ChaCha8Rng::seed_from_u64(10);
        let snap = snapshot(0);
        let resting = vec![PendingOrder {
            order_id: OrderId::new(42),
            intent: OrderIntent {
                contract: key(510_000, OptionStyle::Call),
                action: PositionAction::Open,
                side: Side::Short,
                quantity: qty(1),
                limit: Some(PriceCents::new(2_000)),
                tif: crate::domain::TimeInForce::Gtc,
                decision_mid: PriceCents::new(2_000),
            },
        }];
        let ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &resting,
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        assert!(matches!(
            adapter.request_cancel(&ctx, OrderId::new(42), &mut out),
            Ok(())
        ));
        assert!(matches!(out.as_slice(), [OrderCommand::Cancel(id)] if id.value() == 42));
    }

    #[test]
    fn test_request_replace_foreign_order_is_execution_error() {
        let mut rng = ChaCha8Rng::seed_from_u64(11);
        let snap = snapshot(0);
        let ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let adapter = adapter(ExitPolicy::Expiration);
        let mut out = Vec::new();
        let replacement = OrderIntent {
            contract: key(510_000, OptionStyle::Call),
            action: PositionAction::Open,
            side: Side::Short,
            quantity: qty(1),
            limit: Some(PriceCents::new(1_950)),
            tif: crate::domain::TimeInForce::Gtc,
            decision_mid: PriceCents::new(1_950),
        };
        let result = adapter.request_replace(&ctx, OrderId::new(7), replacement, &mut out);
        assert!(matches!(result, Err(BacktestError::Execution(_))));
        assert!(out.is_empty());
    }

    // --- exit-policy special cases -----------------------------------------

    #[test]
    fn test_policy_triggered_expiration_only_at_zero_days() {
        // Expiration returns None from check_exit_policy; the adapter handles it
        // via days-left == 0.
        assert!(policy_triggered(
            &ExitPolicy::Expiration,
            dec!(20),
            dec!(20),
            0,
            Positive::ZERO,
            pos(dec!(5000)),
            false,
        ));
        assert!(!policy_triggered(
            &ExitPolicy::Expiration,
            dec!(20),
            dec!(20),
            0,
            pos(dec!(5)),
            pos(dec!(5000)),
            false,
        ));
    }

    #[test]
    fn test_policy_reads_inner_is_false_for_all_v01_policies() {
        // The v0.1 exit decision reads snapshot scalars only: no policy reads
        // `inner`'s repriced state, so `exits` skips the per-step reprice. This
        // guards the FOLD-THE-FIX invariant (#19) — the instant this returns
        // `true` for a wired Greek-driven policy, the reprice re-enables.
        for policy in [
            ExitPolicy::Expiration,
            ExitPolicy::DeltaThreshold(dec!(0.1)),
            ExitPolicy::TimeSteps(0),
            ExitPolicy::ProfitPercent(dec!(0.5)),
            ExitPolicy::LossPercent(dec!(1.0)),
            ExitPolicy::And(vec![
                ExitPolicy::ProfitPercent(dec!(0.5)),
                ExitPolicy::Expiration,
            ]),
            ExitPolicy::Or(vec![
                ExitPolicy::TimeSteps(10),
                ExitPolicy::DeltaThreshold(dec!(0.2)),
            ]),
        ] {
            assert!(
                !policy_reads_inner(&policy),
                "no v0.1 policy reads inner: {policy:?}"
            );
        }
    }

    #[test]
    fn test_policy_triggered_delta_threshold_unsupported_never_triggers() {
        // DeltaThreshold is documented unsupported in v0.1: never triggers.
        assert!(!policy_triggered(
            &ExitPolicy::DeltaThreshold(dec!(0.1)),
            dec!(20),
            dec!(20),
            0,
            Positive::ZERO,
            pos(dec!(5000)),
            false,
        ));
    }

    #[test]
    fn test_policy_triggered_or_composes_profit_leg() {
        // Short leg: profit when premium falls. Or([ProfitPercent(0.5), Expiration]).
        let policy = ExitPolicy::Or(vec![
            ExitPolicy::ProfitPercent(dec!(0.5)),
            ExitPolicy::Expiration,
        ]);
        // initial 20, current 8 => 60% decay for a short: profit target hit.
        assert!(policy_triggered(
            &policy,
            dec!(20),
            dec!(8),
            0,
            pos(dec!(30)),
            pos(dec!(5000)),
            false,
        ));
        // current 18 => only 10% decay: neither leg triggers.
        assert!(!policy_triggered(
            &policy,
            dec!(20),
            dec!(18),
            0,
            pos(dec!(30)),
            pos(dec!(5000)),
            false,
        ));
    }

    #[test]
    fn test_exit_reason_maps_the_applied_policy_to_an_upstream_reason() {
        use optionstratlib::backtesting::ExitReason;

        // The trade log's ExitReason is taken from the applied ExitPolicy: a
        // profit target maps to TargetReached, a loss cap to StopLoss, and a
        // genuine expiration policy to Expiration. A step-count hold is NOT an
        // expiry — it records a truthful Other("time_steps").
        assert_eq!(
            adapter(ExitPolicy::ProfitPercent(dec!(50))).exit_reason(),
            ExitReason::TargetReached
        );
        assert_eq!(
            adapter(ExitPolicy::LossPercent(dec!(100))).exit_reason(),
            ExitReason::StopLoss
        );
        assert_eq!(
            adapter(ExitPolicy::TimeSteps(5)).exit_reason(),
            ExitReason::Other("time_steps".to_string())
        );
        assert_eq!(
            adapter(ExitPolicy::Expiration).exit_reason(),
            ExitReason::Expiration
        );
        // A composite the engine cannot disambiguate records a descriptive
        // Other rather than a fabricated specific reason.
        let composite = ExitPolicy::And(vec![ExitPolicy::Expiration]);
        assert!(matches!(
            adapter(composite).exit_reason(),
            ExitReason::Other(_)
        ));
    }

    /// #110: `close_all` reconciles against PENDING (resting GTC) closes — a
    /// leg fully covered by a resting close is skipped, a partially covered leg
    /// is flattened only for its uncovered remainder.
    #[test]
    fn test_close_all_skips_quantity_covered_by_pending_closes() {
        let legs = open_legs();
        let Some(first) = legs.first() else {
            panic!("fixture has legs");
        };
        // A resting GTC close fully covering leg 1 (qty 1).
        let pending = [crate::domain::PendingOrder {
            order_id: OrderId::new(77),
            intent: OrderIntent {
                contract: first.contract.clone(),
                action: PositionAction::Close(first.position_id),
                side: Side::Long, // flattens the short leg
                quantity: first.quantity,
                limit: Some(PriceCents::new(1)),
                tif: crate::domain::TimeInForce::Gtc,
                decision_mid: PriceCents::new(2_000),
            },
        }];
        let mut out: Vec<OrderCommand> = Vec::new();
        let result = super::close_all(&legs, &pending, &snapshot(9), &mut out);
        assert!(matches!(result, Ok(())));
        // Leg 1 is fully covered by the pending close ⇒ skipped; the other
        // three legs are flattened.
        assert_eq!(out.len(), 3, "the pending-covered leg is not re-closed");
        for cmd in &out {
            let OrderCommand::Submit(intent) = cmd else {
                panic!("close_all emits submits only");
            };
            let PositionAction::Close(pid) = intent.action else {
                panic!("close_all emits closes only");
            };
            assert_ne!(pid, first.position_id, "leg 1 must not be re-closed");
        }
    }

    // --- LegSetStrategy (the explicit leg set, #117) ------------------------

    /// The far expiry the leg-set fixtures put their wings in — 7 days beyond
    /// the near [`EXP_NS`], so the fixture set spans TWO expirations.
    const FAR_EXP_NS: i64 = EXP_NS + 7 * super::NANOS_PER_DAY as i64;

    fn leg(strike: u64, style: OptionStyle, side: Side, exp_ns: i64) -> LegSpec {
        LegSpec {
            side,
            style,
            strike: PriceCents::new(strike),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(exp_ns)),
            quantity: qty(1),
            implied_volatility: dec!(0.20),
        }
    }

    /// A four-leg set the named specs cannot express: the two body legs at the
    /// near expiry, the two wings a week later.
    fn leg_set_legs() -> Vec<LegSpec> {
        vec![
            leg(510_000, OptionStyle::Call, Side::Short, EXP_NS),
            leg(490_000, OptionStyle::Put, Side::Short, EXP_NS),
            leg(520_000, OptionStyle::Call, Side::Long, FAR_EXP_NS),
            leg(480_000, OptionStyle::Put, Side::Long, FAR_EXP_NS),
        ]
    }

    fn leg_set_spec_from(legs: Vec<LegSpec>) -> StrategySpec {
        StrategySpec::Legs(LegSetSpec {
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            legs,
            risk_free_rate: dec!(0.05),
            dividend_yield: Decimal::ZERO,
        })
    }

    fn leg_set_spec() -> StrategySpec {
        leg_set_spec_from(leg_set_legs())
    }

    /// A snapshot quoting every condor strike/style at BOTH expirations, so a
    /// leg is only resolvable by its full contract identity.
    fn multi_expiry_snapshot(step: u32) -> ChainSnapshot {
        let mut quotes = BTreeMap::new();
        for (strike, style, mid) in [
            (490_000u64, OptionStyle::Put, 1_800u64),
            (480_000, OptionStyle::Put, 700),
            (510_000, OptionStyle::Call, 2_000),
            (520_000, OptionStyle::Call, 800),
        ] {
            for exp_ns in [EXP_NS, FAR_EXP_NS] {
                let q = quote_at(strike, style, mid, exp_ns);
                quotes.insert(q.contract.clone(), q);
            }
        }
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("valid instrument spec");
        };
        ChainSnapshot {
            // Step-DEPENDENT on purpose: a fixture that pins `ts` for every step
            // cannot tell "resolved against this snapshot" apart from "resolved
            // against the anchor", and it lets a one-shot-entry test pass on a
            // timestamp coincidence rather than on the guard.
            ts: SimTime::new(TS_NS + i64::from(step) * super::NANOS_PER_DAY as i64),
            step: StepIndex::new(step),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes,
        }
    }

    #[test]
    fn test_leg_set_from_spec_opens_every_leg_at_its_own_expiration() {
        let mut rng = ChaCha8Rng::seed_from_u64(117);
        let snap = multi_expiry_snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let Ok(mut strategy) = LegSetStrategy::from_spec(&leg_set_spec(), ExitPolicy::Expiration)
        else {
            panic!("the leg set strategy must build from its spec");
        };
        let mut out = Vec::new();
        assert!(matches!(strategy.on_start(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), 4, "one open per spec leg");
        assert!(strategy.has_entered());

        // Every leg lands on its OWN expiration — the two body legs near, the
        // two wings far — never whichever expiry sorts first in the map.
        let mut opened: Vec<(i64, u64, Side)> = Vec::new();
        for cmd in &out {
            let OrderCommand::Submit(intent) = cmd else {
                panic!("entry emits Submit intents");
            };
            assert!(matches!(intent.action, PositionAction::Open));
            let Ok(exp_ns) = intent.contract.expiration_ns() else {
                panic!("the fixture legs carry resolved expiries");
            };
            opened.push((exp_ns, intent.contract.strike.value(), intent.side));
        }
        opened.sort_unstable_by_key(|(exp, strike, _)| (*exp, *strike));
        assert_eq!(
            opened,
            vec![
                (EXP_NS, 490_000, Side::Short),
                (EXP_NS, 510_000, Side::Short),
                (FAR_EXP_NS, 480_000, Side::Long),
                (FAR_EXP_NS, 520_000, Side::Long),
            ]
        );
    }

    #[test]
    fn test_leg_set_never_enters_outside_on_start() {
        // Entry is `on_start`-only, and that is what makes the anchor claim
        // structural: the snapshot a relative expiry resolves against is the one
        // the loop opens with, not whichever step happened to enter. A driver
        // calling `on_snapshot` must not be able to enter at step k — where
        // `Days(30)` would resolve to `ts_k + 30d` and, on a chain quoting
        // several tenors, silently open a contract the spec never named.
        let mut rng = ChaCha8Rng::seed_from_u64(118);
        let Ok(mut strategy) = LegSetStrategy::from_spec(&leg_set_spec(), ExitPolicy::Expiration)
        else {
            panic!("the leg set strategy must build from its spec");
        };
        let mut out = Vec::new();

        // Never entered, and a LATER snapshot (its own ts, one day on): still
        // nothing. Before the fix this opened the position against `ts_1`.
        let later = multi_expiry_snapshot(1);
        assert_ne!(
            later.ts.value(),
            multi_expiry_snapshot(0).ts.value(),
            "the fixture must carry a step-dependent ts or this proves nothing"
        );
        {
            let mut ctx = ChainContext {
                snapshot: &later,
                open: &[],
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(1),
            };
            assert!(matches!(strategy.on_snapshot(&mut ctx, &mut out), Ok(())));
        }
        assert!(
            out.is_empty(),
            "on_snapshot must never open a leg set, entered or not"
        );
        assert!(!strategy.has_entered(), "and must not mark it entered");

        // `on_start` is the sole entry, and it is still one-shot.
        let first = multi_expiry_snapshot(0);
        {
            let mut ctx = ChainContext {
                snapshot: &first,
                open: &[],
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(0),
            };
            assert!(matches!(strategy.on_start(&mut ctx, &mut out), Ok(())));
        }
        assert_eq!(out.len(), 4, "on_start opens every leg");
        assert!(strategy.has_entered());

        out.clear();
        let mut ctx = ChainContext {
            snapshot: &later,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(1),
        };
        assert!(matches!(strategy.on_start(&mut ctx, &mut out), Ok(())));
        assert!(out.is_empty(), "entry is guarded after the first snapshot");
    }

    #[test]
    fn test_leg_set_unquoted_leg_is_a_typed_execution_error() {
        let mut rng = ChaCha8Rng::seed_from_u64(119);
        // The single-expiry snapshot quotes nothing at 500000.
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let spec = leg_set_spec_from(vec![leg(500_000, OptionStyle::Call, Side::Short, EXP_NS)]);
        let Ok(mut strategy) = LegSetStrategy::from_spec(&spec, ExitPolicy::Expiration) else {
            panic!("the leg set strategy must build from its spec");
        };
        let mut out = Vec::new();
        assert!(matches!(
            strategy.on_start(&mut ctx, &mut out),
            Err(BacktestError::Execution(_))
        ));
    }

    #[test]
    fn test_leg_set_foreign_underlying_is_a_typed_execution_error() {
        let mut rng = ChaCha8Rng::seed_from_u64(120);
        let snap = snapshot(0);
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let Ok(other) = Underlying::new("NDX") else {
            panic!("NDX is a valid underlying");
        };
        let StrategySpec::Legs(mut inner) = leg_set_spec() else {
            panic!("the fixture is a leg set");
        };
        inner.underlying = other;
        let Ok(mut strategy) =
            LegSetStrategy::from_spec(&StrategySpec::Legs(inner), ExitPolicy::Expiration)
        else {
            panic!("the leg set strategy must build from its spec");
        };
        let mut out = Vec::new();
        assert!(matches!(
            strategy.on_start(&mut ctx, &mut out),
            Err(BacktestError::Execution(_))
        ));
    }

    #[test]
    fn test_leg_set_from_spec_rejects_an_empty_leg_set() {
        let spec = leg_set_spec_from(Vec::new());
        assert!(matches!(
            LegSetStrategy::from_spec(&spec, ExitPolicy::Expiration),
            Err(BacktestError::Strategy(_))
        ));
    }

    #[test]
    fn test_leg_set_from_spec_rejects_a_negative_implied_volatility() {
        let mut legs = leg_set_legs();
        let Some(first) = legs.first_mut() else {
            panic!("the fixture carries legs");
        };
        first.implied_volatility = dec!(-0.20);
        assert!(matches!(
            LegSetStrategy::from_spec(&leg_set_spec_from(legs), ExitPolicy::Expiration),
            Err(BacktestError::Strategy(_))
        ));
    }

    #[test]
    fn test_leg_set_from_spec_rejects_a_negative_dividend_yield() {
        let StrategySpec::Legs(mut inner) = leg_set_spec() else {
            panic!("the fixture is a leg set");
        };
        inner.dividend_yield = dec!(-0.01);
        assert!(matches!(
            LegSetStrategy::from_spec(&StrategySpec::Legs(inner), ExitPolicy::Expiration),
            Err(BacktestError::Strategy(_))
        ));
    }

    #[test]
    fn test_leg_set_from_spec_rejects_a_named_kind() {
        // Kind-checked, exactly as the two adapter constructors are: a named
        // spec points the caller at OptStratAdapter rather than building wrong.
        assert!(matches!(
            LegSetStrategy::from_spec(&iron_condor_spec(), ExitPolicy::Expiration),
            Err(BacktestError::Strategy(_))
        ));
        assert!(matches!(
            LegSetStrategy::from_spec(&short_strangle_spec(), ExitPolicy::Expiration),
            Err(BacktestError::Strategy(_))
        ));
    }

    #[test]
    fn test_adapter_from_spec_rejects_a_leg_set_kind() {
        // The mirror of the above: neither adapter can build a leg set (there
        // is no upstream strategy object for one).
        assert!(matches!(
            OptStratAdapter::<IronCondor>::from_spec(&leg_set_spec(), ExitPolicy::Expiration),
            Err(BacktestError::Strategy(_))
        ));
        assert!(matches!(
            OptStratAdapter::<ShortStrangle>::from_spec(&leg_set_spec(), ExitPolicy::Expiration),
            Err(BacktestError::Strategy(_))
        ));
    }

    #[test]
    fn test_leg_set_exits_emits_closes_when_the_policy_triggers() {
        let mut rng = ChaCha8Rng::seed_from_u64(121);
        let snap = snapshot(7);
        let legs = open_legs();
        let ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(7),
        };
        // The SAME shared exit decision the adapter runs: a step-count hold that
        // has elapsed closes every open leg.
        let Ok(mut strategy) = LegSetStrategy::from_spec(&leg_set_spec(), ExitPolicy::TimeSteps(1))
        else {
            panic!("the leg set strategy must build from its spec");
        };
        let mut out = Vec::new();
        assert!(matches!(strategy.exits(&ctx, &mut out), Ok(())));
        assert_eq!(out.len(), legs.len(), "every open leg is closed");
        for cmd in &out {
            let OrderCommand::Submit(intent) = cmd else {
                panic!("exits emits Submit intents");
            };
            assert!(matches!(intent.action, PositionAction::Close(_)));
        }
    }

    #[test]
    fn test_leg_set_on_end_flattens_the_open_inventory() {
        let mut rng = ChaCha8Rng::seed_from_u64(122);
        let snap = snapshot(9);
        let legs = open_legs();
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(9),
        };
        let Ok(mut strategy) =
            LegSetStrategy::from_spec(&leg_set_spec(), ExitPolicy::TimeSteps(1_000_000))
        else {
            panic!("the leg set strategy must build from its spec");
        };
        let mut out = Vec::new();
        assert!(matches!(strategy.on_end(&mut ctx, &mut out), Ok(())));
        assert_eq!(out.len(), legs.len(), "on_end closes every open leg");
    }

    #[test]
    fn test_leg_set_wrong_expiration_is_an_error_not_a_silent_fill() {
        // A leg set exists to name a per-leg expiry, so a leg at FAR_EXP_NS
        // against a chain that quotes only EXP_NS at that strike/style must be a
        // typed error. The adapter's single-candidate fallback would have filled
        // it silently on the wrong week, and the bundle would then record a
        // contract the spec never asked for.
        let mut rng = ChaCha8Rng::seed_from_u64(123);
        let snap = snapshot(0); // single-expiry chain, all legs at EXP_NS
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[],
            marks: &NO_MARKS,
            rng: &mut rng,
            step: StepIndex::new(0),
        };
        let spec = leg_set_spec_from(vec![leg(
            510_000,
            OptionStyle::Call,
            Side::Short,
            FAR_EXP_NS,
        )]);
        let Ok(mut strategy) = LegSetStrategy::from_spec(&spec, ExitPolicy::Expiration) else {
            panic!("the leg set strategy must build from its spec");
        };
        let mut out = Vec::new();
        assert!(matches!(
            strategy.on_start(&mut ctx, &mut out),
            Err(BacktestError::Execution(_))
        ));
        assert!(out.is_empty(), "no leg opens on the wrong expiration");
    }

    #[test]
    fn test_leg_set_relative_expiry_resolves_to_the_same_contract_as_the_absolute_one() {
        // The property the variant is for: `Days(n)` means n days past the tape
        // anchor. The snapshot's ts IS `ts_0` at entry (one-shot at step 0), and
        // EXP_NS is TS_NS + 30 days, so `Days(30)` must open exactly what the
        // resolved spec opens.
        let opened = |expiration: ExpirationDate, seed: u64| -> Vec<i64> {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let snap = multi_expiry_snapshot(0);
            let mut ctx = ChainContext {
                snapshot: &snap,
                open: &[],
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(0),
            };
            let mut spec_leg = leg(510_000, OptionStyle::Call, Side::Short, EXP_NS);
            spec_leg.expiration = expiration;
            let Ok(mut strategy) = LegSetStrategy::from_spec(
                &leg_set_spec_from(vec![spec_leg]),
                ExitPolicy::Expiration,
            ) else {
                panic!("the leg set strategy must build from its spec");
            };
            let mut out = Vec::new();
            assert!(matches!(strategy.on_start(&mut ctx, &mut out), Ok(())));
            out.iter()
                .map(|cmd| {
                    let OrderCommand::Submit(intent) = cmd else {
                        panic!("entry emits Submit intents");
                    };
                    let Ok(ns) = intent.contract.expiration_ns() else {
                        panic!("the opened contract carries a resolved expiry");
                    };
                    ns
                })
                .collect()
        };

        let absolute = opened(
            ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXP_NS)),
            130,
        );
        let relative = opened(ExpirationDate::Days(pos(dec!(30))), 131);
        assert_eq!(absolute, vec![EXP_NS], "the absolute spec opens EXP_NS");
        assert_eq!(
            relative, absolute,
            "a relative Days(30) leg opens the same contract as the resolved spec"
        );
    }

    #[test]
    fn test_leg_set_relative_expiry_reads_n_rather_than_ignoring_it() {
        // The property the removed fallback violated: two different tenors must
        // resolve to two DIFFERENT contracts, not to whichever quote happens to
        // share the strike and style. The fixture chain quotes EXP_NS (+30d) and
        // FAR_EXP_NS (+37d), so Days(30) matches and Days(37) matches a
        // different contract — while Days(90) matches nothing and is typed.
        let attempt = |days: rust_decimal::Decimal, seed: u64| {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let snap = multi_expiry_snapshot(0);
            let mut ctx = ChainContext {
                snapshot: &snap,
                open: &[],
                pending: &[],
                marks: &NO_MARKS,
                rng: &mut rng,
                step: StepIndex::new(0),
            };
            let mut spec_leg = leg(510_000, OptionStyle::Call, Side::Short, EXP_NS);
            spec_leg.expiration = ExpirationDate::Days(pos(days));
            let Ok(mut strategy) = LegSetStrategy::from_spec(
                &leg_set_spec_from(vec![spec_leg]),
                ExitPolicy::Expiration,
            ) else {
                panic!("the leg set strategy must build from its spec");
            };
            let mut out = Vec::new();
            let result = strategy.on_start(&mut ctx, &mut out);
            result.map(|()| {
                out.iter()
                    .map(|cmd| {
                        let OrderCommand::Submit(intent) = cmd else {
                            panic!("entry emits Submit intents");
                        };
                        intent.contract.expiration_ns().unwrap_or_default()
                    })
                    .collect::<Vec<i64>>()
            })
        };

        let near = attempt(dec!(30), 132);
        let far = attempt(dec!(37), 133);
        assert_eq!(near.as_deref().ok(), Some(&[EXP_NS][..]));
        assert_eq!(far.as_deref().ok(), Some(&[FAR_EXP_NS][..]));
        assert_ne!(
            near.ok(),
            far.ok(),
            "a different tenor is a different contract"
        );

        // An unquoted tenor is a typed error, never a silent fill on a neighbour.
        assert!(matches!(
            attempt(dec!(90), 134),
            Err(BacktestError::Execution(_))
        ));
    }

    #[test]
    fn test_leg_set_runs_the_canonical_order_not_the_input_order() {
        // The `run_id` and the manifest record the canonical spec, so the ORDER
        // THE ENGINE RUNS must be that same order: the engine mints order and
        // position ids in submission order, so running the caller's order would
        // put a permuted fills/positions table under an identical run_id.
        let mut rng = ChaCha8Rng::seed_from_u64(125);
        let snap = multi_expiry_snapshot(0);
        let emitted = |spec: &StrategySpec, rng: &mut ChaCha8Rng| -> Vec<(i64, u64)> {
            let mut ctx = ChainContext {
                snapshot: &snap,
                open: &[],
                pending: &[],
                marks: &NO_MARKS,
                rng,
                step: StepIndex::new(0),
            };
            let Ok(mut strategy) = LegSetStrategy::from_spec(spec, ExitPolicy::Expiration) else {
                panic!("the leg set strategy must build from its spec");
            };
            let mut out = Vec::new();
            assert!(matches!(strategy.on_start(&mut ctx, &mut out), Ok(())));
            out.iter()
                .map(|cmd| {
                    let OrderCommand::Submit(intent) = cmd else {
                        panic!("entry emits Submit intents");
                    };
                    let Ok(exp) = intent.contract.expiration_ns() else {
                        panic!("the fixture legs carry resolved expiries");
                    };
                    (exp, intent.contract.strike.value())
                })
                .collect()
        };

        let mut reversed = leg_set_legs();
        reversed.reverse();
        let canonical = emitted(&leg_set_spec(), &mut rng);
        let permuted = emitted(&leg_set_spec_from(reversed), &mut rng);
        assert_eq!(
            canonical, permuted,
            "both leg orders must submit in the same (canonical) order"
        );
        assert_eq!(
            canonical,
            vec![
                (EXP_NS, 490_000),
                (EXP_NS, 510_000),
                (FAR_EXP_NS, 480_000),
                (FAR_EXP_NS, 520_000),
            ],
            "submission follows the canonical (expiration, strike, ...) order"
        );
    }
}
