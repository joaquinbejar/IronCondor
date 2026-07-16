//! The strategy seam over `optionstratlib`.
//!
//! This module owns the [`PositionableStrategy`] trait bound — the family of
//! upstream multi-leg strategies (`IronCondor`, `ShortStrangle`, spreads, …) —
//! plus the engine-facing [`Strategy`] trait the replay loop drives, the
//! [`ChainContext`] a strategy reads, and the [`OptStratAdapter`] that wraps an
//! upstream strategy behind [`Strategy`]
//! ([docs/02 §4/§4.1](../../../docs/02-engine-architecture.md#4-the-strategy-trait)).
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

use optionstratlib::Side;
use optionstratlib::backtesting::ExitReason;
use optionstratlib::prelude::Positive;
use optionstratlib::simulation::{ExitPolicy, check_exit_policy};
use optionstratlib::strategies::base::{Optimizable, Positionable};
use optionstratlib::strategies::{IronCondor, ShortStrangle, Strategies};

use crate::data::convert::{positive_from_price, snapshot_to_option_chain};
use crate::domain::{
    ChainSnapshot, ContractKey, IronCondorSpec, OpenPosition, OrderCommand, OrderId, OrderIntent,
    PendingOrder, PositionAction, PriceCents, Quantity, ShortStrangleSpec, SimTime, StepIndex,
    StrategySpec, TimeInForce,
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
    /// to `snapshot` quotes by strike and style. Guarded by `entered`, so it is
    /// a no-op after the first successful entry.
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
            let quote = snapshot
                .quotes
                .values()
                .find(|q| q.contract.strike == strike && q.contract.style == style)
                .ok_or_else(|| {
                    BacktestError::Execution(format!(
                        "no snapshot quote for leg strike {} style {style:?}",
                        strike.value()
                    ))
                })?;
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

    /// Append a `Close` command for every open leg (closing commands only).
    fn close_all(open: &[OpenPosition], snapshot: &ChainSnapshot, out: &mut Vec<OrderCommand>) {
        for leg in open {
            out.push(close_command(snapshot, leg));
        }
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
        // `underlying` is the ONLY repricing output the v0.1 exit DECISION
        // reads: `check_exit_policy` (docs/specs/optionstratlib.md §9) consumes
        // snapshot-derived scalars only and never reads the wrapped `inner`'s
        // repriced Greeks. Source it directly from the snapshot scalar through
        // the SAME derivation `convert.rs` uses for `chain.underlying_price`
        // (`positive_from_price`), so the value is byte-identical to the old
        // `snapshot_to_option_chain(ctx.snapshot)?.underlying_price` — without
        // rebuilding the whole `OptionChain` each step (~44 alloc events/step,
        // #18/#19) or reaching the upstream `Utc::now()` expiry-check
        // wall-clock. A conversion failure is a genuine data error and
        // propagates.
        let underlying = positive_from_price("underlying", ctx.snapshot.underlying_price)?;

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

        let step = ctx.step.value() as usize;
        let snapshot = ctx.snapshot;
        for leg in ctx.open {
            let days_left = days_to_expiry(&leg.contract, snapshot.ts)?;
            let is_long = matches!(leg.side, Side::Long);
            let initial = leg.entry_premium.to_decimal_dollars();
            let current = current_premium(snapshot, leg);
            if policy_triggered(
                &self.exit, initial, current, step, days_left, underlying, is_long,
            ) {
                out.push(close_command(snapshot, leg));
            }
        }
        Ok(())
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
        Self::close_all(ctx.open, ctx.snapshot, out);
        Ok(())
    }

    fn exit_reason(&self) -> ExitReason {
        exit_policy_to_reason(&self.exit)
    }
}

/// Map an applied [`ExitPolicy`] to the [`ExitReason`] recorded on a
/// policy-triggered leg close ([docs/05 §4](../../../docs/05-analytics-and-reporting.md#4-summary-metrics)).
///
/// Profit-side policies map to [`ExitReason::TargetReached`], loss-side to
/// [`ExitReason::StopLoss`], expiration/time to [`ExitReason::Expiration`]; an
/// ambiguous or composite policy (whose specific triggering leaf the engine
/// cannot disambiguate) maps to a descriptive [`ExitReason::Other`] rather than
/// a fabricated specific reason. The mapping is a **pure** function of the
/// policy — no wall clock, no RNG — so it is deterministic.
#[must_use]
fn exit_policy_to_reason(policy: &ExitPolicy) -> ExitReason {
    match policy {
        ExitPolicy::ProfitPercent(_) => ExitReason::TargetReached,
        ExitPolicy::LossPercent(_) => ExitReason::StopLoss,
        ExitPolicy::Expiration | ExitPolicy::TimeSteps(_) | ExitPolicy::DaysToExpiration(_) => {
            ExitReason::Expiration
        }
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

/// The current mark of a leg in dollars: the snapshot mid of its contract, or
/// the leg's entry premium when the contract is absent this step (`stale_mark`,
/// [docs/01 §6](../../../docs/01-domain-model.md#6-market-data)).
#[must_use]
fn current_premium(snapshot: &ChainSnapshot, leg: &OpenPosition) -> Decimal {
    snapshot.quotes.get(&leg.contract).map_or_else(
        || leg.entry_premium.to_decimal_dollars(),
        |q| q.mid.to_decimal_dollars(),
    )
}

/// Build the closing `Submit` for one open leg: the opposite trade side
/// flattens it, priced against the snapshot mid (falling back to entry premium
/// if the contract is stale).
#[must_use]
fn close_command(snapshot: &ChainSnapshot, leg: &OpenPosition) -> OrderCommand {
    let decision_mid = snapshot
        .quotes
        .get(&leg.contract)
        .map_or(leg.entry_premium, |q| q.mid);
    OrderCommand::Submit(OrderIntent {
        contract: leg.contract.clone(),
        action: PositionAction::Close(leg.position_id),
        side: flip_side(leg.side),
        quantity: leg.quantity,
        limit: None,
        tif: TimeInForce::Ioc,
        decision_mid,
    })
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
        ChainContext, OptStratAdapter, PositionableStrategy, Strategy, policy_reads_inner,
        policy_triggered,
    };
    use crate::domain::{
        ChainSnapshot, ContractKey, InstrumentSpec, IronCondorSpec, OpenPosition, OrderCommand,
        OrderId, OrderIntent, PendingOrder, PositionAction, PositionId, PriceCents, Quantity,
        QuoteView, ShortStrangleSpec, SimTime, StepIndex, StrategySpec, Underlying,
    };
    use crate::error::BacktestError;

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
    fn test_iron_condor_from_spec_exits_emits_closes_when_policy_triggers() {
        let mut rng = ChaCha8Rng::seed_from_u64(21);
        let snap = snapshot(7);
        let legs = open_legs();
        let ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
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
            rng: &mut rng,
            step: StepIndex::new(3),
        };
        let mut adapter = adapter(ExitPolicy::TimeSteps(0));
        let mut out = Vec::new();
        assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        assert!(out.is_empty(), "no open legs: nothing to reprice or close");
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
                rng: &mut rng,
                step: StepIndex::new(9),
            };
            assert!(matches!(adapter.exits(&ctx, &mut out), Ok(())));
        }
        let mut ctx = ChainContext {
            snapshot: &snap,
            open: &legs,
            pending: &[],
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

    // --- pending-order control ---------------------------------------------

    #[test]
    fn test_request_cancel_foreign_order_is_execution_error() {
        let mut rng = ChaCha8Rng::seed_from_u64(9);
        let snap = snapshot(0);
        let ctx = ChainContext {
            snapshot: &snap,
            open: &[],
            pending: &[], // no resting strategy orders
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
        // time/expiration policy to Expiration.
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
            ExitReason::Expiration
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
}
