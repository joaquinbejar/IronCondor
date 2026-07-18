//! Execution-record types: order intents, commands, fills, and the
//! sign-convention helpers.
//!
//! The strategy emits [`OrderCommand`]s; each `Submit` carries an
//! [`OrderIntent`] that an execution model turns into zero or more
//! [`Fill`]s. Both fill models emit the **same** `Fill` shape so analytics
//! is mode-agnostic. Lifecycle ids ([`PositionId`], [`OrderId`],
//! [`TradeId`]) are minted by the engine from seeded counters (issue #14) —
//! only the newtypes live here.

use optionstratlib::Side;
use serde::{Deserialize, Serialize};

use crate::domain::contract::ContractKey;
use crate::domain::money::{Cents, PriceCents, Quantity};
use crate::domain::time::{SimTime, StepIndex};
use crate::error::BacktestError;

/// Which fill model executes a run's order intents.
///
/// Both modes emit the identical [`Fill`] shape so analytics is
/// mode-agnostic; the mode is semantic configuration and (from v0.3) hashes
/// into the `run_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum ExecutionMode {
    /// Mid/spread reference fills with configured slippage and fees — fast
    /// iteration, no order book.
    Naive = 0,
    /// Orders routed through the `option-chain-orderbook` matching engine —
    /// queue position, per-strike liquidity, and market impact (feature
    /// `orderbook`).
    Realistic = 1,
}

/// A leg identity minted by the engine from a seeded counter — identical
/// across reproducible runs. Serialises as its bare inner scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PositionId(u64);

impl PositionId {
    /// Wrap an engine-minted position id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The inner id.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// An order identity minted by the engine from a seeded counter.
/// Serialises as its bare inner scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderId(u64);

impl OrderId {
    /// Wrap an engine-minted order id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The inner id.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// A multi-leg trade identity minted by the engine from a seeded counter —
/// the `Open` intents a strategy emits together in one step share one.
/// Serialises as its bare inner scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TradeId(u64);

impl TradeId {
    /// Wrap an engine-minted trade id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The inner id.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Whether an intent opens fresh exposure or closes an existing leg.
///
/// `Side` alone cannot say this — a *buy* opens a long leg but also
/// *closes* a short leg — so the action is explicit, never inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionAction {
    /// Create a new leg; the engine mints a `position_id`.
    Open,
    /// Close (or partially close) this existing leg.
    Close(PositionId),
}

/// How long a strategy limit rests. Marketable intents are always IOC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum TimeInForce {
    /// Immediate-or-cancel.
    Ioc = 0,
    /// Good-till-cancelled — a resting strategy limit.
    Gtc = 1,
}

/// One order the strategy wants executed.
///
/// Deserialisation is routed through [`OrderIntent::try_from`] so the
/// marketable-order invariant (`limit == None ⇒ tif == Ioc`) is **enforced**,
/// not merely documented: an intent decoded from JSON with `limit = null` and
/// `tif = "gtc"` is a typed [`BacktestError::Execution`], never a resting market
/// order the fill models cannot represent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "OrderIntentWire")]
pub struct OrderIntent {
    /// The contract to trade.
    pub contract: ContractKey,
    /// `Open` or `Close(position_id)` — never inferred from `side`.
    pub action: PositionAction,
    /// Long or short — reused from `optionstratlib`.
    pub side: Side,
    /// Contracts to trade (`> 0`).
    pub quantity: Quantity,
    /// Limit price; `None` ⇒ marketable (always IOC).
    pub limit: Option<PriceCents>,
    /// `Ioc` (default for marketable) or `Gtc` for a resting limit.
    pub tif: TimeInForce,
    /// The mid at decision time — the slippage reference.
    pub decision_mid: PriceCents,
}

impl OrderIntent {
    /// Build a validated order intent, **enforcing** the marketable-order
    /// invariant so a resting market order can never be constructed.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when `limit` is `None` (a
    /// marketable intent) and `tif` is [`TimeInForce::Gtc`] — a market order
    /// is always immediate-or-cancel; it cannot rest in the book.
    pub fn new(
        contract: ContractKey,
        action: PositionAction,
        side: Side,
        quantity: Quantity,
        limit: Option<PriceCents>,
        tif: TimeInForce,
        decision_mid: PriceCents,
    ) -> Result<Self, BacktestError> {
        let intent = Self {
            contract,
            action,
            side,
            quantity,
            limit,
            tif,
            decision_mid,
        };
        intent.validate()?;
        Ok(intent)
    }

    /// Check the marketable-order invariant: a marketable intent
    /// (`limit == None`) must be IOC.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] for a resting market order
    /// (`limit == None && tif == Gtc`).
    pub fn validate(&self) -> Result<(), BacktestError> {
        if self.limit.is_none() && matches!(self.tif, TimeInForce::Gtc) {
            return Err(BacktestError::Execution(
                "marketable order intent (limit = None) must be IOC, not GTC; \
                 a market order cannot rest in the book"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// The wire mirror of [`OrderIntent`], used only as the `try_from`
/// deserialisation target so every decoded intent is validated through
/// [`OrderIntent::new`]'s marketable-order invariant.
#[derive(Deserialize)]
struct OrderIntentWire {
    contract: ContractKey,
    action: PositionAction,
    side: Side,
    quantity: Quantity,
    limit: Option<PriceCents>,
    tif: TimeInForce,
    decision_mid: PriceCents,
}

impl TryFrom<OrderIntentWire> for OrderIntent {
    type Error = BacktestError;

    fn try_from(wire: OrderIntentWire) -> Result<Self, Self::Error> {
        Self::new(
            wire.contract,
            wire.action,
            wire.side,
            wire.quantity,
            wire.limit,
            wire.tif,
            wire.decision_mid,
        )
    }
}

/// The strategy's per-step output — one deterministic ordered channel for
/// new intents and control of its own resting orders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderCommand {
    /// Submit a new order.
    Submit(OrderIntent),
    /// Cancel a resting strategy order by its stable handle.
    Cancel(OrderId),
    /// Cancel-then-submit as one unit.
    Replace {
        /// The resting order to cancel.
        order_id: OrderId,
        /// The intent replacing it.
        replacement: OrderIntent,
    },
}

/// A currently-open strategy leg as the engine tracks it — the read model a
/// [`crate::engine::Strategy`] sees through [`crate::engine::ChainContext::open`].
///
/// The engine mints the stable [`PositionId`] when the opening intent fills
/// and carries the per-contract `entry_premium` (integer cents) forward: that
/// premium is the `initial_premium` an [`crate::engine::OptStratAdapter`]
/// reconciles a price/percent [`optionstratlib::simulation::ExitPolicy`]
/// against. The upstream `optionstratlib` strategy object is a static leg
/// *definition*; this is the engine's authoritative *inventory* of what is
/// actually open, so exit evaluation drives off it, not off the strategy's own
/// `get_positions()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenPosition {
    /// The engine-minted stable identity — the handle a `Close` targets.
    pub position_id: PositionId,
    /// The open leg's contract identity (the join key into a snapshot quote).
    pub contract: ContractKey,
    /// The direction the leg was opened in (`Long` bought, `Short` sold);
    /// closing it trades the opposite side.
    pub side: Side,
    /// The open contract count (`> 0`).
    pub quantity: Quantity,
    /// Per-contract premium paid/received at entry, integer cents — the
    /// `initial_premium` reference for percent/price exit policies.
    pub entry_premium: PriceCents,
}

/// A resting strategy order the engine still holds — the read model a strategy
/// sees through [`crate::engine::ChainContext::pending`], addressable for
/// cancel/replace by its stable [`OrderId`] handle.
///
/// Only the strategy's **own** resting orders appear here; seeded-maker
/// liquidity is never visible or addressable, so membership in `pending` is
/// exactly the ownership test for a `Cancel`/`Replace`
/// ([docs/02 §4](../../../docs/02-engine-architecture.md#4-the-strategy-trait)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingOrder {
    /// The stable handle the strategy cancels or replaces this order by.
    pub order_id: OrderId,
    /// The resting intent still working in the book.
    pub intent: OrderIntent,
}

/// One execution record — the identical shape in both fill modes, so
/// analytics cannot tell which `ExecutionModel` produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    /// The snapshot timestamp the fill executed against.
    pub ts: SimTime,
    /// The step the fill executed in.
    pub step: StepIndex,
    /// The filled contract.
    pub contract: ContractKey,
    /// Long or short — reused from `optionstratlib`.
    pub side: Side,
    /// Filled quantity — may be `<` the intent in realistic mode.
    pub quantity: Quantity,
    /// Executed premium per contract, integer cents.
    pub price: PriceCents,
    /// Fees for this fill, integer cents, always `≥ 0`.
    pub fees: Cents,
    /// Signed slippage vs `decision_mid`; positive = adverse (see
    /// [`sign_convention`]).
    pub slippage: Cents,
    /// Which fill model produced this fill.
    pub mode: ExecutionMode,
}

/// The sign-convention truth table — one place, referenced everywhere.
///
/// | Quantity         | Rule                                                        |
/// |------------------|-------------------------------------------------------------|
/// | cash flow        | positive = credit to the account                            |
/// | `fees`           | always `≥ 0` (a magnitude), subtracted in every P&L identity |
/// | `slippage`       | signed; positive = adverse (fill worse than `decision_mid`) |
/// | `spread_capture` | signed; positive = favourable; `= −Σ slippage` — the single sign flip |
pub mod sign_convention {
    use optionstratlib::Side;

    use crate::domain::money::{Cents, PriceCents, Quantity};
    use crate::error::BacktestError;

    /// `+1` for a buy/long, `−1` for a sell/short.
    #[must_use]
    pub const fn side_sign(side: Side) -> i64 {
        match side {
            Side::Long => 1,
            Side::Short => -1,
        }
    }

    /// Signed slippage in integer cents:
    /// `(price − decision_mid) × quantity × side_sign` — positive exactly
    /// when the fill is worse than `decision_mid` for that side.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] when the product
    /// exceeds the `i64` range.
    #[must_use = "the computed slippage must be used"]
    pub fn slippage_cents(
        price: PriceCents,
        decision_mid: PriceCents,
        quantity: Quantity,
        side: Side,
    ) -> Result<Cents, BacktestError> {
        let diff = i128::from(price.value()) - i128::from(decision_mid.value());
        let product = diff
            .checked_mul(i128::from(quantity.value()))
            .and_then(|p| p.checked_mul(i128::from(side_sign(side))))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let cents = i64::try_from(product).map_err(|_| BacktestError::ArithmeticOverflow)?;
        Ok(Cents::new(cents))
    }

    /// The step's `spread_capture`: `−Σ slippage` over its fills — the
    /// **single** place slippage flips sign, so P&L is never inverted
    /// twice.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] when the sum or the
    /// final negation exceeds the `i64` range.
    #[must_use = "the computed spread capture must be used"]
    pub fn spread_capture_cents<I>(slippages: I) -> Result<Cents, BacktestError>
    where
        I: IntoIterator<Item = Cents>,
    {
        let mut total = Cents::new(0);
        for slippage in slippages {
            total = total.checked_add(slippage)?;
        }
        let negated = total
            .value()
            .checked_neg()
            .ok_or(BacktestError::ArithmeticOverflow)?;
        Ok(Cents::new(negated))
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};

    use super::sign_convention::{side_sign, slippage_cents, spread_capture_cents};
    use super::{OrderIntent, PositionAction, TimeInForce};
    use crate::domain::contract::{ContractKey, Underlying};
    use crate::domain::money::{Cents, PriceCents, Quantity};
    use crate::error::BacktestError;

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    fn contract_key() -> ContractKey {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        ContractKey {
            underlying,
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                1_750_291_200_000_000_000,
            )),
            strike: PriceCents::new(510_000),
            style: OptionStyle::Call,
        }
    }

    #[test]
    fn test_order_intent_new_enforces_marketable_ioc_invariant() {
        // marketable (limit None) + Gtc is a resting market order — rejected.
        let resting_market = OrderIntent::new(
            contract_key(),
            PositionAction::Open,
            Side::Short,
            qty(1),
            None,
            TimeInForce::Gtc,
            PriceCents::new(150),
        );
        assert!(matches!(resting_market, Err(BacktestError::Execution(_))));

        // marketable (limit None) + Ioc is accepted.
        assert!(
            OrderIntent::new(
                contract_key(),
                PositionAction::Open,
                Side::Short,
                qty(1),
                None,
                TimeInForce::Ioc,
                PriceCents::new(150),
            )
            .is_ok()
        );

        // a resting limit (limit Some) + Gtc is accepted.
        assert!(
            OrderIntent::new(
                contract_key(),
                PositionAction::Open,
                Side::Short,
                qty(1),
                Some(PriceCents::new(148)),
                TimeInForce::Gtc,
                PriceCents::new(150),
            )
            .is_ok()
        );
    }

    #[test]
    fn test_order_intent_deserialize_rejects_resting_market_order() {
        let Ok(valid) = OrderIntent::new(
            contract_key(),
            PositionAction::Open,
            Side::Short,
            qty(1),
            None,
            TimeInForce::Ioc,
            PriceCents::new(150),
        ) else {
            panic!("a marketable IOC intent must build");
        };
        let json = serde_json::to_string(&valid).unwrap_or_default();
        // A marketable IOC intent round-trips.
        let back: Result<OrderIntent, _> = serde_json::from_str(&json);
        assert!(back.is_ok(), "marketable IOC intent must round-trip");

        // Flip tif to gtc while limit stays null → a resting market order,
        // rejected on deserialize by the `try_from` invariant.
        let mut value: serde_json::Value = match serde_json::from_str(&json) {
            Ok(value) => value,
            Err(e) => panic!("intent json must parse: {e}"),
        };
        let Some(obj) = value.as_object_mut() else {
            panic!("intent json must be an object");
        };
        obj.insert("limit".to_string(), serde_json::Value::Null);
        obj.insert(
            "tif".to_string(),
            serde_json::Value::String("gtc".to_string()),
        );
        let bad: Result<OrderIntent, _> = serde_json::from_value(value);
        assert!(
            bad.is_err(),
            "a resting (gtc) market order must fail to deserialize"
        );
    }

    #[test]
    fn test_slippage_positive_when_buy_worse_than_mid() {
        // Buying at 152 with mid 150 is adverse for a long: +2 per contract.
        let slippage = slippage_cents(
            PriceCents::new(152),
            PriceCents::new(150),
            qty(1),
            Side::Long,
        );
        assert!(matches!(slippage, Ok(s) if s.value() == 2));
        // Buying below mid is favourable: negative.
        let slippage = slippage_cents(
            PriceCents::new(149),
            PriceCents::new(150),
            qty(1),
            Side::Long,
        );
        assert!(matches!(slippage, Ok(s) if s.value() == -1));
    }

    #[test]
    fn test_slippage_positive_when_sell_worse_than_mid() {
        // Selling at 148 with mid 150 is adverse for a short: +2 per
        // contract (the docs/01 §7.1 worked example).
        let slippage = slippage_cents(
            PriceCents::new(148),
            PriceCents::new(150),
            qty(1),
            Side::Short,
        );
        assert!(matches!(slippage, Ok(s) if s.value() == 2));
        // Selling above mid is favourable: negative.
        let slippage = slippage_cents(
            PriceCents::new(153),
            PriceCents::new(150),
            qty(1),
            Side::Short,
        );
        assert!(matches!(slippage, Ok(s) if s.value() == -3));
    }

    #[test]
    fn test_slippage_scales_with_quantity() {
        let slippage = slippage_cents(
            PriceCents::new(152),
            PriceCents::new(150),
            qty(5),
            Side::Long,
        );
        assert!(matches!(slippage, Ok(s) if s.value() == 10));
    }

    #[test]
    fn test_execution_mode_serde_snake_case_round_trip() {
        let naive = serde_json::to_string(&super::ExecutionMode::Naive);
        assert!(matches!(naive.as_deref(), Ok("\"naive\"")));
        let back: Result<super::ExecutionMode, _> = serde_json::from_str("\"realistic\"");
        assert!(matches!(back, Ok(super::ExecutionMode::Realistic)));
    }

    #[test]
    fn test_side_sign_truth_table() {
        assert_eq!(side_sign(Side::Long), 1);
        assert_eq!(side_sign(Side::Short), -1);
    }

    #[test]
    fn test_spread_capture_is_negated_slippage_sum() {
        let capture = spread_capture_cents([Cents::new(2), Cents::new(-1), Cents::new(4)]);
        assert!(matches!(capture, Ok(c) if c.value() == -5));
        let capture = spread_capture_cents([]);
        assert!(matches!(capture, Ok(c) if c.value() == 0));
    }
}
