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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    use optionstratlib::Side;

    use super::sign_convention::{side_sign, slippage_cents, spread_capture_cents};
    use crate::domain::money::{Cents, PriceCents, Quantity};

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
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
