//! Canonical domain types.
//!
//! Integer-cents money newtypes ([`Cents`], [`PriceCents`], [`Quantity`],
//! [`Ticks`]), simulated time ([`SimTime`], [`StepIndex`]), contract
//! identity ([`Underlying`], [`ContractKey`] and the canonical
//! `contract_id`), market data ([`InstrumentSpec`], [`QuoteView`],
//! [`ChainSnapshot`]), execution records ([`OrderIntent`], [`OrderCommand`],
//! [`Fill`], [`OpenPosition`], [`PendingOrder`], the lifecycle ids, and the
//! [`execution::sign_convention`] truth-table helpers), the run's
//! [`StrategySpec`] (strategy kind + parameters, including the explicit
//! [`LegSetSpec`] leg set), and the four result-bundle row
//! records — [`EquityPoint`] (the replay loop's ledger emits), [`FillRow`] (one
//! executed fill), [`PositionRow`] (a leg's per-step state), and
//! [`GreeksAttributionRow`] (the analytics layer's per-step P&L decomposition).

pub mod contract;
pub mod execution;
pub mod market;
pub mod money;
pub mod result;
pub mod strategy_spec;
pub mod time;

pub use contract::{ContractKey, Underlying};
pub use execution::{
    ExecutionMode, Fill, OpenPosition, OrderCommand, OrderId, OrderIntent, PendingOrder,
    PositionAction, PositionId, TimeInForce, TradeId,
};
pub use market::{ChainSnapshot, InstrumentSpec, QuoteView};
pub use money::{Cents, PriceCents, Quantity, Ticks};
pub use result::{EquityPoint, FillRow, GreeksAttributionRow, PositionRow};
pub use strategy_spec::{IronCondorSpec, LegSetSpec, LegSpec, ShortStrangleSpec, StrategySpec};
pub use time::{SimTime, StepIndex};
