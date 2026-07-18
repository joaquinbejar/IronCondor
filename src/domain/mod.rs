//! Canonical domain types.
//!
//! Integer-cents money newtypes ([`Cents`], [`PriceCents`], [`Quantity`],
//! [`Ticks`]), simulated time ([`SimTime`], [`StepIndex`]), contract
//! identity ([`Underlying`], [`ContractKey`] and the canonical
//! `contract_id`), market data ([`InstrumentSpec`], [`QuoteView`],
//! [`ChainSnapshot`]), and execution records ([`OrderIntent`],
//! [`OrderCommand`], [`Fill`], the lifecycle ids, and the
//! [`execution::sign_convention`] truth-table helpers).

pub mod contract;
pub mod execution;
pub mod market;
pub mod money;
pub mod time;

pub use contract::{ContractKey, Underlying};
pub use execution::{
    ExecutionMode, Fill, OrderCommand, OrderId, OrderIntent, PositionAction, PositionId,
    TimeInForce, TradeId,
};
pub use market::{ChainSnapshot, InstrumentSpec, QuoteView};
pub use money::{Cents, PriceCents, Quantity, Ticks};
pub use time::{SimTime, StepIndex};
