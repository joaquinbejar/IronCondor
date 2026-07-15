//! Canonical domain types.
//!
//! Integer-cents money newtypes ([`Cents`], [`PriceCents`], [`Quantity`],
//! [`Ticks`]), simulated time ([`SimTime`], [`StepIndex`]), contract
//! identity ([`Underlying`], [`ContractKey`] and the canonical
//! `contract_id`), and [`ExecutionMode`]. Market-data and execution-record
//! types land with roadmap issue #4.

pub mod contract;
pub mod money;
pub mod time;

pub use contract::{ContractKey, Underlying};
pub use money::{Cents, PriceCents, Quantity, Ticks};
pub use time::{SimTime, StepIndex};

use serde::{Deserialize, Serialize};

/// Which fill model executes a run's order intents.
///
/// Both modes emit the identical `Fill` shape so analytics is mode-agnostic;
/// the mode is semantic configuration and (from v0.3) hashes into the
/// `run_id`.
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

#[cfg(test)]
mod tests {
    use super::ExecutionMode;

    #[test]
    fn test_execution_mode_serde_snake_case_round_trip() {
        let naive = serde_json::to_string(&ExecutionMode::Naive);
        assert!(matches!(naive.as_deref(), Ok("\"naive\"")));
        let back: Result<ExecutionMode, _> = serde_json::from_str("\"realistic\"");
        assert!(matches!(back, Ok(ExecutionMode::Realistic)));
    }
}
