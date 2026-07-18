//! The deterministic replay engine.
//!
//! The replay loop ([`BacktestEngine::run`]), the `Strategy` seam over
//! `optionstratlib`, the simulation clock, scenario generation, and the
//! mark-to-market ledger (roadmap issues #5, #6, #10, #11, #14, #15, #46).
//!
//! [`clock`] has landed (issue #5): [`SimClock`] is the deterministic,
//! no-wall-clock time source the replay loop advances, and [`Event`] is the
//! conceptual model of the loop's per-step order. [`scenario`] and the
//! [`PositionableStrategy`] bound landed with issue #6. Issue #10 adds the
//! engine-facing [`Strategy`] trait, the [`ChainContext`] a strategy reads, and
//! the [`OptStratAdapter`] that wraps an upstream `optionstratlib` strategy
//! behind it. Issue #14 adds [`BacktestEngine::run`] — the normative replay
//! state machine — over the minimal [`Ledger`] (enriched by #15). Scenario
//! batch orchestration (#46) remains a placeholder.

pub mod backtest;
pub mod bundle_collector;
pub mod clock;
pub mod ledger;
pub mod scenario;
pub mod strategy;
pub mod substrate;
pub mod tradelog;

pub use backtest::{BacktestEngine, BacktestRun};
pub use bundle_collector::{FillRecord, PositionSnapshot};
pub use clock::{Event, SimClock};
pub use ledger::{Ledger, PositionMark, UnitGreeks};
pub use scenario::{
    ConfigOverride, ScenarioParams, ScenarioType, WalkPreset, child_data_seed, child_seed, expand,
};
pub use strategy::{ChainContext, OptStratAdapter, PositionableStrategy, Strategy};
pub use substrate::{AttributionSubstrate, LegAttributionSample, StepAttributionScalars};
pub use tradelog::ClosedTrade;
