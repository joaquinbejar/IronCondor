//! The deterministic replay engine.
//!
//! The replay loop (`BacktestEngine::run`), the `Strategy` seam over
//! `optionstratlib`, the simulation clock, scenario generation, and the
//! mark-to-market ledger (roadmap issues #5, #6, #10, #11, #14, #15, #46).
//!
//! [`clock`] has landed (issue #5): [`SimClock`] is the deterministic,
//! no-wall-clock time source the replay loop advances, and [`Event`] is the
//! conceptual model of the loop's per-step order. The remaining modules are
//! placeholders until their issues land.

pub mod clock;

pub use clock::{Event, SimClock};
