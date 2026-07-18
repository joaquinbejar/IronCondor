//! Simulated time primitives.
//!
//! The engine has no wall clock — all time is derived from the data feed's
//! step cadence. `SimTime` is strictly increasing across a run (a duplicate
//! or out-of-order snapshot is a [`crate::error::BacktestError::DataOutOfOrder`]
//! at tape validation, never silently reordered); gaps are allowed and
//! preserved. The clock never reads `SystemTime::now()` — the load-bearing
//! determinism invariant.

use serde::{Deserialize, Serialize};

/// A market timestamp: nanoseconds since the Unix epoch, UTC.
///
/// Serialises as its bare inner scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SimTime(i64);

impl SimTime {
    /// Wrap a timestamp in nanoseconds since the Unix epoch (UTC).
    #[must_use]
    pub const fn new(nanos: i64) -> Self {
        Self(nanos)
    }

    /// The inner timestamp in nanoseconds since the Unix epoch (UTC).
    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }
}

/// The 0-based ordinal of the current snapshot in the run.
///
/// Serialises as its bare inner scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StepIndex(u32);

impl StepIndex {
    /// Wrap a 0-based step ordinal.
    #[must_use]
    pub const fn new(step: u32) -> Self {
        Self(step)
    }

    /// The inner 0-based step ordinal.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{SimTime, StepIndex};

    #[test]
    fn test_time_newtypes_serialize_as_bare_scalars() {
        assert!(matches!(
            serde_json::to_string(&SimTime::new(1_750_291_200_000_000_000)).as_deref(),
            Ok("1750291200000000000")
        ));
        assert!(matches!(
            serde_json::to_string(&StepIndex::new(7)).as_deref(),
            Ok("7")
        ));
    }

    #[test]
    fn test_sim_time_orders_by_inner_nanos() {
        assert!(SimTime::new(1) < SimTime::new(2));
        assert!(StepIndex::new(0) < StepIndex::new(1));
    }
}
