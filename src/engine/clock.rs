//! The deterministic simulation clock and the conceptual loop event model.
//!
//! [`SimClock`] is the engine's only time source. It advances **solely** from
//! the data feed's snapshot timestamps — it never reads `SystemTime::now()` or
//! `Instant::now()`. This is the load-bearing determinism invariant: the same
//! `(seed, config, data)` produce a byte-identical result because time itself
//! is replayed from the tape, not sampled from the host
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility),
//! [docs/02 §8](../../../docs/02-engine-architecture.md#8-clock-model)).
//!
//! The clock is **naive** for v0.1: a step counter plus the feed's snapshot
//! timestamps, with no trading-day, holiday, or DST logic. The snapshot
//! timestamps *are* the trading calendar. A calendar-aware clock, if ever
//! needed, is a versioned successor, not a silent change.
//!
//! [`Event`] names the three step kinds of the replay loop as a **conceptual**
//! model; the hot loop does not materialise it (see the type's own docs).

use crate::domain::{ChainSnapshot, Fill, OrderCommand, SimTime, StepIndex};
use crate::error::BacktestError;

/// The deterministic simulation clock.
///
/// Holds the current [`StepIndex`] (evaluated against `ExitPolicy::TimeSteps`)
/// and [`SimTime`] (the market timestamp written into every `ts_ns` column and
/// consulted by time-based exit policies). It is advanced by the replay loop
/// once per snapshot and by nothing else; no method consults the host clock.
///
/// # Determinism
///
/// The clock is a determinism kernel: it reads no wall clock and draws no
/// randomness. Its state is a pure function of the timestamps the feed yields,
/// so a replay of the same tape reconstructs the same time sequence exactly
/// ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).
///
/// # Start state
///
/// [`docs/02 §8`](../../../docs/02-engine-architecture.md#8-clock-model) fixes
/// the field shape but is silent on the initial value, so the clock is created
/// at step `0` with a sentinel `ts` of `i64::MIN` (nanoseconds), meaning "no
/// snapshot seen yet". The first [`advance_to`](Self::advance_to) therefore
/// establishes the real `ts` and succeeds for any valid feed timestamp (the
/// sentinel itself, `ts == i64::MIN`, is outside the valid `SimTime` domain
/// and would be rejected); the same `ts <= self.ts` rejection then applies
/// uniformly to every later step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimClock {
    step: StepIndex,
    ts: SimTime,
}

impl SimClock {
    /// Create a clock in its pre-run state: step `0` and a sentinel `ts` of
    /// `i64::MIN` nanoseconds (before any snapshot).
    ///
    /// The first [`advance_to`](Self::advance_to) supplies the real first
    /// timestamp and always succeeds for a valid feed.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            step: StepIndex::new(0),
            ts: SimTime::new(i64::MIN),
        }
    }

    /// The current step index.
    #[inline]
    #[must_use]
    pub const fn step(&self) -> StepIndex {
        self.step
    }

    /// The current simulated timestamp (nanoseconds since the Unix epoch, UTC).
    #[inline]
    #[must_use]
    pub const fn ts(&self) -> SimTime {
        self.ts
    }

    /// Advance the clock to a snapshot's `ts` and `step`.
    ///
    /// Time only ever moves forward: `ts` must be **strictly after** the
    /// current `ts`. Gaps (a jump larger than one step) are allowed and
    /// preserved; only a duplicate or reversed timestamp is rejected. The
    /// clock never silently reorders or deduplicates — a rejected advance
    /// leaves the clock state unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::DataOutOfOrder`] when `ts <= self.ts` (a
    /// duplicate or reversed timestamp), carrying the offending `step`, `ts`,
    /// and the previous `prev` it failed to strictly exceed.
    ///
    /// Returns [`BacktestError::Data`] when `step` regresses below the current
    /// step even though `ts` advanced — a malformed tape whose step ordinals do
    /// not increase monotonically. This is enforced in **release** builds too
    /// (not a `debug_assert!`): a silently reordered step index would corrupt
    /// every step-keyed result, so it is a hard, typed error.
    pub fn advance_to(&mut self, ts: SimTime, step: StepIndex) -> Result<(), BacktestError> {
        if ts <= self.ts {
            return Err(BacktestError::DataOutOfOrder {
                step: step.value(),
                ts: ts.value(),
                prev: self.ts.value(),
            });
        }
        if step.value() < self.step.value() {
            return Err(BacktestError::Data(format!(
                "step index regressed: step {} is before the previous step {} at ts {}",
                step.value(),
                self.step.value(),
                ts.value(),
            )));
        }
        self.step = step;
        self.ts = ts;
        Ok(())
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

/// The conceptual model of the replay loop's three step kinds.
///
/// The loop is a synchronous state machine over these kinds; it never blocks
/// and never `.await`s inside a step.
///
/// # Conceptual, not the hot-path type
///
/// `Event` names the step kinds for documentation and reasoning. The concrete
/// engine (issue #14) does **not** materialise an owned `Vec<OrderCommand>` or
/// `Vec<Fill>` per step: the `Strategy` and `ExecutionModel` seams **append
/// into engine-owned buffers cleared in place** each step, so the steady-state
/// loop allocates nothing
/// ([docs/01 §8](../../../docs/01-domain-model.md#8-the-loop-event-model),
/// [docs/02 §4](../../../docs/02-engine-architecture.md#4-the-strategy-trait)).
///
/// # Canonical per-step order (the contract the loop must follow)
///
/// Each step processes exactly one [`Snapshot`](Event::Snapshot) in this fixed
/// order, with **exits ordered strictly before new entries**:
///
/// 1. **Snapshot** — `S_n` becomes current; [`SimClock`] advances to
///    `S_n.ts` / step `n`.
/// 2. **reprice / mark** — mark every open position at `S_n`'s mid (the
///    valuation the exit policy and strategy see).
/// 3. **exit-policy eval** — the strategy appends closing commands (the
///    `optionstratlib` adapter evaluates its `ExitPolicy`; the engine only
///    routes the intents).
/// 4. **Decision** — the strategy appends entry/adjustment commands to the
///    *same* queue, **after** the exits from step 3.
/// 5. **Fill** — the single execution phase for step `n`; every fill executes
///    against `S_n` and no other snapshot.
/// 6. **ledger mark-to-market** — apply the step's fills, re-mark at `S_n`'s
///    mid, and emit the step's one equity point.
/// 7. **record** — attribution and bundle rows for step `n`.
///
/// A step may produce zero decisions (the strategy holds) and zero fills. The
/// loop reads only `S_n` (and the already-processed `S_{n-1}` for attribution
/// deltas); it never peeks at `S_{n+1}` — no look-ahead, by construction
/// ([docs/02 §3](../../../docs/02-engine-architecture.md#3-the-replay-loop--normative-state-machine)).
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// New market data for this step — the sole source of forward time motion.
    Snapshot(ChainSnapshot),
    /// The strategy's output for this step: submit / cancel / replace commands
    /// (exits ordered strictly before entries).
    Decision(Vec<OrderCommand>),
    /// One execution result produced against the current snapshot.
    Fill(Fill),
}

#[cfg(test)]
mod tests {
    use super::{Event, SimClock};
    use crate::domain::{SimTime, StepIndex};
    use crate::error::BacktestError;

    #[test]
    fn test_clock_step_is_monotonic() {
        let mut clock = SimClock::new();
        // A strictly-increasing ts sequence with a gap between steps 1 and 2.
        let sequence = [(100i64, 0u32), (200, 1), (900, 2), (901, 3)];
        let mut prev_step = clock.step().value();
        let mut prev_ts = clock.ts().value();
        for (ts, step) in sequence {
            let outcome = clock.advance_to(SimTime::new(ts), StepIndex::new(step));
            assert!(matches!(outcome, Ok(())));
            // Step never decreases; ts strictly increases.
            assert!(clock.step().value() >= prev_step);
            assert!(clock.ts().value() > prev_ts);
            assert_eq!(clock.step().value(), step);
            assert_eq!(clock.ts().value(), ts);
            prev_step = clock.step().value();
            prev_ts = clock.ts().value();
        }
    }

    #[test]
    fn test_clock_rejects_reversed_ts_data_out_of_order() {
        let mut clock = SimClock::new();
        assert!(matches!(
            clock.advance_to(SimTime::new(500), StepIndex::new(0)),
            Ok(())
        ));

        // Reversed ts is rejected.
        let reversed = clock.advance_to(SimTime::new(400), StepIndex::new(1));
        assert!(matches!(
            reversed,
            Err(BacktestError::DataOutOfOrder {
                step: 1,
                ts: 400,
                prev: 500,
            })
        ));

        // Duplicate ts (ts == prev) is equally rejected.
        let duplicate = clock.advance_to(SimTime::new(500), StepIndex::new(1));
        assert!(matches!(
            duplicate,
            Err(BacktestError::DataOutOfOrder {
                step: 1,
                ts: 500,
                prev: 500,
            })
        ));

        // A rejected advance leaves the clock state unchanged.
        assert_eq!(clock.step().value(), 0);
        assert_eq!(clock.ts().value(), 500);
    }

    #[test]
    fn test_clock_rejects_step_regression_in_release() {
        // A step that regresses below the current step — even with a strictly
        // later ts — is a malformed tape and a hard typed error, enforced in
        // release builds too (the check is a real `if`, not a `debug_assert!`,
        // so this test holds whether or not `debug_assertions` is set).
        let mut clock = SimClock::new();
        assert!(matches!(
            clock.advance_to(SimTime::new(1_000), StepIndex::new(5)),
            Ok(())
        ));
        // ts advances (2_000 > 1_000) but step regresses (3 < 5): rejected.
        let regressed = clock.advance_to(SimTime::new(2_000), StepIndex::new(3));
        assert!(matches!(regressed, Err(BacktestError::Data(_))));
        // A rejected advance leaves the clock state unchanged.
        assert_eq!(clock.step().value(), 5);
        assert_eq!(clock.ts().value(), 1_000);
    }

    #[test]
    fn test_clock_advance_strictly_later_ts_succeeds() {
        let mut clock = SimClock::new();
        assert!(matches!(
            clock.advance_to(SimTime::new(1_000), StepIndex::new(0)),
            Ok(())
        ));
        // A large gap in ts is allowed and preserved verbatim.
        let later = clock.advance_to(SimTime::new(5_000_000), StepIndex::new(1));
        assert!(matches!(later, Ok(())));
        assert_eq!(clock.ts().value(), 5_000_000);
        assert_eq!(clock.step().value(), 1);
    }

    #[test]
    fn test_clock_default_matches_new() {
        let created = SimClock::new();
        let defaulted = SimClock::default();
        assert_eq!(created, defaulted);
        assert_eq!(created.step().value(), 0);
        assert_eq!(created.ts().value(), i64::MIN);
    }

    #[test]
    fn test_event_decision_variant_holds_command_buffer() {
        // The conceptual Event model compiles against the domain command type.
        let event = Event::Decision(Vec::new());
        assert!(matches!(event, Event::Decision(ref cmds) if cmds.is_empty()));
    }
}
