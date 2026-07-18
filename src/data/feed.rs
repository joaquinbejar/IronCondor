//! The `DataFeed` seam, the `TapeMeta` the engine reads before it opens a
//! writer, and the feed catalogue.
//!
//! Every feed materialises a **validated, strictly-ordered, immutable tape**
//! before the run starts and yields from it synchronously
//! ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop),
//! [docs/02 §5](../../../docs/02-engine-architecture.md#5-seams-datafeed-and-executionmodel)).
//! [`DataFeed::next`] is the only source of forward motion — there is **no**
//! `peek`, rewind, or random access, so the no-look-ahead invariant holds *by
//! construction*, not by discipline. [`DataFeed::tape_meta`] exposes the tape's
//! pinned identity and its non-empty / final-step facts **before** the bundle
//! writer exists, which is what lets startup derive `run_id` and open
//! `<run_id>/` in the right order
//! ([docs/02 §3.1](../../../docs/02-engine-architecture.md#31-startup-before-the-first-snapshot)).
//!
//! The [`FeedKind`] catalogue encodes the normative feed table
//! ([docs/03 §3](../../../docs/03-data-layer.md#3-feed-catalogue)): the CSV and
//! Parquet feeds are always available, the simulator feed only under the
//! `simulator` feature. The run driver matches [`FeedKind::of`] to pick which
//! concrete [`DataFeed`] to construct and hands it to the monomorphised
//! `run::<F: DataFeed>`; the catalogue never forces `dyn` dispatch into the hot
//! loop. The concrete loaders land per-feed (Parquet #9, CSV/simulator later);
//! this module is the seam they plug into.
//!
//! This module never imports `src/engine/` — a lower layer importing `engine`
//! is a review reject ([CLAUDE.md](../../../CLAUDE.md) module boundaries).

use crate::data::DataSourceSpec;
use crate::domain::{ChainSnapshot, SimTime, StepIndex};
use crate::error::BacktestError;

/// Pinned facts about the materialised tape, known **before** the writer or
/// `run_id` exist.
///
/// Returned by feed materialisation and read by engine startup
/// ([docs/02 §5](../../../docs/02-engine-architecture.md#5-seams-datafeed-and-executionmodel),
/// [docs/02 §3.1](../../../docs/02-engine-architecture.md#31-startup-before-the-first-snapshot)).
/// A file feed builds it from the file `sha256` and validated rows; the
/// simulator feed from the `sha256` of the materialised tape
/// ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapeMeta {
    /// The tape's pinned data identity — the `sha256` of the file bytes (file
    /// feed) or of the materialised tape (simulator feed). This is the
    /// `run_id` data-identity input
    /// ([docs/01 §10](../../../docs/01-domain-model.md#10-run-identity-and-manifest)).
    pub data_identity: String,
    /// The non-empty guarantee: always `true` when construction succeeds, since
    /// an empty tape fails to construct — so the loop never has to check.
    pub non_empty: bool,
    /// The tape's first timestamp — the anchor for relative-expiry resolution
    /// ([docs/01 §5.1](../../../docs/01-domain-model.md#51-expiration-resolves-to-one-absolute-instant)).
    pub first_ts: SimTime,
    /// The last step index; the loop knows the tape length up front.
    pub final_step: StepIndex,
}

impl TapeMeta {
    /// Build tape metadata from an ordered, non-empty snapshot tape.
    ///
    /// `data_identity` is the caller-computed identity (file `sha256` or
    /// materialised-tape `sha256`,
    /// [docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)).
    /// The timestamps must be **strictly increasing** across the tape (gaps are
    /// allowed and preserved); the non-empty guarantee is established here so
    /// the loop never has to re-check it.
    ///
    /// # Errors
    ///
    /// - [`BacktestError::Conversion`] if the tape is empty — the non-empty
    ///   guarantee cannot be established.
    /// - [`BacktestError::DataOutOfOrder`] if any timestamp is not strictly
    ///   after its predecessor (a duplicate or reversed `ts`).
    pub fn from_tape(data_identity: String, tape: &[ChainSnapshot]) -> Result<Self, BacktestError> {
        let mut iter = tape.iter();
        let first = iter.next().ok_or_else(|| {
            BacktestError::Conversion("cannot build tape meta from an empty tape".to_string())
        })?;
        let first_ts = first.ts;
        let mut prev = first.ts;
        let mut final_step = first.step;
        for snapshot in iter {
            if snapshot.ts <= prev {
                return Err(BacktestError::DataOutOfOrder {
                    step: snapshot.step.value(),
                    ts: snapshot.ts.value(),
                    prev: prev.value(),
                });
            }
            prev = snapshot.ts;
            final_step = snapshot.step;
        }
        Ok(Self {
            data_identity,
            non_empty: true,
            first_ts,
            final_step,
        })
    }
}

/// The engine's market-data seam: a pull-only iterator over a materialised
/// tape.
///
/// Owned by `src/data/` and consumed by the engine
/// ([docs/03 §2](../../../docs/03-data-layer.md#2-the-datafeed-trait),
/// [docs/02 §5](../../../docs/02-engine-architecture.md#5-seams-datafeed-and-executionmodel)).
/// The trait is deliberately three methods and **no more** — there is no
/// `peek`, rewind, or random access, so a step can never read a later snapshot
/// and no-look-ahead holds by construction.
///
/// # The pull-only, never-blocking contract
///
/// [`Self::next`] **never blocks and never `.await`s**: every feed materialises
/// a validated, strictly-ordered, immutable tape *before* the loop starts, and
/// `next` yields from it synchronously — a pure in-memory read. All network,
/// session, and I/O work is confined to feed construction
/// ([docs/03 §6.1](../../../docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)).
/// [`Self::tape_meta`] is callable the instant the feed exists — before any
/// writer or `run_id` — carrying the `data_identity` that seeds `run_id`
/// derivation ([docs/02 §3.1](../../../docs/02-engine-architecture.md#31-startup-before-the-first-snapshot)).
pub trait DataFeed {
    /// Pull the next snapshot in tape order, or `None` once the feed is
    /// exhausted (and `None` on every call thereafter).
    ///
    /// Never blocks and never `.await`s — a synchronous read from the
    /// pre-materialised tape.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError`] if a feed that lazily surfaces a validation
    /// failure at yield time reports one; the materialised feeds validate up
    /// front, so a well-formed tape yields `Ok` until exhaustion.
    fn next(&mut self) -> Result<Option<ChainSnapshot>, BacktestError>;

    /// Describe the source for the run manifest's `data_source` provenance.
    fn meta(&self) -> DataSourceSpec;

    /// The tape's pinned metadata — available **before** any writer exists so
    /// startup can derive `run_id` and open `<run_id>/` in the right order.
    fn tape_meta(&self) -> &TapeMeta;
}

/// The feed catalogue — the seam of
/// [docs/03 §3](../../../docs/03-data-layer.md#3-feed-catalogue).
///
/// Each variant names one feed the engine can drive; the concrete loaders land
/// per-feed (Parquet #9, CSV/simulator later). The run driver matches
/// [`FeedKind::of`] on a [`DataSourceSpec`] to pick which [`DataFeed`]
/// implementation to construct, then hands the concrete feed to the
/// monomorphised `run::<F: DataFeed>` — so the catalogue selects a feed without
/// ever forcing `dyn` dispatch into the hot loop. The simulator entry exists
/// only under the `simulator` feature, matching the `DataSourceSpec::Simulator`
/// variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedKind {
    /// A directory of per-step CSV chain files (feature `default`).
    Csv,
    /// One columnar Parquet file (feature `default`; the v0.1 release feed).
    Parquet,
    /// An OptionChain-Simulator REST session (feature `simulator`).
    #[cfg(feature = "simulator")]
    Simulator,
}

impl FeedKind {
    /// Classify a [`DataSourceSpec`] into its feed kind.
    #[must_use]
    pub fn of(spec: &DataSourceSpec) -> Self {
        match spec {
            DataSourceSpec::Csv { .. } => Self::Csv,
            DataSourceSpec::Parquet { .. } => Self::Parquet,
            #[cfg(feature = "simulator")]
            DataSourceSpec::Simulator { .. } => Self::Simulator,
        }
    }

    /// The Cargo feature flag that enables this feed
    /// ([docs/03 §3](../../../docs/03-data-layer.md#3-feed-catalogue)).
    #[must_use]
    pub const fn feature_flag(self) -> &'static str {
        match self {
            Self::Csv | Self::Parquet => "default",
            #[cfg(feature = "simulator")]
            Self::Simulator => "simulator",
        }
    }

    /// Whether this feed reproduces the same tape from the same source today.
    ///
    /// File feeds are byte-reproducible (their `sha256` pins the input); the
    /// simulator feed is **not reproducible today** — the server walk is
    /// unseeded, so the same request can yield a different tape
    /// ([docs/03 §3](../../../docs/03-data-layer.md#3-feed-catalogue),
    /// [docs/03 §6](../../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
    #[must_use]
    pub const fn is_reproducible(self) -> bool {
        match self {
            Self::Csv | Self::Parquet => true,
            #[cfg(feature = "simulator")]
            Self::Simulator => false,
        }
    }
}

/// The feeds available in this build — CSV and Parquet always, the simulator
/// feed only under the `simulator` feature
/// ([docs/03 §3](../../../docs/03-data-layer.md#3-feed-catalogue)).
#[must_use]
pub const fn feed_catalogue() -> &'static [FeedKind] {
    &[
        FeedKind::Csv,
        FeedKind::Parquet,
        #[cfg(feature = "simulator")]
        FeedKind::Simulator,
    ]
}

/// A trivial in-memory [`DataFeed`] over a pre-built, ordered tape.
///
/// Test-only scaffolding — compiled under `cfg(test)` only, so it never bloats
/// a release build — shared across the data and engine test suites (issues #9 /
/// #14). It drives an ordered sequence of [`ChainSnapshot`]s to exhaustion,
/// exactly the pull-only contract a real feed presents once its tape is
/// materialised; it performs no I/O and never `.await`s.
#[cfg(test)]
pub(crate) struct InMemoryFeed {
    tape: Vec<ChainSnapshot>,
    cursor: usize,
    meta: TapeMeta,
    source: DataSourceSpec,
}

#[cfg(test)]
impl InMemoryFeed {
    /// Build a feed over `tape`, deriving its [`TapeMeta`] from the snapshots.
    ///
    /// # Errors
    ///
    /// Propagates [`TapeMeta::from_tape`]: an empty tape
    /// ([`BacktestError::Conversion`]) or a non-strictly-increasing timestamp
    /// ([`BacktestError::DataOutOfOrder`]).
    pub(crate) fn new(
        data_identity: String,
        tape: Vec<ChainSnapshot>,
        source: DataSourceSpec,
    ) -> Result<Self, BacktestError> {
        let meta = TapeMeta::from_tape(data_identity, &tape)?;
        Ok(Self {
            tape,
            cursor: 0,
            meta,
            source,
        })
    }
}

#[cfg(test)]
impl DataFeed for InMemoryFeed {
    fn next(&mut self) -> Result<Option<ChainSnapshot>, BacktestError> {
        match self.tape.get(self.cursor) {
            Some(snapshot) => {
                // `get` matched, so `cursor < tape.len() <= isize::MAX` — the
                // increment cannot overflow. Plain `+= 1` keeps to the
                // codebase's no-saturating/wrapping convention.
                self.cursor += 1;
                Ok(Some(snapshot.clone()))
            }
            None => Ok(None),
        }
    }

    fn meta(&self) -> DataSourceSpec {
        self.source.clone()
    }

    fn tape_meta(&self) -> &TapeMeta {
        &self.meta
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{DataFeed, FeedKind, InMemoryFeed, TapeMeta, feed_catalogue};
    use crate::data::DataSourceSpec;
    use crate::domain::{
        ChainSnapshot, InstrumentSpec, PriceCents, SimTime, StepIndex, Underlying,
    };
    use crate::error::BacktestError;

    // A minimal quote-less snapshot is enough to exercise ordering and
    // exhaustion; the conversion boundary is where quotes are validated.
    fn snapshot(ts: i64, step: u32) -> ChainSnapshot {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("5c tick / 100x multiplier is a valid spec");
        };
        ChainSnapshot {
            ts: SimTime::new(ts),
            step: StepIndex::new(step),
            underlying,
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes: BTreeMap::new(),
        }
    }

    fn parquet_source() -> DataSourceSpec {
        DataSourceSpec::Parquet {
            path: "chains/spx.parquet".to_string(),
            sha256: "abc123".to_string(),
        }
    }

    fn feed_of(steps: u32) -> Result<InMemoryFeed, BacktestError> {
        let tape: Vec<ChainSnapshot> = (0..steps)
            .map(|s| snapshot(10 * (i64::from(s) + 1), s))
            .collect();
        InMemoryFeed::new("tape-sha".to_string(), tape, parquet_source())
    }

    #[test]
    fn test_in_memory_feed_yields_in_order_then_none() {
        let Ok(mut feed) = feed_of(3) else {
            panic!("a 3-snapshot ordered tape builds");
        };
        for (expected_ts, expected_step) in [(10, 0), (20, 1), (30, 2)] {
            match feed.next() {
                Ok(Some(snap)) => {
                    assert_eq!(snap.ts, SimTime::new(expected_ts));
                    assert_eq!(snap.step, StepIndex::new(expected_step));
                }
                other => panic!("expected snapshot at step {expected_step}, got {other:?}"),
            }
        }
        // Exhausted — and it stays exhausted on every subsequent pull.
        assert!(matches!(feed.next(), Ok(None)));
        assert!(matches!(feed.next(), Ok(None)));
    }

    #[test]
    fn test_tape_meta_accessible_without_any_writer() {
        // No writer is ever constructed in this test: tape_meta() is available
        // the instant the feed exists, carrying the data_identity run_id needs.
        let Ok(feed) = feed_of(4) else {
            panic!("a 4-snapshot ordered tape builds");
        };
        let meta = feed.tape_meta();
        assert!(meta.non_empty);
        assert_eq!(meta.data_identity, "tape-sha");
        assert_eq!(meta.first_ts, SimTime::new(10));
        assert_eq!(meta.final_step, StepIndex::new(3));
    }

    #[test]
    fn test_tape_meta_from_tape_rejects_empty_tape_conversion_error() {
        let empty: Vec<ChainSnapshot> = Vec::new();
        let meta = TapeMeta::from_tape("id".to_string(), &empty);
        assert!(matches!(meta, Err(BacktestError::Conversion(_))));
    }

    #[test]
    fn test_tape_meta_from_tape_rejects_out_of_order_timestamps() {
        let tape = vec![snapshot(30, 0), snapshot(20, 1)];
        let meta = TapeMeta::from_tape("id".to_string(), &tape);
        assert!(matches!(
            meta,
            Err(BacktestError::DataOutOfOrder {
                step: 1,
                ts: 20,
                prev: 30
            })
        ));
    }

    #[test]
    fn test_feed_meta_returns_the_configured_source() {
        let Ok(feed) = feed_of(1) else {
            panic!("a 1-snapshot tape builds");
        };
        assert_eq!(feed.meta(), parquet_source());
    }

    #[test]
    fn test_feed_catalogue_lists_file_feeds_and_gates_simulator() {
        let catalogue = feed_catalogue();
        assert!(catalogue.contains(&FeedKind::Csv));
        assert!(catalogue.contains(&FeedKind::Parquet));
        assert_eq!(FeedKind::of(&parquet_source()), FeedKind::Parquet);
        assert_eq!(FeedKind::Parquet.feature_flag(), "default");
        assert!(FeedKind::Parquet.is_reproducible());

        #[cfg(feature = "simulator")]
        {
            assert!(catalogue.contains(&FeedKind::Simulator));
            assert_eq!(FeedKind::Simulator.feature_flag(), "simulator");
            assert!(!FeedKind::Simulator.is_reproducible());
        }
        #[cfg(not(feature = "simulator"))]
        {
            assert_eq!(catalogue.len(), 2, "only file feeds without the feature");
        }
    }
}
