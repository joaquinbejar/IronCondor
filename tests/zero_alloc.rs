//! PB-1 zero-steady-state-allocation replay-loop gate (issue #19).
//!
//! This is a **hard CI gate**, not a benchmark: it asserts that allocations
//! inside the engine's per-step body do **not** grow with step count. It gates
//! like a correctness test — a non-zero per-step delta fails the build
//! ([docs/07 §4/§6](../docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop),
//! [docs/TESTING.md §11.1](../docs/TESTING.md#111-performance-regression-gates),
//! [docs/02 §3.2](../docs/02-engine-architecture.md#32-per-step-for-each-snapshot-s_n-on-the-tape-in-order)).
//!
//! # How it measures (no production hook)
//!
//! A process-global counting allocator increments a **per-thread** counter on
//! every `alloc` / `alloc_zeroed` / `realloc` **event** (a `realloc` counts as
//! one event). A [`SamplingStrategy`] decorator wraps the real strategy and, at
//! a **fixed per-step phase** — the very start of `on_snapshot`, before it
//! delegates to the inner strategy — records the current counter into a
//! **pre-sized** buffer. The recording is allocation-free (a thread-local read
//! plus a write into an already-sized `Rc<[Cell<usize>]>` slot), so the act of
//! measuring never perturbs the measurement. Nothing in `src/` changes: the
//! gate is entirely a decorator + a test-only allocator.
//!
//! Sampling at the same phase each step means the delta between step `K`'s
//! sample and the last step's sample equals the allocations of the full step
//! bodies over the steady-state tail `K..last`. If per-step steady-state
//! allocation is zero, that delta is **zero**.
//!
//! # The measurement boundary (what PB-1 gates, precisely)
//!
//! The boundary is the **per-step body only** — steps (b)–(g) of the loop
//! ([docs/02 §3.2](../docs/02-engine-architecture.md#32-per-step-for-each-snapshot-s_n-on-the-tape-in-order)).
//! **Startup** (feed materialisation, `run_id`, writer construction, the
//! one-time `on_start` sizing of `cmds` / `fills` / `equity_curve` /
//! ledger-scratch) and **termination** (`finalize` + Parquet encode) are
//! **outside** it and may allocate — the delta cancels every allocation before
//! step `K` and after the last sample.
//!
//! One seam sits at the edge of that body and is deliberately kept off the
//! measured engine path, matching the boundary above:
//!
//! - **Step (a), "snapshot visible" — the data feed's `next()`.** The measured
//!   engine gate drives an alloc-free [`MoveFeed`] that *moves* each pre-
//!   materialised snapshot out of the validated canonical tape rather than
//!   cloning it, so the step-(a) handoff (the data adapter's job, outside the
//!   (b)–(g) boundary) contributes nothing. (The production [`ParquetFeed`]
//!   clones the snapshot in `next()` — a measured **1 alloc/step**, a data-
//!   layer cost, not an engine-loop-body cost; see `test_parquet_feed_*`.)
//!
//! **Step (c), the `optionstratlib` exit seam, is now inside the gate.** As of
//! the #19 FOLD-THE-FIX verdict the real `IronCondor` adapter's `exits()` sources
//! `underlying` directly from the snapshot scalar and, in v0.1, does **not**
//! rebuild an `OptionChain` (the per-step `inner` reprice — and its upstream
//! `Utc::now()` reach — is deferred until a Greek-driven policy is wired). The
//! exit seam therefore allocates **zero** per step, so THE gate below drives the
//! **real** [`OptStratAdapter<IronCondor>`] (built via `from_spec`, the real v0.1
//! strategy) over the alloc-free [`MoveFeed`] and asserts a zero (b)–(g) tail
//! delta — the honest PB-1 gate over the production strategy path. The
//! zero-activity [`SteadyHold`] probe remains only as the base for the negative
//! test (which injects a deliberate per-step allocation) and the [`ParquetFeed`]
//! step-(a) diagnostic.
//!
//! # `no dyn per element` / `no format!` on the step path
//!
//! These are enforced **by construction** and subsumed by this gate: the engine
//! drives its seams as monomorphised generics (`BacktestEngine::run<F, X, S>`),
//! so there is no per-element `dyn` dispatch in the loop; and any `format!` /
//! `to_string` / `Box::new` / `Vec::new` executed per step would allocate and
//! trip this very gate. A zero per-step delta is therefore direct evidence that
//! no such call runs on the steady-state step path.
//!
//! # Realistic mode is gated too — at a ceiling, not at zero (#127)
//!
//! Everything above describes the **naive** step body, whose gate is an exact
//! zero. Realistic fills route every order through the upstream
//! `option-chain-orderbook` matching engine, so their warm step allocates by
//! construction (≈ 564 events/step, measured). The `realistic` module at the
//! bottom of this file — behind `--features orderbook` — reuses the same
//! harness with `RealisticFill` in place of `NaiveFill` and gates that path at a
//! **measured per-step ceiling** plus a one-sided **linearity** assertion, with
//! one negative test per assertion proving each bites. See `BENCH.md`, PB-3's "Per-step allocation"
//! block, for the measurement and the budget derivation.
//!
//! **Scope, stated honestly: a gated warm step is a book REFRESH with NO fills.**
//! The condor opens once at `on_start` and the exit policy never triggers, so
//! the only fills in the run land at step 0 (the four opens) and at step 63
//! after the last sample (the four `on_end` closes) — both outside the measured
//! window. Every warm step in `K..LAST` therefore issues zero orders of its own,
//! and the ≈ 564 events it allocates are the per-step liquidity reseed alone.
//! **Per-fill allocation in realistic mode is not covered by this gate.**
//!
//! # Test-only `unsafe`
//!
//! `ironcondor` itself is `#![forbid(unsafe_code)]`. A counting `GlobalAlloc`
//! is **unavoidably** `unsafe impl` — but this file is a **separate crate root**
//! (integration test target), which does *not* inherit the library crate's
//! `forbid(unsafe_code)`. The `unsafe` here lives only in the test binary, never
//! in the shipped library, and does nothing but forward to `System` after a
//! non-allocating counter bump.

#![allow(missing_docs)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;
use std::rc::Rc;

use ironcondor::{
    BacktestConfig, BacktestEngine, BacktestError, ChainContext, ChainSnapshot, DataFeed,
    DataSourceSpec, ExecutionModel, NaiveFill, OptStratAdapter, OrderCommand, OrderIntent,
    ParquetFeed, PositionAction, Quantity, ResourceLimits, Strategy, TapeMeta, TimeInForce,
};
use optionstratlib::simulation::ExitPolicy;
use optionstratlib::strategies::IronCondor;
use optionstratlib::{OptionStyle, Side};

mod common;

// --- the counting allocator (test-only unsafe; see the module docs) ---------

thread_local! {
    /// Allocation-event count for the **current thread**. Per-thread (not a
    /// process-global atomic) so the single-threaded engine run on the test's
    /// own thread is counted in isolation — immune to other test threads
    /// allocating concurrently under `cargo test`'s default parallelism.
    /// `const`-initialised and `Drop`-free, so accessing it never allocates and
    /// never recurses into the allocator.
    static TL_ALLOCS: Cell<usize> = const { Cell::new(0) };
}

/// A `System`-backed allocator that counts allocation events. It never
/// allocates inside `alloc` (only a thread-local counter bump), so it cannot
/// deadlock or recurse.
struct CountingAllocator;

// SAFETY (test-only): every method forwards verbatim to the `System` allocator;
// the only added work is a non-allocating thread-local counter bump. This lives
// exclusively in the test binary — the library stays `#![forbid(unsafe_code)]`.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        TL_ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        TL_ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        TL_ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The current thread's cumulative allocation-event count.
fn allocations() -> usize {
    TL_ALLOCS.with(Cell::get)
}

// --- fixed-length fixture ----------------------------------------------------

/// Steps on the gate tape. Long enough that `last (63) >> K (8)`, so a per-step
/// allocation would accumulate into an unmistakable non-zero tail delta.
const STEPS: u32 = 64;

/// Warmup step `K`. The condor opens once (at `on_start`, before step 0) and
/// every one of its four contracts is marked at step 0's ledger settle, so all
/// one-time growth — the `cmds`/`fills`/`equity_curve` buffers (sized at
/// startup), the ledger's `marks` and `greeks` `BTreeMap`s and the
/// `position_marks` scratch (grown on first mark at step 0) — is complete before
/// step 1. `K = 8` is well past
/// that single entry and fully into steady state, so the delta `K..last`
/// measures only warm per-step bodies.
///
/// The realistic gate deliberately shares this `K`, even though the order book
/// has a longer warm-up of its own (its per-step allocation only plateaus around
/// step 37). Including that head makes the realistic window average
/// conservative for a ceiling; see `realistic::REALISTIC_WARM_STEP_BUDGET`.
const K: usize = 8;

/// The last step index on the tape.
const LAST: usize = STEPS as usize - 1;

// --- alloc-free feed ---------------------------------------------------------

/// A [`DataFeed`] that yields snapshots by **moving** them out of a pre-filled
/// `Vec` (no per-step clone), so step (a) — the snapshot handoff — contributes
/// zero allocation and the gate measures only the engine's (b)–(g) body.
///
/// It is seeded from the **canonical** condor Parquet fixture: the tape is
/// materialised and validated through [`ParquetFeed`] exactly as production
/// would, then drained here once (a setup cost, outside the measured window).
struct MoveFeed {
    tape: Vec<Option<ChainSnapshot>>,
    cursor: usize,
    meta: TapeMeta,
    source: DataSourceSpec,
}

impl DataFeed for MoveFeed {
    fn next(&mut self) -> Result<Option<ChainSnapshot>, BacktestError> {
        let out = self.tape.get_mut(self.cursor).and_then(Option::take);
        if out.is_some() {
            self.cursor += 1;
        }
        Ok(out)
    }
    fn meta(&self) -> DataSourceSpec {
        self.source.clone()
    }
    fn tape_meta(&self) -> &TapeMeta {
        &self.meta
    }
}

/// Write the canonical condor fixture to `path` and open a [`ParquetFeed`].
fn parquet_feed(path: &Path) -> ParquetFeed {
    let rows = common::condor_rows(STEPS, None);
    let Ok(()) = common::write_parquet(path, &rows) else {
        panic!("write the canonical condor parquet fixture");
    };
    let Ok(feed) = ParquetFeed::open(path, &ResourceLimits::default()) else {
        panic!("open the canonical condor parquet feed");
    };
    feed
}

/// Drain the canonical [`ParquetFeed`] tape into an alloc-free [`MoveFeed`].
fn move_feed(path: &Path) -> MoveFeed {
    let mut pf = parquet_feed(path);
    let meta = pf.tape_meta().clone();
    let source = pf.meta();
    let mut tape = Vec::new();
    loop {
        match pf.next() {
            Ok(Some(snapshot)) => tape.push(Some(snapshot)),
            Ok(None) => break,
            Err(_) => panic!("the canonical tape drains without error"),
        }
    }
    MoveFeed {
        tape,
        cursor: 0,
        meta,
        source,
    }
}

/// A valid naive config over the canonical fixture at `path`.
fn config(path: &Path) -> BacktestConfig {
    common::condor_config(path, 42)
}

// --- the sampling decorator --------------------------------------------------

/// Wraps any [`Strategy`] and records the global allocation counter at the
/// **start of every `on_snapshot`** (before delegating), into a pre-sized
/// shared buffer. Every callback is forwarded unchanged, so the run's behaviour
/// is identical to the inner strategy's.
struct SamplingStrategy<S: Strategy> {
    inner: S,
    samples: Rc<[Cell<usize>]>,
    idx: usize,
}

impl<S: Strategy> SamplingStrategy<S> {
    fn new(inner: S, samples: &Rc<[Cell<usize>]>) -> Self {
        Self {
            inner,
            samples: Rc::clone(samples),
            idx: 0,
        }
    }
}

impl<S: Strategy> Strategy for SamplingStrategy<S> {
    fn on_start(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        self.inner.on_start(ctx, out)
    }

    fn exits(
        &mut self,
        ctx: &ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        self.inner.exits(ctx, out)
    }

    fn on_snapshot(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        // Fixed per-step sample phase. Alloc-free: a thread-local read plus a
        // write into a slot that already exists (the buffer is pre-sized before
        // the run), so measuring never perturbs the measurement.
        if let Some(slot) = self.samples.get(self.idx) {
            slot.set(allocations());
            self.idx += 1;
        }
        self.inner.on_snapshot(ctx, out)
    }

    fn on_end(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        self.inner.on_end(ctx, out)
    }
}

// --- probe strategies --------------------------------------------------------

/// Zero-per-step-activity engine probe: opens the four condor legs once at
/// `on_start` and holds them with **no** per-step allocation of its own, so a
/// non-zero tail delta can only come from the engine body or an injected
/// regression. The four legs are re-marked by the ledger every step — the
/// representative steady-state workload. It is the base for the negative test
/// (which injects a deliberate per-step allocation) and the [`ParquetFeed`]
/// step-(a) diagnostic; THE gate drives the real adapter instead.
struct SteadyHold;

impl SteadyHold {
    /// Emit one `Submit(Open)` per snapshot quote (the four condor legs). Which
    /// side is irrelevant to the allocation measurement; the fixture's four
    /// strikes match the canonical condor.
    fn open_all(ctx: &ChainContext, out: &mut Vec<OrderCommand>) {
        for quote in ctx.snapshot.quotes.values() {
            let side = match quote.contract.style {
                OptionStyle::Call => Side::Short,
                OptionStyle::Put => Side::Long,
            };
            let Ok(quantity) = Quantity::new(1) else {
                panic!("1 is a valid quantity");
            };
            out.push(OrderCommand::Submit(OrderIntent {
                contract: quote.contract.clone(),
                action: PositionAction::Open,
                side,
                quantity,
                limit: None,
                tif: TimeInForce::Ioc,
                decision_mid: quote.mid,
            }));
        }
    }
}

impl Strategy for SteadyHold {
    fn on_start(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        Self::open_all(ctx, out);
        Ok(())
    }
    fn on_snapshot(
        &mut self,
        _ctx: &mut ChainContext,
        _out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        Ok(())
    }
    fn on_end(
        &mut self,
        _ctx: &mut ChainContext,
        _out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        Ok(())
    }
}

/// [`SteadyHold`] plus a **deliberate** per-step heap allocation inside the step
/// body — the injected regression the negative test proves the gate catches.
struct BadStep {
    inner: SteadyHold,
}

impl Strategy for BadStep {
    fn on_start(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        self.inner.on_start(ctx, out)
    }
    fn on_snapshot(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        // The deliberate per-step allocation the gate must catch.
        let v: Vec<u8> = Vec::with_capacity(64);
        std::hint::black_box(&v);
        self.inner.on_snapshot(ctx, out)
    }
    fn on_end(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        self.inner.on_end(ctx, out)
    }
}

// --- harness -----------------------------------------------------------------

/// Run `inner` (wrapped in a [`SamplingStrategy`]) over the alloc-free
/// [`MoveFeed`] view of the canonical condor fixture, and return the per-step
/// cumulative allocation-count samples (one per step, recorded at each
/// `on_snapshot` start).
fn sample_over_movefeed<S: Strategy>(inner: S) -> Vec<usize> {
    sample_with(
        config,
        |cfg| NaiveFill::new(cfg.slippage.clone(), cfg.fees),
        inner,
    )
}

/// The shared measurement harness behind every sampler in this file: write the
/// canonical condor fixture into a fresh tempdir, build the config with
/// `make_config` and the fill model with `make_execution`, then run `inner`
/// (wrapped in a [`SamplingStrategy`]) over the alloc-free [`MoveFeed`] view of
/// that fixture. Returns the per-step cumulative allocation-count samples (one
/// per step, recorded at each `on_snapshot` start).
///
/// Parameterising the execution model is what lets the naive gate and the
/// realistic budget gate share one tempdir/feed/config plumbing, so the two
/// measurements differ **only** in the fill model under test.
fn sample_with<S, X, C, E>(make_config: C, make_execution: E, inner: S) -> Vec<usize>
where
    S: Strategy,
    X: ExecutionModel,
    C: FnOnce(&Path) -> BacktestConfig,
    E: FnOnce(&BacktestConfig) -> X,
{
    let Ok(dir) = tempfile::tempdir() else {
        panic!("create a tempdir for the fixture");
    };
    let path = dir.path().join("condor_chain.parquet");
    let feed = move_feed(&path);
    let cfg = make_config(&path);
    let execution = make_execution(&cfg);

    // Pre-size the sample buffer BEFORE the run so recording never allocates.
    let samples: Rc<[Cell<usize>]> = (0..STEPS).map(|_| Cell::new(0)).collect();
    let strategy = SamplingStrategy::new(inner, &samples);

    let Ok(_run) = BacktestEngine::run(&cfg, feed, execution, strategy, "zero-alloc-gate") else {
        panic!("the sampled backtest runs to completion");
    };
    samples.iter().map(Cell::get).collect()
}

/// The gate quantity: allocation events over the steady-state tail `K..last`.
/// Samples are cumulative and recorded in order, so `at_last >= at_k`.
fn tail_delta(samples: &[usize]) -> usize {
    let (Some(&at_last), Some(&at_k)) = (samples.get(LAST), samples.get(K)) else {
        panic!("samples cover every step of the fixed-length run");
    };
    let Some(delta) = at_last.checked_sub(at_k) else {
        panic!("cumulative allocation samples must be monotonically non-decreasing");
    };
    delta
}

// --- the gate ----------------------------------------------------------------

/// Build the **real** v0.1 strategy: an `OptStratAdapter<IronCondor>` from the
/// canonical spec with a non-triggering exit, so every step exercises the real
/// exit seam (which, post-#19, does no per-step `OptionChain` rebuild).
fn real_iron_condor_adapter() -> OptStratAdapter<IronCondor> {
    let Ok(adapter) = OptStratAdapter::<IronCondor>::from_spec(
        &common::iron_condor_spec(),
        non_triggering_exit(),
    ) else {
        panic!("the canonical iron-condor spec builds a valid adapter");
    };
    adapter
}

/// THE GATE (build-failing). The real v0.1 strategy path's per-step body
/// allocates nothing in steady state: driving the real
/// `OptStratAdapter<IronCondor>` over the alloc-free [`MoveFeed`], the
/// allocation-event delta between warmup step `K` and the last step is exactly
/// zero. Post-#19 the exit seam no longer rebuilds an `OptionChain` per step, so
/// this gate drives the production strategy directly rather than a probe.
#[test]
fn test_replay_loop_steady_state_body_allocates_zero() {
    let samples = sample_over_movefeed(real_iron_condor_adapter());
    let delta = tail_delta(&samples);
    assert_eq!(
        delta,
        0,
        "PB-1 violated: the real IronCondor adapter allocated {delta} event(s) across the \
         steady-state tail (step {K}..{LAST}); the per-step body must not allocate. \
         Samples: K={:?} last={:?}",
        samples.get(K),
        samples.get(LAST),
    );
}

/// NEGATIVE (proves the gate bites). A deliberate per-step allocation inside the
/// step body makes the tail delta non-zero, so a real regression would fail the
/// gate above. This test PASSES by confirming the bad case is detected.
#[test]
fn test_deliberate_per_step_allocation_makes_the_delta_nonzero() {
    let samples = sample_over_movefeed(BadStep { inner: SteadyHold });
    let delta = tail_delta(&samples);
    assert!(
        delta > 0,
        "the gate failed to detect a deliberate per-step allocation (tail delta was {delta})",
    );
    // One 64-byte Vec per step across the `LAST - K` steady-state steps.
    assert_eq!(
        delta,
        LAST - K,
        "one deliberate allocation per step is expected across the tail",
    );
}

/// FOCUSED (the #19 FOLD-THE-FIX assertion, at the point the defect used to
/// live). The real `IronCondor` adapter's `exits()` no longer rebuilds an
/// `OptionChain` per step — `underlying` is sourced from the snapshot scalar and
/// the `inner` reprice is deferred — so the exit seam allocates **zero** per
/// step. This measures the per-step delta both early in the tail (`K → K+1`) and
/// at its end (`LAST-1 → LAST`) and asserts each is exactly zero, INVERTING the
/// old diagnostic that asserted the (now removed) per-step chain-rebuild cost.
/// It is finer-grained than — and subsumed by — the whole-body gate above.
#[test]
fn test_iron_condor_exit_seam_allocates_zero_per_step() {
    let samples = sample_over_movefeed(real_iron_condor_adapter());

    // Per-step allocation early in the tail vs at its end.
    let (Some(&s_k), Some(&s_k1)) = (samples.get(K), samples.get(K + 1)) else {
        panic!("samples cover the warmup neighbourhood");
    };
    let (Some(&s_l1), Some(&s_l)) = (samples.get(LAST - 1), samples.get(LAST)) else {
        panic!("samples cover the tail end");
    };
    let (Some(per_step_early), Some(per_step_late)) =
        (s_k1.checked_sub(s_k), s_l.checked_sub(s_l1))
    else {
        panic!("cumulative allocation samples must be monotonically non-decreasing");
    };

    assert_eq!(
        per_step_early, 0,
        "post-#19 the real adapter's exit seam must allocate nothing per step \
         (early tail step allocated {per_step_early})",
    );
    assert_eq!(
        per_step_late, 0,
        "post-#19 the real adapter's exit seam must allocate nothing per step \
         (late tail step allocated {per_step_late})",
    );
}

// --- boundary documentation: the data-feed clone is step (a), not (b)-(g) -----

/// The production [`ParquetFeed`] clones the snapshot in `next()` — a data-layer
/// per-step cost (step (a), the snapshot handoff) that is **outside** PB-1's
/// (b)–(g) engine-body boundary. Documented here (not gated): the same zero-
/// activity engine probe run over the production feed shows exactly one extra
/// allocation per step vs the alloc-free feed, and that one allocation is the
/// feed's clone, not the engine.
#[test]
fn test_parquet_feed_snapshot_handoff_is_one_alloc_per_step_outside_the_body() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("create a tempdir for the fixture");
    };
    let path = dir.path().join("condor_chain.parquet");
    let feed = parquet_feed(&path);
    let cfg = config(&path);
    let execution = NaiveFill::new(cfg.slippage.clone(), cfg.fees);
    let samples: Rc<[Cell<usize>]> = (0..STEPS).map(|_| Cell::new(0)).collect();
    let strategy = SamplingStrategy::new(SteadyHold, &samples);
    let Ok(_run) = BacktestEngine::run(&cfg, feed, execution, strategy, "parquet-handoff") else {
        panic!("the sampled backtest runs to completion");
    };
    let samples: Vec<usize> = samples.iter().map(Cell::get).collect();
    let delta = tail_delta(&samples);
    // Exactly the per-step snapshot clone: one allocation per tail step.
    assert_eq!(
        delta,
        LAST - K,
        "ParquetFeed::next is expected to clone one snapshot per step (data-layer, step (a))",
    );
}

/// A non-triggering exit policy so `on_end` is the sole closer and every step
/// exercises the exit seam's repricing path (matching `common::run_condor`).
fn non_triggering_exit() -> ExitPolicy {
    ExitPolicy::TimeSteps(1_000_000)
}

// --- realistic mode: the measured-ceiling budget gate (#127) ------------------

/// The realistic fill model's per-step allocation gate.
///
/// Realistic mode routes every order through the upstream
/// `option-chain-orderbook` matching engine, so its warm step **cannot** be
/// zero-allocation the way the naive step is: each per-step book reseed calls
/// `OptionOrderBook::add_limit_order_full`, which returns an owned `TradeResult`
/// whose `symbol` `String` heap-allocates upstream even when nothing crosses,
/// and the matching engine allocates internally per order. Gating realistic mode
/// at zero would be a lie, so it is gated at a **measured ceiling** instead:
/// the tail must stay under a per-step budget derived from a real measurement
/// plus a fixed headroom, and it must be **flat** (a leak grows; a steady state
/// does not).
///
/// The headroom exists to absorb upstream patch-level churn and the small
/// run-to-run jitter documented on `REALISTIC_GROWTH_NUMERATOR` below —
/// **never** a leak. If this gate starts failing, the answer is to find the new
/// allocation, not to raise the budget.
#[cfg(feature = "orderbook")]
mod realistic {
    use super::{
        BacktestConfig, BacktestError, ChainContext, K, LAST, OrderCommand, Path, STEPS, Strategy,
        common, real_iron_condor_adapter, sample_with, tail_delta,
    };
    use ironcondor::{ExecutionMode, RealisticFill};

    /// Per-warm-step allocation-event ceiling for realistic mode.
    ///
    /// Derived from a measurement on 2026-09-04 (macOS 26.6.2 arm64, rustc
    /// 1.97.0; lockfile identity `option-chain-orderbook` 0.10.0 — since #128
    /// 0.11.0, whose published `src/` tree is byte-identical, over the same
    /// `orderbook-rs` 0.12.1 / `pricelevel` 0.9.1, so the measurement carries
    /// over; 4-leg condor, seed 42): the steady-state tail
    /// delta over the 55 warm steps `K..LAST` was **30 969–31 019 events observed
    /// over 24 runs**, i.e. **≈ 564 events per warm step**. The budget is that
    /// measured count plus 25 % headroom, rounded up — the same
    /// factor-with-headroom discipline as `scripts/bench_gate.sh`. The
    /// measurement therefore sits at ~80 % of this ceiling. CI runs the gate on
    /// `ubuntu-24.04`, which has **not** been measured separately; the counts are
    /// code-path determined and the headroom is expected to cover the difference.
    ///
    /// **564 is the window average, not the plateau.** `K = 8` is shared with the
    /// naive gate and sits inside the book's own warm-up head: the per-step count
    /// starts at ≈ 662 events at step 1 and only settles to a ≈ 555 plateau from
    /// about step 37. Including that head makes the average conservative for a
    /// ceiling, and it is also what produces the window asymmetry the linearity
    /// check has to account for (see [`REALISTIC_GROWTH_NUMERATOR`]).
    ///
    /// **The number is a function of the fixture — re-measure if either input
    /// moves.** The per-step cost is one liquidity reseed ladder per contract per
    /// side: `common::condor_rows` puts 4 quotes on the tape and
    /// `LiquidityProfile::depth_levels` defaults to 5, so the plan is nominally
    /// `4 × 2 × (1 + 5) = 48` reseed orders plus 48 cancels per step (a ladder
    /// stops early if a level's size rounds to zero). Changing the fixture's
    /// quote universe or the default profile changes the measured count, and this
    /// budget must then be re-measured rather than scaled.
    const REALISTIC_WARM_STEP_BUDGET: usize = 705;

    /// Growth tolerance numerator: the second half-window may exceed the first
    /// by at most [`REALISTIC_GROWTH_NUMERATOR`] /
    /// [`REALISTIC_GROWTH_DENOMINATOR`] of the first window.
    ///
    /// **One-sided on purpose.** A *shrinking* second window is never a leak, so
    /// only growth is bounded. Across eight runs the second half measured
    /// 2.47 %–2.77 % **below** the first — the book's warm-up decay, the per-step
    /// count falling from ≈ 660 early to a ≈ 555 plateau — so the observed value
    /// of `second - first` is always negative and a two-sided check would both
    /// hide a leak behind that decay and fire falsely wherever the decay runs
    /// longer.
    ///
    /// **Why 2 % and not less.** The only noise the one-sided form has to absorb
    /// is the run-to-run jitter of a single window, measured at ≤ 0.25 % (the
    /// upstream allocation count is not reproducible run to run: recorded
    /// per-step samples move by a median of 6 and at most 14 events, yet the
    /// 55-step sum moves far less, so the deviations largely cancel — see
    /// [`REALISTIC_WARM_STEP_BUDGET`] and `BENCH.md`), plus any residual reversal
    /// of the decay profile on other hardware. 2 % is 8× that jitter — enough
    /// that CI cannot flake — while
    /// still firing on ≈ 11 extra events per step in the second half. It is a
    /// 4× tightening of the effective leak sensitivity versus a two-sided 5 %
    /// form, which would need ≈ 7.7 % of growth to fire at all.
    const REALISTIC_GROWTH_NUMERATOR: usize = 2;
    /// Denominator of the growth tolerance; see [`REALISTIC_GROWTH_NUMERATOR`].
    const REALISTIC_GROWTH_DENOMINATOR: usize = 100;

    /// Deliberate per-step allocations injected by [`BadStepMany`].
    ///
    /// Must exceed the budget's headroom (`705 - 564 = 141` events/step) by a
    /// margin far wider than the measured run-to-run jitter, so the negative
    /// test can never flip on upstream churn. 400 is ~2.8× that headroom.
    const EXCESS_ALLOCS_PER_STEP: usize = 400;

    /// Slope of [`BadStepGrowing`]: it allocates `GROWING_ALLOCS_SLOPE *
    /// step_index` boxes at step `step_index`, so the injected cost **grows with
    /// step count** — the shape of a real leak, as opposed to [`BadStepMany`]'s
    /// constant-per-step excess.
    ///
    /// Sized so the probe clears the tolerance by construction, not by luck. The
    /// two 27-step windows receive `slope * 567` and `slope * 1323` injected
    /// events (the sums of their step indices), a difference of `slope * 756`. At
    /// slope 8 that is 6048 injected events of growth, which the baseline decay
    /// nets down to a measured **5635–5645** against a tolerance of **399–400**
    /// — roughly 14× the margin, so the probe stays decisive even if the upstream
    /// baseline shifts.
    const GROWING_ALLOCS_SLOPE: usize = 8;

    /// The naive [`common::condor_config`] fixture switched to realistic fills —
    /// the only difference between the two gates' configuration.
    fn realistic_config(path: &Path) -> BacktestConfig {
        let mut config = common::condor_config(path, 42);
        config.mode = ExecutionMode::Realistic;
        config
    }

    /// The realistic analogue of `sample_over_movefeed`: the same [`MoveFeed`],
    /// the same `K` / `STEPS`, the same [`SamplingStrategy`], with
    /// [`RealisticFill`] built exactly as the realistic golden builds it
    /// (`tests/bundle_golden.rs`). Every strike's book is first seeded at step 0,
    /// which is outside the measured window `K..LAST` by construction.
    ///
    /// [`MoveFeed`]: super::MoveFeed
    /// [`SamplingStrategy`]: super::SamplingStrategy
    fn sample_over_movefeed_realistic<S: Strategy>(inner: S) -> Vec<usize> {
        sample_with(
            realistic_config,
            |config| {
                RealisticFill::with_liquidity_profile(
                    config.fees,
                    config.marketable_cap_ticks,
                    config.seed,
                    config.liquidity_profile,
                )
            },
            inner,
        )
    }

    /// Wraps a strategy and allocates [`EXCESS_ALLOCS_PER_STEP`] boxes inside the
    /// step body, keeping them alive for the whole step — the injected regression
    /// the negative test proves the realistic ceiling catches. The backing `Vec`
    /// is pre-sized once at construction and cleared in place each step, so the
    /// per-step cost is exactly the boxes and nothing else.
    struct BadStepMany<S: Strategy> {
        inner: S,
        // `clippy::vec_box` is exactly backwards here: the `Box` IS the point.
        // A `Vec<u64>` would be one allocation for the whole step; this test
        // needs EXCESS_ALLOCS_PER_STEP separate allocation events.
        #[allow(clippy::vec_box)]
        scratch: Vec<Box<usize>>,
    }

    impl<S: Strategy> BadStepMany<S> {
        fn new(inner: S) -> Self {
            Self {
                inner,
                scratch: Vec::with_capacity(EXCESS_ALLOCS_PER_STEP),
            }
        }
    }

    impl<S: Strategy> Strategy for BadStepMany<S> {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.inner.on_start(ctx, out)
        }
        fn exits(
            &mut self,
            ctx: &ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.inner.exits(ctx, out)
        }
        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.scratch.clear();
            for value in 0..EXCESS_ALLOCS_PER_STEP {
                self.scratch.push(Box::new(value));
            }
            std::hint::black_box(&self.scratch);
            self.inner.on_snapshot(ctx, out)
        }
        fn on_end(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.inner.on_end(ctx, out)
        }
    }

    /// THE REALISTIC GATE (build-failing). Driving the real
    /// `OptStratAdapter<IronCondor>` with [`RealisticFill`] over the alloc-free
    /// feed, the steady-state tail must stay under
    /// [`REALISTIC_WARM_STEP_BUDGET`] allocation events per warm step.
    #[test]
    fn test_realistic_warm_step_allocation_stays_under_budget() {
        let samples = sample_over_movefeed_realistic(real_iron_condor_adapter());
        let delta = tail_delta(&samples);
        let warm_steps = LAST - K;
        let Some(ceiling) = REALISTIC_WARM_STEP_BUDGET.checked_mul(warm_steps) else {
            panic!("the realistic ceiling fits in usize");
        };
        assert!(
            delta <= ceiling,
            "realistic per-step allocation budget exceeded: {delta} event(s) across the \
             steady-state tail (step {K}..{LAST}, {warm_steps} warm steps) vs a ceiling of \
             {ceiling} ({REALISTIC_WARM_STEP_BUDGET}/step). The headroom absorbs upstream \
             patch-level churn, never a leak — find the new allocation, do not raise the budget.",
        );
    }

    /// Split the warm window into two **disjoint windows of identical length**
    /// and return `(window_len, first, second)` — the allocation-event deltas
    /// over each. Equal lengths so the counts compare directly, with no per-step
    /// division and no rounding.
    fn half_windows(samples: &[usize]) -> (usize, usize, usize) {
        let half = (LAST - K) / 2;
        let (Some(&first_lo), Some(&first_hi), Some(&second_lo), Some(&second_hi)) = (
            samples.get(K),
            samples.get(K + half),
            samples.get(LAST - half),
            samples.get(LAST),
        ) else {
            panic!("samples cover both halves of the warm window");
        };
        let (Some(first), Some(second)) = (
            first_hi.checked_sub(first_lo),
            second_hi.checked_sub(second_lo),
        ) else {
            panic!("cumulative allocation samples must be monotonically non-decreasing");
        };
        (half, first, second)
    }

    /// The one-sided growth budget the second window must stay within, as a
    /// fraction of the first; see [`REALISTIC_GROWTH_NUMERATOR`].
    fn growth_tolerance(first: usize) -> usize {
        let Some(tolerance) = first
            .checked_mul(REALISTIC_GROWTH_NUMERATOR)
            .map(|scaled| scaled.div_ceil(REALISTIC_GROWTH_DENOMINATOR))
        else {
            panic!("the growth tolerance fits in usize");
        };
        tolerance
    }

    /// LINEARITY. The tail is a steady state, not a slow leak: the allocation
    /// delta over the second half of the warm window must not **exceed** the
    /// delta over the first half by more than [`growth_tolerance`]. The check is
    /// **one-sided** — a leak grows step over step and makes the second window
    /// strictly larger, while a smaller second window (which is what the book's
    /// warm-up decay actually produces) can never be a leak, so bounding the
    /// downward direction would only manufacture false failures.
    #[test]
    fn test_realistic_warm_step_allocation_does_not_grow_across_the_tail() {
        let samples = sample_over_movefeed_realistic(real_iron_condor_adapter());
        let (half, first, second) = half_windows(&samples);
        let tolerance = growth_tolerance(first);
        let growth = second.saturating_sub(first);
        assert!(
            growth <= tolerance,
            "realistic per-step allocation grows across the tail: the first {half}-step window \
             allocated {first} event(s) and the second {second}, a growth of {growth} against a \
             tolerance of {tolerance}. A growing second window means a per-step leak.",
        );
    }

    /// NEGATIVE (proves the realistic ceiling bites). Injecting
    /// [`EXCESS_ALLOCS_PER_STEP`] allocations into the step body pushes the tail
    /// delta above the ceiling, so a real regression of that size would fail the
    /// gate above. This test PASSES by confirming the bad case is detected.
    #[test]
    fn test_deliberate_per_step_allocations_exceed_the_realistic_budget() {
        let samples = sample_over_movefeed_realistic(BadStepMany::new(real_iron_condor_adapter()));
        let delta = tail_delta(&samples);
        let warm_steps = LAST - K;
        let Some(ceiling) = REALISTIC_WARM_STEP_BUDGET.checked_mul(warm_steps) else {
            panic!("the realistic ceiling fits in usize");
        };
        assert!(
            delta > ceiling,
            "the realistic gate failed to detect {EXCESS_ALLOCS_PER_STEP} deliberate per-step \
             allocations (tail delta {delta}, ceiling {ceiling})",
        );
    }

    /// Wraps a strategy and allocates a number of boxes that **grows with the
    /// step index** (`GROWING_ALLOCS_SLOPE * step_index`), the shape of a real
    /// per-step leak: the ceiling alone would not necessarily catch it early,
    /// but the second half-window allocates markedly more than the first. The
    /// backing `Vec` is pre-sized for the largest step once at construction and
    /// cleared in place, so the per-step cost is exactly the boxes.
    struct BadStepGrowing<S: Strategy> {
        inner: S,
        step: usize,
        // See BadStepMany: the Box is the point, one allocation event each.
        #[allow(clippy::vec_box)]
        scratch: Vec<Box<usize>>,
    }

    impl<S: Strategy> BadStepGrowing<S> {
        fn new(inner: S) -> Self {
            let capacity = GROWING_ALLOCS_SLOPE.saturating_mul(STEPS as usize);
            Self {
                inner,
                step: 0,
                scratch: Vec::with_capacity(capacity),
            }
        }
    }

    impl<S: Strategy> Strategy for BadStepGrowing<S> {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.inner.on_start(ctx, out)
        }
        fn exits(
            &mut self,
            ctx: &ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.inner.exits(ctx, out)
        }
        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.scratch.clear();
            let injected = GROWING_ALLOCS_SLOPE.saturating_mul(self.step);
            for value in 0..injected {
                self.scratch.push(Box::new(value));
            }
            std::hint::black_box(&self.scratch);
            self.step = self.step.saturating_add(1);
            self.inner.on_snapshot(ctx, out)
        }
        fn on_end(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), BacktestError> {
            self.inner.on_end(ctx, out)
        }
    }

    /// NEGATIVE (proves the LINEARITY assertion bites). An allocation cost that
    /// grows with the step index — a leak's shape — makes the second half-window
    /// exceed the first by far more than [`growth_tolerance`], so the one-sided
    /// check above would fail on a real leak. This test PASSES by confirming the
    /// bad case is detected.
    #[test]
    fn test_growing_per_step_allocations_break_the_realistic_linearity_check() {
        let samples =
            sample_over_movefeed_realistic(BadStepGrowing::new(real_iron_condor_adapter()));
        let (half, first, second) = half_windows(&samples);
        let tolerance = growth_tolerance(first);
        let growth = second.saturating_sub(first);
        assert!(
            growth > tolerance,
            "the linearity check failed to detect a per-step allocation growing at \
             {GROWING_ALLOCS_SLOPE} event(s) per step index: the first {half}-step window \
             allocated {first} event(s) and the second {second} (growth {growth}, tolerance \
             {tolerance})",
        );
    }
}
