//! PB-5 bundle-writer scaling evidence (issue #37).
//!
//! The companion to the `bundle_write` criterion bench. Where the bench lands
//! the **optimized time** numbers, this test binary lands the **peak-memory**
//! numbers and the machine-checkable **linear-not-quadratic** assertions that
//! are the PB-5 evidence
//! ([docs/07 §3](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite),
//! [docs/07 §5](../docs/07-performance-and-security.md#5-benchmark-methodology)).
//!
//! # What PB-5 asks and how this measures it
//!
//! PB-5: the writer is an **O(rows) single-pass** encode — write time and peak
//! memory grow **linearly** with the number of rows, **not quadratically**. The
//! measurement clause is exactly "assert write time and peak RSS grow linearly,
//! not quadratically" ([docs/07 §3](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
//!
//! - **Time:** wall-clock of one [`write_bundle`] call per size (debug build —
//!   the *scaling ratio* is build-profile independent; the absolute optimized
//!   numbers are the bench's job). Per-row cost stays ~flat for O(rows).
//! - **Peak memory:** a counting `GlobalAlloc` tracks process **outstanding
//!   bytes** and their high-water. We reset the high-water to the current
//!   outstanding total **after** the run + output dir are already allocated,
//!   then call [`write_bundle`], so the measured delta is the **writer's own**
//!   incremental peak — its footprint on top of the run already resident in RAM.
//!   A byte count is build-profile independent, so it is authoritative here.
//!
//! # Honest reading of the memory result
//!
//! The writer sorts each table into a fully-materialised `Vec` of wire rows
//! before streaming row-group batches, so its peak is **O(rows)** — it grows
//! linearly with run length, satisfying the "linear, not quadratic" measurement
//! clause, but it is **not** flat/row-group-bounded (the stricter budget prose).
//! The per-batch Arrow encode buffer *is* bounded by `WRITE_BATCH_ROWS`; the
//! sort materialisation is not. `BENCH.md` records this distinction plainly.
//!
//! # Test-only `unsafe`
//!
//! `ironcondor` itself is `#![forbid(unsafe_code)]`. A counting `GlobalAlloc` is
//! unavoidably an `unsafe impl`, but this is a **separate test crate root** that
//! does not inherit the library's `forbid`; the `unsafe` forwards verbatim to
//! `System` after a non-allocating atomic bump and never ships.

#![allow(missing_docs)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use ironcondor::{
    AttributionSubstrate, BacktestConfig, BacktestRun, Cents, ContractKey, DataSourceSpec,
    EquityPoint, ExecutionMode, FeeSchedule, Fill, FillRecord, GreeksAttributionRow,
    IronCondorSpec, LiquidityProfile, PositionSnapshot, PriceCents, Quantity, ResourceLimits,
    SimTime, SlippageModel, StepIndex, StrategySpec, Underlying, write_bundle,
};
use optionstratlib::backtesting::BacktestResult;
use optionstratlib::{ExpirationDate, OptionStyle, Side};
use rust_decimal::Decimal;

// --- the peak-tracking allocator (test-only unsafe; see the module docs) -----

/// Process outstanding allocated bytes (net of frees).
static CURRENT: AtomicUsize = AtomicUsize::new(0);
/// High-water mark of [`CURRENT`] since the last reset.
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// A `System`-backed allocator that tracks outstanding bytes and their peak. The
/// default `GlobalAlloc::realloc` routes through `alloc` + `dealloc`, so a
/// `realloc` is accounted correctly with no extra override.
struct TrackingAllocator;

// SAFETY (test-only): every method forwards verbatim to `System`; the only added
// work is non-allocating atomic bookkeeping. This lives exclusively in the test
// binary — the library stays `#![forbid(unsafe_code)]`.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let cur = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(cur, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOC: TrackingAllocator = TrackingAllocator;

/// Reset the high-water mark to the current outstanding total and return that
/// baseline — call it after the run is already resident so the subsequent peak
/// delta is the writer's own footprint.
fn reset_peak() -> usize {
    let base = CURRENT.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    base
}

/// The high-water mark since the last [`reset_peak`].
fn peak() -> usize {
    PEAK.load(Ordering::Relaxed)
}

// --- the run fixture (four condor legs, `steps` long) ------------------------

const LEGS: usize = 4;
const TS0: i64 = 1_750_291_200_000_000_000;
const DT_NS: i64 = 60_000_000_000;
const EXPIRY: i64 = TS0 + 30 * 86_400_000_000_000;

/// Total rows a `steps`-length run writes across the four tables.
fn total_rows(steps: u32) -> u64 {
    u64::from(steps) * (2 + LEGS as u64) + LEGS as u64
}

fn legs() -> [(u64, OptionStyle, Side); LEGS] {
    [
        (510_000, OptionStyle::Call, Side::Short),
        (520_000, OptionStyle::Call, Side::Long),
        (490_000, OptionStyle::Put, Side::Short),
        (480_000, OptionStyle::Put, Side::Long),
    ]
}

fn leg_key(strike_cents: u64, style: OptionStyle) -> ContractKey {
    ContractKey {
        underlying: Underlying::new("SPX").expect("SPX is a valid underlying"),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        strike: PriceCents::new(strike_cents),
        style,
    }
}

fn build_run(steps: u32) -> BacktestRun {
    let n = steps as usize;
    let leg_specs = legs();
    let mut equity_curve = Vec::with_capacity(n);
    let mut greeks_attribution = Vec::with_capacity(n);
    let mut positions = Vec::with_capacity(n * LEGS);

    for step in 0..steps {
        let ts = TS0 + i64::from(step) * DT_NS;
        equity_curve.push(EquityPoint::new(
            step,
            ts,
            10_020_000 - i64::from(step),
            -20_000,
            10_000_000 - i64::from(step),
            0.0,
        ));
        greeks_attribution.push(GreeksAttributionRow::new(
            step, ts, 12, -7, 3, 40, 65, 1_935,
        ));
        let open_at_end = step == steps - 1;
        for (leg_id, (strike_cents, style, side)) in leg_specs.into_iter().enumerate() {
            positions.push(PositionSnapshot {
                step,
                ts_ns: ts,
                position_id: leg_id as u64,
                trade_id: 1,
                contract: leg_key(strike_cents, style),
                side,
                quantity: 1,
                avg_price_cents: strike_cents,
                mark_cents: strike_cents,
                unrealized_cents: 500,
                stale_mark: false,
                exit_reason: None,
                open_at_end,
            });
        }
    }

    let mut fills = Vec::with_capacity(LEGS);
    for (order, (strike_cents, style, side)) in leg_specs.into_iter().enumerate() {
        fills.push(FillRecord {
            fill: Fill {
                ts: SimTime::new(TS0),
                step: StepIndex::new(0),
                contract: leg_key(strike_cents, style),
                side,
                quantity: Quantity::new(1).expect("1 is a valid quantity"),
                price: PriceCents::new(strike_cents),
                fees: Cents::new(65),
                slippage: Cents::new(0),
                mode: ExecutionMode::Naive,
            },
            trade_id: 1,
            position_id: order as u64,
            order_id: order as u64,
            fill_seq: 0,
        });
    }

    BacktestRun {
        result: BacktestResult::default(),
        equity_curve,
        open_at_end: Vec::new(),
        trade_log: Vec::new(),
        attribution_substrate: AttributionSubstrate::default(),
        greeks_attribution,
        fills,
        positions,
        data_source: DataSourceSpec::Parquet {
            path: "chains/spx.parquet".to_string(),
            sha256: "bench-tape".to_string(),
        },
        data_identity: "bench-tape".to_string(),
    }
}

fn strategy_spec() -> StrategySpec {
    StrategySpec::IronCondor(IronCondorSpec {
        underlying: Underlying::new("SPX").expect("SPX is a valid underlying"),
        underlying_price: PriceCents::new(500_000),
        short_call_strike: PriceCents::new(510_000),
        short_put_strike: PriceCents::new(490_000),
        long_call_strike: PriceCents::new(520_000),
        long_put_strike: PriceCents::new(480_000),
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
        implied_volatility: Decimal::new(20, 2),
        risk_free_rate: Decimal::new(5, 2),
        dividend_yield: Decimal::ZERO,
        quantity: Quantity::new(1).expect("1 is a valid quantity"),
        premium_short_call: PriceCents::new(2_000),
        premium_short_put: PriceCents::new(1_800),
        premium_long_call: PriceCents::new(800),
        premium_long_put: PriceCents::new(700),
        open_fee: PriceCents::new(65),
        close_fee: PriceCents::new(65),
    })
}

fn config(output_dir: &Path) -> BacktestConfig {
    BacktestConfig {
        data_source: DataSourceSpec::Parquet {
            path: "chains/spx.parquet".to_string(),
            sha256: "bench-tape".to_string(),
        },
        mode: ExecutionMode::Naive,
        seed: 42,
        initial_capital: 10_000_000,
        fees: FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        },
        slippage: SlippageModel::None,
        marketable_cap_ticks: 10,
        liquidity_profile: LiquidityProfile::default(),
        limits: ResourceLimits::default(),
        output_dir: output_dir.to_path_buf(),
        overwrite: true,
    }
}

/// One size's measured point.
struct Point {
    steps: u32,
    total_rows: u64,
    write_ns: u128,
    peak_bytes: usize,
}

impl Point {
    fn ns_per_row(&self) -> f64 {
        if self.total_rows == 0 {
            0.0
        } else {
            self.write_ns as f64 / self.total_rows as f64
        }
    }

    fn bytes_per_row(&self) -> f64 {
        if self.total_rows == 0 {
            0.0
        } else {
            self.peak_bytes as f64 / self.total_rows as f64
        }
    }
}

/// Build a `steps`-length run, then measure one `write_bundle`: its wall-time
/// and its incremental peak allocation high-water (the writer's footprint over
/// the already-resident run).
fn measure(steps: u32) -> Point {
    let run = build_run(steps);
    let strategy = strategy_spec();
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = config(dir.path());

    // Warm the write path once (allocator arenas, file creation) so the measured
    // write is at steady state; overwrite = true republishes the same run_id.
    write_bundle(&run, &cfg, &strategy).expect("warmup write succeeds");

    // Reset the high-water to the current outstanding total (run + dir already
    // resident) so the delta below is the writer's own peak.
    let base = reset_peak();
    let start = Instant::now();
    let published = write_bundle(&run, &cfg, &strategy).expect("measured write succeeds");
    let write_ns = start.elapsed().as_nanos();
    let peak_bytes = peak().saturating_sub(base);

    assert!(published.join("manifest.json").is_file());
    drop(dir);

    Point {
        steps,
        total_rows: total_rows(steps),
        write_ns,
        peak_bytes,
    }
}

fn print_table(title: &str, points: &[Point]) {
    println!("\n=== {title} ===");
    println!(
        "{:>8} {:>10} {:>12} {:>10} {:>12} {:>12}",
        "steps", "total", "write_ms", "ns/row", "peak_KiB", "bytes/row"
    );
    for p in points {
        println!(
            "{:>8} {:>10} {:>12.2} {:>10.2} {:>12} {:>12.1}",
            p.steps,
            p.total_rows,
            p.write_ns as f64 / 1_000_000.0,
            p.ns_per_row(),
            p.peak_bytes / 1024,
            p.bytes_per_row(),
        );
    }
    println!("========================================================\n");
}

/// PB-5 evidence — CI-light. Over a 16x row range, asserts that BOTH the per-row
/// write time and the per-row peak memory stay within a tolerance band (they do
/// NOT grow with size), i.e. the writer is O(rows) linear, not O(rows^2)
/// quadratic. Kept small so it runs in the standard test suite.
#[test]
fn pb5_writer_scales_linearly_not_quadratically() {
    // positions rows: 2048 / 8192 / 32768; total rows: ~3k / 12k / 49k.
    let sizes = [512u32, 2_048, 8_192];
    let points: Vec<Point> = sizes.into_iter().map(measure).collect();
    print_table("PB-5 scaling (CI-light, debug build)", &points);

    let small = &points[0];
    let large = &points[points.len() - 1];
    let row_growth = large.total_rows as f64 / small.total_rows as f64;
    assert!(
        row_growth >= 8.0,
        "the row range must span >= 8x to separate linear from quadratic"
    );

    // Linear tolerance band: per-row cost may DROP as fixed per-write overhead
    // (run_id, manifest, four file opens) amortises over more rows, but must not
    // GROW materially — a quadratic writer's per-row cost would rise ~row_growth.
    const TIME_TOL: f64 = 4.0;
    const MEM_TOL: f64 = 4.0;

    let time_ratio = large.ns_per_row() / small.ns_per_row().max(f64::MIN_POSITIVE);
    let mem_ratio = large.bytes_per_row() / small.bytes_per_row().max(f64::MIN_POSITIVE);

    println!(
        "row_growth={row_growth:.1}x  time per-row ratio={time_ratio:.2}x (tol {TIME_TOL})  \
         mem per-row ratio={mem_ratio:.2}x (tol {MEM_TOL})"
    );

    assert!(
        time_ratio <= TIME_TOL,
        "write time is not linear: per-row cost grew {time_ratio:.2}x over a {row_growth:.1}x row \
         range (a quadratic writer would grow ~{row_growth:.0}x); PB-5 time bound violated"
    );
    assert!(
        mem_ratio <= MEM_TOL,
        "peak memory is not linear: per-row bytes grew {mem_ratio:.2}x over a {row_growth:.1}x row \
         range (a quadratic writer would grow ~{row_growth:.0}x); PB-5 memory bound violated"
    );

    // Sanity: the writer actually allocated a meaningful, positive footprint.
    assert!(
        large.peak_bytes > 0,
        "the writer must have a measurable peak footprint"
    );
}

/// The full-size record for `BENCH.md` — the standard 1k/8k/32k/128k-positions
/// sizes plus two larger sizes that push the dominant `positions` table past
/// ~1M rows, to observe whether the peak flattens (a hard row-group bound) or
/// keeps growing linearly (the in-memory sort dominates). Heavy (multi-GB,
/// seconds): `#[ignore]`d, run manually with
/// `cargo test --release --test bundle_write_pb5 -- --ignored --nocapture`.
#[test]
#[ignore = "heavy: builds multi-million-row runs; run manually to refresh BENCH.md"]
fn pb5_writer_full_size_record() {
    let sizes = [256u32, 2_048, 8_192, 32_768, 131_072, 262_144];
    let points: Vec<Point> = sizes.into_iter().map(measure).collect();
    print_table("PB-5 full-size record (positions = 4x steps)", &points);

    // Report the peak-memory scaling verdict across the whole range for BENCH.md.
    if let (Some(small), Some(large)) = (points.first(), points.last()) {
        let row_growth = large.total_rows as f64 / small.total_rows as f64;
        let mem_ratio = large.bytes_per_row() / small.bytes_per_row().max(f64::MIN_POSITIVE);
        let time_ratio = large.ns_per_row() / small.ns_per_row().max(f64::MIN_POSITIVE);
        println!(
            "FULL row_growth={row_growth:.0}x  time/row ratio={time_ratio:.2}x  \
             mem/row ratio={mem_ratio:.2}x (flat => O(rows) linear; growing => super-linear)"
        );
    }
}
