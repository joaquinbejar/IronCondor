//! Conversion-boundary scaling bench — hot path **H3**/PB-4 (issue #51).
//!
//! Times [`ironcondor::raw_quotes_to_snapshot`] — the single DTO-independent
//! validation core every feed shares (`src/data/convert.rs`) — over snapshots
//! of **growing contract count**, to validate PB-4: the conversion is an
//! **O(contracts) single pass per snapshot**, so its per-contract cost stays
//! ~flat across chain sizes and a **super-linear** regression is caught
//! ([docs/07 §3](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite),
//! [docs/03 §7](../docs/03-data-layer.md#7-chainresponse--optionchain-conversion)).
//!
//! # Why this function, and why read-only
//!
//! `raw_quotes_to_snapshot` is the boundary work a feed pays **once per
//! snapshot** at tape materialisation (the Parquet feed reuses this same core —
//! there is no second validation path). It rejects non-positive / non-tick
//! strikes, crossed quotes, and NaN analytics, derives the one deterministic
//! mid, resolves the expiry, and assembles the `BTreeMap<ContractKey,
//! QuoteView>`. `snapshot_to_option_chain` (the deferred-reprice sibling, not on
//! the per-step loop) is the same O(contracts) complexity but is **not** the
//! per-snapshot materialisation cost, so the gate tracks the core. The bench
//! only *calls* the public conversion API — it does not modify `convert.rs`.
//!
//! # Sizes — a 16x contract-count sweep
//!
//! The snapshots carry `512 / 2048 / 8192` unique contracts (one style, one
//! expiry, distinct tick-aligned strikes), a **16x** range. A genuinely
//! O(contracts) pass keeps a ~flat per-contract cost across that range; the
//! `BTreeMap` assembly adds only a mild `log(contracts)` factor (≈ 1.44x over
//! 16x), so a **flat-ish** per-contract ratio is expected and a **quadratic**
//! regression (per-contract cost rising ~16x) is cleanly separated from it. The
//! range is wide enough to expose super-linearity, small enough to stay quick.
//!
//! # Bench-hdr convention, warmup, coordinated omission
//!
//! Each size records its per-conversion wall-time into an
//! [`hdrhistogram::Histogram`] and reports p50 / p99 / p99.9 latency, the
//! derived per-contract cost, plus a cross-size **linear-scaling verdict**
//! (per-contract cost ~flat for O(contracts); rising for O(contracts^2))
//! ([docs/07 §5](../docs/07-performance-and-security.md#5-benchmark-methodology)).
//! An explicit [`WARMUP_CONVERSIONS`] un-recorded warmup precedes each size's
//! measured phase, on top of criterion's own. Coordinated omission **does not
//! apply**: this is a closed-loop, back-to-back throughput bench with no
//! external arrival schedule, so the histogram records raw latency (`record`,
//! not `record_correct`).
//!
//! # Determinism note
//!
//! `raw_quotes_to_snapshot` is a pure function of its inputs (no wall clock, no
//! RNG); the wall-clock timing lives entirely in this bench harness and never
//! enters the crate.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;

use ironcondor::{
    PriceCents, Quantity, RawQuote, SimTime, SnapshotMeta, StepIndex, Underlying,
    raw_quotes_to_snapshot,
};
use optionstratlib::{ExpirationDate, OptionStyle};

/// Contract counts for the growing snapshots — a 16x sweep spanning below and
/// well above a realistic chain, wide enough to expose super-linearity.
const CONTRACT_SIZES: [u32; 3] = [512, 2_048, 8_192];

/// Un-recorded warmup conversions before each size's measured phase.
const WARMUP_CONVERSIONS: usize = 32;

/// Tape anchor `ts_0` (ns since epoch, UTC) — matches the shared fixtures.
const TS0: i64 = 1_750_291_200_000_000_000;

/// The contracts' shared absolute expiry: `ts_0 + 30 days`.
const EXPIRY: i64 = TS0 + 30 * 86_400_000_000_000;

/// The tick grid every strike / bid / ask aligns to (5 cents), matching the
/// canonical bench fixture.
const TICK_CENTS: u64 = 5;

/// The snapshot-level metadata every size shares (only the quote count varies).
fn snapshot_meta() -> SnapshotMeta {
    SnapshotMeta {
        ts: SimTime::new(TS0),
        step: StepIndex::new(0),
        anchor_ts: SimTime::new(TS0),
        underlying: Underlying::new("SPX").expect("SPX is a valid underlying"),
        underlying_price: PriceCents::new(500_000),
        tick_size_cents: PriceCents::new(TICK_CENTS),
        contract_multiplier: 100,
    }
}

/// Build `n` unique, valid [`RawQuote`]s — one call per distinct tick-aligned
/// strike (`100_000 + i·500` cents, all multiples of the 5c tick), a constant
/// tick-aligned `bid <= ask`, and finite analytics. Distinct strikes keep every
/// `ContractKey` unique so the core's `BTreeMap` inserts all `n` without a
/// duplicate rejection.
fn raw_quotes(n: u32) -> Vec<RawQuote> {
    let mut quotes = Vec::with_capacity(n as usize);
    for i in 0..n {
        // Strike stays a multiple of the 5c tick and never 0.
        let strike = 100_000 + u64::from(i) * 500;
        quotes.push(RawQuote {
            expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(EXPIRY)),
            strike: PriceCents::new(strike),
            style: OptionStyle::Call,
            bid: PriceCents::new(1_000),
            ask: PriceCents::new(1_010),
            bid_size: Quantity::new(50).expect("50 is a valid quantity"),
            ask_size: Quantity::new(50).expect("50 is a valid quantity"),
            implied_volatility: 0.2,
            delta: 0.3,
            gamma: 0.01,
            theta: -0.05,
            vega: 0.1,
        });
    }
    quotes
}

/// The p50/p99/p99.9 conversion latency in ns as `f64` (saturating on `u64`).
fn p_ns(hist: &Histogram<u64>, quantile: f64) -> f64 {
    let v = hist.value_at_quantile(quantile);
    u32::try_from(v).map_or(f64::from(u32::MAX), f64::from)
}

/// Print the per-size summary + the cross-size linear-scaling verdict, plus the
/// machine-readable `PB4_*` lines the CI linearity gate parses.
fn report(rows: &[(u32, Histogram<u64>)]) {
    println!("\n=== conversion — PB-4 scaling (bench-hdr, docs/07 §5) ===");
    println!("function: raw_quotes_to_snapshot (the O(contracts) validation core)");
    println!("coordinated omission: N/A (closed-loop back-to-back; no arrival schedule)");
    println!(
        "{:>10} {:>10} {:>10} {:>10} {:>10} {:>16}",
        "contracts", "samples", "p50_us", "p99_us", "p99.9_us", "ns/contract"
    );

    let mut per_contract: Vec<(u32, f64)> = Vec::with_capacity(rows.len());
    for (contracts, hist) in rows {
        let p50 = p_ns(hist, 0.50);
        let n = f64::from(*contracts);
        let ns_per_contract = if n == 0.0 { 0.0 } else { p50 / n };
        per_contract.push((*contracts, ns_per_contract));
        println!(
            "{:>10} {:>10} {:>10.2} {:>10.2} {:>10.2} {:>16.3}",
            contracts,
            hist.len(),
            p50 / 1_000.0,
            p_ns(hist, 0.99) / 1_000.0,
            p_ns(hist, 0.999) / 1_000.0,
            ns_per_contract,
        );
    }

    // Linear-scaling verdict: an O(contracts) single-pass core keeps the
    // per-contract cost ~flat (a mild log(n) BTreeMap factor aside); an
    // O(contracts^2) core's per-contract cost rises ~with the contract growth.
    if let (Some(first), Some(last)) = (per_contract.first(), per_contract.last()) {
        let (small_n, small_cost) = *first;
        let (large_n, large_cost) = *last;
        let contract_growth = f64::from(large_n) / f64::from(small_n.max(1));
        let cost_ratio = if small_cost == 0.0 {
            f64::INFINITY
        } else {
            large_cost / small_cost
        };
        println!(
            "\ncontracts grew {contract_growth:.0}x (smallest -> largest); \
             per-contract cost ratio = {cost_ratio:.3}x"
        );
        println!(
            "verdict: {} (per-contract cost ~flat => O(contracts) linear; rising => O(contracts^2))",
            if cost_ratio <= 4.0 {
                "LINEAR (PB-4: MET)"
            } else {
                "NON-LINEAR (investigate)"
            }
        );
        // Machine-readable lines the CI linearity gate (#51) / BENCH.md parse.
        println!("PB4_PER_CONTRACT_NS_SMALL={small_cost:.4}");
        println!("PB4_PER_CONTRACT_NS_LARGE={large_cost:.4}");
        println!("PB4_CONVERT_COST_RATIO={cost_ratio:.4}");
        println!("PB4_CONTRACT_GROWTH={contract_growth:.4}");
    }
    println!("=========================================================\n");
}

/// The conversion-boundary scaling benchmark (H3/PB-4).
fn bench_conversion(c: &mut Criterion) {
    let meta = snapshot_meta();

    let mut group = c.benchmark_group("conversion");
    // The largest conversion dominates runtime; keep the sample budget modest.
    group.sample_size(50);

    let mut recorded: Vec<(u32, Histogram<u64>)> = Vec::with_capacity(CONTRACT_SIZES.len());
    for contracts in CONTRACT_SIZES {
        // --- setup (un-timed): build the raw quotes for this size ------------
        let quotes = raw_quotes(contracts);

        // Explicit warmup — un-recorded, on top of criterion's own.
        for _ in 0..WARMUP_CONVERSIONS {
            let snap = raw_quotes_to_snapshot(&meta, &quotes).expect("warmup conversion succeeds");
            let _ = black_box(snap);
        }

        let hist = RefCell::new(Histogram::<u64>::new(3).expect("build the conversion histogram"));
        group.throughput(Throughput::Elements(u64::from(contracts)));
        group.bench_with_input(
            BenchmarkId::new("raw_quotes_to_snapshot", contracts),
            &contracts,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = Instant::now();
                        let snap = raw_quotes_to_snapshot(&meta, &quotes)
                            .expect("measured conversion succeeds");
                        let elapsed = start.elapsed();
                        let _ = black_box(snap);
                        total += elapsed;
                        // Closed-loop: raw latency (no coordinated-omission correction).
                        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
                        hist.borrow_mut().record(ns).ok();
                    }
                    total
                });
            },
        );

        recorded.push((contracts, hist.into_inner()));
    }
    group.finish();

    report(&recorded);
}

criterion_group!(benches, bench_conversion);
criterion_main!(benches);
