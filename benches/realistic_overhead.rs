//! Realistic-vs-naive overhead A/B — hot path **H2**/PB-3 (issue #29).
//!
//! Runs the **same fixed condor scenario** ([`common`]) through
//! [`ironcondor::run_backtest`] in **both** execution modes — naive (mid +
//! configured slippage) and realistic (orders routed through the seeded
//! `option-chain-orderbook` matching engine) — back to back in **one process**,
//! and reports the per-step latency percentiles of each plus the dimensionless
//! **realistic / naive overhead ratio** at p50 and p99. The ratio is the PB-3
//! deliverable ([docs/07 §3](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)):
//! realistic mode is honestly slower, and *how much* slower is measured per
//! release, never assumed.
//!
//! # Why one bench measures both modes
//!
//! The overhead ratio is only meaningful if numerator and denominator are taken
//! under **identical conditions**. Measuring naive and realistic in the same
//! bench invocation — same machine, same allocator state, same thermal regime,
//! same fixed tape — makes the ratio a property of the two fill models, not of
//! two separate runs on possibly different hardware. This is also what makes the
//! ratio **hardware-portable**: it cancels the runner's absolute clock speed, so
//! the CI gate (`scripts/bench_gate.sh`) can compare it against a committed band
//! on a Linux runner even though `BENCH.md`'s absolute numbers are Apple M4 Max.
//!
//! # Feature gate
//!
//! Realistic mode needs the matching engine, which only compiles behind the
//! `orderbook` feature; this whole bench target is `required-features =
//! ["orderbook"]` in `Cargo.toml`, so the default build never sees it. Build /
//! run it with `cargo bench --features orderbook --bench realistic_overhead`.
//!
//! # Bench-hdr convention, warmup, coordinated omission
//!
//! Each mode records per-run latency into its own [`hdrhistogram::Histogram`]
//! and reports p50 / p99 / p99.9 / p99.99 via [`common::report`] — not
//! criterion's mean ([docs/07 §5](../docs/07-performance-and-security.md#5-benchmark-methodology)).
//! Each mode is preceded by an explicit [`common::WARMUP_RUNS`] un-recorded
//! warmup, on top of criterion's own. Coordinated omission **does not apply**:
//! this is a closed-loop, back-to-back throughput bench with no external arrival
//! schedule, so the histogram records raw latency (`record`, not
//! `record_correct`).
//!
//! # Per-step allocation — honest note
//!
//! Realistic mode's warm step is **not** zero-allocation and is deliberately
//! **not** under the PB-1 zero-alloc gate (`tests/zero_alloc.rs` gates the naive
//! path only). The per-step book refresh (#25) calls
//! `OptionOrderBook::add_limit_order_full` for every reseed level, and each call
//! returns an owned `TradeResult` whose `symbol` `String` heap-allocates upstream
//! even when nothing crosses — a bounded per-step cost the overhead ratio here
//! already reflects. See `BENCH.md` PB-3 for the quantified allocation note and
//! `src/execution/realistic.rs` for the upstream-capture-API detail.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use std::hint::black_box;

use ironcondor::{ExecutionMode, run_backtest};

mod common;

/// Run one mode's warmup + measured phase into `hist`, under criterion's
/// scheduling. `label` names the criterion `bench_function` inside the group.
fn measure_mode(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    label: &str,
    mode: ExecutionMode,
    hist: &RefCell<Histogram<u64>>,
) {
    let dir = tempfile::tempdir().expect("create a tempdir for the bench fixture");
    let path = dir.path().join("condor_chain.parquet");
    common::write_parquet(&path, &common::condor_rows())
        .expect("write the canonical bench fixture");
    let config = common::condor_config(&path, mode);
    let spec = common::iron_condor_spec();

    // Explicit warmup — un-recorded, on top of criterion's own warmup phase.
    for _ in 0..common::WARMUP_RUNS {
        let run = run_backtest(&config, &spec, common::exit_policy()).expect("warmup run succeeds");
        let _ = black_box(run);
    }

    group.bench_function(label, |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let run = run_backtest(&config, &spec, common::exit_policy())
                    .expect("measured run_backtest succeeds");
                let elapsed = start.elapsed();
                let _ = black_box(run);
                total += elapsed;
                // Closed-loop: record raw latency (no coordinated-omission
                // correction — see the module disclosure).
                let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
                hist.borrow_mut().record(ns).ok();
            }
            total
        });
    });
}

/// Print the dimensionless realistic / naive overhead ratio at p50 and p99, plus
/// machine-readable lines the CI gate (`scripts/bench_gate.sh`) and `BENCH.md`
/// parse. The ratio is per-step-latency(realistic) / per-step-latency(naive);
/// per-run and per-step ratios are equal (the step count is the same fixed tape),
/// so the ratio is scale-free.
fn report_ratio(naive: &Histogram<u64>, realistic: &Histogram<u64>) {
    let ratio = |q: f64| {
        let n = common::per_step_ns(naive, q);
        let r = common::per_step_ns(realistic, q);
        if n == 0.0 { f64::INFINITY } else { r / n }
    };
    let r50 = ratio(0.50);
    let r99 = ratio(0.99);

    println!("\n=== realistic_overhead — A/B ratio (PB-3, docs/07 §3) ===");
    println!(
        "naive     per-step p50={:.1}ns  p99={:.1}ns",
        common::per_step_ns(naive, 0.50),
        common::per_step_ns(naive, 0.99),
    );
    println!(
        "realistic per-step p50={:.1}ns  p99={:.1}ns",
        common::per_step_ns(realistic, 0.50),
        common::per_step_ns(realistic, 0.99),
    );
    println!("overhead ratio (realistic / naive): p50={r50:.2}x  p99={r99:.2}x");
    // Machine-readable lines — the CI gate greps RATIO_P50; BENCH.md records all.
    println!("RATIO_P50={r50:.4}");
    println!("RATIO_P99={r99:.4}");
    println!(
        "NAIVE_PER_STEP_NS_P50={:.1}",
        common::per_step_ns(naive, 0.50)
    );
    println!(
        "REALISTIC_PER_STEP_NS_P50={:.1}",
        common::per_step_ns(realistic, 0.50)
    );
    println!("=========================================================\n");
}

/// The realistic-vs-naive overhead A/B benchmark.
fn bench_realistic_overhead(c: &mut Criterion) {
    let naive_hist = RefCell::new(Histogram::<u64>::new(3).expect("build the naive histogram"));
    let realistic_hist =
        RefCell::new(Histogram::<u64>::new(3).expect("build the realistic histogram"));

    let mut group = c.benchmark_group("realistic_overhead");
    // Each iteration processes STEPS steps → criterion also reports steps/sec.
    group.throughput(Throughput::Elements(u64::from(common::STEPS)));
    measure_mode(&mut group, "naive", ExecutionMode::Naive, &naive_hist);
    measure_mode(
        &mut group,
        "realistic",
        ExecutionMode::Realistic,
        &realistic_hist,
    );
    group.finish();

    common::report("realistic_overhead/naive", &naive_hist.borrow());
    common::report("realistic_overhead/realistic", &realistic_hist.borrow());
    report_ratio(&naive_hist.borrow(), &realistic_hist.borrow());
}

criterion_group!(benches, bench_realistic_overhead);
criterion_main!(benches);
