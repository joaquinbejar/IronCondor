//! Naive-mode throughput baseline — hot path **H1**/PB-2 (issue #18).
//!
//! Measures the **full** [`ironcondor::run_backtest`] over a canonical Parquet
//! iron-condor chain: Parquet feed open + parse → replay loop → naive fills →
//! mark-to-market ledger → summary metrics, single strategy, single core. This
//! is the project's **first published number**; its measured p50 / p99 / p99.9
//! are recorded in `BENCH.md` and become the v0.1 baseline the #019 zero-alloc
//! gate and the #029 realistic-overhead A/B build on
//! ([docs/07 §3/§5/§6](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite)).
//!
//! The fixed scenario (the canonical condor tape, config, spec, seed, and the
//! `bench-hdr` percentile reporting) is shared with the realistic-overhead A/B
//! (`realistic_overhead.rs`) via [`common`], so both benches run the **same**
//! workload and the overhead ratio is meaningful.
//!
//! # Bench-hdr convention
//!
//! Latency-shaped paths report **percentiles via `hdrhistogram`
//! (p50 / p99 / p99.9 / p99.99), not criterion's default mean** — a mean hides
//! the tail a production sweep actually feels
//! ([docs/07 §5](../docs/07-performance-and-security.md#5-benchmark-methodology)).
//! `criterion` drives warmup + sample scheduling; each timed iteration's latency
//! is recorded into an [`hdrhistogram::Histogram`] and the percentiles are
//! printed by [`common::report`].
//!
//! # Coordinated omission — disclosure
//!
//! This is a **closed-loop throughput** bench: each run starts immediately after
//! the previous one finishes, with no external arrival schedule. Coordinated
//! omission (a slow response masking the lateness of requests that *should* have
//! been issued on a fixed cadence) therefore **does not apply** — there is no
//! expected inter-arrival interval to correct against, so the histogram records
//! raw back-to-back run latency without a corrected-interval replay
//! (`record`, not `record_correct`). The reported quantiles are the true
//! distribution of end-to-end run latency at saturation.
//!
//! # Warmup
//!
//! An explicit warmup of [`common::WARMUP_RUNS`] un-recorded runs precedes the
//! criterion measurement (cache, branch predictor, allocator arenas), on top of
//! criterion's own warmup phase. Every histogram observation is therefore taken
//! at steady state.
//!
//! # Determinism note
//!
//! The engine under measurement is deterministic (fixed [`common::SEED`], the
//! sole seeded `ChaCha8Rng`, no wall clock in the loop). The wall-clock timing
//! here is inherent to *benchmarking* and lives entirely in this bench harness —
//! it never enters the engine, so `(seed, config, data)` stays byte-reproducible.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;

use ironcondor::{ExecutionMode, run_backtest};

mod common;

/// The naive-throughput benchmark.
fn bench_naive_throughput(c: &mut Criterion) {
    // --- setup (outside the measured region) ---------------------------------
    let dir = tempfile::tempdir().expect("create a tempdir for the bench fixture");
    let path = dir.path().join("condor_chain.parquet");
    common::write_parquet(&path, &common::condor_rows())
        .expect("write the canonical bench fixture");
    let config = common::condor_config(&path, ExecutionMode::Naive);
    let spec = common::iron_condor_spec();

    // Explicit warmup — un-recorded, on top of criterion's own warmup phase.
    for _ in 0..common::WARMUP_RUNS {
        let run = run_backtest(&config, &spec, common::exit_policy()).expect("warmup run succeeds");
        let _ = black_box(run);
    }

    // Per-run latencies land here; 3 significant figures, auto-resizing.
    let hist = RefCell::new(Histogram::<u64>::new(3).expect("build the latency histogram"));

    let mut group = c.benchmark_group("naive_throughput");
    // Each iteration processes STEPS steps → criterion also reports steps/sec.
    group.throughput(Throughput::Elements(u64::from(common::STEPS)));
    group.bench_function("run_backtest", |b| {
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
    group.finish();

    common::report("naive_throughput", &hist.borrow());
}

criterion_group!(benches, bench_naive_throughput);
criterion_main!(benches);
