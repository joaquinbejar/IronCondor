//! PyO3 boundary marshalling cost — hot path **H5**/PB-6 (issue #43).
//!
//! Measures the **Rust-side per-call marshalling cost of the PyO3 boundary with
//! the engine elided** — the fixed price a Python `ic.run(cfg)` caller pays to
//! cross into Rust, isolated from the replay loop (H1/PB-2), the fill models
//! (H2/PB-3), and the bundle writer (H4/PB-5). It is the companion of the
//! Python-side per-call-vs-batch script (`python/benches/pyo3_overhead.py`): this
//! bench pins the *absolute* boundary cost in nanoseconds; the script shows how a
//! batch of runs fanned across GIL-releasing threads amortises it
//! ([docs/07 §3 PB-6](../docs/07-performance-and-security.md#3-budgets-design-targets--pending-the-v01-bench-suite),
//! [docs/06 §3](../docs/06-python-bindings.md#3-gil-strategy)).
//!
//! # What is inside the timed region (the honest isolation)
//!
//! The bench drives the **real** registered `#[pymodule]` in an embedded
//! interpreter (the same in-process mechanism `tests/python_parity.rs` uses:
//! [`pyo3::append_to_inittab!`] on [`ironcondor::python::ironcondor`] +
//! [`Python::initialize`]), so every call goes through the genuine PyO3
//! trampoline a `pip install` user hits — not a shortcut.
//!
//! Two measurements:
//!
//! - **`marshal_full_config`** — building one fully-populated `BacktestConfig`
//!   through the chainable builders (`BacktestConfig(seed, capital)` +
//!   `.data_parquet` + `.strategy_iron_condor` + `.exit_time_steps` +
//!   `.execution_naive` + `.fees` + `.output_dir`). This is the **marshal-in
//!   path**: the seven Python→Rust trampoline crossings a caller performs before
//!   `ic.run()` reaches the engine. **INSIDE** the timed region: the seven
//!   crossings, PyO3 argument extraction of every field (Python `int`→`u64`,
//!   `str`→`String`, `float`→`f64`, the 17-key kwargs dict → the typed
//!   `IronCondorSpec` args), the Rust construction each builder does — in
//!   particular `strategy_iron_condor`'s `Underlying::new` grammar check,
//!   `Quantity::new`, three `Decimal::try_from` conversions, and its
//!   `guard_boundary` `catch_unwind` — and the `#[pyclass]` allocation of the new
//!   config object. **OUTSIDE**: the input kwargs / fees `PyDict`s (built once and
//!   reused, so the timed cost is the *marshalling*, not the Python-side dict
//!   construction), the GIL acquire (held across the whole measurement — the
//!   marshal-in path runs under the GIL in a real caller too, since the GIL is
//!   only *released* later, inside `run()` around the engine), and the entire
//!   engine + writer.
//!
//! - **`marshal_single_call`** — one `.seed(u64)` builder crossing, the **fixed
//!   per-boundary-call floor**: a single trampoline + one `u64` extract + the
//!   `PyRefMut` self-return, nothing else. To lift the signal above the
//!   `Instant::now()` resolution (~tens of ns), each timed sample runs an inner
//!   batch of [`SINGLE_CALL_BATCH`] crossings and records the **mean** per
//!   crossing; the reported percentiles are therefore batch-mean percentiles
//!   (tighter than the raw per-call spread would be), disclosed as such.
//!
//! # What is NOT isolable here (documented gap)
//!
//! `PyBacktestConfig::to_rust()` — the final marshal of the accumulated wrapper
//! into the real `BacktestConfig` + `StrategySpec` + `ExitPolicy`, plus its
//! `validate()` — is `pub(crate)` and **not reachable through the Python object
//! protocol**; it runs *inside* `run()`, immediately before the (GIL-released)
//! engine, so it cannot be timed in isolation from the engine on the public
//! surface. `marshal_full_config` therefore measures the *builder marshal-in* a
//! caller pays explicitly; the `to_rust()` finalisation is a bounded, one-time
//! extra folded into `run()` (its cost is dominated by the same field copies +
//! one `validate()` already exercised by the builders). This bench does not widen
//! the binding's public surface to reach it — the isolation is honest about the
//! boundary it can and cannot draw.
//!
//! # Bench-hdr convention
//!
//! Latency-shaped, so it reports **percentiles via `hdrhistogram`
//! (p50 / p99 / p99.9 / p99.99), not criterion's mean**
//! ([docs/07 §5](../docs/07-performance-and-security.md#5-benchmark-methodology)).
//! `criterion` drives warmup + sample scheduling; each timed unit's latency lands
//! in an [`hdrhistogram::Histogram`] and the percentiles are printed by
//! [`report_latency`].
//!
//! # Coordinated omission — disclosure
//!
//! This is a **closed-loop** micro-bench: each marshal starts immediately after
//! the previous finishes, with no external arrival schedule, so coordinated
//! omission **does not apply** — the histogram records raw back-to-back latency
//! (`record`, not `record_correct`).
//!
//! # Warmup
//!
//! An explicit [`WARMUP`]-marshal un-recorded warmup precedes the measured phase,
//! on top of criterion's own warmup — every observation is at steady state.
//!
//! # Determinism note
//!
//! No engine runs here, so there is no seeded RNG to disturb; the wall-clock
//! timing lives entirely in this bench harness and never enters the crate.

use std::cell::RefCell;
use std::sync::Once;
use std::time::{Duration, Instant};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use ironcondor::python::ironcondor as ic_module;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

/// Un-recorded warmup marshals before the measured phase.
const WARMUP: usize = 64;

/// Inner-loop crossings per timed sample in `marshal_single_call`, chosen so the
/// batch spans far more than one `Instant::now()` resolution unit; the recorded
/// value is the mean per crossing.
const SINGLE_CALL_BATCH: u64 = 512;

/// The tape anchor `ts_0` and the `ts_0 + 30 days` expiry, matching
/// `tests/common` and the canonical parity scenario.
const TS0_NS: i64 = 1_750_291_200_000_000_000;
/// The four condor legs' shared absolute expiry (`ts_0 + 30 days`).
const EXPIRY_NS: i64 = TS0_NS + 30 * 86_400_000_000_000;

/// Register the real `ironcondor` `#[pymodule]` and initialise the embedded
/// interpreter — exactly once for the whole bench binary.
fn ensure_python_ready() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        pyo3::append_to_inittab!(ic_module);
        Python::initialize();
    });
}

/// The 17-key `strategy_iron_condor` kwargs dict — the canonical iron-condor legs
/// (integer cents), the expiry as ns since the epoch, and the analytic
/// vol/rate `f64`s. Built once and reused so the timed region measures the
/// **marshalling** of these values across the boundary, not the dict's
/// construction.
fn strat_kwargs<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let strat = PyDict::new(py);
    strat.set_item("underlying", "SPX")?;
    strat.set_item("underlying_price_cents", 500_000_u64)?;
    strat.set_item("short_call_strike_cents", 510_000_u64)?;
    strat.set_item("short_put_strike_cents", 490_000_u64)?;
    strat.set_item("long_call_strike_cents", 520_000_u64)?;
    strat.set_item("long_put_strike_cents", 480_000_u64)?;
    strat.set_item("expiration_ns", EXPIRY_NS)?;
    strat.set_item("quantity", 1_u32)?;
    strat.set_item("premium_short_call_cents", 2_000_u64)?;
    strat.set_item("premium_short_put_cents", 1_800_u64)?;
    strat.set_item("premium_long_call_cents", 800_u64)?;
    strat.set_item("premium_long_put_cents", 700_u64)?;
    strat.set_item("implied_volatility", 0.20_f64)?;
    strat.set_item("risk_free_rate", 0.05_f64)?;
    strat.set_item("dividend_yield", 0.0_f64)?;
    strat.set_item("open_fee_cents", 65_u64)?;
    strat.set_item("close_fee_cents", 65_u64)?;
    Ok(strat)
}

/// The `fees` kwargs dict, built once and reused (see [`strat_kwargs`]).
fn fees_kwargs<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let fees = PyDict::new(py);
    fees.set_item("per_contract_cents", 65_u64)?;
    fees.set_item("per_order_cents", 100_u64)?;
    Ok(fees)
}

/// Marshal a full `BacktestConfig` through the real chainable builders — the
/// seven Python→Rust crossings the timed region measures.
fn build_full_config<'py>(
    ic: &Bound<'py, PyModule>,
    strat: &Bound<'py, PyDict>,
    fees: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyAny>> {
    let cfg = ic
        .getattr("BacktestConfig")?
        .call1((42_u64, 10_000_000_u64))?;
    let cfg = cfg.call_method1("data_parquet", ("chains/bench.parquet",))?;
    let cfg = cfg.call_method("strategy_iron_condor", (), Some(strat))?;
    let cfg = cfg.call_method1("exit_time_steps", (1_000_000_usize,))?;
    let cfg = cfg.call_method0("execution_naive")?;
    let cfg = cfg.call_method("fees", (), Some(fees))?;
    let cfg = cfg.call_method1("output_dir", ("runs/out",))?;
    Ok(cfg)
}

/// Print the hdrhistogram percentiles of one labelled latency series.
fn report_latency(label: &str, hist: &Histogram<u64>, note: &str) {
    let p = |q: f64| hist.value_at_quantile(q);
    println!("\n=== {label} — hdrhistogram (bench-hdr, docs/07 §5) ===");
    println!("recorded samples: {}", hist.len());
    println!("coordinated omission: N/A (closed-loop back-to-back; no arrival schedule)");
    println!(
        "latency  p50={} ns  p99={} ns  p99.9={} ns  p99.99={} ns  max={} ns",
        p(0.50),
        p(0.99),
        p(0.999),
        p(0.9999),
        hist.max(),
    );
    if !note.is_empty() {
        println!("note: {note}");
    }
    println!("===============================================================\n");
}

/// The PyO3-marshalling benchmark (H5/PB-6).
fn bench_pyo3_marshal(c: &mut Criterion) {
    ensure_python_ready();

    Python::attach(|py| {
        let ic = py
            .import("ironcondor")
            .expect("the embedded interpreter imports the real ironcondor module");
        let strat = strat_kwargs(py).expect("build the strategy kwargs once");
        let fees = fees_kwargs(py).expect("build the fees kwargs once");

        // Explicit warmup — un-recorded, on top of criterion's own warmup.
        for _ in 0..WARMUP {
            let cfg = build_full_config(&ic, &strat, &fees).expect("warmup marshal succeeds");
            let _ = black_box(cfg);
        }

        let mut group = c.benchmark_group("pyo3_marshal");

        // --- marshal_full_config: the marshal-in path (7 crossings) ----------
        let hist_full = RefCell::new(Histogram::<u64>::new(3).expect("build the full-config hist"));
        group.bench_function("marshal_full_config", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let cfg = build_full_config(&ic, &strat, &fees)
                        .expect("measured full-config marshal succeeds");
                    let elapsed = start.elapsed();
                    let _ = black_box(&cfg);
                    total += elapsed;
                    let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
                    hist_full.borrow_mut().record(ns).ok();
                }
                total
            });
        });

        // --- marshal_single_call: the fixed per-crossing floor ---------------
        // One reusable config to call `.seed(u64)` on; each call is one trampoline
        // returning the same object, so no engine or state accumulates.
        let base =
            build_full_config(&ic, &strat, &fees).expect("build the single-call base config");
        let hist_single =
            RefCell::new(Histogram::<u64>::new(3).expect("build the single-call hist"));
        group.bench_function("marshal_single_call", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    for k in 0..SINGLE_CALL_BATCH {
                        let r = base
                            .call_method1("seed", (k,))
                            .expect("measured single builder crossing succeeds");
                        let _ = black_box(r);
                    }
                    let elapsed = start.elapsed();
                    total += elapsed;
                    let per =
                        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX) / SINGLE_CALL_BATCH;
                    hist_single.borrow_mut().record(per).ok();
                }
                total
            });
        });

        group.finish();

        report_latency(
            "marshal_full_config (7 builder crossings: 1 ctor + 6 methods)",
            &hist_full.borrow(),
            "one recorded sample = one full BacktestConfig marshalled in; \
             per-crossing ≈ p50 / 7",
        );
        report_latency(
            "marshal_single_call (one .seed(u64) crossing)",
            &hist_single.borrow(),
            "each sample is the MEAN over an inner batch of 512 crossings \
             (lifted above Instant::now resolution); batch-mean percentiles",
        );
        report_ratio(&hist_full.borrow(), &hist_single.borrow());
    });
}

/// Print the dimensionless **full-config / single-crossing** marshalling ratio
/// plus the machine-readable lines the H5 CI gate (`scripts/pyo3_gate.sh`, #51)
/// and `BENCH.md` parse.
///
/// The gate tracks this ratio — not an absolute nanosecond count — because both
/// numbers are measured in the **same** bench, same embedded interpreter, same
/// process, so a uniform hardware clock-speed factor cancels (the same
/// portability argument as `scripts/bench_gate.sh`'s realistic/naive ratio). The
/// ratio only moves when the heavy `strategy_iron_condor` marshal regresses
/// **relative** to the trivial `.seed(u64)` crossing floor — a real boundary
/// regression — not when the runner is merely slow.
fn report_ratio(full: &Histogram<u64>, single: &Histogram<u64>) {
    let full_p50 = full.value_at_quantile(0.50);
    let single_p50 = single.value_at_quantile(0.50);
    let ratio = if single_p50 == 0 {
        f64::INFINITY
    } else {
        full_p50 as f64 / single_p50 as f64
    };
    println!("\n=== pyo3_marshal — full/single ratio (PB-6, docs/07 §6) ===");
    println!(
        "marshal_full_config p50 = {full_p50} ns  |  marshal_single_call p50 = {single_p50} ns"
    );
    println!("full/single crossing ratio: p50 = {ratio:.2}x");
    // Machine-readable lines — the CI gate greps MARSHAL_RATIO_P50; BENCH.md records all.
    println!("MARSHAL_FULL_NS_P50={full_p50}");
    println!("MARSHAL_SINGLE_NS_P50={single_p50}");
    println!("MARSHAL_RATIO_P50={ratio:.4}");
    println!("==========================================================\n");
}

criterion_group!(benches, bench_pyo3_marshal);
criterion_main!(benches);
