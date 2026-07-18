#!/usr/bin/env python3
"""PyO3 per-call overhead vs the batch path — hot path H5 / PB-6 (issue #43).

The Python-side companion to the Rust marshalling criterion bench
(``benches/pyo3_marshal.rs``). Where the Rust bench pins the *absolute* boundary
marshalling cost in nanoseconds, this script measures the two Python-facing
quantities PB-6 asks users to know before they batch:

1. **Single-call latency** — the wall time of one ``ic.run(cfg)`` over a small
   fixed scenario (marshal-in under the GIL, then the GIL-released engine +
   bundle writer, then the handle-out), reported as p50 / p99 / p99.9 over ``M``
   sequential calls after a warmup.

2. **Batch amortisation** — a fixed batch of ``B`` runs fanned out across a
   ``ThreadPoolExecutor`` with ``N`` worker threads for ``N`` in ``1, 2, 4, 8``.
   Because ``ic.run`` releases the GIL for the whole engine + writer
   (``py.detach`` in ``src/python/run.rs``), the N Rust engines execute
   concurrently on N cores, so the **per-run wall time falls as N grows** — the
   across-run parallelism the engine is built for
   (docs/06 §3, docs/02 §9). This is *not* a single-run speedup; it is the
   amortisation of the fixed per-call cost you pay N times either way, by
   overlapping the work.

This is a **measurement, not an optimization**. It is a **standalone script, run
manually** — deliberately kept out of the ``pytest`` test gates: the filename is
``pyo3_overhead.py`` (not ``test_*``) and it lives under ``python/benches/``, so
pytest does not collect it, and it defines no ``test_*`` functions. Run it with
the maturin-``develop``'d wheel in a 3.12 venv:

    maturin develop --features "python orderbook simulator"   # from python/
    python python/benches/pyo3_overhead.py                    # this script

Percentiles use a pure-Python sorted-array reduction (no numpy dependency), so
the script needs only the installed ``ironcondor`` module plus ``pyarrow`` to
generate the canonical chain (the same generator ``python/tests/test_parity.py``
uses). The measured numbers are recorded in ``BENCH.md`` §H5 with their run
conditions and an interpretation block; **no number is written there before it
is measured**.

Coordinated omission: both parts are closed-loop (each call / batch starts only
after the previous finishes; there is no external arrival schedule), so
coordinated omission does not apply — raw back-to-back wall time is recorded.
Warmup: an explicit un-recorded warmup precedes each measured phase.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - environment guard
    sys.exit(
        "pyo3_overhead.py needs pyarrow to generate the canonical chain; "
        "install it (e.g. `pip install pyarrow`) and re-run."
    )

import ironcondor as ic

# --- the canonical iron-condor scenario (mirrors tests/common/mod.rs) --------

TS0 = 1_750_291_200_000_000_000
NANOS_PER_DAY = 86_400_000_000_000
EXPIRY = TS0 + 30 * NANOS_PER_DAY
# 60 s per step, so any step count used here stays strictly before expiry
# (30 days), exactly like the Rust `benches/common` tape.
DT_NS = 60_000_000_000

# The four canonical legs (strike_cents, style, bid_cents, ask_cents), mids
# 2000/800/1800/700 — the same legs `condor_rows` builds.
LEGS = [
    (510_000, "call", 1_995, 2_005),
    (520_000, "call", 795, 805),
    (490_000, "put", 1_795, 1_805),
    (480_000, "put", 695, 705),
]

_FEED_SCHEMA = pa.schema([
    ("step", pa.int32()),
    ("ts", pa.int64()),
    ("underlying", pa.string()),
    ("underlying_price", pa.int64()),
    ("tick_size", pa.int64()),
    ("contract_multiplier", pa.int32()),
    ("expiration", pa.int64()),
    ("strike", pa.int64()),
    ("style", pa.string()),
    ("bid", pa.int64()),
    ("ask", pa.int64()),
    ("bid_size", pa.int32()),
    ("ask_size", pa.int32()),
    ("implied_volatility", pa.float64()),
    ("delta", pa.float64()),
    ("gamma", pa.float64()),
    ("theta", pa.float64()),
    ("vega", pa.float64()),
])


def write_chain(path: Path, steps: int) -> None:
    """Write the canonical iron-condor Parquet chain of `steps` snapshots."""
    rows: list[dict] = []
    for step in range(steps):
        ts = TS0 + step * DT_NS
        for strike, style, bid, ask in LEGS:
            rows.append({
                "step": step, "ts": ts, "underlying": "SPX",
                "underlying_price": 500_000, "tick_size": 5,
                "contract_multiplier": 100, "expiration": EXPIRY,
                "strike": strike, "style": style, "bid": bid, "ask": ask,
                "bid_size": 50, "ask_size": 50, "implied_volatility": 0.2,
                "delta": 0.3, "gamma": 0.01, "theta": -0.05, "vega": 0.1,
            })
    columns = {name: [row[name] for row in rows] for name in _FEED_SCHEMA.names}
    pq.write_table(pa.table(columns, schema=_FEED_SCHEMA), path)


def make_config(chain: Path, out_dir: Path) -> "ic.BacktestConfig":
    """The shared canonical naive config, writing its bundle under `out_dir`."""
    return (
        ic.BacktestConfig(seed=42, capital_cents=10_000_000)
        .data_parquet(str(chain))
        .strategy_iron_condor(
            underlying="SPX",
            underlying_price_cents=500_000,
            short_call_strike_cents=510_000,
            short_put_strike_cents=490_000,
            long_call_strike_cents=520_000,
            long_put_strike_cents=480_000,
            expiration_ns=EXPIRY,
            quantity=1,
            premium_short_call_cents=2_000,
            premium_short_put_cents=1_800,
            premium_long_call_cents=800,
            premium_long_put_cents=700,
            implied_volatility=0.20,
            risk_free_rate=0.05,
            dividend_yield=0.0,
            open_fee_cents=65,
            close_fee_cents=65,
        )
        .exit_time_steps(1_000_000)
        .execution_naive()
        .fees(per_contract_cents=65, per_order_cents=100)
        .output_dir(str(out_dir))
    )


# --- percentiles (pure Python, no numpy) -------------------------------------


def percentile(sorted_vals: list[float], q: float) -> float:
    """Nearest-rank percentile of an already-sorted, non-empty list.

    `q` in [0, 1]. Nearest-rank (not interpolated) — the same shape the
    hdrhistogram entries in BENCH.md report, and adequate for the sample counts
    here (a note in the interpretation discloses tail resolution)."""
    if not sorted_vals:
        raise ValueError("percentile of an empty sample")
    n = len(sorted_vals)
    rank = max(0, min(n - 1, int(round(q * (n - 1)))))
    return sorted_vals[rank]


def summarise(latencies_s: list[float]) -> dict[str, float]:
    """p50/p90/p99/p99.9 + min/max/mean of a latency sample, in milliseconds."""
    ms = sorted(v * 1_000.0 for v in latencies_s)
    return {
        "n": len(ms),
        "min": ms[0],
        "p50": percentile(ms, 0.50),
        "p90": percentile(ms, 0.90),
        "p99": percentile(ms, 0.99),
        "p999": percentile(ms, 0.999),
        "max": ms[-1],
        "mean": statistics.fmean(ms),
    }


# --- 1. single-call latency --------------------------------------------------


def measure_single_call(chain: Path, root: Path, iters: int, warmup: int) -> dict[str, float]:
    """`iters` sequential `ic.run(cfg)` calls (each to a distinct output dir, so
    no run_id collision), timed individually after `warmup` un-recorded calls.
    Only `ic.run(cfg)` is inside the timer — the config build (cheap marshalling,
    measured separately by the Rust bench) is excluded."""
    counter = 0

    def one_run() -> float:
        nonlocal counter
        cfg = make_config(chain, root / f"s{counter}")
        counter += 1
        t0 = time.perf_counter()
        bundle = ic.run(cfg)
        dt = time.perf_counter() - t0
        # touch the handle so the call is not elided, and to exercise the
        # GIL-reacquiring return path.
        _ = bundle.path
        return dt

    for _ in range(warmup):
        one_run()
    return summarise([one_run() for _ in range(iters)])


# --- 2. batch amortisation across GIL-releasing threads ----------------------


# Batches fanned across N threads warm all code paths in a couple of passes
# (the single-call phase already warmed the engine + writer), so the batch phase
# needs only a small, fixed warmup — not the full single-call warmup count.
BATCH_WARMUP = 2


def measure_batch(chain: Path, root: Path, batch: int, threads: list[int]) -> list[dict[str, float]]:
    """For each worker count N in `threads`, run a fixed `batch` of `ic.run`
    calls (distinct output dirs) fanned across a ThreadPoolExecutor(N), and
    measure the wall time of the whole batch. per-run = wall / batch; the GIL is
    released inside each run so the N engines overlap."""
    invocation = 0  # a unique prefix per run_batch call ⇒ no run_id dir collision

    def run_batch(n_workers: int) -> float:
        nonlocal invocation
        tag = invocation
        invocation += 1
        # Pre-build the configs (unique output dirs) OUTSIDE the timed wall so we
        # measure the run parallelism, not config construction.
        cfgs = [make_config(chain, root / f"i{tag}_{i}") for i in range(batch)]
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            list(pool.map(lambda c: ic.run(c).path, cfgs))
        return time.perf_counter() - t0

    # A small fixed warmup at the widest fan-out (threads + code paths).
    for _ in range(BATCH_WARMUP):
        run_batch(max(threads))

    results: list[dict[str, float]] = []
    for n in threads:
        wall = run_batch(n)
        results.append({
            "threads": n,
            "batch": batch,
            "wall_ms": wall * 1_000.0,
            "per_run_ms": wall * 1_000.0 / batch,
        })
    return results


# --- reporting ---------------------------------------------------------------


def print_single(summary: dict[str, float]) -> None:
    print("\n=== 1. single-call ic.run() latency (closed-loop, warm) ===")
    print(f"recorded calls: {int(summary['n'])}")
    print("coordinated omission: N/A (closed-loop back-to-back; no arrival schedule)")
    print(
        "wall/call  "
        f"p50={summary['p50']:.3f} ms  "
        f"p90={summary['p90']:.3f} ms  "
        f"p99={summary['p99']:.3f} ms  "
        f"p99.9={summary['p999']:.3f} ms  "
        f"min={summary['min']:.3f}  max={summary['max']:.3f}  mean={summary['mean']:.3f}"
    )


def print_batch(rows: list[dict[str, float]]) -> None:
    print("\n=== 2. batch amortisation (N GIL-releasing threads) ===")
    base = rows[0]["per_run_ms"] if rows else float("nan")
    print(f"{'threads':>7} | {'batch':>5} | {'wall (ms)':>10} | {'per-run (ms)':>12} | {'speedup':>7}")
    print("-" * 56)
    for r in rows:
        speedup = base / r["per_run_ms"] if r["per_run_ms"] else float("nan")
        print(
            f"{int(r['threads']):>7} | {int(r['batch']):>5} | "
            f"{r['wall_ms']:>10.2f} | {r['per_run_ms']:>12.4f} | {speedup:>6.2f}x"
        )
    print(
        "\nspeedup is per-run(N) vs per-run(1); it plateaus once the engines "
        "saturate the available cores."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--steps", type=int, default=512,
                        help="snapshots per run (4 legs each); default 512")
    parser.add_argument("--iters", type=int, default=300,
                        help="sequential single-call samples; default 300")
    parser.add_argument("--warmup", type=int, default=30,
                        help="un-recorded warmup runs per phase; default 30")
    parser.add_argument("--batch", type=int, default=64,
                        help="runs per batch in the amortisation sweep; default 64")
    parser.add_argument("--threads", type=int, nargs="+", default=[1, 2, 4, 8],
                        help="worker-thread counts to sweep; default 1 2 4 8")
    args = parser.parse_args()

    print(f"ironcondor {ic.__version__} — PyO3 per-call overhead vs batch (PB-6, #43)")
    print(
        f"scenario: {args.steps} steps x 4 condor legs, naive mode, seed 42; "
        f"single-call iters={args.iters} warmup={args.warmup}; "
        f"batch={args.batch} threads={args.threads}"
    )

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        chain = root / "condor.parquet"
        write_chain(chain, args.steps)

        single = measure_single_call(chain, root / "single", args.iters, args.warmup)
        print_single(single)

        batch = measure_batch(chain, root / "batch", args.batch, args.threads)
        print_batch(batch)


if __name__ == "__main__":
    main()
