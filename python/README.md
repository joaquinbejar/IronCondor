# ironcondor (Python bindings)

Python bindings for [`ironcondor`](https://github.com/joaquinbejar/IronCondor),
a high-performance options-strategy backtester with order-book-level fill
simulation, built on [`optionstratlib`](https://crates.io/crates/optionstratlib).

> **Status: v0.5.0, published.** The backtest API (`BacktestConfig`, `run`,
> `Bundle`) is wired over the same Rust engine, and the typed exception
> hierarchy landed with #40: `IronCondorError` is the base, with
> `ConfigError`, `DataError`, `ExecutionError`, `StrategyError`,
> `BundleError`, and `EngineError` beneath it (`ConfigError` is also a
> `ValueError`; `DataError` and `BundleError` are also `OSError`), and no
> panic crosses the FFI boundary. The distribution is on PyPI as
> [`ironcondor`](https://pypi.org/project/ironcondor/): `pip install
> ironcondor`. The 0.5.0 upload carries the macOS `arm64` `cp310-abi3` wheel
> plus the sdist; on Linux and macOS `x86_64` pip builds from the sdist for
> now, which needs a Rust toolchain.

```python
import ironcondor as ic

cfg = (
    ic.BacktestConfig(seed=42, capital_cents=1_000_000)   # $10,000, integer cents
      .data_parquet("chains/spx_2025.parquet")
      .strategy_iron_condor(
          underlying="SPX",
          underlying_price_cents=500_000,
          short_call_strike_cents=510_000, long_call_strike_cents=520_000,
          short_put_strike_cents=490_000,  long_put_strike_cents=480_000,
          expiration_ns=1_752_883_200_000_000_000,
          quantity=1,
          premium_short_call_cents=2_000, premium_short_put_cents=1_800,
          premium_long_call_cents=800,    premium_long_put_cents=700,
      )
      .exit_profit_percent(0.5)            # optionstratlib ExitPolicy
      .execution_naive(slippage_cents=5)   # or .execution_realistic()
      .fees(per_contract_cents=65, per_order_cents=100)
)

bundle  = ic.run(cfg)              # releases the GIL; runs the Rust engine AND
                                   # atomically writes <output_dir>/<run_id>/
print(bundle.path)                 # the on-disk bundle directory run() published
equity  = bundle.equity_curve()    # -> pandas.DataFrame  (needs the [pandas] extra)
metrics = bundle.metrics()         # -> dict (from manifest.json)

reopened = ic.load_bundle(bundle.path)   # validated through the hardened reader
```

Money crosses the boundary as **integer cents** (`*_cents`); the only floats are
the analytic exception (implied volatility, the two rates, `ExitPolicy`
percentages). Only what the engine wires end to end is exposed — a **Parquet**
source and the **iron condor** strategy; a CSV source and the short strangle are
not yet reachable through the binding.

## DataFrame accessors (optional pandas extra)

`bundle.fills()`, `.equity_curve()`, `.positions()`, `.greeks_attribution()`
return **pandas DataFrames** read lazily from the on-disk Parquet. pandas (and
its `pyarrow` engine) is an **optional soft dependency**:

```bash
pip install ironcondor[pandas]
```

`run()`, `load_bundle`, `metrics()`, and `bundle.path` work without it — and
because the bundle is plain Parquet + JSON, `polars` / `pyarrow` can read
`bundle.path` directly instead.

## Building locally

Wheels are built with [maturin](https://www.maturin.rs/) using the `abi3`
stable ABI, so one `cp310-abi3` wheel per platform serves Python 3.10+. The
published wheel is a self-contained backtester — `python` (bindings) plus
`orderbook` (realistic execution) plus `simulator` (the synthetic-chain feed):

```bash
# from this directory (python/)
maturin build --release --features "python orderbook simulator"
# or, for an editable install into the current interpreter:
maturin develop --features "python orderbook simulator"
```

The crate manifest lives at the repository root; `pyproject.toml` points maturin
at it via `manifest-path = "../Cargo.toml"`, and its `[tool.maturin] features`
already lists the same set (plus `pyo3/extension-module`), so a bare
`maturin build --release` produces the same wheel.

## Releases and PyPI status

`ironcondor` is **published on PyPI**; the name is registered and publishing
stays a deliberate, owner-approved action (docs/06 §8, CLAUDE.md).

- **On every pipeline**, CI (`.github/workflows/ci.yml`, job `python-wheels`)
  builds and `pytest`-runs a `cp310-abi3` wheel on Linux + macOS.
- **On a `v*` tag**, the release workflow (`.github/workflows/release.yml`)
  builds release-grade wheels (Linux manylinux, macOS x86_64, macOS arm64)
  plus an sdist, then publishes to PyPI via **trusted publishing (OIDC, no
  token)**, behind a manually-approved GitHub `pypi` environment. Windows
  wheels remain a deferred wishlist item.
- **Known gap at 0.5.0.** The index carries only the macOS `arm64` wheel and
  the sdist: the wheel matrix failed on the v0.5.0 tag run and the fix
  (`bd7313d`) landed after it, so the Linux and macOS `x86_64` wheels still
  owe a green release run.
