# ironcondor (Python bindings)

Python bindings for [`ironcondor`](https://github.com/joaquinbejar/IronCondor),
a high-performance options-strategy backtester with order-book-level fill
simulation, built on [`optionstratlib`](https://crates.io/crates/optionstratlib).

> **Status: scaffold (v0.4, #38).** This is the empty-but-real PyO3 shell — it
> builds a `cp310-abi3` wheel that imports and reports `ironcondor.__version__`.
> The backtest API (`BacktestConfig`, `run`, `Bundle`) lands in later v0.4
> issues (#39/#40). PyPI wheels are a v0.4 deliverable and are **not published
> yet** — the distribution name `ironcondor` is unregistered on PyPI.

```python
import ironcondor
print(ironcondor.__version__)
```

## Building locally

Wheels are built with [maturin](https://www.maturin.rs/) using the `abi3`
stable ABI, so one `cp310-abi3` wheel per platform serves Python 3.10+:

```bash
# from this directory (python/)
maturin build --release --features python
# or, for an editable install into the current interpreter:
maturin develop --features python
```

The crate manifest lives at the repository root; `pyproject.toml` points maturin
at it via `manifest-path = "../Cargo.toml"`.
