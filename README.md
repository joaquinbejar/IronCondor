# IronCondor

> 🚧 **Early development** — v0.0.1 reserves the crate name. APIs do not exist yet.

**IronCondor** is a high-performance backtesting engine for options trading
strategies, written in Rust.

## Why another backtester?

Every existing backtester fills options orders at mid or bid/ask with a fixed
slippage assumption. IronCondor simulates execution **at the order book
level** — queue position, per-strike liquidity, and real market impact —
using a full options matching engine.

## Planned features

- **Order-book-level fills**: route orders through
  [Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook)
  instead of assuming mid-price fills. A fast `naive` mode (mid/spread) is
  also available for quick iteration.
- **Multi-leg strategies**: spreads, condors, butterflies and custom
  strategies via [OptionStratLib](https://github.com/joaquinbejar/OptionStratLib).
- **P&L attribution by Greek**: decompose returns into theta, delta, vega and
  spread capture.
- **Synthetic data**: no historical chains? Generate realistic ones with
  [OptionChain-Simulator](https://github.com/joaquinbejar/OptionChain-Simulator).
- **Python bindings**: first-class Python API via PyO3, published on PyPI.

## Ecosystem

Part of a family of Rust crates for options trading infrastructure:
[OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) ·
[OptionStratLib](https://github.com/joaquinbejar/OptionStratLib) ·
[PriceLevel](https://github.com/joaquinbejar/PriceLevel) ·
[Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook)

## License

MIT — see [LICENSE](./LICENSE).

## Contact

Joaquin Bejar — jb@taunais.com
