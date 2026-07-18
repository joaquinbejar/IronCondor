# IronCondor

> 🚧 **Early development** — the architecture scaffold exists, but no
> engine functionality is implemented yet. APIs are not stable.

**IronCondor** is a high-performance backtesting engine for options
trading strategies, written in Rust.

### Why another backtester?

Every existing backtester fills options orders at mid or bid/ask with a
fixed slippage assumption. IronCondor is designed to simulate execution
**at the order book level** — queue position, per-strike liquidity, and
real market impact — using a full options matching engine.

### Planned features

- **Order-book-level fills**: route orders through
  [Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook)
  instead of assuming mid-price fills. A fast `naive` mode (mid/spread) is
  also available for quick iteration.
- **Multi-leg strategies**: spreads, condors, butterflies and custom
  strategies via [OptionStratLib](https://github.com/joaquinbejar/OptionStratLib).
- **P&L attribution by Greek**: decompose returns into theta, delta, vega
  and spread capture.
- **Synthetic data**: no historical chains? Generate realistic ones with
  [OptionChain-Simulator](https://github.com/joaquinbejar/OptionChain-Simulator).
- **Python bindings**: first-class Python API via PyO3, published on PyPI.

### Architecture

The crate is a single package layered as follows (each module is a
compiling placeholder until its roadmap issue lands):

- [`domain`] — canonical types: integer-cents money, market-data and
  execution records.
- [`engine`] — the deterministic replay loop, strategy seam, simulation
  clock, scenarios, and ledger.
- [`execution`] — fill models: naive (mid/spread + slippage) and realistic
  (order-book matching, feature `orderbook`).
- [`data`] — chain feeds: historical Parquet/CSV loaders and the
  OptionChain-Simulator client (feature `simulator`).
- [`analytics`] — P&L attribution by Greek and summary metrics.
- [`bundle`] — the result-bundle writer (manifest + Parquet tables).
- `python` — PyO3 bindings (feature `python`).
- [`error`] / [`config`] — the shared typed-error and configuration
  boundary.

### Feature flags

- `default` — engine + naive execution + historical feeds.
- `orderbook` — realistic, liquidity-aware execution.
- `simulator` — synthetic chain sessions over HTTP.
- `python` — PyO3 bindings (the `cdylib` is inert without it).

### Ecosystem

Part of a family of Rust crates for options trading infrastructure:
[OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) ·
[OptionStratLib](https://github.com/joaquinbejar/OptionStratLib) ·
[PriceLevel](https://github.com/joaquinbejar/PriceLevel) ·
[Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook)

### Documentation

The full design documentation (PRD, roadmap, domain model, engine
architecture, execution models, analytics/reporting, Python bindings, and
ADRs) is maintained locally during the design phase and will be published
with the first implementation release (v0.1.0).

### Contact

Joaquin Bejar — <jb@taunais.com>

## License

MIT — see [LICENSE](./LICENSE).
