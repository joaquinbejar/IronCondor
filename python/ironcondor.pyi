# Type stubs for the `ironcondor` extension module.
#
# The working v0.4 surface (#39): the `BacktestConfig` integer-cents builder,
# `run`, and the `Bundle` handle with lazy DataFrame accessors. Shipped in the
# wheel (PEP 561, via `py.typed`) so editors and type-checkers see the surface.
# The typed exception hierarchy rooted at `IronCondorError` lands in #40.
#
# Money crosses as INTEGER CENTS everywhere (names say `*_cents`); the only
# floats are the documented analytic exception — implied volatility, the two
# rates, and the `ExitPolicy` percentages (docs/06 §4, ADR-0003).
#
# DataFrame accessors return `Any` rather than `pandas.DataFrame` because pandas
# is an OPTIONAL extra (`pip install ironcondor[pandas]`); a hard `pandas` import
# here would force it on every type-checker.

from typing import Any

__version__: str

# --- Exception hierarchy (#40) ---------------------------------------------
# Every engine exception descends from `IronCondorError`, so `except
# IronCondorError` catches them all. `ConfigError` is also a `ValueError`;
# `DataError` and `BundleError` are also `OSError` (Python-3 `IOError`), so the
# idiomatic builtin `except` clauses keep working. The `BacktestError` → class
# mapping lives in `src/python/errors.rs`.

class IronCondorError(Exception):
    """Common base for every exception the engine raises."""

class ConfigError(IronCondorError, ValueError):
    """Invalid configuration or input value (also a `ValueError`)."""

class DataError(IronCondorError, OSError):
    """Data-source, conversion, tape, or session failure (also an `OSError`)."""

class ExecutionError(IronCondorError):
    """Fill-model or order-book execution failure."""

class StrategyError(IronCondorError):
    """Strategy-adapter failure surfaced from optionstratlib."""

class BundleError(IronCondorError, OSError):
    """Result-bundle write or read-back failure (also an `OSError`)."""

class EngineError(IronCondorError):
    """Arithmetic overflow, or an unexpected internal panic caught at the
    FFI boundary (no panic ever crosses into the interpreter)."""

def _panic_for_test(message: str) -> None:
    """Internal test hook: induces a Rust panic to prove it surfaces as
    `EngineError`, never an aborted interpreter. Not part of the public API."""
    ...

class BacktestConfig:
    """Chainable, integer-cents builder over the Rust `BacktestConfig`.

    Each builder mutates the config in place and returns it, so calls chain:
    `BacktestConfig(seed=42, capital_cents=1_000_000).data_parquet(...)...`.
    """

    def __init__(self, seed: int = ..., capital_cents: int = ...) -> None: ...
    def seed(self, seed: int) -> BacktestConfig: ...
    def capital_cents(self, capital_cents: int) -> BacktestConfig: ...
    def data_parquet(self, path: str) -> BacktestConfig: ...
    def strategy_iron_condor(
        self,
        underlying: str,
        underlying_price_cents: int,
        short_call_strike_cents: int,
        short_put_strike_cents: int,
        long_call_strike_cents: int,
        long_put_strike_cents: int,
        expiration_ns: int,
        quantity: int,
        premium_short_call_cents: int,
        premium_short_put_cents: int,
        premium_long_call_cents: int,
        premium_long_put_cents: int,
        implied_volatility: float = ...,
        risk_free_rate: float = ...,
        dividend_yield: float = ...,
        open_fee_cents: int = ...,
        close_fee_cents: int = ...,
    ) -> BacktestConfig: ...
    def execution_naive(self, slippage_cents: int | None = ...) -> BacktestConfig: ...
    def execution_realistic(self) -> BacktestConfig: ...
    def fees(
        self, per_contract_cents: int = ..., per_order_cents: int = ...
    ) -> BacktestConfig: ...
    def exit_profit_percent(self, percent: float) -> BacktestConfig: ...
    def exit_loss_percent(self, percent: float) -> BacktestConfig: ...
    def exit_time_steps(self, steps: int) -> BacktestConfig: ...
    def exit_expiration(self) -> BacktestConfig: ...
    def output_dir(self, path: str) -> BacktestConfig: ...
    def overwrite(self, overwrite: bool = ...) -> BacktestConfig: ...

class Bundle:
    """A handle to a finalized on-disk result bundle directory.

    The accessors are lazy: each reads the on-disk Parquet / JSON on call.
    """

    @property
    def path(self) -> str: ...
    def fills(self) -> Any: ...  # pandas.DataFrame
    def equity_curve(self) -> Any: ...  # pandas.DataFrame
    def positions(self) -> Any: ...  # pandas.DataFrame
    def greeks_attribution(self) -> Any: ...  # pandas.DataFrame
    def metrics(self) -> dict[str, Any]: ...
    def write(self, dir: str, overwrite: bool = ...) -> Bundle: ...

def run(config: BacktestConfig) -> Bundle:
    """Run one backtest end to end (releasing the GIL) and return a `Bundle`
    handle to the finalized `<output_dir>/<run_id>/` directory."""
    ...

def load_bundle(dir: str) -> Bundle:
    """Open an existing on-disk bundle, validated through the hardened reader."""
    ...
