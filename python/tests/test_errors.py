"""FFI error-mapping and no-panic-across-FFI tests for `ironcondor` (#40).

Every `BacktestError` kind that is reachable through the public Python API is
driven to its typed exception and asserted; the `ic.IronCondorError` base and
the `ValueError` / `OSError` (`IOError`) builtin bases are asserted to catch the
right classes; and an induced Rust panic is proven to surface as
`ic.EngineError` (a catchable `Exception`) rather than aborting the interpreter.

The remaining kinds that cannot be induced from the exposed surface
(`Execution` / `OrderBook` / `Strategy` / `ArithmeticOverflow`) are covered
exhaustively by the Rust-side per-variant mapping test in `src/python/errors.rs`.

The chain fixtures are built in-test with pyarrow (no committed binary),
mirroring the historical-feed schema — the same approach as `test_api.py`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import ironcondor as ic

# pyarrow builds the fixture chains; skip cleanly on a bare wheel (CI installs it).
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

TS0 = 1_750_291_200_000_000_000
NANOS_PER_DAY = 86_400_000_000_000
EXPIRY = TS0 + 30 * NANOS_PER_DAY

# (strike_cents, style, bid_cents, ask_cents) — the canonical iron-condor legs.
LEGS = [
    (510_000, "call", 1_995, 2_005),
    (520_000, "call", 795, 805),
    (490_000, "put", 1_795, 1_805),
    (480_000, "put", 695, 705),
]
STEPS = 4

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


def _row(step: int, ts: int, strike: int, style: str, bid: int, ask: int) -> dict:
    return {
        "step": step,
        "ts": ts,
        "underlying": "SPX",
        "underlying_price": 500_000,
        "tick_size": 5,
        "contract_multiplier": 100,
        "expiration": EXPIRY,
        "strike": strike,
        "style": style,
        "bid": bid,
        "ask": ask,
        "bid_size": 50,
        "ask_size": 50,
        "implied_volatility": 0.2,
        "delta": 0.3,
        "gamma": 0.01,
        "theta": -0.05,
        "vega": 0.1,
    }


def _write_rows(path: Path, rows: list[dict]) -> None:
    columns = {name: [row[name] for row in rows] for name in _FEED_SCHEMA.names}
    pq.write_table(pa.table(columns, schema=_FEED_SCHEMA), path)


def _valid_chain(path: Path) -> None:
    rows = [
        _row(step, TS0 + step * NANOS_PER_DAY, strike, style, bid, ask)
        for step in range(STEPS)
        for strike, style, bid, ask in LEGS
    ]
    _write_rows(path, rows)


def _crossed_quote_chain(path: Path) -> None:
    """One tick-aligned row whose bid exceeds its ask -> BacktestError::CrossedQuote."""
    rows = [_row(0, TS0, 510_000, "call", 2_010, 2_000)]
    _write_rows(path, rows)


def _out_of_order_chain(path: Path) -> None:
    """Steps ascend, timestamps descend -> BacktestError::DataOutOfOrder."""
    rows = []
    for step, ts in ((0, TS0 + NANOS_PER_DAY), (1, TS0)):
        rows.extend(
            _row(step, ts, strike, style, bid, ask)
            for strike, style, bid, ask in LEGS
        )
    _write_rows(path, rows)


def _strategy_kwargs(underlying: str = "SPX", quantity: int = 1) -> dict:
    return {
        "underlying": underlying,
        "underlying_price_cents": 500_000,
        "short_call_strike_cents": 510_000,
        "short_put_strike_cents": 490_000,
        "long_call_strike_cents": 520_000,
        "long_put_strike_cents": 480_000,
        "expiration_ns": EXPIRY,
        "quantity": quantity,
        "premium_short_call_cents": 2_000,
        "premium_short_put_cents": 1_800,
        "premium_long_call_cents": 800,
        "premium_long_put_cents": 700,
    }


def _config(chain: Path, out_dir: Path) -> ic.BacktestConfig:
    return (
        ic.BacktestConfig(seed=42, capital_cents=10_000_000)
        .data_parquet(str(chain))
        .strategy_iron_condor(**_strategy_kwargs())
        .exit_time_steps(1_000_000)
        .execution_naive()
        .output_dir(str(out_dir))
    )


# --- The exception hierarchy is structurally correct ------------------------


def test_all_classes_descend_from_the_common_base() -> None:
    for cls in (
        ic.ConfigError,
        ic.DataError,
        ic.ExecutionError,
        ic.StrategyError,
        ic.BundleError,
        ic.EngineError,
    ):
        assert issubclass(cls, ic.IronCondorError)
    assert issubclass(ic.IronCondorError, Exception)


def test_config_error_is_also_a_value_error() -> None:
    assert issubclass(ic.ConfigError, ValueError)
    assert issubclass(ic.ConfigError, ic.IronCondorError)


def test_data_and_bundle_errors_are_also_os_errors() -> None:
    # IOError is a Python-3 alias of OSError, so `except IOError` catches these.
    assert IOError is OSError
    for cls in (ic.DataError, ic.BundleError):
        assert issubclass(cls, OSError)
        assert issubclass(cls, ic.IronCondorError)


def test_single_base_errors_are_not_value_or_os_errors() -> None:
    for cls in (ic.ExecutionError, ic.StrategyError, ic.EngineError):
        assert not issubclass(cls, ValueError)
        assert not issubclass(cls, OSError)


# --- ConfigError (⊂ ValueError) via the real API ----------------------------


def test_missing_data_source_raises_config_error(tmp_path: Path) -> None:
    cfg = ic.BacktestConfig(seed=1).strategy_iron_condor(**_strategy_kwargs())
    with pytest.raises(ic.ConfigError) as exc:
        ic.run(cfg)
    assert isinstance(exc.value, ValueError)
    assert isinstance(exc.value, ic.IronCondorError)


def test_zero_capital_raises_config_error(tmp_path: Path) -> None:
    chain = tmp_path / "chain.parquet"
    _valid_chain(chain)
    cfg = (
        ic.BacktestConfig(seed=1, capital_cents=0)
        .data_parquet(str(chain))
        .strategy_iron_condor(**_strategy_kwargs())
        .output_dir(str(tmp_path / "out"))
    )
    with pytest.raises(ic.ConfigError):
        ic.run(cfg)


def test_zero_quantity_raises_config_error() -> None:
    # InvalidQuantity is raised at builder time by Quantity::new(0).
    with pytest.raises(ic.ConfigError) as exc:
        ic.BacktestConfig(seed=1).strategy_iron_condor(**_strategy_kwargs(quantity=0))
    assert isinstance(exc.value, ValueError)


def test_crossed_quote_chain_raises_config_error(tmp_path: Path) -> None:
    # A crossed quote (bid > ask) maps to ConfigError per the spec table.
    chain = tmp_path / "crossed.parquet"
    _crossed_quote_chain(chain)
    cfg = (
        ic.BacktestConfig(seed=1, capital_cents=10_000_000)
        .data_parquet(str(chain))
        .strategy_iron_condor(**_strategy_kwargs())
        .output_dir(str(tmp_path / "out"))
    )
    with pytest.raises(ic.ConfigError) as exc:
        ic.run(cfg)
    assert isinstance(exc.value, ValueError)


# --- DataError (⊂ OSError/IOError) via the real API -------------------------


def test_missing_parquet_file_raises_data_error(tmp_path: Path) -> None:
    cfg = (
        ic.BacktestConfig(seed=1, capital_cents=10_000_000)
        .data_parquet(str(tmp_path / "does_not_exist.parquet"))
        .strategy_iron_condor(**_strategy_kwargs())
        .output_dir(str(tmp_path / "out"))
    )
    with pytest.raises(ic.DataError) as exc:
        ic.run(cfg)
    assert isinstance(exc.value, OSError)  # IOError alias
    assert isinstance(exc.value, ic.IronCondorError)


def test_out_of_order_tape_raises_data_error(tmp_path: Path) -> None:
    chain = tmp_path / "out_of_order.parquet"
    _out_of_order_chain(chain)
    cfg = (
        ic.BacktestConfig(seed=1, capital_cents=10_000_000)
        .data_parquet(str(chain))
        .strategy_iron_condor(**_strategy_kwargs())
        .output_dir(str(tmp_path / "out"))
    )
    with pytest.raises(ic.DataError):
        ic.run(cfg)


def test_corrupt_parquet_raises_data_error(tmp_path: Path) -> None:
    # Undecodable bytes -> BacktestError::Data / DataIo, both -> ic.DataError.
    chain = tmp_path / "corrupt.parquet"
    chain.write_bytes(b"this is not a parquet file at all")
    cfg = (
        ic.BacktestConfig(seed=1, capital_cents=10_000_000)
        .data_parquet(str(chain))
        .strategy_iron_condor(**_strategy_kwargs())
        .output_dir(str(tmp_path / "out"))
    )
    with pytest.raises(ic.DataError):
        ic.run(cfg)


def test_invalid_underlying_ticker_raises_data_error() -> None:
    # Underlying::new rejects a bad ticker as BacktestError::Conversion (DataError),
    # at builder time.
    with pytest.raises(ic.DataError):
        ic.BacktestConfig(seed=1).strategy_iron_condor(
            **_strategy_kwargs(underlying="bad-ticker!")
        )


# --- BundleError (⊂ OSError/IOError) via load_bundle ------------------------


def test_load_non_bundle_directory_raises_bundle_error(tmp_path: Path) -> None:
    empty = tmp_path / "not_a_bundle"
    empty.mkdir()
    with pytest.raises(ic.BundleError) as exc:
        ic.load_bundle(str(empty))
    assert isinstance(exc.value, OSError)  # IOError alias
    assert isinstance(exc.value, ic.IronCondorError)


# --- The base clause catches everything -------------------------------------


def test_except_ironcondor_error_catches_every_engine_error(tmp_path: Path) -> None:
    chain = tmp_path / "chain.parquet"
    _out_of_order_chain(chain)
    cfg = (
        ic.BacktestConfig(seed=1, capital_cents=10_000_000)
        .data_parquet(str(chain))
        .strategy_iron_condor(**_strategy_kwargs())
        .output_dir(str(tmp_path / "out"))
    )
    caught = False
    try:
        ic.run(cfg)
    except ic.IronCondorError:
        caught = True
    assert caught, "except ic.IronCondorError must catch the engine error"


# --- No panic crosses the FFI boundary --------------------------------------


def test_induced_panic_surfaces_as_engine_error() -> None:
    with pytest.raises(ic.EngineError) as exc:
        ic._panic_for_test("kaboom from rust")
    # The panic message is preserved, and the class descends from the base.
    assert "kaboom from rust" in str(exc.value)
    assert isinstance(exc.value, ic.IronCondorError)


def test_induced_panic_is_caught_by_the_base_clause_and_interpreter_survives() -> None:
    # A BaseException-derived PanicException would slip past `except Exception`;
    # our EngineError does not — and the interpreter keeps running afterwards.
    caught_as_base = False
    try:
        ic._panic_for_test("still alive")
    except ic.IronCondorError:
        caught_as_base = True
    assert caught_as_base
    # Reaching here at all proves the interpreter was not aborted.
    assert 1 + 1 == 2
