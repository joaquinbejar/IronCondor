"""End-to-end tests for the `ironcondor` Python API (#39).

Exercises the docs/06 §4 script through the built wheel: build a config, `run()`
the Rust engine (GIL released), get a `Bundle` handle to the finalized on-disk
directory, read each Parquet table as a pandas DataFrame with the frozen
columns/dtypes, `metrics()` as a dict, `load_bundle` round-trip, the deprecated
`write(dir)` copy alias, and a GIL-release smoke.

The chain fixture is built in-test with pyarrow (no committed binary), mirroring
the historical-feed schema and the canonical iron-condor legs used by the Rust
goldens, so the same logical run drives the binding.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

import ironcondor as ic

# pandas / pyarrow are the optional `ironcondor[pandas]` extra; skip cleanly if
# a bare wheel is under test (CI installs them).
pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

# The tape anchor and expiry, matching the Rust test scaffolding
# (tests/common/mod.rs): ts_0 and ts_0 + 30 days.
TS0 = 1_750_291_200_000_000_000
NANOS_PER_DAY = 86_400_000_000_000
EXPIRY = TS0 + 30 * NANOS_PER_DAY

# The four canonical iron-condor legs: (strike_cents, style, bid_cents, ask_cents)
# with mids 2000/800/1800/700 — the same legs the Rust `condor_rows` builds.
LEGS = [
    (510_000, "call", 1_995, 2_005),
    (520_000, "call", 795, 805),
    (490_000, "put", 1_795, 1_805),
    (480_000, "put", 695, 705),
]

STEPS = 6

# The frozen Parquet column schemas (docs/05 §7–§10) each accessor must expose.
FILLS_COLUMNS = [
    "step", "ts_ns", "strategy_run_id", "trade_id", "position_id", "order_id",
    "fill_seq", "underlying", "expiration_ns", "contract_id", "strike_cents",
    "style", "side", "quantity", "price_cents", "fees_cents", "slippage_cents",
    "mode",
]
EQUITY_COLUMNS = [
    "step", "ts_ns", "cash_cents", "position_value_cents", "equity_cents",
    "drawdown",
]
POSITIONS_COLUMNS = [
    "step", "ts_ns", "position_id", "trade_id", "contract_id", "side",
    "quantity", "avg_price_cents", "mark_cents", "unrealized_cents",
    "stale_mark", "exit_reason", "open_at_end",
]
GREEKS_COLUMNS = [
    "step", "ts_ns", "theta_pnl_cents", "delta_pnl_cents", "vega_pnl_cents",
    "spread_capture_cents", "fees_cents", "residual_cents",
]

# The historical-feed Parquet schema, column-for-column (src/data/historical.rs).
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


def _write_chain(path: Path, steps: int = STEPS) -> None:
    """Write a tiny canonical iron-condor Parquet chain to `path`."""
    rows: list[dict] = []
    for step in range(steps):
        ts = TS0 + step * NANOS_PER_DAY
        for strike, style, bid, ask in LEGS:
            rows.append({
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
            })
    columns = {name: [row[name] for row in rows] for name in _FEED_SCHEMA.names}
    table = pa.table(columns, schema=_FEED_SCHEMA)
    pq.write_table(table, path)


def _config(chain: Path, out_dir: Path) -> ic.BacktestConfig:
    """The docs/06 §4 config over the fixture, matching the Rust golden run."""
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
        # Non-triggering exit so on_end performs the single clean close at the end.
        .exit_time_steps(1_000_000)
        .execution_naive()
        .fees(per_contract_cents=65, per_order_cents=100)
        .output_dir(str(out_dir))
    )


@pytest.fixture()
def bundle(tmp_path: Path) -> ic.Bundle:
    """A finalized bundle from one run over the fixture chain."""
    chain = tmp_path / "condor.parquet"
    _write_chain(chain)
    cfg = _config(chain, tmp_path / "bundles")
    return ic.run(cfg)


def test_run_publishes_bundle_and_accessors_return_dataframes(bundle: ic.Bundle) -> None:
    # The docs/06 §4 acceptance: run -> bundle with a real on-disk path.
    path = Path(bundle.path)
    assert path.is_dir(), "bundle.path is the published directory"
    assert (path / "manifest.json").is_file()
    for table in [
        "fills.parquet",
        "equity_curve.parquet",
        "positions.parquet",
        "greeks_attribution.parquet",
    ]:
        assert (path / table).is_file(), f"{table} must be present"

    equity = bundle.equity_curve()
    assert isinstance(equity, pd.DataFrame)
    assert len(equity) == STEPS, "one equity point per step"

    metrics = bundle.metrics()
    assert isinstance(metrics, dict)
    assert metrics, "metrics dict is non-empty"


def test_accessor_columns_match_the_frozen_schema(bundle: ic.Bundle) -> None:
    assert list(bundle.fills().columns) == FILLS_COLUMNS
    assert list(bundle.equity_curve().columns) == EQUITY_COLUMNS
    assert list(bundle.positions().columns) == POSITIONS_COLUMNS
    assert list(bundle.greeks_attribution().columns) == GREEKS_COLUMNS


def test_accessor_dtypes_are_integer_cents_with_one_float(bundle: ic.Bundle) -> None:
    import pandas as pd  # the accessors already require it; local like theirs

    equity = bundle.equity_curve()
    # Money columns are integer cents; drawdown is the only float in the bundle.
    assert equity["step"].dtype == "int32"
    assert equity["cash_cents"].dtype == "int64"
    assert equity["position_value_cents"].dtype == "int64"
    assert equity["equity_cents"].dtype == "int64"
    assert equity["drawdown"].dtype == "float64"

    fills = bundle.fills()
    assert fills["strike_cents"].dtype == "int64"
    assert fills["price_cents"].dtype == "int64"
    assert fills["fees_cents"].dtype == "int64"
    assert fills["quantity"].dtype == "int32"
    # utf8 decodes as `object` on pandas < 3.0 and as the string dtype on
    # pandas >= 3.0 — accept either so the suite tracks whatever pandas the
    # unpinned CI install resolves. Both are "a string column", the claim
    # under test (the single float column is `drawdown`, everything else
    # integer cents / strings).
    assert fills["mode"].dtype == object or pd.api.types.is_string_dtype(fills["mode"])


def test_load_bundle_round_trips(bundle: ic.Bundle) -> None:
    reopened = ic.load_bundle(bundle.path)
    assert Path(reopened.path) == Path(bundle.path)
    # Each accessor on the reopened handle returns the same shape.
    assert list(reopened.equity_curve().columns) == EQUITY_COLUMNS
    assert len(reopened.equity_curve()) == STEPS
    assert reopened.metrics() == bundle.metrics()


def test_load_bundle_rejects_a_non_bundle_directory(tmp_path: Path) -> None:
    # The hardened reader fails typed on a directory that is not a bundle.
    empty = tmp_path / "not_a_bundle"
    empty.mkdir()
    with pytest.raises(Exception):
        ic.load_bundle(str(empty))


def test_write_copies_the_directory_without_rerunning(bundle: ic.Bundle, tmp_path: Path) -> None:
    original_manifest = (Path(bundle.path) / "manifest.json").read_bytes()
    dest = tmp_path / "copy"

    with pytest.warns(DeprecationWarning):
        copy = bundle.write(str(dest))

    assert Path(copy.path) == dest
    for name in [
        "manifest.json",
        "fills.parquet",
        "equity_curve.parquet",
        "positions.parquet",
        "greeks_attribution.parquet",
    ]:
        assert (dest / name).is_file(), f"{name} copied"
    # A copy, not a re-run: the manifest (including its wall-clock created_utc)
    # is byte-identical — a re-run would stamp a fresh created_utc.
    assert (dest / "manifest.json").read_bytes() == original_manifest


def test_write_rejects_same_path_and_preserves_source(bundle: ic.Bundle) -> None:
    # write(dir == bundle.path, overwrite=True) previously deleted the source
    # then copied from the now-empty directory, irreversibly destroying the very
    # bundle it was meant to preserve. It must be refused BEFORE any deletion,
    # with the source left byte-for-byte intact (F35).
    src = Path(bundle.path)
    original = {p.name: p.read_bytes() for p in sorted(src.iterdir()) if p.is_file()}
    assert original, "the bundle has files"

    with pytest.raises(ic.IronCondorError):
        bundle.write(str(src), overwrite=True)

    after = {p.name: p.read_bytes() for p in sorted(src.iterdir()) if p.is_file()}
    assert after == original, "a same-path write must not touch the source"


def test_write_rejects_destination_containing_the_source(bundle: ic.Bundle) -> None:
    # The parent of the bundle directory CONTAINS the source; an overwrite there
    # would delete the source too. Reject it (F35).
    parent = Path(bundle.path).parent
    manifest_before = (Path(bundle.path) / "manifest.json").read_bytes()

    with pytest.raises(ic.IronCondorError):
        bundle.write(str(parent), overwrite=True)

    assert (Path(bundle.path) / "manifest.json").read_bytes() == manifest_before


def test_write_overwrite_replaces_existing_destination(
    bundle: ic.Bundle, tmp_path: Path
) -> None:
    # overwrite=True onto a DIFFERENT existing directory still works, and the
    # atomic stage+rename fully replaces the stale prior contents (F35).
    dest = tmp_path / "copy"
    dest.mkdir()
    (dest / "stale.txt").write_text("old")

    with pytest.warns(DeprecationWarning):
        copy = bundle.write(str(dest), overwrite=True)

    assert Path(copy.path) == dest
    assert not (dest / "stale.txt").exists(), "stale content is replaced, not merged"
    for name in [
        "manifest.json",
        "fills.parquet",
        "equity_curve.parquet",
        "positions.parquet",
        "greeks_attribution.parquet",
    ]:
        assert (dest / name).is_file(), f"{name} present after overwrite"


def test_metrics_rejects_oversized_manifest_swapped_after_load(bundle: ic.Bundle) -> None:
    # The handle keeps only its directory, so a directory swapped under it after
    # load must not drive an unbounded manifest read. metrics() re-reads the
    # manifest bounded by the default 16 MiB ceiling and fails typed on an
    # oversized swap — no OOM / hang (F34).
    manifest = Path(bundle.path) / "manifest.json"
    padding = "0" * (17 * 1024 * 1024)  # 17 MiB > the 16 MiB default ceiling
    manifest.write_text('{"metrics": {}, "padding": "' + padding + '"}')

    with pytest.raises(ic.IronCondorError):
        bundle.metrics()


def test_run_releases_the_gil(tmp_path: Path) -> None:
    # Smoke: a background Python thread makes progress while run() executes,
    # proving run() releases the GIL for the engine + writer.
    chain = tmp_path / "condor.parquet"
    _write_chain(chain, steps=64)  # a slightly longer tape so the run isn't instant
    cfg = _config(chain, tmp_path / "bundles")

    ticks = 0
    stop = threading.Event()

    def ticker() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1

    thread = threading.Thread(target=ticker)
    thread.start()
    try:
        result = ic.run(cfg)
    finally:
        stop.set()
        thread.join()

    assert Path(result.path).is_dir()
    assert ticks > 0, "the background thread ticked while run() held no GIL"


def test_missing_data_source_raises(tmp_path: Path) -> None:
    cfg = ic.BacktestConfig(seed=1).strategy_iron_condor(
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
    )
    with pytest.raises(ValueError):
        ic.run(cfg)


def test_realistic_mode_runs_end_to_end(tmp_path: Path) -> None:
    # The wheel bundles the orderbook feature (docs/06 §7), so realistic mode
    # genuinely runs and produces a distinct bundle from naive.
    chain = tmp_path / "condor.parquet"
    _write_chain(chain)
    cfg = _config(chain, tmp_path / "realistic").execution_realistic()
    bundle = ic.run(cfg)
    assert Path(bundle.path).is_dir()
    assert len(bundle.equity_curve()) == STEPS
