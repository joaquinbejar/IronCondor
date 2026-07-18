"""Python-side determinism parity for the `ironcondor` wheel (#42).

Two complementary checks run against the built + installed wheel in the
`python-wheels` CI job (docs/TESTING.md §7):

1. **Run-twice byte-identity** — `ic.run(cfg)` over the same inputs, twice, into
   two output directories, produces byte-identical Parquet tables and a
   `manifest.json` identical after stripping the wall-clock `created_utc` (and
   canonicalising the operational `output_dir`, which legitimately differs). The
   binding adds *no* non-determinism: its writer *is* the Rust writer.

2. **Golden-table reproduction** — a single `ic.run()` over the canonical
   iron-condor chain reproduces the committed `iron_condor_naive` golden's frozen
   Parquet tables, compared with a **lightweight pandas mirror** of the single
   comparison oracle (`tests/oracle/mod.rs`): sort by each table's pinned key,
   compare integer/id/string columns exactly and the one float column
   (`drawdown`) within the stated tolerance.

   This pandas mirror is a **documented lightweight shadow** of the Rust oracle,
   not a second oracle — the authoritative comparator is `tests/oracle`, reused
   by the Rust golden and the Rust parity test (`tests/python_parity.rs`). Two
   caveats, both a consequence of the #36 golden freeze:

   - We compare **tables, not the manifest**, to the golden. The golden's
     `manifest` / `run_id` are pinned to canonical data-identity constants
     (`tests/bundle_golden.rs`) so they are decoupled from the tempdir chain
     bytes; a real `ic.run()` over a freshly generated chain computes the *real*
     tape `sha256`, so its `run_id` legitimately differs. The four tables depend
     only on `(seed, config, data)`.
   - The one exception is `fills.strategy_run_id`, which **is** the `run_id`
     stamped into every fill row. Because the golden pins the `run_id`, that
     single column differs; we exclude it and compare every other fill column
     exactly. All P&L-substantive content matches.

   The chain is generated in-test with pyarrow, mirroring `condor_rows` in
   `tests/common/mod.rs` value-for-value (same schema, same 8 steps, same legs),
   so the *decoded* chain content is identical to the Rust generator's even if
   the Parquet container framing differs — and the tables, which decode content,
   therefore match.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import ironcondor as ic

# pandas / pyarrow are the `ironcondor[pandas]` extra the comparison needs; the
# CI wheel job installs them. Skip cleanly on a bare wheel.
pd = pytest.importorskip("pandas")
np = pytest.importorskip("numpy")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

# The tape anchor + expiry, matching tests/common/mod.rs (ts_0 and ts_0 + 30d).
TS0 = 1_750_291_200_000_000_000
NANOS_PER_DAY = 86_400_000_000_000
EXPIRY = TS0 + 30 * NANOS_PER_DAY

# The four canonical iron-condor legs (strike_cents, style, bid_cents, ask_cents),
# mids 2000/800/1800/700 — the same legs `condor_rows` builds.
LEGS = [
    (510_000, "call", 1_995, 2_005),
    (520_000, "call", 795, 805),
    (490_000, "put", 1_795, 1_805),
    (480_000, "put", 695, 705),
]

# The golden was blessed over GOLDEN_STEPS = 8 (tests/bundle_golden.rs); the
# golden-table comparison must use the same step count to reproduce it.
GOLDEN_STEPS = 8

# The four bundle tables and their oracle sort keys (tests/oracle/mod.rs).
SORT_KEYS = {
    "fills.parquet": ["step", "order_id", "fill_seq"],
    "equity_curve.parquet": ["step"],
    "positions.parquet": ["step", "position_id"],
    "greeks_attribution.parquet": ["step"],
}
TABLE_FILES = list(SORT_KEYS)

# The oracle's fixed cross-environment float tolerance (docs/05 §12.5):
# |a - b| <= max(ABS, REL * max(|a|, |b|)).
ABS_TOLERANCE = 1e-9
REL_TOLERANCE = 1e-6

# The `fills` column that carries the run_id — pinned in the golden, so excluded
# from the golden-table comparison (see the module docstring).
RUN_ID_COLUMN = "strategy_run_id"

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


def _golden_expected_dir() -> Path:
    """The committed `iron_condor_naive` golden bundle directory."""
    # python/tests/test_parity.py -> repo root is parents[2].
    return (
        Path(__file__).resolve().parents[2]
        / "tests" / "golden" / "iron_condor_naive" / "expected"
    )


def _write_chain(path: Path, steps: int = GOLDEN_STEPS) -> None:
    """Write the canonical iron-condor Parquet chain (mirrors `condor_rows`)."""
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
    pq.write_table(pa.table(columns, schema=_FEED_SCHEMA), path)


def _config(chain: Path, out_dir: Path) -> ic.BacktestConfig:
    """The shared canonical config (matches the `iron_condor_naive` golden)."""
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


def _canonical_manifest(bundle_dir: Path) -> dict:
    """Mirror `oracle::canonical_manifest`: drop the wall-clock `created_utc` and
    canonicalise the operational `config.output_dir` (both non-semantic)."""
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    manifest.pop("created_utc", None)
    config = manifest.get("config")
    if isinstance(config, dict) and "output_dir" in config:
        config["output_dir"] = "<output_dir>"
    return manifest


def _read_sorted(path: Path, sort_cols: list[str]) -> pd.DataFrame:
    """Read a bundle table and stable-sort it by its oracle key — the mirror's
    normalisation step before comparison."""
    df = pd.read_parquet(path)
    return df.sort_values(sort_cols, kind="stable").reset_index(drop=True)


# --- 1. The binding adds no non-determinism (run-twice byte-identity) --------


def test_run_twice_is_byte_identical(tmp_path: Path) -> None:
    chain = tmp_path / "condor.parquet"
    _write_chain(chain)

    dir_a = Path(ic.run(_config(chain, tmp_path / "a")).path)
    dir_b = Path(ic.run(_config(chain, tmp_path / "b")).path)

    # Same (seed, config, data) => same run_id (the bundle directory name),
    # independent of the two distinct operational output roots.
    assert dir_a.name == dir_b.name, "the deterministic run_id must match across two runs"

    # The four Parquet tables are byte-for-byte identical (they never embed the
    # output path).
    for name in TABLE_FILES:
        assert (dir_a / name).read_bytes() == (dir_b / name).read_bytes(), (
            f"{name} must be byte-identical across two runs"
        )

    # The manifest is identical after stripping created_utc + canonicalising the
    # operational output_dir (which differs between the two output roots).
    assert _canonical_manifest(dir_a) == _canonical_manifest(dir_b), (
        "manifest.json must be identical after stripping created_utc + output_dir"
    )


# --- 2. Golden-table reproduction (lightweight pandas mirror of the oracle) ---


def _assert_exact_table(produced: Path, golden: Path, sort_cols: list[str],
                        drop: list[str] | None = None) -> None:
    """Full exact comparison of a bundle table against the golden: sort by the
    oracle key, optionally drop a column, then assert frame equality (integers /
    ids / strings / bools are all exact — no float columns in these tables)."""
    p = _read_sorted(produced, sort_cols)
    g = _read_sorted(golden, sort_cols)
    if drop:
        p = p.drop(columns=drop)
        g = g.drop(columns=drop)
    pd.testing.assert_frame_equal(p, g, check_dtype=True)


def _assert_equity_table(produced: Path, golden: Path) -> None:
    """Equity-curve comparison: integer-cents columns exact, `drawdown` (the one
    float column) within the oracle's fixed tolerance."""
    p = _read_sorted(produced, ["step"])
    g = _read_sorted(golden, ["step"])
    assert len(p) == len(g), "equity_curve row count must match the golden"
    for col in ("step", "ts_ns", "cash_cents", "position_value_cents", "equity_cents"):
        assert (p[col].to_numpy() == g[col].to_numpy()).all(), (
            f"equity_curve.{col} must be exact integer cents"
        )
    a = p["drawdown"].to_numpy()
    b = g["drawdown"].to_numpy()
    tol = np.maximum(ABS_TOLERANCE, REL_TOLERANCE * np.maximum(np.abs(a), np.abs(b)))
    assert (np.abs(a - b) <= tol).all(), "drawdown must match the golden within tolerance"


def test_run_reproduces_the_golden_tables(tmp_path: Path) -> None:
    golden = _golden_expected_dir()
    if not all((golden / name).is_file() for name in TABLE_FILES):
        pytest.skip(f"committed golden tables not found at {golden}")

    chain = tmp_path / "condor.parquet"
    _write_chain(chain)
    produced = Path(ic.run(_config(chain, tmp_path / "out")).path)

    # equity_curve — integers exact, drawdown tolerant.
    _assert_equity_table(produced / "equity_curve.parquet", golden / "equity_curve.parquet")

    # positions + greeks_attribution — every column exact.
    _assert_exact_table(
        produced / "positions.parquet", golden / "positions.parquet",
        SORT_KEYS["positions.parquet"],
    )
    _assert_exact_table(
        produced / "greeks_attribution.parquet", golden / "greeks_attribution.parquet",
        SORT_KEYS["greeks_attribution.parquet"],
    )

    # fills — every column exact EXCEPT strategy_run_id (the golden pins the
    # run_id; a real run computes its own — see the module docstring).
    _assert_exact_table(
        produced / "fills.parquet", golden / "fills.parquet",
        SORT_KEYS["fills.parquet"], drop=[RUN_ID_COLUMN],
    )


def test_run_and_golden_run_ids_differ_by_design(tmp_path: Path) -> None:
    # Documents the #36 pinning: a real run's run_id (in fills.strategy_run_id and
    # the manifest) is NOT the golden's pinned canonical run_id — which is exactly
    # why the golden comparison above is on tables (minus that column), not the
    # manifest.
    golden = _golden_expected_dir()
    if not (golden / "fills.parquet").is_file():
        pytest.skip(f"committed golden not found at {golden}")

    chain = tmp_path / "condor.parquet"
    _write_chain(chain)
    produced = Path(ic.run(_config(chain, tmp_path / "out")).path)

    produced_run_id = pd.read_parquet(produced / "fills.parquet")[RUN_ID_COLUMN].iloc[0]
    golden_run_id = pd.read_parquet(golden / "fills.parquet")[RUN_ID_COLUMN].iloc[0]
    assert produced_run_id != golden_run_id, (
        "a real run computes its own run_id; the golden's is a pinned canonical constant"
    )
