"""Freeze the PyO3 public surface for SemVer 1.0 (#49).

The set of names the `ironcondor` extension module exposes at runtime is one of
the four frozen SemVer surfaces (``docs/SEMVER.md`` §"What counts as a public
surface"; the human-readable snapshot is ``python/ironcondor.pyi``). This test
compares the runtime-exposed names — ``dir(ironcondor)`` minus the Python-
injected module dunders — against the committed ``expected_public_names.txt``.

Adding or removing a ``#[pymodule]``-registered name in ``src/python/*`` changes
this set and **fails the test** until ``expected_public_names.txt`` is updated in
the same PR — the visible SemVer event. The same comparison runs as an explicit
named step in the ``python-wheels`` CI job against the built wheel.
"""

import types
from pathlib import Path

import ironcondor

# Attributes Python / the packaging inject on every module object; not part of
# the ironcondor surface. `__version__` and `_panic_for_test` are OURS
# (registered by the `#[pymodule]`) and are intentionally NOT in this set, so
# they stay pinned. The maturin mixed-layout wheel also exposes an `ironcondor`
# submodule self-reference (the compiled `.abi3.so`); it is a packaging artifact,
# not an API item, and is excluded below by dropping every submodule attribute.
_AUTO_DUNDERS = {
    "__name__",
    "__doc__",
    "__package__",
    "__loader__",
    "__spec__",
    "__file__",
    "__builtins__",
    "__path__",
    "__all__",
    "__dict__",
    "__cached__",
}


def _expected() -> set[str]:
    path = Path(__file__).with_name("expected_public_names.txt")
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def _exposed() -> set[str]:
    return {
        name
        for name in dir(ironcondor)
        if name not in _AUTO_DUNDERS
        and not isinstance(getattr(ironcondor, name, None), types.ModuleType)
    }


def test_pyo3_surface_matches_frozen_name_list() -> None:
    exposed = _exposed()
    expected = _expected()
    missing = sorted(expected - exposed)
    added = sorted(exposed - expected)
    assert exposed == expected, (
        f"PyO3 surface drift — missing={missing} added={added}; update "
        "python/tests/expected_public_names.txt (and ironcondor.pyi) and record "
        "the SemVer event (docs/SEMVER.md)"
    )
