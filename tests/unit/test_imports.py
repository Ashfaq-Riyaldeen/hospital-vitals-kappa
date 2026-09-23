"""Every module in the package must import.

This exists because of a real failure: the foundations were adapted from the sibling
project with a blanket rename, which turned `fleet.common` into `ward.common` -- a
package that does not exist here, since this project uses a flat layout. Four modules
were broken and nothing said so until a test happened to import one of them.

An import smoke test is the cheapest possible guard against an incomplete refactor,
and refactors that miss call sites are the recurring failure mode on this codebase.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest
import ward

# Modules whose imports are genuinely optional at test time: they need a Spark
# session or a live Cassandra cluster that the unit suite deliberately does not have.
NEEDS_INFRASTRUCTURE = ("ward.stream.", "ward.store.session", "ward.store.dao")

MODULES = sorted(
    m.name
    for m in pkgutil.walk_packages(ward.__path__, prefix="ward.")
    if not m.name.startswith(NEEDS_INFRASTRUCTURE)
)


def test_the_package_has_modules_to_check() -> None:
    """Guards against this whole file passing vacuously if discovery breaks."""
    assert len(MODULES) >= 5, f"only found {MODULES}"


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name: str) -> None:
    importlib.import_module(name)
