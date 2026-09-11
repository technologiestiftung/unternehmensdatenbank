"""Shared fixtures. Thin wrappers over `tests/loaders.py`, which is where the work happens.

Deliberately thin: the tests also run as plain scripts, without pytest, and a fixture body is
unreachable from there. Every fixture here is one line over a cached loader, so both modes get the
same object and there is nothing to keep in sync.
"""
from __future__ import annotations

import pytest

from tests import loaders


@pytest.fixture(scope="session")
def runner():
    """`run_combined_discovery.py`, exec'd once for the whole session — it is 4,115 lines."""
    return loaders.runner()


@pytest.fixture(scope="session")
def branches():
    """`branches.jsonl` — column 1, the Technologiefeld, one row per company."""
    return loaders.rows("branches.jsonl")


@pytest.fixture(scope="session")
def purposes():
    """`purpose_branches.jsonl` — column 3, the register purpose, one row per company."""
    return loaders.rows("purpose_branches.jsonl")


@pytest.fixture()
def cursor():
    """A database cursor on its own connection, closed after every test. Function-scoped."""
    with loaders.db_cursor() as cur:
        yield cur
