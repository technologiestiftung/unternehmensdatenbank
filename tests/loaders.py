"""What the tests read, loaded once and cached.

These sit beside `paths.py` rather than inside `conftest.py` for the same reason: both run modes
need them. `conftest.py` turns each one into a fixture; the tests' own `_run_without_pytest` calls
them directly. Keeping them here is what stops `conftest.py` having to import a test module to
reach its loaders.
"""
from __future__ import annotations

import importlib.util
import json
import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Iterator

from tests.paths import PG_DSN_HOST_DEFAULT, RUNNER_PATH, add_dags_to_path, artifacts


@lru_cache(maxsize=None)
def runner() -> Any:
    """`scripts/run_combined_discovery.py` as a module, without running its CLI.

    It is a script rather than a package, so it cannot be imported by name. Only pure functions and
    module constants are read from it — `latest_rows`, `read_label_csv`, `LABEL_VALUES` — because a
    test that re-implements which line of an append-only file wins agrees with itself no matter
    what the pipeline does. It is 4,115 lines, hence the cache.
    """
    add_dags_to_path()
    spec = importlib.util.spec_from_file_location("_ground_truth_runner", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=None)
def rows(name: str) -> tuple[dict, ...]:
    """One dict per line. These files are one row per company, not append-only like `results`."""
    path = artifacts() / name
    if not path.exists():
        raise AssertionError(f"no {path}; run `classify` and `classify-purpose` first")
    with path.open(encoding="utf-8") as handle:
        return tuple(json.loads(line) for line in handle if line.strip())


@contextmanager
def db_cursor() -> Iterator[Any]:
    """A cursor on its own connection, closed on the way out.

    Short-lived on purpose: a test that leaves a transaction open cannot then affect the next one,
    and `source_enrichment_common` sets an `idle_in_transaction_session_timeout` on the role, which
    is the other reason not to hold one open across tests.
    """
    add_dags_to_path()
    os.environ.setdefault("PG_DSN", PG_DSN_HOST_DEFAULT)
    from source_enrichment_common import get_connection      # noqa: PLC0415 — after PG_DSN is set

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            yield cur
    finally:
        connection.close()
