"""Where things are. Imported by `conftest.py` and by the tests' standalone path alike.

A plain module rather than part of `conftest.py` on purpose. `conftest.py` is imported by pytest and
by nothing else, and these tests must also run as plain scripts — that is what the `PostToolUse`
hook does, and what the Airflow image does, since it has no pytest. Anything both modes need has to
live somewhere an ordinary `import` can reach.
"""
from __future__ import annotations

import sys
from pathlib import Path

#: The repo root. `parents[1]` from `tests/paths.py`; the test modules sit one level deeper and
#: bootstrap themselves with `parents[2]` before importing this.
REPO = Path(__file__).resolve().parents[1]

#: `run_combined_discovery.py`, loaded by the columns test for `latest_rows`, `read_label_csv` and
#: `LABEL_VALUES`. Spelled out from REPO rather than relative to the test file: it used to be
#: `Path(__file__).parent / "run_combined_discovery.py"`, which was correct only while the test
#: lived in `scripts/` beside it.
RUNNER_PATH = REPO / "scripts" / "run_combined_discovery.py"

#: Host-shaped default for Postgres. Set before `source_enrichment_common` is imported anywhere,
#: because that module reads `PG_DSN` at import time. The DAG default (`host=postgres port=5432`)
#: is right inside a container and wrong outside one. Same line as
#: `scripts/import_northdata_websites.py:95`.
PG_DSN_HOST_DEFAULT = "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"


def artifacts() -> Path:
    """The combined-discovery output directory. Same resolution order as the script that writes it.

    A function rather than a constant so a missing directory fails inside a test with a readable
    message, instead of raising at import time — which pytest reports as a collection error that
    names no test at all.
    """
    for path in (
        Path("/opt/airflow/data/combined_discovery"),
        REPO / "airflow" / "data" / "combined_discovery",
    ):
        if path.is_dir():
            return path
    raise AssertionError(
        "no combined_discovery output directory; run `classify` and `classify-purpose` first"
    )


def add_dags_to_path() -> None:
    """Put `airflow/dags` on `sys.path` so the DAG modules can be imported.

    Under pytest this is redundant — `pytest.ini` sets `pythonpath = airflow/dags` — but the
    standalone path has no pytest to do it.
    """
    for candidate in (REPO / "airflow" / "dags", Path("/opt/airflow/dags")):
        if candidate.is_dir():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return
