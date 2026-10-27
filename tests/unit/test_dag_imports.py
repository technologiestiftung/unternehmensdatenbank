"""Two DAG packages each have a `scorer.py`; one process must load both without mixing them up.

They used to import it as a bare `from scorer import ...` after putting their own folder on
`sys.path`. Whichever loaded first was cached under the name `scorer`, so `airflow dags test` (one
process, every DAG) handed `url_discovery` the OSM scorer and failed with an ImportError.
"""
from __future__ import annotations


def test_osm_then_url_discovery_get_their_own_scorer() -> None:
    import osm_discovery.main  # noqa: F401 — loaded first on purpose, that is the failing order
    import url_discovery.main

    assert url_discovery.main.score_candidate.__module__ == "url_discovery.scorer"
