"""Parallel web search → Impressum verification → observations.

Reference implementation for the search-API providers, and the only one verifiable without a
key: Parallel's MCP endpoint serves anonymous requests. That access is a hard QUOTA, not a rate
limit — measured, roughly 20 searches per process, after which every request 429s no matter how
long you wait. Runs therefore end as `partial` at ~20 companies. A paid key is required for any
batch larger than that, and `PARALLEL_QUOTA_GIVEUP_AFTER` stops the run rather than spending
minutes retrying a wall.

The flow itself lives in `search_discovery_runner` and is shared verbatim with Exa and Brave —
see that module for why identical logic matters here.

DO NOT remove the word "airflow" from this docstring. DAG_DISCOVERY_SAFE_MODE (on by default)
only parses files containing both "dag" and "airflow", and `from airflow import DAG` now lives in
search_discovery_runner.make_dag rather than here. Without the marker the file is skipped
silently: brave_search_discovery simply never appeared in `airflow dags list`, while the stale
serialized rows for the others kept the UI looking correct.
"""
from __future__ import annotations

import sys as _sys, os as _os

_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from parallel_http import (  # noqa: E402
    ParallelQuotaExhausted,
    ParallelRateLimitError,
    require_parallel_api_key,
    search as provider_search,
)

from search_discovery_runner import ProviderSpec, make_dag, run_discovery  # noqa: E402


SPEC = ProviderSpec(
    source_key="parallel_search_discovery",
    source_name="Parallel Search + Imprint Verification",
    provider="parallel",
    search=provider_search,
    transient_errors=(ParallelQuotaExhausted, ParallelRateLimitError),
    require_key=require_parallel_api_key,  # no-op: anonymous access is supported
    # Proven against the live API, the hard way: this is the client whose REST call was malformed
    # for weeks (fixed 2026-08-06), which is the reason the gate above exists for everyone else.
    smoke_required=False,
    dag_description="Parallel web search, verified against the company's Impressum",
)


def run_parallel_search_discovery() -> None:
    run_discovery(SPEC)


dag = make_dag(SPEC, run_parallel_search_discovery)
