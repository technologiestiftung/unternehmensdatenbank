"""Brave web search → Impressum verification → observations.

UNVERIFIED end to end: no `BRAVE_API_KEY` has ever existed here, so neither this DAG nor
`brave_http` has executed against the live API. The client was written from Brave's OpenAPI
schema, and Brave publishes no complete JSON example, so the first real run doubles as contract
verification. Treat a suspiciously empty result list as a possible shape mismatch, not as
"Brave found nothing" — a wrong request shape hid inside the old Parallel REST call for weeks
precisely because it failed quietly.

Brave is NOT a like-for-like competitor to Exa, and the asymmetry is structural, not a bug:

  * SNIPPETS ONLY — `description` plus up to five `extra_snippets`, no page text. Path A is
    unaffected (`imprint_probe` fetches pages itself), but path B gets materially less to read
    than Exa's page contents. When Brave extracts fewer owners, part of the cause is that it
    supplied less text, not that its ranking was worse. Say so when reporting.
  * `q` is capped at 400 characters / 50 words, below what `build_search_queries` produces for
    a long German company name plus address, so queries are truncated client-side.

The flow is shared verbatim with Parallel and Exa — see `search_discovery_runner`.

DO NOT remove the word "airflow" from this docstring — DAG_DISCOVERY_SAFE_MODE skips any file
lacking it, and `from airflow import DAG` lives in search_discovery_runner.make_dag, not here.
This DAG was invisible to `airflow dags list` for exactly that reason.
"""
from __future__ import annotations

import sys as _sys, os as _os

_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from brave_http import (  # noqa: E402
    BraveRateLimitError,
    require_brave_api_key,
    search as provider_search,
)

from search_discovery_runner import ProviderSpec, make_dag, run_discovery  # noqa: E402


SPEC = ProviderSpec(
    source_key="brave_search_discovery",
    source_name="Brave Search + Imprint Verification",
    provider="brave",
    search=provider_search,
    transient_errors=(BraveRateLimitError,),
    require_key=require_brave_api_key,
    dag_description="Brave web search, verified against the company's Impressum",
)


def run_brave_search_discovery() -> None:
    run_discovery(SPEC)


dag = make_dag(SPEC, run_brave_search_discovery)
