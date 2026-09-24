"""Brave web search → Impressum verification → observations.

Verified end to end on 2026-09-01: 20 active Osnabrück companies, all 20 processed, no failures.
What it produced, and the shape of it matters more than the count:

  * **19 `directory_url`, 4 `website`** (mean confidence 0.401). Brave ranks Creditreform,
    Northdata and trade portals above a small company's own site, so most of what it returns is
    correctly rejected by `search_common.is_own_site_candidate`. A provider comparison that
    counted raw hits rather than accepted websites would rank Brave far too highly.
  * 8 `ceo_name_or_contact`, and `$0.0000` reported spend — Brave sends no cost field at all,
    so its cost is requests x plan rate and has to be tracked outside the response.

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

import sys as _sys
import os as _os

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
    # Proven against the live API on 2026-09-01: smoke call plus a 20-company batch, envelope
    # `{mixed, query, type, web}` as assumed. See this module's docstring for what it returned.
    smoke_required=False,
    dag_description="Brave web search, verified against the company's Impressum",
)


def run_brave_search_discovery() -> None:
    run_discovery(SPEC)


dag = make_dag(SPEC, run_brave_search_discovery)
