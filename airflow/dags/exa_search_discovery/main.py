"""Exa web search → Impressum verification → observations.

Exa returns page CONTENT in the same POST as the search (via `contents`), so path B has real
text to read rather than snippets — the opposite end of the spectrum from Brave. It also
reports `costDollars`, making it the one provider whose spend is measured rather than estimated;
`run_discovery` sums that into the finish log.

Requires `EXA_API_KEY`. The flow is shared verbatim with Parallel and Brave — see
`search_discovery_runner` for why.

`EXA_CONTENT_MODE` is the knob that matters for this comparison. Exa's own guidance is
highlights-only for LLM workflows, but highlights are query-biased and the Geschäftsführer line
may not survive them, so owner extraction may want `text`. Default is `highlights`; measure
before changing it, and change it for one provider only if you are prepared to say so when
reporting results.

DO NOT remove the word "airflow" from this docstring — DAG_DISCOVERY_SAFE_MODE skips any file
lacking it, and `from airflow import DAG` lives in search_discovery_runner.make_dag, not here.
"""
from __future__ import annotations

import sys as _sys, os as _os

_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from exa_http import ExaRateLimitError, require_exa_api_key, search as provider_search  # noqa: E402

from search_discovery_runner import ProviderSpec, make_dag, run_discovery  # noqa: E402


SPEC = ProviderSpec(
    source_key="exa_search_discovery",
    source_name="Exa Search + Imprint Verification",
    provider="exa",
    search=provider_search,
    transient_errors=(ExaRateLimitError,),
    require_key=require_exa_api_key,
    dag_description="Exa web search, verified against the company's Impressum",
)


def run_exa_search_discovery() -> None:
    run_discovery(SPEC)


dag = make_dag(SPEC, run_exa_search_discovery)
