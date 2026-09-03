"""Brave Search client — https://api-dashboard.search.brave.com/documentation/services/web-search

VERIFIED against the live API on 2026-09-01, having been written from the OpenAPI schema alone.
What came back, over one smoke call and a 20-company batch:

  * The envelope is `{mixed, query, type, web}`. `web.results` is where the results live, as
    assumed. `mixed` is an ordering block and is not read.
  * A query that matches NOTHING drops `web` altogether and returns `{mixed, query, type}`. That
    is an empty result, not a broken request — see `_hits_from_body`.
  * Snippet text ran 108-369 characters per hit, consistent with the snippets-only note below.

Re-run `scripts/smoke_brave_client.py` after any change here; it is what the DAG's smoke gate
checks, and it prints the envelope's top-level keys precisely so a change to them is visible.

Two properties that make Brave NOT a like-for-like comparison against Exa or Parallel:

  * It returns SNIPPETS ONLY — `description` plus up to five `extra_snippets`. There is no
    page text. Full text is a separate product (`/res/v1/llm/context`). So when Brave scores
    worse at owner extraction, part of the cause is that it supplied less text to extract
    from, not that its ranking was worse. Say so when reporting.
  * `q` is capped at 400 characters / 50 words, well under what build_search_queries()
    produces for a long German company name plus address. Queries are truncated here.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

from search_common import SearchHit, SearchResponse


log = logging.getLogger(__name__)

BRAVE_API_KEY = os.getenv("BRAVE_API_KEY", "")
BRAVE_API_URL = os.getenv("BRAVE_API_URL", "https://api.search.brave.com/res/v1/web/search")
BRAVE_COUNTRY = os.getenv("BRAVE_COUNTRY", "DE")
BRAVE_SEARCH_LANG = os.getenv("BRAVE_SEARCH_LANG", "de")
# count is documented 1-20 for web results; anything higher is rejected.
BRAVE_COUNT = min(int(os.getenv("BRAVE_COUNT", "10")), 20)
BRAVE_HTTP_TIMEOUT_S = float(os.getenv("BRAVE_HTTP_TIMEOUT_S", "30"))
BRAVE_HTTP_MAX_RETRIES = int(os.getenv("BRAVE_HTTP_MAX_RETRIES", "3"))
BRAVE_RETRY_MAX_WAIT_S = float(os.getenv("BRAVE_RETRY_MAX_WAIT_S", "30"))
# Rate limiting is a 1-second sliding window (50 RPS on the Search plan), so a small delay
# is enough for the sequential loops here.
BRAVE_REQUEST_DELAY_S = float(os.getenv("BRAVE_REQUEST_DELAY_S", "0.1"))

Q_MAX_CHARS = 400
Q_MAX_WORDS = 50


class BraveRateLimitError(RuntimeError):
    pass


def require_brave_api_key() -> None:
    if not BRAVE_API_KEY:
        raise RuntimeError(
            "BRAVE_API_KEY is not configured. Brave requires a subscription token; the free "
            "Search plan grants $5/month of credits but signup needs a card on file."
        )


def _truncate_query(query: str) -> str:
    words = query.split()
    if len(words) > Q_MAX_WORDS:
        query = " ".join(words[:Q_MAX_WORDS])
    return query[:Q_MAX_CHARS]


def _get_search(query: str) -> dict[str, Any]:
    headers = {
        "X-Subscription-Token": BRAVE_API_KEY,
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
    }
    params = {
        "q": _truncate_query(query),
        "count": BRAVE_COUNT,
        "country": BRAVE_COUNTRY,
        "search_lang": BRAVE_SEARCH_LANG,
        "result_filter": "web",
        "extra_snippets": "true",
        "text_decorations": "false",
    }

    last_error: Exception | None = None
    for attempt in range(1, BRAVE_HTTP_MAX_RETRIES + 1):
        try:
            if BRAVE_REQUEST_DELAY_S > 0:
                time.sleep(BRAVE_REQUEST_DELAY_S)
            response = requests.get(
                BRAVE_API_URL, headers=headers, params=params, timeout=BRAVE_HTTP_TIMEOUT_S
            )
            if response.status_code == 429:
                # Brave returns X-RateLimit-Reset on the 1-second window.
                reset = response.headers.get("X-RateLimit-Reset")
                if attempt < BRAVE_HTTP_MAX_RETRIES:
                    try:
                        wait_s = min(float(reset), BRAVE_RETRY_MAX_WAIT_S) if reset else 1.5
                    except ValueError:
                        wait_s = 1.5
                    time.sleep(wait_s)
                    continue
                raise BraveRateLimitError(f"429 Too Many Requests for {BRAVE_API_URL}")
            if response.status_code in (401, 403, 422):
                raise RuntimeError(f"Brave HTTP {response.status_code}: {response.text[:200]}")
            response.raise_for_status()
            return response.json()
        except (BraveRateLimitError, RuntimeError):
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < BRAVE_HTTP_MAX_RETRIES:
                time.sleep(min(2**attempt, BRAVE_RETRY_MAX_WAIT_S))
                continue
            break

    raise RuntimeError(f"Brave search request failed: {last_error}") from last_error


def _hits_from_body(body: dict[str, Any]) -> list[SearchHit]:
    results = ((body.get("web") or {}).get("results")) or []
    if not results and body.get("web") is None:
        # Measured on the first live run, 2026-09-01: Brave DROPS the `web` container entirely for
        # a query that matches nothing, leaving `{mixed, query, type}`. It is not a shape error —
        # in the same run, queries that did match returned `web` with up to 14 results. So a
        # missing `web` beside a present `query` is an empty result, and only worth a debug line.
        #
        # `query` and `type` are what make that distinction possible: they were present in every
        # response observed, empty or not. Losing THEM is the envelope change the original warning
        # was written to catch, so that case keeps the warning.
        if "query" in body:
            log.debug("Brave returned no `web` block: no matches for this query.")
        else:
            log.warning(
                "Brave response has neither `web` nor `query` (top-level keys: %s) — the response "
                "envelope has changed. Verify against the current docs before trusting any "
                "zero-hit result, and re-run scripts/smoke_brave_client.py.",
                sorted(body)[:10],
            )
    hits: list[SearchHit] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            continue
        parts = [item.get("description") or ""]
        parts.extend(s for s in (item.get("extra_snippets") or []) if isinstance(s, str))
        hits.append(
            SearchHit(
                url=url,
                title=str(item.get("title") or ""),
                text=" ".join(p for p in parts if p).strip(),
                metadata={"page_age": item.get("page_age"), "provider": "brave"},
            )
        )
    return hits


def search(
    queries: list[str],
    *,
    objective: str = "",
    limit: int = BRAVE_COUNT,
) -> SearchResponse:
    """One GET per query, merged and deduplicated by URL.

    `objective` is accepted for interface parity only; Brave has no objective parameter.
    """
    require_brave_api_key()

    effective = [q for q in queries if q and q.strip()] or ([objective] if objective else [])
    if not effective:
        raise ValueError("brave_http.search() needs at least one query.")

    merged: list[SearchHit] = []
    seen: set[str] = set()
    raw_bodies: list[dict[str, Any]] = []

    for query in effective:
        body = _get_search(query)
        raw_bodies.append(body)
        for hit in _hits_from_body(body)[:limit]:
            if hit.url in seen:
                continue
            seen.add(hit.url)
            merged.append(hit)

    # Brave reports no cost field; spend is requests x plan rate, tracked by the caller.
    return SearchResponse(hits=merged, queries=effective, cost_usd=None, raw=raw_bodies)
