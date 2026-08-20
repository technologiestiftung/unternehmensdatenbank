"""Exa search client — https://exa.ai/docs/reference/search

Returns page TEXT in the same POST as the search (via `contents`), so no second round trip
is needed before extraction. That, plus a self-reported `costDollars`, makes Exa the only
provider here whose per-search spend is measured rather than estimated.

Two schema facts worth pinning down, because older Exa clients get both wrong:
  * `type` no longer accepts "neural" or "keyword". The current enum is
    instant | fast | auto | deep-lite | deep | deep-reasoning. "neural" survives only as a
    cost bucket in the response and as the deprecated `resolvedSearchType`.
  * results have NO `score` field. Only `highlightScores` (parallel to `highlights`).

Follows the convention of mistral_http.py / claude_http.py: hand-rolled `requests`, a
module-level rate-limit error, and a require_*_api_key() guard. No vendor SDK.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

from search_common import SearchHit, SearchResponse


log = logging.getLogger(__name__)

EXA_API_KEY = os.getenv("EXA_API_KEY", "")
EXA_API_BASE_URL = os.getenv("EXA_API_BASE_URL", "https://api.exa.ai")
EXA_SEARCH_TYPE = os.getenv("EXA_SEARCH_TYPE", "auto")
EXA_NUM_RESULTS = int(os.getenv("EXA_NUM_RESULTS", "8"))
# "highlights" | "text" | "both". Exa's own guidance is highlights-only for LLM workflows —
# query-relevant excerpts keep token usage predictable, and combining modes is called an
# antipattern. Kept switchable because owner extraction needs the Impressum *body*, and a
# query-biased highlight may not contain the Geschäftsführer line. Measure before deciding.
EXA_CONTENT_MODE = os.getenv("EXA_CONTENT_MODE", "highlights")
EXA_TEXT_MAX_CHARS = int(os.getenv("EXA_TEXT_MAX_CHARS", "4000"))
EXA_HTTP_TIMEOUT_S = float(os.getenv("EXA_HTTP_TIMEOUT_S", "45"))
EXA_HTTP_MAX_RETRIES = int(os.getenv("EXA_HTTP_MAX_RETRIES", "3"))
EXA_RETRY_MAX_WAIT_S = float(os.getenv("EXA_RETRY_MAX_WAIT_S", "30"))
# /search is documented at 10 QPS. A small floor keeps sequential loops under it without
# needing a token bucket.
EXA_REQUEST_DELAY_S = float(os.getenv("EXA_REQUEST_DELAY_S", "0.15"))


class ExaRateLimitError(RuntimeError):
    pass


def require_exa_api_key() -> None:
    if not EXA_API_KEY:
        raise RuntimeError("EXA_API_KEY is not configured.")


def _headers() -> dict[str, str]:
    return {"x-api-key": EXA_API_KEY, "Content-Type": "application/json"}


def _contents_payload() -> dict[str, Any]:
    mode = EXA_CONTENT_MODE.lower()
    if mode == "text":
        return {"text": {"maxCharacters": EXA_TEXT_MAX_CHARS}}
    if mode == "both":
        return {"text": {"maxCharacters": EXA_TEXT_MAX_CHARS}, "highlights": True}
    return {"highlights": True}


def _post_search(payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{EXA_API_BASE_URL.rstrip('/')}/search"
    last_error: Exception | None = None

    for attempt in range(1, EXA_HTTP_MAX_RETRIES + 1):
        try:
            if EXA_REQUEST_DELAY_S > 0:
                time.sleep(EXA_REQUEST_DELAY_S)
            response = requests.post(
                url, headers=_headers(), json=payload, timeout=EXA_HTTP_TIMEOUT_S
            )
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if attempt < EXA_HTTP_MAX_RETRIES:
                    try:
                        wait_s = min(float(retry_after), EXA_RETRY_MAX_WAIT_S) if retry_after else 2.0
                    except ValueError:
                        wait_s = 2.0
                    time.sleep(wait_s)
                    continue
                detail = f"429 Too Many Requests for {url}"
                if retry_after:
                    detail += f" (Retry-After={retry_after}s)"
                raise ExaRateLimitError(detail)
            # 401/402 are terminal: a bad key or an empty balance will not fix itself, and
            # retrying hides the cause behind a generic failure.
            if response.status_code in (401, 402, 403):
                raise RuntimeError(
                    f"Exa HTTP {response.status_code}: {response.text[:200]}"
                )
            response.raise_for_status()
            return response.json()
        except (ExaRateLimitError, RuntimeError):
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < EXA_HTTP_MAX_RETRIES:
                time.sleep(min(2**attempt, EXA_RETRY_MAX_WAIT_S))
                continue
            break

    raise RuntimeError(f"Exa search request failed: {last_error}") from last_error


def _hits_from_results(results: list[Any]) -> list[SearchHit]:
    hits: list[SearchHit] = []
    for item in results or []:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            continue
        # Prefer full text; fall back to highlights, which are the only content present
        # when `contents.text` was declined or the page could not be fetched.
        text = item.get("text") or ""
        if not text:
            highlights = item.get("highlights") or []
            text = " ".join(h for h in highlights if isinstance(h, str))
        hits.append(
            SearchHit(
                url=url,
                title=str(item.get("title") or ""),
                text=str(text)[:EXA_TEXT_MAX_CHARS],
                metadata={
                    "published_date": item.get("publishedDate"),
                    "author": item.get("author"),
                    "provider": "exa",
                },
            )
        )
    return hits


def search(
    queries: list[str],
    *,
    objective: str = "",
    limit: int = EXA_NUM_RESULTS,
) -> SearchResponse:
    """Run one Exa search per query and merge, deduplicating by URL.

    `objective` is accepted for interface parity with the Parallel client but Exa's /search
    has no objective parameter, so it is only used to seed the query list when empty.
    """
    require_exa_api_key()

    effective = [q for q in queries if q and q.strip()] or ([objective] if objective else [])
    if not effective:
        raise ValueError("exa_http.search() needs at least one query.")

    merged: list[SearchHit] = []
    seen: set[str] = set()
    total_cost = 0.0
    saw_cost = False
    raw_bodies: list[dict[str, Any]] = []

    for query in effective:
        body = _post_search(
            {
                "query": query,
                "type": EXA_SEARCH_TYPE,
                "numResults": limit,
                # text/highlights/summary MUST be nested under `contents` on /search.
                # (They are top-level on /contents — different endpoint, different shape.)
                "contents": _contents_payload(),
            }
        )
        raw_bodies.append(body)

        cost = (body.get("costDollars") or {}).get("total")
        if isinstance(cost, (int, float)):
            total_cost += float(cost)
            saw_cost = True

        for hit in _hits_from_results(body.get("results") or []):
            if hit.url in seen:
                continue
            seen.add(hit.url)
            merged.append(hit)

    return SearchResponse(
        hits=merged,
        queries=effective,
        cost_usd=total_cost if saw_cost else None,
        raw=raw_bodies,
    )
