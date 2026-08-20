"""Parallel Search MCP client — https://docs.parallel.ai (Search MCP).

Free and works ANONYMOUSLY: no API key required, which is what made this the first search
provider runnable here at all. A key only raises the rate limit, so it stays optional. The
MCP endpoint always searches in `basic` mode, so there is no mode knob.

Two things that cost real debugging time and are encoded here:

  * `web_search` requires BOTH `objective` AND `search_queries`. Sending only `objective`
    returns a pydantic validation error. An earlier REST implementation sent neither in the
    right shape, so stage 4 could never have succeeded — and because the exception was
    swallowed, it looked like "search found nothing" rather than a broken request.
  * Anonymous access is a QUOTA, not a sliding rate limit. Measured over 100 companies:
    searches 1-20 succeeded, everything from 21 on returned 429 for the next 13 minutes
    (one lone recovery at 36). So once it trips, stop asking. Retrying cost 5s per row to be
    refused twice — about 6.6 minutes of that run spent sleeping against a closed door.

Transport is Streamable HTTP JSON-RPC, done with `requests` rather than the `mcp` SDK: the
handshake is three calls, the SDK is not installed, and the repo hand-rolls its clients.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import requests

from search_common import SearchHit, SearchResponse


log = logging.getLogger(__name__)

PARALLEL_API_KEY = os.getenv("PARALLEL_API_KEY", "")
PARALLEL_MCP_URL = os.getenv("PARALLEL_MCP_URL", "https://search.parallel.ai/mcp")
PARALLEL_MCP_TIMEOUT_S = float(os.getenv("PARALLEL_MCP_TIMEOUT_S", "120"))
# Consecutive 429/503s before we give up for the whole process.
PARALLEL_QUOTA_GIVEUP_AFTER = int(os.getenv("PARALLEL_QUOTA_GIVEUP_AFTER", "3"))

#: Process-wide session + quota state. Module-level on purpose: the MCP handshake is
#: expensive relative to a search, and the quota is per-account, not per-company.
_MCP_STATE: dict[str, Any] = {}


class ParallelRateLimitError(RuntimeError):
    pass


class ParallelQuotaExhausted(RuntimeError):
    """Raised once the anonymous quota is judged spent, so callers can skip cheaply."""


def require_parallel_api_key() -> None:
    """No-op: anonymous access is supported. Kept for interface parity with the others."""
    return None


def quota_exhausted() -> bool:
    return bool(_MCP_STATE.get("quota_exhausted"))


def reset_state() -> None:
    """Drop the session and quota flags. Mainly for tests and for long-lived processes."""
    _MCP_STATE.clear()


def _mcp_parse(resp: requests.Response) -> dict[str, Any]:
    """Streamable HTTP replies either as JSON or as an SSE `data:` frame."""
    body = resp.text
    if body.lstrip().startswith("{"):
        return resp.json()
    for line in body.splitlines():
        if line.startswith("data:"):
            return json.loads(line[5:].strip())
    raise ValueError(f"unparseable MCP response: {body[:200]}")


def _mcp_session() -> tuple[requests.Session, dict[str, str]]:
    """Initialize the MCP session once and reuse it."""
    if "headers" in _MCP_STATE:
        return _MCP_STATE["session"], _MCP_STATE["headers"]

    session = requests.Session()
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if PARALLEL_API_KEY:
        headers["Authorization"] = f"Bearer {PARALLEL_API_KEY}"

    resp = session.post(
        PARALLEL_MCP_URL,
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "udb-search", "version": "0.1"},
            },
        },
        timeout=PARALLEL_MCP_TIMEOUT_S,
    )
    resp.raise_for_status()
    _mcp_parse(resp)
    session_id = resp.headers.get("Mcp-Session-Id")
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    # Required by the spec before any tools/call; the server rejects calls without it.
    session.post(
        PARALLEL_MCP_URL,
        headers=headers,
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        timeout=PARALLEL_MCP_TIMEOUT_S,
    )

    _MCP_STATE["session"], _MCP_STATE["headers"] = session, headers
    log.info("parallel MCP session ready (%s)", "keyed" if PARALLEL_API_KEY else "anonymous")
    return session, headers


def _hits_from_payload(text: str) -> tuple[list[SearchHit], str]:
    """Parse the tool's structured `results[]` JSON into hits.

    Returns (hits, raw_text). The tool returns JSON as a *text* content block, so the raw
    string is kept: if the shape ever changes, callers can still fall back to it rather than
    silently seeing zero results.
    """
    try:
        results = json.loads(text).get("results") or []
    except (json.JSONDecodeError, AttributeError):
        log.warning("parallel MCP payload was not the expected JSON shape; returning raw text")
        return [], text

    hits: list[SearchHit] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            continue
        excerpts = " ".join(str(e) for e in (item.get("excerpts") or []) if e)
        hits.append(
            SearchHit(
                url=url,
                title=str(item.get("title") or ""),
                text=excerpts.strip(),
                metadata={"publish_date": item.get("publish_date"), "provider": "parallel"},
            )
        )
    return hits, text


def search(
    queries: list[str],
    *,
    objective: str = "",
    limit: int = 0,  # accepted for parity; the MCP tool has no result-count parameter
) -> SearchResponse:
    """One `web_search` tool call. Raises ParallelQuotaExhausted once the quota is spent."""
    if quota_exhausted():
        raise ParallelQuotaExhausted("parallel anonymous quota already exhausted this process")

    effective = [q for q in queries if q and q.strip()]
    if not effective and not objective:
        raise ValueError("parallel_http.search() needs a query or an objective.")

    for attempt in (1, 2):
        session, headers = _mcp_session()
        resp = session.post(
            PARALLEL_MCP_URL,
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "web_search",
                    "arguments": {
                        "objective": objective or effective[0],
                        "search_queries": effective or [objective],
                    },
                },
            },
            timeout=PARALLEL_MCP_TIMEOUT_S,
        )

        if resp.status_code in (429, 503):
            streak = _MCP_STATE.get("throttle_streak", 0) + 1
            _MCP_STATE["throttle_streak"] = streak
            if streak >= PARALLEL_QUOTA_GIVEUP_AFTER:
                _MCP_STATE["quota_exhausted"] = True
                log.warning(
                    "parallel quota looks exhausted after %d consecutive %ss — skipping "
                    "further searches this process. Set PARALLEL_API_KEY for real limits.",
                    streak,
                    resp.status_code,
                )
                raise ParallelQuotaExhausted(f"HTTP {resp.status_code} x{streak}")
            if attempt == 1:
                # Honour Retry-After when sent; a flat sleep is arbitrary and, against a
                # quota, pointless.
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait_s = min(float(retry_after), 30.0) if retry_after else 2.0
                except ValueError:
                    wait_s = 2.0
                time.sleep(wait_s)
                continue
            raise ParallelRateLimitError(f"HTTP {resp.status_code} for {PARALLEL_MCP_URL}")

        resp.raise_for_status()
        payload = _mcp_parse(resp)

        # A JSON-RPC error or an isError result must be visible, never "no hits".
        if "error" in payload:
            raise RuntimeError(f"parallel MCP error: {str(payload['error'].get('message'))[:160]}")
        result = payload.get("result") or {}
        text = "\n\n".join(
            block["text"].strip()
            for block in result.get("content") or []
            if isinstance(block, dict) and block.get("text", "").strip()
        )
        if result.get("isError"):
            raise RuntimeError(f"parallel MCP tool error: {text[:160]}")

        _MCP_STATE["throttle_streak"] = 0  # a success means the quota is not spent
        hits, raw = _hits_from_payload(text)
        return SearchResponse(hits=hits, queries=effective, cost_usd=None, raw=raw)

    raise ParallelRateLimitError("parallel search exhausted its retries")
