from __future__ import annotations

import logging
import os
import time

import requests


log = logging.getLogger(__name__)

SEARXNG_BASE_URL = os.getenv("SEARXNG_BASE_URL", "http://searxng:8080")
SEARXNG_MAX_RESULTS = int(os.getenv("SEARXNG_MAX_RESULTS", "5"))
SEARXNG_TIMEOUT_S = float(os.getenv("SEARXNG_TIMEOUT_S", "15"))
SEARXNG_HTTP_MAX_RETRIES = int(os.getenv("SEARXNG_HTTP_MAX_RETRIES", "3"))
SEARXNG_RETRY_WAIT_S = float(os.getenv("SEARXNG_RETRY_WAIT_S", "2.0"))
SEARXNG_REQUEST_DELAY_S = float(os.getenv("SEARXNG_REQUEST_DELAY_S", "10.0"))


class SearXNGUnavailableError(RuntimeError):
    pass


def search(query: str, limit: int = SEARXNG_MAX_RESULTS) -> list[dict[str, str]]:
    url = f"{SEARXNG_BASE_URL.rstrip('/')}/search"
    params = {
        "q": query,
        "format": "json",
        "language": "de-DE",
        "safesearch": "0",
        "pageno": "1",
    }
    if SEARXNG_REQUEST_DELAY_S > 0:
        time.sleep(SEARXNG_REQUEST_DELAY_S)
    last_error: Exception | None = None
    for attempt in range(1, SEARXNG_HTTP_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=SEARXNG_TIMEOUT_S)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results") or []
            out: list[dict[str, str]] = []
            for r in results[:limit]:
                out.append(
                    {
                        "url": r.get("url") or "",
                        "title": r.get("title") or "",
                        "content": r.get("content") or "",
                    }
                )
            log.debug("SearXNG query=%r returned %d results.", query, len(out))
            return out
        except requests.exceptions.ConnectionError as exc:
            raise SearXNGUnavailableError(
                f"SearXNG not reachable at {SEARXNG_BASE_URL}: {exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < SEARXNG_HTTP_MAX_RETRIES:
                time.sleep(SEARXNG_RETRY_WAIT_S * attempt)
                continue
            break

    raise SearXNGUnavailableError(
        f"SearXNG search failed after {SEARXNG_HTTP_MAX_RETRIES} attempts: {last_error}"
    ) from last_error


def require_searxng() -> None:
    try:
        search("test", limit=1)
    except SearXNGUnavailableError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise SearXNGUnavailableError(
            f"SearXNG availability check failed: {exc}"
        ) from exc
