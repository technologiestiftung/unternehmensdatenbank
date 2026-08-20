"""Search with your own search engine, read the results with your own language model.

Two self-hosted services, both free to run:

- **SearXNG** — a search engine you host yourself. It does not have its own index; it forwards
  your query to Google, DuckDuckGo, Bing and others and merges what comes back.
- **Ollama** — runs a language model on your own machine. Here it reads the search results and
  picks the one URL that is the company's own website.

Both live in `docker-compose.yml` under the `llm` profile:

    docker compose --profile llm up -d searxng ollama
    docker compose --profile llm exec ollama ollama pull qwen2.5:7b

Two things routinely make this stage produce nothing, and `availability()` reports which:

1. **SearXNG gets no results.** It scrapes public search engines, and those serve a CAPTCHA
   to datacentre and office networks. SearXNG then returns an empty list rather than an error,
   so an empty result is not proof that the company has no website.
2. **The model is too slow.** Without a GPU, a 7-billion-parameter model on this hardware was
   measured at 0.38 tokens per second. One company needs roughly 200 output tokens, so about
   nine minutes each — 1000 companies would take over 60 days. `estimate_runtime()` below
   exists to make that visible before a run starts rather than after.

Whatever this stage returns is treated as the weakest evidence in the pipeline. The model has
read *text about* the company, not the company's own page, so `confidence.py` caps it below
the promotion threshold until something actually downloads the page.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

from ollama_search_discovery.ollama_http import (
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    OllamaUnavailableError,
    require_ollama,
    run_chat_prompt,
)
from searxng_http import (
    SEARXNG_BASE_URL,
    SearXNGUnavailableError,
    require_searxng,
    search as searxng_search,
)

log = logging.getLogger(__name__)

#: Results asked of SearXNG per query. Two queries are run per company, so the model sees at
#: most twice this many.
MAX_RESULTS_PER_QUERY = int(os.getenv("COMBINED_SEARXNG_MAX_RESULTS", "5"))

#: How much of each result's snippet the model is shown. Snippets are short; the cut only
#: bites on the occasional page that returns half an article.
SNIPPET_CHARS = int(os.getenv("COMBINED_SEARXNG_SNIPPET_CHARS", "300"))

PROMPT = """You are given web search results for a German company. Find its OFFICIAL WEBSITE.

Company: {name}
Address: {address}

Search results:
{results}

Rules:
- The official website is the company's OWN site. A directory entry (northdata.de,
  firmenwissen.de, wlw.de, gelbeseiten.de, kununu.com, linkedin.com) is NOT the website.
- Only answer if the result clearly refers to THIS company at THIS address.
- If none of the results is the company's own site, answer with null.

Return only JSON, no other text:
{{"website": null, "reason": "", "confidence": 0.0}}"""


@dataclass
class LlmResult:
    website: str | None = None
    reason: str = ""
    #: What the model says about itself. Recorded, never trusted: `confidence.py` decides.
    self_reported_confidence: float | None = None
    search_results: list[dict[str, str]] = field(default_factory=list)
    candidate_urls: list[str] = field(default_factory=list)
    elapsed_s: float = 0.0
    error: str | None = None


def availability() -> tuple[bool, str]:
    """Can this stage run right now? Returns (usable, one-line reason).

    Checks both services AND that SearXNG can actually reach a search engine, because a
    SearXNG that answers HTTP 200 with zero results looks healthy and finds nothing.
    """
    try:
        require_searxng()
    except SearXNGUnavailableError as exc:
        return False, f"SearXNG unusable at {SEARXNG_BASE_URL}: {exc}"
    except Exception as exc:  # noqa: BLE001
        return False, f"SearXNG check failed: {exc}"

    try:
        probe = searxng_search("Volkswagen AG Wolfsburg", limit=3)
    except Exception as exc:  # noqa: BLE001
        return False, f"SearXNG search failed: {exc}"
    if not probe:
        return False, (
            f"SearXNG at {SEARXNG_BASE_URL} answers but returns 0 results for a control query "
            "— its upstream engines are blocking this network"
        )

    try:
        require_ollama()
    except OllamaUnavailableError as exc:
        return False, f"Ollama unusable at {OLLAMA_BASE_URL}: {exc}"
    except Exception as exc:  # noqa: BLE001
        return False, f"Ollama check failed: {exc}"

    return True, f"SearXNG at {SEARXNG_BASE_URL} + Ollama model {OLLAMA_MODEL}"


def estimate_runtime(companies: int, seconds_per_company: float) -> str:
    """Say out loud how long this stage would take, in units people think in."""
    total = companies * seconds_per_company
    if total < 3600:
        return f"{total / 60:.0f} minutes for {companies} companies"
    if total < 86400:
        return f"{total / 3600:.1f} hours for {companies} companies"
    return f"{total / 86400:.1f} DAYS for {companies} companies"


def discover(company: Any, queries: dict[str, str]) -> LlmResult:
    """Search for one company and let the local model pick its website.

    `queries` comes from `source_enrichment_common.build_search_queries`, the same builder the
    hosted-search DAGs use, so this stage searches for exactly what they search for.
    """
    started = time.monotonic()
    result = LlmResult()

    seen: set[str] = set()
    for key in ("general", "website"):
        query = queries.get(key)
        if not query:
            continue
        try:
            for row in searxng_search(query, limit=MAX_RESULTS_PER_QUERY):
                url = row.get("url") or ""
                if url and url not in seen:
                    seen.add(url)
                    result.search_results.append(row)
        except SearXNGUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("SearXNG query failed (%s): %s", key, exc)

    result.candidate_urls = [r.get("url", "") for r in result.search_results if r.get("url")]
    if not result.search_results:
        result.error = "no_search_results"
        result.elapsed_s = round(time.monotonic() - started, 2)
        return result

    rendered = "\n".join(
        f"[{i}] {r.get('title') or '(no title)'}\n    URL: {r.get('url')}\n"
        f"    {(r.get('content') or '')[:SNIPPET_CHARS]}"
        for i, r in enumerate(result.search_results, start=1)
    )
    address = " ".join(
        part for part in [company.address or "", company.postcode or "", company.city or ""] if part
    ).strip()

    try:
        response = run_chat_prompt(
            PROMPT.format(name=company.company_name, address=address or "unknown", results=rendered)
        )
        parsed = response.get("parsed") or {}
        website = parsed.get("website")
        result.website = website if isinstance(website, str) and website.startswith("http") else None
        result.reason = str(parsed.get("reason") or "")[:200]
        raw_confidence = parsed.get("confidence")
        if isinstance(raw_confidence, (int, float)):
            result.self_reported_confidence = float(raw_confidence)
    except OllamaUnavailableError:
        raise
    except Exception as exc:  # noqa: BLE001
        result.error = f"{type(exc).__name__}: {exc}"

    result.elapsed_s = round(time.monotonic() - started, 2)
    return result
