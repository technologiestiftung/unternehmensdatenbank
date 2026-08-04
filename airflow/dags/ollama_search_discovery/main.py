from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .ollama_http import OllamaUnavailableError, require_ollama, run_chat_prompt
from .searxng_http import SearXNGUnavailableError, require_searxng, search as searxng_search
from source_enrichment_common import (
    build_search_queries,
    dump_preview_payload,
    ensure_source,
    fetch_companies_without_source_after_id,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)


log = logging.getLogger(__name__)

SOURCE_KEY = "ollama_search_discovery"
SOURCE_NAME = "Ollama + SearXNG Local Discovery"
SOURCE_TYPE = "web_search_local"
ENABLED_FLAG = "ENABLE_OLLAMA_SEARCH_DISCOVERY"
OLLAMA_PROGRESS_LOG_EVERY = int(os.getenv("OLLAMA_PROGRESS_LOG_EVERY", "10"))
OLLAMA_DISCOVERY_CHECKPOINT_FILE = Path(
    os.getenv(
        "OLLAMA_DISCOVERY_CHECKPOINT_FILE",
        "/opt/airflow/data/ollama_search_discovery_checkpoint.json",
    )
)
SEARXNG_MAX_RESULTS_PER_QUERY = int(os.getenv("SEARXNG_MAX_RESULTS", "5"))


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not enabled.")
    require_searxng()
    require_ollama()


def _infer_candidate_type(url: str) -> str:
    hostname = (urlparse(url).hostname or "").lower()
    path = (urlparse(url).path or "").lower()
    if "linkedin." in hostname:
        return "linkedin"
    if "kununu." in hostname:
        return "kununu"
    if "northdata." in hostname or "openregister." in hostname or "creditreform." in hostname:
        return "company_directory"
    if "impressum" in path:
        return "impressum"
    return "website_candidate"


def _str(val: Any) -> str | None:
    """Return val if it's a non-empty string, None otherwise.

    LLMs occasionally return objects instead of strings for string fields;
    rejecting non-strings here prevents psycopg2 'can't adapt type dict' errors.
    """
    return val if isinstance(val, str) and val else None


def _normalize_result(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "website": _str(raw.get("website")) or _str(raw.get("official_website")),
        "ceo_name_or_contact": (
            _str(raw.get("ceo_name_or_contact"))
            or _str(raw.get("ceo_contact"))
            or _str(raw.get("ceo_name"))
        ),
        "tech_field": _str(raw.get("tech_field")),
        "cluster": _str(raw.get("cluster")),
        "kununu_url": _str(raw.get("kununu_url")),
        "linkedin_url": _str(raw.get("linkedin_url")),
        "candidate_urls": raw.get("candidate_urls") if isinstance(raw.get("candidate_urls"), list) else [],
        "confidence": raw.get("confidence"),
        "notes": _str(raw.get("notes")) or "",
    }


def _build_prompt(
    company_name: str,
    address: str | None,
    registration_number: str | None,
    search_results: list[dict[str, str]],
) -> str:
    results_text = ""
    for i, r in enumerate(search_results, start=1):
        content_preview = (r.get("content") or "")[:300]
        results_text += (
            f"\n[{i}] {r.get('title') or '(no title)'}\n"
            f"    URL: {r.get('url') or ''}\n"
            f"    {content_preview}\n"
        )

    return f"""You are extracting company information from web search results. Return ONLY valid JSON.

Target company:
  Name: {company_name}
  Address: {address or "unknown"}
  Registration number: {registration_number or "unknown"}

Search results:{results_text}
Instructions:
- Only fill a field if you are confident it refers to this exact company (same name AND location).
- For website: prefer the official company homepage over directories.
- For linkedin_url: only accept linkedin.com/company/* URLs.
- For kununu_url: only accept kununu.com/* URLs.
- List all plausible company URLs in candidate_urls.
- Set confidence between 0.0 and 1.0.

Return this exact JSON shape (no extra text, no markdown):
{{
  "website": null,
  "ceo_name_or_contact": null,
  "tech_field": null,
  "cluster": null,
  "kununu_url": null,
  "linkedin_url": null,
  "candidate_urls": [{{"type": "string", "url": "string", "reason": "string"}}],
  "confidence": null,
  "notes": ""
}}""".strip()


def _run_live_discovery(
    *,
    company_name: str,
    address: str | None,
    registration_number: str | None,
    queries: dict[str, str],
) -> dict[str, Any]:
    all_results: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    cap = SEARXNG_MAX_RESULTS_PER_QUERY * 2

    for query_type, query in (
        ("general", queries["general"]),
        ("website", queries["website"]),
        ("kununu", queries["kununu"]),
        ("linkedin", queries["linkedin"]),
    ):
        try:
            results = searxng_search(query, limit=SEARXNG_MAX_RESULTS_PER_QUERY)
            for r in results:
                url = r.get("url") or ""
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_results.append(r)
                    if len(all_results) >= cap:
                        break
        except SearXNGUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("SearXNG query %r (%s) failed: %s", query, query_type, exc)

    response = run_chat_prompt(
        _build_prompt(
            company_name=company_name,
            address=address,
            registration_number=registration_number,
            search_results=all_results,
        )
    )
    result = _normalize_result(response["parsed"])
    result["raw_text"] = response["raw_text"]
    result["search_results_count"] = len(all_results)

    existing_urls = {c.get("url") for c in (result.get("candidate_urls") or []) if c.get("url")}
    for r in all_results:
        url = r.get("url") or ""
        if url and url not in existing_urls:
            result["candidate_urls"].append(
                {
                    "type": _infer_candidate_type(url),
                    "url": url,
                    "reason": r.get("title") or "search_result",
                }
            )
            existing_urls.add(url)

    return result


def _ensure_checkpoint_parent() -> None:
    OLLAMA_DISCOVERY_CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_checkpoint() -> dict[str, Any]:
    if not OLLAMA_DISCOVERY_CHECKPOINT_FILE.exists():
        return {}
    try:
        return json.loads(OLLAMA_DISCOVERY_CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning(
            "Could not parse %s, starting without checkpoint.",
            OLLAMA_DISCOVERY_CHECKPOINT_FILE,
        )
        return {}


def _save_checkpoint(last_company_id: str | None, processed_count: int) -> None:
    _ensure_checkpoint_parent()
    OLLAMA_DISCOVERY_CHECKPOINT_FILE.write_text(
        json.dumps(
            {
                "last_company_id": last_company_id,
                "processed_count": processed_count,
                "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _delete_checkpoint() -> None:
    if OLLAMA_DISCOVERY_CHECKPOINT_FILE.exists():
        OLLAMA_DISCOVERY_CHECKPOINT_FILE.unlink()


def run_ollama_search_discovery() -> None:
    checkpoint = _load_checkpoint()
    last_company_id = checkpoint.get("last_company_id")
    companies = fetch_companies_without_source_after_id(
        SOURCE_KEY,
        after_id=last_company_id,
    )
    if not companies and last_company_id:
        _delete_checkpoint()
        companies = fetch_companies_without_source_after_id(SOURCE_KEY, after_id=None)
    if not companies:
        log.info("No companies pending for %s.", SOURCE_KEY)
        return

    _require_enabled()
    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cur:
                source_id = ensure_source(
                    cur,
                    key=SOURCE_KEY,
                    name=SOURCE_NAME,
                    source_type=SOURCE_TYPE,
                    metadata={"dag_id": SOURCE_KEY},
                )
                sync_run_id = start_sync_run(cur, source_id=source_id)

        created = 0
        updated = 0
        failed = 0
        processed_count = 0
        hit_unavailable = False
        last_completed_company_id = last_company_id

        for index, company in enumerate(companies, start=1):
            try:
                _t0 = time.monotonic()
                queries = build_search_queries(company)
                live_result = _run_live_discovery(
                    company_name=company.company_name,
                    address=company.address,
                    registration_number=company.registration_number,
                    queries=queries,
                )

                with connection:
                    with connection.cursor() as cur:
                        payload = dump_preview_payload(company, SOURCE_KEY)
                        payload["tool"] = "ollama+searxng"
                        payload["enabled"] = True
                        payload["status"] = "fetched"
                        payload["primary_queries"] = queries
                        payload["result"] = live_result

                        source_record_id, is_created = upsert_source_record(
                            cur,
                            company_id=company.id,
                            source_id=source_id,
                            external_id=f"{SOURCE_KEY}:{company.id}",
                            external_url=live_result.get("website"),
                            source_company_name=company.company_name,
                            source_payload=payload,
                        )
                        created += int(is_created)
                        updated += int(not is_created)

                        for field_name, query_type in (
                            ("search_query", "general"),
                            ("website_search_query", "website"),
                            ("kununu_search_query", "kununu"),
                            ("linkedin_search_query", "linkedin"),
                        ):
                            insert_text_observation(
                                cur,
                                company_id=company.id,
                                source_record_id=source_record_id,
                                source_id=source_id,
                                sync_run_id=sync_run_id,
                                field_name=field_name,
                                value_text=queries[query_type],
                                metadata={
                                    "query_type": query_type,
                                    "provider": "ollama_searxng",
                                },
                                confidence=1.0,
                            )

                        for field_name, key in (
                            ("website", "website"),
                            ("ceo_name_or_contact", "ceo_name_or_contact"),
                            ("tech_field", "tech_field"),
                            ("cluster", "cluster"),
                            ("kununu_url", "kununu_url"),
                            ("linkedin_url", "linkedin_url"),
                        ):
                            value = live_result.get(key)
                            if value:
                                insert_text_observation(
                                    cur,
                                    company_id=company.id,
                                    source_record_id=source_record_id,
                                    source_id=source_id,
                                    sync_run_id=sync_run_id,
                                    field_name=field_name,
                                    value_text=value,
                                    metadata={"provider": "ollama_searxng"},
                                    confidence=live_result.get("confidence"),
                                )

                        for candidate in live_result.get("candidate_urls") or []:
                            candidate_url = _str(candidate.get("url")) if isinstance(candidate, dict) else None
                            if candidate_url:
                                insert_text_observation(
                                    cur,
                                    company_id=company.id,
                                    source_record_id=source_record_id,
                                    source_id=source_id,
                                    sync_run_id=sync_run_id,
                                    field_name="candidate_url",
                                    value_text=candidate_url,
                                    metadata={
                                        "provider": "ollama_searxng",
                                        "candidate_type": candidate.get("type"),
                                        "reason": candidate.get("reason"),
                                    },
                                    confidence=live_result.get("confidence"),
                                )

                processed_count += 1
                _save_checkpoint(company.id, processed_count)
                last_completed_company_id = company.id

                elapsed = time.monotonic() - _t0
                found = {k: v for k, v in live_result.items() if v and k in ("website", "linkedin_url", "kununu_url", "ceo_name_or_contact", "tech_field", "cluster")}
                n_candidates = len(live_result.get("candidate_urls") or [])
                log.info(
                    "[%d] %s — %.1fs | results=%d candidates=%d conf=%s | %s",
                    index,
                    company.company_name,
                    elapsed,
                    live_result.get("search_results_count", 0),
                    n_candidates,
                    live_result.get("confidence"),
                    ", ".join(f"{k}={v}" for k, v in found.items()) or "nothing found",
                )

            except (OllamaUnavailableError, SearXNGUnavailableError) as exc:
                failed += 1
                hit_unavailable = True
                _save_checkpoint(last_completed_company_id, processed_count)
                log.warning(
                    "Stopping Ollama discovery — service unavailable at company %s: %s. "
                    "Resume will continue from checkpoint.",
                    company.id,
                    exc,
                )
                break
            except Exception:
                failed += 1
                log.exception("Failed Ollama discovery for company %s", company.id)
                processed_count += 1
                _save_checkpoint(company.id, processed_count)

            if index % OLLAMA_PROGRESS_LOG_EVERY == 0:
                log.info(
                    "Ollama discovery progress: %d/%d processed (%d created, %d updated, %d failed)",
                    index,
                    len(companies),
                    created,
                    updated,
                    failed,
                )

        with connection:
            with connection.cursor() as cur:
                finish_sync_run(
                    cur,
                    sync_run_id=sync_run_id,
                    records_seen=processed_count,
                    records_created=created,
                    records_updated=updated,
                    records_failed=failed,
                    status="partial" if hit_unavailable else None,
                )
        if not hit_unavailable:
            _delete_checkpoint()
    finally:
        connection.close()


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        return None

    with DAG(
        dag_id=SOURCE_KEY,
        description="Self-hosted company discovery via SearXNG search + Ollama LLM extraction",
        schedule=None,
        start_date=datetime(2026, 8, 3, tzinfo=timezone.utc),
        catchup=False,
        tags=["sources", "ollama", "searxng", "discovery", "local"],
    ) as dag:
        PythonOperator(
            task_id="run_ollama_search_discovery",
            python_callable=run_ollama_search_discovery,
        )
    return dag


dag = _make_dag()
