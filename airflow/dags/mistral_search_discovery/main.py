from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Any
from concurrent.futures import Future, ThreadPoolExecutor
import time
from urllib.parse import urlparse

from .mistral_http import MistralRateLimitError, require_mistral_api_key, run_websearch_prompt
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

SOURCE_KEY = "mistral_search_discovery"
SOURCE_NAME = "Mistral Web Search Discovery"
SOURCE_TYPE = "web_search"
ENABLED_FLAG = "ENABLE_MISTRAL_SEARCH_DISCOVERY"
MISTRAL_DISCOVERY_MAX_RESULTS = int(os.getenv("MISTRAL_DISCOVERY_MAX_RESULTS", "5"))
MISTRAL_PROGRESS_LOG_EVERY = int(os.getenv("MISTRAL_PROGRESS_LOG_EVERY", "10"))
MISTRAL_CONCURRENCY = int(os.getenv("MISTRAL_CONCURRENCY", "4"))
MISTRAL_SUBMIT_DELAY_S = float(os.getenv("MISTRAL_SUBMIT_DELAY_S", "0.1"))
MISTRAL_CHECKPOINT_FILE = Path(
    os.getenv("MISTRAL_DISCOVERY_CHECKPOINT_FILE", "/opt/airflow/data/mistral_search_discovery_checkpoint.json")
)


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not enabled.")
    require_mistral_api_key()


def _normalize_result(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "website": raw.get("website") or raw.get("official_website") or None,
        "ceo_name_or_contact": raw.get("ceo_name_or_contact") or raw.get("ceo_contact") or raw.get("ceo_name") or None,
        "tech_field": raw.get("tech_field") or None,
        "cluster": raw.get("cluster") or None,
        "kununu_url": raw.get("kununu_url") or None,
        "linkedin_url": raw.get("linkedin_url") or None,
        "candidate_urls": raw.get("candidate_urls") or [],
        "confidence": raw.get("confidence"),
        "notes": raw.get("notes") or "",
    }


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


def _extract_candidates_from_result_blob(blob: Any) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            maybe_url = node.get("url")
            if isinstance(maybe_url, str) and maybe_url.startswith(("http://", "https://")):
                reason_parts = []
                for key in ("reason", "title", "description", "source"):
                    value = node.get(key)
                    if isinstance(value, str) and value.strip():
                        reason_parts.append(value.strip())
                candidates.append(
                    {
                        "type": _infer_candidate_type(maybe_url),
                        "url": maybe_url,
                        "reason": " | ".join(reason_parts)[:500] if reason_parts else "raw_search_result",
                    }
                )
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(blob)
    return candidates


def _extract_candidates_from_raw_response(raw_response: dict[str, Any] | None) -> list[dict[str, str]]:
    if not raw_response:
        return []

    candidates: list[dict[str, str]] = []
    for output in raw_response.get("outputs") or []:
        if not isinstance(output, dict):
            continue
        if output.get("type") != "tool.execution":
            continue
        info = output.get("info") or {}
        raw_result = info.get("result")
        parsed: Any = None
        if isinstance(raw_result, str):
            try:
                parsed = json.loads(raw_result)
            except json.JSONDecodeError:
                parsed = None
        elif isinstance(raw_result, (dict, list)):
            parsed = raw_result
        if parsed is not None:
            candidates.extend(_extract_candidates_from_result_blob(parsed))

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        url = candidate["url"]
        if url in seen:
            continue
        seen.add(url)
        deduped.append(candidate)
    return deduped


def _merge_candidate_urls(model_candidates: list[dict[str, Any]], raw_candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen: set[str] = set()

    for source in (model_candidates or [], raw_candidates or []):
        for candidate in source if isinstance(source, list) else []:
            if not isinstance(candidate, dict):
                continue
            url = candidate.get("url")
            if not isinstance(url, str) or not url.startswith(("http://", "https://")):
                continue
            if url in seen:
                continue
            seen.add(url)
            merged.append(
                {
                    "type": str(candidate.get("type") or _infer_candidate_type(url)),
                    "url": url,
                    "reason": str(candidate.get("reason") or "candidate"),
                }
            )
    return merged


def _build_prompt(company_name: str, queries: dict[str, str], address: str | None, registration_number: str | None) -> str:
    return f"""
Find the best matching public company information for this German company.

Company:
- name: {company_name}
- address: {address or ""}
- registration_number: {registration_number or ""}

Fields to find if possible:
- website
- ceo_name_or_contact
- tech_field
- cluster
- linkedin_url
- kununu_url

Rules:
1. exact company match
2. same city / postcode
3. avoid person profiles, job posts, and unrelated results
4. prefer official company pages, company directories, LinkedIn company pages, and Kununu employer pages
5. keep the result set tight; inspect at most {MISTRAL_DISCOVERY_MAX_RESULTS} relevant search hits per target
6. if you cannot find a high-confidence final answer, still return best-effort candidate URLs in `candidate_urls`
7. if a company directory strongly matches the legal name and address, you may use it as `website` when no official site is found

Useful search queries:
- general: {queries["general"]}
- website: {queries["website"]}
- kununu: {queries["kununu"]}
- linkedin: {queries["linkedin"]}

Return only JSON with this exact shape:
{{
  "website": string or null,
  "ceo_name_or_contact": string or null,
  "tech_field": string or null,
  "cluster": string or null,
  "kununu_url": string or null,
  "linkedin_url": string or null,
  "candidate_urls": [{{"type": string, "url": string, "reason": string}}],
  "confidence": number or null,
  "notes": string
}}
""".strip()


def _run_live_discovery(*, company_name: str, queries: dict[str, str], address: str | None, registration_number: str | None) -> dict[str, Any]:
    response = run_websearch_prompt(
        _build_prompt(
            company_name=company_name,
            queries=queries,
            address=address,
            registration_number=registration_number,
        )
    )
    result = _normalize_result(response["parsed"])
    result["raw_text"] = response["raw_text"]
    result["raw_response"] = response["raw_response"]
    result["candidate_urls"] = _merge_candidate_urls(
        result.get("candidate_urls") or [],
        _extract_candidates_from_raw_response(response["raw_response"]),
    )
    return result


def _submit_discovery_batch(companies: list[Any]) -> list[tuple[Any, Future]]:
    max_workers = max(1, min(MISTRAL_CONCURRENCY, len(companies)))
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mistral-discovery")
    submitted: list[tuple[Any, Future]] = []
    try:
        for company in companies:
            queries = build_search_queries(company)
            future = executor.submit(
                _run_live_discovery,
                company_name=company.company_name,
                queries=queries,
                address=company.address,
                registration_number=company.registration_number,
            )
            submitted.append((company, future))
            if MISTRAL_SUBMIT_DELAY_S > 0:
                time.sleep(MISTRAL_SUBMIT_DELAY_S)
        executor.shutdown(wait=False)
        return submitted
    except Exception:
        executor.shutdown(wait=False, cancel_futures=True)
        raise


def _ensure_checkpoint_parent() -> None:
    MISTRAL_CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_checkpoint() -> dict[str, Any]:
    if not MISTRAL_CHECKPOINT_FILE.exists():
        return {}
    try:
        return json.loads(MISTRAL_CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning("Could not parse %s, starting without checkpoint.", MISTRAL_CHECKPOINT_FILE)
        return {}


def _save_checkpoint(last_company_id: str | None, processed_count: int) -> None:
    _ensure_checkpoint_parent()
    MISTRAL_CHECKPOINT_FILE.write_text(
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
    if MISTRAL_CHECKPOINT_FILE.exists():
        MISTRAL_CHECKPOINT_FILE.unlink()


def run_mistral_search_discovery() -> None:
    checkpoint = _load_checkpoint()
    checkpoint_after_id = checkpoint.get("last_company_id")
    last_company_id = checkpoint_after_id
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
        hit_rate_limit = False
        last_completed_company_id = checkpoint_after_id

        submitted = _submit_discovery_batch(companies)

        for index, (company, future) in enumerate(submitted, start=1):
            try:
                live_result = future.result()
                with connection:
                    with connection.cursor() as cur:
                        payload = dump_preview_payload(company, SOURCE_KEY)
                        payload["tool"] = "mistral web_search"
                        payload["enabled"] = True
                        queries = build_search_queries(company)
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

                        insert_text_observation(
                            cur,
                            company_id=company.id,
                            source_record_id=source_record_id,
                            source_id=source_id,
                            sync_run_id=sync_run_id,
                            field_name="search_query",
                            value_text=queries["general"],
                            metadata={"query_type": "general"},
                            confidence=1.0,
                        )
                        insert_text_observation(
                            cur,
                            company_id=company.id,
                            source_record_id=source_record_id,
                            source_id=source_id,
                            sync_run_id=sync_run_id,
                            field_name="website_search_query",
                            value_text=queries["website"],
                            metadata={"query_type": "website"},
                            confidence=1.0,
                        )
                        insert_text_observation(
                            cur,
                            company_id=company.id,
                            source_record_id=source_record_id,
                            source_id=source_id,
                            sync_run_id=sync_run_id,
                            field_name="kununu_search_query",
                            value_text=queries["kununu"],
                            metadata={"query_type": "kununu"},
                            confidence=1.0,
                        )
                        insert_text_observation(
                            cur,
                            company_id=company.id,
                            source_record_id=source_record_id,
                            source_id=source_id,
                            sync_run_id=sync_run_id,
                            field_name="linkedin_search_query",
                            value_text=queries["linkedin"],
                            metadata={"query_type": "linkedin"},
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
                                    metadata={"provider": "mistral_web_search"},
                                    confidence=live_result.get("confidence"),
                                )
                        for candidate in live_result.get("candidate_urls") or []:
                            if isinstance(candidate, dict) and candidate.get("url"):
                                insert_text_observation(
                                    cur,
                                    company_id=company.id,
                                    source_record_id=source_record_id,
                                    source_id=source_id,
                                    sync_run_id=sync_run_id,
                                    field_name="candidate_url",
                                    value_text=candidate["url"],
                                    metadata={
                                        "provider": "mistral_web_search",
                                        "candidate_type": candidate.get("type"),
                                        "reason": candidate.get("reason"),
                                    },
                                    confidence=live_result.get("confidence"),
                                )
                processed_count += 1
                _save_checkpoint(company.id, processed_count)
                last_completed_company_id = company.id
            except MistralRateLimitError:
                failed += 1
                hit_rate_limit = True
                _save_checkpoint(last_completed_company_id, processed_count)
                for _, pending_future in submitted[index:]:
                    pending_future.cancel()
                log.warning(
                    "Stopping Mistral discovery after rate limit at company %s. Resume will continue from checkpoint.",
                    company.id,
                )
                break
            except Exception:
                failed += 1
                log.exception("Failed discovery scaffold for company %s", company.id)
                processed_count += 1
                _save_checkpoint(company.id, processed_count)

            if index % MISTRAL_PROGRESS_LOG_EVERY == 0:
                log.info(
                    "Mistral discovery progress: %d/%d processed (%d created, %d updated, %d failed)",
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
                    status="partial" if hit_rate_limit else None,
                )
        if not hit_rate_limit:
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
        description="Use live Mistral web search to discover official, Kununu, and LinkedIn URLs",
        schedule=None,
        start_date=datetime(2026, 7, 30, tzinfo=timezone.utc),
        catchup=False,
        tags=["sources", "mistral", "discovery"],
    ) as dag:
        PythonOperator(
            task_id="prepare_mistral_search_discovery",
            python_callable=run_mistral_search_discovery,
        )
    return dag


dag = _make_dag()
