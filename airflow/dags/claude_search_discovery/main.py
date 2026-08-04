from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sys as _sys, os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from claude_http import ClaudeRateLimitError, require_anthropic_api_key, run_websearch_prompt
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

SOURCE_KEY = "claude_search_discovery"
SOURCE_NAME = "Claude Web Search Discovery"
SOURCE_TYPE = "web_search"
ENABLED_FLAG = "ENABLE_CLAUDE_SEARCH_DISCOVERY"
CLAUDE_DISCOVERY_MAX_RESULTS = int(os.getenv("CLAUDE_DISCOVERY_MAX_RESULTS", "5"))
CLAUDE_PROGRESS_LOG_EVERY = int(os.getenv("CLAUDE_PROGRESS_LOG_EVERY", "10"))
CLAUDE_CHECKPOINT_FILE = Path(
    os.getenv("CLAUDE_DISCOVERY_CHECKPOINT_FILE", "/opt/airflow/data/claude_search_discovery_checkpoint.json")
)


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not enabled.")
    require_anthropic_api_key()


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


def _build_prompt(company_name: str, queries: dict[str, str], address: str | None, registration_number: str | None) -> str:
    return f"""
Find the best matching public company information for this German company using web search.

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
5. keep the result set tight; inspect at most {CLAUDE_DISCOVERY_MAX_RESULTS} relevant search hits per target
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
    result["raw_responses"] = response["raw_responses"]
    return result


def _ensure_checkpoint_parent() -> None:
    CLAUDE_CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_checkpoint() -> dict[str, Any]:
    if not CLAUDE_CHECKPOINT_FILE.exists():
        return {}
    try:
        return json.loads(CLAUDE_CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning("Could not parse %s, starting without checkpoint.", CLAUDE_CHECKPOINT_FILE)
        return {}


def _save_checkpoint(last_company_id: str | None, processed_count: int) -> None:
    _ensure_checkpoint_parent()
    CLAUDE_CHECKPOINT_FILE.write_text(
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
    if CLAUDE_CHECKPOINT_FILE.exists():
        CLAUDE_CHECKPOINT_FILE.unlink()


def run_claude_search_discovery() -> None:
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
        hit_rate_limit = False

        for index, company in enumerate(companies, start=1):
            try:
                with connection:
                    with connection.cursor() as cur:
                        payload = dump_preview_payload(company, SOURCE_KEY)
                        payload["tool"] = "claude web_search"
                        payload["enabled"] = True
                        queries = build_search_queries(company)
                        live_result = _run_live_discovery(
                            company_name=company.company_name,
                            queries=queries,
                            address=company.address,
                            registration_number=company.registration_number,
                        )
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
                            metadata={"query_type": "general", "provider": "claude_web_search"},
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
                            metadata={"query_type": "website", "provider": "claude_web_search"},
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
                            metadata={"query_type": "kununu", "provider": "claude_web_search"},
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
                            metadata={"query_type": "linkedin", "provider": "claude_web_search"},
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
                                    metadata={"provider": "claude_web_search"},
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
                                        "provider": "claude_web_search",
                                        "candidate_type": candidate.get("type"),
                                        "reason": candidate.get("reason"),
                                    },
                                    confidence=live_result.get("confidence"),
                                )
                processed_count += 1
                _save_checkpoint(company.id, processed_count)
            except ClaudeRateLimitError:
                failed += 1
                hit_rate_limit = True
                _save_checkpoint(company.id, processed_count)
                log.warning(
                    "Stopping Claude discovery after rate limit at company %s. Resume will continue from checkpoint.",
                    company.id,
                )
                break
            except Exception:
                failed += 1
                log.exception("Failed Claude discovery for company %s", company.id)
                processed_count += 1
                _save_checkpoint(company.id, processed_count)

            if index % CLAUDE_PROGRESS_LOG_EVERY == 0:
                log.info(
                    "Claude discovery progress: %d/%d processed (%d created, %d updated, %d failed)",
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

    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    with DAG(
        dag_id=SOURCE_KEY,
        description="Use live Claude web search to discover company metadata and candidate URLs",
        schedule=None,
        start_date=datetime(2026, 7, 31, tzinfo=timezone.utc),
        catchup=False,
        tags=["sources", "claude", "discovery"],
    ) as dag:
        discover = PythonOperator(
            task_id="prepare_claude_search_discovery",
            python_callable=run_claude_search_discovery,
        )
        trigger_coordinator = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator",
            trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        discover >> trigger_coordinator
    return dag


dag = _make_dag()
