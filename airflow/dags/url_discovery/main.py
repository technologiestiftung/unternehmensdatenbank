from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .candidate_generator import generate_candidates
from .http_client import fetch_impressum, verify_url
from .imprint_extractor import extract_from_html
from .scorer import AUTO_ACCEPT_SCORE, MIN_STORE_SCORE, score_candidate
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

SOURCE_KEY = "url_discovery"
SOURCE_NAME = "URL Pattern Discovery"
SOURCE_TYPE = "url_matching"
ENABLED_FLAG = "ENABLE_URL_DISCOVERY"

URL_DISCOVERY_BATCH_SIZE = int(os.getenv("URL_DISCOVERY_BATCH_SIZE", "50"))
URL_DISCOVERY_CONCURRENCY = int(os.getenv("URL_DISCOVERY_CONCURRENCY", "5"))
URL_DISCOVERY_REQUEST_DELAY_S = float(os.getenv("URL_DISCOVERY_REQUEST_DELAY_S", "0.5"))
URL_DISCOVERY_PROGRESS_LOG_EVERY = int(os.getenv("URL_DISCOVERY_PROGRESS_LOG_EVERY", "25"))
URL_DISCOVERY_CHECKPOINT_FILE = Path(
    os.getenv(
        "URL_DISCOVERY_CHECKPOINT_FILE",
        "/opt/airflow/data/url_discovery_checkpoint.json",
    )
)

_MAX_CANDIDATES_TO_CRAWL = 5


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


def _load_checkpoint() -> dict[str, Any]:
    if not URL_DISCOVERY_CHECKPOINT_FILE.exists():
        return {}
    try:
        return json.loads(URL_DISCOVERY_CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning("Could not parse checkpoint, starting fresh.")
        return {}


def _save_checkpoint(last_company_id: str | None, processed_count: int) -> None:
    URL_DISCOVERY_CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    URL_DISCOVERY_CHECKPOINT_FILE.write_text(
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
    if URL_DISCOVERY_CHECKPOINT_FILE.exists():
        URL_DISCOVERY_CHECKPOINT_FILE.unlink()


def _verify_candidates(candidates: list[str]) -> list[dict[str, Any]]:
    if not candidates:
        return []
    verified: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=URL_DISCOVERY_CONCURRENCY) as executor:
        futures = {executor.submit(verify_url, url): url for url in candidates}
        for future in as_completed(futures):
            result = future.result()
            if result:
                verified.append(result)
    return verified


def _enrich_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    final_url = candidate["final_url"]
    try:
        imprint_result = fetch_impressum(final_url)
        if imprint_result:
            html, imprint_url = imprint_result
            extracted = extract_from_html(html)
            candidate["imprint_url"] = imprint_url
            candidate["extracted"] = extracted
        else:
            candidate["imprint_url"] = None
            candidate["extracted"] = {}
    except Exception as exc:
        log.debug("Imprint fetch failed for %s: %s", final_url, exc)
        candidate["imprint_url"] = None
        candidate["extracted"] = {}
    return candidate


def run_url_discovery() -> None:
    checkpoint = _load_checkpoint()
    last_company_id = checkpoint.get("last_company_id")

    companies = fetch_companies_without_source_after_id(
        SOURCE_KEY, after_id=last_company_id, limit=URL_DISCOVERY_BATCH_SIZE
    )
    if not companies and last_company_id:
        _delete_checkpoint()
        companies = fetch_companies_without_source_after_id(SOURCE_KEY, after_id=None, limit=URL_DISCOVERY_BATCH_SIZE)
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
        processed_count = checkpoint.get("processed_count", 0)

        for index, company in enumerate(companies, start=1):
            t0 = time.monotonic()
            try:
                if URL_DISCOVERY_REQUEST_DELAY_S > 0 and index > 1:
                    time.sleep(URL_DISCOVERY_REQUEST_DELAY_S)

                candidates = generate_candidates(company.company_name, company.city)

                verified = _verify_candidates(candidates)

                enriched: list[dict[str, Any]] = []
                for c in verified[:_MAX_CANDIDATES_TO_CRAWL]:
                    enriched.append(_enrich_candidate(c))

                for c in enriched:
                    c["score"] = score_candidate(c.get("extracted") or {}, company)

                enriched.sort(key=lambda c: c["score"], reverse=True)
                best = enriched[0] if enriched else None

                with connection:
                    with connection.cursor() as cur:
                        payload = dump_preview_payload(company, SOURCE_KEY)
                        payload["candidates_generated"] = len(candidates)
                        payload["candidates_verified"] = len(verified)
                        payload["results"] = [
                            {
                                "url": c["url"],
                                "final_url": c["final_url"],
                                "score": c["score"],
                                "imprint_url": c.get("imprint_url"),
                                "extracted": c.get("extracted") or {},
                            }
                            for c in enriched
                        ]

                        source_record_id, is_created = upsert_source_record(
                            cur,
                            company_id=company.id,
                            source_id=source_id,
                            external_id=f"{SOURCE_KEY}:{company.id}",
                            external_url=best["final_url"] if best and best["score"] >= 0 else None,
                            source_company_name=company.company_name,
                            source_payload=payload,
                        )
                        created += int(is_created)
                        updated += int(not is_created)

                        for c in enriched:
                            if c["score"] < MIN_STORE_SCORE:
                                continue
                            insert_text_observation(
                                cur,
                                company_id=company.id,
                                source_record_id=source_record_id,
                                source_id=source_id,
                                sync_run_id=sync_run_id,
                                field_name="candidate_url",
                                value_text=c["final_url"],
                                metadata={
                                    "original_url": c["url"],
                                    "score": c["score"],
                                    "imprint_url": c.get("imprint_url"),
                                    "register_number": (c.get("extracted") or {}).get("register_number"),
                                    "postcode": (c.get("extracted") or {}).get("postcode"),
                                },
                                confidence=min(1.0, max(0.0, (c["score"] + 50) / 150)),
                            )

                        if best:
                            if best["score"] >= AUTO_ACCEPT_SCORE:
                                insert_text_observation(
                                    cur,
                                    company_id=company.id,
                                    source_record_id=source_record_id,
                                    source_id=source_id,
                                    sync_run_id=sync_run_id,
                                    field_name="website",
                                    value_text=best["final_url"],
                                    metadata={
                                        "score": best["score"],
                                        "verified_by": "register_number_match",
                                        "register_number": (best.get("extracted") or {}).get("register_number"),
                                    },
                                    confidence=1.0,
                                )
                            elif best["score"] >= 0:
                                insert_text_observation(
                                    cur,
                                    company_id=company.id,
                                    source_record_id=source_record_id,
                                    source_id=source_id,
                                    sync_run_id=sync_run_id,
                                    field_name="website",
                                    value_text=best["final_url"],
                                    metadata={"score": best["score"], "verified_by": "url_pattern"},
                                    confidence=0.5,
                                )

                elapsed = time.monotonic() - t0
                best_url = best["final_url"] if best else None
                best_score = best["score"] if best else None
                log.info(
                    "[%d] %s — %.1fs | generated=%d verified=%d score=%s | %s",
                    index,
                    company.company_name,
                    elapsed,
                    len(candidates),
                    len(verified),
                    best_score,
                    best_url or "nothing",
                )

                processed_count += 1
                _save_checkpoint(company.id, processed_count)

            except Exception:
                failed += 1
                log.exception("URL discovery failed for company %s", company.id)
                processed_count += 1
                _save_checkpoint(company.id, processed_count)

            if index % URL_DISCOVERY_PROGRESS_LOG_EVERY == 0:
                log.info(
                    "URL discovery progress: %d/%d processed (%d created, %d updated, %d failed)",
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
                )
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
        description="Discover company websites via URL pattern matching and imprint verification",
        schedule=None,
        start_date=datetime(2026, 8, 4, tzinfo=timezone.utc),
        catchup=False,
        tags=["sources", "url", "discovery", "matching"],
    ) as dag:
        PythonOperator(
            task_id="run_url_discovery",
            python_callable=run_url_discovery,
        )
    return dag


dag = _make_dag()
