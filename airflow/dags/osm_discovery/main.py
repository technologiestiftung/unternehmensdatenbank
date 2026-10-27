from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from osm_discovery.overpass_client import OverpassError, OverpassRateLimitError, search_company
from osm_discovery.scorer import best_candidate
from source_enrichment_common import (
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

SOURCE_KEY = "osm_discovery"
SOURCE_NAME = "OpenStreetMap Discovery"
SOURCE_TYPE = "osm"
ENABLED_FLAG = "ENABLE_OSM_DISCOVERY"

OSM_DISCOVERY_BATCH_SIZE = int(os.getenv("OSM_DISCOVERY_BATCH_SIZE", "50"))
OSM_DISCOVERY_PROGRESS_LOG_EVERY = int(os.getenv("OSM_DISCOVERY_PROGRESS_LOG_EVERY", "25"))
OSM_DISCOVERY_CHECKPOINT_FILE = Path(
    os.getenv(
        "OSM_DISCOVERY_CHECKPOINT_FILE",
        "/opt/airflow/data/osm_discovery_checkpoint.json",
    )
)


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


def _load_checkpoint() -> dict[str, Any]:
    if not OSM_DISCOVERY_CHECKPOINT_FILE.exists():
        return {}
    try:
        return json.loads(OSM_DISCOVERY_CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning("Could not parse checkpoint, starting fresh.")
        return {}


def _save_checkpoint(last_company_id: str | None, processed_count: int) -> None:
    OSM_DISCOVERY_CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OSM_DISCOVERY_CHECKPOINT_FILE.write_text(
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
    if OSM_DISCOVERY_CHECKPOINT_FILE.exists():
        OSM_DISCOVERY_CHECKPOINT_FILE.unlink()


def run_osm_discovery() -> None:
    checkpoint = _load_checkpoint()
    last_company_id = checkpoint.get("last_company_id")

    companies = fetch_companies_without_source_after_id(
        SOURCE_KEY, after_id=last_company_id, limit=OSM_DISCOVERY_BATCH_SIZE
    )
    if not companies and last_company_id:
        _delete_checkpoint()
        companies = fetch_companies_without_source_after_id(
            SOURCE_KEY, after_id=None, limit=OSM_DISCOVERY_BATCH_SIZE
        )
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
        hit_rate_limit = False

        for index, company in enumerate(companies, start=1):
            t0 = time.monotonic()
            try:
                candidates = search_company(
                    company_name=company.company_name,
                    postcode=company.postcode or None,
                    city=company.city or None,
                )

                match = best_candidate(candidates, company)
                osm_record: dict[str, Any] | None = None
                match_score: float | None = None
                website: str | None = None
                phone: str | None = None
                osm_id: str | None = None

                if match:
                    osm_record, match_score = match
                    osm_id = osm_record.get("osm_id")
                    website = osm_record.get("website")
                    phone = osm_record.get("phone")

                elapsed = time.monotonic() - t0

                with connection:
                    with connection.cursor() as cur:
                        payload = dump_preview_payload(company, SOURCE_KEY)
                        payload["candidates_returned"] = len(candidates)
                        payload["match_score"] = match_score
                        payload["osm_id"] = osm_id
                        payload["website"] = website
                        payload["phone"] = phone
                        if osm_record:
                            payload["osm_name"] = osm_record.get("name")
                            payload["osm_postcode"] = osm_record.get("postcode")
                            payload["osm_city"] = osm_record.get("city")
                            payload["osm_category"] = osm_record.get("osm_category")
                            payload["email"] = osm_record.get("email")

                        source_record_id, is_created = upsert_source_record(
                            cur,
                            company_id=company.id,
                            source_id=source_id,
                            external_id=f"{SOURCE_KEY}:{company.id}",
                            external_url=website,
                            source_company_name=company.company_name,
                            source_payload=payload,
                        )
                        created += int(is_created)
                        updated += int(not is_created)

                        if osm_id:
                            insert_text_observation(
                                cur,
                                company_id=company.id,
                                source_record_id=source_record_id,
                                source_id=source_id,
                                sync_run_id=sync_run_id,
                                field_name="osm_id",
                                value_text=osm_id,
                                metadata={
                                    "match_score": match_score,
                                    "osm_name": (osm_record or {}).get("name"),
                                    "discovered_via": "overpass_api",
                                },
                                confidence=match_score,
                            )

                        if website:
                            insert_text_observation(
                                cur,
                                company_id=company.id,
                                source_record_id=source_record_id,
                                source_id=source_id,
                                sync_run_id=sync_run_id,
                                field_name="candidate_url",
                                value_text=website,
                                metadata={
                                    "osm_id": osm_id,
                                    "match_score": match_score,
                                    "discovered_via": "osm",
                                },
                                confidence=match_score,
                            )

                        if phone:
                            insert_text_observation(
                                cur,
                                company_id=company.id,
                                source_record_id=source_record_id,
                                source_id=source_id,
                                sync_run_id=sync_run_id,
                                field_name="phone",
                                value_text=phone,
                                metadata={
                                    "osm_id": osm_id,
                                    "source": "osm",
                                },
                                confidence=match_score,
                            )

                log.info(
                    "[%d] %s — %.1fs | candidates=%d score=%s | %s",
                    index,
                    company.company_name,
                    elapsed,
                    len(candidates),
                    f"{match_score:.2f}" if match_score is not None else "none",
                    website or "no website",
                )

                processed_count += 1
                _save_checkpoint(company.id, processed_count)

            except OverpassRateLimitError as exc:
                failed += 1
                hit_rate_limit = True
                _save_checkpoint(company.id, processed_count)
                log.warning(
                    "Stopping OSM discovery — rate limit at company %s: %s. "
                    "Resume will continue from checkpoint.",
                    company.id,
                    exc,
                )
                break
            except OverpassError as exc:
                failed += 1
                log.warning("Overpass query failed for company %s: %s", company.id, exc)
                processed_count += 1
                _save_checkpoint(company.id, processed_count)
            except Exception:
                failed += 1
                log.exception("OSM discovery failed for company %s", company.id)
                processed_count += 1
                _save_checkpoint(company.id, processed_count)

            if index % OSM_DISCOVERY_PROGRESS_LOG_EVERY == 0:
                log.info(
                    "OSM discovery progress: %d/%d processed (%d created, %d updated, %d failed)",
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
        description="Discover company data via OpenStreetMap / Overpass API",
        schedule=None,
        start_date=datetime(2026, 8, 4, tzinfo=timezone.utc),
        catchup=False,
        tags=["sources", "osm", "overpass", "discovery"],
    ) as dag:
        discover = PythonOperator(
            task_id="run_osm_discovery",
            python_callable=run_osm_discovery,
        )
        trigger_coordinator = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator",
            trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        discover >> trigger_coordinator
    return dag


dag = _make_dag()
