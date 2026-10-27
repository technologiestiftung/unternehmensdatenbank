from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from url_discovery.candidate_generator import generate_candidates
from url_discovery.http_client import fetch_impressum, verify_url
from url_discovery.imprint_extractor import extract_from_html
from url_discovery.scorer import AUTO_ACCEPT_SCORE, MIN_STORE_SCORE, name_uniqueness, score_candidate
from search_common import is_own_site_candidate, own_site_verdict
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
                # Only a page that is the company's OWN site may become its website. This DAG went
                # without the check for months and it cost 16 of its 53 canonical websites: domain
                # parking marketplaces (`fruits.co/de/domain/wilfried.de`), a domain reseller's
                # landing pages (`ts.domainname.de/...`), `grow.google/intl/de/`, and two pairs of
                # different companies sharing one short domain — `wm.de` for "WM Immo GmbH die
                # Zweite" and "die Dritte", `zum.de/portal` for two unrelated "Zum ..." companies.
                # A high score does not make a page the company's: `is_own_site_candidate` ignores
                # the score it is handed, deliberately, because a register number on a page proves
                # the page is ABOUT a company and never that it IS that company's site.
                #
                # Same call, same order as `search_discovery_runner.py:308-315`. One predicate for
                # this question, everywhere — a looser copy is the recurring bug in this repo.
                own_site = [
                    c for c in enriched
                    if is_own_site_candidate(
                        c["final_url"], company.company_name, c["score"], AUTO_ACCEPT_SCORE,
                    )
                ]
                best = own_site[0] if own_site else None
                rejected_as_third_party = len(enriched) - len(own_site)

                with connection:
                    with connection.cursor() as cur:
                        payload = dump_preview_payload(company, SOURCE_KEY)
                        payload["candidates_generated"] = len(candidates)
                        payload["candidates_verified"] = len(verified)
                        # How many crawled pages were refused as somebody else's site. Same key as
                        # `search_discovery_runner.py:315`, and the only place this DAG records
                        # that a candidate was dropped rather than simply not found.
                        payload["best_rejected_as_third_party"] = rejected_as_third_party
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
                            # `candidate_url` competes to become the website; `directory_url` is a
                            # field the resolver never reads, so a page about the company is kept
                            # as evidence and can never be promoted. Mirrors
                            # `search_discovery_runner.py:415-426`.
                            verdict = own_site_verdict(c["final_url"], company.company_name)
                            insert_text_observation(
                                cur,
                                company_id=company.id,
                                source_record_id=source_record_id,
                                source_id=source_id,
                                sync_run_id=sync_run_id,
                                field_name=(
                                    "candidate_url" if verdict == "own_site" else "directory_url"
                                ),
                                value_text=c["final_url"],
                                metadata={
                                    "original_url": c["url"],
                                    "score": c["score"],
                                    "rejected_as": None if verdict == "own_site" else verdict,
                                    "imprint_url": c.get("imprint_url"),
                                    "register_number": (c.get("extracted") or {}).get("register_number"),
                                    "postcode": (c.get("extracted") or {}).get("postcode"),
                                },
                                confidence=min(1.0, max(0.0, (c["score"] + 50) / 150)),
                            )

                        if best:
                            if best["score"] >= AUTO_ACCEPT_SCORE:
                                _website_conf = 1.0
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
                                    confidence=_website_conf,
                                )
                            elif best["score"] >= 0:
                                _uniq = name_uniqueness(company.company_name)
                                _website_conf = round(0.35 + 0.30 * _uniq, 4)
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
                                        "verified_by": "url_pattern",
                                        "name_uniqueness": round(_uniq, 3),
                                    },
                                    confidence=_website_conf,
                                )
                            else:
                                _website_conf = None

                            # Store imprint-extracted fields with confidence
                            # scaled from website confidence (we trust the imprint
                            # as much as we trust we're on the right site).
                            if _website_conf and _website_conf >= 0.35:
                                _ext = best.get("extracted") or {}
                                _field_conf = round(_website_conf * 0.95, 4)

                                if _ext.get("phone"):
                                    insert_text_observation(
                                        cur,
                                        company_id=company.id,
                                        source_record_id=source_record_id,
                                        source_id=source_id,
                                        sync_run_id=sync_run_id,
                                        field_name="phone",
                                        value_text=_ext["phone"],
                                        metadata={"source": "impressum", "imprint_url": best.get("imprint_url")},
                                        confidence=_field_conf,
                                    )

                                if _ext.get("email"):
                                    insert_text_observation(
                                        cur,
                                        company_id=company.id,
                                        source_record_id=source_record_id,
                                        source_id=source_id,
                                        sync_run_id=sync_run_id,
                                        field_name="email",
                                        value_text=_ext["email"],
                                        metadata={"source": "impressum", "imprint_url": best.get("imprint_url")},
                                        confidence=_field_conf,
                                    )

                                for _ceo in _ext.get("ceo_names") or []:
                                    insert_text_observation(
                                        cur,
                                        company_id=company.id,
                                        source_record_id=source_record_id,
                                        source_id=source_id,
                                        sync_run_id=sync_run_id,
                                        field_name="ceo_name_or_contact",
                                        value_text=_ceo,
                                        metadata={"source": "impressum", "imprint_url": best.get("imprint_url")},
                                        confidence=round(_field_conf * 0.90, 4),
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

    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    with DAG(
        dag_id=SOURCE_KEY,
        description="Discover company websites via URL pattern matching and imprint verification",
        schedule=None,
        start_date=datetime(2026, 8, 4, tzinfo=timezone.utc),
        catchup=False,
        tags=["sources", "url", "discovery", "matching"],
    ) as dag:
        discover = PythonOperator(
            task_id="run_url_discovery",
            python_callable=run_url_discovery,
        )
        trigger_coordinator = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator",
            trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        discover >> trigger_coordinator
    return dag


dag = _make_dag()
