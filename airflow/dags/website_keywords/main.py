"""Website keywords: what a company offers, in the words its own website uses.

Target column 7.1 ("Stichworte zur Tätigkeit und Produkte / Dienstleistungen"). The rule for what
counts as a keyword lives in `enrichment.activity_keywords`; this file runs it. Each run does two
things, then lets `pipeline_coordinator` publish the result:

1. **read.** One batch of companies with a published website that this source has not read yet:
   the homepage, and the services, products and solutions pages it links to. Every keyword-shaped
   term goes into the company's source record (`source_payload.candidates`). Nothing is published
   here, because whether a term is generic is not known from one company.
2. **publish.** No downloads. Counts every stored company's candidates, drops the terms too common
   to say anything (`activity_keywords.generic_terms`), and writes one `activity_keywords`
   observation per company: the keywords joined by "; ", the list itself in `metadata.keywords`.
   Re-run every time, so a term that becomes generic as more companies are read leaves the
   companies read before it too. A company left with no keywords drops to confidence 0 and the
   coordinator un-publishes it.

**No state outside Postgres**, as in `website_discovery`: "already read" is the company's source
record, so every task can run in a fresh pod on the Data Hub.

Nothing here has been hand-checked. 0.6 is the same unmeasured confidence `manager_ner` uses.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json

from enrichment import activity_keywords as ak
from source_enrichment_common import (
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "website_keywords"
SOURCE_KEY = "website_keywords"
FIELD_NAME = "activity_keywords"
ENABLED_FLAG = "ENABLE_WEBSITE_KEYWORDS"
BATCH_SIZE = int(os.getenv("WEBSITE_KEYWORDS_BATCH_SIZE", "50"))
CONFIDENCE = 0.6


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Website keywords", source_type="website",
                         metadata={"dag_id": DAG_ID})


# --- read -----------------------------------------------------------------------------------


def read_site(website: str, company_name: str) -> dict[str, Any]:
    """Download the homepage and its offer pages; return the candidates and the pages read."""
    from enrichment.content_pages import links_from_homepage
    from enrichment.http_client import fetch_page

    homepage = fetch_page(website) or ""
    links = [x for x in links_from_homepage(homepage, website) if x.category in ak.OFFER_CATEGORIES]
    pages: list[str] = []
    offer_html: list[str] = []
    for link in links:
        html = fetch_page(link.url)
        if html:
            pages.append(link.url)
            offer_html.append(html)
    return {
        "candidates": ak.candidates(homepage, offer_html, company_name),
        "pages": pages,
        "homepage_loaded": bool(homepage),
    }


def _companies_to_read(cur: Any) -> list[tuple[str, str, str]]:
    cur.execute(
        """
        SELECT e.id, e.company_name, e.website_url
        FROM public.v_companies_enriched e
        WHERE e.website_url IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.company_source_records r
            JOIN public.sources s ON s.id = r.source_id
            WHERE r.company_id = e.id AND s.key = %s
          )
        ORDER BY e.id
        LIMIT %s
        """,
        (SOURCE_KEY, BATCH_SIZE),
    )
    return cur.fetchall()


def run_read() -> None:
    _require_enabled()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = _companies_to_read(cur)

        with_terms = failed = 0
        for company_id, company_name, website in companies:
            try:
                payload = read_site(website, company_name)
            except Exception as exc:  # noqa: BLE001  one unreachable site must not stop the batch
                log.warning("Website not readable for %s: %s", company_id, type(exc).__name__)
                payload, failed = {"candidates": [], "pages": [], "error": type(exc).__name__}, failed + 1
            with_terms += bool(payload["candidates"])
            with connection, connection.cursor() as cur:
                # Written even with no candidates, so the company counts as read.
                upsert_source_record(
                    cur, company_id=company_id, source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{company_id}", external_url=website,
                    source_company_name=company_name, source_payload=payload,
                )

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=with_terms, records_updated=0, records_failed=failed)
        log.info("Read %d websites: %d with candidate keywords, %d unreadable.",
                 len(companies), with_terms, failed)
    finally:
        connection.close()


# --- publish --------------------------------------------------------------------------------


def planned_write(
    terms: list[str], generic: set[str], existing: tuple[str, float] | None
) -> tuple[str, list[str]] | None:
    """What publishing does for one company: ("insert" | "update" | "drop", keywords), or None.

    Pure, so it can be tested without a database. `existing` is the stored observation's value and
    confidence. "drop" means the company had keywords and now has none: its observation falls to
    confidence 0, below the coordinator's bar.
    """
    found = ak.keywords(terms, generic)
    value = "; ".join(found)
    if existing is None:
        return ("insert", found) if found else None
    old_value, old_confidence = existing
    if not found:
        return ("drop", []) if old_confidence > 0 else None
    if value != old_value or old_confidence != CONFIDENCE:
        return ("update", found)
    return None


def run_publish() -> None:
    _require_enabled()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            cur.execute(
                """
                SELECT r.company_id::text, r.id, r.external_url,
                       COALESCE(r.source_payload -> 'candidates', '[]'::jsonb),
                       COALESCE(r.source_payload -> 'pages', '[]'::jsonb)
                FROM public.company_source_records r
                WHERE r.source_id = %s
                """,
                (source_id,),
            )
            records = cur.fetchall()
            cur.execute(
                """
                SELECT company_id::text, id, value_text, COALESCE(confidence, 0)
                FROM public.company_field_observations
                WHERE source_id = %s AND field_name = %s
                """,
                (source_id, FIELD_NAME),
            )
            existing = {row[0]: (row[1], row[2], float(row[3])) for row in cur.fetchall()}

            generic = ak.generic_terms(terms for _, _, _, terms, _ in records)
            if len(records) < ak.MIN_CORPUS:
                log.info("Only %d companies read; %d needed to measure genericness. Nothing published.",
                         len(records), ak.MIN_CORPUS)
                finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(records),
                                records_created=0, records_updated=0, records_failed=0)
                return

            counts = {"insert": 0, "update": 0, "drop": 0}
            for company_id, record_id, website, terms, pages in records:
                stored = existing.get(company_id)
                plan = planned_write(terms, generic, stored[1:] if stored else None)
                if plan is None:
                    continue
                action, found = plan
                counts[action] += 1
                metadata = {"keywords": found, "matched_url": pages[0] if pages else website,
                            "pages": pages, "generic_measured_over": len(records)}
                if action == "insert":
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=FIELD_NAME, value_text="; ".join(found),
                        confidence=CONFIDENCE, metadata=metadata,
                    )
                else:
                    # Updated in place: `activity_keywords` has no upsert key, and one row per
                    # company is the point.
                    cur.execute(
                        """
                        UPDATE public.company_field_observations
                        SET value_text = COALESCE(NULLIF(%s, ''), value_text), confidence = %s,
                            metadata = %s, updated_at = now()
                        WHERE id = %s
                        """,
                        ("; ".join(found), CONFIDENCE if found else 0, Json(metadata), stored[0]),
                    )

            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(records),
                            records_created=counts["insert"],
                            records_updated=counts["update"] + counts["drop"], records_failed=0)
        log.info("Keywords over %d companies (%d generic terms): %d new, %d changed, %d dropped.",
                 len(records), len(generic), counts["insert"], counts["update"], counts["drop"])
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Product and service keywords from the company's own website",
        schedule=None, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites", "keywords"],
    ) as dag:
        read = PythonOperator(task_id="read_websites", python_callable=run_read)
        publish = PythonOperator(task_id="publish_keywords", python_callable=run_publish)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        read >> publish >> coordinate
    return dag


dag = _make_dag()
