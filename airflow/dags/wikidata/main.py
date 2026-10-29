"""Websites Wikidata lists for Berlin companies, verified like a guessed domain (issue #26).

Two tasks:

1. **match**, at most once a day. One SPARQL query (`enrichment.wikidata`) and every item is matched
   to a company in SQL, with the one name key (`normalize_company_name`):
   - register digits AND the same name key -> `match_method = 'identifier'`, confidence 1.0. Wikidata
     holds the bare register number without the court, so the number alone proves nothing;
   - else the same name key as exactly one company that is not dissolved -> `'fuzzy'`, confidence
     0.9. Never a dissolved one: its name may be in use again (the rule register_announcements uses).
   Each match is a source record, `external_id` = the Wikidata id.
   Measured on 2026-10-30 on the local Berlin database: 531 of 4,174 items matched, 31 of them by
   register number and name.
2. **verify**, in batches, chaining itself while proposals wait. Every listed website goes through
   `pipeline.verify_proposed`, the same page reading and `confidence.score` as an OSM website tag.
   Nothing on the page ties it to the company -> below the bar, never shown. On 2026-10-30, 229 of
   the 531 were proven, 139 of them new to website_discovery; no result was hand-checked.

A website that no longer verifies on a later check, or whose item no longer matches that company,
drops to confidence 0; nothing is deleted.
Register officers are loaded per batch as comparison keys, in memory only, as in website_discovery.
"""
from __future__ import annotations

import dataclasses
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json, execute_values

from enrichment import confidence as conf
from enrichment import name_frequency, pipeline, wikidata
from enrichment.observations import field_name_for
from enrichment.register_names import DISSOLVED
from enrichment.officers import load_officers
from source_enrichment_common import (
    CompanyCandidate,
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
)

log = logging.getLogger(__name__)

DAG_ID = "wikidata"
SOURCE_KEY = "wikidata"
BATCH_SIZE = int(os.getenv("WIKIDATA_BATCH_SIZE", "100"))
WORKERS = int(os.getenv("WIKIDATA_WORKERS", "8"))
RECHECK_AFTER_DAYS = int(os.getenv("WIKIDATA_RECHECK_DAYS", "90"))
_URL_FIELDS = ["website", "candidate_url", "directory_url"]


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Wikidata (official websites, P856)",
                         source_type="knowledge_base", metadata={"dag_id": DAG_ID, "licence": "CC0"})


# --- match ----------------------------------------------------------------------------------


def run_match() -> int:
    """Query Wikidata and link its items to companies. Returns how many items matched; skipped
    (0) when the last match is less than 20 hours old, so a chain of verify batches queries once."""
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            cur.execute("SELECT last_synced_at > now() - interval '20 hours' FROM public.sources WHERE id = %s",
                        (source_id,))
            if cur.fetchone()[0]:
                log.info("Matched less than 20 hours ago; not querying Wikidata again.")
                return 0
        items = wikidata.fetch()
        with connection, connection.cursor() as cur:
            sync_run_id = start_sync_run(cur, source_id=source_id)
            matched = match_items(cur, items, source_id=source_id)
            cur.execute("UPDATE public.sources SET last_synced_at = now() WHERE id = %s", (source_id,))
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(items), records_created=matched,
                            records_updated=0, records_failed=0)
        log.info("%d Wikidata items with a Berlin headquarters and a website; %d matched a company.",
                 len(items), matched)
        return matched
    finally:
        connection.close()


def match_items(cur: Any, items: list[wikidata.Item], *, source_id: str) -> int:
    """Link items to companies (see the module docstring) and keep each match as a source record."""
    cur.execute("CREATE TEMP TABLE _wd (qid text PRIMARY KEY, label text, register_digits text,"
                " websites jsonb) ON COMMIT DROP")
    execute_values(cur, "INSERT INTO _wd VALUES %s",
                   [(i.qid, i.label, i.register_digits, Json(list(i.websites))) for i in items if i.label])
    cur.execute(
        """
        WITH named AS (
          SELECT w.qid, n.company_id, w.register_digits, c.registration_number,
                 st.value_text IS DISTINCT FROM %(dissolved)s AS live
          FROM _wd w
          JOIN public.company_field_observations n
            ON n.field_name = 'name' AND n.is_canonical
           AND public.normalize_company_name(n.value_text) = public.normalize_company_name(w.label)
          JOIN public.companies c ON c.id = n.company_id AND c.merged_into IS NULL
          LEFT JOIN public.company_field_observations st
            ON st.company_id = n.company_id AND st.field_name = 'register_status' AND st.is_canonical
        ),
        by_register AS (
          SELECT qid, company_id FROM named
          WHERE register_digits IS NOT NULL AND registration_number ~ ('\\m' || register_digits || '\\M')
        ),
        by_name AS (
          SELECT qid, (array_agg(company_id))[1] AS company_id FROM named
          WHERE live AND qid NOT IN (SELECT qid FROM by_register)
          GROUP BY qid HAVING count(*) = 1
        )
        SELECT qid, company_id, 'identifier', 1.0 FROM by_register
        UNION ALL
        SELECT qid, company_id, 'fuzzy', 0.9 FROM by_name
        """,
        {"dissolved": DISSOLVED},
    )
    matches = cur.fetchall()
    cur.execute("SELECT qid, label, register_digits, websites FROM _wd")
    item_by_qid = {row[0]: row for row in cur.fetchall()}
    rows = []
    for qid, company_id, method, confidence in matches:
        _, label, digits, websites = item_by_qid[qid]
        rows.append((company_id, source_id, qid, f"https://www.wikidata.org/wiki/{qid}", label,
                     Json({"qid": qid, "label": label, "register_digits": digits, "websites": websites}),
                     method, confidence))
    execute_values(cur, """
        INSERT INTO public.company_source_records
          (company_id, source_id, external_id, external_url, source_company_name, source_payload,
           match_method, match_confidence, matched_at, is_active)
        VALUES %s
        ON CONFLICT (source_id, external_id) DO UPDATE SET
          company_id = EXCLUDED.company_id,
          source_company_name = EXCLUDED.source_company_name,
          -- Keep when it was last verified; new websites, another company or a
          -- record that was inactive (its websites dropped to 0) are checked again.
          source_payload = EXCLUDED.source_payload || jsonb_build_object('verified_at',
            CASE WHEN public.company_source_records.source_payload -> 'websites' = EXCLUDED.source_payload -> 'websites'
                  AND public.company_source_records.company_id = EXCLUDED.company_id
                  AND public.company_source_records.is_active
                 THEN public.company_source_records.source_payload -> 'verified_at' END),
          match_method = EXCLUDED.match_method,
          match_confidence = EXCLUDED.match_confidence,
          matched_at = now(), last_seen_at = now(), last_synced_at = now(), is_active = true
    """, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, now(), true)", page_size=1000)
    # An item that no longer matches keeps its record, inactive: never deleted.
    cur.execute("UPDATE public.company_source_records SET is_active = false"
                " WHERE source_id = %s AND is_active AND NOT (external_id = ANY(%s))",
                (source_id, [m[0] for m in matches]))
    # Its websites drop to 0 as well, and so do those it gave a company it no longer matches.
    cur.execute(
        """
        UPDATE public.company_field_observations o SET confidence = 0, updated_at = now()
        FROM public.company_source_records r
        WHERE o.company_source_record_id = r.id AND r.source_id = %s
          AND (NOT r.is_active OR o.company_id <> r.company_id)
          AND o.field_name = ANY(%s) AND o.confidence > 0
        """,
        (source_id, _URL_FIELDS),
    )
    return len(matches)


# --- verify ---------------------------------------------------------------------------------


def run_verify() -> int:
    """Verify one batch of proposed websites. Returns how many records it checked: 0 skips the
    coordinator, a full batch starts the next run."""
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            cur.execute(
                """
                SELECT r.id, r.company_id::text, r.source_payload
                FROM public.company_source_records r
                WHERE r.source_id = %s AND r.is_active
                  AND (r.source_payload ->> 'verified_at' IS NULL
                       OR (r.source_payload ->> 'verified_at')::timestamptz < now() - %s * interval '1 day')
                ORDER BY r.source_payload ->> 'verified_at' NULLS FIRST, r.id
                LIMIT %s
                """,
                (source_id, RECHECK_AFTER_DAYS, BATCH_SIZE),
            )
            records = cur.fetchall()
            if not records:
                log.info("Every Wikidata proposal was verified in the last %d days.", RECHECK_AFTER_DAYS)
                return 0
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = _companies(cur, [company_id for _, company_id, _ in records])
            officers = load_officers(cur, list(companies))
            cur.execute("SELECT company_name FROM public.v_companies_enriched")
            common = name_frequency.common_words([row[0] for row in cur.fetchall()])

        proven = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            jobs = {}
            for record_id, company_id, payload in records:
                company = dataclasses.replace(companies[company_id], officers=officers.get(company_id, frozenset()))
                jobs[pool.submit(pipeline.verify_proposed, company, payload["websites"],
                                 method=conf.METHOD_WIKIDATA, common=common)] = (record_id, company_id, payload)
            for job in as_completed(jobs):
                record_id, company_id, payload = jobs[job]
                try:
                    answers = job.result()
                except Exception:  # noqa: BLE001  one bad page must not discard the batch
                    log.exception("Verifying Wikidata websites failed for %s; retried later", company_id)
                    continue
                proven += any(field_name_for(a) == "website" for a in answers)
                with connection, connection.cursor() as cur:
                    write_answers(cur, company_id=company_id, record_id=record_id, source_id=source_id,
                                  sync_run_id=sync_run_id, qid=payload["qid"], answers=answers)

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(records), records_created=proven,
                            records_updated=0, records_failed=0)
        log.info("Verified %d Wikidata proposals: %d websites proven.", len(records), proven)
        return len(records)
    finally:
        connection.close()


def _companies(cur: Any, company_ids: list[str]) -> dict[str, CompanyCandidate]:
    cur.execute(
        """
        SELECT id::text, company_name, address, postcode, city, registration_number, legal_form
        FROM public.v_companies_enriched WHERE id = ANY(%s::uuid[])
        """,
        (company_ids,),
    )
    return {row[0]: CompanyCandidate(*row) for row in cur.fetchall()}


def write_answers(cur: Any, *, company_id: str, record_id: str, source_id: str, sync_run_id: str,
                  qid: str, answers: list[dict[str, Any]]) -> None:
    """Every verified URL as an observation, filed like discovery's; earlier ones not confirmed drop to 0."""
    written = []
    for answer in answers:
        written.append(insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
            sync_run_id=sync_run_id, field_name=field_name_for(answer), value_text=answer["url"],
            confidence=answer["confidence"],
            metadata={
                "provider": SOURCE_KEY, "qid": qid, "methods": answer["methods"],
                "verified_by": answer["verified_by"], "band": answer["band"],
                "evidence": sorted(answer["evidence"]), "why": answer["why"],
                "matched_url": answer["imprint_url"] or answer["url"], "imprint_url": answer["imprint_url"],
                "register_number_seen": answer["register_number_seen"],
            },
        ))
    cur.execute(
        """
        UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
        WHERE company_id = %s AND company_source_record_id = %s AND field_name = ANY(%s)
          AND NOT (id = ANY(%s::uuid[])) AND confidence > 0
        """,
        (company_id, record_id, _URL_FIELDS, [str(w) for w in written]),
    )
    cur.execute(
        "UPDATE public.company_source_records SET source_payload = source_payload || jsonb_build_object("
        "'verified_at', now()), last_synced_at = now() WHERE id = %s",
        (record_id,),
    )


def is_full_batch(taken: int | None) -> bool:
    return bool(taken) and taken >= BATCH_SIZE


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    # Like website_discovery: a full verify batch starts the next run; matching inside it queries
    # Wikidata at most once a day (20 hours, so the next @daily run is not skipped).
    with DAG(
        dag_id=DAG_ID,
        description="Websites Wikidata lists for Berlin companies, verified like a guessed domain",
        schedule="@daily", start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites", "wikidata"],
    ) as dag:
        match = pipeline_task("match_items", run_match)
        verify = pipeline_task("verify_websites", run_verify, returns=True)
        took_any = ShortCircuitOperator(
            task_id="took_any", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="verify_websites")),
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        more_left = ShortCircuitOperator(
            task_id="more_left", ignore_downstream_trigger_rules=False,
            python_callable=lambda ti: is_full_batch(ti.xcom_pull(task_ids="verify_websites")),
        )
        next_batch = TriggerDagRunOperator(task_id="next_batch", trigger_dag_id=DAG_ID, wait_for_completion=False)
        match >> verify >> took_any >> [coordinate, more_left]
        more_left >> next_batch
    return dag


dag = _make_dag()
