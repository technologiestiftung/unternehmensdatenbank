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

The page, not the match, proves a website. An item that no longer matches keeps its record,
inactive, and is still verified every 90 days. A website drops to confidence 0 only when no record
of this source for that company still proves it (`proven_urls`): its page stopped naming the
company, or its item now matches another company. Nothing is deleted.
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
from enrichment import corpus, name_frequency, pipeline, wikidata
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
    """Query Wikidata and link its items to companies. Returns how many websites it demoted, so the
    coordinator runs; skipped (0) when the last match is less than 20 hours old, so a chain of verify
    batches queries once."""
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
            matched, demoted = match_items(cur, items, source_id=source_id)
            cur.execute("UPDATE public.sources SET last_synced_at = now() WHERE id = %s", (source_id,))
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(items), records_created=matched,
                            records_updated=0, records_failed=0)
        log.info("%d Wikidata items with a Berlin headquarters and a website; %d matched a company;"
                 " %d websites demoted.", len(items), matched, demoted)
        return demoted
    finally:
        connection.close()


def match_items(cur: Any, items: list[wikidata.Item], *, source_id: str) -> tuple[int, int]:
    """Link items to companies (see the module docstring) and keep each match as a source record.
    Returns (items matched, websites demoted)."""
    cur.execute("CREATE TEMP TABLE _wd (qid text PRIMARY KEY, label text, register_digits text,"
                " websites jsonb) ON COMMIT DROP")
    execute_values(cur, "INSERT INTO _wd VALUES %s",
                   [(i.qid, i.label, i.register_digits, Json(list(i.websites))) for i in items if i.label])
    cur.execute(
        """
        WITH named AS (
          SELECT w.qid, n.company_id, w.register_digits,
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
          WHERE register_digits IS NOT NULL AND EXISTS (
            SELECT 1 FROM public.company_identifiers i
            WHERE i.company_id = named.company_id AND i.scheme = 'register'
              AND i.value ~ ('\\m' || register_digits || '\\M'))
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
    cur.execute("SELECT external_id, company_id::text FROM public.company_source_records WHERE source_id = %s",
                (source_id,))
    left = moved_from(dict(cur.fetchall()), {qid: str(company_id) for qid, company_id, _, _ in matches})
    texts = {i.qid: (i.wikipedia_url, i.description) for i in items}
    rows = []
    for qid, company_id, method, confidence in matches:
        _, label, digits, websites = item_by_qid[qid]
        article, description = texts.get(qid, (None, None))
        rows.append((company_id, source_id, qid, f"https://www.wikidata.org/wiki/{qid}", label,
                     Json({"qid": qid, "label": label, "register_digits": digits, "websites": websites,
                           "wikipedia_url": article, "description": description}),
                     method, confidence))
    execute_values(cur, """
        INSERT INTO public.company_source_records
          (company_id, source_id, external_id, external_url, source_company_name, source_payload,
           match_method, match_confidence, matched_at, is_active)
        VALUES %s
        ON CONFLICT (source_id, external_id) DO UPDATE SET
          company_id = EXCLUDED.company_id,
          source_company_name = EXCLUDED.source_company_name,
          -- Keep when it was last verified; new websites or another company are checked again.
          -- What it proved stays only while it names the same company.
          source_payload = EXCLUDED.source_payload || jsonb_build_object(
            'verified_at',
            CASE WHEN public.company_source_records.source_payload -> 'websites' = EXCLUDED.source_payload -> 'websites'
                  AND public.company_source_records.company_id = EXCLUDED.company_id
                 THEN public.company_source_records.source_payload -> 'verified_at' END,
            'proven_urls',
            CASE WHEN public.company_source_records.company_id = EXCLUDED.company_id
                 THEN public.company_source_records.source_payload -> 'proven_urls' END),
          match_method = EXCLUDED.match_method,
          match_confidence = EXCLUDED.match_confidence,
          matched_at = now(), last_seen_at = now(), last_synced_at = now(), is_active = true
    """, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, now(), true)", page_size=1000)
    # An item that no longer matches keeps its record, inactive: never deleted, still verified.
    cur.execute("UPDATE public.company_source_records SET is_active = false"
                " WHERE source_id = %s AND is_active AND NOT (external_id = ANY(%s))",
                (source_id, [m[0] for m in matches]))
    return len(matches), demote_unproven(cur, source_id=source_id, company_ids=sorted(left))


def moved_from(previous: dict[str, str], current: dict[str, str]) -> set[str]:
    """Companies an item matched before and now matches another company instead."""
    return {company_id for qid, company_id in previous.items() if current.get(qid, company_id) != company_id}


def unproven(observations: list[tuple[str, str, str]], proven: set[tuple[str, str]],
             written: frozenset[str] = frozenset()) -> list[str]:
    """Ids of (id, company_id, url) observations to drop to 0: not just written, and no record of
    their company lists their URL as proven."""
    return [obs_id for obs_id, company_id, url in observations
            if obs_id not in written and (company_id, url) not in proven]


def demote_unproven(cur: Any, *, source_id: str, company_ids: list[str],
                    written: frozenset[str] = frozenset()) -> int:
    """Set these companies' Wikidata URLs to 0, except those just written and those a record of
    theirs still proves. Returns how many it demoted."""
    if not company_ids:
        return 0
    cur.execute(
        """
        SELECT id::text, company_id::text, value_text FROM public.company_field_observations
        WHERE source_id = %s AND company_id = ANY(%s::uuid[]) AND field_name = ANY(%s) AND confidence > 0
        """,
        (source_id, company_ids, _URL_FIELDS),
    )
    observations = cur.fetchall()
    cur.execute(
        """
        SELECT company_id::text, jsonb_array_elements_text(source_payload -> 'proven_urls')
        FROM public.company_source_records
        WHERE source_id = %s AND company_id = ANY(%s::uuid[])
          AND jsonb_typeof(source_payload -> 'proven_urls') = 'array'
        """,
        (source_id, company_ids),
    )
    demote = unproven(observations, set(cur.fetchall()), written)
    if demote:
        cur.execute("UPDATE public.company_field_observations SET confidence = 0, updated_at = now()"
                    " WHERE id = ANY(%s::uuid[])", (demote,))
    return len(demote)


# --- verify ---------------------------------------------------------------------------------


def run_texts() -> int:
    """Keep each matched item's Wikipedia article and Wikidata description in the company corpus
    (#61): texts to train the classifiers on. A text read in the last `CORPUS_MAX_AGE_DAYS` is not
    read again. Returns how many texts were stored; 0 without a corpus."""
    if not corpus.enabled():
        log.info("No company corpus configured (CORPUS_S3_ENDPOINT); Wikipedia texts not stored.")
        return 0
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            cur.execute(
                """
                SELECT r.company_id::text, r.external_url, r.source_payload ->> 'description',
                       r.source_payload ->> 'wikipedia_url',
                       EXISTS (SELECT 1 FROM public.company_documents d
                               WHERE d.company_id = r.company_id AND d.source_id = r.source_id
                                 AND d.kind = 'wikidata_description' AND d.url = r.external_url
                                 AND d.read_at > now() - %s * interval '1 day'),
                       EXISTS (SELECT 1 FROM public.company_documents d
                               WHERE d.company_id = r.company_id AND d.source_id = r.source_id
                                 AND d.kind = 'wikipedia' AND d.url = r.source_payload ->> 'wikipedia_url'
                                 AND d.read_at > now() - %s * interval '1 day')
                FROM public.company_source_records r
                WHERE r.source_id = %s AND r.is_active
                """,
                (corpus.max_age().days, corpus.max_age().days, source_id),
            )
            records = cur.fetchall()
        stored = 0
        for company_id, item_url, description, article, description_fresh, article_fresh in records:
            with connection, connection.cursor() as cur:
                if (description and not description_fresh
                        and corpus.put("wikidata_description", item_url, {"url": item_url, "text": description})):
                    corpus.record_document(cur, company_id=company_id, source_id=source_id,
                                           kind="wikidata_description", url=item_url,
                                           namespace="wikidata_description", language="de")
                    stored += 1
                if article and not article_fresh:
                    try:
                        text = wikidata.wikipedia_text(article)
                    except Exception as exc:  # noqa: BLE001  one missing article must not stop the rest
                        log.warning("Wikipedia article not readable for %s: %s", company_id, type(exc).__name__)
                        continue
                    if text and corpus.put("wikipedia", article, {"url": article, "text": text}):
                        corpus.record_document(cur, company_id=company_id, source_id=source_id,
                                               kind="wikipedia", url=article,
                                               namespace="wikipedia", language="de")
                        stored += 1
        log.info("%d texts stored in the company corpus (Wikipedia articles and Wikidata descriptions).", stored)
        return stored
    finally:
        connection.close()


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
                WHERE r.source_id = %s
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
    """Every verified URL as an observation, filed like discovery's; earlier ones no record of this
    company proves any more drop to 0. Only a URL filed as `website` counts as proven."""
    written = set()
    for answer in answers:
        written.add(str(insert_text_observation(
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
        )))
    cur.execute(
        "UPDATE public.company_source_records SET source_payload = source_payload || jsonb_build_object("
        "'verified_at', now(), 'proven_urls', %s), last_synced_at = now() WHERE id = %s",
        (Json(sorted({a["url"] for a in answers if field_name_for(a) == "website"})), record_id),
    )
    demote_unproven(cur, source_id=source_id, company_ids=[company_id], written=frozenset(written))


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
        match = pipeline_task("match_items", run_match, returns=True)
        texts = pipeline_task("store_texts", run_texts)
        verify = pipeline_task("verify_websites", run_verify, returns=True)
        took_any = ShortCircuitOperator(
            task_id="took_any",
            python_callable=lambda ti: bool(ti.xcom_pull(task_ids="match_items")
                                            or ti.xcom_pull(task_ids="verify_websites")),
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
        match >> texts >> verify >> took_any >> [coordinate, more_left]
        more_left >> next_batch
    return dag


dag = _make_dag()
