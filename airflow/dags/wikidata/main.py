"""Websites Wikidata lists for Berlin companies, verified like a guessed domain (issue #26).

Three tasks, and the figures inside match:

1. **match**, at most once a day. Two SPARQL queries (`enrichment.wikidata`) and every item is matched
   to a company in SQL, with the one name key (`normalize_company_name`):
   - register digits AND the same name key -> `match_method = 'identifier'`, confidence 1.0. Wikidata
     holds the bare register number without the court, so the number alone proves nothing;
   - else the same name key as exactly one company that is not dissolved -> `'fuzzy'`, confidence
     0.9. Never a dissolved one: its name may be in use again (the rule register_announcements uses).
   Each match is a source record, `external_id` = the Wikidata id.
   Measured on 2026-10-30 on the local Berlin database: 531 of 4,174 items matched, 31 of them by
   register number and name.
2. **verify**, in batches, chaining itself while proposals wait. Every listed website goes through
   `pipeline.verify_proposed`, the same page reading and `confidence.score` as a guessed domain.
   Nothing on the page ties it to the company -> below the bar, never shown. On 2026-10-30, 229 of
   the 531 were proven, 139 of them new to website_discovery; no result was hand-checked.
3. **figures**, inside match: each matched item's number of employees and revenue (in euros), the
   newest of each, as `employee_count` / `revenue` observations (#138, #139). The figure's date is the
   observation's `source_updated_at`, so the app shows which year it is for and an older figure ranks
   below a newer one from another source (`resolver.HALF_LIVES`). Often the group's figure, not the
   registered entity's alone, and not checked: confidence `FIGURE_CONFIDENCE`. One row per company and
   field, updated in place; a company whose item no longer matches, or no longer has the figure, keeps
   the row at confidence 0. A failed figures query is logged and skipped; the websites still match.
4. **store_texts**, between the two (`run_texts`): each matched item's German Wikipedia article and
   Wikidata description go into the company corpus (`enrichment.corpus`, #61), for training.

The page, not the match, proves a website. An item that no longer matches keeps its record,
inactive, and is still verified every 90 days. A website drops to confidence 0 only when no record
of this source for that company still proves it (`proven_urls`): its page stopped naming the
company, or its item now matches another company. Nothing is deleted.
Register officers are loaded per batch as comparison keys, in memory only, as in website_discovery.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json, execute_values

from enrichment import confidence as conf
from enrichment import corpus, proposals, wikidata
from enrichment.register_names import DISSOLVED
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    schedule_label,
    start_sync_run,
    with_code_version,
)

log = logging.getLogger(__name__)

DAG_ID = "wikidata"
SOURCE_KEY = "wikidata"
SCHEDULE = "@daily"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Offizielle Websites, die Wikidata für Berliner Organisationen nennt, über Registernummer "
        "oder eindeutigen Namen zugeordnet und wie eine geratene Domain geprüft. Dazu Mitarbeiterzahl "
        "und Umsatz (in Euro) mit ihrem Stichtag, wo Wikidata sie nennt: oft die Zahlen des Konzerns, "
        "nicht nur der eingetragenen Firma, und nicht geprüft."
    ),
    schedule=schedule_label(SCHEDULE),
    base_url="https://www.wikidata.org",
    terms="Wikidata steht unter CC0.",
    terms_url="https://www.wikidata.org/wiki/Wikidata:Licensing",
)
BATCH_SIZE = int(os.getenv("WIKIDATA_BATCH_SIZE", "100"))
#: Unmeasured. Above the bar (0.5), so the only source shows; below a register or an annual account.
FIGURE_CONFIDENCE = 0.6
WORKERS = int(os.getenv("WIKIDATA_WORKERS", "8"))
RECHECK_AFTER_DAYS = int(os.getenv("WIKIDATA_RECHECK_DAYS", "90"))


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Wikidata (official websites, P856)",
                         source_type="knowledge_base", metadata={"dag_id": DAG_ID, "licence": "CC0"}, details=DETAILS)


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
        try:
            figures = wikidata.fetch_figures()
        except Exception as exc:  # noqa: BLE001  the figures are extra; the websites must still match
            log.warning("Wikidata figures not read (%s); the stored figures stay as they are.", type(exc).__name__)
            figures = None
        with connection, connection.cursor() as cur:
            sync_run_id = start_sync_run(cur, source_id=source_id)
            matched, demoted = match_items(cur, items, source_id=source_id)
            figures_changed = write_figures(cur, figures, source_id=source_id, sync_run_id=sync_run_id)
            cur.execute("UPDATE public.sources SET last_synced_at = now() WHERE id = %s", (source_id,))
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(items), records_created=matched,
                            records_updated=0, records_failed=0)
        log.info("%d Wikidata items with a Berlin headquarters and a website; %d matched a company;"
                 " %d websites demoted; %d employee or revenue figures changed.",
                 len(items), matched, demoted, figures_changed)
        return demoted + figures_changed
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
    left = proposals.moved_from(dict(cur.fetchall()), {qid: str(company_id) for qid, company_id, _, _ in matches})
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
    return len(matches), proposals.demote_unproven(cur, source_id=source_id, company_ids=sorted(left))


def planned_figures(
    current: dict[tuple[str, str], tuple[str, float, str | None]],
    stored: dict[tuple[str, str], tuple[str, float, float, str | None]],
) -> list[tuple[str, tuple[str, str], tuple[str, float, str | None] | None]]:
    """What writing the figures does, per (company, field): ("insert" | "update" | "drop", key, figure).

    Pure, so it can be tested without a database. `current` maps (company, field) to (record id,
    amount, date) for every active match with that figure; `stored` to this source's observation
    (id, amount, confidence, date). "drop" sets a figure nothing supports any more to confidence 0.
    """
    plan = []
    for key, figure in sorted(current.items()):
        old = stored.get(key)
        if old is None:
            plan.append(("insert", key, figure))
        elif (old[1], old[2], old[3]) != (figure[1], FIGURE_CONFIDENCE, figure[2]):
            plan.append(("update", key, figure))
    for key, old in sorted(stored.items()):
        if key not in current and old[2] > 0:
            plan.append(("drop", key, None))
    return plan


def write_figures(cur: Any, figures: dict[str, dict[str, wikidata.Figure]] | None, *, source_id: str,
                  sync_run_id: str) -> int:
    """Each active match's employees and revenue as observations (see the module docstring). Returns how
    many rows changed. `figures` None (the query failed) changes nothing."""
    if figures is None:
        return 0
    cur.execute("SELECT id::text, company_id::text, external_id FROM public.company_source_records"
                " WHERE source_id = %s AND is_active ORDER BY external_id", (source_id,))
    best: dict[tuple[str, str], tuple[str, wikidata.Figure]] = {}
    for record_id, company_id, qid in cur.fetchall():
        for field, figure in figures.get(qid, {}).items():
            old = best.get((company_id, field))
            if wikidata.is_newer(figure, old and old[1]):
                best[(company_id, field)] = (record_id, figure)
    current = {key: (record_id, figure.amount, figure.as_of) for key, (record_id, figure) in best.items()}
    cur.execute(
        """
        SELECT company_id::text, field_name, id::text, value_number::float, COALESCE(confidence, 0),
               to_char(source_updated_at, 'YYYY-MM-DD')
        FROM public.company_field_observations
        WHERE source_id = %s AND field_name = ANY(%s)
        """,
        (source_id, list(wikidata.FIGURE_FIELDS.values())),
    )
    stored = {(c, f): (obs_id, amount, float(confidence), as_of) for c, f, obs_id, amount, confidence, as_of
              in cur.fetchall()}
    plan = planned_figures(current, stored)
    for action, (company_id, field), figure in plan:
        if action == "drop":
            cur.execute("UPDATE public.company_field_observations SET confidence = 0, updated_at = now()"
                        " WHERE id = %s", (stored[(company_id, field)][0],))
            continue
        record_id, amount, as_of = figure
        metadata = Json(with_code_version({"as_of": as_of, "unit": "EUR" if field == "revenue" else None}))
        if action == "insert":
            cur.execute(
                """
                INSERT INTO public.company_field_observations
                  (company_id, company_source_record_id, source_id, source_sync_run_id, field_name,
                   value_number, observed_at, source_updated_at, confidence, metadata, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now(), %s::date, %s, %s, now(), now())
                """,
                (company_id, record_id, source_id, sync_run_id, field, amount, as_of, FIGURE_CONFIDENCE, metadata),
            )
        else:
            cur.execute(
                """
                UPDATE public.company_field_observations
                SET company_source_record_id = %s, source_sync_run_id = %s, value_number = %s,
                    source_updated_at = %s::date, confidence = %s, metadata = %s, updated_at = now()
                WHERE id = %s
                """,
                (record_id, sync_run_id, amount, as_of, FIGURE_CONFIDENCE, metadata,
                 stored[(company_id, field)][0]),
            )
    return len(plan)


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
    """Verify one batch of proposed websites (`enrichment.proposals`). Returns how many records it
    checked: 0 skips the coordinator, a full batch starts the next run."""
    return proposals.verify_batch(
        source_id_of=_source_id, provider=SOURCE_KEY, method=conf.METHOD_WIKIDATA, batch_size=BATCH_SIZE,
        recheck_after_days=RECHECK_AFTER_DAYS, workers=WORKERS, metadata_of=lambda payload: {"qid": payload["qid"]},
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
        schedule=SCHEDULE, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
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
