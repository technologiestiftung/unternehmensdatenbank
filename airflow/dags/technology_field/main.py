"""Technologiefeld: which of Berlin's 77 fields a company works in, from its own website (#62).

The rule lives in `enrichment.branch_classifier`; this file runs it. Each run does two things, then
lets `pipeline_coordinator` publish the result:

1. **read.** One batch of companies with a published website that this source has not read yet:
   the homepage and the services and about pages it links to (`content_pages`). Room left in the
   batch goes to companies read before whose homepage is no longer in the corpus, or was read more
   than `corpus.max_age()` ago (`text_to_read_again`): the text classifier trains on that text, and
   play lost the pages of its first 562 reads when its store changed (2026-11-04). The keywords that
   fired, and on which of the two texts, go into the company's source record. A site whose homepage
   did not load is marked `failed` and tried again, at most `MAX_READ_ATTEMPTS` times.
2. **classify.** No downloads. A keyword's weight depends on how many companies use it, so every
   run weighs over all companies read so far and classifies each of them again. A company that no
   longer gets a value keeps its row at confidence 0, below the coordinator's bar: never deleted.
   Nothing is written until `MIN_COMPANIES` are read, because a weight measured over 50 companies
   lets one rare word decide. Where no keyword fired, a promoted text classifier may fill the gap;
   its prediction is cached in the source record with the model version (`textcat`), so a company
   is predicted again only for a new model or after it was read again. If the store fails during
   the run, the task fails before writing: a text it did not return is not a text that is gone.

**No state outside Postgres**, as in `website_keywords`: "already read" is the source record.

The source record keeps the keywords, not the page text. The text belongs in the company corpus
(#61), where the text classifier (`branch_textcat`) will train on it.

Nothing here has been hand-checked; 0.60 is the policy value in `branch_observations`, not a
measurement.
"""
from __future__ import annotations

import logging
import os
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from psycopg2.extras import Json

from enrichment import branch_classifier as bc, corpus
from enrichment import branch_textcat as textcat
from enrichment.branch_observations import SOURCE_KEY, planned_writes
from source_enrichment_common import (
    SourceDetails,
    companies_with_website_to_read,
    MAX_READ_ATTEMPTS,
    ensure_source,
    failed_read_payload,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    schedule_label,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "technology_field"
#: Daily restarts the chain of batches (below) for new websites and pages grown too old.
SCHEDULE = "@daily"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Das Technologiefeld aus den Stichworten auf der Website (Berlins 77 Felder). Ein Textmodell "
        "füllt nur Lücken, in denen kein Stichwort vorkam."
    ),
    schedule=f"{schedule_label(SCHEDULE)} in Paketen",
)
BATCH_SIZE = int(os.getenv("TECHNOLOGY_FIELD_BATCH_SIZE", "50"))
#: Companies read before whose stored homepage one run checks, oldest read first.
MAX_TEXT_CHECKS = 500
MIN_COMPANIES = int(os.getenv("TECHNOLOGY_FIELD_MIN_COMPANIES", "500"))
FIELDS = ("technology_field", "technology_field_withheld")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Branch Classifier (Berlin 77-field taxonomy)",
                         source_type="classifier", metadata={"dag_id": DAG_ID}, details=DETAILS)


# --- read -----------------------------------------------------------------------------------


def keywords_by_source(texts: Mapping[str, str], taxonomy: bc.Taxonomy) -> dict[str, str]:
    """keyword -> the first text it fired in. `texts` in the order they count: services, homepage."""
    found: dict[str, str] = {}
    for source, text in texts.items():
        for keyword in sorted(bc.matched_keywords(bc.normalize(text), taxonomy)):
            found.setdefault(keyword, source)
    return found


def read_site(website: str, taxonomy: bc.Taxonomy) -> dict[str, Any]:
    """Download the homepage and its services and about pages; return the keywords that fired."""
    from enrichment.content_pages import content_for
    from enrichment.http_client import fetch_page
    from enrichment.url_patterns import visible_text

    homepage = fetch_page(website) or ""
    pages = content_for(website, homepage) if homepage else None
    # Same names and order as `branch_textcat.classifier_text`, so a model reads what the keywords read.
    texts = {"services": pages.text if pages else "", "homepage": visible_text(homepage)}
    return {
        "keywords": keywords_by_source(texts, taxonomy),
        "pages": [link.url for link in pages.links] if pages else [],
        "had_text": any(t.strip() for t in texts.values()),
        "homepage_loaded": bool(homepage),
        "taxonomy_version": taxonomy.version,
    }


def run_read() -> bool:
    """Read one batch. Returns whether it was full, so the DAG starts the next one."""
    taxonomy = bc.load_taxonomy()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = companies_with_website_to_read(cur, source_id=source_id, limit=BATCH_SIZE,
                                                       read_key="keywords")
            again = text_to_read_again(cur, source_id, BATCH_SIZE - len(companies)) if len(companies) < BATCH_SIZE else {}
        companies += [(company_id, name, website, 0) for company_id, (name, website, _) in again.items()]

        with_keywords = failed = 0
        for company_id, company_name, website, attempts in companies:
            try:
                payload = read_site(website, taxonomy)
            except Exception as exc:  # noqa: BLE001  one unreachable site must not stop the batch
                log.warning("Website not readable for %s: %s", company_id, type(exc).__name__)
                payload = {"keywords": {}, "had_text": False, "error": type(exc).__name__}
            if not payload.get("homepage_loaded"):
                failed += 1
                if company_id in again:  # the earlier answer stays; the failed re-read is counted
                    earlier = again[company_id][2]
                    payload = {**earlier, "reread_attempts": int(earlier.get("reread_attempts", 0)) + 1}
                else:
                    payload = failed_read_payload(payload, attempts)
            with_keywords += bool(payload["keywords"])
            with connection, connection.cursor() as cur:
                upsert_source_record(
                    cur, company_id=company_id, source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{company_id}", external_url=website,
                    source_company_name=company_name, source_payload=payload,
                )
                # The pages are in the company corpus (S3); this says they are this company's, for training.
                corpus.record_document(cur, company_id=company_id, source_id=source_id, kind="webpage",
                                       url=website, namespace="page")
                for url in payload.get("pages", []):
                    corpus.record_document(cur, company_id=company_id, source_id=source_id, kind="webpage",
                                           url=url, namespace="content")

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=with_keywords, records_updated=0, records_failed=failed)
        log.info("Read %d websites: %d with a keyword, %d unreadable.", len(companies), with_keywords, failed)
        return len(companies) >= BATCH_SIZE
    finally:
        connection.close()


def needs_text(read_at: datetime | None, now: datetime, stored: Callable[[], bool]) -> bool:
    """Read again: no homepage recorded, recorded longer ago than the corpus keeps it, or gone from the
    store. `stored` is asked last, because it costs a request."""
    return read_at is None or read_at < now - corpus.max_age() or not stored()


def text_to_read_again(cur: Any, source_id: str, limit: int) -> dict[str, tuple[str, str, dict[str, Any]]]:
    """company_id -> (name, website, earlier payload) for up to `limit` companies read before whose
    homepage text is missing or too old, oldest first. Only companies still on the website they were
    read from, and not after `MAX_READ_ATTEMPTS` failed re-reads; at most `MAX_TEXT_CHECKS` checked.
    None while the store is off or down: a re-read then indexes nothing, so it would repeat every run."""
    if not corpus.enabled():
        return {}
    cur.execute(
        """
        SELECT r.company_id::text, e.company_name, r.external_url, r.source_payload, max(d.read_at)
        FROM public.company_source_records r
        JOIN public.v_companies_enriched e ON e.id = r.company_id AND e.website_url = r.external_url
        LEFT JOIN public.company_documents d
          ON d.company_id = r.company_id AND d.source_id = r.source_id AND d.url = r.external_url
         AND d.s3_key LIKE 'page/%%'
        WHERE r.source_id = %s AND r.source_payload ? 'keywords'
          AND r.source_payload ->> 'failed' IS DISTINCT FROM 'true'
          AND COALESCE((r.source_payload ->> 'reread_attempts')::int, 0) < %s
        GROUP BY 1, 2, 3, 4
        ORDER BY max(d.read_at) NULLS FIRST, 1
        LIMIT %s
        """,
        (source_id, MAX_READ_ATTEMPTS, MAX_TEXT_CHECKS),
    )
    now = datetime.now(timezone.utc)
    again: dict[str, tuple[str, str, dict[str, Any]]] = {}
    rows = cur.fetchall()
    if rows:
        corpus.exists("page", rows[0][2])  # one request proves the store answers before NULL read_at is trusted
    for company_id, name, website, payload, read_at in rows:
        if len(again) == limit or not corpus.enabled():
            break
        if needs_text(read_at, now, lambda: corpus.exists("page", website)):
            again[company_id] = (name, website, payload or {})
    return again


# --- classify -------------------------------------------------------------------------------


def classify_all(
    reads: Mapping[str, tuple[dict[str, str], bool]], taxonomy: bc.Taxonomy
) -> dict[str, bc.Assignment]:
    """company_id -> its answer, weighed over every company in `reads` ({id: (keywords, had_text)}).

    Removed keywords are dropped here, so removing one (CLAUDE.md, rule 5) takes effect on the next
    run. shortcut: a keyword ADDED by a migration only reaches companies read after it; re-read from
    the corpus (#61) once it stores the text.
    """
    current = {
        company_id: ({k: s for k, s in keywords.items() if k in taxonomy.keyword_branches}, had_text)
        for company_id, (keywords, had_text) in reads.items()
    }
    df = bc.document_frequency(keywords for keywords, _ in current.values())
    return {
        company_id: bc.classify(taxonomy=taxonomy, df=df, n_companies=len(current),
                                keywords_by_source=keywords, had_text=had_text, has_website=True)
        for company_id, (keywords, had_text) in current.items()
    }


def read_records(cur: Any, source_id: str) -> list[tuple]:
    """Every company read: (id, record id, read at, keywords, had text, website still published, website)."""
    cur.execute(
        """
        SELECT r.company_id::text, r.id, r.last_synced_at, r.source_payload -> 'keywords',
               COALESCE((r.source_payload ->> 'had_text')::boolean, false),
               e.website_url IS NOT DISTINCT FROM r.external_url, r.external_url
        FROM public.company_source_records r
        LEFT JOIN public.v_companies_enriched e ON e.id = r.company_id
        WHERE r.source_id = %s
          AND r.source_payload ? 'keywords'
          AND r.source_payload ->> 'failed' IS DISTINCT FROM 'true'
        """,
        (source_id,),
    )
    return cur.fetchall()


def keyword_answers(records: list[tuple], taxonomy: bc.Taxonomy) -> dict[str, bc.Assignment]:
    """What the keywords say for every company read (`read_records`)."""
    # A company whose website changed was read off the old one: its keywords count for nothing.
    # shortcut: it is not read again from the new website; same limit as `website_keywords`.
    return classify_all(
        {cid: (keywords if still_published else {}, had_text and still_published)
         for cid, _, _, keywords, had_text, still_published, _ in records},
        taxonomy,
    )


def gap_predictions(
    gaps: Mapping[str, tuple[str, Mapping[str, Any] | None]], version: str, model: Any
) -> tuple[dict[str, Mapping[str, Any]], dict[str, dict[str, Any]]]:
    """(prediction per gap company, the new ones to cache) for {company_id: (website, cached)}.

    A cached prediction of `version` is reused; otherwise the model reads the stored text. Without a
    stored text the cached prediction stays, even an older model's, rather than one made on nothing.
    """
    from technology_field.train import stored_text

    predictions: dict[str, Mapping[str, Any]] = {}
    new: dict[str, dict[str, Any]] = {}
    for company_id, (website, cached) in gaps.items():
        if cached and cached.get("model_version") == version:
            predictions[company_id] = cached
            continue
        text = stored_text(website)
        if not text:
            if cached:
                predictions[company_id] = cached
            continue
        label, probability = textcat.predict(model, text)
        predictions[company_id] = new[company_id] = {"model_version": version, "label": label,
                                                     "probability": probability}
    return predictions, new


def run_classify() -> None:
    from technology_field.train import load_current, require_store

    taxonomy = bc.load_taxonomy()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            records = read_records(cur, source_id)
            if len(records) < MIN_COMPANIES:
                log.info("Only %d companies read; %d needed to weigh keywords. Nothing written.",
                         len(records), MIN_COMPANIES)
                finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(records),
                                records_created=0, records_updated=0, records_failed=0)
                return

            cur.execute(
                """
                SELECT company_id::text, field_name, value_text
                FROM public.company_field_observations
                WHERE source_id = %s AND field_name = ANY(%s) AND confidence > 0
                """,
                (source_id, list(FIELDS)),
            )
            shown: dict[str, set[tuple[str, str]]] = {}
            for company_id, field_name, value in cur.fetchall():
                shown.setdefault(company_id, set()).add((field_name, value))

            answers = keyword_answers(records, taxonomy)
            # Where no keyword fired, a promoted text classifier may fill the gap (`branch_textcat`).
            version, model = load_current()
            predictions: dict[str, Mapping[str, Any]] = {}
            if model is not None:
                cur.execute(
                    """
                    SELECT company_id::text, source_payload -> 'textcat'
                    FROM public.company_source_records
                    WHERE source_id = %s AND source_payload ? 'textcat'
                    """,
                    (source_id,),
                )
                cached = dict(cur.fetchall())
                predictions, new = gap_predictions(
                    {cid: (website, cached.get(cid)) for cid, *_, still_published, website in records
                     if answers[cid].outcome == "no_keyword" and still_published},
                    version, model,
                )
                require_store()
                for cid, prediction in predictions.items():
                    answers[cid] = textcat.fill_gap(answers[cid], prediction["label"], prediction["probability"])
                record_ids = {cid: record_id for cid, record_id, *_ in records}
                for cid, prediction in new.items():
                    cur.execute(
                        "UPDATE public.company_source_records SET source_payload = source_payload || %s WHERE id = %s",
                        (Json({"textcat": prediction}), record_ids[cid]),
                    )
            written = dropped = 0
            kept: dict[str, set[tuple[str, str]]] = {}
            for company_id, record_id, read_at, *_ in records:
                row = {**asdict(answers[company_id]), "technology_field": answers[company_id].branch,
                       "taxonomy_version": taxonomy.version, "scope_companies": len(records)}
                if row["decided_by"] == textcat.DECIDED_BY_TEXTCAT:
                    row["model_name"] = f"{textcat.MODEL_NAME}:{predictions[company_id]['model_version']}"
                writes = planned_writes(row, None)
                for field_name, value, confidence, metadata in writes:
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=field_name, value_text=value,
                        confidence=confidence, metadata=metadata,
                        # The value is as old as the page it came from, not this run (#49).
                        source_updated_at=read_at,
                    )
                kept[company_id] = {(f, v) for f, v, _, _ in writes}
                written += len(writes)
            for company_id, values in shown.items():
                for field_name, value in values - kept.get(company_id, set()):
                    cur.execute(
                        """
                        UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
                        WHERE company_id = %s AND source_id = %s AND field_name = %s AND value_text = %s
                        """,
                        (company_id, source_id, field_name, value),
                    )
                    dropped += 1

            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(records),
                            records_created=written, records_updated=dropped, records_failed=0)
        log.info("Classified %d companies: %s. %d values written, %d dropped.", len(records),
                 dict(Counter(a.outcome for a in answers.values())), written, dropped)
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from pipeline_task import pipeline_task
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None

    # Each run starts the next while batches come back full, so the companies to read (new, or whose
    # stored text is missing or too old) are worked through without anyone re-triggering; the daily
    # schedule restarts the chain. max_active_runs=1 queues a run behind a running one.
    with DAG(
        dag_id=DAG_ID,
        description="Berlin's Technologiefeld from the keywords on the company's own website",
        schedule=SCHEDULE, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites", "classification"],
    ) as dag:
        read = pipeline_task("read_websites", run_read, returns=True)
        classify = pipeline_task("classify", run_classify)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        more_left = ShortCircuitOperator(
            task_id="more_left", ignore_downstream_trigger_rules=False,
            python_callable=lambda ti: bool(ti.xcom_pull(task_ids="read_websites")),
        )
        next_batch = TriggerDagRunOperator(task_id="next_batch", trigger_dag_id=DAG_ID, wait_for_completion=False)
        read >> classify >> [coordinate, more_left]
        more_left >> next_batch
    return dag


dag = _make_dag()
