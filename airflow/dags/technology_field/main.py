"""Technologiefeld: which of Berlin's 77 fields a company works in, from its own website (#62).

The rule lives in `enrichment.branch_classifier`; this file runs it. Each run does two things, then
lets `pipeline_coordinator` publish the result:

1. **read.** One batch of companies with a published website that this source has not read yet:
   the homepage and the services and about pages it links to (`content_pages`). The keywords that
   fired, and on which of the two texts, go into the company's source record. A site whose homepage
   did not load is marked `failed` and tried again, at most `MAX_READ_ATTEMPTS` times.
2. **classify.** No downloads. A keyword's weight depends on how many companies use it, so every
   run weighs over all companies read so far and classifies each of them again. A company that no
   longer gets a value keeps its row at confidence 0, below the coordinator's bar: never deleted.
   Nothing is written until `MIN_COMPANIES` are read, because a weight measured over 50 companies
   lets one rare word decide.

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
from typing import Any, Mapping

from enrichment import branch_classifier as bc
from enrichment.branch_observations import SOURCE_KEY, planned_writes
from source_enrichment_common import (
    companies_with_website_to_read,
    ensure_source,
    failed_read_payload,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "technology_field"
BATCH_SIZE = int(os.getenv("TECHNOLOGY_FIELD_BATCH_SIZE", "50"))
MIN_COMPANIES = int(os.getenv("TECHNOLOGY_FIELD_MIN_COMPANIES", "500"))
FIELDS = ("technology_field", "technology_field_withheld")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Branch Classifier (Berlin 77-field taxonomy)",
                         source_type="classifier", metadata={"dag_id": DAG_ID})


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
    # Same names and order as `branch_textcat.classifier_text`, so a model reads what the keywords read.
    texts = {
        "services": content_for(website, homepage).text if homepage else "",
        "homepage": visible_text(homepage),
    }
    return {
        "keywords": keywords_by_source(texts, taxonomy),
        "had_text": any(t.strip() for t in texts.values()),
        "homepage_loaded": bool(homepage),
        "taxonomy_version": taxonomy.version,
    }


def run_read() -> None:
    taxonomy = bc.load_taxonomy()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = companies_with_website_to_read(cur, source_id=source_id, limit=BATCH_SIZE)

        with_keywords = failed = 0
        for company_id, company_name, website, attempts in companies:
            try:
                payload = read_site(website, taxonomy)
            except Exception as exc:  # noqa: BLE001  one unreachable site must not stop the batch
                log.warning("Website not readable for %s: %s", company_id, type(exc).__name__)
                payload = {"keywords": {}, "had_text": False, "error": type(exc).__name__}
            if not payload.get("homepage_loaded"):
                payload, failed = failed_read_payload(payload, attempts), failed + 1
            with_keywords += bool(payload["keywords"])
            with connection, connection.cursor() as cur:
                upsert_source_record(
                    cur, company_id=company_id, source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{company_id}", external_url=website,
                    source_company_name=company_name, source_payload=payload,
                )

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=with_keywords, records_updated=0, records_failed=failed)
        log.info("Read %d websites: %d with a keyword, %d unreadable.", len(companies), with_keywords, failed)
    finally:
        connection.close()


# --- classify -------------------------------------------------------------------------------


def classify_all(
    reads: Mapping[str, tuple[dict[str, str], bool]], taxonomy: bc.Taxonomy
) -> dict[str, bc.Assignment]:
    """company_id -> its answer, weighed over every company in `reads` ({id: (keywords, had_text)}).

    Keywords no longer in the YAML are dropped here, so deleting one (CLAUDE.md, rule 5) takes effect
    on the next run. shortcut: a keyword ADDED to the YAML only reaches companies read after the
    edit; re-read from the corpus (#61) once it stores the text.
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


def run_classify() -> None:
    taxonomy = bc.load_taxonomy()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            cur.execute(
                """
                SELECT r.company_id::text, r.id, r.source_payload -> 'keywords',
                       COALESCE((r.source_payload ->> 'had_text')::boolean, false),
                       e.website_url IS NOT DISTINCT FROM r.external_url
                FROM public.company_source_records r
                LEFT JOIN public.v_companies_enriched e ON e.id = r.company_id
                WHERE r.source_id = %s
                  AND r.source_payload ? 'keywords'
                  AND r.source_payload ->> 'failed' IS DISTINCT FROM 'true'
                """,
                (source_id,),
            )
            records = cur.fetchall()
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

            # A company whose website changed was read off the old one: its keywords count for nothing.
            # shortcut: it is not read again from the new website; same limit as `website_keywords`.
            answers = classify_all(
                {cid: (keywords if still_published else {}, had_text and still_published)
                 for cid, _, keywords, had_text, still_published in records},
                taxonomy,
            )
            written = dropped = 0
            for company_id, record_id, *_ in records:
                row = {**asdict(answers[company_id]), "technology_field": answers[company_id].branch,
                       "taxonomy_version": taxonomy.version, "scope_companies": len(records)}
                writes = planned_writes(row, None)
                for field_name, value, confidence, metadata in writes:
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=field_name, value_text=value,
                        confidence=confidence, metadata=metadata,
                    )
                for field_name, value in shown.get(company_id, set()) - {(f, v) for f, v, _, _ in writes}:
                    cur.execute(
                        """
                        UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
                        WHERE company_id = %s AND source_id = %s AND field_name = %s AND value_text = %s
                        """,
                        (company_id, source_id, field_name, value),
                    )
                    dropped += 1
                written += len(writes)

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
        from pipeline_task import pipeline_task
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Berlin's Technologiefeld from the keywords on the company's own website",
        schedule=None, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites", "classification"],
    ) as dag:
        read = pipeline_task("read_websites", run_read)
        classify = pipeline_task("classify", run_classify)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        read >> classify >> coordinate
    return dag


dag = _make_dag()
