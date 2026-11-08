"""purpose_field: which of Berlin's 77 fields a company was set up for, from its register purpose (#178).

Column 3 of three (CLAUDE.md, "Industry classification"). The rule lives in `enrichment.purpose_branch`
and the write rules in `enrichment.branch_observations`; this file only runs them. Read
[purpose-classification](../../../docs/purpose-classification.md) first.

Each run reads every company's **published** business purpose (the `business_purpose` field the
coordinator chose, today always from `openregister`), classifies all of them again and lets
`pipeline_coordinator` publish the result. No downloads, no model, no state outside Postgres.

- **Every run weighs over every purpose.** A keyword's weight depends on how many companies use it,
  so a new purpose can change another company's answer. Only usable purposes (over
  `purpose_branch.MIN_PURPOSE_CHARS` characters) count for the weights; shorter ones are still matched,
  so the field the length floor cost is kept as `purpose_field_withheld`, which nothing publishes.
- **Nothing is written until `MIN_COMPANIES` usable purposes exist**, as in `technology_field`: a
  weight over 50 texts lets one rare word decide.
- A company that no longer gets a value keeps its row at confidence 0, below the coordinator's bar:
  never deleted.
- A value is as old as the register text it came from (#49): `source_updated_at` is the purpose's.
- Its own source row (`purpose_classifier`). Sharing `branch_classifier` with `technology_field` would
  overwrite that source's description, and its source records, which hold the website keywords.

Nothing here has been hand-checked; 0.60 is the policy value in `branch_observations`, not a
measurement. 94.5% of keyword-decided purpose fields rested on one keyword on the earlier data set.
"""
from __future__ import annotations

import logging
import os
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Mapping

from enrichment import branch_classifier as bc
from enrichment import purpose_branch as pb
from enrichment.branch_observations import planned_writes
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    schedule_label,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "purpose_field"
SOURCE_KEY = "purpose_classifier"
#: Daily: OpenRegister adds purposes every day while its backlog walk (#143) runs.
SCHEDULE = "@daily"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Das Feld aus dem Unternehmensgegenstand im Handelsregister (Berlins 77 Felder), über "
        "Stichworte. Sagt, wofür die Firma gegründet wurde, nicht, woran sie heute arbeitet."
    ),
    schedule=schedule_label(SCHEDULE),
)
MIN_COMPANIES = int(os.getenv("PURPOSE_FIELD_MIN_COMPANIES", "500"))
FIELDS = ("purpose_field", "purpose_field_withheld")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Purpose Classifier (Berlin 77-field taxonomy)",
                         source_type="classifier", metadata={"dag_id": DAG_ID}, details=DETAILS)


def classify_purposes(purposes: Mapping[str, str | None], taxonomy: bc.Taxonomy) -> dict[str, pb.PurposeAssignment]:
    """company_id -> its answer, weighed over the usable purposes in `purposes`.

    Stubs are matched but left out of the frequencies, so the length floor does not shift the scores
    of the purposes it ignores (docs/purpose-classification.md).
    """
    keywords = {cid: pb.purpose_keywords(text, taxonomy) for cid, text in purposes.items()}
    usable = {cid for cid, text in purposes.items() if pb.is_usable(text)}
    df = bc.document_frequency(keywords[cid] for cid in usable)
    return {
        cid: pb.classify_purpose(taxonomy=taxonomy, df=df, n_companies=len(usable),
                                 keywords_by_source=found, had_purpose=cid in usable)
        for cid, found in keywords.items()
    }


def published_purposes(cur: Any) -> list[tuple[str, str, str, datetime | None]]:
    """(company id, name, purpose, read at) for every company with a published business purpose."""
    cur.execute(
        """
        SELECT c.id::text, e.company_name, o.value_text, o.source_updated_at
        FROM public.company_canonical_field_sources s
        JOIN public.company_field_observations o ON o.id = s.company_field_observation_id
        JOIN public.companies c ON c.id = s.company_id AND c.merged_into IS NULL
        JOIN public.v_companies_enriched e ON e.id = c.id
        WHERE s.field_name = 'business_purpose'
        """
    )
    return cur.fetchall()


def row_for(answer: pb.PurposeAssignment, taxonomy: bc.Taxonomy, n_usable: int) -> dict[str, Any]:
    """The shape `branch_observations.planned_writes` reads for column 3."""
    return {**asdict(answer), "purpose_field": answer.branch, "taxonomy_version": taxonomy.version,
            "scope_companies": n_usable}


def run_classify() -> None:
    taxonomy = bc.load_taxonomy()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            records = published_purposes(cur)
            answers = classify_purposes({cid: text for cid, _, text, _ in records}, taxonomy)
            n_usable = sum(a.outcome != pb.NO_PURPOSE for a in answers.values())
            if n_usable < MIN_COMPANIES:
                log.info("Only %d usable purposes; %d needed to weigh keywords. Nothing written.",
                         n_usable, MIN_COMPANIES)
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

            written = dropped = 0
            kept: dict[str, set[tuple[str, str]]] = {}
            for company_id, company_name, _, read_at in records:
                answer = answers[company_id]
                writes = planned_writes(None, row_for(answer, taxonomy, n_usable))
                kept[company_id] = {(f, v) for f, v, _, _ in writes}
                if not writes:
                    continue
                record_id, _ = upsert_source_record(
                    cur, company_id=company_id, source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{company_id}", external_url=None,
                    source_company_name=company_name,
                    source_payload={"status": "fetched", "outcome": answer.outcome, "value": answer.branch,
                                    "withheld": answer.withheld_by_floor, "keywords": sorted(answer.keywords),
                                    "taxonomy_version": taxonomy.version},
                )
                for field_name, value, confidence, metadata in writes:
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=field_name, value_text=value,
                        confidence=confidence, metadata=metadata, source_updated_at=read_at,
                    )
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
        log.info("Classified %d purposes (%d usable): %s. %d values written, %d dropped.", len(records),
                 n_usable, dict(Counter(a.outcome for a in answers.values())), written, dropped)
    finally:
        connection.close()


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Berlin's 77 fields from the register's business purpose (column 3)",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "register", "classification"],
    ) as dag:
        classify = pipeline_task("classify", run_classify)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        classify >> coordinate
    return dag


dag = _make_dag()
