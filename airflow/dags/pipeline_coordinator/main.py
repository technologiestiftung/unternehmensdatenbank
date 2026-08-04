from __future__ import annotations

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timezone
from resolver import (
    compute_quality_scores,
    report_field_coverage,
    resolve_ceo_contact,
    resolve_cluster,
    resolve_employee_count,
    resolve_revenue,
    resolve_tech_field,
    resolve_website,
)
from source_enrichment_common import get_connection

log = logging.getLogger(__name__)

DAG_ID = "pipeline_coordinator"


def run_pipeline_coordinator() -> None:
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Snapshot before
                before = report_field_coverage(cur)

        log.info(
            "Pipeline status BEFORE resolution — %d companies | "
            "website=%s cluster=%s tech_field=%s ceo=%s revenue=%s employees=%s | "
            "avg_score=%.3f",
            before["total_companies"],
            before["fields"]["website"]["pct"],
            before["fields"]["cluster"]["pct"],
            before["fields"]["tech_field"]["pct"],
            before["fields"]["ceo_contact"]["pct"],
            before["fields"]["revenue"]["pct"],
            before["fields"]["employee_count"]["pct"],
            before["avg_quality_score"],
        )
        log.info(
            "Tiers BEFORE — complete=%d partial=%d minimal=%d",
            before["tiers"]["complete (≥0.90)"],
            before["tiers"]["partial  (≥0.50)"],
            before["tiers"]["minimal  (<0.50)"],
        )

        # Resolve each target field
        with conn:
            with conn.cursor() as cur:
                n_website   = resolve_website(cur)
                n_cluster   = resolve_cluster(cur)
                n_tech      = resolve_tech_field(cur)
                n_revenue   = resolve_revenue(cur)
                n_employees = resolve_employee_count(cur)
                n_ceo       = resolve_ceo_contact(cur)

        log.info(
            "Resolution complete — website=%d cluster=%d tech_field=%d "
            "revenue=%d employee_count=%d ceo_contact=%d canonical records upserted",
            n_website, n_cluster, n_tech, n_revenue, n_employees, n_ceo,
        )

        # Recompute quality scores
        with conn:
            with conn.cursor() as cur:
                n_scored = compute_quality_scores(cur)

        log.info("Quality scores recomputed for %d companies.", n_scored)

        # Snapshot after
        with conn:
            with conn.cursor() as cur:
                after = report_field_coverage(cur)

        log.info(
            "Pipeline status AFTER resolution — %d companies | "
            "website=%s cluster=%s tech_field=%s ceo=%s revenue=%s employees=%s | "
            "avg_score=%.3f",
            after["total_companies"],
            after["fields"]["website"]["pct"],
            after["fields"]["cluster"]["pct"],
            after["fields"]["tech_field"]["pct"],
            after["fields"]["ceo_contact"]["pct"],
            after["fields"]["revenue"]["pct"],
            after["fields"]["employee_count"]["pct"],
            after["avg_quality_score"],
        )
        log.info(
            "Tiers AFTER — complete=%d partial=%d minimal=%d",
            after["tiers"]["complete (≥0.90)"],
            after["tiers"]["partial  (≥0.50)"],
            after["tiers"]["minimal  (<0.50)"],
        )

        # Identify which layer to run next
        needs_website = (
            before["total_companies"]
            - before["fields"]["website"]["filled"]
        )
        needs_llm = (
            before["total_companies"]
            - before["tiers"]["complete (≥0.90)"]
            - before["tiers"]["partial  (≥0.50)"]
        )
        log.info(
            "Next steps — ~%d companies still need website discovery | "
            "~%d companies below partial threshold (priority LLM targets)",
            needs_website,
            needs_llm,
        )
    finally:
        conn.close()


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description=(
            "Resolve canonical observations → companies table, "
            "compute quality scores, report pipeline status"
        ),
        schedule=None,
        start_date=datetime(2026, 8, 4, tzinfo=timezone.utc),
        catchup=False,
        tags=["pipeline", "coordinator", "quality"],
    ) as dag:
        PythonOperator(
            task_id="resolve_and_score",
            python_callable=run_pipeline_coordinator,
        )
    return dag


dag = _make_dag()
