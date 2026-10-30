from __future__ import annotations

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timezone
from resolver import (
    report_field_coverage,
    resolve_activity_keywords,
    resolve_ceo_contact,
    resolve_deeptech_and_ai,
    resolve_employee_count,
    resolve_purpose_field,
    resolve_register_announcement,
    resolve_register_fields,
    resolve_revenue,
    resolve_technology_field,
    resolve_website,
)
from source_enrichment_common import get_connection

log = logging.getLogger(__name__)

DAG_ID = "pipeline_coordinator"


def _log_coverage(when: str, coverage: dict) -> None:
    fields = " ".join(f"{name}={field['pct']}" for name, field in coverage["fields"].items())
    log.info("Coverage %s resolution — %d companies | %s", when, coverage["total_companies"], fields)


def run_pipeline_coordinator() -> None:
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            _log_coverage("BEFORE", report_field_coverage(cur))

        with conn, conn.cursor() as cur:
            changed = {
                "website": resolve_website(cur),
                "revenue": resolve_revenue(cur),
                "employee_count": resolve_employee_count(cur),
                "ceo_contact": resolve_ceo_contact(cur),
                # The two industry columns. Neither has an alias list, and the two `_withheld`
                # field names are deliberately resolved by nothing at all — see resolver.py.
                "technology_field": resolve_technology_field(cur),
                "purpose_field": resolve_purpose_field(cur),
                "activity_keywords": resolve_activity_keywords(cur),
                "register_announcement": resolve_register_announcement(cur),
                "register fields": resolve_register_fields(cur),
                "deeptech/is_ai": resolve_deeptech_and_ai(cur),
            }
        log.info("Resolution complete, canonical records changed — %s",
                 " ".join(f"{name}={n}" for name, n in changed.items()))

        # What changed in this run, for the company page's history (#54). Append-only; one
        # comparison of what is shown now with the latest history row per field.
        with conn, conn.cursor() as cur:
            cur.execute("SELECT public.record_company_history()")
            log.info("History: %d changes recorded.", cur.fetchone()[0])

        # The hand-check sample (V10): topped up, never redrawn, so a stratum fills once data exists.
        with conn, conn.cursor() as cur:
            cur.execute("SELECT public.draw_review_sample()")
            added = cur.fetchone()[0]
        if added:
            log.info("Review sample: %d companies added.", added)

        # The app reads a stored copy of every profile (`company_profile_mv`); bring it up to date
        # with what was just resolved.
        with conn, conn.cursor() as cur:
            cur.execute("SELECT public.refresh_company_profile()")
        log.info("Company profiles refreshed for the app.")

        with conn, conn.cursor() as cur:
            _log_coverage("AFTER", report_field_coverage(cur))
    finally:
        conn.close()


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Decide which observation every field shows, then refresh the stored profile",
        schedule=None,
        start_date=datetime(2026, 8, 4, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        tags=["pipeline", "coordinator"],
    ) as dag:
        pipeline_task("resolve_and_score", run_pipeline_coordinator)
    return dag


dag = _make_dag()
