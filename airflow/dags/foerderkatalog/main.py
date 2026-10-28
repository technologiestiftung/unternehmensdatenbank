"""Grants from the Förderkatalog des Bundes into `public.company_fundings`, once a month.

The scraping is `scraper.py`, vendored unchanged from udb-foerderkatalog-scraper: one search for
Bundesland Berlin, the full CSV export, and a per-year fallback if the export comes back short.
This module only turns its rows into table rows and upserts them by Förderkennzeichen (FKZ).

Fundings are NOT observations and do not go through `pipeline_coordinator`: a company has many
grants, not one winning value. After each load `refresh_company_funding_links()` links the
grants to companies by name and city (the Förderkatalog carries no register number to link by).
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone

from source_enrichment_common import get_connection

log = logging.getLogger(__name__)

DAG_ID = "foerderkatalog"
ENABLED_FLAG = "ENABLE_FOERDERKATALOG"

_UPSERT_SQL = """
    INSERT INTO public.company_fundings
      (fkz, recipient_name, city, amount_eur, period_from, period_to, topic, ministry, funding_type)
    VALUES %s
    ON CONFLICT (fkz) DO UPDATE SET
      recipient_name = EXCLUDED.recipient_name,
      city = EXCLUDED.city,
      amount_eur = EXCLUDED.amount_eur,
      period_from = EXCLUDED.period_from,
      period_to = EXCLUDED.period_to,
      topic = EXCLUDED.topic,
      ministry = EXCLUDED.ministry,
      funding_type = EXCLUDED.funding_type,
      last_seen_at = NOW()
"""


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


def _german_date(value: str) -> date | None:
    """'TT.MM.JJJJ' -> date; empty or malformed -> None."""
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


def to_funding_rows(grants: list[dict[str, str]]) -> list[tuple]:
    """`scraper.build_grant_rows` output -> rows for `_UPSERT_SQL`, one per FKZ.

    A repeated FKZ keeps its last row: Postgres refuses to update the same key twice in one
    statement. The export of 2026-10-01 had none, so this only guards the per-year fallback.
    """
    by_fkz = {
        grant["FKZ"]: (
            grant["FKZ"],
            grant["Förderempfänger"],
            grant["Ort"] or None,
            float(grant["Fördersumme_EUR"]) if grant["Fördersumme_EUR"] else None,
            _german_date(grant["Laufzeit von"]),
            _german_date(grant["Laufzeit bis"]),
            grant["Thema des geförderten Vorhabens"] or None,
            grant["Ressort"] or None,
            grant["Förderart"] or None,
        )
        for grant in grants
    }
    return list(by_fkz.values())


def run_foerderkatalog() -> None:
    _require_enabled()
    # Imported here, not at module top: scraper.py reads config.toml at import and warns when it is
    # missing, which it always is in Airflow, so a top-level import would warn on every DAG parse.
    from psycopg2.extras import execute_values
    from foerderkatalog.scraper import FoekatClient, build_grant_rows, scrape

    with FoekatClient() as client:
        result = scrape(client)
    if not result.rows:
        raise RuntimeError("Förderkatalog returned no rows; refusing to treat that as 'no grants'.")

    rows = to_funding_rows(build_grant_rows(result.rows))
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, rows, page_size=1000)
            # Grants are linked to companies once here, not on every read (`company_funding_links`).
            cur.execute("SELECT public.refresh_company_funding_links()")
            linked = cur.fetchone()[0]
            # The grant counts in the app's company list come from the stored profile (`company_profile_mv`).
            cur.execute("SELECT public.refresh_company_profile()")
    finally:
        connection.close()
    log.info("Upserted %d grants from %d export rows; %d linked to a company.", len(rows), len(result.rows), linked)


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Monthly grants for Berlin recipients from the Förderkatalog des Bundes",
        schedule="@monthly",
        start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        tags=["sources", "fundings"],
    ) as dag:
        pipeline_task("load_fundings", run_foerderkatalog)
    return dag


dag = _make_dag()
