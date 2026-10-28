"""Grants from Berlin's Zuwendungsdatenbank into `public.company_fundings`, once a month.

The Senatsverwaltung für Finanzen publishes every grant of the Land Berlin since 2021 as one JSON
file (55,454 rows on 2026-10-28): recipient, giver, kind, year, address, policy area, purpose,
amount and `empfaengerid`, the recipient's register entry (`hrb_084522`, `vr_027417`, ...).

Only grants to a commercial-register entry (HRA/HRB) are loaded: those are the companies this
database holds, and they are linked by register number, exactly (`refresh_company_funding_links`). The rest are associations,
public bodies and recipients without a register entry, which we cannot link and do not need.

Like the Förderkatalog, grants are NOT observations: a company has many grants, not one winning
value, so they do not go through `pipeline_coordinator`.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timezone
from typing import Any

from source_enrichment_common import get_connection

log = logging.getLogger(__name__)

DAG_ID = "berlin_zuwendungen"
ENABLED_FLAG = "ENABLE_BERLIN_ZUWENDUNGEN"
SOURCE_KEY = "berlin_zuwendungen"
URL = os.getenv(
    "BERLIN_ZUWENDUNGEN_URL",
    "https://www.berlin.de/sen/finanzen/service/zuwendungsdatenbank/index.php/index/all.json?q=",
)
#: The file had 55,454 rows; far fewer means a broken download, not fewer grants.
MIN_ROWS = 10_000

_REGISTER = re.compile(r"^(hr[ab])_0*(\d+)$", re.IGNORECASE)
_POSTCODE_CITY = re.compile(r"\b\d{5}\s+(.+)$")

_UPSERT_SQL = """
    INSERT INTO public.company_fundings
      (fkz, recipient_name, city, amount_eur, period_from, period_to, topic, ministry, funding_type,
       source, recipient_register, policy_area)
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
      source = EXCLUDED.source,
      recipient_register = EXCLUDED.recipient_register,
      policy_area = EXCLUDED.policy_area,
      last_seen_at = NOW()
"""


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


def register_entry(recipient_id: str | None) -> str | None:
    """'hrb_084522' -> 'HRB 84522', as in `company_identifiers` after the court; anything else -> None."""
    match = _REGISTER.match((recipient_id or "").strip())
    return f"{match[1].upper()} {int(match[2])}" if match else None


def _city(address: str | None) -> str | None:
    match = _POSTCODE_CITY.search(address or "")
    return match[1].strip() if match else None


def _amount(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_funding_rows(grants: list[dict[str, Any]]) -> list[tuple]:
    """The JSON's `index` rows -> rows for `_UPSERT_SQL`, commercial-register recipients only."""
    rows = []
    for grant in grants:
        register = register_entry(grant.get("empfaengerid"))
        if register is None:
            continue
        year = int(grant["jahr"]) if str(grant.get("jahr", "")).isdigit() else None
        rows.append((
            f"berlin-{grant['id']}",
            grant.get("name") or "",
            _city(grant.get("anschrift")),
            _amount(grant.get("betrag")),
            date(year, 1, 1) if year else None,
            date(year, 12, 31) if year else None,
            grant.get("zweck") or None,
            grant.get("geber") or None,
            grant.get("art") or None,
            SOURCE_KEY,
            register,
            grant.get("politikbereich") or None,
        ))
    return rows


def run_berlin_zuwendungen() -> None:
    _require_enabled()
    import requests
    from psycopg2.extras import execute_values

    response = requests.get(URL, timeout=300)
    response.raise_for_status()
    grants = response.json().get("index") or []
    if len(grants) < MIN_ROWS:
        raise RuntimeError(f"Zuwendungsdatenbank returned {len(grants)} rows; refusing to load that.")

    rows = to_funding_rows(grants)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, rows, page_size=1000)
            cur.execute("SELECT public.refresh_company_funding_links()")
            linked = cur.fetchone()[0]
            cur.execute("SELECT public.refresh_company_profile()")
    finally:
        connection.close()
    log.info("Upserted %d register-linked grants of %d; %d grants of all sources linked.", len(rows), len(grants), linked)


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Monthly grants of the Land Berlin from the Zuwendungsdatenbank",
        schedule="@monthly",
        start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        tags=["sources", "fundings"],
    ) as dag:
        pipeline_task("load_fundings", run_berlin_zuwendungen)
    return dag


dag = _make_dag()
