"""Grants from Berlin's Zuwendungsdatenbank into `public.company_fundings`, once a month.

The Senatsverwaltung für Finanzen publishes every grant of the Land Berlin since 2021 as one JSON
file (55,454 rows on 2026-10-28): recipient, giver, kind, year, address, policy area, purpose,
amount and `empfaengerid`, the recipient's register entry (`hrb_084522`, `vr_027417`, ...).

Only grants to a commercial-register entry (HRA/HRB) are loaded: those are the companies this
database holds, and they are linked by register number, exactly (`refresh_company_funding_links`). The rest are associations,
public bodies and recipients without a register entry, which we cannot link and do not need.

Like the Förderkatalog, grants are NOT observations: a company has many grants, not one winning
value, so they do not go through `pipeline_coordinator`.

**Companies we lack (#103).** A grant names a register number we do not have when the company was
registered after the OffeneRegister snapshot (2017–2019): 1,796 numbers on 2026-11-04, 1,665 of the
1,763 HRB numbers above HRB 205,000, where the snapshot ends. `create_companies` hands each to
`enrichment.register_match.write_entry`, the path `register_announcements` and `gleif` use: found by
register number, else by a unique live name in the same city (a change of legal form), else created.
Only a number above the snapshot's own range is created (`snapshot_line`: the 99.9th percentile of
OffeneRegister's numbers of that register type); one below it that we do not know is more likely a
typo than a new firm, so it is left alone. A created company gets no register status: a grant says the
firm existed when it was paid, not that it still does. The company gets the newest grant's name, legal form (read
off the name) and city, dated by that grant's year, so any register source read later wins by age.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timezone
from typing import Any

from enrichment.register_match import RegisterEntry, write_entry
from enrichment.register_names import legal_form
from source_enrichment_common import ensure_source, finish_sync_run, get_connection, start_sync_run

log = logging.getLogger(__name__)

DAG_ID = "berlin_zuwendungen"
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


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Zuwendungsdatenbank des Landes Berlin", source_type="fundings",
                         metadata={"dag_id": DAG_ID})


def snapshot_line(cur: Any) -> dict[str, int]:
    """Per register type, the number OffeneRegister's Berlin range ends at (99.9th percentile)."""
    cur.execute(
        r"""
        SELECT substring(i.value from '(HR[AB]) \d'),
               percentile_disc(0.999) WITHIN GROUP (ORDER BY substring(i.value from 'HR[AB] (\d+)')::int)
        -- The numbers OffeneRegister itself wrote: one a later source adds to such a company (a new
        -- number after a change of legal form) must not move the line.
        FROM public.company_identifiers i
        JOIN public.sources s ON s.id = i.source_id AND s.key = 'offeneregister'
        WHERE i.scheme = 'register' AND i.value ~ '^Berlin \(Charlottenburg\) HR[AB] \d+'
        GROUP BY 1
        """
    )
    return dict(cur.fetchall())


def is_new_firm(register: str, line: dict[str, int]) -> bool:
    """'HRB 239529' above the snapshot's HRB range. False without a line: nothing to compare with."""
    kind, _, number = register.partition(" ")
    return kind in line and number.isdigit() and int(number) > line[kind]


def entry_for(register: str, name: str, city: str | None, year: date | None, grants: int,
              line: dict[str, int]) -> RegisterEntry:
    """The recipient of a grant as the register entry `write_entry` writes."""
    fields = {"name": name, "legal_form": legal_form(name), "city": city}
    updated = (year or date.today()).isoformat()
    return RegisterEntry(
        external_id=f"recipient:{register}", external_url=URL.split("/index.php")[0], name=name,
        registers=[f"Berlin (Charlottenburg) {register}", f"Berlin (Charlottenburg) {register} B"],
        city=city, active=is_new_firm(register, line),
        fields={field: value for field, value in fields.items() if value}, updated=updated,
        payload={"register": register, "grants": grants, "newest_grant_year": updated[:4]},
    )


def run_create_companies() -> int:
    """Link or create the company of every grant recipient whose register number we do not have.
    Returns how many companies it linked or created, so the coordinator runs."""
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            line = snapshot_line(cur)
            cur.execute(
                """
                SELECT DISTINCT ON (f.recipient_register) f.recipient_register, f.recipient_name, f.city,
                       f.period_from, count(*) OVER (PARTITION BY f.recipient_register)
                FROM public.company_fundings f
                WHERE f.source = %s AND f.recipient_register IS NOT NULL AND f.recipient_name <> ''
                  AND NOT EXISTS (SELECT 1 FROM public.company_funding_links l WHERE l.fkz = f.fkz)
                ORDER BY f.recipient_register, f.period_from DESC NULLS LAST, f.last_seen_at DESC
                """,
                (SOURCE_KEY,),
            )
            recipients = cur.fetchall()
            sync_run_id = start_sync_run(cur, source_id=source_id)
        methods: dict[str, int] = {}
        for register, name, city, year, grants in recipients:
            with connection, connection.cursor() as cur:
                method = write_entry(cur, entry_for(register, name, city, year, grants, line),
                                     source_id=source_id, sync_run_id=sync_run_id, created_by=SOURCE_KEY)
            methods[method] = methods.get(method, 0) + 1
        with connection, connection.cursor() as cur:
            cur.execute("SELECT public.refresh_company_funding_links()")
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(recipients),
                            records_created=methods.get("created", 0),
                            records_updated=methods.get("identifier", 0) + methods.get("fuzzy", 0),
                            records_failed=0)
        log.info("%d recipient register numbers we did not have (snapshot ends at %s): %s.",
                 len(recipients), line, methods)
        return len(recipients) - methods.get("skipped", 0)
    finally:
        connection.close()


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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
        load = pipeline_task("load_fundings", run_berlin_zuwendungen)
        create = pipeline_task("create_companies", run_create_companies, returns=True)
        changed = ShortCircuitOperator(
            task_id="changed_any", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="create_companies")),
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        load >> create >> changed >> coordinate
    return dag


dag = _make_dag()
