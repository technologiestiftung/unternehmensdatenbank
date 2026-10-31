"""Handelsregister lookups: a company's current name, seat and status, read from handelsregister.de (#66).

**Started by a person, never by a schedule.** The portal's Nutzungsordnung (read 2026-10-31 on its
"Informationen" page) allows "einzelne Abrufe" for information and says: "Systematische Abrufe, um
parallele Voll- oder Teilregister aufzubauen, auszubauen oder zu aktualisieren, sind unzulässig"
(Ziffer 3), and no more than 60 searches or company views an hour (Ziffer 5). The developer decided
on 2026-10-31, knowing Ziffer 3, to build this anyway, triggered by hand only. Do not give the DAG a
schedule or let it trigger itself without a new decision.

**What a run does.** At most `MAX_LOOKUPS` (50) companies, one search each, by register number at
Berlin's court, deleted entries included. Before every search it waits `PAUSE_S` (72 s), so even two
runs back to back stay at 50 an hour; a run takes about an hour. Which companies: those with a
published website (about 6,200), never looked up first, then the longest ago. Three failed lookups in
a row end the run: the portal may be blocking us, and asking again would make it worse.

**What it writes.** One source record per company (`hr:<register number>`): name, seat, status and the
former names the result list shows. `company_source_records` is not exposed through Hasura. From it,
three register fields as observations, dated today (#49), so they beat the 2017–2019 OffeneRegister
values by age: `name`, `city` (the seat) and `register_status`: `aktuell` is `active`, `gelöscht` is
`dissolved`. "Geschlossenes Registerblatt" (seen 2026-10-31) writes no status: a sheet is closed when
a company is deleted, but also when it moves to another court and gets a new number there. The list
cannot show a liquidation. A company not found writes nothing but its record:
absent from a search is not deleted.

Only the result list is read, never a document (AD, CD, ...): those cost a retrieval each and name
officers. The data may not be republished as "Handelsregister" (§ 8 Abs. 2 HGB, per the portal's
legal notes), so the source is named after the portal.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from enrichment.register_names import ACTIVE, DISSOLVED
from source_enrichment_common import (
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "handelsregister_lookup"
SOURCE_KEY = "handelsregister_lookup"
URL = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"
#: Says who we are. Never a browser's user agent.
USER_AGENT = "unternehmensdatenbank (Technologiestiftung Berlin; single register lookups, started by hand)"
MAX_LOOKUPS = 50
#: Seconds before every search: 50 an hour, below the portal's 60.
PAUSE_S = 3600 / MAX_LOOKUPS
TIMEOUT_S = 60
MAX_FAILURES_IN_A_ROW = 3
#: Berlin (Charlottenburg) in the portal's court list.
BERLIN_COURT_CODE = "F1103"
#: As `company_identifiers` holds it: "Berlin (Charlottenburg) HRB 136057", or with the portal's " B".
_IDENTIFIER = re.compile(r"^Berlin \(Charlottenburg\) (HRA|HRB|GnR|VR|PR|GsR) (\d+)( B)?$")
#: Only words that say the status. "Geschlossenes Registerblatt" does not (see the docstring).
_STATUSES = {"aktuell": ACTIVE, "gelöscht": DISSOLVED}
_NUMBERED = re.compile(r"^\d+\.\)\s*")


@dataclass(frozen=True)
class Entry:
    #: As `company_identifiers` holds it.
    register: str
    name: str
    seat: str | None
    #: The portal's word: "aktuell", "gelöscht", ...
    status: str
    #: Former (name, seat), oldest first, as the list numbers them.
    history: list[tuple[str, str]] = field(default_factory=list)


def search_fields(register: str) -> tuple[str, str] | None:
    """(register type, number) for the search form, or None for a register number of another court."""
    match = _IDENTIFIER.match(register)
    return (match.group(1), match.group(2)) if match else None


def status_value(status: str) -> str | None:
    """Our `register_status` for the portal's word, or None when the list does not say."""
    return _STATUSES.get(status.strip().lower())


def parse_results(html: str) -> list[Entry]:
    """Every entry of a result page. Pure, so it is tested on a page shaped like the portal's.

    An entry is a `tr[data-ri]` of the grid; inside, the court line, then name, seat and status, then
    the numbered history.
    """
    from bs4 import BeautifulSoup

    grid = BeautifulSoup(html, "html.parser").find("table", role="grid")
    if grid is None:
        raise RuntimeError("No result table; the page has changed.")
    entries = []
    for row in grid.find_all("tr", attrs={"data-ri": True}):
        court = row.select_one("td.fontTableNameSize span.fontWeightBold")
        name = row.select_one("td.paddingBottom20Px span.marginLeft20")
        seat = row.select_one("td.sitzSuchErgebnisse")
        status = seat.find_next_sibling("td") if seat else None
        if not (court and name and status):
            continue
        history = [
            (_NUMBERED.sub("", n.get_text(" ", strip=True)), _NUMBERED.sub("", s.get_text(" ", strip=True)))
            for n, s in zip(row.select("td.RegPortErg_HistorieZn"), row.select("td.RegPortErg_SitzStatus"))
        ]
        entries.append(Entry(
            register=" ".join(court.get_text(" ", strip=True).split()).removeprefix("Amtsgericht "),
            name=name.get_text(" ", strip=True), seat=seat.get_text(" ", strip=True) or None,
            status=status.get_text(" ", strip=True), history=history,
        ))
    return entries


def entry_for(register: str, entries: list[Entry]) -> Entry | None:
    """The one entry with exactly this register number; None when there is none or more than one."""
    same = [e for e in entries if e.register.removesuffix(" B") == register.removesuffix(" B")]
    return same[0] if len(same) == 1 else None


def look_up(register: str) -> list[Entry]:
    """One search on the portal: the form page for a session, then the search itself."""
    import requests
    from bs4 import BeautifulSoup

    register_type, number = search_fields(register)  # the caller only passes Berlin numbers
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    page = session.get(URL, timeout=TIMEOUT_S)
    page.raise_for_status()
    view_state = BeautifulSoup(page.text, "html.parser").find("input", attrs={"name": "javax.faces.ViewState"})
    if view_state is None:
        raise RuntimeError("The search form is gone; the page has changed.")
    time.sleep(2)
    result = session.post(URL, timeout=TIMEOUT_S, data={
        "form": "form",
        "javax.faces.ViewState": view_state.get("value") or "",
        "suchTyp": "e",
        "form:Berlin_input": "on",
        "form:schlagwoerter": "",
        "form:schlagwortOptionen": "1",
        "form:auchGeloeschte_input": "on",
        "form:registerArt_input": register_type,
        "form:registerNummer": number,
        "form:registergericht_input": BERLIN_COURT_CODE,
        "form:ergebnisseProSeite_input": "10",
        "form:btnSuche": "",
    })
    result.raise_for_status()
    return parse_results(result.text)


# --- writing --------------------------------------------------------------------------------


def companies_to_look_up(cur: Any, source_id: str, limit: int) -> list[tuple[str, str]]:
    """(company_id, register number): companies with a published website and a Berlin register
    number, never looked up first, then the longest ago. The newest register number per company."""
    cur.execute(
        """
        SELECT w.company_id::text, i.value
        FROM (SELECT DISTINCT company_id FROM public.company_canonical_field_sources
              WHERE field_name = 'website') w
        JOIN public.companies c ON c.id = w.company_id AND c.merged_into IS NULL
        CROSS JOIN LATERAL (
          SELECT value FROM public.company_identifiers
          WHERE company_id = w.company_id AND scheme = 'register' AND value ~ %s
          ORDER BY created_at DESC LIMIT 1
        ) i
        LEFT JOIN public.company_source_records r ON r.company_id = w.company_id AND r.source_id = %s
        ORDER BY r.last_synced_at NULLS FIRST, w.company_id
        LIMIT %s
        """,
        (_IDENTIFIER.pattern, source_id, limit),
    )
    return cur.fetchall()


def planned_fields(entry: Entry) -> dict[str, str]:
    """The register fields an entry states: name, seat as `city`, and the status when the list says it."""
    fields = {"name": entry.name, "city": entry.seat, "register_status": status_value(entry.status)}
    return {name: value for name, value in fields.items() if value}


def write_lookup(cur: Any, *, company_id: str, register: str, entry: Entry | None, source_id: str,
                 sync_run_id: str, read_at: datetime) -> int:
    """The lookup as a source record, and its register fields as observations. Returns fields written."""
    payload = {"register": register, "found": entry is not None, "read_at": read_at.isoformat()}
    if entry:
        payload |= asdict(entry)
    record_id, _ = upsert_source_record(
        cur, company_id=company_id, source_id=source_id, external_id=f"hr:{register.removesuffix(' B')}", external_url=URL,
        source_company_name=entry.name if entry else "", source_payload=payload,
    )
    fields = planned_fields(entry) if entry else {}
    for field_name, value in fields.items():
        insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
            sync_run_id=sync_run_id, field_name=field_name, value_text=value, confidence=1.0,
            metadata={"register": register, "matched_url": URL}, source_updated_at=read_at,
        )
    return len(fields)


def run_lookups() -> int:
    """Look up the next companies, one search at a time. Returns how many fields were written."""
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur, key=SOURCE_KEY, name="Registerportal lookups (handelsregister.de)",
                source_type="register", metadata={"dag_id": DAG_ID, "base_url": URL},
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = companies_to_look_up(cur, source_id, MAX_LOOKUPS)

        written = found = failed = failed_in_a_row = 0
        for company_id, register in companies:
            time.sleep(PAUSE_S)
            try:
                entry = entry_for(register, look_up(register))
            except Exception as exc:  # noqa: BLE001  one failed search must not lose the others
                log.warning("Lookup failed for %s: %s", register, type(exc).__name__)
                failed, failed_in_a_row = failed + 1, failed_in_a_row + 1
                if failed_in_a_row >= MAX_FAILURES_IN_A_ROW:
                    log.error("%d lookups failed in a row; stopping, the portal may be blocking us.",
                              failed_in_a_row)
                    break
                continue
            failed_in_a_row = 0
            found += entry is not None
            with connection, connection.cursor() as cur:
                written += write_lookup(cur, company_id=company_id, register=register, entry=entry,
                                        source_id=source_id, sync_run_id=sync_run_id,
                                        read_at=datetime.now(timezone.utc))

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=found, records_updated=written, records_failed=failed)
        log.info("Looked up %d companies: %d found, %d failed, %d fields written.",
                 len(companies), found, failed, written)
        return written
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------


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
        description="Name, seat and status of up to 50 companies from handelsregister.de; started by hand only",
        # Never a schedule and never a self-trigger: see the module docstring (Nutzungsordnung, Ziffer 3).
        schedule=None, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "register"],
    ) as dag:
        lookups = pipeline_task("look_up_companies", run_lookups, returns=True)
        wrote_any = ShortCircuitOperator(
            task_id="wrote_any", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="look_up_companies")),
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        lookups >> wrote_any >> coordinate
    return dag


dag = _make_dag()
