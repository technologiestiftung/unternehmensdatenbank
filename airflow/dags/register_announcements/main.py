"""Register announcements: what Berlin's register court published about a company this week.

Since August 2022 the register courts publish their announcements (Registerbekanntmachungen) on
handelsregister.de, each for 8 weeks. Five kinds exist; Berlin's 900 of 2026-09-02 to 2026-10-28:

    Registerbekanntmachung nach dem Umwandlungsgesetz   544   merger, split, change of legal form
    Löschungsankündigung                                249   the court intends to delete the company
    Einreichung neuer Dokumente                          60   e.g. a new list of shareholders
    Sonstige Registerbekanntmachung                      47
    Sonderregisterbekanntmachung                          0

New registrations and ordinary changes are **not** among them: since 2022 an entry counts as
announced once it is in the register itself (§ 10 HGB, to be checked), so this page does not replace a
register snapshot.

**What a run does.** Once a week: one GET of the page (for the JSF session) and one POST of its own
search form with Bundesland = Berlin and the default 8-week window. Only the result list is read,
never an announcement's detail page: the list already has the register number, and the details of
deletions and mergers name liquidators and officers. Two requests a week; the portal's terms reportedly
allow 60 an hour.

**Every announcement becomes a source record** (`external_id` = the portal's id), linked to its
company by `enrichment.register_match`: register number first, then a unique live company with the same
name and city, else a new company (name, legal form and city as observations of this source, the legal
form read as the OffeneRegister import reads it, `enrichment.register_names`). On 2026-10-28, 451 of the
900 matched a company we had, every one of them `active` in the 2017–2019 snapshot; 14 of the 425
companies created without the name rule shared name key and city with a company we had (11 `active`),
and 66 grant links by name and city would have been lost, because those need a unique name per city.
Only Berlin's court is read, so only Berlin companies are created.

**What the app shows** is a hint, never the status: `register_announcement` is the latest announcement's
kind, with its date. A deletion announcement does not make a company `dissolved`; the court deletes it
later, or not. One observation per announcement; the latest has confidence 1.0, older ones 0, so
`pipeline_coordinator` publishes the latest.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urljoin

from enrichment.register_match import company_for, record_match
from pipeline_coordinator.resolver import resolve_register_fields
from source_enrichment_common import (
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "register_announcements"
SOURCE_KEY = "registerbekanntmachungen"
FIELD_NAME = "register_announcement"
URL = "https://www.handelsregister.de/rp_web/bekanntmachungen/welcome.xhtml"
#: Says who we are. Never a browser's user agent.
USER_AGENT = "unternehmensdatenbank (Technologiestiftung Berlin; register announcements, weekly)"
#: Seconds between the two requests.
PAUSE_S = 5
TIMEOUT_S = 120
FORM = "bekanntMachungenForm"
#: The court line of a Berlin row starts with this: "Berlin Amtsgericht Berlin (Charlottenburg) HRB 211322".
BERLIN_COURT = "Berlin Amtsgericht "

_ROW_ONCLICK = re.compile(r"fireBekanntmachung\d+\('([^']+)',\s*'(\d+)'\)")
#: Java's Date.toString(): "Wed Oct 28 00:00:00 CET 2026".
_JAVA_DATE = re.compile(r"^\w{3} (\w{3}) (\d{1,2}) [\d:]+ \w+ (\d{4})$")
_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


@dataclass(frozen=True)
class Announcement:
    portal_id: str
    published_on: date
    category: str
    #: As `company_identifiers` holds it: "Berlin (Charlottenburg) HRB 211322".
    register: str
    name: str
    city: str | None


def parse_java_date(value: str) -> date:
    match = _JAVA_DATE.match(value.strip())
    if not match:
        raise ValueError(f"not a Java date: {value!r}")
    month, day, year = match.groups()
    return date(int(year), _MONTHS[month], int(day))


def parse_announcements(html: str) -> list[Announcement]:
    """Every Berlin row of a result page. Pure, so it is tested on a saved page.

    A row is a link whose `onclick` carries the date and the portal's id, and whose label has three
    lines: the category, the court with the register number, and "Name – City".
    """
    from bs4 import BeautifulSoup

    found: list[Announcement] = []
    for link in BeautifulSoup(html, "html.parser").select("a.ui-commandlink"):
        match = _ROW_ONCLICK.search(link.get("onclick") or "")
        label = link.find("label")
        if not match or not label:
            continue
        lines = [line.strip() for line in label.get_text("\n").split("\n") if line.strip()]
        if len(lines) != 3 or not lines[1].startswith(BERLIN_COURT + "Berlin"):
            continue
        name, separator, city = lines[2].rpartition(" – ")
        if not separator:
            name, city = lines[2], ""
        found.append(Announcement(
            portal_id=match.group(2), published_on=parse_java_date(match.group(1)), category=lines[0],
            register=lines[1].removeprefix(BERLIN_COURT), name=name.strip() or lines[2], city=city.strip() or None,
        ))
    return found


def fetch_berlin_page() -> str:
    """The result list for Berlin, last 8 weeks: one GET for the session, one POST of the search form."""
    import requests
    from bs4 import BeautifulSoup

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    page = session.get(URL, timeout=TIMEOUT_S)
    page.raise_for_status()
    form = BeautifulSoup(page.text, "html.parser").find("form", id=FORM)
    if form is None:
        raise RuntimeError("The search form is gone; the page has changed.")

    def value(name: str) -> str:
        field = form.find("input", attrs={"name": name})
        return (field.get("value") or "") if field else ""

    data = {
        FORM: FORM,
        f"{FORM}:datum_von_input": value(f"{FORM}:datum_von_input"),
        f"{FORM}:datum_bis_input": value(f"{FORM}:datum_bis_input"),
        f"{FORM}:land_input": "BE",
        f"{FORM}:registergericht_input": "",
        f"{FORM}:sitz": "",
        f"{FORM}:kategorie_input": "",
        f"{FORM}:rrbSuche": "",
        "javax.faces.ViewState": value("javax.faces.ViewState"),
    }
    time.sleep(PAUSE_S)
    result = session.post(urljoin(URL, form.get("action") or URL), data=data, timeout=TIMEOUT_S)
    result.raise_for_status()
    return result.text


# --- writing --------------------------------------------------------------------------------


def _company_for(cur: Any, item: Announcement, source_id: str) -> tuple[str, str, list[tuple[str, str | None]]]:
    return company_for(cur, registers=[item.register], name=item.name, city=item.city, source_id=source_id,
                       created_by=SOURCE_KEY)


def register_fields(item: Announcement) -> dict[str, str]:
    """What an announcement says about a company it creates: name, legal form, city."""
    from enrichment.register_names import legal_form

    fields = {"name": item.name, "legal_form": legal_form(item.name), "city": item.city}
    return {field: value for field, value in fields.items() if value}


def write_announcement(cur: Any, item: Announcement, *, source_id: str, sync_run_id: str) -> tuple[bool, str]:
    """One announcement as a source record, plus its observation the first time it is seen.

    Returns (new announcement?, "identifier" | "fuzzy" | "created"). Re-runs see the same announcement
    for 8 weeks; only the first sight writes an observation.
    """
    company_id, method, candidates = _company_for(cur, item, source_id)
    payload = asdict(item) | {"published_on": item.published_on.isoformat()}
    record_id, is_new = upsert_source_record(
        cur, company_id=company_id, source_id=source_id, external_id=f"rb:{item.portal_id}",
        external_url=URL, source_company_name=item.name, source_payload=payload,
    )
    record_match(cur, record_id=record_id, method=method, candidates=candidates)
    if method == "created":
        for field, value in register_fields(item).items():
            insert_text_observation(
                cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                sync_run_id=sync_run_id, field_name=field, value_text=value, confidence=1.0,
                metadata={"decided_by": "read from the end of the registered name"} if field == "legal_form" else {},
                source_updated_at=item.published_on,
            )
    if is_new:
        insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
            sync_run_id=sync_run_id, field_name=FIELD_NAME, value_text=item.category, confidence=0.0,
            metadata={"published_on": payload["published_on"], "announcement_id": item.portal_id,
                      "register": item.register, "matched_url": URL},
            source_updated_at=item.published_on,
        )
    return is_new, method


def mark_latest(cur: Any, source_id: str) -> int:
    """The latest announcement per company gets confidence 1.0, every older one 0. Returns rows changed."""
    cur.execute(
        """
        WITH ranked AS (
          SELECT id, row_number() OVER (
                   PARTITION BY company_id
                   ORDER BY (metadata ->> 'published_on')::date DESC, (metadata ->> 'announcement_id')::bigint DESC
                 ) AS rank
          FROM public.company_field_observations
          WHERE source_id = %s AND field_name = %s
        )
        UPDATE public.company_field_observations o
        SET confidence = CASE WHEN r.rank = 1 THEN 1.0 ELSE 0 END, updated_at = now()
        FROM ranked r
        WHERE o.id = r.id AND o.confidence IS DISTINCT FROM CASE WHEN r.rank = 1 THEN 1.0 ELSE 0 END
        """,
        (source_id, FIELD_NAME),
    )
    return cur.rowcount


def run_read() -> None:
    announcements = parse_announcements(fetch_berlin_page())
    if not announcements:
        # Berlin has had about 110 a week; none at all means the page changed, not a quiet week.
        raise RuntimeError("No Berlin announcements on the page; the page format has probably changed.")

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur, key=SOURCE_KEY, name="Registerbekanntmachungen (handelsregister.de)",
                source_type="register_announcements", metadata={"dag_id": DAG_ID, "base_url": URL},
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)
            new = created = linked = 0
            for item in announcements:
                is_new, method = write_announcement(cur, item, source_id=source_id, sync_run_id=sync_run_id)
                new += is_new
                created += method == "created"
                linked += method == "fuzzy"
            changed = mark_latest(cur, source_id)
            if created:
                # The grant links read the published name and city, which a new company only has once resolved.
                resolve_register_fields(cur)
            if created or linked:
                # A new company or a new register number can be the recipient of a grant that names it.
                cur.execute("SELECT public.refresh_company_funding_links()")
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(announcements),
                            records_created=new, records_updated=changed, records_failed=0)
        log.info("%d Berlin announcements on the page: %d new, %d companies created, %d linked by name, "
                 "%d marked latest/older.", len(announcements), new, created, linked, changed)
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Berlin's register announcements of the last 8 weeks from handelsregister.de",
        schedule="@weekly", start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "register"],
    ) as dag:
        read = pipeline_task("read_announcements", run_read)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        read >> coordinate
    return dag


dag = _make_dag()
