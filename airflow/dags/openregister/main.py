"""OpenRegister: Berlin companies with address and business purpose, new ones first, then back in time (#88, #143).

OpenRegister (openregister.de) serves the Handelsregister as an API, updated at least monthly. Its
company search returns, per company, the register entry, name, legal form, current address, business
purpose and whether it is active, without officers. OffeneRegister is a 2017–2019 snapshot, so this
adds the companies founded since and the business purpose OffeneRegister never had.

**Started by hand only, never scheduled** (the developer's decision of 2026-11-04). Whether OpenRegister
allows storing its results and retrieving them regularly was asked of its operators and is not answered
yet; its own source, handelsregister.de, forbids systematic retrieval for itself (Nutzungsordnung,
Ziffer 3), which is why `handelsregister_lookup` is started by hand too.

**Credits.** The free plan has 500 credits in a rolling 30 days; a search costs 10, reading the credit
balance nothing. A run makes at most `MAX_SEARCHES` searches (default 10, so 100 credits) and stops early
when the balance would fall below one search. Each search is one page of Berlin companies incorporated
since `INCORPORATED_FROM`; `pipeline_state` (openregister / search) remembers the next page, so the
next run continues there and the whole list is read across runs. After the last page the next run
starts again at page 1, now only for companies incorporated since 31 days before the last pass read its
first page, so a company incorporated while a long pass ran is read by the next one.

**Back in time (#143).** Once that pass has read its last page, the searches a run has left go to the
**backlog**: the companies incorporated before `INCORPORATED_FROM`, one founding year per pass (2018, then
2017, …, down to `OLDEST_YEAR`), page by page across runs. Each run reads the new-companies pass first, so
the backlog never delays a new company. A company without an incorporation date is in no year and is not
read. Rough size: 100,000+ older companies at 100 per search, so 10,000+ credits, about 20 months on the
free plan. The search's `max` bound on `incorporated_at` is from OpenRegister's filter guide
(docs.openregister.de/filtering, read 2026-11-07), not yet tried against the live API.

**What a run writes**, per company, like `gleif` (`enrichment.register_match.write_entry`): a source
record (`external_id` = OpenRegister's company id), linked by register number, then a unique live name in
the same city, else a new company (only when active); name, legal form (read from the end of the name),
address, postcode, city, status and business purpose as observations dated by the run.

Needs `OPENREGISTER_API_KEY`, an Airflow variable (Admin → Variables; `pipeline_task`, "API keys"). No personal data is requested: the search does not return
officers.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from psycopg2.extras import Json

from enrichment.common_crawl import identifier_values
from enrichment.register_match import RegisterEntry, write_entry
from enrichment.register_names import ACTIVE, DISSOLVED, legal_form
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    schedule_label,
    start_sync_run,
)

log = logging.getLogger(__name__)

DAG_ID = "openregister"
SOURCE_KEY = "openregister"
SCHEDULE = None
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Das Handelsregister als API: Berliner Firmen mit Adresse und Unternehmensgegenstand, zuerst "
        "die seit 2019 eingetragenen, danach Jahr für Jahr die älteren. Über die Registernummer "
        "zugeordnet; fehlt die Firma bei uns, wird sie angelegt."
    ),
    schedule=schedule_label(SCHEDULE),
    base_url="https://openregister.de",
    terms=(
        "Ob OpenRegister das Speichern und regelmäßige Abrufen erlaubt, ist angefragt und noch nicht "
        "beantwortet. Deshalb startet jeden Lauf ein Mensch."
    ),
)
API = "https://api.openregister.de"
BERLIN_COURT = "Berlin (Charlottenburg)"
#: Searches per run, 10 credits each. The free plan has 500 credits in 30 days.
MAX_SEARCHES = int(os.getenv("OPENREGISTER_MAX_SEARCHES", "10"))
#: Results per search. Not documented; set here so a measured maximum can be used.
PER_PAGE = int(os.getenv("OPENREGISTER_PER_PAGE", "100"))
#: The first pass reads companies incorporated since this day (DD-MM-YYYY, the API's format).
INCORPORATED_FROM = os.getenv("OPENREGISTER_INCORPORATED_FROM", "01-01-2019")
#: The backlog walks back one founding year at a time down to this one, then stops.
OLDEST_YEAR = 1900
SEARCH_COST = 10
TIMEOUT_S = 60
_STATE = ("openregister", "search")
_REGISTER = re.compile(r"(\d+)")


def search_body(incorporated_from: str, page: int, incorporated_to: str | None = None) -> dict[str, Any]:
    """One page of Berlin companies incorporated since `incorporated_from` (and up to `incorporated_to`)."""
    incorporated = {"field": "incorporated_at", "min": incorporated_from}
    if incorporated_to:
        incorporated["max"] = incorporated_to
    return {
        "filters": [{"field": "register_court", "value": BERLIN_COURT}, incorporated],
        "pagination": {"page": page, "per_page": PER_PAGE},
    }


def entry_for(item: dict[str, Any], today: str) -> RegisterEntry | None:
    """A search result as the register entry `write_entry` writes, or None when it is not a Berlin entry
    we can read."""
    number = _REGISTER.search(str(item.get("register_number") or ""))
    kind = str(item.get("register_type") or "").strip()
    if item.get("register_court") != BERLIN_COURT or not number or kind not in {"HRA", "HRB", "VR", "GnR", "PR"}:
        return None
    name = (item.get("name") or "").strip()
    if not name:
        return None
    address = item.get("address") or {}
    street, postcode, city = (address.get(k) or None for k in ("street", "postal_code", "city"))
    active = item.get("active")
    fields = {
        "name": name, "legal_form": legal_form(name),
        "address": ", ".join(p for p in (street, " ".join(p for p in (postcode, city) if p)) if p) or None,
        "postcode": postcode, "city": city,
        "register_status": ACTIVE if active is True else DISSOLVED if active is False else None,
        "business_purpose": (item.get("purpose") or "").strip() or None,
    }
    registers = identifier_values(kind, number[1])
    return RegisterEntry(
        external_id=str(item["company_id"]), external_url=f"https://openregister.de/company/{item['company_id']}",
        name=name, registers=registers, city=city, active=active, updated=today,
        fields={k: v for k, v in fields.items() if v},
        payload={"company_id": item["company_id"], "register": registers[0], "active": active},
    )


def next_state(state: dict[str, Any] | None, total_pages: int, today: date) -> dict[str, Any]:
    """Where the next search starts: the next page, or after the last page a new pass for the companies
    incorporated since 31 days before the day this pass read its first page (`pass_started`)."""
    state = dict(state or {"incorporated_from": INCORPORATED_FROM, "page": 1})
    if state["page"] == 1:
        state["pass_started"] = today.isoformat()
    if state["page"] < total_pages:
        return {**state, "page": state["page"] + 1}
    started = date.fromisoformat(state["pass_started"])
    return {"incorporated_from": (started - timedelta(days=31)).strftime("%d-%m-%Y"), "page": 1}


def _day(text: str) -> date:
    return datetime.strptime(text, "%d-%m-%Y").date()


def first_backlog() -> dict[str, int]:
    """The backlog's first pass: the founding year of the day before `INCORPORATED_FROM`."""
    return {"year": (_day(INCORPORATED_FROM) - timedelta(days=1)).year, "page": 1}


def backlog_bounds(year: int) -> tuple[str, str]:
    """A backlog year as the search's (min, max), ending the day before `INCORPORATED_FROM`: a first pass from
    mid-2019 must not skip January to June."""
    last = min(date(year, 12, 31), _day(INCORPORATED_FROM) - timedelta(days=1))
    return f"01-01-{year}", last.strftime("%d-%m-%Y")


def next_backlog(backlog: dict[str, int], total_pages: int) -> dict[str, int] | None:
    """The backlog's next page, or after a year's last page the year before; None once `OLDEST_YEAR` is read."""
    if backlog["page"] < total_pages:
        return {**backlog, "page": backlog["page"] + 1}
    return {"year": backlog["year"] - 1, "page": 1} if backlog["year"] > OLDEST_YEAR else None


def next_search(state: dict[str, Any], backlog: dict[str, int] | None, new_pass_done: bool) -> dict[str, Any] | None:
    """The next search's body: the new-companies pass until this run has read it to its end, then the
    backlog; None once the backlog is read too."""
    if not new_pass_done:
        return search_body(state["incorporated_from"], state["page"])
    if backlog is None:
        return None
    low, high = backlog_bounds(backlog["year"])
    return search_body(low, backlog["page"], high)


def _session():  # noqa: ANN202
    import requests

    key = os.getenv("OPENREGISTER_API_KEY", "")
    if not key:
        raise RuntimeError("OPENREGISTER_API_KEY is not set: add it as an Airflow variable (Admin → Variables).")
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {key}", "User-Agent": "unternehmensdatenbank (Technologiestiftung Berlin)"})
    return session


def credits_left(session: Any) -> int:
    response = session.get(f"{API}/v1/credits", timeout=TIMEOUT_S)
    response.raise_for_status()
    # Never negative; on a paid plan credits beyond it are billed, which this DAG never spends.
    return int(response.json()["remaining_credits"])


def run_search() -> int:
    """Up to `MAX_SEARCHES` pages of Berlin companies: the new-companies pass, then the backlog. Returns how
    many companies it wrote."""
    from pipeline_coordinator.resolver import resolve_register_fields

    session = _session()
    today = datetime.now(timezone.utc).date()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute("SELECT value FROM public.pipeline_state WHERE pipeline = %s AND key = %s", _STATE)
            row = cur.fetchone()
            state = dict(row[0]) if row else {"incorporated_from": INCORPORATED_FROM, "page": 1}
            # A state from before #143 has no backlog: it starts with the first year (None: all read).
            backlog = state.pop("backlog", first_backlog())
            source_id = ensure_source(cur, key=SOURCE_KEY, name="OpenRegister (Handelsregister API)",
                                      source_type="register_api", metadata={"dag_id": DAG_ID, "base_url": API}, details=DETAILS)
            sync_run_id = start_sync_run(cur, source_id=source_id)
        methods: dict[str, int] = {}
        searches = 0
        new_pass_done = False  # this run has read the new-companies pass to its end
        while searches < MAX_SEARCHES:
            body = next_search(state, backlog, new_pass_done)
            if body is None:
                log.info("The backlog is read down to %d; stopping.", OLDEST_YEAR)
                break
            if credits_left(session) < SEARCH_COST:
                log.info("Fewer than %d credits left; stopping.", SEARCH_COST)
                break
            response = session.post(f"{API}/v1/search/company", timeout=TIMEOUT_S, json=body)
            if response.status_code == 402:  # credits used up between the balance check and the search
                log.info("OpenRegister refused the search: no credits left; stopping.")
                break
            response.raise_for_status()
            data = response.json()
            searches += 1
            pagination = data["pagination"]
            total_pages = int(pagination["total_pages"])
            # Each page in its own transaction: a later failure must not undo pages whose credits are spent.
            with connection, connection.cursor() as cur:
                for item in data["results"]:
                    entry = entry_for(item, today.isoformat())
                    method = write_entry(cur, entry, source_id=source_id, sync_run_id=sync_run_id,
                                         created_by=SOURCE_KEY) if entry else "unreadable"
                    methods[method] = methods.get(method, 0) + 1
                log.info("Page %d of %d (%s companies, %s, %s per page).", body["pagination"]["page"], total_pages,
                         pagination.get("total_results"), body["filters"][1], pagination.get("per_page"))
                if new_pass_done:
                    backlog = next_backlog(backlog, total_pages)
                else:
                    state = next_state(state, total_pages, today)
                    new_pass_done = state["page"] == 1  # the next run starts the next pass
                cur.execute(
                    """
                    INSERT INTO public.pipeline_state (pipeline, key, value) VALUES (%s, %s, %s)
                    ON CONFLICT (pipeline, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                    """,
                    (*_STATE, Json({**state, "backlog": backlog})),
                )
        written = sum(v for k, v in methods.items() if k not in {"skipped", "unreadable"})
        with connection, connection.cursor() as cur:
            if written:
                resolve_register_fields(cur)
                cur.execute("SELECT public.refresh_company_funding_links()")
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=sum(methods.values()),
                            records_created=methods.get("created", 0), records_updated=written - methods.get("created", 0),
                            records_failed=methods.get("unreadable", 0))
        log.info("OpenRegister: %d searches; %s.", searches, methods)
        return written
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
        description="By hand only: Berlin companies from OpenRegister's API, new ones first, then older years",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "register"],
    ) as dag:
        # retries=0: a retry would spend credits again.
        search = pipeline_task("search", run_search, retries=0, secrets=("OPENREGISTER_API_KEY",))
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        search >> coordinate
    return dag


dag = _make_dag()
