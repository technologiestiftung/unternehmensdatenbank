"""GLEIF: Berlin companies with an LEI, from the Global Legal Entity Identifier Foundation's open data (#88).

An **LEI** is a 20-character code a legal entity needs to trade securities or report to a supervisor.
GLEIF publishes every LEI record under CC0: legal name, legal address, the register entry
(`RegistrationAuthorityEntityID`) and whether the entity is active. No officers, so no personal data
beyond what a firm name itself carries.

**Measured on 2026-11-04** (golden copy, 3,484,093 records): 14,183 LEIs belong to the Berlin register
court, Charlottenburg (GLEIF's registration authority `RA000217`). 8,889 of them match one of our
companies by register number. **5,122 active ones are companies we do not have**, 4,446 of them with an
LEI record created in 2019 or later: firms founded after the OffeneRegister snapshot. For 465 companies we
show `active`, GLEIF says inactive.

**What a run reads.** GLEIF publishes the full file (485 MB zipped, 5 GB of CSV) and change files: every
record changed in the last day, week or month. The first run reads the full file; later ones the change
file that covers the time since the last good run (`pipeline_state` gleif / last_run). The DAG runs
daily: up to 5 days without a good run the weekly change file, up to 28 the monthly one, else the full
file again (each change file covers 7 or about 30 days; the margin absorbs a late run). Each is streamed out of its zip, keeping only `RA000217` rows.

**What a run writes**, per LEI: a source record (`external_id` = the LEI), linked by
`enrichment.register_match` (register number, then a unique live name in the same city, else a new
company; only an active entity is created or matched by name, an inactive one only updates a company
that holds its register number), the LEI as
an identifier, and name, legal form, address, postcode, city and register status as
observations dated by GLEIF's last update of the record (`source_updated_at`), so they compete by age with
OffeneRegister's values. Status: `ACTIVE` is `active`, `INACTIVE` is `dissolved` (the user's decision of
2026-11-04). A value GLEIF no longer gives drops to confidence 0. The legal form is read from the end of
the name, as for the other register sources (`enrichment.register_names`). A name that differs from
another source's only in case or punctuation is not written: GLEIF often writes capitals. Once GLEIF's
own name has won, a re-read writes it again rather than dropping it. Measured on the local copy:
of 8,860 names of companies we had, 7,289 identical, 338 case or punctuation only, 1,233 other words,
mostly renames and changes of legal form ("VOSTELL Holding UG" became "VOSTELL Holding GmbH").

**A full run on the local copy** (2026-11-04, golden copy of that day, as the writer role, rolled back):
625 of the 14,183 have a legal address outside Berlin and are left out. Of the 13,500 records written,
8,558 were found by register number, 62 by name, 4,781 active companies were created and 99 inactive ones
we do not have were skipped; 33 match candidates wait for a person. GLEIF's name wins for 5,976 companies.
"""
from __future__ import annotations

import csv
import io
import logging
import re
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

from psycopg2.extras import Json

from enrichment.common_crawl import identifier_values
from enrichment.register_match import company_for, record_match
from enrichment.register_names import ACTIVE, DISSOLVED, legal_form
from source_enrichment_common import (
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "gleif"
SOURCE_KEY = "gleif"
LATEST = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"
#: GLEIF's id for the Handelsregister at Amtsgericht Berlin (Charlottenburg).
BERLIN_REGISTER = "RA000217"
_STATE = ("gleif", "last_run")
#: The change file a run reads, by days since the last good run; older than all of them: the full file.
DELTAS = ((5, "LastWeek"), (28, "LastMonth"))
TIMEOUT_S = 600

_REGISTER = re.compile(r"\b(HRA|HRB|VR|GnR|PR)\s*(\d+)", re.IGNORECASE)
STATUS = {"ACTIVE": ACTIVE, "INACTIVE": DISSOLVED}
FIELDS = ("name", "legal_form", "address", "postcode", "city", "register_status")

COLUMNS = {
    "lei": "LEI", "name": "Entity.LegalName", "authority": "Entity.RegistrationAuthority.RegistrationAuthorityID",
    "register": "Entity.RegistrationAuthority.RegistrationAuthorityEntityID", "status": "Entity.EntityStatus",
    "street": "Entity.LegalAddress.FirstAddressLine", "postcode": "Entity.LegalAddress.PostalCode",
    "city": "Entity.LegalAddress.City", "updated": "Registration.LastUpdateDate",
}


@dataclass(frozen=True)
class LeiRecord:
    lei: str
    name: str
    #: How `company_identifiers` may hold the entry, the plain spelling first.
    registers: list[str]
    status: str | None
    street: str | None
    postcode: str | None
    city: str | None
    updated: str

    def fields(self) -> dict[str, str]:
        """The register fields this record states, as our observations hold them."""
        address = ", ".join(p for p in (self.street, " ".join(p for p in (self.postcode, self.city) if p)) if p)
        values = {"name": self.name, "legal_form": legal_form(self.name), "address": address or None,
                  "postcode": self.postcode, "city": self.city, "register_status": self.status}
        return {field: value for field, value in values.items() if value}


def in_berlin(city: str | None, postcode: str | None) -> bool:
    """A legal address in Berlin: the city starts with "Berlin" ("Berlin-Mitte", not "Schönefeld bei
    Berlin"), or, only when there is no city, a Berlin postcode (10115 to 14199; 12529 is Schönefeld too)."""
    if city and city.strip():
        return city.strip().lower().startswith("berlin")
    return bool(postcode and postcode.isdigit() and 10115 <= int(postcode) <= 14199)


def parse_row(row: dict[str, str]) -> LeiRecord | None:
    """One CSV row, or None when it is not a Berlin register entry we can read.

    A Charlottenburg entry whose legal address is outside Berlin is left out: the register court is the
    one at the seat, so it is stale or wrong. 625 of the 14,183 (2026-11-04), e.g. "General Electric
    Deutschland Holding GmbH" in Frankfurt under HRB 98088, which in Berlin is another company.
    """
    if row.get(COLUMNS["authority"]) != BERLIN_REGISTER:
        return None
    if not in_berlin(row.get(COLUMNS["city"]), (row.get(COLUMNS["postcode"]) or "").strip()):
        return None
    match = _REGISTER.search(row.get(COLUMNS["register"]) or "")
    if not match:  # e.g. "GsR 828", the GbR register since 2024, which `company_identifiers` does not hold
        return None
    clean = lambda key: (row.get(COLUMNS[key]) or "").strip() or None  # noqa: E731
    # in_berlin passed, so a city is a Berlin one: "Berlin-Mitte", but OffeneRegister and the name match know only "Berlin"
    city = "Berlin" if clean("city") else None
    return LeiRecord(
        lei=row[COLUMNS["lei"]], name=clean("name") or "", status=STATUS.get(row.get(COLUMNS["status"]) or ""),
        registers=identifier_values(match[1].upper().replace("GNR", "GnR"), match[2]),
        street=clean("street"), postcode=clean("postcode"), city=city, updated=row[COLUMNS["updated"]],
    )


def same_name(a: str, b: str) -> bool:
    """The same name but for case, spaces and punctuation: "e. V." and "e.V.", "GMBH" and "GmbH"."""
    return re.sub(r"\W+", "", a.lower()) == re.sub(r"\W+", "", b.lower())


def berlin_records(lines: Iterator[str]) -> Iterator[LeiRecord]:
    """The Berlin records of a GLEIF CSV, read line by line: the full file is 5 GB."""
    for row in csv.DictReader(lines):
        record = parse_row(row)
        if record and record.name:
            yield record


def file_to_read(last_run: str | None, now: datetime) -> str:
    """"full", or the change file that covers the days since the last good run."""
    if last_run:
        days = (now - datetime.fromisoformat(last_run)).days
        for most, name in DELTAS:
            if days <= most:
                return name
    return "full"


def _download(kind: str) -> Iterator[LeiRecord]:
    import requests

    latest = requests.get(LATEST, timeout=60)
    latest.raise_for_status()
    files = latest.json()["data"]["lei2"]
    url = files["full_file"]["csv"]["url"] if kind == "full" else files["delta_files"][kind]["csv"]["url"]
    with tempfile.TemporaryFile() as zipped:
        with requests.get(url, stream=True, timeout=TIMEOUT_S) as response:
            response.raise_for_status()
            for chunk in response.iter_content(1 << 20):
                zipped.write(chunk)
        with zipfile.ZipFile(zipped) as archive, archive.open(archive.namelist()[0]) as raw:
            yield from berlin_records(io.TextIOWrapper(raw, encoding="utf-8", newline=""))


def write_record(cur: Any, record: LeiRecord, *, source_id: str, sync_run_id: str) -> str:
    """One LEI: its company (found, or created when active), source record, identifier and observations.
    Returns how it was linked, "skipped" for an inactive entity we do not have."""
    company_id, method, candidates = company_for(
        cur, registers=record.registers, name=record.name, city=record.city, source_id=source_id,
        created_by=SOURCE_KEY, create=record.status == ACTIVE)
    if company_id is None:
        return "skipped"
    record_id, _ = upsert_source_record(
        cur, company_id=company_id, source_id=source_id, external_id=record.lei,
        external_url=f"https://search.gleif.org/#/record/{record.lei}", source_company_name=record.name,
        source_payload={"lei": record.lei, "register": record.registers[0], "status": record.status,
                        "updated": record.updated},
    )
    record_match(cur, record_id=record_id, method=method, candidates=candidates)
    cur.execute(
        """
        INSERT INTO public.company_identifiers (company_id, scheme, value, source_id)
        VALUES (%s, 'lei', %s, %s) ON CONFLICT (scheme, value) DO NOTHING
        """,
        (company_id, record.lei, source_id),
    )
    fields = record.fields()
    cur.execute(
        """
        SELECT value_text FROM public.company_field_observations
        WHERE company_id = %s AND field_name = 'name' AND is_canonical AND source_id <> %s
        """,
        (company_id, source_id),
    )
    current = cur.fetchone()
    if current and same_name(current[0], record.name):
        # "EVENIAT GMBH" for "eveniat GmbH": GLEIF often writes capitals; only a different name is news.
        del fields["name"]
    for field, value in fields.items():
        insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id, sync_run_id=sync_run_id,
            field_name=field, value_text=value, confidence=1.0, source_updated_at=record.updated,
            metadata={"decided_by": "read from the end of the registered name"} if field == "legal_form" else {},
        )
    # A value this record no longer states (a moved address, a new name) drops out of the race.
    cur.execute(
        """
        UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
        WHERE company_source_record_id = %s AND field_name = ANY(%s) AND confidence <> 0
          AND NOT (field_name, value_text) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
        """,
        (record_id, list(FIELDS), list(fields), list(fields.values())),
    )
    return method


def run_load() -> int:
    """Read the file this run needs and write its Berlin records. Returns how many records it wrote."""
    from pipeline_coordinator.resolver import resolve_register_fields

    now = datetime.now(timezone.utc)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute("SELECT value FROM public.pipeline_state WHERE pipeline = %s AND key = %s", _STATE)
            row = cur.fetchone()
        kind = file_to_read((row[0] or {}).get("finished_at") if row else None, now)
        log.info("Reading GLEIF's %s file.", kind)

        with connection, connection.cursor() as cur:
            source_id = ensure_source(cur, key=SOURCE_KEY, name="GLEIF (LEI records, Berlin register)",
                                      source_type="lei", metadata={"dag_id": DAG_ID, "base_url": LATEST})
            sync_run_id = start_sync_run(cur, source_id=source_id)
            methods: dict[str, int] = {}
            for record in _download(kind):
                method = write_record(cur, record, source_id=source_id, sync_run_id=sync_run_id)
                methods[method] = methods.get(method, 0) + 1
            seen = sum(methods.values())
            if seen:
                resolve_register_fields(cur)
                # A new company or a new register number can be the recipient of a grant that names it.
                cur.execute("SELECT public.refresh_company_funding_links()")
            cur.execute(
                """
                INSERT INTO public.pipeline_state (pipeline, key, value) VALUES (%s, %s, %s)
                ON CONFLICT (pipeline, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                """,
                (*_STATE, Json({"finished_at": now.isoformat(), "file": kind, **methods})),
            )
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=seen,
                            records_created=methods.get("created", 0), records_updated=seen - methods.get("created", 0),
                            records_failed=0)
        log.info("GLEIF %s file: %d Berlin records; linked %s.", kind, seen, methods)
        return seen
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
        description="Berlin companies with an LEI from GLEIF's open data: new firms, names, addresses, status",
        schedule="@daily", start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "register"],
    ) as dag:
        load = pipeline_task("load", run_load)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        load >> coordinate
    return dag


dag = _make_dag()

