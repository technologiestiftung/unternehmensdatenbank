"""The company's current address, as its own Impressum states it (#141).

The address comes mostly from OffeneRegister, a 2017–2019 snapshot. A company that moved since keeps
the old one, although its own website says otherwise: the Impressum of inomics.com names our
register number HRB 126307 B and an address in 10623, while we showed a 2018 address in 10117.

**An Impressum speaks for the company only when it names our register number and no other.** That
is the proof the page is the company's own (`confidence.identifies_company`), and "no other" keeps
out a group's or an agency's Impressum that lists several companies. A register number printed
with a space ("HRB 794 28") is read both ways (`extract_register_numbers`); the shorter reading of
our own number does not count as another.

**What is read** (`address_on`): the page's **first** postal address, only when it is in Berlin,
with the street and house number right before the postcode (same line or the line above). Later
addresses on a page were the host (STRATO), the IHK, a co-working space, the agency, or the
office that licensed the company; a company whose first address is in Hamburg has moved away,
and its page's first *Berlin* address is not its own. A room ("Raum 128") or a mailbox is not a street.

**Not read:**

- **The name.** In the sample below, 9 lines ending in a legal form were read next to the address;
  most were not the company's name ("c/o COLLECTION Business Centers GmbH", a heading, a
  description). At this confidence they would replace correct register names. Left for later (#141).
- **The managing directors.** They are people; `manager_ner` reads them.

**How it ranks.** Priority 100, like the registers, at `CONFIDENCE`, dated by the day the page was
read. The resolver halves an address's weight every 5 years, counted from when its source read it:
OffeneRegister's 2018 address counts about 0.33 today, so the Impressum wins against it, while an
address GLEIF or OpenRegister read this year (1.0) still wins against the Impressum.

**Measured on play, 2026-11-04 (#141):** 10,451 published websites are proven by the company's own
register number; 3,204 of these companies have no address at all, 1,965 an OffeneRegister postcode
the page did not show. A sample of 26 of the 1,965 pages: 23 give a different Berlin postcode.
Against GLEIF, current in 2025/26: on 11 of 13 pages the first Berlin postcode is GLEIF's.

Each company is read once, like `website_contacts` (whose `read_impressum` this uses; the corpus
shares the download). A read that failed is tried again, at most `MAX_READ_ATTEMPTS` times. A company
without a register number cannot be proven and is marked read without a download.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from enrichment.confidence import register_parts
from enrichment.imprint_extractor import (
    _UNIT_NOT_A_TOWN,
    POSTCODE_RE,
    extract_register_numbers,
    is_berlin_postcode,
    looks_like_person_name,
)
from enrichment.register_names import LEGAL_FORMS
from source_enrichment_common import (
    SourceDetails,
    companies_with_website_to_read,
    ensure_source,
    failed_read_payload,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    schedule_label,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "impressum_address"
SOURCE_KEY = "impressum"
SCHEDULE = "@daily"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Anschrift, wie die Firma sie selbst im Impressum ihrer Website angibt. Nur wenn das "
        "Impressum ihre Registernummer nennt und keine andere. Eine neuere Angabe eines Registers geht vor."
    ),
    schedule=schedule_label(SCHEDULE),
)
BATCH_SIZE = int(os.getenv("IMPRESSUM_ADDRESS_BATCH_SIZE", "300"))
#: On 11 of 13 pages the address agreed with GLEIF's current one (#141): below a register read now.
CONFIDENCE = 0.8
RULE = "own-register-only-first-berlin-address-2026-11-04"

# A street and house number: "Bleibtreustr. 4", "Am Kupfergraben 6a", "Rheinsberger Str. 76/77".
_STREET = re.compile(
    r"([A-ZÄÖÜa-zäöü][\wÄÖÜäöüß.'’\- ]{1,60}?)\s\d{1,4}\s?[a-zA-Z]?(?:\s?[-–/]\s?\d{1,4}\s?[a-zA-Z]?)?"
)
# A room, a building or a mailbox is not the street ("Raum 128"); whole words, so "Bleibtreustr." passes.
_NOT_A_STREET = re.compile(r"\b(?:postfach|raum|haus|etage|zimmer|gebäude|aufgang|tel|fax|hr[ab]|ust)\b", re.IGNORECASE)
_SEGMENT_SPLIT = re.compile(r"[,|·•;]")
# A company name or a person ahead of the street on the same line is not part of the address.
_LEGAL_FORM = re.compile("|".join(rf"(?:{pattern})(?![a-zäöüß])" for _, pattern in LEGAL_FORMS), re.IGNORECASE)
_MAX_STREET_WORDS = 4


def _is_street(name: str) -> bool:
    words = name.split()
    return (len(words) <= _MAX_STREET_WORDS and not _LEGAL_FORM.search(name)
            and not (len(words) > 2 and looks_like_person_name(" ".join(words[:2]))))


def _street_before(text: str, start: int) -> str | None:
    """The street and house number that ends right before `start`: this line, else the line above."""
    line_start = text.rfind("\n", 0, start) + 1
    before = text[line_start:start].strip(" ,")
    if not before and line_start:
        before = text[text.rfind("\n", 0, line_start - 1) + 1:line_start - 1].strip(" ,")
    segment = _SEGMENT_SPLIT.split(before)[-1].strip()
    street = _STREET.fullmatch(segment)
    if street and _is_street(street.group(1)) and not _NOT_A_STREET.search(segment):
        return re.sub(r"\s+", " ", segment)
    return None


def address_on(text: str) -> dict[str, str] | None:
    """`address`, `postcode` and `city` of the page's first postal address, when it is in Berlin."""
    for match in POSTCODE_RE.finditer(text):
        postcode, town = match.group(1), match.group(2)
        if postcode.startswith("00") or _UNIT_NOT_A_TOWN.match(town):
            continue  # not an address: the same filter as `extract_postcodes`
        # Only the first address counts. A company that moved to Hamburg names the Berlin office that
        # licensed it further down; the first Berlin address on its page is not its own.
        if not (is_berlin_postcode(postcode) and town.split("-")[0].casefold() == "berlin"):
            return None
        street = _street_before(text, match.start())
        return {"address": f"{street}, {postcode} Berlin", "postcode": postcode, "city": "Berlin"} if street else None
    return None


def names_only_own_register(text: str, own: list[str]) -> bool:
    """The page names one of our register entries and no other entry."""
    ours = {register_parts(value) for value in own}
    found = [register_parts(entry) for entry in extract_register_numbers(text)]
    if not any(entry in ours for entry in found):
        return False
    # "HRB 794 28" is also read as "HRB 794": that shorter reading is not another entry, but only
    # where the page prints our number split there ("HRB 1263 B" next to our 126307 is another).
    return all(
        (kind, digits) in ours or any(
            kind == own_kind and len(own_digits) > len(digits) and own_digits.startswith(digits)
            and re.search(rf"(?<!\d){digits}[ \u00a0]{own_digits[len(digits):]}(?!\d)", text)
            for own_kind, own_digits in ours)
        for kind, digits in found
    )


def claims_from_impressum(text: str, own_registers: list[str]) -> dict[str, str]:
    """Field name to value: the address this Impressum gives; empty when it does not prove the company."""
    if not text or not names_only_own_register(text, own_registers):
        return {}
    return address_on(text) or {}


def _registers(cur: Any, company_ids: list[str]) -> dict[str, list[str]]:
    cur.execute(
        """
        SELECT company_id::text, array_agg(value) FROM public.company_identifiers
        WHERE scheme = 'register' AND company_id = ANY(%s::uuid[]) GROUP BY 1
        """,
        (company_ids,),
    )
    return dict(cur.fetchall())


def run_read() -> None:
    from website_contacts.main import read_impressum

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur, key=SOURCE_KEY, name="Impressum (address)", source_type="website",
                metadata={"dag_id": DAG_ID, "rule": RULE}, details=DETAILS)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = companies_with_website_to_read(cur, source_id=source_id, limit=BATCH_SIZE)
            registers = _registers(cur, [str(c[0]) for c in companies])

        written = failed = proven = 0
        today = datetime.now(timezone.utc).date().isoformat()
        for company_id, company_name, website, attempts in companies:
            own = registers.get(str(company_id), [])
            text, imprint_url, claims = "", None, {}
            if own:
                try:
                    text, imprint_url = read_impressum(website)
                    claims = claims_from_impressum(text, own)
                except Exception as exc:  # noqa: BLE001  one unreachable site must not stop the batch
                    log.warning("Impressum not readable for %s: %s", company_id, type(exc).__name__)
                    text = ""
            payload = {"rule": RULE, "imprint_url": imprint_url, "found": sorted(claims)}
            if own and not text:
                payload, failed = failed_read_payload(payload, attempts), failed + 1
            proven += bool(claims)
            with connection, connection.cursor() as cur:
                # Written even when nothing was found, so the company counts as read (unless `failed`).
                record_id, _ = upsert_source_record(
                    cur, company_id=company_id, source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{company_id}", external_url=imprint_url or website,
                    source_company_name=company_name, source_payload=payload,
                )
                for field_name, value in claims.items():
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=field_name, value_text=value,
                        confidence=CONFIDENCE, source_updated_at=today,
                        metadata={"rule": RULE, "imprint_url": imprint_url},
                    )
                    written += 1

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=written, records_updated=0, records_failed=failed)
        log.info("Read %d websites: %d Impressums name the company's own register number alone and give an "
                 "address, %d values written, %d unreadable.", len(companies), proven, written, failed)
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
        description="Current address from the Impressum that names the company's own register number",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites", "register"],
    ) as dag:
        read = pipeline_task("read_impressum", run_read)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        read >> coordinate
    return dag


dag = _make_dag()
