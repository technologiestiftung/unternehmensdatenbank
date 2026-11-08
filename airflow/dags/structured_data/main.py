"""The founding date or year a company's own website states in its structured data (#101).

Many sites embed a schema.org business card as JSON-LD (`enrichment.structured_data`). Some give
`foundingDate`. This DAG reads it from pages we already stored, never downloading anything:

- **Which pages.** Only the stored pages (`company_documents`, the corpus) of the company's
  *published* website, so a candidate site of another company never speaks for it.
- **Which nodes.** Only a business node that names the company: its name or legal name, without the
  legal form, contains the company's whole name as whole words. "Siemens" does not name "Siemens
  Partner Berlin", so a page about a client or a partner does not give its founding date to the
  site's owner.
- **What it writes.** A year becomes `founding_year` ('YYYY', V6). A full date also becomes
  `established_at` ('YYYY-MM-DD'); a year alone never becomes 1 January, a precision the page did
  not claim. The profile shows `established_year` = founding_year, else the year of
  established_at; the app shows the date only when its year equals established_year. Pages
  disagreeing on the year write nothing. Each value is dated by the newest read date of the pages
  that state it; confidence `CONFIDENCE`, unmeasured, the same as other claims a site makes about
  itself. A value the site no longer states drops to confidence 0.

Measured 2026-11-04 on the full Berlin data's local corpus, with the whole-name rule above: 513
companies with stored pages of their website, 12 of them state a founding year in a node naming
them, 0 a full date.

**The year in the page text (#137), its own source `website_founding_text`.** The same pages, read
as text: "gegründet 1990", "1990 gegründet", "Gründungsjahr 1990", "founded in 1990", each only
with the company's own name within `NAME_WINDOW` characters ("Die Beispiel GmbH wurde 1990
gegründet"), so a client's or a partner's history does not count. Pages disagreeing on the year write
nothing. Only `founding_year`, at `TEXT_CONFIDENCE`: above the resolver's 0.50 bar, below the
schema.org year (0.6) and GLEIF (1.0, the current legal entity). Measured 2026-11-08 on the local
corpus (5,332 companies with stored pages; 677 also have a GLEIF year), year within one of GLEIF's,
before the "1990 gegründet" pattern stopped skipping over a nearer year:

- with the name next to the phrase: 25 of 34 (74 %); without it: 29 of 62 (47 %), so it is required;
- "seit 1990" is left out: 67 of 186 (36 %), and 391 of 1,338 companies give several years. It dates
  the trade or the family, not the company.

A miss is mostly an earlier year: the page dates the business, GLEIF the current legal entity. The
full run with the final patterns (local, 2026-11-08, 5,674 companies with stored pages): 318 with a
text year, 261 of them with no year from any other source; 32 of 42 (76 %) within a year of GLEIF;
18 of the 19 that also have a schema.org year agree.
"""
from __future__ import annotations

import gzip
import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any

from enrichment import corpus
from enrichment import structured_data as sd
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    normalize_company_name_for_search,
    schedule_label,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "structured_data"
SOURCE_KEY = "structured_data"
SCHEDULE = "@weekly"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Das Gründungsdatum oder -jahr, das die belegte Website einer Firma in ihren strukturierten "
        "Daten (schema.org, JSON-LD) über sich selbst angibt. Nur Angaben, die die Firma beim Namen "
        "nennen; keine Downloads, nur gespeicherte Seiten."
    ),
    schedule=schedule_label(SCHEDULE),
)
CONFIDENCE = 0.6
FIELDS = ("established_at", "founding_year")
TEXT_SOURCE_KEY = "website_founding_text"
TEXT_DETAILS = SourceDetails(
    description=(
        "Das Gründungsjahr, das der Text der belegten Website einer Firma nennt (\"1990 gegründet\", "
        "\"Gründungsjahr 1990\"), nur mit dem Namen der Firma in der Nähe (150 Zeichen). Datiert oft das Geschäft, "
        "nicht die heutige Gesellschaft. Keine Downloads, nur gespeicherte Seiten."
    ),
    schedule=schedule_label(SCHEDULE),
)
#: Unmeasured as a probability: 32 of 42 within a year of GLEIF (see the docstring).
TEXT_CONFIDENCE = 0.55
#: How near the company's name has to stand to the phrase, in characters either side.
NAME_WINDOW = 150
_TEXT_YEAR = r"(1[89]\d\d|20\d\d)"
_TEXT_PATTERNS = (
    re.compile(r"\bgegr(?:ü|ue)ndet\b[^.\n]{0,25}?\b" + _TEXT_YEAR + r"\b", re.I),
    re.compile(r"\b" + _TEXT_YEAR + r"\b[^.\n\d]{0,25}?\bgegr(?:ü|ue)ndet\b", re.I),
    re.compile(r"\bGr(?:ü|ue)ndung(?:sjahr)?\b[^.\n]{0,15}?\b" + _TEXT_YEAR + r"\b", re.I),
    re.compile(r"\b(?:founded|established)\b[^.\n]{0,20}?\b" + _TEXT_YEAR + r"\b", re.I),
)
_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_GERMAN_DATE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
_YEAR = re.compile(r"^(\d{4})(?:-\d{2})?$")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Structured data on the company website (schema.org)",
                         source_type="website", metadata={"dag_id": DAG_ID}, details=DETAILS)


def _text_source_id(cur: Any) -> str:
    return ensure_source(cur, key=TEXT_SOURCE_KEY, name="Founding year in the company website's text",
                         source_type="website", metadata={"dag_id": DAG_ID}, details=TEXT_DETAILS)


def text_years(text: str, company_key: str, today: date | None = None) -> set[str]:
    """The founding years the page text states next to the company's name (see the docstring)."""
    if len(company_key) < 3:
        return set()
    this_year = (today or date.today()).year
    name = re.compile(rf"(?<!\w){re.escape(company_key)}(?!\w)")
    years = set()
    for pattern in _TEXT_PATTERNS:
        for found in pattern.finditer(text):
            near = text[max(0, found.start() - NAME_WINDOW): found.end() + NAME_WINDOW]
            if 1800 <= int(found.group(1)) <= this_year and name.search(normalize_company_name_for_search(near)):
                years.add(found.group(1))
    return years


def founding(raw: str | None, today: date | None = None) -> tuple[str, str] | None:
    """(`established_at`, 'YYYY-MM-DD') for a full date, (`founding_year`, 'YYYY') for a year
    alone, None for anything else or a value outside 1800..today."""
    today = today or date.today()
    text = (raw or "").strip()
    for pattern, order in ((_DATE, (1, 2, 3)), (_GERMAN_DATE, (3, 2, 1))):
        found = pattern.match(text)
        if found:
            try:
                day = date(*(int(found.group(i)) for i in order))
            except ValueError:
                return None
            return ("established_at", day.isoformat()) if date(1800, 1, 1) <= day <= today else None
    found = _YEAR.match(text)
    if found and 1800 <= int(found.group(1)) <= today.year:
        return ("founding_year", found.group(1))
    return None


def names_company(org: dict[str, Any], company_key: str) -> bool:
    """The node is about this company: its name or legal name contains the company's as whole words."""
    if len(company_key) < 3:
        return False
    whole = re.compile(rf"(?<!\w){re.escape(company_key)}(?!\w)")
    return any(whole.search(normalize_company_name_for_search(value or ""))
               for value in (org.get("name"), org.get("legal_name")))


def decide(values: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """What to write from what the pages state: the year if they all agree on it, plus the date if
    exactly one date is stated; nothing if the years disagree."""
    years = {value[:4] for _, value in values}
    if len(years) != 1:
        return []
    decided = [("founding_year", years.pop())]
    dates = {value for field, value in values if field == "established_at"}
    return decided + [("established_at", dates.pop())] if len(dates) == 1 else decided


def _pages(cur: Any) -> dict[str, dict[str, Any]]:
    """Per company with a published website: its name and the stored pages of that website."""
    cur.execute(
        """
        SELECT DISTINCT ON (d.company_id, d.url) d.company_id::text, p.name, d.url, d.s3_key, d.read_at
        FROM public.company_profile_mv p
        JOIN public.company_documents d ON d.company_id = p.id AND d.kind = 'webpage'
        WHERE p.website IS NOT NULL
          AND lower(split_part(regexp_replace(d.url, '^https?://(www\\.)?', ''), '/', 1))
            = lower(split_part(regexp_replace(p.website, '^https?://(www\\.)?', ''), '/', 1))
        ORDER BY d.company_id, d.url, d.read_at DESC
        """
    )
    companies: dict[str, dict[str, Any]] = defaultdict(lambda: {"pages": []})
    for company_id, name, url, s3_key, read_at in cur.fetchall():
        companies[company_id]["name"] = name
        companies[company_id]["pages"].append((url, s3_key, read_at))
    return companies


def _html(s3_key: str) -> str:
    raw = corpus.get_bytes(s3_key)
    return (json.loads(gzip.decompress(raw)).get("html") or "") if raw else ""


def read_company(name: str, pages: list[tuple[str, str, Any]],
                 html_of: Any = None) -> tuple[list[tuple[str, str, Any]], dict[str, Any]]:
    """The values the company's pages state about its founding, each with the newest read date of the
    pages stating it, and the evidence for the record."""
    html_of = html_of or _html
    key = normalize_company_name_for_search(name or "")
    found: list[tuple[str, str]] = []
    read: list[Any] = []
    evidence: list[dict[str, Any]] = []
    for url, s3_key, read_at in pages:
        for org in sd.organizations(html_of(s3_key)):
            parsed = founding(org.get("founding_date"))
            if parsed and names_company(org, key):
                found.append(parsed)
                read.append(read_at)
                evidence.append({"url": url, "stated": org.get("founding_date"), "read_at": read_at.isoformat()})
    values = [(field, text, max(at for (_, stated), at in zip(found, read) if stated.startswith(text)))
              for field, text in decide(found)]
    return values, {"evidence": evidence}


def _text(html: str) -> str:
    """The page as plain text, from its first 300 KB: a founding sentence sits there, and a huge page
    must not exhaust a pod (an uncapped read of the local corpus was killed for memory)."""
    from enrichment.imprint_extractor import page_text

    return page_text(html[:300_000])


def read_company_text(name: str, pages: list[tuple[str, str, Any]],
                      text_of: Any = None) -> tuple[list[tuple[str, str, Any]], dict[str, Any]]:
    """The founding year the pages' text states next to the company's name, like `read_company`."""
    text_of = text_of or (lambda s3_key: _text(_html(s3_key)))
    key = normalize_company_name_for_search(name or "")
    found: list[tuple[str, str]] = []
    read: list[Any] = []
    evidence: list[dict[str, Any]] = []
    for url, s3_key, read_at in pages:
        years = text_years(text_of(s3_key), key)
        for year in sorted(years):
            found.append(("founding_year", year))
            read.append(read_at)
            evidence.append({"url": url, "stated": year, "read_at": read_at.isoformat()})
    values = [(field, text, max(at for (_, stated), at in zip(found, read) if stated == text))
              for field, text in decide(found)]
    return values, {"evidence": evidence}


def run_read() -> int:
    """Read every company's stored pages and write what they state. Returns how many values changed."""
    if not corpus.enabled():
        log.info("No company corpus configured (CORPUS_S3_ENDPOINT); nothing to read.")
        return 0
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id, text_source_id = _source_id(cur), _text_source_id(cur)
            companies = _pages(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            text_sync_run_id = start_sync_run(cur, source_id=text_source_id)
        changed = text_changed = written = text_written = 0
        for company_id, company in companies.items():
            # Each page downloaded once for both readings; only its text is kept, never the HTML.
            texts: dict[str, str] = {}

            def html_of(s3_key: str) -> str:
                html = _html(s3_key)
                texts[s3_key] = _text(html)
                return html

            try:
                values, payload = read_company(company["name"], company["pages"], html_of)
                text_values, text_payload = read_company_text(company["name"], company["pages"], texts.__getitem__)
            except Exception:  # noqa: BLE001  one unreadable page must not stop the rest
                log.exception("Reading the stored pages of %s failed; tried again next run", company_id)
                continue
            with connection, connection.cursor() as cur:
                changed += write(cur, company_id=company_id, company=company, values=values, payload=payload,
                                 source_id=source_id, sync_run_id=sync_run_id)
                text_changed += write(cur, company_id=company_id, company=company, values=text_values,
                                 payload=text_payload, source_id=text_source_id, sync_run_id=text_sync_run_id,
                                 confidence=TEXT_CONFIDENCE, provider=TEXT_SOURCE_KEY)
                written += bool(values)
                text_written += bool(text_values)
        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies), records_created=written,
                            records_updated=changed, records_failed=0)
            finish_sync_run(cur, sync_run_id=text_sync_run_id, records_seen=len(companies),
                            records_created=text_written, records_updated=text_changed, records_failed=0)
        log.info("%d companies with stored pages of their website; %d state a founding date or year in "
                 "structured data, %d in the text; %d changed.", len(companies), written, text_written,
                 changed + text_changed)
        return changed + text_changed
    finally:
        connection.close()


def write(cur: Any, *, company_id: str, company: dict[str, Any], values: list[tuple[str, str, Any]],
          payload: dict[str, Any], source_id: str, sync_run_id: str, confidence: float = CONFIDENCE,
          provider: str = SOURCE_KEY) -> int:
    """Write the values and drop any other this source stated before to 0. Returns 1 if anything changed."""
    cur.execute(
        "SELECT field_name, value_text FROM public.company_field_observations"
        " WHERE company_id = %s AND source_id = %s AND field_name = ANY(%s) AND confidence > 0",
        (company_id, source_id, list(FIELDS)),
    )
    before = set(cur.fetchall())
    stated = {(field, text) for field, text, _ in values}
    if not stated and not before:
        return 0
    record_id, _ = upsert_source_record(
        cur, company_id=company_id, source_id=source_id, external_id=company_id,
        external_url=company["pages"][0][0], source_company_name=company["name"],
        source_payload={**payload, "values": sorted(stated)},
    )
    for field, text, read_at in values:
        insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
            sync_run_id=sync_run_id, field_name=field, value_text=text, confidence=confidence,
            metadata={"provider": provider, "evidence": payload["evidence"]}, source_updated_at=read_at,
        )
    cur.execute(
        "UPDATE public.company_field_observations SET confidence = 0, updated_at = now()"
        " WHERE company_id = %s AND source_id = %s AND field_name = ANY(%s) AND confidence > 0"
        " AND field_name || '=' || value_text <> ALL(%s::text[])",
        (company_id, source_id, list(FIELDS), [f"{field}={text}" for field, text in stated]),
    )
    return int(before != stated)


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
        description="Founding date or year from the structured data and the text of the company's own stored pages",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites"],
    ) as dag:
        read = pipeline_task("read_structured_data", run_read, returns=True)
        changed = ShortCircuitOperator(
            task_id="changed_any", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="read_structured_data")),
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        read >> changed >> coordinate
    return dag


dag = _make_dag()
