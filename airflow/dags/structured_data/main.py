"""The founding date or year a company's own website states in its structured data (#101).

Many sites embed a schema.org business card as JSON-LD (`enrichment.structured_data`). Some give
`foundingDate`. This DAG reads it from pages we already stored, never downloading anything:

- **Which pages.** Only the stored pages (`company_documents`, the corpus) of the company's
  *published* website, so a candidate site of another company never speaks for it.
- **Which nodes.** Only a business node that names the company (its name or legal name, without the
  legal form, contains the company's name or the other way round), so a page about a client or a
  partner does not give its founding date to the site's owner.
- **What it writes.** A full date becomes `established_at` ('YYYY-MM-DD'); a year alone becomes
  `founding_year` ('YYYY', V6), never 1 January of that year, a precision the page did not claim.
  Pages disagreeing on the year write nothing. Dated by the page's read date; confidence
  `CONFIDENCE`, unmeasured, the same as other claims a site makes about itself. A value the site no
  longer states drops to confidence 0.

Measured 2026-11-04 on the local corpus (567 pages, 513 companies): 14 companies with a founding
date in a node naming them, 12 of them a year only.
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
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    normalize_company_name_for_search,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "structured_data"
SOURCE_KEY = "structured_data"
CONFIDENCE = 0.6
FIELDS = ("established_at", "founding_year")
_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_GERMAN_DATE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
_YEAR = re.compile(r"^(\d{4})(?:-\d{2})?$")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Structured data on the company website (schema.org)",
                         source_type="website", metadata={"dag_id": DAG_ID})


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
    """The node is about this company: its name or legal name contains the company's, or the reverse."""
    if len(company_key) < 3:
        return False
    for value in (org.get("name"), org.get("legal_name")):
        key = normalize_company_name_for_search(value or "")
        if key and (company_key in key or key in company_key):
            return True
    return False


def decide(values: list[tuple[str, str]]) -> tuple[str, str] | None:
    """One value from what the pages state: the date if every value has its year, else the year if
    they all agree on it, else nothing."""
    years = {value[:4] for _, value in values}
    if len(years) != 1:
        return None
    dates = sorted({value for field, value in values if field == "established_at"})
    if len(dates) == 1:
        return ("established_at", dates[0])
    return ("founding_year", years.pop()) if not dates else None


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


def read_company(name: str, pages: list[tuple[str, str, Any]]) -> tuple[tuple[str, str] | None, dict[str, Any]]:
    """The value the company's pages state about its founding, and the evidence for the record."""
    key = normalize_company_name_for_search(name or "")
    found: list[tuple[str, str]] = []
    evidence: list[dict[str, Any]] = []
    for url, s3_key, read_at in pages:
        for org in sd.organizations(_html(s3_key)):
            parsed = founding(org.get("founding_date"))
            if parsed and names_company(org, key):
                found.append(parsed)
                evidence.append({"url": url, "stated": org.get("founding_date"), "read_at": read_at.isoformat()})
    return decide(found), {"evidence": evidence}


def run() -> int:
    """Read every company's stored pages and write what they state. Returns how many values changed."""
    if not corpus.enabled():
        log.info("No company corpus configured (CORPUS_S3_ENDPOINT); nothing to read.")
        return 0
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            companies = _pages(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
        changed = written = 0
        for company_id, company in companies.items():
            try:
                value, payload = read_company(company["name"], company["pages"])
            except Exception:  # noqa: BLE001  one unreadable page must not stop the rest
                log.exception("Reading the stored pages of %s failed; tried again next run", company_id)
                continue
            with connection, connection.cursor() as cur:
                changed += write(cur, company_id=company_id, company=company, value=value, payload=payload,
                                 source_id=source_id, sync_run_id=sync_run_id)
                written += value is not None
        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies), records_created=written,
                            records_updated=changed, records_failed=0)
        log.info("%d companies with stored pages of their website; %d state a founding date or year; %d changed.",
                 len(companies), written, changed)
        return changed
    finally:
        connection.close()


def write(cur: Any, *, company_id: str, company: dict[str, Any], value: tuple[str, str] | None,
          payload: dict[str, Any], source_id: str, sync_run_id: str) -> int:
    """Write the value and drop any other this source stated before to 0. Returns 1 if anything changed."""
    cur.execute(
        "SELECT field_name, value_text FROM public.company_field_observations"
        " WHERE company_id = %s AND source_id = %s AND field_name = ANY(%s) AND confidence > 0",
        (company_id, source_id, list(FIELDS)),
    )
    before = set(cur.fetchall())
    if value is None and not before:
        return 0
    record_id, _ = upsert_source_record(
        cur, company_id=company_id, source_id=source_id, external_id=company_id,
        external_url=company["pages"][0][0], source_company_name=company["name"],
        source_payload={**payload, "value": list(value) if value else None},
    )
    if value:
        field, text = value
        read_at = max(read_at for _, _, read_at in company["pages"])
        insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
            sync_run_id=sync_run_id, field_name=field, value_text=text, confidence=CONFIDENCE,
            metadata={"provider": SOURCE_KEY, "evidence": payload["evidence"]}, source_updated_at=read_at,
        )
    cur.execute(
        "UPDATE public.company_field_observations SET confidence = 0, updated_at = now()"
        " WHERE company_id = %s AND source_id = %s AND field_name = ANY(%s) AND confidence > 0"
        " AND (field_name, value_text) IS DISTINCT FROM (%s, %s)",
        (company_id, source_id, list(FIELDS), value[0] if value else None, value[1] if value else None),
    )
    return int(before != ({value} if value else set()))


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
        description="Founding date or year from the structured data of the company's own stored pages",
        schedule="@weekly", start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites"],
    ) as dag:
        read = pipeline_task("read_structured_data", run, returns=True)
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
