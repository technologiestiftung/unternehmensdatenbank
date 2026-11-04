"""EU research grants from CORDIS into `public.company_fundings`, once a month (#104).

CORDIS publishes every project of Horizon Europe (2021–2027) and Horizon 2020 (2014–2020) as CSV files,
under the European Commission's reuse notice (exact terms to be confirmed): per project its title and
dates, per participating organisation its name, address, kind and EU contribution.

**Measured on 2026-11-04:** 813 Berlin organisations, 523 of them private companies (`activityType`
PRC) with 1,198 project participations and EUR 748 million of EU contribution. Two measurements:

1. Name match only, on the play server (our test server with the current data), 2026-11-04: 299 of
   the 523 match exactly one company of ours by name key and Berlin, 7 several, 217 none: mostly firms
   founded after the OffeneRegister snapshot, and names CORDIS writes without umlauts
   ("INGENIEURBURO"), which the name key keeps apart from "Ingenieurbüro".
2. The full load with the existing linking, on the local database copy, rolled back: 1,188
   participations of 518 companies, 761 of them linked to 319 companies, EUR 248 million.

Only private companies are loaded, one row per participation (`fkz` = "cordis-<project>-<organisation>"):
research institutes, universities and public bodies are not companies of this database. CORDIS gives no
register number, so, like the Förderkatalog, the grants are linked by `refresh_company_funding_links`
through a name key unique in its city, and never create a company.

Grants are NOT observations: a company has many grants, not one winning value.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import tempfile
import zipfile
from datetime import date, datetime, timezone
from typing import Iterable

from source_enrichment_common import SourceDetails, ensure_source, get_connection, schedule_label

log = logging.getLogger(__name__)

DAG_ID = "cordis"
SOURCE_KEY = "cordis"
SCHEDULE = "@monthly"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Förderungen der EU-Forschungsprogramme Horizon Europe und Horizon 2020 an Firmen in Berlin, "
        "über Name und Ort einer Firma zugeordnet."
    ),
    schedule=schedule_label(SCHEDULE),
    base_url="https://cordis.europa.eu",
    terms=(
        "Weiterverwendung nach dem Reuse Notice der Europäischen Kommission; die genauen Bedingungen "
        "sind noch nicht geprüft."
    ),
    terms_url=(
        "https://data.europa.eu/data/datasets/cordis-eu-research-projects-under-horizon-"
        "europe-2021-2027"
    ),
)
BASE_URL = os.getenv("CORDIS_BASE_URL", "https://cordis.europa.eu/data/")
#: Programme -> (file, name shown as the funding type).
PROGRAMMES = {
    "HORIZON": ("cordis-HORIZONprojects-csv.zip", "Horizon Europe"),
    "H2020": ("cordis-h2020projects-csv.zip", "Horizon 2020"),
}
FUNDER = "Europäische Kommission"
#: 1,198 Berlin company participations on 2026-11-04; far fewer means a broken download.
MIN_ROWS = 500
TIMEOUT_S = 600

_UPSERT_SQL = """
    INSERT INTO public.company_fundings
      (fkz, recipient_name, city, amount_eur, period_from, period_to, topic, ministry, funding_type, source)
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
      last_seen_at = NOW()
"""


def _date(value: str | None) -> date | None:
    try:
        return date.fromisoformat((value or "").strip()[:10])
    except ValueError:
        return None


def _euro(value: str | None) -> float | None:
    try:
        return float((value or "").replace(",", "."))
    except ValueError:
        return None


def is_berlin_company(org: dict[str, str]) -> bool:
    """A private company (`PRC`) with a German address in Berlin ("Berlin", "BERLIN", "Berlin-Adlershof")."""
    city = (org.get("city") or "").strip().lower()
    return org.get("country") == "DE" and org.get("activityType") == "PRC" and (
        city == "berlin" or city.startswith(("berlin-", "berlin "))
    )


def funding_rows(orgs: Iterable[dict[str, str]], projects: dict[str, dict[str, str]], funding_type: str) -> list[tuple]:
    """CORDIS organisation rows -> rows for `_UPSERT_SQL`, Berlin companies only."""
    rows = []
    for org in orgs:
        if not is_berlin_company(org):
            continue
        project = projects.get(org["projectID"], {})
        title = (project.get("title") or "").strip()
        acronym = (project.get("acronym") or org.get("projectAcronym") or "").strip()
        rows.append((
            f"cordis-{org['projectID']}-{org['organisationID']}",
            (org.get("name") or "").strip(),
            "Berlin",
            _euro(org.get("ecContribution")),
            _date(project.get("startDate")),
            _date(project.get("endDate")),
            f"{acronym}: {title}" if acronym and title else (title or acronym or None),
            FUNDER,
            funding_type,
            SOURCE_KEY,
        ))
    # An organisation can be listed more than once in one project (several roles): one row, amounts added.
    merged: dict[str, tuple] = {}
    for row in rows:
        if row[0] in merged and row[3] is not None:
            prev = merged[row[0]]
            merged[row[0]] = prev[:3] + ((prev[3] or 0) + row[3],) + prev[4:]
        else:
            merged.setdefault(row[0], row)
    return list(merged.values())


def _read(archive: zipfile.ZipFile, name: str) -> Iterable[dict[str, str]]:
    with archive.open(name) as raw:
        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", newline=""), delimiter=";")


def _programme_rows(file_name: str, funding_type: str) -> list[tuple]:
    import requests

    with tempfile.TemporaryFile() as zipped:
        with requests.get(BASE_URL + file_name, stream=True, timeout=TIMEOUT_S) as response:
            response.raise_for_status()
            for chunk in response.iter_content(1 << 20):
                zipped.write(chunk)
        with zipfile.ZipFile(zipped) as archive:
            projects = {p["id"]: p for p in _read(archive, "project.csv")}
            return funding_rows(_read(archive, "organization.csv"), projects, funding_type)


def run_cordis() -> int:
    """Load every programme's Berlin company participations and link them. Returns how many rows."""
    from psycopg2.extras import execute_values

    rows = [row for file_name, funding_type in PROGRAMMES.values() for row in _programme_rows(file_name, funding_type)]
    if len(rows) < MIN_ROWS:
        raise RuntimeError(f"CORDIS gave {len(rows)} Berlin company participations; refusing to load that.")
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            ensure_source(cur, key=SOURCE_KEY, name="CORDIS (EU research projects)", source_type="fundings",
                          metadata={"dag_id": DAG_ID}, details=DETAILS)
            execute_values(cur, _UPSERT_SQL, rows, page_size=1000)
            cur.execute("SELECT public.refresh_company_funding_links()")
            cur.execute(
                """
                SELECT count(*) FILTER (WHERE l.fkz IS NOT NULL), count(*)
                FROM public.company_fundings f LEFT JOIN public.company_funding_links l ON l.fkz = f.fkz
                WHERE f.source = %s
                """,
                (SOURCE_KEY,),
            )
            linked, total = cur.fetchone()
            cur.execute("SELECT public.refresh_company_profile()")
    finally:
        connection.close()
    log.info("CORDIS: %d Berlin company participations upserted; %d of %d linked to a company.", len(rows), linked, total)
    return len(rows)


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Monthly EU research grants (Horizon Europe, Horizon 2020) of Berlin companies from CORDIS",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "fundings"],
    ) as dag:
        pipeline_task("load_fundings", run_cordis)
    return dag


dag = _make_dag()

