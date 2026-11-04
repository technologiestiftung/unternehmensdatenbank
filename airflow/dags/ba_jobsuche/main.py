"""Berlin employers with open positions, from the Federal Employment Agency's job search (#106).

**Started by a person, never by a schedule, and against the portal's terms.** The Bundesagentur für
Arbeit offers no official API. The interface its job search web app uses (`rest.arbeitsagentur.de`,
described by the community project bund.dev) is read here. The portal's terms (Nutzungsbedingungen,
Stand 08.07.2026) forbid it: 2a (3) "Robots, Webspider oder ähnliche Technologien einzusetzen sowie
bestehende … Programmierschnittstellen entgegen dem von der BA beabsichtigten Zweck zu verwenden und so
Inhalte … zum Zweck der Datensammlung und -auswertung auszulesen"; 4 reserves blocking and damages.
The developer decided on 2026-11-04, knowing this, to build it and start runs by hand. Do not give it
a schedule or let it trigger itself without a new decision.

**What a run does.** Reads every open job ad in Berlin (`wo=Berlin`, no radius), `PAGE_SIZE` per
request, `PAUSE_S` apart. Groups the ads by employer (`arbeitgeberKundennummerHash`, the agency's
pseudonymous employer id): name, Berlin postcodes of the ads, number of ads. Matches each employer to
one of our companies by name and postcode (`enrichment.name_match`). Only a match is stored, as a
source record of that company; an employer matching none is counted in the log, never stored. A job ad
names contact persons in its detail view; the list read here does not, and nothing but the employer
is kept. Nothing is written as an observation yet: the counts are a signal of activity and size, never
an employee count, and showing them needs a decision first.

`BA_JOBSUCHE_API_KEY` is the public client id the job search web app sends (see bund.dev); read from
the environment, never from code.
"""
from __future__ import annotations

import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

import requests
from psycopg2.extras import Json, execute_values

from enrichment.imprint_extractor import is_berlin_postcode
from enrichment.name_match import match_by_name
from source_enrichment_common import ensure_source, finish_sync_run, get_connection, start_sync_run

log = logging.getLogger(__name__)

DAG_ID = "ba_jobsuche"
SOURCE_KEY = "ba_jobsuche"
URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
USER_AGENT = "unternehmensdatenbank/1.0 (Technologiestiftung Berlin)"
PAGE_SIZE = 100
#: Seconds between two requests: about 340 pages for 33,514 ads (2026-11-04) take about 6 minutes.
PAUSE_S = float(os.getenv("BA_JOBSUCHE_PAUSE_S", "1.0"))
MAX_PAGES = int(os.getenv("BA_JOBSUCHE_MAX_PAGES", "500"))


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Bundesagentur für Arbeit, Jobsuche (Berlin employers)",
                         source_type="job_board",
                         metadata={"dag_id": DAG_ID, "terms": "https://www.arbeitsagentur.de/nutzungsbedingungen"})


def read_ads() -> tuple[list[dict[str, Any]], int]:
    """Every Berlin job ad of the list view, and how many the agency says there are."""
    key = os.environ["BA_JOBSUCHE_API_KEY"]
    session = requests.Session()
    session.headers.update({"X-API-Key": key, "User-Agent": USER_AGENT})
    ads: list[dict[str, Any]] = []
    total = 0
    for page in range(1, MAX_PAGES + 1):
        response = session.get(URL, params={"wo": "Berlin", "umkreis": 0, "angebotsart": 1,
                                            "size": PAGE_SIZE, "page": page}, timeout=30)
        if response.status_code != 200:
            # The interface may cap how deep one can page; what was read is kept and the gap logged.
            log.warning("Page %d answered HTTP %d; stopping with %d ads read.", page, response.status_code, len(ads))
            break
        data = response.json()
        total = data.get("maxErgebnisse") or total
        batch = data.get("ergebnisliste") or []
        ads += batch
        if not batch or len(ads) >= total:
            break
        time.sleep(PAUSE_S)
    return ads, total


def employers(ads: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Ads grouped by employer: {employer id: {name, postcodes, ads}}. The name is the one most ads use."""
    names: dict[str, Counter] = {}
    postcodes: dict[str, set[str]] = {}
    counts: Counter = Counter()
    for ad in ads:
        name = (ad.get("firma") or "").strip()
        employer = ad.get("arbeitgeberKundennummerHash") or name
        if not employer or not name:
            continue
        names.setdefault(employer, Counter())[name] += 1
        counts[employer] += 1
        for place in ad.get("stellenlokationen") or []:
            postcode = (place.get("adresse") or {}).get("plz")
            if is_berlin_postcode(postcode):
                postcodes.setdefault(employer, set()).add(postcode)
    return {employer: {"name": names[employer].most_common(1)[0][0],
                       "postcodes": sorted(postcodes.get(employer, set())), "ads": counts[employer]}
            for employer in counts}


def run_read() -> int:
    """Read the ads, match the employers, store the matches. Returns how many employers matched."""
    ads, total = read_ads()
    found = employers(ads)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            matches = match_by_name(cur, [(e, v["name"], v["postcodes"]) for e, v in found.items()])
            today = datetime.now(timezone.utc).date().isoformat()
            if matches:
                execute_values(cur, """
                    INSERT INTO public.company_source_records
                      (company_id, source_id, external_id, external_url, source_company_name, source_payload,
                       match_method, match_confidence, matched_at, is_active)
                    VALUES %s
                    ON CONFLICT (source_id, external_id) DO UPDATE SET
                      company_id = EXCLUDED.company_id, source_company_name = EXCLUDED.source_company_name,
                      source_payload = EXCLUDED.source_payload, match_confidence = EXCLUDED.match_confidence,
                      matched_at = now(), last_seen_at = now(), last_synced_at = now(), is_active = true
                """, [(company_id, source_id, employer, "https://www.arbeitsagentur.de/jobsuche/",
                       found[employer]["name"], Json({**found[employer], "read_on": today}), "fuzzy", confidence)
                      for employer, (company_id, confidence) in matches.items()],
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, now(), true)", page_size=1000)
            # An employer without ads now keeps its record, inactive: never deleted.
            cur.execute("UPDATE public.company_source_records SET is_active = false"
                        " WHERE source_id = %s AND is_active AND NOT (external_id = ANY(%s))",
                        (source_id, list(matches)))
            cur.execute("UPDATE public.sources SET last_synced_at = now() WHERE id = %s", (source_id,))
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(found), records_created=len(matches),
                            records_updated=0, records_failed=0)
    finally:
        connection.close()
    log.info("%d of %d Berlin ads read; %d employers, %d matched one of our companies (%d by postcode),"
             " %d matched none (not stored).", len(ads), total, len(found), len(matches),
             sum(1 for _, conf in matches.values() if conf >= 0.9), len(found) - len(matches))
    return len(matches)


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Berlin employers from the BA job search; started by hand only (against the portal's terms, #106)",
        schedule=None, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "manual"],
    ) as dag:
        pipeline_task("read", run_read, returns=True)
    return dag


dag = _make_dag()
