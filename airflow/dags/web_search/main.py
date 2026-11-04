"""Websites of companies discovery missed, from a web search API, verified like a guessed domain (#92).

**Started by a person, never by a schedule.** Each search costs credit and sends a company name to a
third party (the Brave Search API). The developer decided on 2026-11-04 to build it with every run
started by hand, before the API's price and terms are settled; nobody has checked whether search
results may be stored (Brave's plans differ on that).

**What a run does.** Two tasks:

1. **search**: up to `MAX_SEARCHES` active companies without a shown website and not searched in the
   last `RECHECK_AFTER_DAYS`, in a fixed order (md5 of the id). Query: the company's name and "Berlin".
   Sole traders are left out: their name is a person's name, and it would go to the search provider.
   Brave returns up to `RESULTS_PER_QUERY` results for the price of one query. The first `MAX_RESULTS`
   hosts that carry the company's name (`own_site.own_site_verdict`, the rule verification applies)
   become one source record per company, proposing `https://host`. Without that filter, directories
   not on the directory list took the slots: 36 of 38 hosts in the first run (20 searches, 2026-11-04).
2. **verify**: those proposals through `enrichment.proposals`, the check Common Crawl's get. A search
   rank proves nothing; only the page read again does (`confidence.METHOD_WEB_SEARCH`).

Needs `BRAVE_SEARCH_API_KEY`, an Airflow variable (Admin → Variables; `pipeline_task`, "API keys").
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlparse

from psycopg2.extras import Json

from enrichment import confidence as conf
from enrichment import proposals
from enrichment.own_site import own_site_verdict
from enrichment.register_names import SOLE_TRADER_NAME_SQL
from source_enrichment_common import SourceDetails, ensure_source, get_connection, schedule_label

log = logging.getLogger(__name__)

DAG_ID = "web_search"
SOURCE_KEY = "web_search"
SCHEDULE = None
API = "https://api.search.brave.com/res/v1/web/search"
MAX_SEARCHES = int(os.getenv("WEB_SEARCH_MAX_SEARCHES", "100"))
MAX_RESULTS = 3
RESULTS_PER_QUERY = 20  # the API's maximum; a query costs the same with 10 or 20
RECHECK_AFTER_DAYS = 180
PAUSE_S = 1.1  # the free plan allows one request a second
WORKERS = int(os.getenv("WEB_SEARCH_WORKERS", "8"))

DETAILS = SourceDetails(
    description=(
        "Websites aus einer Websuche (Brave Search API) nach Name der Firma und \"Berlin\", für Firmen, "
        "deren Website die Website-Suche nicht gefunden hat. Jeder Treffer wird wie eine geratene Domain "
        "geprüft. Einzelkaufleute werden nicht gesucht."
    ),
    schedule=schedule_label(SCHEDULE),
    base_url="https://brave.com/search/api/",
    terms="Preis und ob Suchergebnisse gespeichert werden dürfen, sind nicht geprüft. Deshalb startet jeden Lauf ein Mensch.",
    terms_url="https://api-dashboard.search.brave.com/terms-of-service",
)


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Web search (Brave Search API)", source_type="search_api",
                         metadata={"dag_id": DAG_ID, "issue": 92}, details=DETAILS)


def hosts(results: Iterable[dict[str, Any]], company_name: str, limit: int = MAX_RESULTS) -> list[str]:
    """The first `limit` distinct result hosts that carry the company's name, as `https://host`.

    A host without the name is refused at verification anyway (`host_name_mismatch`), unless the page's
    structured data names the company; such a site is given up here so it cannot crowd out the rest.
    """
    found: dict[str, str] = {}
    for result in results:
        url = result.get("url") or ""
        host = (urlparse(url).hostname or "").lower()
        if not host or host.removeprefix("www.") in found:
            continue
        if own_site_verdict(f"https://{host}", company_name) != "own_site":
            continue
        found[host.removeprefix("www.")] = f"https://{host}"
        if len(found) >= limit:
            break
    return list(found.values())


def search_query(name: str) -> str:
    """The company's name as one quoted phrase, then "Berlin"; a quote inside the name would end the phrase."""
    phrase = name.replace('"', "")
    return f'"{phrase}" Berlin'


def _companies_to_search(cur: Any, source_id: str) -> list[tuple[str, str]]:
    cur.execute(
        r"""
        SELECT e.id::text, e.company_name
        FROM public.v_companies_enriched e
        WHERE e.register_status = 'active' AND e.website_url IS NULL AND e.merged_into IS NULL
          AND e.legal_form IS DISTINCT FROM 'e. K.'
          AND e.company_name !~* %s
          AND NOT EXISTS (
            SELECT 1 FROM public.company_source_records r
            WHERE r.source_id = %s AND r.company_id = e.id
              AND r.last_synced_at > now() - %s * interval '1 day')
        ORDER BY md5(e.id::text)
        LIMIT %s
        """,
        (SOLE_TRADER_NAME_SQL, source_id, RECHECK_AFTER_DAYS, MAX_SEARCHES),
    )
    return cur.fetchall()


def run_search() -> int:
    """Search up to `MAX_SEARCHES` companies. Returns how many got at least one proposal."""
    import requests

    key = os.getenv("BRAVE_SEARCH_API_KEY", "")
    if not key:
        raise RuntimeError("BRAVE_SEARCH_API_KEY is not set: add it as an Airflow variable (Admin → Variables).")
    session = requests.Session()
    session.headers.update({"Accept": "application/json", "X-Subscription-Token": key})
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            companies = _companies_to_search(cur, source_id)
        searched, with_hits = 0, 0
        for company_id, name in companies:
            query = search_query(name)
            response = session.get(API, params={"q": query, "country": "DE", "search_lang": "de", "count": RESULTS_PER_QUERY},
                                   timeout=20)
            response.raise_for_status()
            proposed = hosts((response.json().get("web") or {}).get("results") or [], name)
            with_hits += bool(proposed)
            # Written at once, so a later failure does not throw away searches already paid for. A company
            # with no usable hit is recorded too, so it is not searched again for RECHECK_AFTER_DAYS.
            with connection, connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.company_source_records
                      (company_id, source_id, external_id, source_company_name, source_payload,
                       match_method, match_confidence, matched_at, is_active)
                    VALUES (%s, %s, %s, %s, %s, 'identifier', 1.0, now(), true)
                    ON CONFLICT (source_id, external_id) DO UPDATE SET
                      source_payload = EXCLUDED.source_payload, last_seen_at = now(), last_synced_at = now(),
                      is_active = true
                """, (company_id, source_id, company_id, name,
                      Json({"query": query, "websites": proposed,
                            **({} if proposed else {"verified_at": datetime.now(timezone.utc).isoformat()})})))
            searched += 1
            time.sleep(PAUSE_S)
    finally:
        connection.close()
    log.info("Searched %d companies; %d got at least one proposal.", searched, with_hits)
    return with_hits


def run_verify() -> int:
    return proposals.verify_batch(
        source_id_of=_source_id, provider=SOURCE_KEY, method=conf.METHOD_WEB_SEARCH,
        batch_size=MAX_SEARCHES, recheck_after_days=RECHECK_AFTER_DAYS, workers=WORKERS,
    )


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
        description="By hand only: websites discovery missed, from the Brave Search API, verified like a guess",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites"],
    ) as dag:
        # retries=0: a retry would spend searches again.
        search = pipeline_task("search", run_search, retries=0, secrets=("BRAVE_SEARCH_API_KEY",))
        verify = pipeline_task("verify_websites", run_verify, returns=True)
        coordinate = TriggerDagRunOperator(task_id="trigger_pipeline_coordinator",
                                           trigger_dag_id="pipeline_coordinator", wait_for_completion=False)
        search >> verify >> coordinate
    return dag


dag = _make_dag()
