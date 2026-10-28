"""Website discovery: find each company's website, and keep only the ones proven to be its own.

Each run does two things, then lets `pipeline_coordinator` publish the result:

1. **rescore_websites.** Every stored website observation is scored again against the current
   identity rule (`confidence.identifies_company`), from the evidence stored with it. One that no
   longer qualifies drops to `UNVERIFIED_CAP`, below the bar, and the coordinator un-publishes it.
   This is how the rule of 2026-10-28 reaches the websites found before it. Cheap and idempotent.
2. **discover.** One batch of companies this source has not looked at yet: guess domains from the
   name (`url_patterns`), optionally match OpenStreetMap, download the page and its Impressum, and
   check whose they are, including whether a Geschäftsführer on the Impressum is one of the
   company's register officers. Every candidate URL is stored; only a proven one can become the
   company's website.

**No state outside Postgres.** "Already looked at" is the company's source record under this source,
so a re-run continues where the last stopped, and on the Data Hub every task can run in a fresh pod.
The page cache stays off unless `COMBINED_PAGE_CACHE_DIR` is set.

**Personal data.** Register officers are loaded per batch as comparison keys only, used in memory,
and never written: the stored evidence says `officer_name_on_imprint`, never which person matched.
"""
from __future__ import annotations

import dataclasses
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from enrichment import confidence as conf
from enrichment import name_frequency, pipeline
from enrichment.officers import load_officers
from enrichment.observations import SOURCE_KEY, SOURCE_NAME, WEBSITE_SOURCE_KEYS, write_company_result
from source_enrichment_common import (
    fetch_companies_without_source,
    finish_sync_run,
    get_connection,
    ensure_source,
    start_sync_run,
)

log = logging.getLogger(__name__)

DAG_ID = "website_discovery"
ENABLED_FLAG = "ENABLE_WEBSITE_DISCOVERY"
BATCH_SIZE = int(os.getenv("WEBSITE_DISCOVERY_BATCH_SIZE", "50"))
#: OpenStreetMap found 3 websites in 1,000 active Berlin companies and needs a city snapshot on
#: disk, which a hub pod does not keep. Off unless asked for.
USE_OSM = os.getenv("WEBSITE_DISCOVERY_OSM", "false").strip().lower() in {"1", "true", "yes"}
#: Companies checked at once. One company is ~11 s, almost all of it waiting on other servers, so
#: threads help; each company's domains are different hosts, so no one server sees more traffic.
WORKERS = int(os.getenv("WEBSITE_DISCOVERY_WORKERS", "8"))
RULE = "identity-2026-10-28"


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


# --- rescore --------------------------------------------------------------------------------


def demotion(metadata: dict[str, Any], confidence: float | None) -> float | None:
    """The confidence a stored website drops to under the identity rule, or None when it stays.

    Pure, so the guard can test it. Judged on the evidence stored with the observation; a website
    whose page never identified the company falls to the unverified cap, below the bar.
    """
    evidence = set(metadata.get("evidence") or [])
    if conf.identifies_company(evidence):
        return None
    if confidence is None or confidence <= conf.UNVERIFIED_CAP:
        return None
    return conf.UNVERIFIED_CAP


def rescore_websites(cur: Any) -> int:
    """Apply the identity rule to every stored website observation. Returns how many dropped."""
    cur.execute(
        """
        SELECT o.id, o.confidence, o.metadata
        FROM public.company_field_observations o
        JOIN public.sources s ON s.id = o.source_id
        WHERE o.field_name = 'website' AND s.key = ANY(%s)
        """,
        (list(WEBSITE_SOURCE_KEYS),),
    )
    dropped = 0
    for obs_id, confidence, metadata in cur.fetchall():
        new = demotion(metadata or {}, float(confidence) if confidence is not None else None)
        if new is None:
            continue
        cur.execute(
            """
            UPDATE public.company_field_observations
            SET confidence = %s,
                metadata = metadata || jsonb_build_object(
                  'rescored', jsonb_build_object('rule', %s, 'was', confidence, 'at', now())),
                updated_at = now()
            WHERE id = %s
            """,
            (new, RULE, obs_id),
        )
        dropped += 1
    return dropped


def run_rescore() -> None:
    _require_enabled()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            dropped = rescore_websites(cur)
        log.info("Re-scored stored websites against %s: %d dropped below the bar", RULE, dropped)
    finally:
        connection.close()


# --- discover -------------------------------------------------------------------------------


def _common_words(cur: Any) -> set[str]:
    """Name words too common to identify a company, measured over the whole table."""
    cur.execute("SELECT company_name FROM public.companies")
    return name_frequency.common_words([row[0] for row in cur.fetchall()])


def run_discover() -> None:
    _require_enabled()
    companies = fetch_companies_without_source(SOURCE_KEY, limit=BATCH_SIZE)
    if not companies:
        log.info("Every company has been looked at by %s.", SOURCE_KEY)
        return
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(cur, key=SOURCE_KEY, name=SOURCE_NAME, source_type="discovery",
                                      metadata={"dag_id": DAG_ID, "rule": RULE})
            sync_run_id = start_sync_run(cur, source_id=source_id)
            officers = load_officers(cur, [c.id for c in companies])
            common = _common_words(cur)

        stages = {"url"} | ({"osm"} if USE_OSM else set())
        skip = {} if USE_OSM else {"osm": "WEBSITE_DISCOVERY_OSM is off"}
        found = errors = 0
        prepared = [dataclasses.replace(c, officers=officers.get(c.id, frozenset())) for c in companies]
        # Pages are fetched in threads; every write stays on this thread and its one connection.
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            jobs = {pool.submit(pipeline.discover, c, use_osm=USE_OSM, common_words=common): c
                    for c in prepared}
            for job in as_completed(jobs):
                company = jobs[job]
                try:
                    result = job.result()
                except Exception:  # noqa: BLE001  one bad page must not discard the batch
                    # Raising here left the pool finishing every other company and threw all of
                    # their results away. Nothing is written, so a later batch tries it again.
                    log.exception("Discovery failed for company %s; retried in a later batch", company.id)
                    errors += 1
                    continue
                found += bool(result.website)
                errors += bool(result.errors)
                with connection, connection.cursor() as cur:
                    write_company_result(cur, company=company, result=dataclasses.asdict(result),
                                         source_id=source_id, sync_run_id=sync_run_id,
                                         stages_run=stages, skip_reasons=skip)

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=found, records_updated=0, records_failed=errors)
        log.info("Looked at %d companies: %d websites proven, %d with errors", len(companies), found, errors)
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Find company websites and keep only those the Impressum proves",
        schedule=None, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites"],
    ) as dag:
        rescore = PythonOperator(task_id="rescore_websites", python_callable=run_rescore)
        discover = PythonOperator(task_id="discover", python_callable=run_discover)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        rescore >> discover >> coordinate
    return dag


dag = _make_dag()
