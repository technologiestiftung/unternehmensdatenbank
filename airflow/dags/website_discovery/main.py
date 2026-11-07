"""Website discovery: find each company's website, and keep only the ones proven to be its own.

Each run does two things, then lets `pipeline_coordinator` publish the result:

1. **rescore_websites.** Every stored website observation is scored again against the current
   identity rule (`confidence.identifies_company`), from the evidence stored with it. One that no
   longer qualifies drops to `UNVERIFIED_CAP`, below the bar, and the coordinator un-publishes it.
   This is how the rule of 2026-10-28 reaches the websites found before it. It also takes away a dissolved
   company's stale register number on another company's page (#168), for every source's websites.
   Cheap and idempotent. `pipeline_coordinator` runs the same rescore before it publishes, so a
   source that writes such a website back (a recheck, Common Crawl) never gets it shown.
2. **discover.** One batch of companies this source has not looked at yet: guess domains from the
   name and up to 3 former names (`url_patterns`), download the
   page and its Impressum, and check whose they are, including whether a Geschäftsführer on the
   Impressum is one of the company's register officers. Every candidate URL is stored; only a proven one can become the
   company's website.

**No state outside Postgres.** "Already looked at" is the company's source record under this source,
so a re-run continues where the last stopped, and on the Data Hub every task can run in a fresh pod.
A record older than `RECHECK_AFTER_DAYS` (90) counts as not looked at, so every company is checked
again each quarter; a website a clean recheck no longer finds drops below the bar
(`observations.write_company_result`, step 4). A full batch starts the next run itself, and the hourly schedule restarts the
chain if it breaks; a run with nothing to do ends after `discover` and triggers nothing.
Downloaded pages that loaded are kept in the company corpus (S3, `page_cache`) when
`CORPUS_S3_ENDPOINT` is set; a failed download is not kept, so the next run tries again.

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
from urllib.parse import urlparse

from enrichment import name_frequency, pipeline
from enrichment.officers import load_officers
from enrichment.register_names import load_former_names
from enrichment.observations import (
    RULE,
    SOURCE_KEY,
    SOURCE_NAME,
    rescore_websites,
    write_company_result,
)
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    fetch_companies_without_source,
    finish_sync_run,
    get_connection,
    schedule_label,
    start_sync_run,
)

log = logging.getLogger(__name__)

DAG_ID = "website_discovery"
SCHEDULE = "@hourly"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Rät Domains aus dem Firmennamen und liest das Impressum. Eine Website zählt nur, wenn die "
        "Seite die Firma belegt: eigene Registernummer, ein Geschäftsführer aus dem Register oder "
        "Name und Postleitzahl."
    ),
    schedule=f"{schedule_label(SCHEDULE)} in Paketen",
)
BATCH_SIZE = int(os.getenv("WEBSITE_DISCOVERY_BATCH_SIZE", "50"))
#: Companies checked at once. One company is ~11 s, almost all of it waiting on other servers, so
#: threads help; each company's domains are different hosts, so no one server sees more traffic.
WORKERS = int(os.getenv("WEBSITE_DISCOVERY_WORKERS", "8"))
#: A company is checked again once its last check is older than this: sites appear, move and close.
RECHECK_AFTER_DAYS = int(os.getenv("WEBSITE_DISCOVERY_RECHECK_DAYS", "90"))


# --- rescore --------------------------------------------------------------------------------


def run_rescore() -> None:
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
    cur.execute("SELECT company_name FROM public.v_companies_enriched")
    return name_frequency.common_words([row[0] for row in cur.fetchall()])


def _affixes(cur: Any) -> tuple[str, ...]:
    """What the published websites add to their company's name, measured (#89)."""
    cur.execute("SELECT company_name, website_url FROM public.v_companies_enriched WHERE website_url IS NOT NULL")
    return name_frequency.common_affixes([(name, urlparse(url).hostname or "") for name, url in cur.fetchall()])


def run_discover() -> int:
    """Check one batch. Returns how many companies it took: 0 skips the coordinator, a full batch
    starts the next run."""
    companies = fetch_companies_without_source(SOURCE_KEY, limit=BATCH_SIZE, recheck_after_days=RECHECK_AFTER_DAYS)
    if not companies:
        log.info("Every company was looked at by %s in the last %d days.", SOURCE_KEY, RECHECK_AFTER_DAYS)
        return 0
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(cur, key=SOURCE_KEY, name=SOURCE_NAME, source_type="discovery",
                                      metadata={"dag_id": DAG_ID, "rule": RULE}, details=DETAILS)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            officers = load_officers(cur, [c.id for c in companies])
            former_names = load_former_names(cur, [c.id for c in companies])
            common = _common_words(cur)
            affixes = _affixes(cur)

        stages = {"url"}
        found = errors = 0
        prepared = [dataclasses.replace(c, officers=officers.get(c.id, frozenset()), former_names=former_names.get(c.id, ()))
                    for c in companies]
        # Pages are fetched in threads; every write stays on this thread and its one connection.
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            jobs = {pool.submit(pipeline.discover, c, common_words=common, affixes=affixes): c
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
                                         stages_run=stages)

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=found, records_updated=0, records_failed=errors)
        log.info("Looked at %d companies: %d websites proven, %d with errors", len(companies), found, errors)
        return len(companies)
    finally:
        connection.close()


def is_full_batch(taken: int | None) -> bool:
    """A full batch means more companies are probably waiting; a short one means none are."""
    return bool(taken) and taken >= BATCH_SIZE


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from pipeline_task import pipeline_task
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None

    # Each run starts the next while full batches keep coming, so the whole table is worked through
    # without anyone re-triggering. The hourly schedule restarts the chain after a sleep or a failed
    # run, and starts the recheck once checks grow older than RECHECK_AFTER_DAYS. max_active_runs=1
    # queues a scheduled run behind a running one instead of running two.
    with DAG(
        dag_id=DAG_ID,
        description="Find company websites and keep only those the Impressum proves",
        schedule=SCHEDULE, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites"],
    ) as dag:
        rescore = pipeline_task("rescore_websites", run_rescore)
        # Returns how many companies it took, as XCom: from a PythonOperator directly, from a hub pod
        # through /airflow/xcom/return.json (run_task). The two decisions below run in Airflow itself.
        discover = pipeline_task("discover", run_discover, returns=True)
        took_any = ShortCircuitOperator(
            task_id="took_any", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="discover")),
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        more_left = ShortCircuitOperator(
            task_id="more_left", ignore_downstream_trigger_rules=False,
            python_callable=lambda ti: is_full_batch(ti.xcom_pull(task_ids="discover")),
        )
        next_batch = TriggerDagRunOperator(task_id="next_batch", trigger_dag_id=DAG_ID, wait_for_completion=False)
        rescore >> discover >> took_any >> [coordinate, more_left]
        more_left >> next_batch
    return dag


dag = _make_dag()
