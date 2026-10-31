"""Company websites from Impressum pages in the Common Crawl, verified like a guessed domain (#56).

Two tasks per run:

1. **harvest** `BLOCKS_PER_RUN` blocks of the newest crawl's `.de` index (`enrichment.common_crawl`).
   Where it got to is `pipeline_state` (`common_crawl` / `harvest`), so a run continues where the
   last stopped and every task can run in a fresh pod. Each Impressum page that names a Berlin court
   is read for register numbers; a number that is one of our companies' register identifiers becomes
   a source record of that company, `external_id` = "host register", proposing `https://host`. A page
   listing several of our companies proposes its host to each of them. When the crawl is done, the
   harvest waits for the next one.
2. **verify**, in batches: every proposal goes through `enrichment.proposals`, the check Wikidata's
   proposals get. A register number proves the page is ABOUT the company; only the page read again
   can prove the host is the company's own. Pilot (#56): 21 of 57 proposals for companies without a
   website were proven, all by the own register number; refused were institute subdomains, a film
   distributor's film sites, a newspaper's subscription shop. End to end on a database copy
   (CC-MAIN-2026-43, 640 of 38,118 blocks): 154 proposals, 50 companies proven.

A full harvest or verify batch starts the next run; the weekly schedule restarts the chain.
"""
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json, execute_values

from enrichment import common_crawl as cc
from enrichment import confidence as conf
from enrichment import proposals
from source_enrichment_common import ensure_source, finish_sync_run, get_connection, start_sync_run

log = logging.getLogger(__name__)

DAG_ID = "common_crawl"
SOURCE_KEY = "common_crawl"
#: About 6 minutes of index and 1,500 archive pages; a crawl's .de part is ~38,000 blocks.
BLOCKS_PER_RUN = int(os.getenv("COMMON_CRAWL_BLOCKS_PER_RUN", "300"))
WORKERS = int(os.getenv("COMMON_CRAWL_WORKERS", "8"))
BATCH_SIZE = int(os.getenv("COMMON_CRAWL_VERIFY_BATCH_SIZE", "100"))
RECHECK_AFTER_DAYS = int(os.getenv("COMMON_CRAWL_RECHECK_DAYS", "90"))
_STATE = ("common_crawl", "harvest")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Common Crawl (Impressum pages with a register number)",
                         source_type="web_archive", metadata={"dag_id": DAG_ID, "terms": "https://commoncrawl.org/terms-of-use"})


# --- harvest --------------------------------------------------------------------------------


def next_state(state: dict[str, Any] | None, latest: str) -> dict[str, Any] | None:
    """Where the harvest goes on: the same crawl until its blocks are done, then the newest crawl.
    None: nothing to do. A new crawl's block list is filled in by the caller (`blocks: None`)."""
    if state and state["next"] < len(state["blocks"]):
        return state
    if state and state["crawl"] == latest:
        return None
    return {"crawl": latest, "blocks": None, "next": 0}


def run_harvest() -> int:
    """Read the next blocks of the index. Returns how many: a full batch starts the next run."""
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute("SELECT value FROM public.pipeline_state WHERE pipeline = %s AND key = %s", _STATE)
            row = cur.fetchone()
        state = next_state(row[0] if row else None, cc.latest_crawl())
        if state is None:
            log.info("The newest crawl is harvested; waiting for the next one.")
            return 0
        if state["blocks"] is None:
            state["blocks"] = cc.tld_blocks(state["crawl"])
            log.info("New crawl %s: %d .de index blocks.", state["crawl"], len(state["blocks"]))
        crawl, start = state["crawl"], state["next"]
        blocks = state["blocks"][start:start + BLOCKS_PER_RUN]

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            # Each block keeps only its Impressum records: 300 blocks of lines would be ~500 MB.
            records = [r for found_in_block in pool.map(lambda b: cc.impressum_records(cc.read_block(crawl, b)), blocks)
                       for r in found_in_block]
            found = [(record, registers) for record, registers in
                     zip(records, pool.map(_registers, records)) if registers]

        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            proposed = propose(cur, found, source_id=source_id, crawl=crawl)
            state["next"] = start + len(blocks)
            cur.execute(
                """
                INSERT INTO public.pipeline_state (pipeline, key, value) VALUES (%s, %s, %s)
                ON CONFLICT (pipeline, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                """,
                (*_STATE, Json(state)),
            )
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(records), records_created=proposed,
                            records_updated=0, records_failed=0)
        log.info("%s blocks %d-%d of %d: %d Impressum pages, %d with a Berlin register number, %d proposals.",
                 crawl, start, state["next"], len(state["blocks"]), len(records), len(found), proposed)
        return len(blocks)
    finally:
        connection.close()


def _registers(record: cc.Record) -> list[tuple[str, str]]:
    try:
        return cc.berlin_registers(cc.read_page(record))
    except Exception:  # noqa: BLE001  one unreadable page must not discard the batch
        log.warning("Could not read %s from the archive", record.url)
        return []


def propose(cur: Any, found: list[tuple[cc.Record, list[tuple[str, str]]]], *, source_id: str, crawl: str) -> int:
    """A source record per (host, register identifier of one of our companies). Returns how many."""
    wanted: dict[str, list[cc.Record]] = {}
    for record, registers in found:
        for register in registers:
            for value in cc.identifier_values(*register):
                wanted.setdefault(value, []).append(record)
    if not wanted:
        return 0
    cur.execute("SELECT value, company_id FROM public.company_identifiers WHERE scheme = 'register' AND value = ANY(%s)",
                (list(wanted),))
    # Keyed by external_id: a host can turn up in two blocks, and one INSERT may not hit a row twice.
    rows = {}
    for value, company_id in cur.fetchall():
        for record in wanted[value]:
            rows[f"{record.host} {value}"] = (
                company_id, source_id, f"{record.host} {value}", record.url, record.host,
                Json({"crawl": crawl, "register": value, "imprint_url": record.url,
                      "websites": [f"https://{record.host}"]}))
    execute_values(cur, """
        INSERT INTO public.company_source_records
          (company_id, source_id, external_id, external_url, source_company_name, source_payload,
           match_method, match_confidence, matched_at, is_active)
        VALUES %s
        ON CONFLICT (source_id, external_id) DO UPDATE SET
          external_url = EXCLUDED.external_url,
          -- Found again in a later crawl: what it proved and when it was verified stay.
          source_payload = EXCLUDED.source_payload || jsonb_build_object(
            'verified_at', public.company_source_records.source_payload -> 'verified_at',
            'proven_urls', public.company_source_records.source_payload -> 'proven_urls'),
          last_seen_at = now(), last_synced_at = now(), is_active = true
    """, list(rows.values()), template="(%s, %s, %s, %s, %s, %s, 'identifier', 1.0, now(), true)", page_size=1000)
    return len(rows)


# --- verify ---------------------------------------------------------------------------------


def run_verify() -> int:
    return proposals.verify_batch(
        source_id_of=_source_id, provider=SOURCE_KEY, method=conf.METHOD_COMMON_CRAWL, batch_size=BATCH_SIZE,
        recheck_after_days=RECHECK_AFTER_DAYS, workers=WORKERS, metadata_of=lambda payload: {"crawl": payload["crawl"]},
    )


def is_full(harvested: int | None, verified: int | None) -> bool:
    return bool(harvested and harvested >= BLOCKS_PER_RUN) or bool(verified and verified >= BATCH_SIZE)


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    # A new crawl comes every few weeks; the weekly run starts the chain, full batches continue it.
    with DAG(
        dag_id=DAG_ID,
        description="Company websites from Common Crawl Impressum pages, verified like a guessed domain",
        schedule="@weekly", start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites", "common_crawl"],
    ) as dag:
        harvest = pipeline_task("harvest", run_harvest, returns=True)
        verify = pipeline_task("verify_websites", run_verify, returns=True)
        verified_any = ShortCircuitOperator(
            task_id="verified_any", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="verify_websites")),
            ignore_downstream_trigger_rules=False,
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        more_left = ShortCircuitOperator(
            task_id="more_left", ignore_downstream_trigger_rules=False,
            python_callable=lambda ti: is_full(ti.xcom_pull(task_ids="harvest"), ti.xcom_pull(task_ids="verify_websites")),
        )
        next_batch = TriggerDagRunOperator(task_id="next_batch", trigger_dag_id=DAG_ID, wait_for_completion=False)
        harvest >> verify >> [verified_any, more_left]
        verified_any >> coordinate
        more_left >> next_batch
    return dag


dag = _make_dag()
