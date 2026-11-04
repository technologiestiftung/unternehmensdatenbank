"""Websites of companies whose register number discovery found on another company's guessed site (#112).

Discovery guesses a domain for company A, reads the page, and finds a register number that is not A's:
`REGISTER_MISMATCH`, and the page is refused for A. When that number belongs to company B in our
database, the page is very likely B's website: `sobera-capital.com`, guessed for a fund KG, is the site
of Sobera Capital GmbH (#96 hand check). Until now that find was thrown away.

Two tasks per run, no new download for the harvest:

1. **harvest**: every discovery observation with a mismatch and the number it saw, joined to our
   register identifiers by the entry (`HRB 12345`). Each (host, entry of one of our companies) becomes
   a source record of that company, proposing `https://host`. A host naming more than
   `MAX_COMPANIES_PER_HOST` of our companies is a group or directory page and proposes nothing (the
   same rule as Common Crawl's).
2. **verify**, in batches: every proposal goes through `enrichment.proposals`, the check Common
   Crawl's and Wikidata's get. The number on the page lacks its court, so `HRB 12345` of a Hamburg
   company matches our Berlin `HRB 12345`; only the page read again for B (its name in the domain,
   its own register number or officer) proves it.

Counted on play, 2026-11-04: 27,543 mismatch observations on 13,322 sites; 10,881 (site, company)
pairs where the number is one of ours, 6,504 of those companies without a shown website (2,019
active). Where B already had a proven website, it was this host in 1,022 of 1,243 cases.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlparse

from psycopg2.extras import Json, execute_values

from enrichment import confidence as conf
from enrichment import proposals
from source_enrichment_common import ensure_source, get_connection

log = logging.getLogger(__name__)

DAG_ID = "register_crosslink"
SOURCE_KEY = "register_crosslink"
#: More of our companies on one host is a group or directory page (Common Crawl's rule, measured there).
MAX_COMPANIES_PER_HOST = 2
BATCH_SIZE = int(os.getenv("REGISTER_CROSSLINK_VERIFY_BATCH_SIZE", "100"))
WORKERS = int(os.getenv("REGISTER_CROSSLINK_WORKERS", "8"))
RECHECK_AFTER_DAYS = int(os.getenv("REGISTER_CROSSLINK_RECHECK_DAYS", "90"))


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Register numbers on other companies' guessed sites",
                         source_type="derived", metadata={"dag_id": DAG_ID, "issue": 112})


def host_of(url: str) -> str:
    return (urlparse(url).hostname or "").lower().removeprefix("www.")


def proposals_from(rows: Iterable[tuple[str, str, str]]) -> dict[str, tuple[str, str, str]]:
    """(page url, register entry, company id) -> {external_id: (company id, host, entry)}, without hosts
    that name more than `MAX_COMPANIES_PER_HOST` of our companies."""
    by_host: dict[str, dict[str, str]] = {}
    for url, entry, company_id in rows:
        host = host_of(url)
        if host:
            by_host.setdefault(host, {})[company_id] = entry
    return {
        f"{host} {entry}": (company_id, host, entry)
        for host, named in by_host.items() if len(named) <= MAX_COMPANIES_PER_HOST
        for company_id, entry in named.items()
    }


def run_harvest() -> int:
    """Turn discovery's mismatches into proposals. Returns how many (site, company) proposals exist."""
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            cur.execute(
                r"""
                WITH seen AS (
                  SELECT DISTINCT o.company_id, o.value_text AS url, o.metadata ->> 'register_number_seen' AS entry
                  FROM public.company_field_observations o
                  JOIN public.sources s ON s.id = o.source_id AND s.key = 'website_discovery'
                  WHERE o.metadata -> 'evidence' ? %s AND o.metadata ->> 'register_number_seen' IS NOT NULL
                )
                SELECT seen.url, seen.entry, ci.company_id::text
                FROM seen
                JOIN public.company_identifiers ci
                  ON ci.scheme = 'register' AND ci.company_id <> seen.company_id
                 AND substring(ci.value from '((?:HRA|HRB|VR|GnR|PR|GsR) \d+)(?: B)?$') = seen.entry
                """,
                (conf.REGISTER_MISMATCH,),
            )
            found = proposals_from(cur.fetchall())
            if not found:
                return 0
            execute_values(cur, """
                INSERT INTO public.company_source_records
                  (company_id, source_id, external_id, external_url, source_company_name, source_payload,
                   match_method, match_confidence, matched_at, is_active)
                VALUES %s
                ON CONFLICT (source_id, external_id) DO UPDATE SET
                  -- Found again: what it proved and when it was verified stay.
                  source_payload = EXCLUDED.source_payload || jsonb_build_object(
                    'verified_at', public.company_source_records.source_payload -> 'verified_at',
                    'proven_urls', public.company_source_records.source_payload -> 'proven_urls'),
                  last_seen_at = now(), last_synced_at = now(), is_active = true
            """, [(company_id, source_id, external_id, f"https://{host}", host,
                   Json({"register": entry, "websites": [f"https://{host}"]}))
                  for external_id, (company_id, host, entry) in found.items()],
                template="(%s, %s, %s, %s, %s, %s, 'identifier', 1.0, now(), true)", page_size=1000)
    finally:
        connection.close()
    log.info("%d proposals from discovery pages that print one of our companies' register numbers.", len(found))
    return len(found)


def run_verify() -> int:
    return proposals.verify_batch(
        source_id_of=_source_id, provider=SOURCE_KEY, method=conf.METHOD_REGISTER_CROSSLINK,
        batch_size=BATCH_SIZE, recheck_after_days=RECHECK_AFTER_DAYS, workers=WORKERS,
    )


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    # Weekly, after discovery has read more sites; a full verify batch starts the next run.
    with DAG(
        dag_id=DAG_ID,
        description="Websites from register numbers discovery found on other companies' guessed sites",
        schedule="@weekly", start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites"],
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
            python_callable=lambda ti: (ti.xcom_pull(task_ids="verify_websites") or 0) >= BATCH_SIZE,
        )
        next_batch = TriggerDagRunOperator(task_id="next_batch", trigger_dag_id=DAG_ID, wait_for_completion=False)
        harvest >> verify >> [verified_any, more_left]
        verified_any >> coordinate
        more_left >> next_batch
    return dag


dag = _make_dag()
