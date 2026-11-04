"""Company websites from Berlin Partner's Wirtschaftsatlas Berlin, verified like a guessed domain (#105).

**Started by a person, never by a schedule.** The atlas (businesslocationcenter.de, run by Berlin
Partner für Wirtschaft und Technologie GmbH) offers no licence, download or API. Its Impressum says:
"Die Verwendung von Texten, Textteilen oder Bildmaterial bedarf der vorherigen Zustimmung der Berlin
Partner für Wirtschaft und Technologie GmbH." The developer decided on 2026-11-04, knowing this, to
build the DAG while Berlin Partner is asked, triggered by hand only. Do not give it a schedule or let
it trigger itself without a new decision.

**What a run does.** Two tasks:

1. **match**: read the map's config (`CONFIG_URL`), then every company layer it lists (about 57
   GeoJSON files, `PAUSE_S` apart; schools, childcare, research, administration, associations and
   locations such as co-working spaces are skipped by `is_company_layer`). One entry per name key and
   postcode, with every layer it appears in. Matched to a company in SQL with the one name key
   (`normalize_company_name`): the same key and postcode as exactly one company -> confidence 0.9;
   else the same key as exactly one company that is not dissolved -> 0.8. Each match is a source
   record, `external_id` = name key and postcode. An entry matching none of our companies is counted
   in the log, not stored: `company_source_records` needs a company.
   Measured once on 2026-11-04 (local database): 7,393 entries, 2,263 matched by name, 1,842 of them
   with a website we lacked; where both had one, 346 of 415 were the same host.
2. **verify**: up to `MAX_VERIFY` proposals through `enrichment.proposals`, the check Wikidata's
   websites get. The atlas only proposes; the page proves. A run does not start itself again: a
   person runs it again to verify the rest (matching is skipped then, see `REFRESH_HOURS`).

The layers (Künstliche Intelligenz, FinTech, Robotik, ...) stay in the record's payload. They are a
Technologiefeld signal, but nothing writes them as an observation yet: that needs a mapping table
and a measurement on the review sample first.
"""
from __future__ import annotations

import ast
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import Json, execute_values

from enrichment import confidence as conf
from enrichment import proposals
from enrichment.register_names import DISSOLVED
from source_enrichment_common import ensure_source, finish_sync_run, get_connection, start_sync_run

log = logging.getLogger(__name__)

DAG_ID = "wirtschaftsatlas"
SOURCE_KEY = "wirtschaftsatlas"
BASE_URL = "https://www.businesslocationcenter.de/wirtschaftsatlas-berlin/"
CONFIG_URL = BASE_URL + "config/wab.config.json"
USER_AGENT = "unternehmensdatenbank/1.0 (Technologiestiftung Berlin)"
#: Seconds between two layer downloads: 57 files take about two minutes.
PAUSE_S = 2.0
#: A second run within this time verifies only, without downloading the layers again.
REFRESH_HOURS = 20
MAX_VERIFY = int(os.getenv("WIRTSCHAFTSATLAS_MAX_VERIFY", "500"))
WORKERS = int(os.getenv("WIRTSCHAFTSATLAS_WORKERS", "8"))
RECHECK_AFTER_DAYS = 90
#: Parts of a layer's path in the map's content tree that are not companies.
NOT_COMPANIES = ("Schools", "Childcare", "Science and research", "Research institutions", "Universities",
                 "Colleges", "Politics and Administration", "Associations", "Location Information.Locations")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Wirtschaftsatlas Berlin (Berlin Partner)",
                         source_type="directory",
                         metadata={"dag_id": DAG_ID, "base_url": BASE_URL,
                                   "terms": "Impressum: use needs Berlin Partner's prior consent (#105)"})


# --- reading the atlas ----------------------------------------------------------------------


def is_company_layer(path: str) -> bool:
    return not any(part in path for part in NOT_COMPANIES)


def company_layers(config: dict[str, Any]) -> list[tuple[str, str]]:
    """(GeoJSON path, content-tree path) of every company layer in the map's config."""
    by_name = {layer.get("name"): layer for layer in config.get("layers", [])}
    found: dict[str, str] = {}
    for item in config.get("contentTree", []):
        url = (by_name.get(item.get("layerName")) or {}).get("url")
        url = url.get("de") if isinstance(url, dict) else None
        path = item.get("name") or ""
        if url and "wsv-live" in url and is_company_layer(path):
            found.setdefault(url, path)
    return sorted(found.items())


def entries(features: list[dict[str, Any]], layer: str) -> list[dict[str, Any]]:
    """One row per named feature: name, street, postcode, city, website, layer."""
    rows = []
    for feature in features:
        props = feature.get("properties") or {}
        address = props.get("address") or {}
        if isinstance(address, str):  # the atlas serves it as a Python dict literal: "{'street': ...}"
            try:
                address = ast.literal_eval(address)
            except (ValueError, SyntaxError):
                address = {}
        if not isinstance(address, dict):
            address = {}
        name = (props.get("name") or "").strip()
        if name:
            rows.append({"name": name, "street": (address.get("street") or "").strip(),
                         "postcode": str(address.get("zip") or "").strip(),
                         "city": (address.get("city") or "").strip(),
                         "website": (props.get("website") or "").strip(), "layer": layer})
    return rows


def download() -> list[dict[str, Any]]:
    from enrichment.http_client import session

    def get(url: str) -> Any:
        response = session().get(url, timeout=60, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        return response.json()

    rows = []
    for url, path in company_layers(get(CONFIG_URL)):
        time.sleep(PAUSE_S)
        rows += entries(get(BASE_URL + url).get("features", []), path)
    return rows


# --- match ----------------------------------------------------------------------------------


def run_match() -> int:
    """Download the layers and link their entries to companies. Returns how many records it wrote
    plus how many websites it demoted, so the coordinator runs; skipped (0) when the last download
    is less than `REFRESH_HOURS` old."""
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            cur.execute("SELECT last_synced_at > now() - %s * interval '1 hour' FROM public.sources WHERE id = %s",
                        (REFRESH_HOURS, source_id))
            if cur.fetchone()[0]:
                log.info("Downloaded less than %d hours ago; verifying only.", REFRESH_HOURS)
                return 0
        rows = download()
        with connection, connection.cursor() as cur:
            sync_run_id = start_sync_run(cur, source_id=source_id)
            seen, matched, demoted = match_entries(cur, rows, source_id=source_id)
            cur.execute("UPDATE public.sources SET last_synced_at = now() WHERE id = %s", (source_id,))
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=seen, records_created=matched,
                            records_updated=0, records_failed=0)
        log.info("%d layer entries, %d distinct companies; %d matched one of ours and list a website,"
                 " %d matched none or list no website;"
                 " %d websites demoted.", len(rows), seen, matched, seen - matched, demoted)
        return matched + demoted
    finally:
        connection.close()


def match_entries(cur: Any, rows: list[dict[str, Any]], *, source_id: str) -> tuple[int, int, int]:
    """Group entries by name key and postcode, match them (module docstring), keep each match as a
    source record. A company an entry now leaves for another loses the websites only that entry
    proved. Returns (distinct entries, matched, websites demoted)."""
    cur.execute("CREATE TEMP TABLE _atlas (name text, street text, postcode text, city text, website text,"
                " layer text) ON COMMIT DROP")
    execute_values(cur, "INSERT INTO _atlas VALUES %s",
                   [(r["name"], r["street"], r["postcode"], r["city"], r["website"], r["layer"]) for r in rows])
    cur.execute(
        """
        CREATE TEMP TABLE _entry ON COMMIT DROP AS
        SELECT public.normalize_company_name(name) || ' ' || postcode AS key,
               public.normalize_company_name(name) AS name_key, postcode,
               min(name) AS name, min(street) AS street, min(city) AS city,
               array_remove(array_agg(DISTINCT nullif(website, '')), NULL) AS websites,
               array_agg(DISTINCT layer ORDER BY layer) AS layers
        FROM _atlas WHERE public.normalize_company_name(name) <> ''
        GROUP BY 2, 3
        """
    )
    cur.execute(
        """
        WITH named AS (
          SELECT e.key, p.id AS company_id, p.postcode = e.postcode AS same_postcode,
                 p.register_status IS DISTINCT FROM %(dissolved)s AS live
          FROM _entry e
          JOIN public.company_profile_mv p ON public.normalize_company_name(p.name) = e.name_key
        ),
        by_postcode AS (
          SELECT key, (array_agg(company_id))[1] AS company_id FROM named
          WHERE same_postcode GROUP BY key HAVING count(*) = 1
        ),
        by_name AS (
          SELECT key, (array_agg(company_id))[1] AS company_id FROM named
          WHERE live AND key NOT IN (SELECT key FROM by_postcode) GROUP BY key HAVING count(*) = 1
        )
        SELECT e.key, m.company_id, m.confidence, e.name, e.street, e.postcode, e.city, e.websites, e.layers
        FROM (SELECT key, company_id, 0.9 AS confidence FROM by_postcode
              UNION ALL SELECT key, company_id, 0.8 FROM by_name) m
        JOIN _entry e USING (key)
        WHERE cardinality(e.websites) > 0
        """,
        {"dissolved": DISSOLVED},
    )
    matches = cur.fetchall()
    cur.execute("SELECT count(*) FROM _entry")
    seen = cur.fetchone()[0]
    cur.execute("SELECT external_id, company_id::text FROM public.company_source_records WHERE source_id = %s",
                (source_id,))
    left = proposals.moved_from(dict(cur.fetchall()), {m[0]: str(m[1]) for m in matches})
    records = [
        (company_id, source_id, key, BASE_URL, name,
         Json({"name": name, "street": street, "postcode": postcode, "city": city,
               "websites": websites, "layers": layers}), "fuzzy", confidence)
        for key, company_id, confidence, name, street, postcode, city, websites, layers in matches
    ]
    execute_values(cur, """
        INSERT INTO public.company_source_records
          (company_id, source_id, external_id, external_url, source_company_name, source_payload,
           match_method, match_confidence, matched_at, is_active)
        VALUES %s
        ON CONFLICT (source_id, external_id) DO UPDATE SET
          company_id = EXCLUDED.company_id,
          source_company_name = EXCLUDED.source_company_name,
          -- Verified again only when the websites or the company changed.
          source_payload = EXCLUDED.source_payload || jsonb_build_object(
            'verified_at',
            CASE WHEN public.company_source_records.source_payload -> 'websites' = EXCLUDED.source_payload -> 'websites'
                  AND public.company_source_records.company_id = EXCLUDED.company_id
                 THEN public.company_source_records.source_payload -> 'verified_at' END,
            'proven_urls',
            CASE WHEN public.company_source_records.company_id = EXCLUDED.company_id
                 THEN public.company_source_records.source_payload -> 'proven_urls' END),
          match_confidence = EXCLUDED.match_confidence,
          matched_at = now(), last_seen_at = now(), last_synced_at = now(), is_active = true
    """, records, template="(%s, %s, %s, %s, %s, %s, %s, %s, now(), true)", page_size=1000)
    # An entry gone from the atlas keeps its record, inactive: never deleted.
    cur.execute("UPDATE public.company_source_records SET is_active = false"
                " WHERE source_id = %s AND is_active AND NOT (external_id = ANY(%s))",
                (source_id, [m[0] for m in matches]))
    return seen, len(matches), proposals.demote_unproven(cur, source_id=source_id, company_ids=sorted(left))


# --- verify ---------------------------------------------------------------------------------


def run_verify() -> int:
    """Verify up to `MAX_VERIFY` proposed websites. Returns how many records it checked."""
    checked = 0
    while checked < MAX_VERIFY:
        taken = proposals.verify_batch(
            source_id_of=_source_id, provider=SOURCE_KEY, method=conf.METHOD_WIRTSCHAFTSATLAS,
            batch_size=min(100, MAX_VERIFY - checked), recheck_after_days=RECHECK_AFTER_DAYS, workers=WORKERS,
            metadata_of=lambda payload: {"layers": payload.get("layers", [])},
        )
        checked += taken
        if taken < 100:
            break
    return checked


# --- DAG ------------------------------------------------------------------------------------


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
        description="Websites from Berlin Partner's Wirtschaftsatlas, verified like a guessed domain; started by hand only",
        # Never a schedule and never a self-trigger: see the module docstring (Berlin Partner's consent).
        schedule=None, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites"],
    ) as dag:
        match = pipeline_task("match_entries", run_match, returns=True, retries=0)
        verify = pipeline_task("verify_websites", run_verify, returns=True)
        took_any = ShortCircuitOperator(
            task_id="took_any",
            python_callable=lambda ti: bool(ti.xcom_pull(task_ids="match_entries")
                                            or ti.xcom_pull(task_ids="verify_websites")),
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        match >> verify >> took_any >> coordinate
    return dag


dag = _make_dag()
