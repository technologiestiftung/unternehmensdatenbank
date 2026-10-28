#!/usr/bin/env python3
"""The register's current Geschäftsführer as management names, from the OffeneRegister records.

`import_offeneregister_records.py` keeps every officer in the company's source record. This script
turns the current Geschäftsführer among them (167,140 for Berlin) into `ceo_name_or_contact`
observations of the source `offeneregister`; `pipeline_coordinator` then publishes them like any
other name. Before this, the app's management names came only from spaCy: 29 of them.

Current means the register gives no end date and does not mark the person as dismissed. That is
current as of the snapshot, 2017–2019, so many of these people will have left since. Hence the low
confidence: a name read off the company's own Impressum today (spaCy, 0.6) is better evidence.

PERSONAL DATA. Showing the register's Geschäftsführer in the app was approved in the privacy
review of 2026-10-28 (issue #18). The hub and exports are not covered by it. Only the name and the
position are written; birth dates, places and the raw address stay in the source record.

Safe to re-run: this source's management observations are replaced in one transaction. Their
`company_managers` rows lose their pointer and the next coordinator run rebuilds them.

Usage:
    python scripts/import_register_managers.py     # PG_DSN to override the target; then run the coordinator
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "airflow" / "dags"))

import psycopg2  # noqa: E402
from source_enrichment_common import finish_sync_run, insert_text_observation, start_sync_run  # noqa: E402

SOURCE_KEY = "offeneregister"
FIELD = "ceo_name_or_contact"
POSITION = "Geschäftsführer"
#: Above the resolver's 0.5 floor, below spaCy's 0.6: the snapshot is 6–9 years old.
CONFIDENCE = 0.55


def current_managers(officers: list[dict] | None) -> list[str]:
    """The names of the people the register lists as current Geschäftsführer, once each."""
    names: list[str] = []
    for officer in officers or []:
        if officer.get("position") != POSITION or (officer.get("type") or "person") != "person":
            continue
        if officer.get("dismissed") or officer.get("end_date"):
            continue
        name = " ".join((officer.get("name") or "").split())
        if name and name not in names:
            names.append(name)
    return names


def main() -> int:
    dsn = os.getenv("PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app")
    with psycopg2.connect(dsn) as pg, pg.cursor() as cur:
        cur.execute("SELECT id FROM public.sources WHERE key = %s", (SOURCE_KEY,))
        row = cur.fetchone()
        if row is None:
            print("No OffeneRegister source: run import_offeneregister_records.py first.")
            return 1
        source_id = row[0]
        sync_run_id = start_sync_run(cur, source_id=source_id)
        cur.execute("DELETE FROM public.company_field_observations WHERE source_id = %s AND field_name = %s",
                    (source_id, FIELD))
        replaced = cur.rowcount
        cur.execute(
            "SELECT id, company_id, source_payload -> 'officers' FROM public.company_source_records WHERE source_id = %s",
            (source_id,),
        )
        written = companies = 0
        for record_id, company_id, officers in cur.fetchall():
            names = current_managers(officers)
            companies += bool(names)
            for name in names:
                insert_text_observation(
                    cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                    sync_run_id=sync_run_id, field_name=FIELD, value_text=name, confidence=CONFIDENCE,
                    metadata={"position": POSITION},
                )
                written += 1
        finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=companies, records_created=written,
                        records_updated=0, records_failed=0)

    print(json.dumps({"names": written, "companies": companies, "replaced": replaced}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
