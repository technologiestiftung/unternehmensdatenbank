#!/usr/bin/env python3
"""Load `results.jsonl` into the database as a source, so the free findings can compete.

## Why this exists

`run_combined_discovery.py` deliberately writes nothing to the database. That is the right
default for an experiment: it can be re-run, argued with and thrown away without touching
anything the application reads. The cost of it is that its **902 websites over all 14,443
companies** have been sitting in a file, invisible to the resolver, while paid providers
rediscovered a few hundred of them. Tradineo GmbH is the example that made this obvious: URL
guessing found `tradineo.com` at confidence 0.99 with a register-number match, and the company
still showed up in the database as a Brave find, because Brave was the only one that wrote.

This script closes that gap, and nothing else. It reads the file, writes one `website`
observation per company, and stops. It decides no winners — `pipeline_coordinator` does that,
and it is the only thing that may.

## What it writes, and what it refuses to write

One observation per company that has a website, carrying the confidence the pipeline already
measured (0.5687 to 0.99 over the 902) and the evidence behind it. Nothing is re-scored here: a
number invented at import time would be a second opinion competing with the first, and there
would be no way to tell later which one a canonical field came from.

The `reject` band is not imported, and neither are the runner-up answers. A rejected URL is a
useful record of *why* coverage is what it is, but it is not a claim that a company has a
website, and the observations table is for claims.

## Re-running

Idempotent, and since V17 it is idempotent *without deleting what it is about to rewrite*. The
unique key on `(company_id, field_name, source_id, value_text)` lets `insert_text_observation`
update a row in place, so a company whose website has not changed keeps the same observation row —
and therefore keeps its canonical field, because `company_canonical_field_sources` points at that
row and cascades when it is deleted.

That cascade is why this matters. Until V17 this script deleted every one of its own `website`
rows for the companies it was about to write, which took them out of the canonical table for the
length of the gap between the import and the next `pipeline_coordinator` run. On 2026-09-03 that
gap lasted four days and 558 websites: the Brave runs that were meant to follow failed, and a
failed task never triggers the coordinator.

One delete remains, and it is narrow: a value the file has **superseded**. If a company's website
has genuinely changed since the last import, the old observation is wrong and has to go, canonical
pointer included, or it would out-rank the correction on confidence alone. Measured on 2026-09-09,
940 of 940 rows in the file carry the URL already in the table, so that statement removes nothing
today.

Still a deliberate difference from the provider DAGs, which append: they watch a web that changes,
so their history is worth keeping. This reads a file, and a file's history is the file.

## Usage

    python scripts/import_combined_discovery.py --dry-run     # counts only, writes nothing
    python scripts/import_combined_discovery.py

Then trigger `pipeline_coordinator` in Airflow. Until that runs, the observations exist but no
canonical field has changed, and `v_companies_enriched` looks exactly as it did before.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "airflow" / "dags"))

# Running on the host means Postgres is on localhost:5433, not on the compose network. The DAG
# default (`host=postgres port=5432`) is right inside a container and wrong outside one, so supply
# a host-shaped default without overriding an explicit PG_DSN. Same line as
# run_combined_discovery.py, and it has to be set before source_enrichment_common is imported --
# that module reads PG_DSN at import time, which is why the import lives inside `write()`.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("import_combined_discovery")

DEFAULT_RESULTS = REPO / "airflow" / "data" / "combined_discovery" / "results.jsonl"

SOURCE_KEY = "combined_discovery"
SOURCE_NAME = "Combined Discovery (URL patterns + OpenStreetMap + local judge)"

#: Bands worth importing, best first. `reject` is excluded on purpose — see the module docstring.
IMPORTABLE_BANDS = ("high", "medium")


def load_rows(path: Path) -> list[dict[str, Any]]:
    """Every company in the file, deduplicated, last record winning.

    The runner appends and is resumable, so a company re-run after a scoring change appears
    twice. The later line is the newer verdict.
    """
    if not path.exists():
        raise SystemExit(f"{path} does not exist. Run run_combined_discovery.py first.")
    by_company: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        by_company[row["company_id"]] = row
    return list(by_company.values())


def importable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if not row.get("website") or row.get("band") not in IMPORTABLE_BANDS:
            continue
        # A confidence the pipeline never assigned cannot be invented here; without one the row
        # would land at NULL and the resolver could not compare it to anything.
        if row.get("confidence_value") is None:
            log.warning("skipping %s: band %s but no confidence_value", row["company_id"], row["band"])
            continue
        out.append(row)
    return out


def observation_metadata(row: dict[str, Any]) -> dict[str, Any]:
    """What a reader needs to judge this claim without opening results.jsonl.

    `verified_by` and `why` are the two that matter: the first says which rule accepted the URL,
    the second is the full confidence arithmetic as a string. Both travel to the canonical field
    source, so "why does this company have this website" is answerable from the database alone.
    """
    return {
        "verified_by": row.get("verified_by"),
        "band": row.get("band"),
        "methods": row.get("methods") or [],
        "pattern": row.get("pattern"),
        "evidence": sorted(row.get("evidence") or []),
        "imprint_url": row.get("imprint_url"),
        "why": row.get("why"),
    }


def summarise(rows: list[dict[str, Any]]) -> None:
    log.info("%d companies with an importable website", len(rows))
    for label, counter in (
        ("band", Counter(r["band"] for r in rows)),
        ("verified_by", Counter(r.get("verified_by") for r in rows)),
        ("methods", Counter(",".join(sorted(r.get("methods") or [])) for r in rows)),
    ):
        log.info("  %s: %s", label, dict(counter.most_common()))
    values = sorted(r["confidence_value"] for r in rows)
    if values:
        log.info(
            "  confidence: min %.4f, median %.4f, max %.4f",
            values[0], values[len(values) // 2], values[-1],
        )


def write(rows: list[dict[str, Any]]) -> None:
    from source_enrichment_common import (
        ensure_source,
        finish_sync_run,
        get_connection,
        insert_text_observation,
        start_sync_run,
        upsert_source_record,
    )

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur,
                key=SOURCE_KEY,
                name=SOURCE_NAME,
                source_type="combined",
                metadata={"script": "scripts/run_combined_discovery.py", "writes": "website only"},
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)

            company_ids = [r["company_id"] for r in rows]
            websites = [r["website"] for r in rows]

            # Which of these claims the table already holds, so the sync run can report inserts and
            # updates apart. Read before anything is written; the upsert itself cannot tell us,
            # because ON CONFLICT DO UPDATE returns a row either way.
            cur.execute(
                """
                SELECT company_id::text, value_text
                FROM public.company_field_observations
                WHERE source_id = %s AND field_name = 'website'
                  AND company_id = ANY(%s::uuid[])
                """,
                (source_id, company_ids),
            )
            already = {(company_id, value) for company_id, value in cur.fetchall()}

            # Clear only the values the file has SUPERSEDED — a company whose website has changed
            # since the last import. The row that is merely being re-confirmed is left alone and
            # updated in place by the upsert below, which is the entire point of V17: on 2026-09-03
            # a blanket delete of this source's rows took 558 websites out of
            # `company_canonical_field_sources` (it cascades) and they stayed out for four days,
            # because the Brave runs that followed failed and a failed task never reaches
            # `pipeline_coordinator`. Nothing that survives is un-published now.
            #
            # A superseded row still has to go, and its canonical pointer with it: the old answer is
            # wrong, and leaving it would let it out-rank the correction on confidence alone.
            # Measured 2026-09-09: 940 of 940 file rows carry the URL already in the table, so this
            # statement deletes nothing today. It exists for the run where that stops being true.
            cur.execute(
                """
                DELETE FROM public.company_field_observations o
                WHERE o.source_id = %s
                  AND o.field_name = 'website'
                  AND EXISTS (
                    SELECT 1
                    FROM unnest(%s::uuid[], %s::text[]) AS f(company_id, website)
                    WHERE f.company_id = o.company_id
                      AND f.website <> o.value_text
                  )
                """,
                (source_id, company_ids, websites),
            )
            superseded = cur.rowcount

            created = 0
            replaced = 0
            for row in rows:
                record_id, _is_new = upsert_source_record(
                    cur,
                    company_id=row["company_id"],
                    source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{row['company_id']}",
                    external_url=row["website"],
                    source_company_name=row["company_name"],
                    source_payload={
                        "website": row["website"],
                        "band": row.get("band"),
                        "confidence_value": row.get("confidence_value"),
                        "verified_by": row.get("verified_by"),
                        "candidates_generated": row.get("candidates_generated"),
                        "candidates_resolved": row.get("candidates_resolved"),
                    },
                )
                insert_text_observation(
                    cur,
                    company_id=row["company_id"],
                    source_record_id=record_id,
                    source_id=source_id,
                    sync_run_id=sync_run_id,
                    field_name="website",
                    value_text=row["website"],
                    confidence=row["confidence_value"],
                    metadata=observation_metadata(row),
                )
                if (row["company_id"], row["website"]) in already:
                    replaced += 1
                else:
                    created += 1

            finish_sync_run(
                cur,
                sync_run_id=sync_run_id,
                records_seen=len(rows),
                records_created=created,
                records_updated=replaced,
                records_failed=0,
            )
    finally:
        connection.close()

    log.info(
        "%d website observations: %d new, %d updated in place, %d superseded values removed",
        created + replaced, created, replaced, superseded,
    )
    if created or superseded:
        log.info("Nothing is canonical yet. Trigger pipeline_coordinator in Airflow to resolve.")
    else:
        log.info(
            "Every row was an update in place, so no canonical field was disturbed and "
            "`v_companies_enriched` is unchanged. A pipeline_coordinator run is still the way to "
            "pick up any confidence that moved."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS, help="path to results.jsonl")
    parser.add_argument("--dry-run", action="store_true", help="count and summarise, write nothing")
    args = parser.parse_args()

    rows = load_rows(args.results)
    log.info("%s: %d companies", args.results, len(rows))
    keep = importable(rows)
    summarise(keep)

    if not keep:
        log.warning("nothing to import")
        return 1
    if args.dry_run:
        log.info("--dry-run: nothing written")
        return 0
    write(keep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
