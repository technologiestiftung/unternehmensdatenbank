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

This script closes that gap. It decides no winners — `pipeline_coordinator` does that, and it is
the only thing that may.

## What it writes

Since step 3 of the unification it writes nothing itself: every row goes through
`combined_discovery/observations.py`, the module that was written for this job in the same commit
that created the package and then sat unused for a month. So the shape of the write is decided in
one place, and a live run and a file replay produce the same rows.

Per company, `observations.write_company_result` writes:

- one `search_query` marker, so the company is visible in `v_discovery_scorecard` at all;
- three `discovery_tier` rows, one per method, recording whether it ran and what it found. Without
  these, "OpenStreetMap had nothing for this company" and "OpenStreetMap was never asked" are the
  same absence, and the second reads as evidence that a company has no website;
- one row **per answer**, not just the winner, filed as `website` (confidence ≥ 0.35),
  `candidate_url` (below the bar) or `directory_url` (a page *about* the company);
- an `osm_id` row when OpenStreetMap matched, which is a stable handle for the business whether or
  not the map knew a website.

Nothing is re-scored here. A number invented at import time would be a second opinion competing
with the first, and there would be no way to tell later which one a canonical field came from.

Two filters were removed in step 3, both of which had been losing data. The band filter dropped
`low`, which is at or above 0.35 and therefore promotable — a coverage bug. And requiring a website
dropped every company that found nothing, which is the population the tier ledger exists to
describe.

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

Since step 7 it resolves what it wrote: `pipeline_coordinator` is called directly when anything
changed, so `v_companies_enriched` is up to date when the script exits. Pass `--skip-coordinator` to
leave the observations unresolved for somebody else to pick up.
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

#: Bands that used to be worth importing. **No longer applied**, and kept only so the removal is
#: legible: this filter dropped the `low` band, which is at or above `WEBSITE_MIN_CONFIDENCE` (0.35)
#: and therefore promotable by `pipeline_coordinator`. That was a coverage bug, not a policy — the
#: gate belongs at 0.35, where `observations.field_name_for` already puts it, not at a band name.
#: `reject` needs no filter either: a rejected URL lands under `candidate_url`, which the resolver
#: reads at 0.35 and so never promotes.
_REMOVED_IMPORTABLE_BANDS = ("high", "medium")


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
    """Every company the runner looked at — which is now all of them.

    The band filter is gone (see `_REMOVED_IMPORTABLE_BANDS`) and so is the website requirement.
    `observations.write_company_result` writes a `search_query` marker and three `discovery_tier`
    rows for every company whether it found anything or not, and those rows are the whole point:
    without them "OpenStreetMap had nothing for this company" and "OpenStreetMap was never asked"
    are the same absence in the database, and the second one reads as evidence that a company has
    no website.

    The only row still refused is one claiming a website with no confidence behind it. A number the
    pipeline never assigned cannot be invented here; it would land NULL, and the resolver's
    `COALESCE(confidence, 0) >= 0.35` would silently exclude it anyway.
    """
    out = []
    for row in rows:
        if row.get("website") and row.get("confidence_value") is None:
            log.warning(
                "skipping %s: band %s claims a website with no confidence_value",
                row["company_id"], row.get("band"),
            )
            continue
        out.append(row)
    return out


def summarise(rows: list[dict[str, Any]]) -> None:
    """What is about to be written. Every company now, not only the ones with a website.

    The band and confidence lines describe the subset that claims a website; the rest of the rows
    are the `search_query` and `discovery_tier` ledger, which is exactly the population that used
    to be invisible.
    """
    with_website = [r for r in rows if r.get("website")]
    log.info("%d companies to write, %d of them claiming a website", len(rows), len(with_website))
    for label, counter in (
        ("band", Counter(r.get("band") for r in rows)),
        ("verified_by", Counter(r.get("verified_by") for r in with_website)),
        ("methods", Counter(",".join(sorted(r.get("methods") or [])) for r in with_website)),
        ("stages_run", Counter(",".join(sorted(r.get("stages_run") or [])) for r in rows)),
    ):
        log.info("  %s: %s", label, dict(counter.most_common(8)))
    values = sorted(r["confidence_value"] for r in with_website if r.get("confidence_value"))
    if values:
        log.info(
            "  confidence of the website claims: min %.4f, median %.4f, max %.4f",
            values[0], values[len(values) // 2], values[-1],
        )
    answers = sum(len(r.get("answers") or []) for r in rows)
    log.info("  %d answers across those companies — each one becomes a URL observation", answers)


def trigger_coordinator() -> bool:
    """Resolve what was just written into canonical fields. Returns True if it ran.

    A direct call to `run_pipeline_coordinator`, not an Airflow trigger, for the reasons set out at
    length in `run_combined_discovery.trigger_coordinator` -- one implementation, two callers, no
    scheduler required, and no `dag_run` row to show for it. Deliberately duplicated here rather
    than imported: this script has no other reason to load 4,200 lines of runner.

    Never fatal. The observations are already written; leaving them unresolved is the state every
    previous version of this script left them in, and the log says so.
    """
    log.info("triggering pipeline_coordinator")
    try:
        coordinator = REPO / "airflow" / "dags" / "pipeline_coordinator"
        if str(coordinator) not in sys.path:
            sys.path.insert(0, str(coordinator))
        from pipeline_coordinator.main import run_pipeline_coordinator

        run_pipeline_coordinator()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "pipeline_coordinator did not run: %s. The observations are written and unresolved; "
            "trigger the DAG in Airflow, or re-run this script.", exc,
        )
        return False
    log.info("pipeline_coordinator finished. Canonical fields are up to date.")
    return True


def _company_from_row(row: dict[str, Any]) -> Any:
    """A `CompanyCandidate` built from the file, so no read of `public.companies` is needed.

    `observations.write_company_result` only ever touches `.id`, `.company_name` and — through
    `build_search_queries` — `.address`, `.postcode`, `.city`, `.registration_number`. Every one of
    those is a top-level key in `results.jsonl`.
    """
    from source_enrichment_common import CompanyCandidate

    return CompanyCandidate(
        id=row["company_id"],
        company_name=row["company_name"],
        address=row.get("address"),
        postcode=row.get("postcode"),
        city=row.get("city"),
        registration_number=row.get("registration_number"),
    )


def write(rows: list[dict[str, Any]]) -> bool:
    from combined_discovery import observations
    from source_enrichment_common import (
        ensure_source,
        finish_sync_run,
        get_connection,
        start_sync_run,
    )

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur,
                key=SOURCE_KEY,
                name=SOURCE_NAME,
                source_type="combined",
                metadata={
                    "script": "scripts/run_combined_discovery.py",
                    "writes": "website, candidate_url, directory_url, discovery_tier, "
                              "search_query, osm_id",
                },
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)

            company_ids = [r["company_id"] for r in rows]
            # Every (company, url) this import will file as `website` -- one per qualifying ANSWER,
            # not one per company. Comparing against `row["website"]` alone was wrong: since step 3
            # `observations.py` writes a website row for each answer above the bar, so a runner-up
            # URL that also clears 0.35 is legitimate and permanent. The old comparison called all
            # 78 of those superseded on every run, deleted them, and let the upsert put them back --
            # net zero rows and pure churn, which is exactly what V17 existed to stop.
            website_ids: list[str] = []
            websites: list[str] = []
            for row in rows:
                for answer in row.get("answers") or []:
                    url = answer.get("url")
                    if url and observations.field_name_for(answer) == "website":
                        website_ids.append(row["company_id"])
                        websites.append(url)

            # How many rows this source already holds, so the sync run can say what the write
            # actually changed. Counted before anything is written: the upsert cannot tell us,
            # because ON CONFLICT DO UPDATE returns a row either way.
            cur.execute(
                """
                SELECT count(*) FROM public.company_field_observations
                WHERE source_id = %s AND company_id = ANY(%s::uuid[])
                """,
                (source_id, company_ids),
            )
            rows_before = cur.fetchone()[0]

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
            # Measured 2026-09-09: with the comparison made against every website-eligible answer
            # (916 winners plus 78 runners-up), this deletes nothing on a repeat run. It exists for
            # the run where a company's website genuinely changes.
            cur.execute(
                """
                DELETE FROM public.company_field_observations o
                WHERE o.source_id = %s
                  AND o.field_name = 'website'
                  AND o.company_id = ANY(%s::uuid[])
                  AND NOT EXISTS (
                    SELECT 1
                    FROM unnest(%s::uuid[], %s::text[]) AS f(company_id, website)
                    WHERE f.company_id = o.company_id
                      AND f.website = o.value_text
                  )
                """,
                (source_id, company_ids, website_ids, websites),
            )
            superseded = cur.rowcount

            # The whole write, for every company, is `observations.write_company_result`. Nothing is
            # duplicated here: it decides the field name per answer, files third-party pages under
            # `directory_url`, gates `website` at 0.35, and writes the tier ledger. This script's
            # remaining job is to turn a file row into the two arguments that function needs.
            records_created = 0
            for row in rows:
                was_created = observations.write_company_result(
                    cur,
                    company=_company_from_row(row),
                    result=row,
                    source_id=source_id,
                    sync_run_id=sync_run_id,
                    # From the file, not invented. `run` records which methods it actually ran per
                    # company, and the tier rows are only meaningful because of it.
                    stages_run=set(row.get("stages_run") or ()),
                )
                records_created += int(was_created)

            cur.execute(
                """
                SELECT count(*) FROM public.company_field_observations
                WHERE source_id = %s AND company_id = ANY(%s::uuid[])
                """,
                (source_id, company_ids),
            )
            rows_after = cur.fetchone()[0]

            finish_sync_run(
                cur,
                sync_run_id=sync_run_id,
                records_seen=len(rows),
                records_created=records_created,
                records_updated=len(rows) - records_created,
                records_failed=0,
            )
    finally:
        connection.close()

    log.info(
        "%d companies written. observations for this source: %d -> %d (%+d); "
        "%d new source records, %d superseded website values removed",
        len(rows), rows_before, rows_after, rows_after - rows_before,
        records_created, superseded,
    )
    if rows_after == rows_before and not superseded:
        log.info(
            "Every row was an update in place, so no canonical field was disturbed and "
            "`v_companies_enriched` is unchanged."
        )
    return rows_after != rows_before or bool(superseded)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS, help="path to results.jsonl")
    parser.add_argument("--dry-run", action="store_true", help="count and summarise, write nothing")
    parser.add_argument(
        "--skip-coordinator", action="store_true",
        help="write the observations but leave them unresolved for somebody else to resolve",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="write only the first N companies of the file, in file order. For inspecting the "
             "shape of the write before committing to all 14,443.",
    )
    args = parser.parse_args()

    rows = load_rows(args.results)
    log.info("%s: %d companies", args.results, len(rows))
    keep = importable(rows)
    if args.limit:
        keep = keep[: args.limit]
        log.info("--limit %d: writing a prefix of the file", args.limit)
    summarise(keep)

    if not keep:
        log.warning("nothing to import")
        return 1
    if args.dry_run:
        log.info("--dry-run: nothing written")
        return 0
    changed = write(keep)
    if args.skip_coordinator:
        log.info("--skip-coordinator: nothing is canonical until pipeline_coordinator runs.")
    elif changed:
        trigger_coordinator()
    else:
        log.info("nothing changed, so there is nothing for pipeline_coordinator to resolve.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
