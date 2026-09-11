#!/usr/bin/env python3
"""Load the two classification files into the database, so the industry columns can be served.

## Why this exists

`classify` and `classify-purpose` write `branches.jsonl` and `purpose_branches.jsonl` and stop.
V20 reserved `technologiefeld` and `purpose_field` as columns of `v_companies_enriched`, added the
`branch_classifier` source and widened the observation upsert key to ten field names -- and then
nothing wrote them, so both columns read NULL on all 14,443 rows and anyone consuming the platform
could not reach either industry column except by opening a file on somebody's laptop.

This closes that gap. It decides no winners -- `pipeline_coordinator` does that, and it is the only
thing that may.

## Why a separate importer rather than writing inside `classify`

`classify` re-scores every company on every run and takes two minutes with no network, which is what
makes arguing with the taxonomy cheap. Attaching a 5,660-row write to that loop would put a database
consequence behind a command run dozens of times a day.

The sharper reason is `--limit`. A keyword's weight depends on **how many companies use that word**,
so `classify --limit 1000` is not a subset of a full run -- it is different answers. A write reachable
only through the finished file cannot land a partially-scored population. Same split, and the same
reasoning, as `run_combined_discovery.py` / `import_combined_discovery.py`, which this mirrors.

## What it writes

Every row goes through `combined_discovery/branch_observations.py`, so the shape of the write is
decided in one place. Per company, at most one observation per column:

- `technologiefeld` -- outcome `assigned`, 403 rows at confidence 0.60
- `technologiefeld_withheld` -- the field the website rule refused, 535 rows
- `purpose_field` -- outcome in `ASSIGNED_OUTCOMES`, 4,705 rows at 0.60 / 0.55 / 0.52 by `decided_by`
- `purpose_field_withheld` -- the field the 40-character floor cost, 17 rows

**Nothing resolves the two `_withheld` names**, by design: a field name no `resolve_*` function reads
cannot be published by any threshold change.

Nothing is re-scored here. A number invented at import time would be a second opinion competing with
the first, and there would be no way to tell later which one a canonical field came from.

## Re-running

Idempotent, and idempotent *without deleting what it is about to rewrite*. V20's unique key on
`(company_id, field_name, source_id, value_text)` lets `insert_text_observation` update a row in
place, so a company whose field has not changed keeps the same observation row -- and therefore keeps
its canonical field, because `company_canonical_field_sources` points at that row and cascades when
it is deleted.

There is deliberately **no superseded-delete** here, unlike `import_combined_discovery.py`. That
script needs one because a company's website genuinely changes and the stale URL would out-rank the
correction on confidence alone. A classification is a full re-score of a rewritten file: if a
company's field changes, the new value inserts as a new row and the resolver picks between them on
confidence, which is the behaviour you want -- the old answer stays visible as evidence.

## Usage

    python scripts/import_branch_classification.py --dry-run          # forecast only, writes nothing
    python scripts/import_branch_classification.py --limit 50         # inspect the shape first
    python scripts/import_branch_classification.py --skip-coordinator # write, but do NOT publish
    python scripts/import_branch_classification.py                    # write and resolve

`--skip-coordinator` is the staged path: the observations land, `v_companies_enriched` still reads
NULL, and publishing stays a separate, deliberate decision -- one `pipeline_coordinator` run.
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
# default (`host=postgres port=5432`) is right inside a container and wrong outside one, so supply a
# host-shaped default without overriding an explicit PG_DSN. Same line as
# import_combined_discovery.py, and it has to be set before source_enrichment_common is imported --
# that module reads PG_DSN at import time, which is why the import lives inside `write()`.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("import_branch_classification")

OUT_DIR = REPO / "airflow" / "data" / "combined_discovery"
DEFAULT_BRANCHES = OUT_DIR / "branches.jsonl"
DEFAULT_PURPOSES = OUT_DIR / "purpose_branches.jsonl"

SOURCE_KEY = "branch_classifier"
SOURCE_NAME = "Branch Classifier (Berlin 77-field taxonomy)"


def load_rows(path: Path, *, what: str) -> dict[str, dict[str, Any]]:
    """Every company in one file, keyed by id, last record winning.

    `classify` deletes and rewrites its whole file each run, so duplicates should not occur -- but
    `classify-purpose` appends per company, and a run resumed after a prompt change can leave two
    lines for one company. The later line is the newer verdict.
    """
    if not path.exists():
        raise SystemExit(
            f"{path} does not exist. Run `python scripts/run_combined_discovery.py {what}` first."
        )
    by_company: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        if row.get("company_id"):
            by_company[row["company_id"]] = row
    return by_company


def writable(
    branches: dict[str, dict[str, Any]], purposes: dict[str, dict[str, Any]]
) -> list[tuple[str, dict[str, Any] | None, dict[str, Any] | None]]:
    """The companies with something to write, in company-id order.

    A company with no field in either column contributes nothing -- not even a source record. 9,568
    of 14,443 are in that state, and the two files already record why for every one of them.

    The filter itself lives in `branch_observations.planned_writes`, which gates on the OUTCOME and
    never on the truthiness of the value. That is not a detail: the value is the literal string
    `"NONE"` on 23,778 rows across the two files, and writing those would give the resolver a
    canonical industry field of `'NONE'` for thousands of companies.
    """
    from combined_discovery import branch_observations as bo

    out = []
    for company_id in sorted(set(branches) | set(purposes)):
        branch_row = branches.get(company_id)
        purpose_row = purposes.get(company_id)
        if bo.planned_writes(branch_row, purpose_row):
            out.append((company_id, branch_row, purpose_row))
    return out


def summarise(rows: list[tuple[str, dict[str, Any] | None, dict[str, Any] | None]]) -> None:
    """What is about to be written, by field name and by confidence. The forecast a dry run prints."""
    from combined_discovery import branch_observations as bo

    by_field: Counter[str] = Counter()
    by_conf: Counter[tuple[str, float]] = Counter()
    scored = unscored = 0
    for _company_id, branch_row, purpose_row in rows:
        for field_name, value, confidence, metadata in bo.planned_writes(branch_row, purpose_row):
            by_field[field_name] += 1
            by_conf[(field_name, confidence)] += 1
            if value == "NONE":  # cannot happen; see writable() -- asserted, not assumed
                raise SystemExit(f"refusing to write the literal 'NONE' for {_company_id}")
            if field_name == "purpose_field":
                scored += int(metadata["score"] is not None)
                unscored += int(metadata["score"] is None)

    log.info("%d companies to write, %d observations", len(rows), sum(by_field.values()))
    for field_name in (
        "technologiefeld", "technologiefeld_withheld", "purpose_field", "purpose_field_withheld"
    ):
        log.info("  %-28s %6d", field_name, by_field[field_name])
    log.info("  confidence split:")
    for (field_name, confidence), count in sorted(by_conf.items()):
        log.info("    %-28s %.2f  %6d", field_name, confidence, count)
    log.info(
        "  purpose_field metadata.score: %d measured, %d None (a model row has no weight to sum)",
        scored, unscored,
    )
    log.info(
        "  NOTHING resolves technologiefeld_withheld or purpose_field_withheld -- they are stored "
        "as evidence and can never become a canonical field."
    )


def trigger_coordinator() -> bool:
    """Resolve what was just written into canonical fields. Returns True if it ran.

    A direct call to `run_pipeline_coordinator`, not an Airflow trigger, for the reasons set out at
    length in `run_combined_discovery.trigger_coordinator` -- one implementation, two callers, no
    scheduler required, and no `dag_run` row to show for it. Deliberately duplicated here rather than
    imported: this script has no other reason to load 4,300 lines of runner.

    Never fatal. The observations are already written; leaving them unresolved is exactly the state
    `--skip-coordinator` asks for, and the log says so.
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


def _company_from_rows(
    company_id: str, branch_row: dict[str, Any] | None, purpose_row: dict[str, Any] | None
) -> Any:
    """A `CompanyCandidate` built from the files, so no read of `public.companies` is needed.

    `write_company_classification` only ever touches `.id` and `.company_name`, both of which are
    top-level keys on every row of both files. The address fields are required by the dataclass and
    are passed as None deliberately: this source classifies text somebody else already fetched, so it
    has no address of its own to assert and inventing one would put unsourced data on the record.
    """
    from source_enrichment_common import CompanyCandidate

    row = branch_row or purpose_row or {}
    return CompanyCandidate(
        id=company_id,
        company_name=row.get("company_name") or "",
        address=None,
        postcode=None,
        city=None,
        registration_number=None,
    )


def write(rows: list[tuple[str, dict[str, Any] | None, dict[str, Any] | None]]) -> bool:
    """Write every planned observation in one transaction. Returns True if anything changed."""
    from combined_discovery import branch_observations as bo
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
                source_type="classifier",
                # Merged onto V20's row by `ensure_source`'s `metadata || EXCLUDED.metadata`. The
                # policy here CORRECTS V20's: model_direct was 0.50, exactly on the resolver's bar,
                # and is now 0.52. That is data, not schema, so it needs no migration.
                metadata={
                    "script": "scripts/import_branch_classification.py",
                    "writes": [
                        "technologiefeld", "purpose_field",
                        "technologiefeld_withheld", "purpose_field_withheld",
                    ],
                    "confidence_basis": "policy_not_measurement",
                    "confidence_by_decided_by": dict(bo._CONFIDENCE),
                    "note": (
                        "Confidence is policy, not measurement: these columns score by unbounded "
                        "keyword weight and 4,095 of 4,705 purpose fields have no score at all. The "
                        "real score is in each observation's metadata.score, and is None where none "
                        "was ever measured. model_direct raised 0.50 -> 0.52 so it no longer sits "
                        "exactly on the resolver's _FIELD_MIN_CONF."
                    ),
                },
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)

            company_ids = [company_id for company_id, _b, _p in rows]
            cur.execute(
                """
                SELECT count(*) FROM public.company_field_observations
                WHERE source_id = %s AND company_id = ANY(%s::uuid[])
                """,
                (source_id, company_ids),
            )
            rows_before = cur.fetchone()[0]

            records_created = written = 0
            for company_id, branch_row, purpose_row in rows:
                was_created, n = bo.write_company_classification(
                    cur,
                    company=_company_from_rows(company_id, branch_row, purpose_row),
                    branch_row=branch_row,
                    purpose_row=purpose_row,
                    source_id=source_id,
                    sync_run_id=sync_run_id,
                )
                records_created += int(was_created)
                written += n

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
        "%d companies written, %d observations attempted. Observations for this source: "
        "%d -> %d (%+d); %d new source records",
        len(rows), written, rows_before, rows_after, rows_after - rows_before, records_created,
    )
    if rows_after == rows_before:
        log.info(
            "Every row was an update in place, so no canonical field was disturbed and "
            "`v_companies_enriched` is unchanged."
        )
    return rows_after != rows_before


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--branches", type=Path, default=DEFAULT_BRANCHES,
                        help="path to branches.jsonl (column 1)")
    parser.add_argument("--purposes", type=Path, default=DEFAULT_PURPOSES,
                        help="path to purpose_branches.jsonl (column 3)")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the forecast and write nothing")
    parser.add_argument(
        "--skip-coordinator", action="store_true",
        help="write the observations but leave them unresolved. Nothing reaches "
             "v_companies_enriched until pipeline_coordinator runs, so publishing stays a separate "
             "decision.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="write only the first N companies, in company-id order. For inspecting the shape of "
             "the write before committing to all of them.",
    )
    args = parser.parse_args()

    branches = load_rows(args.branches, what="classify")
    purposes = load_rows(args.purposes, what="classify-purpose")
    log.info("%s: %d companies", args.branches.name, len(branches))
    log.info("%s: %d companies", args.purposes.name, len(purposes))

    rows = writable(branches, purposes)
    if args.limit:
        rows = rows[: args.limit]
        log.info("--limit %d: writing a prefix in company-id order", args.limit)
    summarise(rows)

    if not rows:
        log.warning("nothing to import")
        return 1
    if args.dry_run:
        log.info("--dry-run: nothing written")
        return 0

    changed = write(rows)
    if args.skip_coordinator:
        log.info(
            "--skip-coordinator: the observations are written and UNRESOLVED. "
            "v_companies_enriched still reads NULL for both columns. Run pipeline_coordinator when "
            "you want them published."
        )
    elif changed:
        trigger_coordinator()
    else:
        log.info("nothing changed, so there is nothing for pipeline_coordinator to resolve.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
