#!/usr/bin/env python3
"""Load a Northdata JSONL export into `public.northdata_export`, whole.

## Why the table exists

The export has been arriving for months and only a handful of its columns were ever read. The rest
went into `companies.metadata` as loose JSON, where nothing looks. That has cost real work twice:
`register_status` sat unread until V13 promoted it, and `website` is doing the same right now —
`metadata ->> 'website'` holds 3,283 URLs, 2,394 of them for companies discovery has never found a
site for.

So this lands the export verbatim: scalar columns for the flat half, `raw` for everything else.
Read V14__northdata_export.sql for the reasoning behind the key and the two status vocabularies.

## What it does NOT do

It writes no observations and changes no canonical field. `v_companies_enriched` looks exactly the
same afterwards. The 3,283 websites become queryable, not used — deciding whether an unverified
third-party URL should outrank a page carrying the company's own register number is a separate
question, and the 207 companies where the two disagree are the evidence for it.

## Re-running

Idempotent per file: `ON CONFLICT (source_file, internal_id) DO UPDATE`. Loading a second export
adds rows rather than replacing the first, which is the point of keying on the file — two snapshots
a week apart is how "what did Northdata change" gets answered.

## Usage

    python scripts/import_northdata_export.py <export.jsonl> --dry-run
    python scripts/import_northdata_export.py <export.jsonl>
    python scripts/import_northdata_export.py <export.jsonl> --source-file 2026-04-01-osnabrueck
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Iterator

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "airflow" / "dags"))
sys.path.insert(0, str(REPO / "scripts"))

# Before source_enrichment_common is imported: it reads PG_DSN at import time, and its default
# `host=postgres` resolves only inside the compose network. Same line as the other host-side
# importers.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("import_northdata")

#: Column order for the INSERT. `raw` and `company_id` are appended by the writer.
COLUMNS = (
    "source_file", "internal_id", "name", "legal_form", "status",
    "register_id", "register_city", "register_unique_key", "eu_id",
    "street", "postal_code", "city", "state", "country", "address_formatted", "lat", "lng",
    "website", "email", "phone", "fax", "vat_id",
    "nace", "nace2025", "wz", "wz2025",
    "subject", "north_data_url", "last_update",
)


def rows_from(path: Path, source_file: str) -> Iterator[dict[str, Any]]:
    """One dict per record, scalar columns only. `raw` and `company_id` are added by the caller."""
    # `contacts` already knows the website hides at extras[].items[] under id 'url'; that knowledge
    # lives in one place rather than two.
    from northdata_jsonl_to_table import contacts

    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            address = record.get("address") or {}
            name = record.get("name") or {}
            register = record.get("register") or {}
            segments = record.get("segmentCodes") or {}
            found = contacts(record)
            yield {
                "source_file": source_file,
                "internal_id": record.get("id"),
                "name": name.get("name"),
                "legal_form": name.get("legalForm"),
                "status": record.get("status"),
                "register_id": register.get("id"),
                "register_city": register.get("city"),
                "register_unique_key": register.get("uniqueKey"),
                "eu_id": record.get("euId"),
                "street": address.get("street"),
                "postal_code": address.get("postalCode"),
                "city": address.get("city"),
                "state": address.get("state"),
                "country": address.get("country"),
                "address_formatted": address.get("formattedValue"),
                "lat": address.get("lat"),
                "lng": address.get("lng"),
                "website": found.get("url"),
                "email": found.get("email"),
                "phone": found.get("phone"),
                "fax": found.get("fax"),
                "vat_id": found.get("vatId"),
                "nace": segments.get("nace") or None,
                "nace2025": segments.get("nace2025") or None,
                "wz": segments.get("wz") or None,
                "wz2025": segments.get("wz2025") or None,
                "subject": record.get("subject"),
                "north_data_url": record.get("northDataUrl"),
                "last_update": record.get("lastUpdateTime"),
                "_raw": record,
            }


def write(rows: list[dict[str, Any]], batch: int) -> tuple[int, int]:
    """Upsert every row, then match `company_id`. Returns (written, matched)."""
    from psycopg2.extras import Json, execute_values

    from source_enrichment_common import get_connection

    columns = ", ".join(COLUMNS) + ", raw"
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c not in ("source_file", "internal_id"))
    sql = f"""
        INSERT INTO public.northdata_export ({columns})
        VALUES %s
        ON CONFLICT (source_file, internal_id) DO UPDATE
        SET {updates}, raw = EXCLUDED.raw, updated_at = now()
    """

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            for start in range(0, len(rows), batch):
                chunk = rows[start:start + batch]
                execute_values(
                    cur, sql,
                    [tuple(r[c] for c in COLUMNS) + (Json(r["_raw"]),) for r in chunk],
                )
                log.info("  %d/%d", min(start + batch, len(rows)), len(rows))

            # Matched in SQL rather than by loading 14,443 companies into Python. `internal_id` is
            # unique across the whole table; `unique_key` and `registration_number` are not.
            cur.execute(
                """
                UPDATE public.northdata_export n
                   SET company_id = c.id
                  FROM public.companies c
                 WHERE c.metadata ->> 'internal_id' = n.internal_id
                   AND n.source_file = %s
                   AND n.company_id IS DISTINCT FROM c.id
                """,
                (rows[0]["source_file"],),
            )
            cur.execute(
                "SELECT count(*), count(company_id) FROM public.northdata_export WHERE source_file = %s",
                (rows[0]["source_file"],),
            )
            written, matched = cur.fetchone()
    finally:
        connection.close()
    return written, matched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("export", type=Path, help="the Northdata .jsonl export")
    parser.add_argument("--source-file", help="provenance label; defaults to the file's name")
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true", help="count and summarise, write nothing")
    args = parser.parse_args()

    if not args.export.exists():
        raise SystemExit(f"{args.export} does not exist")

    source_file = args.source_file or args.export.name
    rows = list(rows_from(args.export, source_file))
    if not rows:
        raise SystemExit("no records read — is this a Northdata JSONL export?")

    log.info("%s: %d records, source_file=%s", args.export.name, len(rows), source_file)
    for field in ("website", "email", "phone", "vat_id"):
        n = sum(1 for r in rows if r[field])
        log.info("  with %-8s %6d  %5.1f%%", field, n, 100 * n / len(rows))
    missing_id = sum(1 for r in rows if not r["internal_id"])
    if missing_id:
        # Without it there is nothing to key on and nothing to match a company by.
        raise SystemExit(f"{missing_id} records have no `id` — refusing to load an unkeyable export")

    if args.dry_run:
        log.info("--dry-run: nothing written")
        return 0

    written, matched = write(rows, args.batch)
    log.info("\n%d rows in the table for this file, %d matched a company (%.1f%%)",
             written, matched, 100 * matched / written if written else 0)
    log.info("Nothing else changed: no observations, no canonical field, v_companies_enriched "
             "is untouched.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
