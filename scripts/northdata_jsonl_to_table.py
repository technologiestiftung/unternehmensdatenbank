#!/usr/bin/env python3
"""Turn a Northdata export — JSONL or CSV — into one row per company, as Excel or CSV.

## Which input to give it

**The CSV export, if you want everything.** It carries 70 flat, named columns: the financials
(`Umsatz EUR`, `Gewinn EUR`, `Bilanzsumme EUR`, `Mitarbeiterzahl`), the five managers with their
roles, and the dates. That is the file the database was imported from.

**The JSONL export, if you want the curated set.** It holds the same companies but nested 230 leaf
paths deep — related companies, financial sheets, per-year history — so a full flatten is
unreadable. This script takes 20 documented fields from it instead. Both exports of the same date
agree: 3,283 websites, 3,677 emails, 2,977 phones.

## Why this exists

The Northdata JSON export is not the CSV export. It is deeply nested, 142 MB for Osnabrück, and
the fields worth having are not where anyone looks first. In particular there is **no `Website`
column**: a company's own web address lives at `extras[].items[]` where `id == "url"`, alongside
`email`, `phone`, `fax` and `vatId` from the same source block. Measured on the Osnabrück export
of 2026-03-23: 14,439 records, of which 3,286 (22.8%) carry a url and 3,639 an email.

That matters beyond convenience. The discovery pipeline had found 1,105 websites at the time this
was written; the export it was built from already contained three times as many, unread.

## What one row holds

Register identity and address, then the contact block, then the classification codes. Nested
values are taken by their documented path rather than by a generic flattener, because a generic
one produces 400 columns nobody reads and renames them whenever Northdata adds a field.

## Reading the CSV is not `csv.DictReader`

An unquoted comma inside `Gegenstand` shifts a row's fields. Reading the file naively gives 10,960
rows instead of 14,456, with dates and phone numbers landing in `Status`. `prepare_company_import`
already solves this and is reused here rather than restated: 99.0% of rows are clean 70-column
records, and the 145 that are not have their first 27 fields repaired by anchor (PLZ, Branchencode,
Eintragung Datum). A repaired row keeps those 27 and leaves the rest blank, because the shift makes
its tail unrecoverable — `_repaired` marks them so they are never mistaken for empty financials.

## Usage

    python scripts/northdata_jsonl_to_table.py <file.csv>                    # all 70 columns
    python scripts/northdata_jsonl_to_table.py <file.jsonl>                  # 20 curated columns
    python scripts/northdata_jsonl_to_table.py <file> --format csv           # semicolon + BOM
    python scripts/northdata_jsonl_to_table.py <file> --only-with-url
    python scripts/northdata_jsonl_to_table.py <file> -o out.xlsx

CSV is written semicolon-separated with a BOM, which is what German-locale Excel opens correctly;
pandas then needs `sep=';', encoding='utf-8-sig'`.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("northdata")

#: The contact fields Northdata files under `extras[].items[]`, keyed by that item's `id`.
CONTACT_IDS = ("url", "email", "phone", "fax", "vatId")

COLUMNS = [
    "id", "name", "legal_form", "status", "register_id", "register_city",
    "street", "postcode", "city", "country",
    "url", "email", "phone", "fax", "vat_id",
    "nace", "nace2025", "subject", "north_data_url", "last_update",
]


def contacts(record: dict) -> dict[str, str]:
    """The contact block, flattened. First non-empty value wins when a source repeats a field."""
    out: dict[str, str] = {}
    for extra in record.get("extras") or []:
        for item in extra.get("items") or []:
            key, value = item.get("id"), item.get("value")
            if key in CONTACT_IDS and value and key not in out:
                out[key] = value
    return out


def row_for(record: dict) -> dict[str, object]:
    address = record.get("address") or {}
    name = record.get("name") or {}
    register = record.get("register") or {}
    segments = record.get("segmentCodes") or {}
    found = contacts(record)
    return {
        "id": record.get("id"),
        "name": name.get("name"),
        "legal_form": name.get("legalForm"),
        "status": record.get("status"),
        "register_id": register.get("id"),
        "register_city": register.get("city"),
        "street": address.get("street") or address.get("formattedValue"),
        "postcode": address.get("postalCode") or address.get("zip"),
        "city": address.get("city"),
        "country": address.get("country"),
        "url": found.get("url", ""),
        "email": found.get("email", ""),
        "phone": found.get("phone", ""),
        "fax": found.get("fax", ""),
        "vat_id": found.get("vatId", ""),
        "nace": ", ".join(segments.get("nace") or []),
        "nace2025": ", ".join(segments.get("nace2025") or []),
        # One line: the register purpose runs to thousands of characters and Excel caps a cell at
        # 32,767. Truncated here rather than letting the write fail on one long row.
        "subject": (record.get("subject") or "")[:2000],
        "north_data_url": record.get("northDataUrl"),
        "last_update": record.get("lastUpdateTime"),
    }


def read_csv_export(path: Path) -> tuple[list[str], list[dict[str, object]]]:
    """Every column of the CSV export, with the shifted rows repaired.

    Returns (columns, rows). `_repaired` is appended as the last column so a row whose tail could
    not be recovered is visible rather than looking like a company with no financials.
    """
    import sys as _sys

    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from prepare_company_import import (  # noqa: E402
        HEADER_PREFIX,
        parse_clean_csv_line,
        prefix_is_aligned,
        repair_prefix_row,
    )

    with path.open(encoding="utf-8-sig") as handle:
        header = parse_clean_csv_line(handle.readline())
        columns = header + ["_repaired"]
        rows: list[dict[str, object]] = []
        repaired = 0
        for line in handle:
            if not line.strip():
                continue
            fields = parse_clean_csv_line(line)
            if prefix_is_aligned(fields):
                values = (fields + [""] * len(header))[: len(header)]
                was_repaired = False
            else:
                try:
                    fixed = repair_prefix_row(fields)
                except ValueError:
                    continue  # unrepairable: the anchors are gone, so any mapping would be a guess
                values = fixed[: len(HEADER_PREFIX)] + [""] * (len(header) - len(HEADER_PREFIX))
                was_repaired = True
                repaired += 1
            row = dict(zip(header, values))
            row["_repaired"] = was_repaired
            rows.append(row)
    log.info("%d rows, %d repaired", len(rows), repaired)
    return columns, rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("jsonl", type=Path, help="the .jsonl or .csv export")
    parser.add_argument("-o", "--out", type=Path)
    parser.add_argument("--format", choices=("xlsx", "csv"), default="xlsx")
    parser.add_argument("--only-with-url", action="store_true",
                        help="keep only companies that carry a website")
    args = parser.parse_args()

    if not args.jsonl.exists():
        raise SystemExit(f"{args.jsonl} does not exist")

    if args.jsonl.suffix.lower() == ".csv":
        columns, rows = read_csv_export(args.jsonl)
        url_column, contact_columns = "Website", ("Website", "E-Mail", "Tel.")
    else:
        columns, url_column, contact_columns = COLUMNS, "url", ("url", "email", "phone")
        rows = []
        with args.jsonl.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(row_for(json.loads(line)))

    if args.only_with_url:
        before = len(rows)
        rows = [r for r in rows if str(r.get(url_column) or "").strip()]
        log.info("%d rows (%d without a website skipped)", len(rows), before - len(rows))
    else:
        log.info("%d rows", len(rows))

    for field in contact_columns:
        n = sum(1 for r in rows if str(r.get(field) or "").strip())
        log.info("  with %-8s %6d  %5.1f%%", field, n, 100 * n / len(rows) if rows else 0)

    out = args.out or args.jsonl.with_suffix(".xlsx" if args.format == "xlsx" else ".csv")
    if args.format == "xlsx":
        from openpyxl import Workbook

        book = Workbook(write_only=True)  # write-only: 14k rows without holding a full DOM
        sheet = book.create_sheet("companies")
        sheet.append(columns)
        for row in rows:
            sheet.append([row.get(c, "") for c in columns])
        book.save(out)
    else:
        with out.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)
    log.info("wrote %s", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
