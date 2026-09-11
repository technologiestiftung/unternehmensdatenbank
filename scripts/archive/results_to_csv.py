#!/usr/bin/env python3
"""Turn a JSONL results file into a CSV you can read in a spreadsheet.

JSONL means "JSON Lines": one complete JSON object per line, no commas
between them, no wrapping list. The discovery DAGs write their results that
way because a crashed run still leaves every finished line readable.

That format is awkward to look at. This script flattens it into one CSV, which
Rainbow CSV in VS Code (or Excel, or LibreOffice) shows as a real table with
sortable, filterable columns.

Usage:

    # defaults to airflow/data/combined_discovery/results.jsonl -> results.csv
    python scripts/results_to_csv.py

    # or say exactly which files
    python scripts/results_to_csv.py path/to/input.jsonl path/to/output.csv

    # keep the "answers" column, which repeats the top-level fields once per
    # candidate website. Dropped by default because it makes rows unreadable.
    python scripts/results_to_csv.py --keep answers

The CSV is written with a BOM (three invisible marker bytes at the start).
Excel needs them to show "Osnabrück" instead of "OsnabrÃ¼ck". Everything else
ignores them.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = REPO_ROOT / "airflow" / "data" / "combined_discovery" / "results.jsonl"

# Columns that are technically valid but ruin the table: each one holds a list
# of objects that repeats what the top-level columns already say.
NOISY_COLUMNS = {"answers"}


def flatten(value: object) -> str:
    """Turn one JSON value into one spreadsheet cell.

    Numbers and strings pass through. A list of plain values becomes
    "a; b; c". Anything with structure inside it (a dict, or a list of dicts)
    is written back as compact JSON, so nothing is silently lost.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    if isinstance(value, list):
        if all(isinstance(item, (str, int, float, bool)) for item in value):
            return "; ".join(str(item) for item in value)
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def read_records(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"{path}:{line_number}: skipped unreadable line ({exc})", file=sys.stderr)
                continue
            if not isinstance(record, dict):
                print(f"{path}:{line_number}: skipped, not a JSON object", file=sys.stderr)
                continue
            records.append(record)
    return records


def column_order(records: list[dict], drop: set[str]) -> list[str]:
    """Every key any record has, in the order they first show up.

    Later runs sometimes add a field. Sorting alphabetically would scramble
    the readable order (company_name next to city), so first-seen wins.
    """
    columns: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record:
            if key in seen or key in drop:
                continue
            seen.add(key)
            columns.append(key)
    return columns


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", nargs="?", type=Path, default=DEFAULT_INPUT, help="the .jsonl file to read")
    parser.add_argument("output", nargs="?", type=Path, default=None, help="the .csv file to write (default: same name, .csv)")
    parser.add_argument(
        "--keep",
        action="append",
        default=[],
        metavar="COLUMN",
        help=f"keep a column that is dropped by default ({', '.join(sorted(NOISY_COLUMNS))}). Repeatable.",
    )
    args = parser.parse_args()

    input_path: Path = args.input
    if not input_path.exists():
        print(f"no such file: {input_path}", file=sys.stderr)
        return 1

    output_path: Path = args.output or input_path.with_suffix(".csv")

    records = read_records(input_path)
    if not records:
        print(f"{input_path} has no readable records", file=sys.stderr)
        return 1

    drop = NOISY_COLUMNS - set(args.keep)
    columns = column_order(records, drop)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig writes the BOM Excel needs; newline="" stops Windows from
    # doubling the line endings inside the CSV.
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for record in records:
            writer.writerow([flatten(record.get(column)) for column in columns])

    dropped = sorted(drop & {key for record in records for key in record})
    note = f", dropped {', '.join(dropped)}" if dropped else ""
    print(f"{len(records)} rows, {len(columns)} columns{note} -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
