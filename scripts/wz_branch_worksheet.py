#!/usr/bin/env python3
"""Turn WZ codes into branches, with a human doing the deciding.

A **WZ code** is the official German industry code — *Klassifikation der Wirtschaftszweige*. `56.1`
means restaurants, `62.10.9` software, `64.2` holding companies. The Northdata export filled it in
for 8,483 of our 14,443 companies, and it is the cheapest industry signal we have: no page to
download, no model to run, and it works for the 1,943 companies that have no describing text at all.

A **branch** is Berlin's industry-plus-technology-field label, from
`airflow/dags/combined_discovery/branch_taxonomy.yaml`.

Nothing can map one to the other automatically — only 3 of the 78 rows in Berlin's sheet even hint at
a code. So this script splits the job in two:

    write   a spreadsheet of the most common codes, with example company names, for a human to fill in
    apply   read that spreadsheet back and write wz_to_branch.yaml

Why the most common codes only: 756 distinct codes exist, but the **100 most common cover 5,845 of
the 8,483 coded companies — 69%**. The remaining 656 codes are a later afternoon and need no code
change.

**"No branch applies" is a real answer and has its own column.** The largest codes here are holding
companies (508), membership organisations (383) and property management (369), and Berlin's list —
built for a technology economy — has no home for any of them. Writing `none` records that decision
so nobody later files 508 holding companies under Finanzwirtschaft to make a number look better.

    python scripts/wz_branch_worksheet.py write --top 100
    # fill in the `branch` column in airflow/data/combined_discovery/wz_branch_todo.csv
    python scripts/wz_branch_worksheet.py apply
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

# Running on the host means Postgres is on localhost:5433, not on the compose network. Same
# reasoning as scripts/run_combined_discovery.py, which explains it at length.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wz_branch")

OUT_DIR = Path(
    os.getenv("COMBINED_OUT_DIR")
    or (Path("/opt/airflow/data") if Path("/opt/airflow/data").is_dir() else REPO_ROOT / "airflow" / "data")
) / "combined_discovery"

WORKSHEET_PATH = OUT_DIR / "wz_branch_todo.csv"
MAP_PATH = DAGS_DIR / "combined_discovery" / "wz_to_branch.yaml"
TAXONOMY_PATH = DAGS_DIR / "combined_discovery" / "branch_taxonomy.yaml"

#: The value that means "this industry has no branch in Berlin's list". Spelled out rather than left
#: blank, because a blank cell is indistinguishable from an unanswered row.
NO_BRANCH = "none"

COLUMNS = [
    "wz_code",
    "companies",
    "example_1",
    "example_2",
    "example_3",
    # The two a human fills in.
    #   branch  a branch name from branch_taxonomy.yaml, or `none` if no branch applies
    #   note    why. Never parsed, only read — but it ends up as a comment in the YAML.
    "branch",
    "note",
]

CODE_SQL = """
SELECT wz_code, count(*) AS n,
       (array_agg(company_name ORDER BY company_name))[1:3] AS examples
FROM public.companies
WHERE wz_code IS NOT NULL AND company_name IS NOT NULL
GROUP BY wz_code
ORDER BY n DESC, wz_code
"""


def load_codes() -> list[dict[str, Any]]:
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(CODE_SQL)
            rows = cur.fetchall()
    finally:
        connection.close()
    return [{"wz_code": r[0], "companies": r[1], "examples": list(r[2] or [])} for r in rows]


def load_branch_names() -> set[str]:
    """Every branch name the taxonomy knows, so a typo in the worksheet fails loudly."""
    import yaml

    data = yaml.safe_load(TAXONOMY_PATH.read_text(encoding="utf-8")) or {}
    return set((data.get("branches") or {}).keys())


def read_worksheet() -> dict[str, dict[str, str]]:
    """What is already in the worksheet, keyed by code. Missing file is fine — it means "nothing"."""
    if not WORKSHEET_PATH.exists():
        return {}
    with WORKSHEET_PATH.open(encoding="utf-8-sig", newline="") as handle:
        return {(row.get("wz_code") or ""): row for row in csv.DictReader(handle)}


def write(top: int) -> None:
    """Write (or extend) the worksheet, never losing a decision already made.

    Codes are taken in descending company count, so filling in the file top-down spends the effort
    where it buys the most companies. A code that drops out of the top N but already has a branch
    written against it stays in the file: the work is the human's, not this script's.
    """
    codes = load_codes()
    existing = read_worksheet()
    decided = sum(1 for row in existing.values() if (row.get("branch") or "").strip())
    if existing:
        log.info("%s already has %d rows, %d decided", WORKSHEET_PATH.name, len(existing), decided)

    chosen = codes[:top]
    kept = [
        c for c in codes[top:]
        if (existing.get(c["wz_code"], {}).get("branch") or "").strip()
    ]

    out: list[dict[str, Any]] = []
    for code in chosen + kept:
        was = existing.get(code["wz_code"], {})
        examples = code["examples"] + ["", "", ""]
        out.append(
            {
                "wz_code": code["wz_code"],
                "companies": code["companies"],
                "example_1": examples[0],
                "example_2": examples[1],
                "example_3": examples[2],
                # Carried over verbatim. This is the human's work.
                "branch": was.get("branch", ""),
                "note": was.get("note", ""),
            }
        )

    WORKSHEET_PATH.parent.mkdir(parents=True, exist_ok=True)
    with WORKSHEET_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(out)

    covered = sum(c["companies"] for c in chosen)
    total = sum(c["companies"] for c in codes)
    print(f"\nWrote {len(out)} rows to {WORKSHEET_PATH}")
    print(f"{len(codes)} distinct codes exist, covering {total} companies.")
    print(f"The {len(chosen)} in this file cover {covered} of them ({100.0 * covered / total:.0f}%).")
    print(f"\nFill in `branch` with a name from {TAXONOMY_PATH.name}, or `{NO_BRANCH}` when no branch")
    print("applies — holding companies, landlords and associations genuinely have none, and saying")
    print("so is the point. Put the reason in `note`; it becomes a comment in the YAML.")
    print(f"\nThen: python {Path(__file__).name} apply")


def apply() -> None:
    """Read the worksheet and write the mapping. Refuses to write anything it cannot verify."""
    if not WORKSHEET_PATH.exists():
        raise SystemExit(f"no {WORKSHEET_PATH}; run `write` first")
    known = load_branch_names()
    rows = list(read_worksheet().values())

    mapped: list[dict[str, Any]] = []
    unknown: list[tuple[str, str]] = []
    for row in rows:
        branch = (row.get("branch") or "").strip()
        if not branch:
            continue
        if branch.lower() == NO_BRANCH:
            mapped.append({**row, "branch": None})
        elif branch in known:
            mapped.append({**row, "branch": branch})
        else:
            unknown.append((row.get("wz_code") or "", branch))

    if unknown:
        print(f"\n!! {len(unknown)} row(s) name a branch that is not in {TAXONOMY_PATH.name}:")
        for code, branch in unknown[:15]:
            print(f"     {code}: {branch!r}")
        raise SystemExit(
            "Nothing was written. Fix the spelling, or add the branch to the taxonomy first — a\n"
            "mapping to a branch that does not exist would classify companies into nowhere."
        )
    if not mapped:
        raise SystemExit(f"no decided rows in {WORKSHEET_PATH.name}; nothing to write")

    mapped.sort(key=lambda r: (-int(r.get("companies") or 0), r["wz_code"]))
    MAP_PATH.write_text(render(mapped), encoding="utf-8")

    with_branch = sum(1 for r in mapped if r["branch"])
    companies = sum(int(r.get("companies") or 0) for r in mapped)
    none_companies = sum(int(r.get("companies") or 0) for r in mapped if not r["branch"])
    print(f"\nWrote {len(mapped)} codes to {MAP_PATH}")
    print(f"  {with_branch} map to a branch, {len(mapped) - with_branch} to \"no branch applies\"")
    print(f"  they cover {companies} companies, of which {none_companies} get no branch")


HEADER = """\
# WZ code -> branch. The cheapest industry signal in this repo.
#
# A WZ code is the official German industry code (Klassifikation der Wirtschaftszweige): "56.1" is
# restaurants, "62.10.9" software, "64.2" holding companies. It is already in public.companies for
# 8,483 companies, so this mapping costs one SQL query and no downloads — and it reaches the 1,943
# companies that have no describing text at all.
#
# Generated from airflow/data/combined_discovery/wz_branch_todo.csv by
# scripts/wz_branch_worksheet.py apply. Edit the worksheet and re-run rather than editing here, or
# the next `apply` overwrites your change.
#
# MATCHED BY LONGEST PREFIX. Our codes arrive at three different depths — "64.2", "64.21", "94.99"
# and "94.99.9" all occur — so a company's "94.99.9" matches the entry for "94.99" when there is no
# more specific one. A flat exact-match table would miss most of the table.
#
# `null` means NO BRANCH APPLIES: the company really is a holding company or a housing association,
# and Berlin's list, built for a technology economy, has no home for it. That is a real answer and
# is reported separately from "we could not tell" — collapsing the two is how a blank turns into a
# fact.
"""


def render(rows: list[dict[str, Any]]) -> str:
    def quote(value: str) -> str:
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

    lines = [HEADER, "mappings:"]
    for row in rows:
        note = (row.get("note") or "").strip()
        comment = f"  # {row['companies']} companies"
        if note:
            comment += f" — {note}"
        value = quote(row["branch"]) if row["branch"] else "null"
        lines.append(f"  {quote(row['wz_code'])}: {value}{comment}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="WZ code to branch, with a human in the middle.")
    sub = parser.add_subparsers(dest="command", required=True)
    p_write = sub.add_parser("write", help="write the worksheet (merges with what is decided)")
    p_write.add_argument(
        "--top", type=int, default=100,
        help="how many codes, most common first (default 100, which covers 69%% of coded companies)",
    )
    sub.add_parser("apply", help="read the worksheet back and write wz_to_branch.yaml")

    args = parser.parse_args()
    if args.command == "write":
        write(args.top)
    else:
        apply()
    return 0


if __name__ == "__main__":
    sys.exit(main())
