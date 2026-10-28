#!/usr/bin/env python3
"""Convert the government's WZ spreadsheet into the checked-in wz_titles.yaml.

**WZ** stands for *Klassifikation der Wirtschaftszweige* — the official German industry
classification. A code names an industry: `56.11.1` is "Restaurants mit Bedienung", `64.2` is
"Beteiligungsgesellschaften". `public.companies` carries one for 8,483 of 14,443 companies, courtesy
of the Northdata export.

The classification has five levels, and this file needs the top two:

    Abschnitt    a letter        I    GASTGEWERBE                 22 of them
    Abteilung    two digits      56   Gastronomie                 87
    Gruppe       nn.n            56.1 Restaurants, Gaststätten…   287
    Klasse       nn.nn           56.11                            651
    Unterklasse  nn.nn.n         56.11.1                          983

A company's **WZ cluster** is its Abteilung and the Abschnitt above it, and both are derived with no
judgement whatsoever: the Abteilung is the first two characters of whatever code the company carries,
and the Abschnitt is the letter that owns that Abteilung. That is why this conversion exists — so the
cluster is a lookup in a checked-in file rather than anybody's opinion.

The output is checked into the repo on purpose, so nothing here depends on a spreadsheet in
somebody's Downloads folder. Run this once per WZ edition:

    python scripts/import_wz_titles.py "path/to/gliederung-klassifikation-wz-*.xlsx"

Needs `openpyxl`, which is deliberately **not** in `airflow/requirements.txt`: this runs on a laptop
once every few years and nothing in the pipeline imports it.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "airflow" / "dags" / "enrichment" / "wz_titles.yaml"

#: The sheet's own level numbers. 1 is an Abschnitt (a letter), 2 an Abteilung (two digits).
LEVEL_SECTION = "1"
LEVEL_DIVISION = "2"

HEADER = """\
# The official German industry classification: what every WZ code means.
#
# WZ stands for Klassifikation der Wirtschaftszweige. A code names an industry: "56.11.1" is
# "Restaurants mit Bedienung", "64.2" is "Beteiligungsgesellschaften". public.companies carries one
# for 8,483 of 14,443 companies.
#
# THIS IS A DIFFERENT CLASSIFICATION FROM branch_taxonomy.yaml, AND THE TWO MUST NOT BE MIXED.
# branch_taxonomy.yaml holds Berlin's 77 "Technologie- und Anwendungsfelder", which say what field a
# company WORKS IN. A WZ code says what it is REGISTERED AS. Neither can be derived from the other,
# and an earlier version of this project mapped WZ codes onto Berlin's fields and thereby made 2,632
# companies look like they had been assigned a technology field when nothing they published said so.
# The two now live in two separate columns. Keep it that way.
#
# THIS EDITION IS WZ 2025. All 100 of the most common codes in our table match it exactly, including
# codes like "70.10.4" and "62.10.9" that WZ 2008 does not contain at all. Anyone checking these
# against a 2008 table will mismatch silently, which is the worst way to be wrong.
#
# Two blocks:
#
#   sections:  Abschnitt (a letter) -> its title and the Abteilungen it owns. This is what makes a
#              company's section a lookup rather than a guess.
#   codes:     every code -> its official title.
#
# A code's level is its shape, so it is not stored: a letter is an Abschnitt, two digits an
# Abteilung, "nn.n" a Gruppe, "nn.nn" a Klasse, "nn.nn.n" the German Unterklasse.
#
# Checked in on purpose, so nothing depends on a spreadsheet in somebody's Downloads folder. Source:
# Statistisches Bundesamt, "Gliederung der Klassifikation der Wirtschaftszweige", rebuilt with
#
#     python scripts/import_wz_titles.py <the .xlsx>
#
# which is needed once per WZ edition, not once per run. Edit by hand in between: a clearer wording
# for a vague official title is an improvement, and a comment saying what a code covers in practice
# is worth more here than anywhere else.
#
# {sections} sections, {divisions} divisions, {codes} codes.
"""


def quote(value: str) -> str:
    """A YAML double-quoted scalar. Everything is quoted so nobody has to remember what needs it."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def read_sheet(xlsx_path: Path) -> list[tuple[str, str, str]]:
    """(level, code, title) for every row of the classification, in the sheet's own order."""
    import openpyxl

    workbook = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    sheet = next((workbook[name] for name in workbook.sheetnames if "Struktur" in name), None)
    if sheet is None:
        raise SystemExit(
            f"{xlsx_path.name} has no sheet with 'Struktur' in its name.\n"
            f"Found: {workbook.sheetnames}\n"
            "If the Statistisches Bundesamt renamed it, fix the match rather than the sheet."
        )
    rows: list[tuple[str, str, str]] = []
    for level, code, title in sheet.iter_rows(values_only=True):
        if not code or str(code).strip().lower() == "code":
            continue
        rows.append((str(level or "").strip(), str(code).strip(), str(title or "").strip()))
    if not rows:
        raise SystemExit(f"no codes found in {xlsx_path.name}")
    return rows


def render(rows: list[tuple[str, str, str]]) -> tuple[str, int, int, int]:
    """The YAML file, plus the three counts for its header.

    Written by hand rather than with yaml.dump, for the reason the header explains: yaml.dump cannot
    write a comment, and the comments are half of why this is a YAML file. It also emits flow lists,
    which diff as one unreadable line.
    """
    # Sections first, so the `sections:` block can list the Abteilungen each one owns. The sheet is
    # in classification order, so an Abteilung belongs to the last Abschnitt seen above it.
    sections: list[tuple[str, str, list[str]]] = []
    for level, code, title in rows:
        if level == LEVEL_SECTION:
            sections.append((code, title, []))
        elif level == LEVEL_DIVISION and sections:
            sections[-1][2].append(code)

    lines = ["sections:"]
    for code, title, divisions in sections:
        lines.append(f"  {quote(code)}:")
        lines.append(f"    title: {quote(title)}")
        lines.append(f"    divisions: [{', '.join(quote(d) for d in divisions)}]")

    lines.append("")
    lines.append("codes:")
    code_count = 0
    for level, code, title in rows:
        # An Abschnitt is a heading rather than a code a company carries, so it stays a comment here
        # and appears as data in the block above. The comment is what makes 2,000 lines readable.
        if level == LEVEL_SECTION:
            lines.append(f"\n  # ---- {code}  {title} ----")
            continue
        lines.append(f"  {quote(code)}: {quote(title)}")
        code_count += 1

    division_count = sum(len(d) for _, _, d in sections)
    return "\n".join(lines) + "\n", len(sections), division_count, code_count


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert the WZ spreadsheet to wz_titles.yaml.")
    parser.add_argument("xlsx_path", type=Path,
                        help="the Statistisches Bundesamt 'Gliederung der Klassifikation' .xlsx")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="where to write. Point it elsewhere to diff a new edition first.")
    args = parser.parse_args()

    if not args.xlsx_path.exists():
        raise SystemExit(f"no such file: {args.xlsx_path}")

    rows = read_sheet(args.xlsx_path)
    body, sections, divisions, codes = render(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        HEADER.format(sections=sections, divisions=divisions, codes=codes) + "\n" + body,
        encoding="utf-8",
    )

    print(f"Wrote {args.out}")
    print(f"  {sections} sections, {divisions} divisions, {codes} codes")
    print(f"  source: {args.xlsx_path}")
    print("  this file is checked in — nothing else needs the spreadsheet again")
    return 0


if __name__ == "__main__":
    sys.exit(main())
