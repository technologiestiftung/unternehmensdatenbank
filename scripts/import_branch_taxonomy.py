#!/usr/bin/env python3
"""Turn Berlin's branch spreadsheet into the YAML the classifier reads.

A **branch** (German *Branche*) is an industry plus the technology field it sits in —
"Bahnindustrie", "Gastgewerbe", "Additive Fertigung". Berlin's economic-development people keep a
list of 77 of them, each with a handful of **keywords**: words that suggest a company belongs there.
"Schienenfahrzeuge" suggests Bahnindustrie.

The list arrives as a spreadsheet. This script converts it **once**. After that the YAML is the
source of truth: it is edited by hand, it carries comments explaining why a keyword was dropped, and
a `git diff` of it shows one keyword per line. Re-running with `--out` writes somewhere new, so a
freshly exported sheet can be diffed against the live file instead of silently overwriting those
hand edits.

Three things about the spreadsheet bite anyone who reads it naively, and each is handled below:

1. **It is cp1252, not UTF-8.** Decoding it as UTF-8 fails on byte 227, and `errors="replace"` turns
   every umlaut into a question mark. "Ernaehrungsindustrie" written with an umlaut stops matching
   anything at all.
2. **Non-breaking spaces sit inside words.** "Advanced\xa0Materials", "Autonomes,\xa0vernetzes
   \xa0Fahren". U+00A0 looks exactly like a space and compares as a different character, so a branch
   name copied from the sheet would never equal the same name typed by hand. NFKC normalisation
   turns it into a real space; then runs of whitespace collapse.
3. **One branch appears twice.** "Automobilwirtschaft" has two rows, with two different Berlin owners
   and two different keyword sets. The keywords are merged and both owners kept — dropping either row
   would lose 14 keywords, or an owner nobody could then ask about.

Usage:

    python scripts/import_branch_taxonomy.py "C:/.../ALLE_Abfrage_Labels(IIIB).csv"
    python scripts/import_branch_taxonomy.py input.csv --out /tmp/new_taxonomy.yaml
"""
from __future__ import annotations

import argparse
import collections
import csv
import io
import re
import sys
import unicodedata
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "airflow" / "dags" / "combined_discovery" / "branch_taxonomy.yaml"

#: The sheet's own column headings. Missing ones stop the run rather than writing an empty file: a
#: taxonomy that silently lost its keywords classifies nothing and looks like a scoring bug.
COL_BRANCH = "Branche / Technologie- und Anwendungsfeld"
COL_UNIT = "FG SenWEB"
COL_PARTNER = "FB Berlin Partner"
COL_KEYWORDS = "Keywords"
COL_RELATED = "Oft verwandte Technologiethemen (siehe erste Spalte)"

#: The sheet separates keywords with commas, and a few rows use semicolons instead.
KEYWORD_SPLIT = re.compile(r"[,;]")

#: Two characters is the floor. "AI", "5G" and "KI" are real keywords; one character is noise.
MIN_KEYWORD_LENGTH = 2


def normalize(value: str | None) -> str:
    """One spelling for text that arrives in several.

    NFKC folds the non-breaking spaces and the compatibility characters a spreadsheet accumulates
    into their ordinary equivalents, then runs of whitespace become single spaces. Applied to every
    field, so what lands in the YAML is what a person would type.
    """
    text = unicodedata.normalize("NFKC", value or "")
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def read_rows(path: Path) -> list[dict[str, str]]:
    """The spreadsheet, decoded and split. See point 1 in the module docstring for the encoding."""
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")
    rows = list(csv.DictReader(io.StringIO(text), delimiter=";"))
    if not rows:
        raise SystemExit(f"{path} has no data rows")
    missing = [c for c in (COL_BRANCH, COL_KEYWORDS) if c not in rows[0]]
    if missing:
        raise SystemExit(
            f"{path} is missing the column(s) {missing}.\nFound: {list(rows[0])}\n"
            "The sheet's headings changed — fix the COL_* constants rather than the sheet."
        )
    return rows


def split_keywords(cell: str | None) -> list[str]:
    """The keywords out of one cell, in the order written, without the empties.

    "V2X, , V2I (Vehicle-to-Infrastructure)" really does occur, hence the length check.
    """
    out: list[str] = []
    for part in KEYWORD_SPLIT.split(cell or ""):
        keyword = normalize(part)
        if len(keyword) >= MIN_KEYWORD_LENGTH:
            out.append(keyword)
    return out


class Branch:
    """One branch, possibly assembled from more than one row of the sheet."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.units: list[str] = []
        self.partners: list[str] = []
        self.related: list[str] = []
        #: Lower-cased keyword -> the spelling to write out. Keeps the first spelling seen, and makes
        #: the de-duplication case-insensitive: the sheet holds both "Automotive" and "automotive".
        self.keywords: dict[str, str] = {}

    def absorb(self, row: dict[str, str]) -> None:
        for field, target in ((COL_UNIT, self.units), (COL_PARTNER, self.partners)):
            value = normalize(row.get(field))
            if value and value not in target:
                target.append(value)
        for related in split_keywords(row.get(COL_RELATED)):
            if related not in self.related:
                self.related.append(related)
        for keyword in split_keywords(row.get(COL_KEYWORDS)):
            self.keywords.setdefault(keyword.lower(), keyword)


def build(rows: list[dict[str, str]]) -> dict[str, Branch]:
    """Branch name -> Branch, with the duplicate rows merged. See point 3 in the docstring."""
    branches: dict[str, Branch] = {}
    for row in rows:
        name = normalize(row.get(COL_BRANCH))
        if not name:
            continue
        branches.setdefault(name, Branch(name)).absorb(row)
    return dict(sorted(branches.items()))


def quote(value: str) -> str:
    """A YAML double-quoted scalar.

    Everything is quoted rather than only what needs it. Branch names contain "&", "/" and ":" —
    "Gesundheitsversorgung & Pflege", "Branche / Technologie" — and each of those means something to
    a YAML parser when unquoted. Quoting unconditionally means nobody has to remember which.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return '"' + escaped + '"'


HEADER = """\
# Berlin's branch list: which industry a company belongs to, and the words that say so.
#
# A BRANCH is an industry plus its technology field. A KEYWORD is a word on a company's website, or
# in its register purpose, that suggests that branch.
#
# Generated once from {source} by scripts/import_branch_taxonomy.py.
# AFTER THAT THIS FILE IS THE SOURCE OF TRUTH — edit it by hand. Re-running the importer with --out
# writes elsewhere, so a newly exported sheet can be diffed against this one rather than
# overwriting the edits and comments below.
#
# {branch_count} branches, {keyword_total} keyword entries, {distinct} distinct keywords.
#
# Keyed by branch, not by keyword, because a keyword belongs to several branches ("Sensorik" to
# seven of them) and because a person editing this asks "what belongs to Bahnindustrie?". The
# keyword -> branches index is built in memory when the file loads.
#
# Matching is case-insensitive and needs a word boundary on both sides. It deliberately does NOT
# match inside a German compound: allowing that made "Sicherheit" hit 139 pages instead of 90,
# because it hides inside Arbeitssicherheit and Sicherheitstechnik — and it belongs to Defence
# Tech, which briefly became Osnabrueck's largest industry.
#
# A keyword shared by many branches is already worth less: its weight is divided across the
# branches that claim it, and again by how many companies use the word at all. So do NOT delete a
# keyword for being too common — that is measured from the data rather than from anyone's opinion.
# Delete a keyword only when it is WRONG for its branch, and say so in a comment.
"""


def render(branches: dict[str, Branch], source: Path) -> str:
    """The YAML file, written by hand rather than with yaml.dump.

    yaml.dump cannot write a comment, and the comments are half the reason this is a YAML file
    rather than a database table. It also emits flow lists — `[a, b, c]` — which diff as one
    unreadable line, and the point of the format here is that changing one keyword changes one line.
    """
    keyword_total = sum(len(b.keywords) for b in branches.values())
    distinct = len({k for b in branches.values() for k in b.keywords})
    lines = [
        HEADER.format(
            source=source.name,
            branch_count=len(branches),
            keyword_total=keyword_total,
            distinct=distinct,
        ),
        "branches:",
    ]
    for branch in branches.values():
        lines.append(f"  {quote(branch.name)}:")
        if branch.units:
            lines.append(f"    senweb_unit: {quote(', '.join(branch.units))}")
        if branch.partners:
            lines.append(f"    berlin_partner: {quote(', '.join(branch.partners))}")
        if branch.related:
            lines.append("    related:")
            lines.extend(f"      - {quote(value)}" for value in branch.related)
        lines.append("    keywords:")
        lines.extend(f"      - {quote(keyword)}" for keyword in branch.keywords.values())
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert the branch spreadsheet to YAML.")
    parser.add_argument("csv_path", type=Path, help="the exported spreadsheet")
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="where to write. Point it elsewhere to diff a new export against the live file "
        "instead of overwriting it.",
    )
    args = parser.parse_args()

    if not args.csv_path.exists():
        raise SystemExit(f"no such file: {args.csv_path}")

    rows = read_rows(args.csv_path)
    branches = build(rows)

    shared: collections.Counter[str] = collections.Counter()
    for branch in branches.values():
        shared.update(branch.keywords.keys())
    keyword_total = sum(len(b.keywords) for b in branches.values())
    merged = [n for n, b in branches.items() if len(b.units) > 1 or len(b.partners) > 1]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(branches, args.csv_path), encoding="utf-8")

    print(f"read   {len(rows)} rows from {args.csv_path.name}")
    print(f"wrote  {len(branches)} branches, {keyword_total} keyword entries to {args.out}")
    print(
        f"       {len(shared)} distinct keywords; "
        f"{sum(1 for v in shared.values() if v > 1)} claimed by more than one branch"
    )
    if len(rows) != len(branches):
        print(f"merged {len(rows) - len(branches)} duplicate row(s): {', '.join(merged) or '(none)'}")
    print("       most shared: " + ", ".join(f"{k} ({v})" for k, v in shared.most_common(5)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
