#!/usr/bin/env python3
"""OffeneRegister's SQLite dump → the same CSV `prepare_company_import.py` writes, Berlin only.

OffeneRegister is a snapshot of the German Handelsregister retrieved between June 2017 and January
2019: 5.3 million companies, 296,486 of them at the Berlin court (Charlottenburg), 142,571 of
those still registered at the time. It is example data until Fusionbase delivers the real set.

What it has: name, register number, status, address (127,025 of the Berlin rows). What it lacks:
WZ code, purpose, founding date, website — those columns stay empty. Its `registrations` table
records register moves, not founding dates, so it is not read.

The `officer` table is deliberately NOT read. It holds names of real people (348,904 for Berlin),
which is personal data under the TSB rules and needs a privacy review before it enters a database.

Usage:
    python scripts/prepare_offeneregister_import.py openregister.db berlin.csv
then create the database and load the CSV: docs/data-model.md, "Loading the OffeneRegister Berlin set".
"""
from __future__ import annotations

import csv
import json
import re
import sqlite3
import sys
from pathlib import Path

from prepare_company_import import OUTPUT_COLUMNS, normalize_company_name

FEDERAL_STATE = "Berlin"

# OffeneRegister's status, in the register's own German words that `register_status` uses everywhere
# else (the discovery filter is `= 'aktiv'`).
STATUS = {"currently registered": "aktiv", "removed": "erloschen"}

# Longest first: "GmbH & Co. KG" must win over "KG", "UG (haftungsbeschränkt)" over "UG".
LEGAL_FORMS = [
    ("GmbH & Co. KG", r"gmbh\s*&\s*co\.?\s*kg"),
    ("UG & Co. KG", r"ug\s*\(haftungsbeschränkt\)\s*&\s*co\.?\s*kg"),
    # legal_form() strips a trailing ")", so the closing parenthesis is optional here.
    ("UG", r"\(haftungsbeschränkt\)?|\bug"),
    ("gGmbH", r"ggmbh"),
    ("GmbH", r"gmbh|g\.\s*m\.\s*b\.\s*h|\bm\.?\s*b\.?\s*h|mit beschränkter haftung"),
    ("PartG mbB", r"\bmbb"),
    ("KGaA", r"kgaa"),
    ("AG", r"\bag|aktiengesellschaft"),
    ("SE", r"\bse"),
    ("Ltd.", r"\bltd|\blimited"),
    ("e. V.", r"\be\.\s*v"),
    ("eG", r"\beg"),
    ("e. K.", r"\be\.\s*k(fm|ffr|fr)?"),
    ("OHG", r"ohg"),
    ("KG", r"\bk\.?\s*g|kommanditgesellschaft"),
    ("PartG", r"partg|partnerschaft"),
]
_LEGAL_FORM_RES = [(form, re.compile(rf"(?:{pattern})$", re.IGNORECASE)) for form, pattern in LEGAL_FORMS]
# Words that often follow the legal form: "Beispiel GmbH Berlin", "… GmbH Steuerberatungsgesellschaft".
_TRAILING_WORDS = re.compile(
    r"(?:[\s,]+(?:berlin|deutschland|in liquidation|i\.\s*l|-|steuerberatungsgesellschaft"
    r"|wirtschaftsprüfungsgesellschaft|rechtsanwaltsgesellschaft|rechtsanwälte)[.)]*)+$",
    re.IGNORECASE,
)
# The address ends at "<postcode> <city>". What follows in some rows is register text, sometimes with
# managers' names and dates of birth, which must not land in `address`.
# shortcut: the city is one word, so "Frankfurt am Main" becomes "Frankfurt"; fine for Berlin.
_ADDRESS = re.compile(r"^(.*?\b(\d{5})\s+([^\s,.:;]+))")
# "c/o Muster, Erika, Weg 1, 10115 Berlin": a segment naming who receives the post is a person or another
# company, not a place. 10,423 of the 116,527 Berlin addresses have one. Before the postcode, only
# segments with a digit (house number, building, postcode) and no such marker are kept.
_CARE_OF = re.compile(r"c/o|c\.o\.|\bbei\b|z\.\s*hd\.|zu händen", re.IGNORECASE)
_DIGIT = re.compile(r"\d")


def legal_form(name: str) -> str | None:
    """The legal form from the END of the name; the dump has no field for it."""
    core = _TRAILING_WORDS.sub("", name.strip()).rstrip(" .)")
    for form, pattern in _LEGAL_FORM_RES:
        if pattern.search(core):
            return form
    return None


def parse_address(raw: str) -> tuple[str | None, str | None, str | None]:
    """"Street 1, 10119 Berlin. Gegenstand: …" → ("Street 1, 10119 Berlin", "10119", "Berlin").

    No postcode, no address: what is left is register text, not a place. Before the postcode, a
    comma-separated segment is kept only if it has a digit and no "c/o", "bei" or "z. Hd." (10,423 of
    the 116,527 Berlin addresses have one); the postcode's own segment then starts at the postcode.
    """
    match = _ADDRESS.match(raw.strip())
    if not match:
        return None, None, None
    address = match.group(1)
    *rest, last = address.split(",")
    if _CARE_OF.search(last):
        last = address[match.start(2):]
    kept = [part.strip() for part in rest if _DIGIT.search(part) and not _CARE_OF.search(part)]
    return ", ".join([*kept, last.strip()]), match.group(2), match.group(3)


def to_row(record: dict) -> dict:
    name = record["name"].strip()
    address, postcode, city = parse_address(record["registered_address"] or "")
    return {
        "company_name": name,
        "normalized_name": normalize_company_name(name),
        # Northdata's format, so both datasets key companies the same way.
        "registration_number": f"Amtsgericht {record['native_company_number']}",
        "legal_form": legal_form(name),
        "business_purpose": None,
        "wz_code": None,
        "address": address,
        "established_at": None,
        "postcode": postcode,
        "city": city,
        "register_status": STATUS.get(record["current_status"]),
        "metadata_json": json.dumps(
            {
                "source_file": "openregister.db",
                "opencorporates_number": record["company_number"],
                "status": record["current_status"],
                "retrieved_at": record["retrieved_at"],
            },
            ensure_ascii=False,
        ),
    }


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    source, target = Path(sys.argv[1]), Path(sys.argv[2])
    db = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT company_number, native_company_number, name, current_status, registered_address, retrieved_at"
        " FROM company WHERE federal_state = ? AND name <> ''",
        (FEDERAL_STATE,),
    )
    written = 0
    with target.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for record in rows:
            writer.writerow(to_row(dict(record)))
            written += 1
    print(f"{written} {FEDERAL_STATE} companies written to {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
