#!/usr/bin/env python3
"""Everything OffeneRegister knows about a Berlin company, kept as its source record.

`prepare_offeneregister_import.py` fills the identity columns of `companies`. This script keeps the
rest, so nothing in the dump is lost: one `company_source_records` row per company under the source
`offeneregister`, with the whole record in `source_payload`:

  register      court, type, number, suffix, OpenCorporates number, registered seat, status,
                retrieved_at, and the seven document flags (AD, CD, DK, HD, SI, UT, VÖ)
  former_names  the names the company was registered under before (32,170 Berlin companies) —
                useful to discovery, since a domain often still carries the old name
  registrations register moves: earlier or later numbers, with dates (25,584 companies)

It also fills `companies.city` from the registered seat where the address gave none: every Berlin
row has a seat, but only 116,527 have an address. Rows that already have a city are not touched.

  officers      every officer the register lists (348,904 entries for 143,355 companies): name,
                position, start and end date, place, flags
  address       the cleaned address, and the raw register text it came from

PERSONAL DATA. Officers and the raw address (which in some rows names managers with their birth
dates) are personal data under the GDPR. They stay in `company_source_records`, which Hasura does
not expose, and a privacy review is needed before any of it leaves local development (hub, app,
export). One use is reviewed: the current Geschäftsführer may be shown in the app, which
`import_register_managers.py` does. The data is a 2017–2019 snapshot, so much of it is out of date.

Matches companies by `registration_number`, so run it after the CSV load. The match is recorded on
the source record (`match_method = 'identifier'`, confidence 1.0), and the register number and the
OpenCorporates number become the company's `company_identifiers`. Safe to re-run: the record is
upserted on (source, external_id), an identifier on (scheme, value).

Usage:
    python scripts/import_offeneregister_records.py openregister.db     # PG_DSN to override the target
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "airflow" / "dags"))

import psycopg2  # noqa: E402
from psycopg2.extras import Json, execute_values  # noqa: E402
from source_enrichment_common import ensure_source  # noqa: E402

from prepare_offeneregister_import import FEDERAL_STATE, parse_address  # noqa: E402

SOURCE_KEY = "offeneregister"
FLAGS = {"AD": "register_flag_AD", "CD": "register_flag_CD", "DK": "register_flag_DK",
         "HD": "register_flag_HD", "SI": "register_flag_SI", "UT": "register_flag_UT",
         "VÖ": "register_flag_VOE"}
OFFICER_FIELDS = (
    "name", "firstname", "lastname", "maidenname", "title", "position", "type", "city", "flag",
    "start_date", "end_date", "dismissed", "reference_no",
)
REGISTRATION_FIELDS = (
    "data_type", "confidence", "publication_date", "retrieved_at", "source_url", "start_date",
    "start_date_type", "sample_date", "previous_company_number", "previous_jurisdiction_code",
    "previous_entity_type", "previous_registration_end_date", "subsequent_company_number",
    "subsequent_jurisdiction_code", "subsequent_entity_type", "subsequent_registration_start_date",
    "alternate_company_number", "alternate_jurisdiction_code", "alternate_entity_type",
)


def grouped(db: sqlite3.Connection, sql: str) -> dict[str, list[sqlite3.Row]]:
    out: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in db.execute(sql, (FEDERAL_STATE,)):
        out[row["company_id"]].append(row)
    return out


def _same_name(a: str, b: str) -> bool:
    return " ".join(a.split()).casefold() == " ".join(b.split()).casefold()


def _present(row: sqlite3.Row, fields: tuple[str, ...]) -> dict:
    return {field: row[field] for field in fields if row[field] not in (None, "")}


def payload(company: sqlite3.Row, names: list[sqlite3.Row], registrations: list[sqlite3.Row],
            officers: list[sqlite3.Row]) -> dict:
    address, postcode, city = parse_address(company["registered_address"] or "")
    return {
        "register": {
            "court": company["registrar"],
            "type": company["register_art"],
            "number": company["register_nummer"],
            "number_suffix": company["_registerNummerSuffix"] or None,
            "native_company_number": company["native_company_number"],
            "former_court": company["former_registrar"] or None,
            "opencorporates_number": company["company_number"],
            "jurisdiction_code": company["jurisdiction_code"],
            "federal_state": company["federal_state"],
            "registered_office": (company["registered_office"] or "").strip() or None,
            "status": company["current_status"],
            "retrieved_at": company["retrieved_at"],
            "document_flags": {flag: bool(company[column]) for flag, column in FLAGS.items()},
        },
        "address": {"address": address, "postcode": postcode, "city": city,
                    "raw": company["registered_address"] or None},
        # The name table repeats the current name in another spacing or case for 435 companies.
        "former_names": sorted({n["company_name"].strip() for n in names if not _same_name(n["company_name"], company["name"])}),
        "registrations": [_present(r, REGISTRATION_FIELDS) for r in registrations],
        "officers": [_present(o, OFFICER_FIELDS) for o in officers],
    }


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    lite = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    lite.row_factory = sqlite3.Row
    berlin = "SELECT c.company_number FROM company c WHERE c.federal_state = ?"
    names = grouped(lite, f"SELECT company_id, company_name FROM name WHERE company_id IN ({berlin})")
    registrations = grouped(lite, f"SELECT * FROM registrations WHERE company_id IN ({berlin})")
    officers = grouped(lite, f"SELECT * FROM officer WHERE company_id IN ({berlin}) ORDER BY id")

    dsn = os.getenv("PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app")
    with psycopg2.connect(dsn) as pg, pg.cursor() as cur:
        source_id = ensure_source(
            cur, key=SOURCE_KEY, name="OffeneRegister (Handelsregister snapshot 2017–2019)",
            source_type="register", metadata={"file": Path(sys.argv[1]).name, "federal_state": FEDERAL_STATE},
        )
        cur.execute("SELECT registration_number, id FROM public.companies WHERE registration_number IS NOT NULL")
        company_ids = dict(cur.fetchall())

        rows, identifiers, missing = [], [], 0
        for company in lite.execute("SELECT * FROM company WHERE federal_state = ? AND name <> ''", (FEDERAL_STATE,)):
            company_id = company_ids.get(f"Amtsgericht {company['native_company_number']}")
            if company_id is None:
                missing += 1
                continue
            number = company["company_number"]
            identifiers += [(company_id, "register", company["native_company_number"], source_id),
                            (company_id, "opencorporates", number, source_id)]
            rows.append((company_id, source_id, number, company["name"],
                         Json(payload(company, names.get(number, []), registrations.get(number, []),
                                      officers.get(number, [])))))

        execute_values(cur, """
            INSERT INTO public.company_source_records
              (company_id, source_id, external_id, source_company_name, source_payload,
               match_method, match_confidence, matched_at)
            VALUES %s
            ON CONFLICT (source_id, external_id) DO UPDATE SET
              company_id = EXCLUDED.company_id,
              source_company_name = EXCLUDED.source_company_name,
              source_payload = EXCLUDED.source_payload,
              match_method = EXCLUDED.match_method,
              match_confidence = EXCLUDED.match_confidence,
              matched_at = EXCLUDED.matched_at,
              last_seen_at = now(),
              last_synced_at = now()
        """, rows, template="(%s, %s, %s, %s, %s, 'identifier', 1.0, now())", page_size=2000)

        # Matched by register number, so both numbers become identifiers of that company. An
        # identifier already held by ANOTHER company is a conflict to look at, never overwritten.
        execute_values(cur, """
            INSERT INTO public.company_identifiers (company_id, scheme, value, source_id)
            VALUES %s
            ON CONFLICT (scheme, value) DO NOTHING
        """, identifiers, page_size=5000)
        cur.execute("""
            SELECT count(*) FROM public.company_identifiers i
            JOIN public.company_source_records r ON r.external_id = i.value AND r.source_id = %s
            WHERE i.scheme = 'opencorporates' AND i.company_id <> r.company_id
        """, (source_id,))
        conflicts = cur.fetchone()[0]

        # Cities may have been filled in, which changes which grants find their company.
        cur.execute("SELECT public.refresh_company_funding_links()")
        linked_grants = cur.fetchone()[0]
        # The app reads a stored copy of every profile (V5).
        cur.execute("SELECT public.refresh_company_profile()")

        cur.execute("""
            UPDATE public.companies c
            SET city = r.source_payload #>> '{register,registered_office}'
            FROM public.company_source_records r
            WHERE r.company_id = c.id AND r.source_id = %s
              AND c.city IS NULL AND r.source_payload #>> '{register,registered_office}' IS NOT NULL
        """, (source_id,))
        cities = cur.rowcount

    print(json.dumps({"source_records": len(rows), "identifiers": len(identifiers),
                      "identifier_conflicts": conflicts, "not_in_companies": missing,
                      "cities_filled_from_seat": cities, "grants_linked": linked_grants}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
