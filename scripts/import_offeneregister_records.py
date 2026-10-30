#!/usr/bin/env python3
"""OffeneRegister's SQLite dump into the database, Berlin only: companies, source records, register fields.

OffeneRegister is a snapshot of the German Handelsregister retrieved between June 2017 and January
2019: 5.3 million companies, 296,486 of them at the Berlin court (Charlottenburg), 142,571 of
those still registered at the time. It is example data until Fusionbase delivers the real set.

Per company, three things:

1. **The company**, found by its register number, created if we do not have it. A company is
   identity only: an id and `metadata` (V3); the register number is an identifier (V7).
2. **Its source record**, one `company_source_records` row under the source `offeneregister`, with
   everything the dump knows in `source_payload`, so nothing is lost:

  register      court, type, number, suffix, OpenCorporates number, registered seat, status,
                retrieved_at, and the seven document flags (AD, CD, DK, HD, SI, UT, VÖ)
  former_names  the names the company was registered under before (32,170 Berlin companies) —
                useful to discovery, since a domain often still carries the old name
  registrations register moves: earlier or later numbers, with dates (25,584 companies)
  officers      every officer the register lists (348,904 entries for 143,355 companies): name,
                position, start and end date, place, flags
  address       the cleaned address, and the raw register text it came from

3. **Its register fields as observations** (`to_row`): name, legal form (read from the end of the
   name; the dump has no field for it), address, postcode, city and status, confidence 1.0. The
   dump has no WZ code, purpose or founding date. The city is the registered seat where the address
   has none: every Berlin row has a seat, only 116,527 an address. `pipeline_coordinator` decides
   what is shown; this script runs that step for the register fields itself, so the app is current
   when it ends.

PERSONAL DATA. Officers and the raw address (which in some rows names managers with their birth
dates) are personal data under the GDPR. They stay in `company_source_records`, which Hasura does
not expose. Only the current Geschäftsführer is shown in the app, through
`import_register_managers.py`. The data is a 2017–2019 snapshot, so much of it is out of date.

The match is recorded on the source record (`match_method` 'identifier' or 'created', confidence
1.0), and the register number and the OpenCorporates number become the company's
`company_identifiers`. Safe to re-run: the record is upserted on (source, external_id), an
identifier on (scheme, value), an observation on its upsert key. A value the dump no longer
delivers drops to confidence 0 and is un-published; it is never deleted.

Usage:
    python scripts/import_offeneregister_records.py openregister.db     # PG_DSN to override the target
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "airflow" / "dags"))

import psycopg2  # noqa: E402
from psycopg2.extras import Json, execute_values  # noqa: E402
from enrichment.register_names import ACTIVE, DISSOLVED, legal_form  # noqa: E402
from pipeline_coordinator.resolver import REGISTER_FIELDS, resolve_register_fields  # noqa: E402
from source_enrichment_common import ensure_source  # noqa: E402

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


FEDERAL_STATE = "Berlin"

# OffeneRegister's status, in the register's own German words that `register_status` uses everywhere
# else, translated (#47): `register_names.ACTIVE` / `DISSOLVED`.
STATUS = {"currently registered": ACTIVE, "removed": DISSOLVED}

# The address ends at "<postcode> <city>". What follows in some rows is register text, sometimes with
# managers' names and dates of birth, which must not land in `address`.
# shortcut: the city is one word, so "Frankfurt am Main" becomes "Frankfurt"; fine for Berlin.
_ADDRESS = re.compile(r"^(.*?\b(\d{5})\s+([^\s,.:;]+))")
# "c/o Muster, Erika, Weg 1, 10115 Berlin": a segment naming who receives the post is a person or another
# company, not a place. 10,423 of the 116,527 Berlin addresses have one. Before the postcode, only
# segments with a digit (house number, building, postcode) and no such marker are kept.
_CARE_OF = re.compile(r"c/o|c\.o\.|\bbei\b|z\.\s*hd\.|zu händen", re.IGNORECASE)
_DIGIT = re.compile(r"\d")


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
    """The register number, `metadata` and register fields of one dump row; a field it lacks is None."""
    name = record["name"].strip()
    address, postcode, city = parse_address(record["registered_address"] or "")
    return {
        # Northdata's format, so both datasets key companies the same way.
        "registration_number": f"Amtsgericht {record['native_company_number']}",
        "metadata": {
            "source_file": "openregister.db",
            "opencorporates_number": record["company_number"],
            "status": record["current_status"],
            "retrieved_at": record["retrieved_at"],
        },
        "name": name,
        "legal_form": legal_form(name),
        "address": address,
        "postcode": postcode,
        # The registered seat when the address has none: every row has a seat, a third an address.
        "city": city or (record.get("registered_office") or "").strip() or None,
        "register_status": STATUS.get(record["current_status"]),
    }


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
    companies = [(dict(c), to_row(dict(c))) for c in
                 lite.execute("SELECT * FROM company WHERE federal_state = ? AND name <> ''", (FEDERAL_STATE,))]

    dsn = os.getenv("PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app")
    with psycopg2.connect(dsn) as pg, pg.cursor() as cur:
        source_id = ensure_source(
            cur, key=SOURCE_KEY, name="OffeneRegister (Handelsregister snapshot 2017–2019)",
            source_type="register", metadata={"file": Path(sys.argv[1]).name, "federal_state": FEDERAL_STATE},
        )
        cur.execute("SELECT 'Amtsgericht ' || value, company_id FROM public.company_identifiers WHERE scheme = 'register'")
        company_ids = dict(cur.fetchall())
        new = {row["registration_number"]: row for _, row in companies if row["registration_number"] not in company_ids}
        # The ids are made here: `companies` holds no number to map a returned id back to. The register
        # identifier is written with the other identifiers below.
        created = {number: str(uuid.uuid4()) for number in new}
        execute_values(cur, "INSERT INTO public.companies (id, metadata) VALUES %s",
                       [(created[number], Json(row["metadata"])) for number, row in new.items()], page_size=5000)
        company_ids.update(created)

        records, identifiers = [], []
        for company, row in companies:
            company_id = company_ids[row["registration_number"]]
            number = company["company_number"]
            identifiers += [(company_id, "register", company["native_company_number"], source_id),
                            (company_id, "opencorporates", number, source_id)]
            records.append((company_id, source_id, number, company["name"],
                            Json(payload(company, names.get(number, []), registrations.get(number, []),
                                         officers.get(number, []))),
                            "created" if row["registration_number"] in new else "identifier"))

        record_ids = dict(execute_values(cur, """
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
            RETURNING external_id, id
        """, records, template="(%s, %s, %s, %s, %s, %s, 1.0, now())", page_size=2000, fetch=True))

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

        observations = write_register_fields(cur, source_id, [
            (company_ids[row["registration_number"]], record_ids[company["company_number"]], row)
            for company, row in companies
        ])
        published = resolve_register_fields(cur)
        # Names and cities may have changed, which changes which grants find their company.
        cur.execute("SELECT public.refresh_company_funding_links()")
        linked_grants = cur.fetchone()[0]
        # The app reads a stored copy of every profile (V5).
        cur.execute("SELECT public.refresh_company_profile()")

    print(json.dumps({"source_records": len(records), "companies_created": len(new),
                      "identifiers": len(identifiers), "identifier_conflicts": conflicts,
                      "register_field_observations": observations, "published": published,
                      "grants_linked": linked_grants}, indent=2))
    return 0


def write_register_fields(cur: Any, source_id: str, rows: list[tuple[str, str, dict]]) -> int:
    """Every register field of every company as an observation at confidence 1.0; returns how many.

    A value this source delivered before and no longer does drops to 0, which un-publishes it at the
    next resolution. `ON CONFLICT DO NOTHING` without a target uses whichever unique index fires,
    so the upsert key's predicate is not written out a third time (see `insert_text_observation`).
    """
    values = [
        (company_id, record_id, source_id, field, row[field], row["metadata"]["retrieved_at"],
         Json({"decided_by": "read from the end of the registered name"} if field == "legal_form" else {}))
        for company_id, record_id, row in rows
        for field in REGISTER_FIELDS
        if row.get(field)
    ]
    cur.execute("CREATE TEMP TABLE _delivered (company_id uuid, field_name text, value_text text) ON COMMIT DROP")
    execute_values(cur, "INSERT INTO _delivered VALUES %s", [v[0:1] + v[3:5] for v in values], page_size=10000)
    execute_values(cur, """
        INSERT INTO public.company_field_observations
          (company_id, company_source_record_id, source_id, field_name, value_text, observed_at,
           source_updated_at, confidence, metadata)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, [v[:6] + (v[5],) + v[6:] for v in values],
       template="(%s, %s, %s, %s, %s, %s::timestamptz, %s::timestamptz, 1.0, %s)", page_size=5000)
    cur.execute("""
        WITH target AS (
          SELECT o.id, CASE WHEN d.company_id IS NULL THEN 0 ELSE 1.0 END AS confidence
          FROM public.company_field_observations o
          LEFT JOIN _delivered d
            ON d.company_id = o.company_id AND d.field_name = o.field_name AND d.value_text = o.value_text
          WHERE o.source_id = %s AND o.field_name = ANY(%s)
        )
        UPDATE public.company_field_observations o
        SET confidence = t.confidence, updated_at = now()
        FROM target t
        WHERE o.id = t.id AND o.confidence IS DISTINCT FROM t.confidence
    """, (source_id, list(REGISTER_FIELDS)))
    return len(values)


if __name__ == "__main__":
    raise SystemExit(main())
