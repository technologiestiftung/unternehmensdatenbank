"""Which of our companies a register entry from another source is: register number first, name second.

Shared by every source that names a Berlin register entry (`register_announcements`, `gleif`,
`openregister`, `berlin_zuwendungen`), so they cannot drift apart:

1. **The register number**, through `company_identifiers` (scheme `register`).
2. **The name**, when the number is unknown: a change of legal form gets a new number, so the company
   can be ours under its old one. When exactly one company that is not merged and not `dissolved` has
   the same name key (`normalize_company_name` in SQL) and city, it is the company
   (`match_method = 'fuzzy'`, confidence 0.9) and gets the number as an identifier. A deleted company is
   never linked by name: a firm name must only be distinguishable at its place (§ 30 HGB, to be checked),
   so a deleted firm's name can be used again.
   Only when the caller may create a company: an entry that may only update ours (an inactive GLEIF
   entity) is matched by register number alone, so a deleted firm never marks a newer namesake dissolved.
3. Otherwise the company is **created** (`match_method = 'created'`); every company that shared name and
   city becomes an open `match_candidates` row for a person to decide.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from enrichment.register_names import DISSOLVED


def name_match(
    same_name_and_city: list[tuple[str, str | None]],
) -> tuple[str | None, list[tuple[str, str | None]]]:
    """Fuzzy second: (the company, candidates for a person) from (id, register_status) of the companies
    sharing name key and city.

    Exactly one that is not `dissolved` is the company. Otherwise all of them are candidates: the
    entry's company is created, and a person decides which, if any, it is. None: it is created.
    """
    live = [company_id for company_id, status in same_name_and_city if status != DISSOLVED]
    if len(live) == 1:
        return live[0], []
    return None, same_name_and_city


def company_for(
    cur: Any, *, registers: list[str], name: str, city: str | None, source_id: str, created_by: str,
    create: bool = True,
) -> tuple[str | None, str, list[tuple[str, str | None]]]:
    """The company's id, how it was found ("identifier" | "fuzzy" | "created"), and candidates for a person.

    `registers` are the spellings `company_identifiers` may hold the entry under, the first being the one
    written for a company found by name or created. A created company is only its identity here; the
    caller gives it its fields. With `create=False` only the register number links (no name match), and an entry it does not find
    returns (None, "skipped", []).
    """
    cur.execute(
        "SELECT company_id FROM public.company_identifiers WHERE scheme = 'register' AND value = ANY(%s) LIMIT 1",
        (registers,),
    )
    row = cur.fetchone()
    if row:
        return row[0], "identifier", []
    method, candidates = "identifier", []
    if city and create:
        # The name key is computed in SQL, by the function the name index and the grant links use.
        # Straight on the published observations, not `v_companies_enriched`: the view computes every
        # company's register fields before it filters, about 10 s per entry.
        cur.execute(
            """
            SELECT n.company_id, st.value_text
            FROM public.company_field_observations n
            JOIN public.companies c ON c.id = n.company_id AND c.merged_into IS NULL
            JOIN public.company_field_observations ci
              ON ci.company_id = n.company_id AND ci.field_name = 'city' AND ci.is_canonical
             AND lower(ci.value_text) = lower(%s)
            LEFT JOIN public.company_field_observations st
              ON st.company_id = n.company_id AND st.field_name = 'register_status' AND st.is_canonical
            WHERE n.field_name = 'name' AND n.is_canonical
              AND public.normalize_company_name(n.value_text) = public.normalize_company_name(%s)
            ORDER BY n.company_id
            """,
            (city, name),
        )
        match, candidates = name_match([(r[0], r[1]) for r in cur.fetchall()])
        if match:
            row, method = (match,), "fuzzy"
    if row is None and not create:
        return None, "skipped", []
    if row is None:
        method = "created"
        cur.execute(
            "INSERT INTO public.companies (metadata) VALUES (jsonb_build_object('created_by', %s::text)) RETURNING id",
            (created_by,),
        )
        row = cur.fetchone()
    company_id = row[0]
    cur.execute(
        """
        INSERT INTO public.company_identifiers (company_id, scheme, value, source_id)
        VALUES (%s, 'register', %s, %s)
        ON CONFLICT (scheme, value) DO NOTHING
        """,
        (company_id, registers[0], source_id),
    )
    return company_id, method, candidates


def snapshot_line(cur: Any) -> dict[str, int]:
    """Per register type, the number OffeneRegister's Berlin range ends at (99.9th percentile). A number
    above it was registered after the 2017–2019 snapshot (`berlin_zuwendungen`, `common_crawl`)."""
    cur.execute(
        r"""
        SELECT substring(i.value from '(HR[AB]) \d'),
               percentile_disc(0.999) WITHIN GROUP (ORDER BY substring(i.value from 'HR[AB] (\d+)')::int)
        -- The numbers OffeneRegister itself wrote: one a later source adds to such a company (a new
        -- number after a change of legal form) must not move the line.
        FROM public.company_identifiers i
        JOIN public.sources s ON s.id = i.source_id AND s.key = 'offeneregister'
        WHERE i.scheme = 'register' AND i.value ~ '^Berlin \(Charlottenburg\) HR[AB] \d+'
        GROUP BY 1
        """
    )
    return dict(cur.fetchall())


def is_new_firm(register: str, line: dict[str, int]) -> bool:
    """'HRB 239529' above the snapshot's HRB range. False without a line: nothing to compare with."""
    kind, _, number = register.partition(" ")
    return kind in line and number.isdigit() and int(number) > line[kind]


def record_match(cur: Any, *, record_id: str, method: str, candidates: list[tuple[str, str | None]]) -> None:
    """How a source record was linked, set once, and the candidates a person has to decide."""
    # Set once: on a re-run a created company is found by its identifier, and that must not overwrite
    # "created" on the record that created it.
    cur.execute(
        """
        UPDATE public.company_source_records
        SET match_method = %s, match_confidence = %s, matched_at = now()
        WHERE id = %s AND match_method IS NULL
        """,
        (method, 0.9 if method == "fuzzy" else 1.0, record_id),
    )
    for candidate_id, register_status in candidates:
        cur.execute(
            """
            INSERT INTO public.match_candidates (source_record_id, company_id, score, reasons, status)
            VALUES (%s, %s, 0.9, jsonb_build_object('normalized_name', true, 'city', true,
                                                    'register_status', %s::text), 'open')
            ON CONFLICT DO NOTHING
            """,
            (record_id, candidate_id, register_status),
        )


# --- writing a register entry ------------------------------------------------------------------

#: The register facts a register source may state. A value a source no longer states drops to 0.
ENTRY_FIELDS = ("name", "legal_form", "address", "postcode", "city", "register_status", "business_purpose")


def same_name(a: str, b: str) -> bool:
    """The same name but for case, spaces and punctuation: "e. V." and "e.V.", "GMBH" and "GmbH"."""
    return re.sub(r"\W+", "", a.lower()) == re.sub(r"\W+", "", b.lower())


@dataclass(frozen=True)
class RegisterEntry:
    """One company as a register source states it."""

    external_id: str
    external_url: str
    name: str
    #: How `company_identifiers` may hold the entry, the plain spelling first.
    registers: list[str]
    city: str | None
    active: bool | None
    #: Field name to value, among `ENTRY_FIELDS`.
    fields: dict[str, str]
    #: When the source last confirmed the entry, as `source_updated_at`. None only with no fields.
    updated: str | None
    payload: dict[str, Any]
    #: Further identifiers, scheme to value, e.g. {"lei": "5299..."}.
    identifiers: dict[str, str] = field(default_factory=dict)


def write_entry(cur: Any, entry: RegisterEntry, *, source_id: str, sync_run_id: str, created_by: str) -> str:
    """One entry: its company (found, or created when active), source record, identifiers and observations.

    Returns how it was linked, "skipped" for an inactive entry we cannot match by register number: a
    deleted firm's name can be used again, so an inactive entry is never linked by name (#88).
    """
    from source_enrichment_common import insert_text_observation, upsert_source_record

    company_id, method, candidates = company_for(
        cur, registers=entry.registers, name=entry.name, city=entry.city, source_id=source_id,
        created_by=created_by, create=bool(entry.active))
    if company_id is None:
        return "skipped"
    record_id, _ = upsert_source_record(
        cur, company_id=company_id, source_id=source_id, external_id=entry.external_id,
        external_url=entry.external_url, source_company_name=entry.name, source_payload=entry.payload,
    )
    record_match(cur, record_id=record_id, method=method, candidates=candidates)
    for scheme, value in entry.identifiers.items():
        cur.execute(
            """
            INSERT INTO public.company_identifiers (company_id, scheme, value, source_id)
            VALUES (%s, %s, %s, %s) ON CONFLICT (scheme, value) DO NOTHING
            """,
            (company_id, scheme, value, source_id),
        )
    fields = dict(entry.fields)
    # Against another source's name only: compared with its own, a source would drop its name on the next
    # read, the older name would win again, and the name would flip on every run.
    cur.execute(
        """
        SELECT value_text FROM public.company_field_observations
        WHERE company_id = %s AND field_name = 'name' AND is_canonical AND source_id <> %s
        """,
        (company_id, source_id),
    )
    current = cur.fetchone()
    if current and "name" in fields and same_name(current[0], fields["name"]):
        # "EVENIAT GMBH" for "eveniat GmbH": only a different name is news.
        del fields["name"]
    for field_name, value in fields.items():
        insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id, sync_run_id=sync_run_id,
            field_name=field_name, value_text=value, confidence=1.0, source_updated_at=entry.updated,
            metadata={"decided_by": "read from the end of the registered name"} if field_name == "legal_form" else {},
        )
    # A value this entry no longer states (a moved address, a new name) drops out of the race.
    cur.execute(
        """
        UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
        WHERE company_source_record_id = %s AND field_name = ANY(%s) AND confidence <> 0
          AND NOT (field_name, value_text) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
        """,
        (record_id, list(ENTRY_FIELDS), list(fields), list(fields.values())),
    )
    return method
