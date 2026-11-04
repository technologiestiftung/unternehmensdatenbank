"""Which of our companies a register entry from another source is: register number first, name second.

Shared by every source that names a Berlin register entry (`register_announcements`, `gleif`), so they
cannot drift apart:

1. **The register number**, through `company_identifiers` (scheme `register`).
2. **The name**, when the number is unknown: a change of legal form gets a new number, so the company
   can be ours under its old one. When exactly one company that is not merged and not `dissolved` has
   the same name key (`normalize_company_name` in SQL) and city, it is the company
   (`match_method = 'fuzzy'`, confidence 0.9) and gets the number as an identifier. A deleted company is
   never linked by name: a firm name must only be distinguishable at its place (§ 30 HGB, to be checked),
   so a deleted firm's name can be used again.
3. Otherwise the company is **created** (`match_method = 'created'`); every company that shared name and
   city becomes an open `match_candidates` row for a person to decide.
"""
from __future__ import annotations

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
    caller gives it its fields. With `create=False` an entry we cannot match returns (None, "skipped", []).
    """
    cur.execute(
        "SELECT company_id FROM public.company_identifiers WHERE scheme = 'register' AND value = ANY(%s) LIMIT 1",
        (registers,),
    )
    row = cur.fetchone()
    if row:
        return row[0], "identifier", []
    method, candidates = "identifier", []
    if city:
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
