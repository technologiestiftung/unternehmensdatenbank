"""A company's register officers as comparison keys, never as names to keep.

Officers are personal data. They are loaded per batch, compared in memory against names read
from a page, and never written: what is stored is evidence that a name matched, never which
officer. Used by the website's Geschäftsführer match (issue #18) and the spaCy manager names'
`register_officer` mark.
"""
from __future__ import annotations

from typing import Any

from enrichment.imprint_extractor import person_key


def officer_keys(officers: list[dict[str, Any]] | None) -> frozenset[str]:
    """The register officers of one company as `person_key` comparison keys (people only)."""
    keys: set[str] = set()
    for officer in officers or []:
        if (officer.get("type") or "person") != "person":
            continue
        full = " ".join(x for x in (officer.get("firstname"), officer.get("lastname")) if x)
        for name in (full, officer.get("name")):
            if name and name.strip():
                keys.add(person_key(name))
    return frozenset(keys)


def load_officers(cur: Any, company_ids: list[str]) -> dict[str, frozenset[str]]:
    cur.execute(
        """
        SELECT r.company_id::text, r.source_payload -> 'officers'
        FROM public.company_source_records r
        JOIN public.sources s ON s.id = r.source_id AND s.source_type = 'register'
        WHERE r.company_id = ANY(%s::uuid[])
        """,
        (company_ids,),
    )
    found: dict[str, frozenset[str]] = {}
    for company_id, officers in cur.fetchall():
        found[company_id] = found.get(company_id, frozenset()) | officer_keys(officers)
    return found
