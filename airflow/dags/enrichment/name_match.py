"""Which of our companies an entry is, by name and postcode alone: for sources without a register number.

Shared by `wirtschaftsatlas` and `ba_jobsuche`. The name key is `normalize_company_name` in SQL:

1. the same name key as exactly one company, at one of the entry's postcodes -> confidence 0.9;
2. else the same name key as exactly one company that is not dissolved -> 0.8;
3. else no match. A name alone never creates a company: these sources are not registers.
"""
from __future__ import annotations

from typing import Any, Iterable

from psycopg2.extras import execute_values

from enrichment.register_names import DISSOLVED

BY_POSTCODE, BY_NAME = 0.9, 0.8


def match_by_name(cur: Any, entries: Iterable[tuple[str, str, list[str]]]) -> dict[str, tuple[str, float]]:
    """(key, name, postcodes) -> {key: (company id, confidence)} for the entries that match one company."""
    cur.execute("CREATE TEMP TABLE _name_entry (key text, name_key text, postcodes text[]) ON COMMIT DROP")
    execute_values(cur, "INSERT INTO _name_entry SELECT v.key, public.normalize_company_name(v.name), v.postcodes"
                        " FROM (VALUES %s) AS v (key, name, postcodes)",
                   [(key, name, list(postcodes)) for key, name, postcodes in entries], template="(%s, %s, %s::text[])")
    cur.execute(
        """
        WITH named AS (
          SELECT e.key, p.id::text AS company_id, p.postcode = ANY (e.postcodes) AS same_postcode,
                 p.register_status IS DISTINCT FROM %(dissolved)s AS live
          FROM _name_entry e
          JOIN public.company_profile_mv p ON public.normalize_company_name(p.name) = e.name_key
          WHERE e.name_key <> ''
        ),
        by_postcode AS (
          SELECT key, (array_agg(company_id))[1] AS company_id FROM named
          WHERE same_postcode GROUP BY key HAVING count(*) = 1
        ),
        by_name AS (
          SELECT key, (array_agg(company_id))[1] AS company_id FROM named
          WHERE live AND key NOT IN (SELECT key FROM by_postcode) GROUP BY key HAVING count(*) = 1
        )
        SELECT key, company_id, %(by_postcode)s FROM by_postcode
        UNION ALL SELECT key, company_id, %(by_name)s FROM by_name
        """,
        {"dissolved": DISSOLVED, "by_postcode": BY_POSTCODE, "by_name": BY_NAME},
    )
    matches = {key: (company_id, float(confidence)) for key, company_id, confidence in cur.fetchall()}
    cur.execute("DROP TABLE _name_entry")
    return matches
