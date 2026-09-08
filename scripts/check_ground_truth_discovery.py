#!/usr/bin/env python3
"""Ground truth for the website column. Free: no API, no model, no network. Reads the database.

Websites arrive from six sources at two different priorities and are resolved by one DAG, so a
count can move for reasons that have nothing to do with discovery working better or worse: a
re-import un-publishes what it previously won, a priority edit re-orders everybody, a coordinator
run that has not happened yet leaves observations that are correct and invisible.

This pins the per-source canonical counts, the priorities that produce them, and the invariants the
resolver is supposed to hold. It asserts COUNTS, never correctness: not one of the 2,816 websites
that came from the export has ever been fetched.

Not in the `PostToolUse` hook on purpose — it needs a running Postgres, and a guard that fails
because Docker is down would block editing.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

for candidate in (REPO / "airflow" / "dags", Path("/opt/airflow/dags")):
    if candidate.is_dir():
        sys.path.insert(0, str(candidate))
        break

# Running on the host means Postgres is on localhost:5433, not on the compose network. The DAG
# default (`host=postgres port=5432`) is right inside a container and wrong outside one, so supply
# a host-shaped default without overriding an explicit PG_DSN. Same line as
# import_northdata_websites.py, and it has to be set BEFORE source_enrichment_common is imported --
# that module reads PG_DSN at import time.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

from source_enrichment_common import get_connection  # noqa: E402

TOTAL_COMPANIES = 14_443

#: Measured 2026-09-08. Re-measure with docs/core-queries.sql, or the query in this file.
#:
#: Count these THROUGH `company_canonical_field_sources`, never by grouping on the observation's
#: own `field_name`. The resolver aliases `website <- [candidate_url, website]`, so a company whose
#: winning observation is stored as `candidate_url` still has a canonical website. Grouping on the
#: observation name splits Brave's 85 into 82 + 3 and reads like three lost websites; the first
#: version of this baseline said 82 for exactly that reason.
CANONICAL_WEBSITES = {
    "northdata_export": 2_816, "combined_discovery": 551, "brave_search_discovery": 85,
    "url_discovery": 53, "exa_search_discovery": 36, "parallel_search_discovery": 1,
}
CANONICAL_TOTAL = 3_542

#: The alias itself, pinned. If a resolver change stopped promoting `candidate_url`, the totals
#: above would fall by 3 and nothing would say why — this is the line that says why.
CANONICAL_BY_OBSERVATION_NAME = {"website": 3_539, "candidate_url": 3}
CANONICAL_BRAVE_SPLIT = {"website": 82, "candidate_url": 3}
CANONICAL_OTHER_FIELDS = {"cluster": 532, "tech_field": 508}
#: Lower wins. The export at 50 outranks every discovery source, which is why 2,816 URLs nobody
#: fetched beat pages that were downloaded and checked.
SOURCE_PRIORITIES = {
    "northdata_export": 50, "combined_discovery": 100, "brave_search_discovery": 100,
    "exa_search_discovery": 100, "parallel_search_discovery": 100, "url_discovery": 100,
}
WEBSITE_MIN_CONFIDENCE = 0.35   # resolver's bar; LLM-only guesses are capped at 0.34
#: Observations are never deleted, only un-canonicalised, so this floor may rise and never fall.
WEBSITE_OBSERVATION_FLOOR = 5_457
#: Live known bug: pipeline_coordinator inserts into company_managers with ON CONFLICT DO NOTHING
#: against a table with no matching unique constraint, so every run adds another copy.
MANAGER_ROWS_CEILING = 7_167
MANAGER_DISTINCT = 1_047

FAILURES: list[str] = []


def check(label: str, got, want) -> None:
    ok = got == want
    if not ok:
        FAILURES.append(label)
    print(f"{'ok  ' if ok else 'FAIL'} {label:<62} got={got!r:<28} want={want!r}")


def section(title: str) -> None:
    print(f"\n-- {title} --")


def main() -> int:
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            section("the table itself")
            cur.execute("SELECT count(*) FROM public.companies")
            check("companies", cur.fetchone()[0], TOTAL_COMPANIES)

            section("source priorities — the thing that decides who wins")
            cur.execute("SELECT key, priority FROM public.sources")
            check("priorities", dict(cur.fetchall()), SOURCE_PRIORITIES)

            section("canonical websites, per source (never regress)")
            cur.execute("""
                SELECT s.key, count(*)
                FROM public.company_canonical_field_sources cf
                JOIN public.company_field_observations o
                  ON o.id = cf.company_field_observation_id
                JOIN public.company_source_records r ON r.id = o.company_source_record_id
                JOIN public.sources s ON s.id = r.source_id
                WHERE cf.field_name = 'website'
                GROUP BY 1
            """)
            got = dict(cur.fetchall())
            for key, want in CANONICAL_WEBSITES.items():
                check(f"canonical websites from {key}", got.get(key, 0), want)
            check("canonical websites in total", sum(got.values()), CANONICAL_TOTAL)

            section("candidate_url is an alias for website, not a second field")
            cur.execute("""
                SELECT o.field_name, count(*)
                FROM public.company_canonical_field_sources cf
                JOIN public.company_field_observations o
                  ON o.id = cf.company_field_observation_id
                WHERE cf.field_name = 'website'
                GROUP BY 1
            """)
            check("canonical websites by observation name",
                  dict(cur.fetchall()), CANONICAL_BY_OBSERVATION_NAME)
            cur.execute("""
                SELECT o.field_name, count(*)
                FROM public.company_canonical_field_sources cf
                JOIN public.company_field_observations o
                  ON o.id = cf.company_field_observation_id
                JOIN public.company_source_records r ON r.id = o.company_source_record_id
                JOIN public.sources s ON s.id = r.source_id
                WHERE cf.field_name = 'website' AND s.key = 'brave_search_discovery'
                GROUP BY 1
            """)
            check("brave's 85 split by observation name",
                  dict(cur.fetchall()), CANONICAL_BRAVE_SPLIT)

            section("the other two canonical fields")
            cur.execute("""SELECT field_name, count(*) FROM public.company_canonical_field_sources
                           WHERE field_name <> 'website' GROUP BY 1""")
            check("cluster and tech_field", dict(cur.fetchall()), CANONICAL_OTHER_FIELDS)

            section("resolver invariants")
            cur.execute("""SELECT count(*) FROM public.company_canonical_field_sources cf
                           JOIN public.company_field_observations o
                             ON o.id = cf.company_field_observation_id
                           WHERE cf.field_name = 'website' AND o.confidence < %s""",
                        (WEBSITE_MIN_CONFIDENCE,))
            check("no canonical website below the bar", cur.fetchone()[0], 0)
            cur.execute("""SELECT count(*) FROM public.company_canonical_field_sources cf
                           JOIN public.company_field_observations o
                             ON o.id = cf.company_field_observation_id
                           WHERE cf.field_name = 'website'
                             AND o.field_name = 'directory_url'""")
            check("no directory_url promoted to website", cur.fetchone()[0], 0)
            cur.execute("""SELECT count(*) FROM (
                             SELECT company_id, field_name FROM public.company_field_observations
                             WHERE is_canonical GROUP BY 1, 2 HAVING count(*) > 1) t""")
            check("one canonical observation per company and field", cur.fetchone()[0], 0)
            cur.execute("""SELECT count(*) FROM public.company_canonical_field_sources cf
                           JOIN public.company_field_observations o
                             ON o.id = cf.company_field_observation_id
                           WHERE NOT o.is_canonical""")
            check("every canonical row points at a canonical observation", cur.fetchone()[0], 0)

            section("observations are never deleted, only un-canonicalised")
            cur.execute("""SELECT count(*) FROM public.company_field_observations
                           WHERE field_name IN ('website', 'candidate_url')""")
            check("website-ish observations (>= baseline)",
                  cur.fetchone()[0] >= WEBSITE_OBSERVATION_FLOOR, True)

            section("known live bug: company_managers duplicates every run")
            cur.execute("""SELECT count(*), count(DISTINCT (company_id, full_name))
                           FROM public.company_managers""")
            total, distinct = cur.fetchone()
            check("duplication is not getting worse", total <= MANAGER_ROWS_CEILING, True)
            check("distinct managers", distinct, MANAGER_DISTINCT)

            section("the spend guard cannot fire (documented, still true)")
            cur.execute("""SELECT max(data_quality_score),
                                  count(*) FILTER (WHERE data_quality_score >= 0.90)
                           FROM public.companies""")
            best, at_guard = cur.fetchone()
            check("ceiling is still 0.85", float(best), 0.85)
            check("nobody reaches the 0.90 guard", at_guard, 0)
    finally:
        connection.close()

    print(f"\n{'FAILED' if FAILURES else 'all checks passed'}")
    for label in FAILURES:
        print(f"  - {label}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
