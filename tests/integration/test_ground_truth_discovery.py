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

**Two ways to run it, and both must keep working.**

    python tests/integration/test_ground_truth_discovery.py    # standalone, exit 1 on failure
    pytest tests/integration/test_ground_truth_discovery.py    # 9 tests

Neither requires pytest to be importable: without it, `_run_without_pytest` opens the connection
itself and calls each test. See the note on `check()` in `test_ground_truth_columns.py` for why it
accumulates instead of raising.
"""
from __future__ import annotations

import inspect
import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))   # repo root, for `tests.…`

from tests import loaders                                     # noqa: E402

try:
    import pytest
except ImportError:            # the Airflow image has no pytest; standalone mode still works
    pytest = None

if pytest is not None:
    pytestmark = pytest.mark.integration

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
CANONICAL_OTHER_FIELDS = {"cluster": 532, "tech_field": 508}

#: The alias itself, pinned. If a resolver change stopped promoting `candidate_url`, the totals
#: above would fall by 3 and nothing would say why — this is the line that says why.
CANONICAL_BY_OBSERVATION_NAME = {"website": 3_539, "candidate_url": 3}
CANONICAL_BRAVE_SPLIT = {"website": 82, "candidate_url": 3}

#: Lower wins. The export at 50 outranks every discovery source, which is why 2,816 URLs nobody
#: fetched beat pages that were downloaded and checked.
SOURCE_PRIORITIES = {
    "northdata_export": 50, "combined_discovery": 100, "brave_search_discovery": 100,
    "exa_search_discovery": 100, "parallel_search_discovery": 100, "url_discovery": 100,
}
WEBSITE_MIN_CONFIDENCE = 0.35   # resolver's bar; LLM-only guesses are capped at 0.34
#: Observations are never deleted, only un-canonicalised, so this floor may rise and never fall.
#:
#: V17 deduped 49 duplicate groups; floor is now 5,408, down from 5,457. `candidate_url`
#: legitimately has up to 5 rows per company/source — the key includes value_text so a re-run of the
#: same URL updates while a new URL still inserts.
#:
#: This is the one number in this file that a migration was allowed to lower, and only because the
#: rows removed were exact duplicates: same company, same field, same source, same URL. The loser of
#: each pair was never canonical — all six per-source counts above are unchanged — so nothing was
#: un-published. If this floor ever falls again, that is the failure this assertion exists to catch.
WEBSITE_OBSERVATION_FLOOR = 5_408
#: Live known bug: pipeline_coordinator inserts into company_managers with ON CONFLICT DO NOTHING
#: against a table with no matching unique constraint, so every run adds another copy.
MANAGER_ROWS_CEILING = 7_167
MANAGER_DISTINCT = 1_047

FAILURES: list[str] = []


def check(label: str, got, want) -> None:
    """Record and print one comparison. Never raises — `checks()` does that, at the end."""
    ok = got == want
    if not ok:
        FAILURES.append(f"{label}  got={got!r} want={want!r}")
    print(f"{'ok  ' if ok else 'FAIL'} {label:<62} got={got!r:<28} want={want!r}")


def section(title: str) -> None:
    print(f"\n-- {title} --")


@contextmanager
def checks(title: str):
    """Every check in the block runs; the test then fails once, listing all of them."""
    section(title)
    first = len(FAILURES)
    yield
    new = FAILURES[first:]
    assert not new, "\n".join(["", *new])


def _one(cur, sql: str, params: tuple = ()) -> tuple:
    cur.execute(sql, params)
    return cur.fetchone()


def _all(cur, sql: str, params: tuple = ()) -> list[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


# ---- the tests --------------------------------------------------------------------------------

def test_company_count(cursor):
    with checks("the table itself"):
        check("companies", _one(cursor, "SELECT count(*) FROM public.companies")[0],
              TOTAL_COMPANIES)


def test_source_priorities(cursor):
    with checks("source priorities — the thing that decides who wins"):
        check("priorities",
              dict(_all(cursor, "SELECT key, priority FROM public.sources")),
              SOURCE_PRIORITIES)


def test_canonical_websites_per_source(cursor):
    with checks("canonical websites, per source (never regress)"):
        got = dict(_all(cursor, """
            SELECT s.key, count(*)
            FROM public.company_canonical_field_sources cf
            JOIN public.company_field_observations o
              ON o.id = cf.company_field_observation_id
            JOIN public.company_source_records r ON r.id = o.company_source_record_id
            JOIN public.sources s ON s.id = r.source_id
            WHERE cf.field_name = 'website'
            GROUP BY 1
        """))
        for key, want in CANONICAL_WEBSITES.items():
            check(f"canonical websites from {key}", got.get(key, 0), want)
        check("canonical websites in total", sum(got.values()), CANONICAL_TOTAL)


def test_candidate_url_alias(cursor):
    with checks("candidate_url is an alias for website, not a second field"):
        check("canonical websites by observation name",
              dict(_all(cursor, """
                  SELECT o.field_name, count(*)
                  FROM public.company_canonical_field_sources cf
                  JOIN public.company_field_observations o
                    ON o.id = cf.company_field_observation_id
                  WHERE cf.field_name = 'website'
                  GROUP BY 1
              """)),
              CANONICAL_BY_OBSERVATION_NAME)
        check("brave's 85 split by observation name",
              dict(_all(cursor, """
                  SELECT o.field_name, count(*)
                  FROM public.company_canonical_field_sources cf
                  JOIN public.company_field_observations o
                    ON o.id = cf.company_field_observation_id
                  JOIN public.company_source_records r ON r.id = o.company_source_record_id
                  JOIN public.sources s ON s.id = r.source_id
                  WHERE cf.field_name = 'website' AND s.key = 'brave_search_discovery'
                  GROUP BY 1
              """)),
              CANONICAL_BRAVE_SPLIT)


def test_other_canonical_fields(cursor):
    with checks("the other two canonical fields"):
        check("cluster and tech_field",
              dict(_all(cursor, """
                  SELECT field_name, count(*) FROM public.company_canonical_field_sources
                  WHERE field_name <> 'website' GROUP BY 1
              """)),
              CANONICAL_OTHER_FIELDS)


def test_resolver_invariants(cursor):
    with checks("resolver invariants"):
        check("no canonical website below the bar",
              _one(cursor, """SELECT count(*) FROM public.company_canonical_field_sources cf
                              JOIN public.company_field_observations o
                                ON o.id = cf.company_field_observation_id
                              WHERE cf.field_name = 'website' AND o.confidence < %s""",
                   (WEBSITE_MIN_CONFIDENCE,))[0], 0)
        check("no directory_url promoted to website",
              _one(cursor, """SELECT count(*) FROM public.company_canonical_field_sources cf
                              JOIN public.company_field_observations o
                                ON o.id = cf.company_field_observation_id
                              WHERE cf.field_name = 'website'
                                AND o.field_name = 'directory_url'""")[0], 0)
        check("one canonical observation per company and field",
              _one(cursor, """SELECT count(*) FROM (
                                SELECT company_id, field_name
                                FROM public.company_field_observations
                                WHERE is_canonical GROUP BY 1, 2 HAVING count(*) > 1) t""")[0], 0)
        check("every canonical row points at a canonical observation",
              _one(cursor, """SELECT count(*) FROM public.company_canonical_field_sources cf
                              JOIN public.company_field_observations o
                                ON o.id = cf.company_field_observation_id
                              WHERE NOT o.is_canonical""")[0], 0)


def test_observations_never_deleted(cursor):
    with checks("observations are never deleted, only un-canonicalised"):
        check("website-ish observations (>= baseline)",
              _one(cursor, """SELECT count(*) FROM public.company_field_observations
                              WHERE field_name IN ('website', 'candidate_url')""")[0]
              >= WEBSITE_OBSERVATION_FLOOR, True)


def test_company_managers_bug(cursor):
    with checks("known live bug: company_managers duplicates every run"):
        total, distinct = _one(cursor, """SELECT count(*), count(DISTINCT (company_id, full_name))
                                          FROM public.company_managers""")
        check("duplication is not getting worse", total <= MANAGER_ROWS_CEILING, True)
        check("distinct managers", distinct, MANAGER_DISTINCT)


def test_spend_guard(cursor):
    with checks("the spend guard cannot fire (documented, still true)"):
        best, at_guard = _one(cursor, """SELECT max(data_quality_score),
                                                count(*) FILTER (WHERE data_quality_score >= 0.90)
                                         FROM public.companies""")
        check("ceiling is still 0.85", float(best), 0.85)
        check("nobody reaches the 0.90 guard", at_guard, 0)


#: Definition order, which is the order both run modes report in.
_TESTS = (
    test_company_count,
    test_source_priorities,
    test_canonical_websites_per_source,
    test_candidate_url_alias,
    test_other_canonical_fields,
    test_resolver_invariants,
    test_observations_never_deleted,
    test_company_managers_bug,
    test_spend_guard,
)


def _run_without_pytest() -> int:
    """Run every test on its own connection, the way the `cursor` fixture would. No pytest."""
    for test in _TESTS:
        if "cursor" not in inspect.signature(test).parameters:
            continue
        try:
            with loaders.db_cursor() as cur:
                test(cur)
        except AssertionError:
            pass        # `check` has already recorded and printed every line of it
    print(f"\n{'FAILED' if FAILURES else 'all checks passed'}")
    for failure in FAILURES:
        print(f"  - {failure}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    if pytest is None:
        raise SystemExit(_run_without_pytest())
    # `-s` so the ok/FAIL table streams instead of being captured.
    raise SystemExit(pytest.main([__file__, "-q", "-s"]))
