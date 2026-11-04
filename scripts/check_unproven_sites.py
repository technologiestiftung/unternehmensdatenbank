"""Websites that name the company but prove nothing: draw 50 for a hand check, then score it (#96).

The population: active companies without a proven website whose guessed site was read, carries the
company's name, prints no register number, and so stays at `confidence.UNVERIFIED_CAP` (0.34), below
the bar. Before any of them is shown, a person checks 50 and this script says how many were right.

    PG_DSN="host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app" \\
      python scripts/check_unproven_sites.py --count
    ... python scripts/check_unproven_sites.py --draw > airflow/data/unproven_sites.csv
    python scripts/check_unproven_sites.py --score airflow/data/unproven_sites.csv

`--draw` takes companies from the fixed `review_sample` first, then fills up in a fixed order (md5 of
the id), so the same database draws the same 50. The file has the company's name for the person
checking: keep it in the git-ignored `airflow/data/`; only the numbers go into
`docs/website-verification.md`. Sole traders are left out, as in #89: their name is a person's name.

In the `verdict` column, one of `VERDICTS`. `--score` needs no database.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))

from enrichment import confidence as conf  # noqa: E402

SAMPLE_SIZE = 50
#: own_site: the company's own website. The others are the wrong answers the issue names;
#: `unreachable` and `unclear` are left out of the precision.
VERDICTS = ("own_site", "directory", "parent", "namesake", "other", "unreachable", "unclear")
#: Evidence flags kept per row, so the score can say whether a narrower rule would be precise enough.
FLAGS = {
    "host_exact": conf.HOST_EXACT,
    "email_at_domain": conf.IMPRINT_EMAIL_ON_DOMAIN,
    "jsonld_name": conf.JSONLD_NAME_MATCH,
    "city_on_page": conf.CITY_ON_PAGE,
}

POPULATION = """
    SELECT DISTINCT ON (o.company_id)
           o.company_id, e.registration_number, e.company_name, o.value_text,
           o.metadata -> 'evidence' AS evidence
    FROM public.company_field_observations o
    JOIN public.sources s ON s.id = o.source_id AND s.key = 'website_discovery'
    JOIN public.v_companies_enriched e ON e.id = o.company_id
    WHERE o.field_name = 'candidate_url'
      AND round(o.confidence::numeric, 2) = round(%(cap)s::numeric, 2)
      AND o.metadata -> 'evidence' ? %(name_on_page)s
      AND o.metadata ->> 'register_number_seen' IS NULL
      AND e.register_status = 'active'
      AND e.website_url IS NULL
      AND e.merged_into IS NULL
      AND e.legal_form IS DISTINCT FROM 'e. K.'
      AND e.company_name !~* '(eingetragene[rn]? kauf|inhaber)'
    ORDER BY o.company_id, o.observed_at DESC
"""


def _rows(cur) -> list[tuple]:  # noqa: ANN001
    cur.execute(POPULATION, {"cap": conf.UNVERIFIED_CAP, "name_on_page": conf.NAME_ON_PAGE})
    return cur.fetchall()


def count() -> None:
    from source_enrichment_common import get_connection

    with get_connection() as connection, connection.cursor() as cur:
        rows = _rows(cur)
    flags = Counter(name for *_, evidence in rows for name, ev in FLAGS.items() if ev in (evidence or []))
    print(f"companies: {len(rows)}")
    for name in FLAGS:
        print(f"  {name}: {flags[name]}")


def draw() -> None:
    from source_enrichment_common import get_connection

    with get_connection() as connection, connection.cursor() as cur:
        rows = {row[0]: row for row in _rows(cur)}
        cur.execute("SELECT company_id FROM public.review_sample WHERE company_id = ANY(%s::uuid[])"
                    " ORDER BY drawn_at, md5(company_id::text)", (list(rows),))
        first = [company_id for (company_id,) in cur.fetchall()]
        cur.execute("SELECT id FROM unnest(%s::uuid[]) AS id ORDER BY md5(id::text)", (list(rows),))
        order = first + [company_id for (company_id,) in cur.fetchall() if company_id not in set(first)]
    writer = csv.writer(sys.stdout)
    writer.writerow(["register", "name", "url", *FLAGS, "verdict", "note"])
    for company_id in order[:SAMPLE_SIZE]:
        _, register, name, url, evidence = rows[company_id]
        writer.writerow([register, name, url, *(int(ev in (evidence or [])) for ev in FLAGS.values()), "", ""])


def score(path: Path) -> dict[str, tuple[int, int]]:
    """(own sites, judged) for all rows and for each flag. Raises on a verdict not in `VERDICTS`."""
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if "verdict" not in (reader.fieldnames or []):
            raise ValueError(f"no 'verdict' column in {path}; save the file comma-separated")
        rows = [row for row in reader if (row["verdict"] or "").strip()]
    unknown = sorted({row["verdict"].strip() for row in rows} - set(VERDICTS))
    if unknown:
        raise ValueError(f"unknown verdicts {unknown}; use one of {VERDICTS}")
    judged = [row for row in rows if row["verdict"].strip() not in {"unreachable", "unclear"}]
    groups = {"all": judged, **{name: [r for r in judged if r.get(name) == "1"] for name in FLAGS}}
    groups["host_exact+email_at_domain"] = [r for r in groups["host_exact"] if r.get("email_at_domain") == "1"]
    result = {name: (sum(r["verdict"].strip() == "own_site" for r in group), len(group))
              for name, group in groups.items()}
    print(f"rows with a verdict: {len(rows)}; judged: {len(judged)}; "
          + ", ".join(f"{v} {n}" for v, n in Counter(r['verdict'].strip() for r in rows).most_common()))
    for name, (own, n) in result.items():
        print(f"  {name:28} {own:3} of {n:3} own sites" + (f"  ({own / n:.0%})" if n else ""))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--count", action="store_true")
    mode.add_argument("--draw", action="store_true")
    mode.add_argument("--score", type=Path)
    args = parser.parse_args()
    count() if args.count else draw() if args.draw else score(args.score)
