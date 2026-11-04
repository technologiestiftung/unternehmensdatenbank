"""Does `url_patterns.generate_candidates` guess the websites we know? Offline, against the test set (#89).

`scripts/url_discovery_test_set.csv` holds a register entry and the website's host per company,
never a name: the names come from the database. Sole traders (e.K., "Inhaber") are left out, because
their domain can carry the owner's name.

- `discovery=missed`: websites discovery never found. Wikidata or Common Crawl proved them
  (`found_by`), or a person found them by searching (`hand`).
- `discovery=found`: websites discovery proved on play, 300 drawn with seed 89 (292 kept). A change that
  guesses fewer of them breaks existing finds.

Prints, per group, how many hosts are guessed at all, how many by the older rules alone, and the
guesses per company (each one a DNS lookup). Needs the database, no network:

    PG_DSN="host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app" \\
      python scripts/measure_url_guesses.py

`--draw-hand-sample` prints the 200 companies for the hand search as CSV: active, no website, in a
fixed order (md5 of the id), so the same database draws the same sample.
"""
from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))

from enrichment import url_patterns  # noqa: E402
from source_enrichment_common import get_connection  # noqa: E402
from website_discovery.main import _affixes, _common_words  # noqa: E402

TEST_SET = Path(__file__).with_name("url_discovery_test_set.csv")
HAND_SAMPLE_SIZE = 200


def _companies(cur, registers: list[str]) -> dict[str, tuple[str, str | None]]:
    cur.execute(
        """
        SELECT DISTINCT ON (i.value) i.value, e.company_name, e.city
        FROM public.company_identifiers i JOIN public.v_companies_enriched e ON e.id = i.company_id
        WHERE i.scheme = 'register' AND i.value = ANY(%s)
        """,
        (registers,),
    )
    return {value: (name, city) for value, name, city in cur.fetchall()}


def measure() -> None:
    rows = list(csv.DictReader(TEST_SET.open(encoding="utf-8")))
    with get_connection() as connection, connection.cursor() as cur:
        companies = _companies(cur, [r["register"] for r in rows])
        common, affixes = frozenset(_common_words(cur)), _affixes(cur)
    print(f"learned affixes: {affixes}")
    totals: dict[str, Counter] = {}
    for row in rows:
        if row["register"] not in companies:
            continue
        name, city = companies[row["register"]]
        guesses = url_patterns.generate_candidates(name, city, common=common, affixes=affixes)
        hits = [g for g in guesses if g.host.removeprefix("www.") == row["host"]]
        group = totals.setdefault(f"{row['discovery']} / {row['found_by']}", Counter())
        group["companies"] += 1
        group["guesses"] += len(guesses)
        group["guessed"] += bool(hits)
        group["older"] += any(g.pattern not in url_patterns.EXTRA_PATTERNS for g in hits)
    for group, n in sorted(totals.items()):
        print(f"{group:28} {n['companies']:4} companies: {n['guessed']:4} guessed ({n['older']} by the older "
              f"rules), {n['guesses'] / n['companies']:.1f} guesses each")


def draw_hand_sample() -> None:
    with get_connection() as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT registration_number, company_name FROM public.v_companies_enriched
            WHERE register_status = 'active' AND website_url IS NULL
            ORDER BY md5(id::text) LIMIT %s
            """,
            (HAND_SAMPLE_SIZE,),
        )
        writer = csv.writer(sys.stdout)
        writer.writerow(["register", "name", "result (website | directory_only | none)", "url"])
        writer.writerows([register, name, "", ""] for register, name in cur.fetchall())


if __name__ == "__main__":
    draw_hand_sample() if "--draw-hand-sample" in sys.argv else measure()
