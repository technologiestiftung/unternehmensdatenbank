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

`--round <id>` reads the hand search instead (#89): a Prüfen round of the review queue, drawn with
the DAG `review_round` (field `website`, kind `gap`, size 200, mode `measure`). Its tasks are active
companies without a website, in a random order stored with the round. On the Prüfen page the person
types the website found, or answers "gibt es nicht" (no own website: none found, or only a directory or
social page) or "?". Prints the answers, sorts each found website the way the 110 misses were sorted
(discovery never ran, never generated, another TLD, guessed but lost at the DNS lookup or probe, lost at the page check),
and the test-set rows to append (sole traders left out). `--verify` also reads each found page
again through `pipeline.verify_proposed` (network) and counts which ones prove their company.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))

from enrichment import confidence as conf, pipeline, proposals, url_patterns  # noqa: E402
from enrichment.register_names import SOLE_TRADER_NAME_SQL  # noqa: E402
from source_enrichment_common import get_connection  # noqa: E402
from website_discovery.main import _affixes, _common_words  # noqa: E402

TEST_SET = Path(__file__).with_name("url_discovery_test_set.csv")


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


def host_of(url: str) -> str:
    """The host a person typed, with or without a scheme or path, without `www.`."""
    url = url.strip()
    return (urlparse(url if "//" in url else f"//{url}").hostname or "").removeprefix("www.")


def miss_reason(host: str, guessed: list[str], stored: set[str] | None) -> str:
    """Why discovery missed a website a person found: the buckets of #89. `stored` is None when
    discovery never ran on the company (no `discovery_tier` row)."""
    if stored is None:
        return "discovery never ran"
    if host in stored:
        return "lost at the page check"
    if host in guessed:
        return "guessed, lost at the DNS lookup or probe"
    label = host.split(".", 1)[0]
    if any(g.split(".", 1)[0] == label for g in guessed):
        return "another TLD"
    return "never generated"


def read_round(round_id: str, *, verify: bool) -> None:
    with get_connection() as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT t.company_id::text, p.name, p.city,
                   -- the test set's form of the register entry, as `_companies` reads it
                   (SELECT i.value FROM public.company_identifiers i WHERE i.company_id = t.company_id
                    AND i.scheme = 'register' ORDER BY i.created_at DESC LIMIT 1),
                   v.verdict, v.correction,
                   p.name ~* %s
            FROM public.review_tasks t
            JOIN public.review_rounds r ON r.id = t.round_id AND r.field_name = 'website' AND r.kind = 'gap'
            JOIN public.company_profile_mv p ON p.id = t.company_id
            LEFT JOIN public.review_verdicts v ON v.task_id = t.id
            WHERE t.round_id = %s ORDER BY t.position
            """,
            (SOLE_TRADER_NAME_SQL, round_id),
        )
        tasks = cur.fetchall()
        found = [t for t in tasks if t[5]]
        cur.execute(
            """
            SELECT o.company_id::text, o.field_name, o.value_text FROM public.company_field_observations o
            JOIN public.sources s ON s.id = o.source_id AND s.key = 'website_discovery'
            WHERE o.company_id = ANY(%s::uuid[])
            """,
            ([t[0] for t in found],),
        )
        observations = cur.fetchall()
        stored: dict[str, set[str]] = {c: set() for c, field, _ in observations if field == "discovery_tier"}
        for company_id, field, value in observations:
            if field != "discovery_tier" and company_id in stored:
                stored[company_id].add(host_of(value))
        common, affixes = frozenset(_common_words(cur)), _affixes(cur)
        companies = proposals._companies(cur, [t[0] for t in found]) if verify else {}
    if not tasks:
        sys.exit(f"no website gap round {round_id}")

    answers = Counter("found" if t[5] else {"correct": "gibt es nicht", "unsure": "?"}.get(t[4], "not judged")
                      for t in tasks)
    print(f"{len(tasks)} companies: " + ", ".join(f"{n} {a}" for a, n in answers.most_common()))
    reasons, proves, rows = Counter(), Counter(), []
    for company_id, name, city, register, _, correction, sole_trader in found:
        host = host_of(correction)
        guessed = [g.host.removeprefix("www.")
                   for g in url_patterns.generate_candidates(name, city, common=common, affixes=affixes)]
        reasons[miss_reason(host, guessed, stored.get(company_id))] += 1
        if verify:
            best = pipeline.verify_proposed(companies[company_id], [f"https://{host}"],
                                            method=conf.METHOD_WEB_SEARCH, common=common)
            proven = bool(best) and conf.identifies_company(best[0]["evidence"])
            proves["proves its company" if proven else "proves nothing"] += 1
        if register and not sole_trader:
            rows.append((register, host, "hand", "missed"))
    for counts in (reasons, proves):
        for bucket, n in counts.most_common():
            print(f"  {bucket:42} {n:4}")
    print(f"\nrows for {TEST_SET.name}:")
    csv.writer(sys.stdout).writerows(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--round", help="id of a website gap round (the hand search)")
    parser.add_argument("--verify", action="store_true", help="also read each found page again (network)")
    args = parser.parse_args()
    read_round(args.round, verify=args.verify) if args.round else measure()
