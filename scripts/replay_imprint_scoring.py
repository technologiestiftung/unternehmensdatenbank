#!/usr/bin/env python3
"""Re-score the pages a provider already found, using today's Impressum code. No search API.

## Why this can be free

A provider run costs money in exactly one place: the search call. Everything after it —
fetching the page, finding the Impressum, reading the register number, scoring — is plain HTTP
against the company's own site. Those URLs are already in the database, stored as
`candidate_url` and `directory_url` observations with the score they got at the time.

So a change to the Impressum code can be measured over every company a provider has already
seen, for the price of some bandwidth, without buying a single search.

## What it is measuring right now

`url_discovery.http_client._find_impressum_link` used to resolve a link by gluing strings:
`base.rstrip("/") + "/" + href.lstrip("/")`. That is correct only when `base_url` is an origin,
and `search_discovery_runner` hands over whatever the search engine ranked — usually a deep page.
A root-relative `/service/impressum` on `/planen/service/kontakt` became
`/planen/service/kontakt/service/impressum`, a 404; the code then settled for the contact page,
never read the register number, and scored 20 instead of 120.

`score_candidate` is only two rules (`url_discovery/scorer.py:64`): register number matches
`+100`, mismatches `-200`, postcode matches `+20`. `AUTO_ACCEPT_SCORE` turns >=100 into
confidence 1.0 and `verified_by = register_number_match`; anything less lands in the 0.35-0.65
pattern band. So "did the fix work" is exactly "how many companies cross 100".

## What it does NOT do

It writes nothing. It re-scores in memory and prints the comparison, because deciding whether to
re-run the provider for real is a judgement call and this is the evidence for it.

By default it skips companies whose stored website ALREADY has a register-number match — nothing
there can improve. `--all` includes them, which is the honest way to check the fix has not made
anything worse.

## Usage

    python scripts/replay_imprint_scoring.py --limit 10          # a taste, ~10 companies
    python scripts/replay_imprint_scoring.py                     # every company, ~420 for brave
    python scripts/replay_imprint_scoring.py --source exa_search_discovery
    python scripts/replay_imprint_scoring.py --csv replay.csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "airflow" / "dags"))

os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("urllib3").setLevel(logging.ERROR)
log = logging.getLogger("replay")


@dataclass
class Target:
    """One company, the pages a provider found for it, and what it scored at the time."""

    company_id: str
    company_name: str
    postcode: str | None
    registration_number: str | None
    urls: list[str]
    old_score: int
    old_verified_by: str | None
    old_confidence: float | None


def load_targets(source_key: str, include_verified: bool) -> list[Target]:
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.id, c.company_name, c.postcode, c.registration_number,
                  array_agg(DISTINCT o.value_text) FILTER (
                    WHERE o.field_name IN ('candidate_url', 'directory_url')
                  ) AS candidate_urls,
                  max((o.metadata ->> 'score')::int) FILTER (WHERE o.field_name = 'website')
                    AS old_score,
                  max(o.metadata ->> 'verified_by') FILTER (WHERE o.field_name = 'website')
                    AS old_verified_by,
                  max(o.confidence) FILTER (WHERE o.field_name = 'website') AS old_confidence
                FROM public.company_field_observations o
                JOIN public.sources s ON s.id = o.source_id
                JOIN public.companies c ON c.id = o.company_id
                WHERE s.key = %s
                GROUP BY c.id, c.company_name, c.postcode, c.registration_number
                ORDER BY c.company_name
                """,
                (source_key,),
            )
            rows = cur.fetchall()
    finally:
        connection.close()

    targets = []
    for cid, name, postcode, reg, urls, old_score, old_vb, old_conf in rows:
        if not urls:
            continue
        # Nothing to gain: a register match is already the top of the ladder.
        if old_vb == "register_number_match" and not include_verified:
            continue
        targets.append(
            Target(cid, name, postcode, reg, list(urls), old_score or 0, old_vb, old_conf)
        )
    return targets


def rescore(target: Target) -> tuple[Target, int, str | None]:
    """The best score these pages get under today's code, and the page that earned it.

    `is_own_site_candidate` is applied here for the same reason the runner applies it
    (`search_discovery_runner.py:308`): a register number on a page proves the page is ABOUT a
    company, never that it IS that company's site, and northdata.de prints the register number of
    everybody it lists. Without this filter the first version of this script reported two
    northdata.de pages as newly-won register matches — the exact bug the predicate exists for.
    """
    from imprint_probe import best_imprint
    from search_common import is_own_site_candidate
    from url_discovery.scorer import AUTO_ACCEPT_SCORE, score_candidate

    best_score = 0
    best_url: str | None = None
    for url in target.urls:
        try:
            probed = best_imprint(url)
        except Exception:  # noqa: BLE001  an unreachable page is not a failure of the replay
            continue
        if not probed:
            continue
        _html, _imprint_url, extracted = probed
        score = score_candidate(extracted, target)
        if not is_own_site_candidate(url, target.company_name, score, AUTO_ACCEPT_SCORE):
            continue
        if best_url is None or score > best_score:
            best_score, best_url = score, url
    return target, best_score, best_url


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default="brave_search_discovery")
    parser.add_argument("--limit", type=int, help="stop after this many companies")
    parser.add_argument("--workers", type=int, default=8, help="parallel page fetches")
    parser.add_argument("--all", action="store_true",
                        help="include companies that already have a register-number match")
    parser.add_argument("--csv", type=Path, help="write the per-company comparison here")
    args = parser.parse_args()

    # From the scorer, not from the runner that re-exports it: the runner pulls in Airflow and
    # every provider client, and this script needs neither.
    from url_discovery.scorer import AUTO_ACCEPT_SCORE

    targets = load_targets(args.source, include_verified=args.all)
    if args.limit:
        targets = targets[: args.limit]
    pages = sum(len(t.urls) for t in targets)
    log.info(
        "%s: %d companies, %d stored pages to re-read. No search calls.",
        args.source, len(targets), pages,
    )
    if not targets:
        log.warning("nothing to replay")
        return 1

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i, (target, new_score, best_url) in enumerate(pool.map(rescore, targets), start=1):
            results.append((target, new_score, best_url))
            if i % 25 == 0:
                log.info("  %d/%d", i, len(targets))

    def verdict(score: int) -> str:
        if score >= AUTO_ACCEPT_SCORE:
            return "register_number_match"
        return "url_pattern" if score >= 0 else "rejected"

    gained = [r for r in results if r[1] >= AUTO_ACCEPT_SCORE and r[0].old_score < AUTO_ACCEPT_SCORE]
    lost = [r for r in results if r[1] < AUTO_ACCEPT_SCORE and r[0].old_score >= AUTO_ACCEPT_SCORE]
    worse = [r for r in results if r[1] < r[0].old_score]

    log.info("\n=== GAINED a register-number match (%d) ===", len(gained))
    for target, new_score, best_url in sorted(gained, key=lambda r: r[0].company_name):
        log.info("  %-44s %3d -> %3d   %s", target.company_name[:44], target.old_score, new_score, best_url)

    if lost:
        log.info("\n=== LOST a register-number match (%d) — investigate before trusting this ===", len(lost))
        for target, new_score, _url in lost:
            log.info("  %-44s %3d -> %3d", target.company_name[:44], target.old_score, new_score)

    log.info(
        "\n%d companies replayed: %d gained a register match, %d lost one, %d scored lower than before.",
        len(results), len(gained), len(lost), len(worse),
    )
    log.info("old verdicts: %s", dict(Counter(verdict(t.old_score) for t, _s, _u in results)))
    log.info("new verdicts: %s", dict(Counter(verdict(s) for _t, s, _u in results)))

    if args.csv:
        with args.csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, delimiter=";")
            writer.writerow([
                "company_name", "old_score", "new_score", "old_verified_by", "new_verified_by",
                "old_confidence", "best_url", "pages_tried",
            ])
            for target, new_score, best_url in sorted(results, key=lambda r: -(r[1] - r[0].old_score)):
                writer.writerow([
                    target.company_name, target.old_score, new_score, target.old_verified_by,
                    verdict(new_score), target.old_confidence, best_url, len(target.urls),
                ])
        log.info("wrote %s", args.csv)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
