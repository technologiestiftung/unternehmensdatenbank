#!/usr/bin/env python3
"""Re-read the register number off pages already downloaded, and see who was wrongly rejected.

## Why this can be free

`combined_discovery` caches every Impressum it fetches, keyed by the site's base URL
(`pipeline.py:427`, namespace `imprint`, 12,658 records on this machine). The HTML is on disk. So a
change to the extraction can be re-run over the whole corpus with **no network, no model, no search
API** — only the regex and some gzip.

That matters because the alternative is a 7-hour `run --force`, and this answers the same question
in under a minute.

## What it is checking

`extract_register_numbers` used to stop at the first run of digits. `ok-personalprofis.de` prints
`HRB 222 442` — ordinary German typesetting — so it read `222`, and `score_candidate` compared 222
against 222442, called it a MISMATCH and applied -200: the penalty meaning "this page belongs to a
different company". A correct website was punished harder than a page with no number at all.

The comparison itself was never the problem — both `scorer._reg_digits` and
`confidence.register_digits` already reduce to digits. The number simply arrived truncated.

So this script asks, for every company that has no website today: does a page we already
downloaded print this company's register number, once the number is read whole?

## What it does NOT do

It writes nothing — not to the database, not to `results.jsonl`. It reports, and the report is the
input to deciding whether a re-run is worth it. A page can print a register number and still not be
that company's site (`search_common.is_own_site_candidate` is the predicate for that question), so
a hit here is evidence, not a verdict.

## Usage

    python scripts/recheck_register_numbers.py
    python scripts/recheck_register_numbers.py --csv recheck.csv
    python scripts/recheck_register_numbers.py --all      # include companies that already have one
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "airflow" / "dags"))

RESULTS = REPO / "airflow" / "data" / "combined_discovery" / "results.jsonl"
PAGES = REPO / "airflow" / "data" / "combined_discovery" / "pages"
os.environ.setdefault("COMBINED_PAGE_CACHE_DIR", str(PAGES))

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("recheck")


def digits(text: str | None) -> str:
    """Every digit, not the first run of them.

    Deliberately different from `scorer._reg_digits`, which takes the first run and is right for
    the register column ("Amtsgericht Osnabrück HRB 222442"). Here both sides are already reduced
    to one entry, so joining the groups of "HRB 222 442" is exactly what is wanted.
    """
    return re.sub(r"\D", "", text or "")


def cached_imprint(page_cache, url: str):
    """The cached Impressum for a site, whichever spelling it was stored under.

    The key is whatever `pipeline._fetch_and_read` was called with — the URL after redirects, which
    normally keeps its trailing slash. `results.jsonl` stores `origin_url(...)`, which strips it.
    Asking for only one spelling found 33 of 643 answers; asking for both finds them.
    """
    for candidate in (url, url.rstrip("/") + "/", url.rstrip("/")):
        record = page_cache.get("imprint", candidate)
        if record:
            return record
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", type=Path)
    parser.add_argument("--all", action="store_true",
                        help="also check companies that already have a website")
    parser.add_argument(
        "--every-page", action="store_true",
        help=(
            "re-parse every cached page rather than only the known-truncated ones. Thorough and "
            "SLOW — 13,389 companies, tens of thousands of BeautifulSoup parses, tens of minutes. "
            "The default restricts to answers whose stored register number is a prefix of the "
            "real one, which is the signature of the truncation bug and takes seconds."
        ),
    )
    args = parser.parse_args()

    from combined_discovery import page_cache
    # The pipeline's own entry point, not the regex directly — so this measures what the pipeline
    # would now see, including whatever text extraction does to the HTML first.
    from url_discovery.imprint_extractor import extract_from_html

    rows = {}
    for line in RESULTS.open(encoding="utf-8"):
        line = line.strip()
        if line:
            r = json.loads(line)
            rows[r["company_id"]] = r
    log.info("%d companies in results.jsonl, cache at %s", len(rows), PAGES)

    hits, checked, cached_pages = [], 0, 0
    for r in rows.values():
        want = digits(r.get("registration_number"))
        if not want:
            continue
        if r.get("website") and not args.all:
            continue
        checked += 1
        for answer in r.get("answers") or []:
            # The signature of the bug: the number stored from this page is a strict PREFIX of the
            # company's real one, which is what a separator-truncated read looks like. Restricting
            # to it turns tens of thousands of page parses into a few dozen.
            if not args.every_page:
                seen = digits(answer.get("register_number_seen"))
                if not seen or seen == want or not want.startswith(seen):
                    continue
            record = cached_imprint(page_cache, answer["url"])
            if not record or not record.get("html"):
                continue
            cached_pages += 1
            found = extract_from_html(record["html"]).get("register_numbers") or []
            if any(digits(entry) == want for entry in found):
                hits.append({
                    "company_name": r["company_name"],
                    "register": r.get("registration_number"),
                    "url": answer["url"],
                    "imprint_url": record.get("imprint_url") or "",
                    "band_today": answer.get("band"),
                    "seen_before": answer.get("register_number_seen") or "",
                    "read_now": next(e for e in found if digits(e) == want),
                })
                break

    log.info("%d companies checked, %d cached pages read", checked, cached_pages)
    log.info("\n%d companies whose OWN register number is on a page we already have:\n", len(hits))
    for h in sorted(hits, key=lambda x: x["company_name"])[:40]:
        log.info("  %-40s %-11s -> %-11s %-7s %s",
                 h["company_name"][:40], h["seen_before"] or "(none)", h["read_now"],
                 h["band_today"], h["url"][:46])
    if len(hits) > 40:
        log.info("  ... and %d more", len(hits) - 40)

    if args.csv:
        with args.csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(hits[0]) if hits else ["company_name"],
                                    delimiter=";")
            writer.writeheader()
            writer.writerows(hits)
        log.info("\nwrote %s", args.csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
