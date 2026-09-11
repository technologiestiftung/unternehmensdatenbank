#!/usr/bin/env python3
"""Find directories that got promoted, by asking which companies share one website.

## The idea

Two companies holding the same canonical website is usually FINE. German firms split themselves
into a `GmbH & Co. KG` that trades and a `Verwaltungs-GmbH` that manages it, and the pair has one
website between them. `Bäckerei Grave GmbH & Co. KG` and `Bäckerei Grave Verwaltungs GmbH` on
`baeckerei-grave.de` is correct data, not a bug.

What is NOT fine is two UNRELATED companies sharing one, because a URL that belongs to nobody in
particular usually belongs to a directory. `Tankstelle Harmsen GmbH` and `Tankstelle Waschitschek
GmbH` both landed on `tankstelle.org` at confidence 1.0000 — a petrol-station listing site that
gives each company its own page, and prints its register number on it.

So the whole test is: **can the companies sharing this URL be told apart from each other?**

  * `Wenner Immobilien GmbH & Co. KG` vs `Wenner Immobilien Verwaltungs GmbH` — after the legal
    form and the ownership word `Verwaltungs` are dropped, the two names are identical. Neither
    holds a word the other lacks. A family. Leave it alone.
  * `Tankstelle Harmsen` vs `Tankstelle Waschitschek` — one holds `harmsen`, the other holds
    `waschitschek`. Each can be told from the other, so they are two businesses, and a URL both
    of them own is a URL neither of them owns. Suspect.

Formally: a group is suspect when some PAIR in it is mutually distinguishable — A has a word B
lacks and B has a word A lacks.

## Why not just ask whether they share a distinctive word

That was the first attempt and it was wrong in a way worth recording. `name_frequency.common_words`
calls a word common when at least `COMBINED_COMMON_WORD_MIN` (8) companies use it — so a corporate
family with eight entities **makes its own name common**, every member then has no distinctive word
left, and the family reports itself as suspect. Measured: `intan`, `wenner`, `paracelsus` and
`osna` all came back common, and 20 of the 27 first-round "suspects" were ordinary families.

Telling companies apart from each other has no such feedback loop, because it never consults a
frequency count.

## Why this is still a measurement and not a list

Nothing here knows that tankstelle.org is a directory, and nothing should — the repo rule is that
genericness is counted, never enumerated, because the local ISP `osnanet.de` is the kind of thing
no hand-written list ever contains. The only inputs are the company names themselves.

Note what is deliberately NOT dropped: `GENERIC_WORDS`. `Wilhelm Bau` and `Wilhelm Karmann` are
told apart by `bau` and `karmann`, and `bau` is as generic as a word gets. Dropping it would
collapse them into one name and hide six companies sitting on `wilhelm.de`.

The same reasoning finds the other family of bugs in one pass: `url_discovery` put six unrelated
companies on `wilhelm.de` and five on `wolfgang.de`, because that DAG guesses from a first name.
`combined_discovery/url_patterns._stems` refuses exactly that; the DAG package has no such guard.

## Usage

    python scripts/audit_shared_websites.py                  # the report
    python scripts/audit_shared_websites.py --csv out.csv    # same, as a file
    python scripts/audit_shared_websites.py --source url_discovery   # one source only

Read-only. It writes nothing to the database and deletes nothing: which of these to remove is a
judgement call, and this script's job is to put the evidence in front of the person making it.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "airflow" / "dags"))

os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("audit_shared_websites")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", type=Path, help="also write the suspect groups to this file")
    parser.add_argument("--source", help="only groups touching this source key")
    args = parser.parse_args()

    from combined_discovery import url_patterns as up
    from source_enrichment_common import get_connection

    def identifying_words(name: str, city: str | None) -> set[str]:
        """The words that could tell this company from another one in the same town.

        Not `name_frequency.distinctive_words`: that also drops words measured common across the
        table, which is right for "does this domain name a business" and wrong here — see the
        module docstring. Kept: everything except the legal form (removed by `name_words`), the
        ownership words a sister company differs by, the town, and anything under four characters.
        """
        city_words = set(up.name_words(city or ""))
        return {
            word
            for word in up.name_words(name)
            if len(word) >= 4 and not up.is_low_signal(word) and word not in city_words
        }

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            # The whole table, because how common a word is describes the town rather than the
            # companies under the microscope. `common_words` says so in its own docstring.
            cur.execute(
                """
                -- `city` is on `companies`, not on the view, and it is needed: distinctive_words
                -- drops the town from a name, because every company here is in the same one.
                SELECT v.website_url, v.company_name, c.city, v.website_source, v.website_confidence
                FROM public.v_companies_enriched v
                JOIN public.companies c ON c.id = v.id
                WHERE v.website_url IS NOT NULL
                """
            )
            rows = cur.fetchall()
    finally:
        connection.close()

    groups: dict[str, list[tuple[str, str | None, str, float]]] = defaultdict(list)
    for url, name, city, source, confidence in rows:
        groups[url].append((name, city, source, confidence))

    families: list[tuple[str, list]] = []
    suspects: list[tuple[str, list, tuple[str, str]]] = []
    for url, members in groups.items():
        if len(members) < 2:
            continue
        if args.source and not any(m[2] == args.source for m in members):
            continue
        words = {name: identifying_words(name, city) for name, city, _s, _c in members}

        def missing_from(a: set[str], b: set[str]) -> set[str]:
            """Words of `a` that `b` has nothing resembling.

            A prefix counts as a match, because German writes one name several ways and the
            spacing is not a different business: `Bridge Tec Verwaltungs GmbH` against `BridgeTec
            GmbH & Co. KG`, and `bi-markt Bürobedarfs-...` against `...Bürobedarfsgesellschaft`.
            Both were false positives before this. It does not rescue a real pair — neither
            `harmsen`/`waschitschek` nor `sunvoltenergie`/`energie` is a prefix of the other.
            """
            return {w for w in a if not any(w.startswith(o) or o.startswith(w) for o in b)}

        # The first mutually distinguishable pair is the evidence, and it is worth keeping: it is
        # what a person needs to see to agree or disagree with the verdict.
        pair: tuple[str, str] | None = None
        names = sorted(words)
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                if missing_from(words[a], words[b]) and missing_from(words[b], words[a]):
                    pair = (a, b)
                    break
            if pair:
                break
        if pair:
            suspects.append((url, members, pair))
        else:
            families.append((url, members))

    log.info(
        "%d URLs held by more than one company: %d are one business under several legal entities, "
        "%d are companies that can be told apart.",
        len(families) + len(suspects), len(families), len(suspects),
    )

    log.info("\n=== SUSPECT: distinguishable companies on one URL ===")
    for url, members, pair in sorted(suspects, key=lambda g: -len(g[1])):
        by_source = {m[2] for m in members}
        log.info("\n%s   (%d companies, %s)", url, len(members), ", ".join(sorted(by_source)))
        a, b = pair
        log.info("    told apart by: %s  vs  %s",
                 sorted(identifying_words(a, None) - identifying_words(b, None)),
                 sorted(identifying_words(b, None) - identifying_words(a, None)))
        for name, _city, source, confidence in sorted(members):
            log.info("    %.4f  %-26s  %s", confidence, source, name)

    log.info("\n=== suspect groups per source ===")
    per_source: dict[str, int] = defaultdict(int)
    for _url, members, _pair in suspects:
        for source in {m[2] for m in members}:
            per_source[source] += 1
    for source, n in sorted(per_source.items(), key=lambda kv: -kv[1]):
        log.info("  %-28s %d", source, n)

    if args.csv:
        with args.csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["website_url", "companies_on_url", "company_name", "source", "confidence"])
            for url, members, _pair in sorted(suspects, key=lambda g: -len(g[1])):
                for name, _city, source, confidence in sorted(members):
                    writer.writerow([url, len(members), name, source, confidence])
        log.info("\nwrote %s", args.csv)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
