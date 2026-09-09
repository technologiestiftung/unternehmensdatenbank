#!/usr/bin/env python3
"""Turn the Northdata export's website column into observations, so the resolver can see it.

## Why this exists

`import_northdata_export.py` lands the export whole and deliberately writes no observations. That
was the right first step — the table became queryable without changing anything the application
serves — but it left **3,283 websites** sitting in `public.northdata_export.website` that nothing
reads. **2,375 of them belong to companies discovery has never found a website for**, while paid
search providers kept looking for URLs that were in the import all along. This is the third time a
column of that import has gone unread: `Status` sat in `metadata` until V13 promoted it, `website`
until now.

This script closes that gap and nothing else. It reads the table, writes one `website` observation
per company, and stops. It decides no winners — `pipeline_coordinator` does that, and it is the only
thing that may.

## Northdata wins, and that is a decision, not an accident

`SOURCE_PRIORITY = 50` against 100 for every other source in the table. The resolver orders winners by
`best_priority ASC` **before** confidence (`pipeline_coordinator/resolver.py`), so this source takes
every company it has an opinion about. Measured over the 908 companies where the export and
discovery both name a website, on 2026-09-03:

    same host                        724   nothing changes
    DIFFERENT host                   184   the export replaces the discovered answer
      of those, same brand, new TLD   39   lambus.de -> lambus.com, pheal.com -> pheal.care
      genuinely different            145   personalleitung.de -> ruf-personal.de

The 184 displace `combined_discovery` 137 (mean confidence 0.9521 — the register-number-verified
ones), `url_discovery` 20, `brave_search_discovery` 15, `exa_search_discovery` 12. `--dry-run` prints
that list in full before anything is written, because 184 rows is short enough to read and a
displaced register match is exactly the thing worth a second look.

**It is reversible, which is what makes the trade safe.** The resolver only sets `is_canonical =
false` on the observation it displaces; it never deletes one. Every discovery answer survives in
`company_field_observations`. To undo the decision:

    UPDATE public.sources SET priority = 200 WHERE key = 'northdata_export';

then trigger `pipeline_coordinator` once. No re-discovery, nothing lost.

## What these observations claim, and what they do not

**Nobody has fetched any of these URLs.** They are a third-party register aggregator's opinion, and
they carry `verified: False` in their metadata to say so. That matters because after this import
`v_companies_enriched` serves websites of two very different provenances, and every number published
in `docs/` for combined discovery alone — 943 websites, 886 of them `high` — counts that pipeline's
own finds and not these. `website_source = 'northdata_export'` separates them.

`OBSERVATION_CONFIDENCE` is a flat 0.50 for every row, and flat is deliberate: the export gives no
per-row evidence to vary it by, and a number invented here would look like a measurement. It exists
only to clear the resolver's `_WEBSITE_MIN_CONF` of 0.35. Priority is what decides this source's
contests, not confidence.

## Re-running

Idempotent, and since 2026-09-09 idempotent **without un-publishing anything in between**.

`company_field_observations` is what `company_canonical_field_sources` points at, and that pointer
**cascades when the observation is deleted**. So this script used to delete its own `website` rows
for the companies it was about to re-write, which took every one of them out of the canonical table
until the next `pipeline_coordinator` run. Running the import therefore dropped **2,815 canonical
websites** — 80% of the application's coverage — and left them dropped for as long as the gap
lasted. It is the same failure that cost 558 websites for four days on 2026-09-03, when the Brave
runs meant to follow the import failed and a failed task never triggers the coordinator.

Now the write is an **upsert**. `insert_text_observation` has a unique key on
`(company_id, field_name, source_id, value_text)` from V17, so re-confirming a URL updates the row
in place, keeps its id, and keeps it canonical. Nothing is un-published, and no coordinator run is
needed to get back to where you were.

One delete remains and it is narrow: a value this export has **superseded**. If a company's website
genuinely changed between snapshots, the old observation is wrong and has to go, canonical pointer
included — leaving it would let a stale URL out-rank the correction, since both carry this source's
flat 0.50 and the resolver then falls back to the value itself. Measured 2026-09-09: on a repeat
import of the same snapshot this deletes **0 rows**, which is the whole point.

Still scoped to the companies in *this* run, not the whole source: a company that drops out of a
later export keeps its old observation rather than silently losing its website.

`--source-file` scopes to one snapshot. The export table is keyed on `(source_file, internal_id)`
precisely so two snapshots a week apart can coexist; importing observations from both at once would
mix them.

## Usage

    python scripts/import_northdata_websites.py --dry-run
    python scripts/import_northdata_websites.py
    python scripts/import_northdata_websites.py --source-file 2026-04-01-osnabrueck

Trigger `pipeline_coordinator` afterwards only if the run reports new or superseded rows — it says
so explicitly, and a re-import of an unchanged snapshot needs nothing. A row updated in place keeps
its id, so it keeps its canonical pointer and `v_companies_enriched` never stops serving it.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "airflow" / "dags"))

# Running on the host means Postgres is on localhost:5433, not on the compose network. The DAG
# default (`host=postgres port=5432`) is right inside a container and wrong outside one, so supply
# a host-shaped default without overriding an explicit PG_DSN. Same line as
# import_combined_discovery.py, and it has to be set before source_enrichment_common is imported --
# that module reads PG_DSN at import time, which is why every import from it lives inside a function.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("import_northdata_websites")

SOURCE_KEY = "northdata_export"
SOURCE_NAME = "Northdata Export (register snapshot)"

#: Lower wins. Every discovery source sits at the default 100, so 50 means this source takes every
#: company it has an opinion about — including the 184 where it disagrees. See the module docstring
#: for what that costs and how to undo it.
SOURCE_PRIORITY = 50

#: Flat for every row, and flat on purpose — the export offers nothing to vary it by. Its only job is
#: to clear `resolver._WEBSITE_MIN_CONF` (0.35); `SOURCE_PRIORITY` is what wins the contests.
OBSERVATION_CONFIDENCE = 0.50

_HTTP_SCHEME = re.compile(r"^https?://", re.IGNORECASE)
_ANY_SCHEME = re.compile(r"^[a-z][a-z0-9+.\-]*:", re.IGNORECASE)


def normalise_website(raw: str) -> str | None:
    """The export's value reduced to `scheme://host`, or None if there is no usable URL in it.

    Three shapes have to be handled, and the first two were counted on the Osnabrück export rather
    than guessed at.

    **3,264 of the 3,283 values carry a path** — one of them is
    `https://www.reinhard-taeuber.de/wp/index.php/de/` — and every other source in this database
    stores a company's *site*, not the page a crawler happened to see, so the path goes.

    **20 values have no scheme at all**, all of them bare hostnames like `www.baeckerei-rahe.de`.
    That is why the scheme is prepended before `origin_url` is called: `origin_url` hands its input
    straight back when `urlparse` finds no scheme, so a bare hostname would quietly be stored where
    a URL belongs.

    **A value carrying some other scheme is refused**, and this one is defensive rather than
    measured — the Osnabrück export has none. It matters because prepending `https://` to
    `ftp://x.de` produces `https://ftp://x.de`, which `urlparse` reads as the host `ftp:` and
    `origin_url` faithfully returns as `https://ftp:`. A URL nobody can visit, stored with no error
    anywhere, is exactly the kind of quiet nonsense a Berlin export could arrive with.
    """
    from search_common import origin_url

    value = (raw or "").strip()
    if not value:
        return None
    if not _HTTP_SCHEME.match(value):
        if _ANY_SCHEME.match(value):
            return None
        value = f"https://{value}"
    reduced = origin_url(value)
    # `origin_url` also hands back its input when there is no netloc, so `https://` on its own
    # reaches here unchanged. An unusable claim is worse than no claim.
    if not _HTTP_SCHEME.match(reduced):
        return None
    # A host has to look like a host: something, a dot, something. `https://ftp:` and `https://x`
    # both get this far otherwise. An explicit port is allowed through — rare for a company site,
    # but a real URL, and silently dropping one would be the worse error.
    host = reduced.split("://", 1)[1]
    host = re.sub(r":\d+$", "", host)
    return reduced if "." in host and not host.endswith(".") and ":" not in host else None


SELECT_SQL = """
SELECT n.internal_id, n.company_id::text, n.name, n.website, n.north_data_url, n.source_file,
       c.company_name
FROM public.northdata_export n
JOIN public.companies c ON c.id = n.company_id
WHERE n.website IS NOT NULL
  AND n.source_file = %s
ORDER BY n.internal_id
"""

#: What the import is about to change, read before it changes it. The host comparison drops the
#: scheme, the `www.` and everything after the host for the same reason
#: `run_combined_discovery.agreement()` does: `http://www.x.de/impressum` and `https://x.de` are one
#: company's website, not two.
#:
#: **The cut is `[/?#]`, not `/`.** Four rows in the Osnabrück export carry a query string with no
#: path at all — `https://www.daza.nl?lang=de` — and cutting at `/` alone leaves `daza.nl?lang=de`
#: to be compared against a stored, already-normalised `daza.nl`. All four then report as websites
#: about to change when the host is identical, which is a diff that cries wolf on exactly the list
#: someone is meant to read by hand.
#:
#: This asks `v_companies_enriched` rather than the observations table, because the question is what
#: the APPLICATION currently serves — a company can hold half a dozen rejected claims and none of
#: them is at risk here.
CHANGES_SQL = """
WITH pair AS (
  SELECT c.company_name,
         lower(regexp_replace(regexp_replace(regexp_replace(n.website, '^https?://', ''),
                              '^www\\.', ''), '[/?#].*$', ''))            AS export_host,
         rtrim(lower(regexp_replace(regexp_replace(regexp_replace(v.website_url, '^https?://', ''),
                              '^www\\.', ''), '[/?#].*$', '')), '.')      AS current_host,
         v.website_source, v.website_confidence
  FROM public.northdata_export n
  JOIN public.companies c ON c.id = n.company_id
  JOIN public.v_companies_enriched v ON v.id = n.company_id
  WHERE n.website IS NOT NULL
    AND n.source_file = %s
    AND v.website_url IS NOT NULL
)
SELECT company_name, current_host, export_host, website_source, website_confidence
FROM pair
WHERE export_host <> current_host
ORDER BY website_confidence DESC NULLS LAST, company_name
"""

BLANK_SQL = """
SELECT count(*)
FROM public.northdata_export n
LEFT JOIN public.company_canonical_field_sources ccfs
       ON ccfs.company_id = n.company_id AND ccfs.field_name = 'website'
WHERE n.website IS NOT NULL
  AND n.company_id IS NOT NULL
  AND n.source_file = %s
  AND ccfs.company_id IS NULL
"""


def source_files() -> list[tuple[str, int]]:
    """Every snapshot in the table with a count of its rows carrying a website, newest label last."""
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(
                """
                SELECT source_file, count(*) FILTER (WHERE website IS NOT NULL)
                FROM public.northdata_export GROUP BY 1 ORDER BY 1
                """
            )
            return [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        connection.close()


def load_rows(source_file: str) -> list[dict[str, Any]]:
    """One dict per export row that has a website AND a matched company, website normalised.

    A row with no `company_id` is dropped and counted: the observation tables are keyed on a
    company, so an unmatched export row has nowhere to land. On the Osnabrück export 13 rows are
    unmatched and **none of them has a website**, so this drops nothing today — but a Berlin export
    matched by a different key could, and a silent drop is how the last three of these went unnoticed.
    """
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(SELECT_SQL, (source_file,))
            raw = cur.fetchall()
    finally:
        connection.close()

    rows, unusable = [], 0
    for internal_id, company_id, export_name, website, north_data_url, src_file, company_name in raw:
        normalised = normalise_website(website)
        if not normalised:
            log.warning("skipping %s (%s): %r is not a usable URL", internal_id, company_name, website)
            unusable += 1
            continue
        rows.append({
            "internal_id": internal_id,
            "company_id": company_id,
            "company_name": company_name,
            "export_name": export_name,
            "website": normalised,
            "raw_website": website,
            "north_data_url": north_data_url,
            "source_file": src_file,
        })
    if unusable:
        log.warning("%d rows held a website column that is not a URL", unusable)
    return rows


def observation_metadata(row: dict[str, Any]) -> dict[str, Any]:
    """What a reader needs to judge this claim without opening the export.

    `verified: False` is the load-bearing one. It is the difference between "a page carrying this
    company's register number said so" and "an aggregator's database said so", and after this import
    both kinds sit side by side in `v_companies_enriched`.
    """
    return {
        "raw_website": row["raw_website"],
        "verified": False,
        "source_file": row["source_file"],
        "internal_id": row["internal_id"],
        "north_data_url": row["north_data_url"],
        "export_name": row["export_name"],
        "why": "third-party register export; the URL has never been fetched or verified",
    }


def summarise(source_file: str, rows: list[dict[str, Any]]) -> None:
    """Counts, then the list of canonical websites this import is about to replace."""
    from source_enrichment_common import get_connection

    log.info("%d companies with an importable website from %s", len(rows), source_file)
    hosts = Counter(r["website"].split("://", 1)[1] for r in rows)
    shared = {h: n for h, n in hosts.items() if n > 1}
    if shared:
        # Usually FINE: a GmbH & Co. KG and its Verwaltungs-GmbH share one site, and Piepenbrock has
        # 26 entities on piepenbrock.de. `scripts/audit_shared_websites.py` is the tool that tells a
        # corporate family from a directory, and it reads the view, so it covers this source for free
        # once the resolver has run.
        log.info("  %d hosts are claimed by more than one company (%d companies); biggest: %s",
                 len(shared), sum(shared.values()),
                 ", ".join(f"{h} x{n}" for h, n in Counter(shared).most_common(5)))
        log.info("  that is usually a corporate family, not a bug — run audit_shared_websites.py after")

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(BLANK_SQL, (source_file,))
            blank = cur.fetchone()[0]
            cur.execute(CHANGES_SQL, (source_file,))
            changes = cur.fetchall()
    finally:
        connection.close()

    log.info("  %d of them have NO canonical website today — pure gain", blank)
    if not changes:
        log.info("  no canonical website would change")
        return

    by_source = Counter(row[3] for row in changes)
    log.info("  %d canonical websites WOULD CHANGE, displacing: %s",
             len(changes), dict(by_source.most_common()))
    print(f"\n=== {len(changes)} canonical websites this import would replace ===\n")
    print(f"{'company':<44}{'loses':<34}{'wins':<30}{'source':<26}conf")
    for company_name, current_host, export_host, website_source, confidence in changes:
        conf = f"{confidence:.4f}" if confidence is not None else "—"
        print(f"{(company_name or '')[:43]:<44}{current_host[:33]:<34}{export_host[:29]:<30}"
              f"{(website_source or '')[:25]:<26}{conf}")
    print()


def write(rows: list[dict[str, Any]]) -> None:
    from source_enrichment_common import (
        ensure_source,
        finish_sync_run,
        get_connection,
        insert_text_observation,
        start_sync_run,
        upsert_source_record,
    )

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur,
                key=SOURCE_KEY,
                name=SOURCE_NAME,
                source_type="register_export",
                metadata={
                    "script": "scripts/import_northdata_websites.py",
                    "writes": "website only",
                    "verified": False,
                },
            )
            # `ensure_source` does not take a priority, and its ON CONFLICT clause does not touch the
            # column — so without this the source lands at the default 100, sits level with the five
            # discovery sources, and the whole priority decision silently does not happen. Set
            # explicitly, every run, so the row cannot drift away from the constant above.
            cur.execute(
                "UPDATE public.sources SET priority = %s, updated_at = now() WHERE id = %s",
                (SOURCE_PRIORITY, source_id),
            )
            log.info("source %s at priority %d (discovery sources are at 100)",
                     SOURCE_KEY, SOURCE_PRIORITY)

            sync_run_id = start_sync_run(cur, source_id=source_id)

            company_ids = [r["company_id"] for r in rows]
            websites = [r["website"] for r in rows]

            # What this source already holds for these companies, read BEFORE anything is written.
            # Two things need it, and neither can be got from `rowcount` afterwards:
            #
            # 1. Splitting inserts from updates. The write below is `ON CONFLICT DO UPDATE`, which
            #    returns a row either way, so `rowcount` is 1 whether it inserted or updated and
            #    cannot tell the sync run which happened.
            # 2. Deciding what the export has SUPERSEDED — see the delete below.
            cur.execute(
                """
                SELECT company_id::text, value_text
                FROM public.company_field_observations
                WHERE source_id = %s AND field_name = 'website' AND company_id = ANY(%s::uuid[])
                """,
                (source_id, company_ids),
            )
            held: set[tuple[str, str]] = {(cid, url) for cid, url in cur.fetchall()}

            # Clear only the values this export has SUPERSEDED — one company, one website here
            # (verified: 3,283 rows, 3,283 distinct companies), so this fires when a company's URL
            # has genuinely changed between snapshots. A row merely being re-confirmed is left alone
            # and updated in place by the upsert below.
            #
            # That distinction is the entire reason V17 added the unique key. A blanket
            # `DELETE ... WHERE source_id = %s AND field_name = 'website'` stood here until
            # 2026-09-09 and cascaded through `company_canonical_field_sources`, so every run took
            # this source's 2,815 canonical websites out of the application until the coordinator
            # ran again. On 2026-09-03 the same shape of delete in the combined-discovery importer
            # cost 558 websites for four days.
            #
            # A superseded row still has to go, canonical pointer and all: the old URL is wrong, and
            # this source's confidence is a flat 0.50 on every row, so the resolver could not prefer
            # the correction on confidence. Measured 2026-09-09: re-importing the same snapshot
            # deletes 0.
            cur.execute(
                """
                DELETE FROM public.company_field_observations o
                WHERE o.source_id = %s
                  AND o.field_name = 'website'
                  AND o.company_id = ANY(%s::uuid[])
                  AND NOT EXISTS (
                    SELECT 1
                    FROM unnest(%s::uuid[], %s::text[]) AS f(company_id, website)
                    WHERE f.company_id = o.company_id
                      AND f.website = o.value_text
                  )
                """,
                (source_id, company_ids, company_ids, websites),
            )
            superseded = cur.rowcount

            created = 0
            updated = 0
            for row in rows:
                record_id, _is_new = upsert_source_record(
                    cur,
                    company_id=row["company_id"],
                    source_id=source_id,
                    # Keyed on internal_id, not company_id, matching the export table's own
                    # (source_file, internal_id) key — so two snapshots of one company coexist.
                    external_id=f"{SOURCE_KEY}:{row['source_file']}:{row['internal_id']}",
                    external_url=row["website"],
                    source_company_name=row["export_name"] or row["company_name"],
                    source_payload={
                        "website": row["website"],
                        "raw_website": row["raw_website"],
                        "source_file": row["source_file"],
                        "internal_id": row["internal_id"],
                        "north_data_url": row["north_data_url"],
                    },
                )
                insert_text_observation(
                    cur,
                    company_id=row["company_id"],
                    source_record_id=record_id,
                    source_id=source_id,
                    sync_run_id=sync_run_id,
                    field_name="website",
                    value_text=row["website"],
                    confidence=OBSERVATION_CONFIDENCE,
                    metadata=observation_metadata(row),
                )
                # Decided from what was held before the write, not from `rowcount`: the upsert
                # returns a row whether it inserted or updated.
                if (row["company_id"], row["website"]) in held:
                    updated += 1
                else:
                    created += 1

            finish_sync_run(
                cur,
                sync_run_id=sync_run_id,
                records_seen=len(rows),
                records_created=created,
                records_updated=updated,
                records_failed=0,
            )
    finally:
        connection.close()

    log.info(
        "%d website observations: %d new, %d updated in place, %d superseded values removed",
        len(rows), created, updated, superseded,
    )
    # An update in place keeps the observation's id, so it keeps its `company_canonical_field_sources`
    # pointer and stays canonical. Only a NEW row or a REMOVED one leaves something for the resolver
    # to do, which is why this is conditional: telling somebody to run the coordinator after a no-op
    # import is how the previous version hid the fact that it had just un-published 2,815 websites.
    if created or superseded:
        log.info(
            "%d new and %d removed rows need resolving. Trigger pipeline_coordinator in Airflow.",
            created, superseded,
        )
    else:
        log.info(
            "nothing changed: every row was already stored with this value, and no canonical "
            "website was touched. No coordinator run needed."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source-file",
        help="which snapshot to import; defaults to the only one in the table",
    )
    parser.add_argument("--dry-run", action="store_true", help="count and summarise, write nothing")
    args = parser.parse_args()

    available = source_files()
    if not available:
        raise SystemExit(
            "public.northdata_export is empty. Run import_northdata_export.py first."
        )

    source_file = args.source_file
    if source_file is None:
        if len(available) > 1:
            raise SystemExit(
                "the table holds more than one snapshot; pass --source-file with one of: "
                + ", ".join(f"{name} ({n} websites)" for name, n in available)
            )
        source_file = available[0][0]
    elif source_file not in {name for name, _ in available}:
        raise SystemExit(
            f"no snapshot called {source_file!r}; the table holds: "
            + ", ".join(name for name, _ in available)
        )

    rows = load_rows(source_file)
    if not rows:
        log.warning("nothing to import for %s", source_file)
        return 1

    summarise(source_file, rows)

    if args.dry_run:
        log.info("--dry-run: nothing written")
        return 0
    write(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
