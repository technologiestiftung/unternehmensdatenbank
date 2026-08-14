"""Run the combined discovery pipeline over a fixed sample of companies and write the list.

Three free methods — guessed URLs, OpenStreetMap, and optionally a self-hosted search engine
plus a self-hosted language model — run over the same companies, and every URL they propose
gets one confidence number from `combined_discovery.confidence`.

Writes NOTHING to the database. It reads `public.companies`, probes the web, and writes two
files. Storing results is the DAGs' job; a measurement that also mutated state could not be
re-run and compared.

    run            do the work, one JSON line per company, resumable
    report         read the JSON lines and print the scorecard
    csv            read the JSON lines and write the handover CSV
    labels         write a worksheet of ~60 companies for a human to label by hand
    labels-report  read the worksheet back: precision, rejection accuracy, and the ceiling

`report` counts what the pipeline did. `labels-report` is the only one that says whether what it
did was RIGHT — everything else compares the pipeline against itself.

From the repo root, with Postgres up:

    python scripts/run_combined_discovery.py run --limit 1000
    python scripts/run_combined_discovery.py report
    python scripts/run_combined_discovery.py csv

The sample is `ORDER BY md5(id::text) LIMIT n` — the same companies every time, so a second
run measures the same population and a `--limit 50` run is a prefix of the 1000.

Output goes to `airflow/data/combined_discovery/`, which is gitignored. The CSV holds company
names, addresses, URLs and confidence numbers. It deliberately holds no manager names: those
are personal data, and nothing here needs them.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

# Running on the host means Postgres is on localhost:5433, not on the compose network. The
# DAG default (`host=postgres port=5432`) is right inside a container and wrong outside one,
# so supply a host-shaped default without overriding an explicit PG_DSN.
os.environ.setdefault(
    "PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"
)
# Same for the two self-hosted services: service names only resolve inside compose.
#
# `host.docker.internal` is in the same category and catches people out in the opposite direction.
# It is the correct value in `.env`, because that file configures the containers, and it is wrong
# here — it does not resolve on the host. Anyone who sources `.env` into their shell before running
# this script would otherwise get a connection error pointing at Ollama rather than at the address.
_OLLAMA_URL = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
if "://ollama" in _OLLAMA_URL or "host.docker.internal" in _OLLAMA_URL:
    os.environ["OLLAMA_BASE_URL"] = "http://localhost:11434"
if "://searxng" in os.environ.get("SEARXNG_BASE_URL", "http://searxng:8080"):
    os.environ.setdefault("SEARXNG_BASE_URL", "http://localhost:8888")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("urllib3").setLevel(logging.ERROR)
log = logging.getLogger("combined_discovery")

OUT_DIR = Path(
    os.getenv("COMBINED_OUT_DIR")
    or (
        Path("/opt/airflow/data")
        if Path("/opt/airflow/data").is_dir()
        else REPO_ROOT / "airflow" / "data"
    )
) / "combined_discovery"

RESULTS_PATH = OUT_DIR / "results.jsonl"
OSM_CACHE_PATH = OUT_DIR / "osm_cache.json"
OSM_SNAPSHOT_PATH = OUT_DIR / "osm_snapshot.json"
CSV_PATH = OUT_DIR / "combined_discovery_urls.csv"
PAGE_CACHE_PATH = OUT_DIR / "pages"

# Keep every downloaded page, for this script only.
#
# `combined_discovery.page_cache` is off unless this variable is set, and it stays off inside Airflow
# deliberately: a DAG must see the web as it is today, and a cache would let it report a page that
# went offline last month as a company's current website. This script is a measurement rather than a
# data load, and a measurement wants the opposite — the same input every time, so that a re-scored
# comparison is a comparison.
#
# The payoff is that changing what gets read out of a page no longer costs 2.4 hours. Roughly 150-300
# MB per 1,000 companies, gzipped, in a gitignored directory. `setdefault`, so an explicit
# COMBINED_PAGE_CACHE_DIR still wins and setting it to empty switches caching off.
os.environ.setdefault("COMBINED_PAGE_CACHE_DIR", str(PAGE_CACHE_PATH))

#: Same ordering as scripts/ac_measurement.py, for the same reason: md5 of the id spreads the
#: sample across the whole table, and it is stable, so every run sees the same companies.
SAMPLE_SQL = """
SELECT id::text, company_name, address, postcode, city, registration_number,
       legal_form, wz_code, business_purpose
FROM public.companies
WHERE company_name IS NOT NULL
ORDER BY md5(id::text)
LIMIT %s
"""

_WRITE_LOCK = threading.Lock()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a results file, splitting on newlines ONLY.

    Not `splitlines()`: German page text carries raw U+2028 and U+2029, which `splitlines`
    treats as line breaks and `json.dumps` has no reason to escape. One record then gets cut
    in two and reads exactly like a truncated write. Same trap as in ac_measurement.py.
    """
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").split("\n"), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{path}:{number} is not valid JSON: {exc}") from exc
    return rows


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _WRITE_LOCK, path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


#: The stages, in the order they are reported everywhere. `llm` and `judge` are both local models
#: and are easy to mix up: `llm` searches with SearXNG and picks a URL out of the results, `judge`
#: searches for nothing and only reads pages the other stages already downloaded.
ALL_STAGES = ("url", "osm", "llm", "judge")


def latest_rows(path: Path) -> list[dict[str, Any]]:
    """One row per company: the newest one.

    The results file is append-only, so adding a method to a company that was already run
    writes a second line for it. The newest line is the complete one — it was produced by
    running the whole requested method set together, not by patching the old row. Reading
    "last one wins" is what makes adding a method later safe.
    """
    latest: dict[str, dict[str, Any]] = {}
    for row in read_jsonl(path):
        company_id = row.get("company_id")
        if company_id:
            latest[company_id] = row
    return list(latest.values())


def stages_run_for(row: dict[str, Any]) -> set[str]:
    """Which methods have actually been run for this company.

    Rows written before this field existed do not carry it. They came from the old default of
    guessing URLs plus OpenStreetMap, so that is what they are credited with.
    """
    recorded = row.get("stages_run")
    if recorded is None:
        return {"url", "osm"}
    return set(recorded)


def tier_status(row: dict[str, Any], stage: str) -> str:
    """What happened to one method for one company. Four values, and the distinction matters.

    found        the method ran and proposed at least one URL
    no_result    the method ran and proposed nothing. We looked.
    error        the method ran and raised; see the errors column
    never_tried  the method did not run for this company. NOT evidence of anything.

    `no_result` and `never_tried` must never be collapsed. Reading 850 `never_tried` rows as
    "these businesses have no website" is the single easiest way to misreport this data.
    """
    method_name = {"url": "url_pattern", "osm": "osm", "llm": "local_llm", "judge": "llm_judge"}[
        stage
    ]
    if stage not in stages_run_for(row):
        return "never_tried"
    for error in row.get("errors") or []:
        if error.startswith(("url_patterns:", "osm:", "local_llm:", "llm_judge:")):
            if method_name in error:
                return "error"

    # The judge is not a method that proposes URLs, so "did it appear in an answer's methods" —
    # the test the other three use — would report `no_result` for every judged company. It ran if
    # it actually looked at something.
    if stage == "judge":
        if row.get("judge_error"):
            return "error"
        if any(answer.get("judged") for answer in row.get("answers") or []):
            return "found"
        # It ran, and every candidate was already disqualified or already proven. Nothing to judge
        # is a real outcome, not a failure — see the note above about never collapsing these.
        return "no_result"

    for answer in row.get("answers") or []:
        if method_name in (answer.get("methods") or []):
            return "found"
    return "no_result"


#: What the database already believes about these companies' websites. `website_url` is a column
#: on this VIEW only — the underlying field is called `website` and lives in
#: `company_canonical_field_sources`. Reading the view keeps us honest about what the
#: application actually serves.
DB_WEBSITE_SQL = """
SELECT id::text, website_url, website_source, website_confidence
FROM public.v_companies_enriched
WHERE id::text = ANY(%s)
"""


def load_db_websites(company_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Company id -> the website already stored, its source and its confidence.

    Companies with no stored website are absent from the result, not present with None, so the
    caller can tell "the database has nothing" from "the company is not in the database".
    """
    if not company_ids:
        return {}
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(DB_WEBSITE_SQL, (company_ids,))
            rows = cur.fetchall()
    finally:
        connection.close()
    return {
        r[0]: {"website": r[1], "source": r[2], "confidence": r[3]}
        for r in rows
        if r[1]
    }


def _compare_host(url: str) -> str:
    """The part of a URL that decides whether two URLs are the same site.

    `http://www.x.de/impressum` and `https://x.de` are the same company's website, so the scheme,
    the `www.` and the path all have to go before comparing. `search_common.origin_url` keeps the
    scheme and the `www.` — right for storing a website, too strict for comparing two of them.
    """
    from urllib.parse import urlparse

    host = (urlparse(url).hostname or url).strip().lower()
    return host.removeprefix("www.").rstrip(".")


def agreement(ours: str | None, theirs: str | None) -> str:
    """Do we and the database name the same website? Five answers, all of them useful.

    same_origin  both point at the same host, ignoring scheme, `www.` and path
    different    both name a website and they are different hosts — read these by hand
    db_blank     we found one, the database has none. This is a candidate to import.
    ours_blank   the database has one, we found none. Our methods missed something.
    both_blank   neither has one
    """
    if ours and theirs:
        return "same_origin" if _compare_host(ours) == _compare_host(theirs) else "different"
    if ours:
        return "db_blank"
    if theirs:
        return "ours_blank"
    return "both_blank"


def all_company_names() -> list[str]:
    """Every company name in the table. Used to measure which words are too common to identify
    a business — see `osm_snapshot.common_words`. The whole table, not the sample: a word's
    commonness is a property of the town, not of the 100 companies we happen to be measuring."""
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(
                "SELECT company_name FROM public.companies WHERE company_name IS NOT NULL"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        connection.close()


def load_sample(limit: int) -> list[Any]:
    from source_enrichment_common import CompanyCandidate, get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(SAMPLE_SQL, (limit,))
            rows = cur.fetchall()
    finally:
        connection.close()
    return [
        CompanyCandidate(
            id=r[0], company_name=r[1], address=r[2], postcode=r[3], city=r[4],
            registration_number=r[5], legal_form=r[6], wz_code=r[7], business_purpose=r[8],
        )
        for r in rows
    ]


def run(
    limit: int,
    stages: set[str],
    workers: int,
    refresh_osm: bool = False,
    force: bool = False,
) -> None:
    from source_enrichment_common import build_search_queries

    from combined_discovery import (
        llm_judge,
        local_llm,
        name_frequency,
        osm_lookup,
        osm_snapshot,
        pipeline,
    )

    use_llm = "llm" in stages
    if use_llm:
        usable, reason = local_llm.availability()
        if not usable:
            log.warning("Local search + model stage is OFF: %s", reason)
            log.warning("Continuing with the other stages. Nothing else changes.")
            use_llm = False
        else:
            log.info("Local search + model stage is ON: %s", reason)

    # Checked separately from `llm`, and against Ollama only. The judge reads pages that are
    # already downloaded, so a blocked SearXNG says nothing about whether it can run — gating it on
    # `local_llm.availability()` would switch it off on every network where search engines refuse.
    use_judge = "judge" in stages
    if use_judge:
        usable, reason = llm_judge.availability()
        if not usable:
            log.warning("Judge stage is OFF: %s", reason)
            log.warning("Continuing with the other stages. Nothing else changes.")
            use_judge = False
        else:
            log.info("Judge stage is ON: %s", reason)

    # What actually gets run. A model stage drops out when its service is not reachable, and a
    # stage that drops out must not be recorded as having run — that is the difference between
    # "we asked the model and it found nothing" and "we never asked".
    effective = (
        {s for s in stages if s not in ("llm", "judge")}
        | ({"llm"} if use_llm else set())
        | ({"judge"} if use_judge else set())
    )

    companies = load_sample(limit)
    already = {row["company_id"]: stages_run_for(row) for row in latest_rows(RESULTS_PATH)}

    # A company is pending when a method we want has never been run for it. The whole method
    # set is then re-run for that company, not just the missing one: confidence depends on how
    # many methods agree on a host, so a partial re-run would produce a number that no
    # complete run could produce. Re-running is cheap — OSM answers come from the cache file
    # and URL guessing is about five seconds.
    # `--force` re-runs companies that already have every requested method. Needed when HOW a
    # method works changed rather than which methods ran: the OSM stage switched from a
    # per-company Overpass query to a city-wide snapshot, and a sample where some rows came from
    # each is not one measurement.
    pending = (
        list(companies) if force
        else [c for c in companies if effective - already.get(c.id, set())]
    )
    log.info(
        "%d companies in sample, %d already have %s, %d to run | stages: %s",
        len(companies),
        len(companies) - len(pending),
        "+".join(sorted(effective)) or "nothing",
        len(pending),
        ", ".join(sorted(effective)) or "none",
    )
    if not pending:
        return

    # Always built, even when this run was not asked for OpenStreetMap. A company that already
    # had an OSM lookup keeps it on re-run, and without the cache that re-run would go back to
    # the public Overpass service — 24 seconds and a rate limit, for an answer already on disk.
    cache = osm_lookup.OsmCache(OSM_CACHE_PATH)

    # One query for every usable map object in the city, then matching happens offline. Measured:
    # 6 s for ~2000 Osnabrück objects, against 11-15 s PER COMPANY plus rate limits for the
    # per-company queries. If the download fails, the OSM stage is switched off rather than
    # silently matching against nothing — an empty snapshot reads exactly like "the map knows
    # nothing about this city".
    # Which words are too common in THIS town to identify one business. Measured over every
    # company name in the table, not just the sample, and used by both stages: the OSM matcher
    # asks whether a shared word proves two businesses are one, and the URL side asks whether a
    # domain names an industry. One measurement, one meaning.
    common = name_frequency.common_words(all_company_names())
    log.info("%d words appear in %d+ company names and cannot identify a business",
             len(common), name_frequency.MIN_COMPANIES)

    snapshot = None
    # Email domains shared by several mapped businesses. Measured from the snapshot, so it stays
    # empty when there is no snapshot — and an empty set refuses nothing, which is the safe
    # direction: without the measurement the email-domain candidate is simply never blocked, and
    # its own scoring still has to carry it.
    mail_hosts: set[str] = set()
    if "osm" in effective:
        try:
            snapshot = osm_snapshot.load_or_fetch(OSM_SNAPSHOT_PATH, refresh=refresh_osm)
            snapshot.set_generic_words(common)
            mail_hosts = name_frequency.common_email_domains(snapshot.records)
            log.info(
                "%d email domains are shared by %d+ mapped businesses and name none of them",
                len(mail_hosts),
                name_frequency.MIN_BUSINESSES_PER_EMAIL_DOMAIN,
            )
        except Exception as exc:  # noqa: BLE001
            log.error("OSM snapshot unavailable (%s)", exc)
            log.error("Dropping the OSM stage for this run. Nothing else changes.")
            effective = effective - {"osm"}
            if not force:
                pending = [c for c in companies if effective - already.get(c.id, set())]
            if not pending:
                return

    started = time.monotonic()
    counter = {"n": 0, "found": 0}

    def one(company: Any) -> None:
        # Everything already run for this company plus everything we are running now. Re-running
        # a company never loses a method it already had.
        stages_for_row = sorted((already.get(company.id, set()) | effective) & set(ALL_STAGES))
        try:
            result = pipeline.discover(
                company,
                use_url_patterns="url" in stages_for_row,
                use_osm="osm" in stages_for_row,
                use_local_llm="llm" in stages_for_row,
                use_llm_judge="judge" in stages_for_row,
                osm_cache=cache,
                osm_snapshot=snapshot,
                common_words=common,
                mail_hosts=mail_hosts,
                search_queries=(
                    build_search_queries(company) if "llm" in stages_for_row else None
                ),
            )
            row = asdict(result)
        except Exception as exc:  # noqa: BLE001
            log.exception("failed for %s", company.id)
            row = {
                "company_id": company.id,
                "company_name": company.company_name,
                "website": None,
                "confidence_value": 0.0,
                "band": "error",
                "errors": [f"{type(exc).__name__}: {exc}"],
            }
        row["stages_run"] = stages_for_row
        append_jsonl(RESULTS_PATH, row)
        with _WRITE_LOCK:
            counter["n"] += 1
            counter["found"] += int(bool(row.get("website")))
            n = counter["n"]
        if n % 10 == 0 or n == len(pending):
            rate = n / max(time.monotonic() - started, 1e-6)
            remaining = (len(pending) - n) / rate if rate else 0
            log.info(
                "[%d/%d] %d with a website (%.0f%%) | %.1f companies/min | ~%.0f min left",
                n, len(pending), counter["found"], 100.0 * counter["found"] / n,
                rate * 60, remaining / 60,
            )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, pending))

    log.info("Done. %d companies written to %s", counter["n"], RESULTS_PATH)


# ---------------------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------------------


#: Bands from worst to best, so "did the judge move this row up or down" is a subtraction.
_BAND_ORDER = ("reject", "low", "medium", "high")


def _report_judge(rows: list[dict[str, Any]], n: int) -> None:
    """What the local model actually changed. Silent when the judge never ran.

    The number that matters is not how many pages it read but how many verdicts it *moved*. A judge
    that agrees with the hand-written rules on every row is costing 15 seconds a page and buying
    nothing — and that is a perfectly possible outcome, so it has to be visible.
    """
    judged = [r for r in rows if tier_status(r, "judge") == "found"]
    if not judged:
        return

    print("\n-- what the local model said (judge) --")
    verdicts: dict[str, int] = {}
    for r in judged:
        key = r.get("judge_verdict") or "(no verdict)"
        verdicts[key] = verdicts.get(key, 0) + 1
    for verdict, count in sorted(verdicts.items(), key=lambda kv: -kv[1]):
        print(f"{verdict:<34}{count:>7}  {_pct(count, n)}")

    moved_up, moved_down = [], []
    for r in judged:
        before, after = r.get("band_without_judge"), r.get("band")
        if not before or before == after or after not in _BAND_ORDER or before not in _BAND_ORDER:
            continue
        (moved_up if _BAND_ORDER.index(after) > _BAND_ORDER.index(before) else moved_down).append(r)

    print("\n-- did it change the answer --")
    print(f"{'pages read':<34}{len(judged):>7}")
    print(f"{'moved a company UP a band':<34}{len(moved_up):>7}")
    print(f"{'moved a company DOWN a band':<34}{len(moved_down):>7}")
    print(f"{'agreed with the rules':<34}{len(judged) - len(moved_up) - len(moved_down):>7}")
    if moved_up or moved_down:
        print(
            "\n  These are the rows to check by hand. Nothing here says the model was RIGHT —\n"
            "  only that it disagreed. Read judge_reason in the CSV against the actual page."
        )
        for r in (moved_up + moved_down)[:10]:
            print(
                f"    {(r.get('company_name') or '')[:32]:<32} "
                f"{r.get('band_without_judge')} -> {r.get('band')}  {r.get('website') or '(none)'}"
            )


def _pct(part: int, whole: int) -> str:
    return f"{100.0 * part / whole:5.1f}%" if whole else "    -"


def sample_rows(limit: int | None) -> list[dict[str, Any]]:
    """The result rows for the first `limit` companies of the sample, newest row per company.

    Without this, a report over a results file that has grown past the sample describes a mixture:
    148 rows where 100 had OpenStreetMap run and 48 did not, printed as "48 not run". The number
    is true and the population is not the one that was measured.
    """
    rows = latest_rows(RESULTS_PATH)
    if not rows:
        raise SystemExit(f"nothing in {RESULTS_PATH}; run first")
    if not limit:
        return rows
    wanted = {c.id for c in load_sample(limit)}
    scoped = [r for r in rows if r.get("company_id") in wanted]
    log.info("Scoped to the first %d companies of the sample: %d rows", limit, len(scoped))
    return scoped


def report(limit: int | None = None) -> None:
    rows = sample_rows(limit)
    n = len(rows)
    with_site = [r for r in rows if r.get("website")]

    print(f"\n=== Combined discovery over {n} companies ===\n")
    print(f"{'companies':<34}{n:>7}")
    print(f"{'with a website at >= 0.35':<34}{len(with_site):>7}  {_pct(len(with_site), n)}")

    print("\n-- by confidence band --")
    for band in ("high", "medium", "low"):
        count = sum(1 for r in with_site if r.get("band") == band)
        print(f"{band:<34}{count:>7}  {_pct(count, n)}")

    print("\n-- per method: did it run, and did it produce a URL --")
    print(f"{'method':<22}{'found':>7}{'nothing':>9}{'error':>7}{'not run':>9}")
    labels = {
        "url": "url_pattern",
        "osm": "osm",
        "llm": "ollama_searxng",
        # `found` here means "the model was asked about at least one page", not "it found a URL" —
        # the judge proposes nothing. `nothing` means every candidate was already settled.
        "judge": "llm_judge (read only)",
    }
    for stage in ALL_STAGES:
        tally = {"found": 0, "no_result": 0, "error": 0, "never_tried": 0}
        for r in rows:
            tally[tier_status(r, stage)] += 1
        print(
            f"{labels[stage]:<22}{tally['found']:>7}{tally['no_result']:>9}"
            f"{tally['error']:>7}{tally['never_tried']:>9}"
        )

    # The two ways of having no website are not the same claim, and the difference is the whole
    # reason the columns exist. Printed every time, because these numbers get pasted into email.
    all_ran = [r for r in rows if not any(tier_status(r, s) == "never_tried" for s in ALL_STAGES)]
    no_site_all_ran = sum(1 for r in all_ran if not r.get("website"))
    partial = [r for r in rows if any(tier_status(r, s) == "never_tried" for s in ALL_STAGES)]
    no_site_partial = sum(1 for r in partial if not r.get("website"))
    print("\n-- the two kinds of blank --")
    print(f"{'all methods ran, none found a URL':<34}{no_site_all_ran:>7}  {_pct(no_site_all_ran, n)}")
    print(f"{'a method never ran, and no URL yet':<34}{no_site_partial:>7}  {_pct(no_site_partial, n)}")
    print(
        "\n  Only the first line supports \"this business has no website\". The second line means\n"
        "  we did not finish looking. Counting it as a miss understates coverage; calling it\n"
        "  \"no website\" is simply false."
    )

    print("\n-- what proved it (website_verified_by) --")
    reasons: dict[str, int] = {}
    for r in with_site:
        reasons[r.get("verified_by") or "?"] = reasons.get(r.get("verified_by") or "?", 0) + 1
    for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
        print(f"{reason:<34}{count:>7}  {_pct(count, n)}")

    _report_judge(rows, n)

    print("\n-- which method found the winner --")
    methods: dict[str, int] = {}
    for r in with_site:
        key = "+".join(r.get("methods") or []) or "?"
        methods[key] = methods.get(key, 0) + 1
    for key, count in sorted(methods.items(), key=lambda kv: -kv[1]):
        print(f"{key:<34}{count:>7}  {_pct(count, n)}")

    print("\n-- which naming rule guessed the winner --")
    patterns: dict[str, int] = {}
    for r in with_site:
        patterns[r.get("pattern") or "(not a guess)"] = patterns.get(r.get("pattern") or "(not a guess)", 0) + 1
    for key, count in sorted(patterns.items(), key=lambda kv: -kv[1]):
        print(f"{key:<34}{count:>7}  {_pct(count, n)}")

    print("\n-- stage 1: URL guessing --")
    generated = sum(r.get("candidates_generated") or 0 for r in rows)
    resolved = sum(r.get("candidates_resolved") or 0 for r in rows)
    probed = sum(r.get("candidates_probed") or 0 for r in rows)
    parked = sum(r.get("parked_rejected") or 0 for r in rows)
    print(f"{'domains guessed':<34}{generated:>7}  ({generated / n:.1f} per company)")
    print(f"{'…that exist in DNS':<34}{resolved:>7}  {_pct(resolved, generated)}")
    print(f"{'…pages actually fetched':<34}{probed:>7}")
    print(f"{'…thrown out as parked/empty':<34}{parked:>7}")

    print("\n-- stage 2: OpenStreetMap --")
    osm_seen = sum(1 for r in rows if (r.get("osm_objects_seen") or 0) > 0)
    osm_matched = sum(1 for r in rows if r.get("osm_id"))
    osm_site = sum(1 for r in rows if r.get("osm_website"))
    # "Objects with a similar name", not "objects in the city": the snapshot holds every usable
    # map object in Osnabrück, and this counts the ones whose name was close enough to score.
    print(f"{'objects with a similar name':<34}{osm_seen:>7}  {_pct(osm_seen, n)}")
    print(f"{'…one matched name+address':<34}{osm_matched:>7}  {_pct(osm_matched, n)}")
    print(f"{'…and carried a website tag':<34}{osm_site:>7}  {_pct(osm_site, n)}")

    llm_rows = [r for r in rows if r.get("llm_website") or r.get("llm_error")]
    if llm_rows:
        print("\n-- stage 3: SearXNG + Ollama --")
        picked = sum(1 for r in rows if r.get("llm_website"))
        print(f"{'model picked a URL':<34}{picked:>7}  {_pct(picked, n)}")

    print("\n-- why the rest have nothing --")
    without = [r for r in rows if not r.get("website")]
    refusals: dict[str, int] = {}
    for r in without:
        answers = r.get("answers") or []
        if not answers:
            key = "no candidate survived DNS + fetch"
        else:
            key = f"best candidate refused: {answers[0].get('verified_by') or '?'}"
        refusals[key] = refusals.get(key, 0) + 1
    for key, count in sorted(refusals.items(), key=lambda kv: -kv[1]):
        print(f"{key:<44}{count:>7}  {_pct(count, n)}")

    errors = sum(1 for r in rows if r.get("errors"))
    elapsed = [r.get("elapsed_s") or 0 for r in rows]
    print(f"\n{'companies with an error':<34}{errors:>7}")
    print(f"{'median seconds per company':<34}{sorted(elapsed)[len(elapsed) // 2]:>7.1f}")
    print()


CSV_COLUMNS = [
    "company_id", "company_name", "postcode", "city", "address", "registration_number",
    "url", "confidence", "band", "verified_by", "method", "pattern",
    "evidence", "why", "imprint_url", "osm_id", "osm_match_score",
    # Contact details the map object carried. Free, since Overpass was queried anyway. The email
    # is reduced to its domain — see pipeline._email_domain.
    "osm_phone", "osm_email_domain",
    "candidates_generated", "candidates_resolved", "runner_up_url", "runner_up_confidence",
    # Which stage ran for this company, and what it produced. Read these before concluding
    # anything from a blank url: `never_tried` means nobody looked.
    "tier1_url_pattern", "tier2_osm", "tier3_ollama_searxng", "tier4_llm_judge",
    # What the local model said about the winning URL, and what that URL would have scored without
    # it. When `band` and `band_without_judge` differ, the model changed the answer — those are the
    # rows to read by hand, because they are where it is either earning its runtime or wrong.
    "judge_verdict", "judge_reason", "band_without_judge", "judge_changed_band",
    # What the application already serves for this company, and whether we agree with it.
    "db_website", "db_website_source", "db_website_confidence", "agrees_with_db",
]


def write_csv(limit: int | None = None) -> None:
    rows = sample_rows(limit)
    rows.sort(key=lambda r: (-(r.get("confidence_value") or 0.0), r.get("company_name") or ""))
    db = load_db_websites([r["company_id"] for r in rows if r.get("company_id")])
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            answers = r.get("answers") or []
            runner_up = answers[1] if len(answers) > 1 else {}
            stored = db.get(r.get("company_id") or "") or {}
            writer.writerow({
                "company_id": r.get("company_id"),
                "company_name": r.get("company_name"),
                "postcode": r.get("postcode"),
                "city": r.get("city"),
                "address": r.get("address"),
                "registration_number": r.get("registration_number"),
                "url": r.get("website") or "",
                "confidence": r.get("confidence_value") or 0.0,
                "band": r.get("band"),
                "verified_by": r.get("verified_by"),
                "method": "+".join(r.get("methods") or []),
                "pattern": r.get("pattern") or "",
                "evidence": ";".join(r.get("evidence") or []),
                "why": r.get("why") or "",
                "imprint_url": r.get("imprint_url") or "",
                "osm_id": r.get("osm_id") or "",
                "osm_match_score": r.get("osm_match_score") or "",
                "osm_phone": r.get("osm_phone") or "",
                "osm_email_domain": r.get("osm_email_domain") or "",
                "candidates_generated": r.get("candidates_generated") or 0,
                "candidates_resolved": r.get("candidates_resolved") or 0,
                "runner_up_url": runner_up.get("url") or "",
                "runner_up_confidence": runner_up.get("confidence") or "",
                "tier1_url_pattern": tier_status(r, "url"),
                "tier2_osm": tier_status(r, "osm"),
                "tier3_ollama_searxng": tier_status(r, "llm"),
                "tier4_llm_judge": tier_status(r, "judge"),
                "judge_verdict": r.get("judge_verdict") or "",
                "judge_reason": r.get("judge_reason") or "",
                "band_without_judge": r.get("band_without_judge") or "",
                "judge_changed_band": (
                    "yes"
                    if r.get("band_without_judge") and r.get("band_without_judge") != r.get("band")
                    else ""
                ),
                "db_website": stored.get("website") or "",
                "db_website_source": stored.get("source") or "",
                "db_website_confidence": stored.get("confidence") or "",
                "agrees_with_db": agreement(r.get("website"), stored.get("website")),
            })
    found = sum(1 for r in rows if r.get("website"))
    log.info("Wrote %s — %d rows, %d with a URL", CSV_PATH, len(rows), found)

    # The comparison is the point of the handover, so it is printed rather than left to whoever
    # opens the CSV. `different` rows are the ones worth reading by hand.
    tally: dict[str, int] = {}
    for r in rows:
        key = agreement(r.get("website"), (db.get(r.get("company_id") or "") or {}).get("website"))
        tally[key] = tally.get(key, 0) + 1
    log.info("Against what the database already serves: %s", tally)


# ---------------------------------------------------------------------------------------
# The labelled set. The only thing here that measures whether the pipeline is RIGHT.
# ---------------------------------------------------------------------------------------
#
# Everything else in this file counts what the pipeline did. None of it can say whether what it did
# was correct, because there is nothing to compare against: "47 websites found" is not a score
# without knowing how many of the 47 are real and how many real ones were missed. Comparing a new
# version against the language model's opinion only measures agreement with the model.
#
# So: a human looks at about sixty companies and writes down the answer. Three groups, because there
# are three different questions and one sample cannot answer them all.
#
#   winner          we claim a website. Is it that company's? -> PRECISION.
#   judge_rejected  the rules proposed a website and it was rejected. Was rejecting it right?
#                   -> the false-positive rate, and whether the rejections are throwing away
#                      real websites.
#   nothing         we found nothing at all. Does this company have a website?
#                   -> THE CEILING. If most of these do have one, 4.7% is a recall problem worth
#                      paying a search API for. If most do not, 4.7% is close to the truth and
#                      buying search would be wasted money.
#
# The file is hand-filled and is never overwritten: `labels` merges, keeping every label already
# written. Nothing in the pipeline may write to the label column — a measuring instrument the
# thing being measured can adjust is not one.

LABELS_PATH = OUT_DIR / "labels_todo.csv"

LABEL_COLUMNS = [
    "bucket", "company_id", "company_name", "address", "registration_number",
    "candidate_url", "our_verdict", "confidence", "band", "band_without_judge",
    "judge_verdict", "judge_reason", "why",
    # The three a human fills in. Everything to the left is context for the decision.
    #   label        yes | no | unclear   — is candidate_url this company's own website?
    #   true_website for a `nothing` row, the website you found by hand, if any. Blank = none exists.
    #   notes        anything worth remembering. Never parsed, only read.
    "label", "true_website", "notes",
]

#: The three answers the label column accepts. `unclear` is deliberately available: forcing a
#: yes/no on a genuinely ambiguous page produces a confident wrong number, and a count of how many
#: cases a human could not decide is itself worth knowing.
LABEL_VALUES = ("yes", "no", "unclear")


def _demoted_answer(row: dict[str, Any]) -> dict[str, Any] | None:
    """The candidate this company would have reported if the judge had not spoken.

    The highest-scoring answer that was over the bar on the rules alone and is not over it now.
    """
    for answer in row.get("answers") or []:
        was = answer.get("band_without_judge")
        if was in ("low", "medium", "high") and answer.get("band") == "reject":
            return answer
    return None


def _label_candidates(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Sort every company into one of the three groups, or none of them."""
    buckets: dict[str, list[dict[str, Any]]] = {"winner": [], "judge_rejected": [], "nothing": []}
    for row in rows:
        if row.get("website") and row.get("band") in ("high", "medium", "low"):
            buckets["winner"].append(
                {
                    "bucket": "winner",
                    "candidate_url": row.get("website"),
                    "our_verdict": "we say this is the company's website",
                    "confidence": row.get("confidence_value"),
                    "band": row.get("band"),
                    "band_without_judge": row.get("band_without_judge"),
                    "judge_verdict": row.get("judge_verdict"),
                    "judge_reason": row.get("judge_reason"),
                    "why": row.get("why"),
                    "_row": row,
                }
            )
            continue
        demoted = _demoted_answer(row)
        if demoted:
            buckets["judge_rejected"].append(
                {
                    "bucket": "judge_rejected",
                    "candidate_url": demoted.get("url"),
                    "our_verdict": "the rules proposed this and it was rejected",
                    "confidence": demoted.get("confidence"),
                    "band": demoted.get("band"),
                    "band_without_judge": demoted.get("band_without_judge"),
                    "judge_verdict": demoted.get("judge_verdict"),
                    "judge_reason": demoted.get("judge_reason"),
                    "why": demoted.get("why"),
                    "_row": row,
                }
            )
            continue
        buckets["nothing"].append(
            {
                "bucket": "nothing",
                "candidate_url": "",
                "our_verdict": "we found nothing — does a website exist at all?",
                "confidence": "",
                "band": "",
                "band_without_judge": "",
                "judge_verdict": "",
                "judge_reason": "",
                "why": "",
                "_row": row,
            }
        )
    return buckets


def labels(limit: int | None = None, per_bucket: int = 20) -> None:
    """Write (or extend) the hand-labelling worksheet.

    Deterministic: companies are taken in `company_id` order, so asking for more of them later adds
    rows rather than reshuffling the ones already labelled.
    """
    rows = sample_rows(limit)
    buckets = _label_candidates(rows)

    existing: dict[tuple[str, str], dict[str, str]] = {}
    if LABELS_PATH.exists():
        with LABELS_PATH.open(encoding="utf-8-sig", newline="") as handle:
            for old in csv.DictReader(handle):
                existing[(old.get("bucket") or "", old.get("company_id") or "")] = old
        labelled = sum(1 for v in existing.values() if (v.get("label") or "").strip())
        log.info("Found %d rows in %s, %d already labelled", len(existing), LABELS_PATH.name, labelled)

    out: list[dict[str, Any]] = []
    for name, entries in buckets.items():
        entries.sort(key=lambda e: e["_row"].get("company_id") or "")
        chosen = entries[:per_bucket]
        # Never drop a row somebody has already labelled, even if it falls outside the first N.
        already = [
            e for e in entries[per_bucket:]
            if (existing.get((name, e["_row"].get("company_id") or ""), {}).get("label") or "").strip()
        ]
        for entry in chosen + already:
            row = entry.pop("_row")
            was = existing.get((name, row.get("company_id") or ""), {})
            out.append(
                {
                    **entry,
                    "company_id": row.get("company_id"),
                    "company_name": row.get("company_name"),
                    "address": row.get("address"),
                    "registration_number": row.get("registration_number"),
                    # Carried over verbatim. This is the human's work.
                    "label": was.get("label", ""),
                    "true_website": was.get("true_website", ""),
                    "notes": was.get("notes", ""),
                }
            )
        log.info("%-15s %d rows (%d available)", name, len(chosen) + len(already), len(entries))

    LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LABELS_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LABEL_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for entry in out:
            writer.writerow(entry)

    print(f"\nWrote {len(out)} rows to {LABELS_PATH}")
    print(f"Fill in the `label` column with one of: {', '.join(LABEL_VALUES)}")
    print("  winner / judge_rejected -> is candidate_url that company's OWN website?")
    print("  nothing                 -> search by hand; put what you find in true_website,")
    print("                             and label `yes` if a website exists, `no` if none does.")
    print(f"\nThen: python {Path(__file__).name} labels-report")


def labels_report() -> None:
    """Read the worksheet back and print the three numbers it exists to produce."""
    if not LABELS_PATH.exists():
        raise SystemExit(f"no {LABELS_PATH}; run `labels` first")
    with LABELS_PATH.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    def bucket(name: str) -> list[dict[str, str]]:
        return [r for r in rows if (r.get("bucket") or "") == name]

    def tally(entries: list[dict[str, str]]) -> dict[str, int]:
        counts = {value: 0 for value in LABEL_VALUES}
        counts["unlabelled"] = 0
        for entry in entries:
            value = (entry.get("label") or "").strip().lower()
            counts[value if value in counts else "unlabelled"] += 1
        return counts

    print(f"\n=== {len(rows)} rows in {LABELS_PATH.name} ===")
    unknown = [
        (r.get("company_name"), r.get("label"))
        for r in rows
        if (r.get("label") or "").strip()
        and (r.get("label") or "").strip().lower() not in LABEL_VALUES
    ]
    if unknown:
        print(f"\n!! {len(unknown)} rows have a label that is not one of {LABEL_VALUES}:")
        for name, value in unknown[:10]:
            print(f"     {name}: {value!r}")
        print("   Those are counted as unlabelled.")

    winners = tally(bucket("winner"))
    decided = winners["yes"] + winners["no"]
    print("\n-- precision: of the websites we report, how many are right? --")
    print(f"{'labelled yes (correct)':<38}{winners['yes']:>5}")
    print(f"{'labelled no (wrong)':<38}{winners['no']:>5}")
    print(f"{'unclear':<38}{winners['unclear']:>5}")
    print(f"{'not yet labelled':<38}{winners['unlabelled']:>5}")
    if decided:
        print(f"\n  PRECISION {winners['yes']}/{decided} = {100.0 * winners['yes'] / decided:.0f}%")
    else:
        print("\n  PRECISION unknown — nothing in this group is labelled yet")

    rejected = tally(bucket("judge_rejected"))
    decided = rejected["yes"] + rejected["no"]
    print("\n-- the rejections: was throwing these away right? --")
    print(f"{'labelled no (rightly rejected)':<38}{rejected['no']:>5}")
    print(f"{'labelled yes (a real site we lost)':<38}{rejected['yes']:>5}")
    print(f"{'unclear':<38}{rejected['unclear']:>5}")
    print(f"{'not yet labelled':<38}{rejected['unlabelled']:>5}")
    if decided:
        print(
            f"\n  REJECTIONS CORRECT {rejected['no']}/{decided} "
            f"= {100.0 * rejected['no'] / decided:.0f}%"
        )
        if rejected["yes"]:
            print(f"  {rejected['yes']} real websites were rejected. Those rows are worth reading.")

    nothing = bucket("nothing")
    counts = tally(nothing)
    decided = counts["yes"] + counts["no"]
    found_by_hand = sum(1 for r in nothing if (r.get("true_website") or "").strip())
    print("\n-- the ceiling: do the companies we find nothing for have websites? --")
    print(f"{'a website exists (label yes)':<38}{counts['yes']:>5}")
    print(f"{'no website exists (label no)':<38}{counts['no']:>5}")
    print(f"{'unclear':<38}{counts['unclear']:>5}")
    print(f"{'not yet labelled':<38}{counts['unlabelled']:>5}")
    print(f"{'true_website filled in':<38}{found_by_hand:>5}")
    if decided:
        rate = 100.0 * counts["yes"] / decided
        print(f"\n  MISSED {counts['yes']}/{decided} = {rate:.0f}% of the companies we found nothing for")
        print("  have a website that a human can find. That is the recall left on the table —")
        print("  it is what buying a search API would be paying for.")
    else:
        print("\n  CEILING unknown — this is the group that decides whether to pay for search.")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run the pipeline (resumable)")
    p_run.add_argument("--limit", type=int, default=1000)
    p_run.add_argument(
        "--stages", default="url,osm",
        help=(
            "comma-separated: url, osm, llm, judge. Default url,osm. "
            "`llm` searches with SearXNG and needs it to work, which it does not on an office "
            "network. `judge` needs Ollama only: it reads the pages url/osm already downloaded "
            "and says whether each belongs to the company."
        ),
    )
    p_run.add_argument("--workers", type=int, default=int(os.getenv("COMBINED_WORKERS", "8")))
    p_run.add_argument(
        "--refresh-osm", action="store_true",
        help="re-download the city-wide OSM snapshot even if the one on disk is fresh",
    )
    p_run.add_argument(
        "--force", action="store_true",
        help="re-run every company in the sample, including ones that already have every method",
    )

    p_report = sub.add_parser("report", help="print the scorecard")
    p_report.add_argument(
        "--limit", type=int, default=None,
        help="describe only the first N companies of the sample — use the same N the run used",
    )
    p_csv = sub.add_parser("csv", help="write the handover CSV")
    p_csv.add_argument(
        "--limit", type=int, default=None,
        help="write only the first N companies of the sample — use the same N the run used",
    )

    p_labels = sub.add_parser(
        "labels", help="write the hand-labelling worksheet (merges with what is already labelled)"
    )
    p_labels.add_argument("--limit", type=int, default=None)
    p_labels.add_argument(
        "--per-bucket", type=int, default=20,
        help="how many companies per group (default 20, so about 60 rows in total)",
    )
    sub.add_parser("labels-report", help="read the worksheet back: precision, rejections, ceiling")

    args = parser.parse_args()
    if args.command == "run":
        stages = {s.strip() for s in args.stages.split(",") if s.strip()}
        unknown = stages - set(ALL_STAGES)
        if unknown:
            raise SystemExit(f"unknown stage(s): {sorted(unknown)}")
        run(
            args.limit, stages, args.workers,
            refresh_osm=args.refresh_osm, force=args.force,
        )
    elif args.command == "report":
        report(args.limit)
    elif args.command == "labels":
        labels(args.limit, args.per_bucket)
    elif args.command == "labels-report":
        labels_report()
    else:
        write_csv(args.limit)


if __name__ == "__main__":
    sys.exit(main())
