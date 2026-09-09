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
import collections
import csv
import io
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

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
#:
#: `{active_only}` is where `DISCOVERY_ACTIVE_ONLY` lands, and it is worth being blunt about what it
#: does to this query: **it changes WHICH companies a given `--limit` selects, and it does not make a
#: limited run cheaper.** The order is `md5(id)` over whatever survives the WHERE, so the limit is
#: applied *after* the filter: measured on this table, `--limit 200` unfiltered holds 78 live
#: companies, while `--limit 200` filtered holds 200 live companies and overlaps the first set by
#: exactly those 78. That is 122 companies of NEW work, not a saving.
#:
#: The saving is real only where the limit stops binding — a full run, 14,443 companies down to
#: 6,298. On a limited run the flag buys density instead: every company processed is one that might
#: still have a website. Two consequences, both of which have bitten someone in this repo already:
#: keyword weights are computed over whatever was scoped (`df_companies`), so a filtered run scores
#: the same company differently; and every number in `docs/` was measured with the flag off, so a
#: filtered run compares only against another filtered run.
SAMPLE_SQL = """
SELECT id::text, company_name, address, postcode, city, registration_number,
       legal_form, wz_code, business_purpose
FROM public.companies c
WHERE company_name IS NOT NULL
  {active_only}
ORDER BY md5(id::text)
LIMIT %s
"""

_WRITE_LOCK = threading.Lock()


#: What a labelling worksheet is written with. **A semicolon, because these files exist to be
#: double-clicked in Excel on a German locale** — and there, a comma-delimited CSV opens with all 25
#: columns in column A. Nobody labels 110 rows like that.
#:
#: This was comma-delimited on the reasoning that a comma is "the portable form". It is, and it was
#: the wrong trade: the only consumer is `read_label_csv`, which sniffs either delimiter, and the only
#: human is on a German locale. Portability was being bought with the one thing that mattered.
#:
#: Quoting is `csv`'s job either way, so a `purpose_text` containing a semicolon — and German
#: register text does — survives the round trip. Overridable for anyone on an English locale, where
#: Excel wants the comma back.
LABEL_CSV_DELIMITER = os.getenv("COMBINED_LABEL_CSV_DELIMITER", ";")


def read_label_csv(path: Path) -> list[dict[str, str]]:
    """Read a hand-labelling worksheet back, whatever Excel did to it on the way.

    Excel on a German (or French, or Dutch) locale writes CSV with **semicolons**, and it does that
    silently on save — the file still opens, still has the right rows, and `csv.DictReader`'s comma
    default then parses the whole header as one column name. That happened here: 40 labelled-ready
    rows came back as 2 columns and `classify-labels-report` would have reported nothing at all
    rather than failing.

    So the delimiter is sniffed from the header rather than assumed, and only from these two
    candidates — a real `csv` reader still handles quoting, so a company name containing a comma
    survives either way. Reading is therefore delimiter-agnostic in both directions, which is what
    makes an Excel visit harmless.

    **Writing uses `LABEL_CSV_DELIMITER`, which is `;`** — see the note there. These files exist to
    be opened in Excel on a German locale, and a comma-delimited one lands entirely in column A.
    """
    text = path.read_text(encoding="utf-8-sig")
    header = text.splitlines()[0] if text else ""
    delimiter = ";" if header.count(";") > header.count(",") else ","
    if delimiter != ",":
        log.info("%s is %r-delimited (Excel does this); reading it as such", path.name, delimiter)
    return list(csv.DictReader(io.StringIO(text), delimiter=delimiter))


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


#: Companies whose canonical website was actually FETCHED. Not every canonical website: 2,800 of
#: the 3,532 came from the Northdata export and not one of them has ever been loaded.
#:
#: This is the gate for the Technologiefeld, and the gate's whole justification is that a live
#: website is evidence a company trades, where a register purpose is only evidence of what it is
#: permitted to do. A URL in a CSV is neither — it is evidence that somebody recorded a URL. Gating
#: on all 3,532 would release 188 withheld fields, but 169 of them would be computed from the
#: register purpose alone, because those companies have no downloaded page. Column 1 would then be
#: a partial duplicate of column 3 for those 169, which is the one thing the three-column split
#: exists to prevent.
#:
#: "Fetched" is recorded two ways, and both are needed: a `verified_by` that implies a download
#: (`register_number_match`, `page_content_match`, `method_agreement`, `url_pattern` — but never
#: `llm_snippet`, which is a model reading search snippets), or an `imprint_url`, which only exists
#: because a page was fetched and its Impressum probed. Three Brave rows have the second and not
#: the first.
#:
#: **Any observation, not the canonical one.** This asked the canonical row first, and that was
#: wrong in a way worth recording: the canonical row is picked by `sources.priority`, the export
#: sits at 50, and it wins every contest it enters. So for the ~370 companies where the export
#: displaced a page combined discovery had actually downloaded — same URL, 438 of them agree
#: exactly — the surviving row is the unfetched one, and the company failed a gate it used to pass.
#: `assigned` fell 382 → 270 and `no_website` rose 556 → 668 before this was fixed. The question is
#: "did anybody fetch a page for this company", which is a question about the evidence, not about
#: which row won a priority contest. Above the resolver's own 0.35 bar, so a rejected candidate
#: cannot answer it. 1,150 companies of 14,443.
VERIFIED_WEBSITE_SQL = """
SELECT DISTINCT o.company_id::text
FROM public.company_field_observations o
WHERE o.field_name IN ('website', 'candidate_url')
  AND COALESCE(o.confidence, 0) >= 0.35
  AND (
        (o.metadata->>'verified_by' IS NOT NULL AND o.metadata->>'verified_by' <> 'llm_snippet')
     OR o.metadata ? 'imprint_url'
  )
"""


def load_verified_websites() -> set[str]:
    """Company ids with a canonical website somebody actually downloaded.

    Read from the database rather than from `results.jsonl`, which is the point of this function:
    the file only knows what combined discovery found itself, so it saw 940 websites while the
    resolver had settled 3,532. Every source that fetched and checked a page now counts, whoever
    fetched it.
    """
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(VERIFIED_WEBSITE_SQL)
            return {row[0] for row in cur.fetchall()}
    finally:
        connection.close()


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


#: Companies the Northdata export already knows a website for. Ids only — the URL is deliberately
#: not returned, because nothing here should be tempted to compare against it.
#:
#: `company_id::text = ANY(%s)`, matching `DB_WEBSITE_SQL` above, because the ids arrive from
#: `CompanyCandidate.id` as strings while the column is `uuid` — and `uuid = text` has no operator
#: in Postgres, so the uncast version fails outright rather than returning the wrong rows.
EXPORT_WEBSITE_SQL = """
SELECT company_id::text
FROM public.northdata_export
WHERE company_id::text = ANY(%s)
  AND website IS NOT NULL
"""


def load_export_websites(company_ids: list[str]) -> set[str]:
    """Which of these companies have a website in `public.northdata_export`.

    **Why this is not folded into `load_db_websites`.** That function feeds the report path, where
    `agreement()` turns it into the CSV's `agrees_with_db` column — `db_blank` means the database has
    nothing, `different` means it has something else. Adding the export to it would redefine "what
    the database believes" in every report and silently reclassify those rows, changing published
    numbers to fix a skip. So the skip gets its own loader and the report keeps meaning what it meant.

    Ids only, and no URL: the export's opinion has no business influencing what this script scores.
    It answers exactly one question, "would work here be wasted", and nothing else.
    """
    if not company_ids:
        return set()
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(EXPORT_WEBSITE_SQL, (company_ids,))
            return {row[0] for row in cur.fetchall()}
    finally:
        connection.close()


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


def active_only() -> bool:
    """Whether to skip companies the register no longer lists as `aktiv`.

    One definition of the flag, in `source_enrichment_common`, read through here so the script and
    the DAGs can never end up with two env var names that drift apart. Read lazily, like every other
    import from that module, because it pulls in psycopg2.
    """
    from source_enrichment_common import ACTIVE_ONLY

    return ACTIVE_ONLY


def skip_resolved_website() -> bool:
    """Whether to skip companies that already HAVE a resolved website.

    One definition of the flag, in `source_enrichment_common`, read through here for the same reason
    as `active_only` above — the DAGs and this script must not end up with two env var names that
    drift apart.

    Two predicates, both meaning "we already know this company's website": a canonical row in
    `company_canonical_field_sources`, or a `website` in `northdata_export` that has not been
    imported yet. The second earns its keep only in that window — once
    `import_northdata_websites.py` and `pipeline_coordinator` have run, every export website is
    canonical and the first predicate covers it, measured at **3,109** active companies either way.

    Where it is applied is the part worth reading. **Not in `SAMPLE_SQL`.** `load_sample` also backs
    `sample_rows`, which is what `report --limit N` scopes with, so filtering the query would
    silently rescope every reported number and drop already-solved companies out of the report — the
    published figures would stop being reproducible. `{active_only}` already carries that hazard;
    a second one is not worth adding. So the filter lands on `pending` inside `run`, where it changes
    what gets *worked on* and nothing about what gets *counted*.

    Note for anyone running this on the host: `.env` configures the containers and this script never
    reads it, so the flag has to be in the shell — `$env:DISCOVERY_SKIP_RESOLVED_WEBSITE = "true"`.
    """
    from source_enrichment_common import SKIP_RESOLVED_WEBSITE

    return SKIP_RESOLVED_WEBSITE


def load_sample(limit: int) -> list[Any]:
    from source_enrichment_common import _ACTIVE_ONLY_SQL, CompanyCandidate, get_connection

    # `_ACTIVE_ONLY_SQL` is one fixed literal or the empty string, chosen by an env var — a `%s`
    # parameter can be neither, so it is formatted in. The limit stays bound. SAMPLE_SQL aliases
    # `public.companies` as `c` purely so this one shared fragment drops in verbatim, rather than a
    # second spelling of the same predicate existing here to be kept in sync.
    sql = SAMPLE_SQL.format(active_only=_ACTIVE_ONLY_SQL)

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
            # A filter that removes EVERYTHING is not a quiet no-op, it is a broken import. The
            # Berlin CSV goes into Postgres by hand and `register_status` is a new column in that
            # copy list; forget it and every row is NULL, `= 'aktiv'` excludes NULL, and this would
            # otherwise report a clean, fast, complete run over zero companies.
            if active_only() and not rows:
                cur.execute("SELECT count(*) FROM public.companies")
                total = cur.fetchone()[0]
                if total:
                    raise SystemExit(
                        f"DISCOVERY_ACTIVE_ONLY is on and matched 0 of {total:,} companies. "
                        "register_status is probably NULL for the whole table — the manual CSV load "
                        "has to map that column. See docs/data-model.md."
                    )
    finally:
        connection.close()
    if active_only():
        log.info(
            "DISCOVERY_ACTIVE_ONLY: %d companies, all of them aktiv. This is a DIFFERENT sample "
            "from an unfiltered --limit %d, not a subset of it — the limit applies after the "
            "filter, so a limited run does more work rather than less. The saving is on a full run.",
            len(rows), limit,
        )
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
    skip_coordinator: bool = False,
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

    # Companies the resolver has already settled a website for. `load_db_websites` returns only the
    # ones that actually have one, which is the predicate needed here. Applied to the work list and
    # not to the sample — see `skip_resolved_website` for why that distinction matters to `report`.
    #
    # It applies regardless of `--force`: force means "re-run methods whose implementation changed",
    # not "re-find websites we already have".
    resolved: set[str] = set()
    if skip_resolved_website():
        ids = [c.id for c in companies]
        canonical = set(load_db_websites(ids))
        export_only = load_export_websites(ids) - canonical
        resolved = canonical | export_only
        # Counted apart, and that is the point. A single total would hide whether the export is
        # contributing anything: once the import has run it contributes zero, and `export_only`
        # above 0 means `import_northdata_websites.py` is owed a run.
        log.info(
            "DISCOVERY_SKIP_RESOLVED_WEBSITE: %d of %d companies in the sample will not be worked "
            "on — %d have a resolved website, %d more are known only to northdata_export. They "
            "stay in the sample, so `report` still counts them; only the work is withheld.",
            len(resolved), len(companies), len(canonical), len(export_only),
        )
        if export_only:
            log.warning(
                "%d of those are export websites that were never imported. Until "
                "scripts/import_northdata_websites.py runs and pipeline_coordinator resolves them, "
                "they are skipped here but a paid provider DAG only skips them because this same "
                "flag is on in its container.",
                len(export_only),
            )

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
    pending = [
        c for c in companies
        if c.id not in resolved and (force or effective - already.get(c.id, set()))
    ]
    log.info(
        "%d companies in sample, %d already have %s, %d withheld for a resolved website, "
        "%d to run | stages: %s",
        len(companies),
        len(companies) - len(pending) - len(resolved),
        "+".join(sorted(effective)) or "nothing",
        len(resolved),
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

    written = write_observations_for([c.id for c in pending])
    if written is None:
        return
    log.info("%d observations in the database for these %d companies.", written, len(pending))
    if skip_coordinator:
        log.info(
            "--skip-coordinator: the observations are written and unresolved. Nothing is canonical "
            "until pipeline_coordinator runs."
        )
        return
    trigger_coordinator()


def write_observations_for(company_ids: list[str]) -> int | None:
    """Land this run's results in Postgres. Returns the row count, or None if it could not.

    Postgres is the store the application reads; `results.jsonl` is the cache that makes this
    resumable and lets the judge gate be replayed offline. Both are written, in that order, and the
    file is written first on purpose: if this function fails, the run's work is still on disk and
    `scripts/import_combined_discovery.py` can land it later. A run that has already cost an hour
    must not be lost to a database that happens to be down.

    One transaction at the end rather than a write per company, because `run` fans out over eight
    threads and a psycopg2 connection is not thread-safe. It reads the rows back through
    `latest_rows`, so what lands is exactly what the file says — the same path the importer takes,
    and the reason a live run and a file replay produce identical rows.
    """
    if not company_ids:
        return None
    try:
        from combined_discovery import observations
        from source_enrichment_common import (
            ensure_source,
            finish_sync_run,
            get_connection,
            start_sync_run,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("no database write: %s. The file is intact; use import_combined_discovery.py", exc)
        return None

    wanted = set(company_ids)
    rows = [r for r in latest_rows(RESULTS_PATH) if r.get("company_id") in wanted]
    try:
        connection = get_connection()
    except Exception as exc:  # noqa: BLE001
        log.warning("no database write: %s. The file is intact; use import_combined_discovery.py", exc)
        return None
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur,
                key=observations.SOURCE_KEY,
                name="Combined Discovery (URL patterns + OpenStreetMap + local judge)",
                source_type="combined",
                metadata={"script": "scripts/run_combined_discovery.py"},
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)
            created = 0
            for row in rows:
                created += int(
                    observations.write_company_result(
                        cur,
                        company=_company_for_observations(row),
                        result=row,
                        source_id=source_id,
                        sync_run_id=sync_run_id,
                        stages_run=set(row.get("stages_run") or ()),
                    )
                )
            cur.execute(
                """
                SELECT count(*) FROM public.company_field_observations
                WHERE source_id = %s AND company_id = ANY(%s::uuid[])
                """,
                (source_id, list(wanted)),
            )
            total = cur.fetchone()[0]
            finish_sync_run(
                cur,
                sync_run_id=sync_run_id,
                records_seen=len(rows),
                records_created=created,
                records_updated=len(rows) - created,
                records_failed=0,
            )
        return total
    finally:
        connection.close()


def trigger_coordinator() -> bool:
    """Resolve the observations just written into canonical fields. Returns True if it ran.

    **A direct call, not an Airflow trigger.** `pipeline_coordinator/main.py` keeps its Airflow
    imports inside `_make_dag()` behind a `try/except ImportError`, so `run_pipeline_coordinator`
    is importable from the host and opens its own psycopg2 connection. It is the same function the
    DAG's `PythonOperator` calls, so there is one implementation and two callers rather than a copy
    that can drift.

    What that costs: no `dag_run` row, so this does not appear in the Airflow UI or in
    `airflow.dag_run`. What it buys: no scheduler, no container spin-up, no REST auth, and it works
    when Airflow is not running at all — which is the normal state for a script that is
    deliberately not a DAG. A caller who wants the Airflow bookkeeping can pass
    `--skip-coordinator` and trigger the DAG itself.

    Never fatal. A run of this script can cost an hour and its results are already safe in
    `results.jsonl` and in `company_field_observations`; failing the whole thing because the
    resolver could not be reached would throw that away. The observations simply stay unresolved,
    which is exactly the state every previous version of this script left them in.

    Safe to call repeatedly since V19: `resolve_ceo_contact` used to append a duplicate
    `company_managers` row for every company on every run — 9,195 rows for 1,047 distinct names —
    and `uq_company_managers_company_name` now makes a repeat run a no-op.
    """
    log.info("triggering pipeline_coordinator")
    try:
        coordinator = REPO_ROOT / "airflow" / "dags" / "pipeline_coordinator"
        if str(coordinator) not in sys.path:
            sys.path.insert(0, str(coordinator))
        from pipeline_coordinator.main import run_pipeline_coordinator

        run_pipeline_coordinator()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "pipeline_coordinator did not run: %s. The observations are written and unresolved; "
            "trigger the DAG in Airflow, or re-run this script with the same arguments.", exc,
        )
        return False
    log.info("pipeline_coordinator finished. Canonical fields are up to date.")
    return True


def _company_for_observations(row: dict[str, Any]) -> Any:
    """A `CompanyCandidate` from a result row. Every field it needs is a key in the file."""
    from source_enrichment_common import CompanyCandidate

    return CompanyCandidate(
        id=row["company_id"],
        company_name=row["company_name"],
        address=row.get("address"),
        postcode=row.get("postcode"),
        city=row.get("city"),
        registration_number=row.get("registration_number"),
    )


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


def _hms(seconds: float) -> str:
    """`2h14m` / `9m30s` / `45s`. Two units is enough to plan around and short enough to sit in a log line."""
    seconds = int(max(seconds, 0))
    if seconds >= 3600:
        return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"
    if seconds >= 60:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds}s"


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


#: The two independent classifications, as CSV columns. Named so that a reader of the file can never
#: mistake one for the other, which is exactly what went wrong when they shared a column.
_TECHNOLOGIEFELD_COLUMNS = (
    "technologiefeld", "technologiefeld_reason", "technologiefeld_score",
    "technologiefeld_text_source", "technologiefeld_decided_by", "technologiefeld_keywords",
    "technologiefeld_runner_up", "technologiefeld_needs_review",
    # Only filled when the tie-break model ran. `decided_by = model` says it FIRED; these two say
    # whether it was thinking. Read twenty reasons and you know if it is reading the page or
    # bluffing — which is the question "coverage went up" cannot answer.
    "technologiefeld_llm_reason", "technologiefeld_llm_refused",
)
_WZ_COLUMNS = ("wz_code", "wz_division", "wz_division_title", "wz_section", "wz_section_title")

#: COLUMN 3, from the register purpose alone. Prefixed and grouped like the other two, because the
#: whole point of three columns is that a reader can see which evidence produced which answer.
#: `purpose_decided_by` is the one to read first: `keywords` means a word from the taxonomy is
#: literally in the register text, `model_direct` means a model chose from all 77 with nothing
#: anchoring it to the text.
_PURPOSE_COLUMNS = (
    "purpose_field", "purpose_outcome", "purpose_decided_by", "purpose_keywords",
    "purpose_score", "purpose_llm_reason", "purpose_model",
    # The source. Read `purpose_llm_reason` against `purpose_text` and a model row can be judged
    # without opening anything else — the only check this column currently has. 251 characters on
    # average, 541 at the 95th percentile, 7,321 at the longest; well inside Excel's 32,767-character
    # cell limit, so it is written whole rather than abridged.
    "purpose_text", "purpose_chars",
)


def _purpose_columns(row: dict[str, Any] | None) -> dict[str, Any]:
    """Column 3 for one company. All blank when `classify-purpose` has never run for it.

    Blank is a legitimate state and deliberately not spelled `NONE`: "nobody has run this" and "the
    register named no field" are different claims, and only the second one is an answer.
    """
    if not row:
        return {c: "" for c in _PURPOSE_COLUMNS}
    return {
        "purpose_field": row.get("purpose_field") or "",
        "purpose_outcome": row.get("outcome") or "",
        "purpose_decided_by": row.get("decided_by") or "",
        "purpose_keywords": ";".join(sorted(row.get("keywords") or {})),
        "purpose_score": row.get("score") or "",
        "purpose_llm_reason": row.get("llm_reason") or "",
        "purpose_model": row.get("model_name") or "",
        "purpose_text": row.get("purpose_text") or "",
        "purpose_chars": row.get("purpose_chars") or "",
    }


def _branch_columns(row: dict[str, Any] | None) -> dict[str, Any]:
    """Both classifications for one company. All blank when `classify` has never run for it.

    **Column 1, Technologiefeld**: one of Berlin's 77 fields, or the literal `NONE`. Nothing else can
    appear. `technologiefeld_reason` says which of `assigned`, `torn`, `no_keyword` or `no_text`
    produced it, because "the keywords were too close to call" and "we had nothing to read" are
    different facts and a reader needs to see which applies.
    `technologiefeld_text_source` names the text the keywords were found in — `services`, `homepage`,
    `imprint` or `purpose`. Every one of those is text the company published about itself.

    **Column 2, WZ cluster**: the official classification the company is registered under. Derived
    from `wz_code` with no judgement: the division is the code's first two characters, the section is
    the letter that owns the division.

    The two are never combined, and neither is evidence about the other.
    """
    if not row:
        return {c: "" for c in _TECHNOLOGIEFELD_COLUMNS + _WZ_COLUMNS}
    keywords = row.get("keywords") or {}
    return {
        "technologiefeld": row.get("technologiefeld") or row.get("branch") or "",
        "technologiefeld_reason": row.get("outcome") or "",
        "technologiefeld_score": row.get("score") or "",
        "technologiefeld_text_source": "+".join(sorted(set(keywords.values()))),
        "technologiefeld_decided_by": row.get("decided_by") or "",
        "technologiefeld_keywords": ";".join(sorted(keywords)),
        "technologiefeld_runner_up": row.get("runner_up") or "",
        "technologiefeld_needs_review": "yes" if row.get("needs_review") else "",
        "technologiefeld_llm_reason": row.get("llm_reason") or "",
        "technologiefeld_llm_refused": row.get("llm_refused") or "",
        "wz_code": row.get("wz_code") or "",
        "wz_division": row.get("wz_division") or "",
        "wz_division_title": row.get("wz_division_title") or "",
        "wz_section": row.get("wz_section") or "",
        "wz_section_title": row.get("wz_section_title") or "",
    }


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
    # Two independent classifications from `classify`, and they must stay independent: the
    # Technologiefeld is Berlin's field list filled from what the company published, the WZ cluster
    # is the official register classification. Blank for every row when `classify` has never run —
    # which is not the same as "this company has no industry".
    *_TECHNOLOGIEFELD_COLUMNS,
    *_WZ_COLUMNS,
    # Column 3, from `classify-purpose`. Independent of the other two: it needs no website, so it is
    # filled for thousands of companies the Technologiefeld can never reach — and for some of those
    # the two will disagree, which is a finding rather than a fault.
    *_PURPOSE_COLUMNS,
]


def write_csv(limit: int | None = None) -> None:
    rows = sample_rows(limit)
    rows.sort(key=lambda r: (-(r.get("confidence_value") or 0.0), r.get("company_name") or ""))
    db = load_db_websites([r["company_id"] for r in rows if r.get("company_id")])
    # Branches, if `classify` has ever run. Absent is a legitimate state and leaves the columns
    # blank rather than inventing an "unclassified" that nobody measured.
    branches = {r["company_id"]: r for r in read_jsonl(BRANCHES_PATH) if r.get("company_id")}
    if branches:
        log.info("Joining branches for %d companies from %s", len(branches), BRANCHES_PATH.name)
    # Column 3 joins the same way and is just as optional. Missing leaves its columns blank.
    purposes = {
        r["company_id"]: r
        for r in read_jsonl(PURPOSE_BRANCHES_PATH) if r.get("company_id")
    }
    if purposes:
        log.info("Joining register purposes for %d companies from %s",
                 len(purposes), PURPOSE_BRANCHES_PATH.name)
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
                **_branch_columns(branches.get(r.get("company_id") or "")),
                **_purpose_columns(purposes.get(r.get("company_id") or "")),
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
        for old in read_label_csv(LABELS_PATH):
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
        # `LABEL_CSV_DELIMITER` for the same reason as the other two worksheets: this one is opened
        # in Excel to be filled in, and a comma file lands entirely in column A on a German locale.
        writer = csv.DictWriter(handle, fieldnames=LABEL_COLUMNS, extrasaction="ignore",
                                delimiter=LABEL_CSV_DELIMITER)
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
    rows = read_label_csv(LABELS_PATH)

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


# ---------------------------------------------------------------------------------------
# Which industry is each company in — TWO columns, and they must never be mixed
# ---------------------------------------------------------------------------------------
#
# COLUMN 1, the TECHNOLOGIEFELD. Berlin's column "Branche / Technologie- und Anwendungsfeld": 77
# values in `combined_discovery/branch_taxonomy.yaml`, each with KEYWORDS that suggest it. A company
# has one of those 77 or it has NONE, and the only thing that may fill it is text the company
# published about itself. Four such texts, richest first:
#
#   services   /leistungen, /ueber-uns, /produkte — fetched by `fetch-content`, richest text
#   homepage   already cached by `run`. 68% carry at least one keyword
#   imprint    already cached by `run`. 44%. Kept because on 72 companies it adds a keyword
#   purpose    `business_purpose` from the register. 11% of 7,478, but the matches are cleaner
#
# COLUMN 2, the WZ CLUSTER. The official German industry classification the company is registered
# under, from `public.companies.wz_code` for 8,483 companies. The division is the code's first two
# characters and the section is the letter above it, both from `combined_discovery/wz_titles.yaml`.
# Derived, never decided — see `combined_discovery/wz_cluster.py`.
#
# A WZ CODE MAY NEVER FILL THE TECHNOLOGIEFELD. An earlier version mapped one onto the other and gave
# 2,632 companies a technology field that nothing they published supported, while flattening 22
# sections and 87 divisions of good official classification to do it. `--source wz` is refused, and
# `branch_classifier.classify()` has no parameter to pass a code through.
#
# `classify` makes NO web requests. Everything it reads is already on disk, which is what makes
# re-running it after a taxonomy edit cost two minutes. `fetch-content` is the only part that
# downloads.

BRANCHES_PATH = OUT_DIR / "branches.jsonl"
BRANCH_LABELS_PATH = OUT_DIR / "branch_labels_todo.csv"
#: Candidate keywords a model read off pages where none of ours fired. A worksheet for a person, not
#: an output: nothing here reaches a column until somebody puts it in `branch_taxonomy.yaml` by hand.
KEYWORD_PROPOSALS_PATH = OUT_DIR / "keyword_proposals.csv"
#: COLUMN 3, the PURPOSE FIELD. What the commercial register says the company was set up to do,
#: from `business_purpose` alone. Its own file, because it is its own column: a Technologiefeld
#: requires a website and this deliberately does not, so mixing them would silently reverse that
#: rule. See `combined_discovery/purpose_branch.py`.
PURPOSE_BRANCHES_PATH = OUT_DIR / "purpose_branches.jsonl"

#: COLUMN 3's hand-labelling worksheet. Its own file, beside `branch_labels_todo.csv` and never
#: sharing with it: the two columns ask different questions and a label for one is not a label for
#: the other.
PURPOSE_LABELS_PATH = OUT_DIR / "purpose_labels_todo.csv"

#: The cache namespaces each purpose tier reads, **newest first**. A new call is only ever written to
#: element 0; everything after it is read-only history.
#:
#: Why a list and not a string. `purpose_cache_key` is built from
#: `taxonomy_version|model_name|company_id|options` and does **not** include the prompt shape, so a
#: changed prompt has to move to a new namespace or it is served the old answers — see the warning on
#: `choose_for_purpose`. But moving namespace empties it, and this file is rewritten from scratch on
#: every run: the first free re-run after Tier B moved to `-rank3` dropped **1,285 paid-for answers**
#: (553 fields, 732 abstentions) and reported them as `no_keyword`, with the cache still holding all
#: of them. Reading the old namespaces too is what makes a prompt change cost only the new calls.
#:
#: A row served from history carries no ranking, and that is correct — it was a single-answer ask.
#: **`purpose-llm-direct-rank3` is deliberately NOT in this list.** It holds 215 answers from the
#: first ranked prompt, which demanded exactly three fields and thereby suppressed the abstention:
#: 17.6% of them named a field while their own reason said none fits, against 2.8% for the
#: single-answer prompt. Those answers are not corrupt, they are biased, and a biased answer served
#: from a cache is worse than no answer because nothing downstream can see where it came from. Left
#: on disk rather than deleted — they are the evidence for the prompt change — and simply not read,
#: which is what makes the re-ask cost 215 calls instead of 5,000.
#:
#: `purpose-llm-direct` (1,307 single-answer entries) IS still read. Those came from the unbiased
#: prompt and are fine; they carry no ranking, which is the truth about them.
PURPOSE_TIER_A_NAMESPACES = ("purpose-llm-tiebreak",)
PURPOSE_TIER_B_NAMESPACES = ("purpose-llm-direct-ranked", "purpose-llm-direct")

#: Quarantined, for the guard to assert against. Never read; kept named so that "why is this
#: directory here" has an answer six months from now.
PURPOSE_TIER_B_QUARANTINED = ("purpose-llm-direct-rank3",)

#: Read in this order, so a keyword found in several texts is credited to the most specific one.
SOURCE_ORDER = ("services", "homepage", "imprint", "purpose")

#: Note what is NOT here: `DISCOVERY_ACTIVE_ONLY` does not filter this query, deliberately. The
#: keyword pass is free — two minutes over 14,443 companies, no network — and both classification
#: files are expected to carry one row per company in the table, so dropping the dead ones here would
#: shrink `branches.jsonl` and `purpose_branches.jsonl` and make every outcome count stop summing to
#: the table. `register_status` is selected instead, and only the MODEL CALLS are gated on it.
COMPANY_FACTS_SQL = """
SELECT id::text, company_name, wz_code, business_purpose, register_status
FROM public.companies
WHERE company_name IS NOT NULL
"""


def load_company_facts() -> dict[str, dict[str, Any]]:
    """Company id -> name, WZ code, register purpose and register status, for the whole table.

    The whole table rather than the sample: the WZ code answers for companies that have no website
    at all, and those are exactly the ones the website path can never reach.
    """
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(COMPANY_FACTS_SQL)
            rows = cur.fetchall()
    finally:
        connection.close()
    return {
        r[0]: {
            "company_name": r[1],
            "wz_code": r[2],
            "business_purpose": r[3],
            "register_status": r[4],
        }
        for r in rows
    }


def _url_variants(url: str) -> list[str]:
    """The spellings of one URL that the page cache might be keyed under.

    The cache key is the URL as it was *requested*, and `results.jsonl` stores the URL as it was
    *resolved* — `origin_url(probe.final_url)`. Those differ whenever a site redirected from
    `http://x.de` to `https://www.x.de`, which most of them do. Trying the four spellings finds 860
    of 902 pages; without it, several hundred readable pages look missing.
    """
    out = [url, url.rstrip("/"), url + "/"]
    for old, new in (("https://", "http://"), ("http://", "https://")):
        if url.startswith(old):
            out.append(new + url[len(old):])
    for old, new in (("://www.", "://"), ("://", "://www.")):
        if old in url:
            out.append(url.replace(old, new, 1))
    seen: set[str] = set()
    return [u for u in out if not (u in seen or seen.add(u))]


def cached_html(namespace: str, url: str) -> str:
    """The stored HTML for a URL, trying the spellings above. Empty string for a miss."""
    from combined_discovery import page_cache

    for candidate in _url_variants(url):
        record = page_cache.get(namespace, candidate)
        if record and record.get("html"):
            return record["html"]
    return ""


def company_texts(
    row: dict[str, Any],
    facts: dict[str, Any],
    *,
    use_pages: bool,
    use_purpose: bool,
) -> dict[str, str]:
    """Source name -> readable text, from the cache and the database only. Never downloads."""
    from combined_discovery import content_pages
    from combined_discovery.url_patterns import visible_text

    texts: dict[str, str] = {}
    website = row.get("website")
    if use_pages and website:
        homepage = cached_html("probe", website)
        if homepage:
            texts["homepage"] = visible_text(homepage)
            got = content_pages.content_for(website, homepage, fetch=False)
            if got.text:
                texts["services"] = got.text
        imprint = cached_html("imprint", website)
        if imprint:
            texts["imprint"] = visible_text(imprint)
    if use_purpose and facts.get("business_purpose"):
        texts["purpose"] = facts["business_purpose"]
    return {name: text for name, text in texts.items() if text}


#: How much text to show either side of a matched keyword in the labelling worksheet. Wide enough
#: for the sentence, narrow enough that 40 of them fit in a spreadsheet column.
EVIDENCE_WIDTH = int(os.getenv("BRANCH_EVIDENCE_WIDTH", "110"))


def evidence_snippets(
    texts: dict[str, str], keywords_by_source: dict[str, str], taxonomy: Any
) -> tuple[dict[str, str], str]:
    """The company's own words around each keyword that fired, plus a fallback snippet.

    This exists for one reason: a person labelling 40 rows should not have to open 40 websites. The
    question "is `Handel` right for this company?" is usually answerable from the sentence the word
    appeared in — and when it is not, that is itself worth knowing, because it means the keyword fired
    on something incidental.

    Returns `(keyword -> snippet, fallback)`. The fallback is the start of the most specific text we
    read, and it is what a `no_keyword` row gets: "here is what the company says about itself, and
    none of Berlin's 527 words appeared in it" is a claim a human can check in seconds.

    Snippets come from the NORMALISED text, so offsets line up with the patterns that matched. That
    also means the reader sees lower case and collapsed whitespace — fine for judging meaning.
    """
    from combined_discovery import branch_classifier as bc

    normalised = {source: bc.normalize(text) for source, text in texts.items()}
    snippets: dict[str, str] = {}
    for keyword, source in keywords_by_source.items():
        pattern = taxonomy.patterns.get(keyword)
        text = normalised.get(source) or ""
        match = pattern.search(text) if (pattern and text) else None
        if not match:
            continue
        start = max(0, match.start() - EVIDENCE_WIDTH)
        end = min(len(text), match.end() + EVIDENCE_WIDTH)
        snippets[keyword] = f"…{text[start:end]}…" if start or end < len(text) else text

    fallback = ""
    for source in SOURCE_ORDER:
        if normalised.get(source):
            fallback = normalised[source][: EVIDENCE_WIDTH * 2].strip()
            fallback = f"[{source}] {fallback}…"
            break
    return snippets, fallback


def keywords_with_source(texts: dict[str, str], taxonomy: Any) -> dict[str, str]:
    """Keyword -> the source it was first found in, most specific source first.

    `normalize` runs once per text rather than once per keyword. The other way round lower-cases the
    same 6,000 characters 527 times, which turns a seconds-long pass into a minutes-long one.
    """
    from combined_discovery import branch_classifier as bc

    found: dict[str, str] = {}
    for source in SOURCE_ORDER:
        text = texts.get(source)
        if not text:
            continue
        for keyword in sorted(bc.matched_keywords(bc.normalize(text), taxonomy)):
            found.setdefault(keyword, source)
    return found


def fetch_content(limit: int | None, workers: int) -> None:
    """Download the /leistungen and /ueber-uns pages the cached homepages link to.

    The only part of branch classification that touches the network, and it pays once: every page
    lands in the cache and a second run downloads nothing.

    Measured over the 766 cached homepages: 644 of them (84%) link to a services or about-style
    page, and fetching them under the cap is about 1,813 requests. The links are found in HTML that
    is already on disk, so nothing is downloaded to decide what to download.
    """
    from combined_discovery import content_pages, page_cache

    # `limit` counts companies that HAVE a website, not companies in the sample. Only about 6% of
    # the table has one, so `--limit 100` on the raw sample would fetch pages for six companies and
    # answer nothing. Ordered by company_id so that `--limit 100` is a prefix of the full run and
    # the trial is part of the measurement rather than a separate one.
    rows = sorted(
        (r for r in latest_rows(RESULTS_PATH) if r.get("website")),
        key=lambda r: r.get("company_id") or "",
    )
    if limit:
        rows = rows[:limit]
    log.info("%d companies with a website in scope", len(rows))

    found_links = 0
    counter = {"done": 0, "fetched": 0, "failed": 0, "no_homepage": 0, "no_links": 0}
    started = time.monotonic()

    def one(row: dict[str, Any]) -> None:
        website = row["website"]
        homepage = cached_html("probe", website)
        if not homepage:
            with _WRITE_LOCK:
                counter["no_homepage"] += 1
                counter["done"] += 1
            return
        got = content_pages.content_for(website, homepage, fetch=True)
        with _WRITE_LOCK:
            counter["done"] += 1
            counter["fetched"] += got.fetched
            counter["failed"] += got.failed
            if not got.links:
                counter["no_links"] += 1
            done = counter["done"]
        if done % 50 == 0 or done == len(rows):
            rate = done / max(time.monotonic() - started, 1e-6)
            log.info(
                "[%d/%d] %d pages fetched, %d would not load | ~%.0f min left",
                done, len(rows), counter["fetched"], counter["failed"],
                (len(rows) - done) / rate / 60 if rate else 0,
            )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, rows))

    for row in rows:
        # Same key `content_pages.content_for` wrote: the selection rules are part of it, so this
        # count follows the caps and the category patterns instead of reporting a stale choice.
        index = page_cache.get(
            "content-index", f"{content_pages.selection_version()}|{row['website']}"
        )
        found_links += len(((index or {}).get("links")) or [])

    print(f"\n{'companies in scope':<40}{len(rows):>7}")
    print(f"{'no homepage in the cache':<40}{counter['no_homepage']:>7}")
    print(f"{'homepage but no services-style link':<40}{counter['no_links']:>7}")
    print(f"{'pages chosen':<40}{found_links:>7}")
    print(f"{'…that came back':<40}{counter['fetched']:>7}")
    print(f"{'…that would not load':<40}{counter['failed']:>7}")
    print(
        "\n  Run `classify` next. If the number of companies with at least one keyword has not\n"
        "  risen above the 68% the homepage alone manages, these downloads bought nothing — say so\n"
        "  and take the stage out rather than keeping it because it exists."
    )


def classify(
    limit: int | None,
    sources: set[str],
    use_llm: bool,
) -> None:
    """Work out each company's branch and write one JSON line per company.

    Three passes, and the middle one is why it cannot be done company by company: a keyword's weight
    depends on **how many companies use that word**, so every company's text has to be read before
    any company can be scored.
    """
    from combined_discovery import branch_classifier as bc
    from combined_discovery import branch_llm, wz_cluster

    taxonomy = bc.load_taxonomy()
    clusters = wz_cluster.load_clusters()
    log.info(
        "%d Technologiefelder, %d keywords, taxonomy %s | %d WZ divisions in %d sections",
        len(taxonomy.branches), len(taxonomy.keyword_branches), taxonomy.version,
        len(clusters.division_titles), len({s for s, _ in clusters.division_sections.values()}),
    )

    rows = {r["company_id"]: r for r in sample_rows(limit) if r.get("company_id")}
    facts = load_company_facts()
    scope = [cid for cid in facts if cid in rows] if rows else list(facts)
    log.info("%d companies in scope", len(scope))

    # The website GATE comes from the database; the website TEXT still comes from the file. That
    # asymmetry is deliberate and worth stating, because it looks like an oversight.
    #
    # The gate asks "did anyone prove this company trades", and the answer belongs to the resolver:
    # any source that fetched and checked a page counts, not only this script's own guesses. The
    # text asks "what words are on that page", and only the file can answer, because the page cache
    # is keyed by the URL as it was requested and the canonical string often differs by scheme,
    # `www` or a trailing slash — 300 of the 3,532 canonical URLs hit the cache, against 940 of the
    # file's own. So a company can now pass the gate and still have nothing to read, which lands it
    # in `no_keyword` or `no_text` rather than `no_website`. That is the honest outcome: we know it
    # trades and we have not read its words.
    verified = load_verified_websites()
    in_scope = sum(1 for cid in scope if cid in verified)
    log.info(
        "%d companies have a website somebody downloaded; %d of them are in scope. The file knows "
        "of %d — the difference is what this gate exists to recover.",
        len(verified), in_scope, sum(1 for r in rows.values() if r.get("website")),
    )

    # ---- pass 1: read every text once ---------------------------------------------------
    per_company: dict[str, dict[str, Any]] = {}
    for company_id in scope:
        texts = company_texts(
            rows.get(company_id, {}),
            facts[company_id],
            use_pages="page" in sources,
            use_purpose="purpose" in sources,
        )
        found = keywords_with_source(texts, taxonomy)
        # Built here, while the texts are in hand, and kept as a handful of short strings rather
        # than the whole text — this runs over 14,443 companies.
        snippets, fallback = evidence_snippets(texts, found, taxonomy)
        per_company[company_id] = {
            "texts": sorted(texts),
            "keywords": found,
            "had_text": bool(texts),
            "page_text": " ".join(texts.get(s, "") for s in ("services", "homepage")).strip(),
            "snippets": snippets,
            "fallback": fallback,
        }
    with_text = sum(1 for v in per_company.values() if v["had_text"])
    log.info("%d companies have text to read", with_text)

    # ---- pass 2: how common is each keyword, counted over COMPANIES ---------------------
    df = bc.document_frequency(v["keywords"] for v in per_company.values())
    log.info("%d of %d keywords appear at least once", len(df), len(taxonomy.keyword_branches))

    # ---- pass 3: score, and ask the model only where it can help ------------------------
    if use_llm:
        usable, reason = branch_llm.availability()
        if not usable:
            log.warning("The tie-break model is OFF: %s", reason)
            log.warning("Continuing without it. Torn rows stay torn.")
            use_llm = False
        else:
            log.info("The tie-break model is ON: %s", reason)

    # Which rows may not spend a NEW model call. Every company is still scored by keywords and still
    # gets a line in the file — this decides only where the slow part is allowed to run. Computed
    # once rather than per row, because the flag is one env var and this loop is 14,443 long.
    skip_model_for = {
        company_id: facts[company_id].get("register_status") != "aktiv" for company_id in scope
    } if active_only() else {}
    if skip_model_for:
        log.info(
            "DISCOVERY_ACTIVE_ONLY: %d of %d companies are not aktiv — keywords still score them, "
            "the model is not asked anything new about them",
            sum(1 for skip in skip_model_for.values() if skip), len(scope),
        )

    BRANCHES_PATH.parent.mkdir(parents=True, exist_ok=True)
    if BRANCHES_PATH.exists():
        BRANCHES_PATH.unlink()  # a full re-score, not an increment: every weight may have moved

    asked = 0
    for company_id in scope:
        state = per_company[company_id]
        # A Technologiefeld requires a website. The register purpose may still be what DECIDES the
        # field — it does for 15 companies — but a company we never found a website for gets NONE
        # with the reason `no_website`, and what it would have been is kept beside it.
        # From the resolver, not from this script's own file. See the note where `verified` is
        # loaded for why the gate and the text now come from different places.
        has_website = company_id in verified
        result = bc.classify(
            taxonomy=taxonomy,
            df=df,
            n_companies=max(with_text, 1),
            keywords_by_source=state["keywords"],
            had_text=state["had_text"],
            has_website=has_website,
        )

        # What the answer would have been without the pages `fetch-content` downloaded. The same
        # shape as `band_without_judge`: it is the only honest way to say whether that stage paid
        # for its 1,813 requests.
        without_services = None
        if any(source == "services" for source in state["keywords"].values()):
            without_services = bc.classify(
                taxonomy=taxonomy,
                df=df,
                n_companies=max(with_text, 1),
                keywords_by_source={
                    k: s for k, s in state["keywords"].items() if s != "services"
                },
                had_text=state["had_text"],
                # The counterfactual asks what the SERVICES PAGES bought, so it has to hold the
                # website gate constant. Leaving this at its default would turn every row into
                # `no_website` and report that the downloads bought nothing.
                has_website=has_website,
            )

        verdict = None
        if use_llm and result.shortlist and state["page_text"]:
            # A dead company gets no NEW model call when the flag is on — 11 of the 346 fields ever
            # assigned went to one, and the model is the slow part of this command. A verdict already
            # in the cache is still applied, which is what `cached_only` means.
            verdict = branch_llm.choose(
                company_name=facts[company_id].get("company_name") or "",
                text=state["page_text"],
                shortlist=result.shortlist,
                url=rows.get(company_id, {}).get("website") or company_id,
                taxonomy_version=taxonomy.version,
                cached_only=skip_model_for.get(company_id, False),
            )
            asked += 0 if verdict.cached else 1
            if verdict.branch:
                result.branch = verdict.branch
                result.outcome = "assigned"
                result.decided_by = "model"
            elif verdict.rejected_all:
                # The model read the page and refused all three. That is not the same as no keyword
                # firing, and a human should see it.
                result.needs_review = True

        # The second column, attached to every company whether or not any text was read. It is a
        # lookup in the official classification, not a decision, so it neither needs nor gets any of
        # the machinery above.
        cluster = wz_cluster.cluster_for(facts[company_id].get("wz_code"))

        # The words a human needs in order to disagree with this row. Prefer a keyword that belongs
        # to the field we actually assigned — the others fired for some other field and would explain
        # the wrong thing. With nothing assigned, show what we read instead, so `no_keyword` is a
        # checkable claim rather than a blank.
        for_branch = [
            k for k in state["snippets"]
            if result.branch and result.branch in (taxonomy.keyword_branches.get(k) or ())
        ]
        chosen = (for_branch or list(state["snippets"]))[:1]
        evidence = state["snippets"][chosen[0]] if chosen else state["fallback"]

        append_jsonl(BRANCHES_PATH, {
            "company_id": company_id,
            "company_name": facts[company_id].get("company_name"),
            "website": rows.get(company_id, {}).get("website"),
            # Does the register still list this company? 53.1% of Osnabrück is `erloschen`, and every
            # count this file has ever produced was measured over those too. Written on every row so
            # that is answerable from the file, whether or not the run filtered anything.
            "register_status": facts[company_id].get("register_status"),
            # ---- column 1: Berlin's Technologie- und Anwendungsfeld, from the company's own words
            # `technologiefeld` is what a consumer reads: a Berlin name or the literal NONE, never
            # anything else. `outcome` says WHY when it is NONE, and the three reasons stay apart.
            "technologiefeld": result.value,
            "outcome": result.outcome,
            # `branch` is null where `technologiefeld` is the string "NONE", and the difference is
            # deliberate: null is for code that needs to test "was a field established", the string
            # is for a human reading a column that must never contain a blank cell.
            "branch": result.branch,
            # What the register text claimed and no website confirmed. Set only on `no_website`
            # rows, and never the column's value — it exists so the gate's price can be counted and
            # so a person can label whether withholding the answer was right.
            "withheld_branch": result.withheld_branch,
            "score": result.score,
            "decided_by": result.decided_by,
            "runner_up": result.runner_up,
            "runner_up_score": result.runner_up_score,
            "ranked": result.ranked[:5],
            "keywords": result.keywords,
            # The company's own sentence around the deciding keyword, or the start of whatever text
            # we read when nothing fired. Written here so `classify-labels` needs no cache and no
            # database, and so a label can be made without opening the company's website.
            "evidence": evidence,
            "text_sources": state["texts"],
            "shortlist": result.shortlist,
            "needs_review": result.needs_review,
            "outcome_without_services": without_services.outcome if without_services else None,
            "branch_without_services": without_services.branch if without_services else None,
            "llm_branch": verdict.branch if verdict else None,
            "llm_reason": verdict.reason if verdict else "",
            "llm_refused": verdict.refused if verdict else "",
            "taxonomy_version": taxonomy.version,
            # What the keyword weights were computed over. A keyword's weight depends on how many
            # companies use it, so `--limit 1000` and a full run give the same company DIFFERENT
            # answers — unlike `fetch-content`, where a limited run is a prefix of the whole. Two
            # `branches.jsonl` files are otherwise indistinguishable, and "the numbers changed"
            # would look like the code changed.
            "scope_companies": len(scope),
            "df_companies": max(with_text, 1),
            # ---- column 2: the official WZ cluster. A different classification by a different
            # authority, answering a different question. Never mixed with column 1.
            "wz_code": cluster.code if cluster else None,
            "wz_division": cluster.division if cluster else None,
            "wz_division_title": cluster.division_title if cluster else None,
            "wz_section": cluster.section if cluster else None,
            "wz_section_title": cluster.section_title if cluster else None,
        })

    if use_llm:
        log.info("%d model calls made (the rest were already cached)", asked)
    log.info("Wrote %d companies to %s", len(scope), BRANCHES_PATH)


#: Below this many companies, "one keyword carries the whole field" says nothing — a field with two
#: assignments is 100% one keyword by arithmetic, not by weakness.
MIN_FIELD_SIZE_FOR_LONE_KEYWORD_WARNING = 5


def branch_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Technologiefeld -> how many companies got it. `technologiefeld` first, `branch` as a fallback
    for rows written before the column was named."""
    counts: dict[str, int] = {}
    for row in rows:
        key = row.get("technologiefeld") or row.get("branch") or "?"
        counts[key] = counts.get(key, 0) + 1
    return counts


PROPOSAL_COLUMNS = [
    # What the model read off a page, normalised so it is comparable with a real keyword.
    "word",
    # How many of the target companies this word FIRES on, by the real matcher. This is the value of
    # adding it, and it is the column to sort by.
    "companies",
    #   new              fires on a page and is not in branch_taxonomy.yaml  <- the prize
    #   already_keyword  fires and IS in the list, so something else stopped it. Rare, worth a look.
    #   does_not_fire    the model paraphrased. Adding it would change nothing at all.
    "status",
    "example_companies", "example_snippet",
    # The two a person fills in. The model is NEVER asked which field a word belongs to — the moment
    # it names one we are back to the 77-way guessing this design exists to avoid.
    "suggested_field", "decision",
]


def _snippet_around(word: str, text: str, width: int = EVIDENCE_WIDTH) -> str:
    """The company's own sentence around a proposed word, so it can be judged in context."""
    from combined_discovery import branch_classifier as bc

    match = bc._pattern_for(word).search(text)
    if not match:
        return ""
    start, end = max(0, match.start() - width), min(len(text), match.end() + width)
    return f"…{text[start:end]}…"


def propose_keywords(limit: int | None) -> None:
    """Ask a model what the companies with NO keyword call themselves, and check every answer.

    The second of the model's two jobs, and the one that needs no shortlist. `classify --llm` breaks
    ties between fields the keywords already nominated; this runs where nothing fired at all — 332
    companies with a website whose pages contain none of our 500 words.

    It cannot move the Technologiefeld column and does not try. It writes one worksheet of candidate
    keywords. A person places them in `branch_taxonomy.yaml`, and the NEXT `classify` run assigns the
    fields through the ordinary matcher — so every field stays explainable by a word on a page.

    **A proposal is only useful if the word really appears in that company's text.** Asked about an
    ice-cream café the model may well answer "Gastronomie", which is already a keyword and did not
    fire, because the page says *Eiscafé*. Adding it again changes nothing. So each word is run through
    the real matcher against the same text `classify` reads, and the three outcomes are kept apart.
    """
    from combined_discovery import branch_classifier as bc
    from combined_discovery import branch_llm

    taxonomy = bc.load_taxonomy()
    rows = {r["company_id"]: r for r in read_jsonl(BRANCHES_PATH) if r.get("company_id")}
    if not rows:
        raise SystemExit(f"nothing in {BRANCHES_PATH}; run `classify` first")

    # The target: a website to read, and not one keyword found in it. Ordered by company id so a
    # --limit trial is a prefix of the full run rather than a different sample.
    scope = sorted(
        (r for r in rows.values() if r.get("outcome") == "no_keyword" and r.get("website")),
        key=lambda r: r.get("company_id") or "",
    )
    if limit:
        scope = scope[:limit]
    log.info("%d companies with a website and no keyword", len(scope))

    usable, reason = branch_llm.availability()
    if not usable:
        raise SystemExit(f"the model is unreachable, and this command is nothing without it: {reason}")
    log.info("Model: %s", reason)

    facts = load_company_facts()
    #: word -> the companies it actually fires on, and the companies that merely suggested it.
    fires: dict[str, list[dict[str, Any]]] = {}
    suggested: dict[str, int] = {}
    asked = refused = reached = 0

    for row in scope:
        company_id = row["company_id"]
        texts = company_texts(row, facts.get(company_id, {}), use_pages=True, use_purpose=True)
        joined = bc.normalize(" ".join(texts.get(s, "") for s in SOURCE_ORDER))
        if not joined:
            continue

        proposal = branch_llm.propose_vocabulary(
            company_name=facts.get(company_id, {}).get("company_name") or row.get("company_name") or "",
            text=joined,
            url=row["website"],
            taxonomy_version=taxonomy.version,
        )
        asked += 0 if proposal.cached else 1
        if proposal.refused:
            refused += 1
            continue

        gained = False
        for word in proposal.words:
            suggested[word] = suggested.get(word, 0) + 1
            if bc._pattern_for(word).search(joined):
                fires.setdefault(word, []).append({
                    "company_name": row.get("company_name") or "",
                    "snippet": _snippet_around(word, joined),
                })
                if word not in taxonomy.keyword_branches:
                    gained = True
        reached += gained
        if (scope.index(row) + 1) % 25 == 0:
            log.info("[%d/%d] %d distinct words fire so far", scope.index(row) + 1, len(scope), len(fires))

    # ---- the worksheet -------------------------------------------------------------------
    out: list[dict[str, Any]] = []
    for word, count in sorted(suggested.items(), key=lambda kv: -kv[1]):
        hits = fires.get(word) or []
        status = ("already_keyword" if word in taxonomy.keyword_branches
                  else "new" if hits else "does_not_fire")
        out.append({
            "word": word,
            "companies": len(hits),
            "status": status,
            "example_companies": "; ".join(h["company_name"][:40] for h in hits[:3]),
            "example_snippet": (hits[0]["snippet"] if hits else "")[:300],
            "suggested_field": "",
            "decision": "",
        })
    out.sort(key=lambda r: (r["status"] != "new", -r["companies"], r["word"]))

    KEYWORD_PROPOSALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with KEYWORD_PROPOSALS_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROPOSAL_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out)

    # ---- what happened ------------------------------------------------------------------
    by_status = collections.Counter(r["status"] for r in out)
    print(f"\n=== {len(scope)} companies asked · {asked} model calls · {refused} gave nothing ===\n")
    print(f"{'distinct words proposed':<40}{len(out):>7}")
    for status, label in (("new", "…fire on a page, NOT yet a keyword"),
                          ("already_keyword", "…fire but are already keywords"),
                          ("does_not_fire", "…do not appear on the page at all")):
        print(f"{label:<40}{by_status.get(status, 0):>7}")

    print("\n-- the candidates, most companies first --")
    for row in [r for r in out if r["status"] == "new"][:20]:
        print(f"   {row['word'][:32]:<34}{row['companies']:>4}  {row['example_companies'][:44]}")

    print(f"\n{'companies a new word could reach':<40}{reached:>7}")
    print(f"{'…still nothing, even if you accept all':<40}{len(scope) - reached:>7}")
    print(
        "\n  That second number is the honest ceiling. It is the count of companies whose pages carry\n"
        "  no word for any industry Berlin's list contains — and no vocabulary work reaches them,\n"
        "  because the gap is in the list rather than in the matching.\n"
        f"\n  Nothing above has touched a column. Next: fill `suggested_field` and `decision` in\n"
        f"  {KEYWORD_PROPOSALS_PATH.name}, add what you accept to branch_taxonomy.yaml with the count\n"
        "  in a comment, then re-run `classify`. Apply the `clubs` test to every addition: is the word\n"
        "  specific in GERMAN, or only useful in Osnabrück?"
    )
    if by_status.get("does_not_fire", 0) > by_status.get("new", 0):
        print(
            "\n  !! more paraphrases than real words — the prompt is drifting away from the page.\n"
            "     Tighten it before anybody curates this."
        )


def branch_llm_cached(**kwargs):
    """A purpose verdict already in the cache, or None. Never calls a model, so it is always safe."""
    from combined_discovery import branch_llm

    return branch_llm.cached_purpose_verdict(**kwargs)


def _ask_the_model(
    assignment: Any,
    fact: dict[str, Any],
    purpose: str | None,
    company_id: str,
    taxonomy: Any,
    *,
    options: list[str],
    namespace: str,
    tier: str,
) -> tuple[Any, int]:
    """One model call, folded into the assignment. Returns `(assignment, 1 if it actually asked)`.

    Shared by both tiers on purpose. The two differ only in how long `options` is and which cache
    namespace they use — so keeping one call site means the answer validation, the caching and the
    failure handling cannot drift apart between a 3-way question and a 77-way one.
    """
    from combined_discovery import branch_llm
    from combined_discovery import purpose_branch as pb

    # Tier B asks for three ranked fields; Tier A asks for one of three. Derived from the tier rather
    # than passed in, so the ranked prompt and the `-rank3` namespace can never be paired with the
    # wrong question by a caller that forgot one of them.
    ranked = tier == pb.DECIDED_BY_MODEL_DIRECT

    verdict = branch_llm.choose_for_purpose(
        company_name=fact.get("company_name") or "",
        purpose=purpose or "",
        options=options,
        company_id=company_id,
        taxonomy_version=taxonomy.version,
        namespace=namespace,
        ranked=ranked,
    )
    return pb.apply_model_verdict(
        assignment,
        branch=verdict.branch,
        rejected_all=verdict.rejected_all,
        unsure=verdict.unsure,
        refused=verdict.refused,
        reason=verdict.reason,
        model_name=verdict.model_name,
        tier=tier,
        model_ranked=verdict.ranked,
    ), (0 if verdict.cached else 1)


def classify_purpose(
    limit: int | None,
    use_llm: bool = False,
    semantic: bool = False,
    llm_budget: int | None = None,
    only: list[str] | None = None,
) -> None:
    """COLUMN 3: what the register says each company was set up to do. Free unless `--llm`.

    Reads `business_purpose` and nothing else. That independence is the point — this column reaches
    the 6,477 companies that have a register purpose and no website at all, which the Technologiefeld
    can never reach by design.

    Three passes, because the middle one needs every company before it can weight anything:

    1. match keywords in each company's purpose, with the strict word-boundary matcher
    2. count how many COMPANIES use each keyword — the document frequency the weights divide by
    3. score, and settle each company's outcome

    Pass 2 is why `--limit` still reads the whole table for frequencies: a keyword's weight depends on
    how common it is, so measuring it over 50 companies and over 7,141 gives different answers for the
    same company. The limit selects which companies get *written*, not which ones get *counted*, and
    the count that was used is recorded in every row as `df_companies`.

    Ordered by company id, so `--limit 50` is a prefix of the full run rather than a different sample.

    `only` names specific companies to spend model calls on, jumping the queue. The queue is
    `company_id` order, which has nothing to do with the handover CSV's order (confidence, then name)
    — so a row that looks interesting near the top of the spreadsheet can easily sit at position 2,903
    and go untouched for hours. That mismatch is confusing enough to deserve an escape hatch.

    `use_llm` turns on **Tier A only**: the 3-way tie-break on `torn` rows, against a local model.
    It cannot create a field from nothing — it only converts `torn` into a field, an abstention, or
    a recorded failure — so it never touches the ~6,481 `no_keyword` rows. Its reach is 17 companies
    on today's data, and `BRANCH_MIN_KEYWORDS_FOR_MODEL` is what governs that, not the prompt.
    """
    from combined_discovery import branch_classifier as bc
    from combined_discovery import purpose_branch as pb

    taxonomy = bc.load_taxonomy()
    facts = load_company_facts()
    log.info("%d companies in the table", len(facts))

    # ---- pass 1: what fired, per company ------------------------------------------------
    # Over the WHOLE table, not the limit — see the docstring. `usable` is the corpus the weights are
    # measured against, and it is the honest denominator for this column: 7,141 of 14,443.
    # Matched for every company with ANY purpose text, including the stubs the length floor will
    # exclude. Matching is free, and a stub that would have fired is what `withheld_by_floor`
    # records — the floor's cost has to be countable or it cannot be argued with.
    keywords: dict[str, dict[str, str]] = {}
    usable: list[str] = []
    for company_id, fact in facts.items():
        purpose = fact.get("business_purpose")
        if not (purpose or "").strip():
            continue
        keywords[company_id] = pb.purpose_keywords(purpose, taxonomy)
        if pb.is_usable(purpose):
            usable.append(company_id)
    with_keyword = sum(1 for c in usable if keywords.get(c))
    log.info(
        "%d usable purposes (over %d chars), %d with at least one keyword",
        len(usable), pb.MIN_PURPOSE_CHARS, with_keyword,
    )

    # ---- pass 2: document frequency over PURPOSE TEXTS ONLY ------------------------------
    # Never borrowed from the website corpus. Register prose is a different vocabulary, and weighting
    # it by how often a word appears on homepages would rank the wrong words. Counted per COMPANY, so
    # one company cannot inflate its own words.
    # Over the usable purposes ONLY — the stubs are matched but must not shift the weights, or the
    # floor would be affecting the scores of the very rows it claims to have no opinion about.
    df = bc.document_frequency(keywords[c].keys() for c in usable)
    n_companies = len(usable)

    # ---- pass 3: the answer -------------------------------------------------------------
    scope = sorted(facts)
    if limit:
        scope = scope[:limit]

    # Which model last answered each company, read from the file we are about to replace.
    #
    # The cache key contains the model name, so a lookup only finds answers from the model configured
    # RIGHT NOW. Without `$env:OLLAMA_MODEL` set, that is the `qwen2.5:7b` default — which is not even
    # pulled on this machine — so every previous answer would miss and a free re-run would silently
    # report the keyword-only numbers. Carrying the names forward makes the file self-describing:
    # re-running finds last time's answers whatever the environment says today.
    previous_model: dict[str, str] = {}
    if PURPOSE_BRANCHES_PATH.exists():
        for row in read_jsonl(PURPOSE_BRANCHES_PATH):
            if row.get("company_id") and row.get("model_name"):
                previous_model[row["company_id"]] = row["model_name"]
        if previous_model:
            log.info("%d companies were answered by %s on an earlier run",
                     len(previous_model), ", ".join(sorted(set(previous_model.values()))))

    PURPOSE_BRANCHES_PATH.parent.mkdir(parents=True, exist_ok=True)
    if PURPOSE_BRANCHES_PATH.exists():
        PURPOSE_BRANCHES_PATH.unlink()

    # ---- the model, if it was asked for ------------------------------------------------
    # Checked once, up front, and it stops the run rather than degrading it. A command that silently
    # does nothing because Ollama is down is worse than one that refuses to start: the numbers would
    # look like "the model had no opinion" and nobody would know to re-run.
    all_fields = taxonomy.branch_names()
    if use_llm or semantic:
        from combined_discovery import branch_llm

        reachable, detail = branch_llm.availability()
        if not reachable:
            raise SystemExit(f"a model was asked for and it is unreachable: {detail}")
        if use_llm:
            log.info("Tier A (3-way tie-break) enabled: %s", detail)
        if semantic:
            log.info("Tier B (%d-way, no keyword to anchor it) enabled: %s", len(all_fields), detail)
            log.info("  budget this run: %s new calls",
                     llm_budget if llm_budget is not None else "unlimited")

    # ---- the named companies, resolved from whatever was typed -------------------------
    # A UUID is 36 characters and nobody wants to retype one, so a unique prefix is accepted. An
    # ambiguous or unknown one is an error rather than a silent no-op: "I asked about this company and
    # nothing happened" is the exact confusion this flag exists to remove.
    only_ids: set[str] | None = None
    if only:
        only_ids = set()
        for wanted in only:
            wanted = wanted.strip().lower()
            if not wanted:
                continue
            matches = [cid for cid in facts if cid.lower().startswith(wanted)]
            if not matches:
                raise SystemExit(f"--company-id {wanted!r} matches no company in public.companies")
            if len(matches) > 1:
                raise SystemExit(
                    f"--company-id {wanted!r} is ambiguous — it matches {len(matches)} companies. "
                    f"Use more characters: {', '.join(sorted(matches)[:3])}…"
                )
            only_ids.add(matches[0])
        log.info("restricting NEW model calls to %d named company/companies", len(only_ids))
        for cid in sorted(only_ids):
            name = facts[cid].get("company_name") or ""
            chars = len(bc.normalize(facts[cid].get("business_purpose")))
            log.info("  %s  %-34s %d chars of register text", cid, name[:34], chars)
            if chars <= pb.MIN_PURPOSE_CHARS:
                log.warning(
                    "  !! that is at or under the %d-char floor, so there is nothing to ask about — "
                    "this row stays `no_purpose` whatever the model could have said",
                    pb.MIN_PURPOSE_CHARS,
                )

    # Rows barred from a NEW model call because the register no longer lists them as `aktiv`. Same
    # shape and same rule as column 1: keywords still score every company, every company still gets a
    # line, only the slow part is withheld.
    skip_model_for = {
        company_id: facts[company_id].get("register_status") != "aktiv" for company_id in scope
    } if active_only() else {}
    if skip_model_for:
        log.info(
            "DISCOVERY_ACTIVE_ONLY: %d of %d companies are not aktiv — no new model call for them",
            sum(1 for skip in skip_model_for.values() if skip), len(scope),
        )

    counts: dict[str, int] = {}
    asked = served_from_cache = 0
    started_at = time.time()
    for company_id in scope:
        fact = facts[company_id]
        purpose = fact.get("business_purpose")
        assignment = pb.classify_purpose(
            taxonomy=taxonomy,
            df=df,
            n_companies=n_companies,
            keywords_by_source=keywords.get(company_id),
            had_purpose=pb.is_usable(purpose),
        )

        # Which question, if any, this row is eligible for. Tier A arbitrates between 3 candidates the
        # keywords nominated; Tier B asks about all 77 where no keyword fired at all.
        if assignment.outcome == pb.TORN and assignment.askable:
            eligible = (assignment.shortlist, PURPOSE_TIER_A_NAMESPACES,
                        pb.DECIDED_BY_MODEL_TIEBREAK, use_llm)
        elif assignment.outcome == pb.NO_KEYWORD:
            eligible = (all_fields, PURPOSE_TIER_B_NAMESPACES,
                        pb.DECIDED_BY_MODEL_DIRECT, semantic)
        else:
            eligible = None

        if eligible:
            options, namespaces, tier, tier_on = eligible
            # A NEW call always goes to the newest namespace; the older ones are read-only history.
            namespace = namespaces[0]
            # Named companies jump the queue; everything else is frozen out of NEW calls. A cached
            # answer is still applied below either way, so this never loses work already paid for.
            if only_ids is not None and company_id not in only_ids:
                tier_on = False

            # Same mechanism, different reason: a company the register no longer lists as `aktiv`
            # buys no NEW call. It is worth 41% of this command's model time — 232 of the 567 answers
            # in the file went to a dead company, because a register purpose survives the company
            # that filed it. Both tiers are covered, since both arrive through `tier_on`.
            if skip_model_for.get(company_id):
                tier_on = False

            # **An answer already paid for is applied on EVERY run, tier flag or not.** Otherwise a
            # free re-run rewrites this file as though the model had never spoken, the counts drop,
            # and nothing says why — the cache would still hold all of it. Only a NEW call is gated.
            # Newest namespace first, then the older ones. That second loop is what stops a prompt
            # change from destroying answers: moving Tier B to `-rank3` emptied its namespace, and on
            # the first free re-run 1,285 paid-for answers — 553 fields and 732 abstentions —
            # vanished from the file and reappeared as `no_keyword`. The cache still held every one of
            # them. An older entry is applied exactly as it was written, with no ranking, which is the
            # truth about a single-answer ask.
            verdict = None
            for lookup_namespace in namespaces:
                for candidate_model in (previous_model.get(company_id), None):
                    verdict = branch_llm_cached(
                        options=options, company_id=company_id,
                        taxonomy_version=taxonomy.version, namespace=lookup_namespace,
                        model_name=candidate_model,
                    )
                    if verdict is not None:
                        break
                if verdict is not None:
                    break
            if verdict is not None:
                served_from_cache += 1
                assignment = pb.apply_model_verdict(
                    assignment,
                    branch=verdict.branch, rejected_all=verdict.rejected_all,
                    unsure=verdict.unsure, refused=verdict.refused,
                    reason=verdict.reason, model_name=verdict.model_name, tier=tier,
                    # Empty for every answer cached before the ranked tier existed, which is the
                    # truth about them: they were single-answer asks and had no ordering.
                    model_ranked=verdict.ranked,
                )
            # `llm_budget` counts NEW calls only, which is what makes re-running make progress rather
            # than repeat itself — 6,481 rows is too many for one sitting at any local speed.
            elif tier_on and (llm_budget is None or asked < llm_budget):
                assignment, called = _ask_the_model(
                    assignment, fact, purpose, company_id, taxonomy,
                    options=options, namespace=namespace, tier=tier,
                )
                asked += called
                if called:
                    # ONE LINE PER CALL, with what the model actually said.
                    #
                    # It used to print every 25 calls, which at ten seconds a call is four minutes of
                    # silence — indistinguishable from a hung process, and this run takes hours. A
                    # line per call also lets a bad batch be spotted and killed early: twenty
                    # `Dienstleistung` answers in a row is visible immediately, and everything
                    # answered so far is already cached, so stopping costs nothing.
                    said = (
                        assignment.branch or
                        ("keine" if assignment.outcome == pb.NONE_APPLY else
                         "unsicher" if assignment.outcome == pb.UNSURE else
                         f"FAILED {assignment.llm_refused}"[:34])
                    )
                    budget_label = f"/{llm_budget}" if llm_budget else ""
                    log.info(
                        "  [%d%s] %-30s -> %-34s %s",
                        asked, budget_label,
                        (fact.get("company_name") or "")[:30],
                        said[:34],
                        (assignment.llm_reason or "")[:46],
                    )
                    # A rollup every 50, because a per-call line cannot show a rate or an ETA and
                    # "how long is left" is the question a multi-hour run actually raises.
                    if asked % 50 == 0:
                        elapsed = time.time() - started_at
                        per_call = elapsed / asked
                        left = (llm_budget - asked) if llm_budget else (
                            counts.get(pb.NO_KEYWORD, 0) + len(scope) - sum(counts.values())
                        )
                        fields = sum(counts.get(o, 0) for o in pb.ASSIGNED_OUTCOMES)
                        log.info(
                            "  --- %d calls in %s, %.1f s each | %d fields so far | %s left (~%s) ---",
                            asked, _hms(elapsed), per_call, fields, left, _hms(left * per_call),
                        )

        counts[assignment.outcome] = counts.get(assignment.outcome, 0) + 1

        snippets: dict[str, str] = {}
        fallback = ""
        if assignment.keywords or purpose:
            snippets, fallback = evidence_snippets(
                {"purpose": purpose or ""}, assignment.keywords, taxonomy
            )
        append_jsonl(PURPOSE_BRANCHES_PATH, {
            "company_id": company_id,
            "company_name": fact.get("company_name") or "",
            # Whether the register still lists the company. This column reaches far more dead
            # companies than column 1 does — a purpose outlives the company that filed it, while a
            # website does not — so slicing the counts by this is the only way to see it.
            "register_status": fact.get("register_status"),
            # The column itself. `purpose_field` rather than `branch`, so a row from this file can
            # never be mistaken for a row from `branches.jsonl` by a reader looking at keys.
            "purpose_field": assignment.value,
            "outcome": assignment.outcome,
            "decided_by": assignment.decided_by,
            "score": assignment.score,
            "runner_up": assignment.runner_up,
            "runner_up_score": assignment.runner_up_score,
            "ranked": assignment.ranked[:5],
            # The top three, best first, as {rank, field, score, basis}. `purpose_field` above stays
            # the rank-1 string — this is additive, so `classify-purpose-report`, `compare-columns`
            # and the handover CSV keep reading what they always read.
            #
            # `score` is filled only where one was MEASURED. A keyword row carries its weight sum
            # (basis `keyword_weight`); a model row carries None (basis `model_rank`), because a
            # model has no calibrated number and inventing one would put a plausible decimal in the
            # same column as 7.487 with nothing able to tell them apart.
            "candidates": pb.purpose_candidates(assignment),
            "keywords": assignment.keywords,
            "evidence": snippets or ({"_text": fallback} if fallback else {}),
            "shortlist": assignment.shortlist,
            # What the length floor took away, when it took anything. Never an answer; the outcome
            # stays `no_purpose`. Reported so the floor can be argued with rather than assumed.
            "withheld_by_floor": assignment.withheld_by_floor,
            # The text every answer above was derived from, EXACTLY as the register filed it — not
            # normalised, not truncated. This is what makes a model answer checkable: `llm_reason`
            # says why it chose, and this says what it was looking at. Abridging it would leave a
            # reader checking a claim against a shortened source.
            "purpose_text": (purpose or "").strip(),
            # Phase 2 fills these. Written now, empty, so the schema does not change when the model
            # is switched on and a reader can tell "not asked" from "key missing".
            "llm_branch": assignment.branch if assignment.decided_by in (
                pb.DECIDED_BY_MODEL_TIEBREAK, pb.DECIDED_BY_MODEL_DIRECT) else None,
            "llm_reason": assignment.llm_reason,
            "llm_refused": assignment.llm_refused,
            "model_name": assignment.model_name,
            # Which run this is. A keyword's weight depends on the corpus, so the same company can
            # carry different answers in two files and neither is wrong.
            "taxonomy_version": taxonomy.version,
            "n_companies": n_companies,
            "df_companies": n_companies,
            "purpose_chars": len(bc.normalize(purpose)),
        })

    # ---- what the named companies came back with ---------------------------------------
    # Printed in full, because the whole point of the flag is answering "why is this row empty"
    # without opening a 7 MB spreadsheet afterwards.
    if only_ids:
        print()
        print(f"=== the {len(only_ids)} company/companies you named ===")
        for row in read_jsonl(PURPOSE_BRANCHES_PATH):
            if row.get("company_id") not in only_ids:
                continue
            print()
            print(f"  {row['company_name']}")
            print(f"    company_id : {row['company_id']}")
            print(f"    outcome    : {row['outcome']}")
            print(f"    field      : {row['purpose_field']}")
            print(f"    decided_by : {row['decided_by'] or '(nobody)'}")
            if row.get("keywords"):
                print(f"    keywords   : {', '.join(sorted(row['keywords']))}")
            if row.get("model_name"):
                print(f"    model      : {row['model_name']}")
            if row.get("llm_reason"):
                print(f"    reason     : {row['llm_reason']}")
            if row.get("llm_refused"):
                print(f"    call FAILED: {row['llm_refused']}")
            text = row.get("purpose_text") or ""
            print(f"    register   : {text[:400]}{'...' if len(text) > 400 else ''}")
            if row["outcome"] == "no_keyword":
                print(f"    ^^ no model has answered this row yet. Add --llm with the semantic tier.")

    log.info("wrote %d rows to %s", len(scope), PURPOSE_BRANCHES_PATH)
    for outcome, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        log.info("  %-20s %6d", outcome, count)
    # Printed on EVERY run, not only when a tier is on. The run that silently dropped 1,285
    # paid-for answers was a free one, and it said nothing at all: without this line the only symptom
    # was an outcome count that had quietly changed, in a file nobody diffs.
    log.info("model answers applied from cache: %d (new calls this run: %d)", served_from_cache, asked)

    # The signature of a cache that has become unreachable rather than empty. Two ways to get here,
    # and the fix differs, so both are named:
    #
    #  * the model name. The cache key includes it, and `OLLAMA_MODEL` defaults in code to
    #    `qwen2.5:7b`, which is not the model that answered. `previous_model` normally supplies the
    #    right name from THIS file — so a file rewritten while the variable was wrong loses the
    #    ability to find its own answers, which is how 1,285 of them went missing.
    #  * a prompt change that moved to a new namespace without keeping the old one readable. See
    #    `PURPOSE_TIER_B_NAMESPACES`.
    if served_from_cache == 0 and counts.get(pb.NO_KEYWORD, 0):
        from combined_discovery import page_cache

        on_disk = sum(page_cache.stats().get(name, 0) for name in PURPOSE_TIER_B_NAMESPACES)
        if on_disk:
            log.warning(
                "%d answers sit in the Tier B cache and NONE was applied. They are unreachable, not "
                "absent. Almost always the model name: set $env:OLLAMA_MODEL to the model that "
                "answered (the code default qwen2.5:7b is not it) and re-run. Namespaces read: %s",
                on_disk, ", ".join(PURPOSE_TIER_B_NAMESPACES),
            )

    if use_llm or semantic:
        if semantic and llm_budget is not None and asked >= llm_budget:
            remaining = counts.get(pb.NO_KEYWORD, 0)
            log.info("budget of %d spent; %d rows are still `no_keyword`. Re-run to continue — the "
                     "answers so far are cached and will not be paid for twice.", llm_budget, remaining)


def classify_purpose_report() -> None:
    """What COLUMN 3 says. NOT whether it is right — no row here has ever been hand-checked."""
    from combined_discovery import purpose_branch as pb

    rows = read_jsonl(PURPOSE_BRANCHES_PATH)
    if not rows:
        raise SystemExit(f"nothing in {PURPOSE_BRANCHES_PATH}; run `classify-purpose` first")
    n = len(rows)
    counts: dict[str, int] = {}
    for row in rows:
        key = row.get("outcome") or "?"
        counts[key] = counts.get(key, 0) + 1

    print(f"\n=== COLUMN 3: what the register says, for {n} companies ===")

    versions = {r.get("taxonomy_version") for r in rows if r.get("taxonomy_version")}
    corpora = {r.get("df_companies") for r in rows if r.get("df_companies")}
    print(f"\n  taxonomy+rule version   {', '.join(sorted(str(v) for v in versions)) or '?'}")
    print(f"  keyword weights over    {', '.join(str(c) for c in sorted(corpora)) or '?'} usable purposes")
    if len(versions) > 1 or len(corpora) > 1:
        print("  !! this file mixes runs — the rows are not comparable; re-run `classify-purpose`")
    print(
        "\n  This column answers 'what was it REGISTERED to do?'. The Technologiefeld answers 'what\n"
        "  does it WORK in?' and requires a website; this one deliberately does not. Neither is a\n"
        "  check on the other, and neither may be written into the other's column."
    )

    # ---- the outcomes, never summed -----------------------------------------------------
    order = (
        pb.ASSIGNED_KEYWORDS, pb.ASSIGNED_MODEL_3WAY, pb.ASSIGNED_MODEL_77,
        pb.TORN, pb.NONE_APPLY, pb.UNSURE, pb.NO_KEYWORD, pb.NO_PURPOSE,
    )
    print("\n-- one field, or NONE for one of several DIFFERENT reasons --")
    for outcome in order:
        got = counts.get(outcome, 0)
        label = "a Berlin field" if outcome in pb.ASSIGNED_OUTCOMES else "NONE"
        print(f"{label:<16}{outcome:<22}{got:>7}  {_pct(got, n)}")
    for outcome, count in counts.items():
        if outcome not in order:
            print(f"{'?':<16}{outcome:<22}{count:>7}  {_pct(count, n)}   !! unknown outcome")
    total = sum(counts.values())
    print(f"{'':<16}{'TOTAL':<22}{total:>7}")
    if total != n:
        print("  !! the outcomes do not sum to the row count — something is being double-counted")
    print(
        "\n  NEVER add two of those together. `no_purpose` means the register said nothing we could\n"
        "  read; `no_keyword` means we read it and not one of the taxonomy's words appeared; `torn`\n"
        "  means two fields were too close; `none_apply` means a model read it and said no field\n"
        "  fits; `unsure` means a model read it and could not tell. Only the last two are a claim\n"
        "  about the company — the rest are statements about our evidence."
    )

    # ---- the denominator that actually describes this column -----------------------------
    usable = n - counts.get(pb.NO_PURPOSE, 0)
    assigned = sum(counts.get(o, 0) for o in pb.ASSIGNED_OUTCOMES)
    print("\n-- measured against the companies this column can reach --")
    print(f"{'companies with a usable purpose':<40}{usable:>7}  {_pct(usable, n)} of the table")
    print(f"{'…given a field':<40}{assigned:>7}  {_pct(assigned, usable)} of those")

    # ---- what the length floor cost, which is the whole point of recording it -------------
    floored = [r for r in rows if r.get("withheld_by_floor")]
    print("\n-- what the length floor threw away --")
    print(f"{'the floor, in characters':<40}{pb.MIN_PURPOSE_CHARS:>7}")
    print(f"{'purposes excluded as too short':<40}{counts.get(pb.NO_PURPOSE, 0):>7}")
    print(f"{'…that WOULD have produced a field':<40}{len(floored):>7}")
    if floored:
        by_field: dict[str, int] = {}
        for row in floored:
            by_field[row["withheld_by_floor"]] = by_field.get(row["withheld_by_floor"], 0) + 1
        print("\n  the fields it cost, and the shortest text in each:")
        shortest: dict[str, tuple[int, str]] = {}
        for row in floored:
            field_name = row["withheld_by_floor"]
            chars = row.get("purpose_chars") or 0
            if field_name not in shortest or chars < shortest[field_name][0]:
                shortest[field_name] = (chars, row.get("company_name") or "")
        for field_name, count in sorted(by_field.items(), key=lambda kv: -kv[1]):
            chars, name = shortest[field_name]
            print(f"    {field_name[:30]:<32}{count:>4}   shortest {chars:>3} chars  {name[:26]}")
        print(
            "\n  Read that list before defending the floor. A short register purpose is often the\n"
            "  LEAST ambiguous one there is — \"Gastronomie\", \"Pflegedienst\", \"Textil-Einzelhandel\"\n"
            "  name a trade and nothing else. The floor was set to exclude vague text, and length\n"
            "  turns out to be a poor proxy for vagueness. Lowering it is a measured decision, and\n"
            "  this is the measurement; PURPOSE_MIN_CHARS is the knob."
        )

    # ---- who decided, and how much to trust it -------------------------------------------
    by_decider: dict[str, int] = {}
    for row in rows:
        if row.get("decided_by"):
            by_decider[row["decided_by"]] = by_decider.get(row["decided_by"], 0) + 1
    if by_decider:
        print("\n-- who decided (this is what says how far to trust a row) --")
        for decider in (pb.DECIDED_BY_KEYWORDS, pb.DECIDED_BY_MODEL_TIEBREAK, pb.DECIDED_BY_MODEL_DIRECT):
            print(f"{decider:<40}{by_decider.get(decider, 0):>7}")
        print(
            "\n  A `keywords` row is traceable to a word literally in the register text. A\n"
            "  `model_tiebreak` row chose among 3 candidates the keywords had already nominated. A\n"
            "  `model_direct` row chose from all 77 with no keyword at all — nothing anchors it to\n"
            "  the text, so it is the one to distrust."
        )

    # ---- how thin the evidence is --------------------------------------------------------
    assigned_rows = [r for r in rows if r.get("outcome") in pb.ASSIGNED_OUTCOMES]
    one_keyword = sum(1 for r in assigned_rows if len(r.get("keywords") or {}) == 1)
    if assigned_rows:
        print("\n-- how much evidence each field rests on --")
        print(f"{'fields resting on ONE keyword':<40}{one_keyword:>7}  "
              f"{_pct(one_keyword, len(assigned_rows))} of the fields")
        print(
            "\n  Register prose is short — 251 characters on average — so most rows will fire exactly\n"
            "  one keyword. That is not a bug, but it is the reason this column deserves less trust\n"
            "  than the website column, where four texts can agree."
        )

    # ---- the score distribution, so the thresholds can be argued with --------------------
    scored = sorted(r.get("score") or 0.0 for r in rows if r.get("score"))
    if scored:
        import bisect

        from combined_discovery import branch_classifier as bc

        # Two different populations, kept apart on purpose. Mixing them produces the nonsense of a
        # "winning" score below the floor that no winner could have had.
        won = sorted(r.get("score") or 0.0 for r in assigned_rows if r.get("score"))
        print("\n-- the score distribution, against thresholds calibrated on WEBSITE text --")
        print(f"{'MIN_SCORE (the floor)':<40}{bc.MIN_SCORE:>7.2f}")
        print(f"{'MIN_MARGIN (times the runner-up)':<40}{bc.MIN_MARGIN:>7.2f}")
        print(f"\n{'rows that scored anything at all':<40}{len(scored):>7}")
        for label, value in (("lowest", scored[0]), ("median", scored[len(scored) // 2]),
                             ("highest", scored[-1])):
            print(f"{'  ' + label:<40}{value:>7.2f}")
        if won:
            print(f"\n{'…of those, the ones that WON a field':<40}{len(won):>7}")
            for label, value in (("lowest", won[0]), ("median", won[len(won) // 2]),
                                 ("highest", won[-1])):
                print(f"{'  ' + label:<40}{value:>7.2f}")
        below = bisect.bisect_left(scored, bc.MIN_SCORE)
        print(f"\n{'scored something but under the floor':<40}{below:>7}")
        print(
            "\n  Purposes are far shorter than homepages, so every score here sits lower than the\n"
            "  numbers these two thresholds were tuned against. The last row is the one to argue\n"
            "  with: those companies had evidence that the floor threw away. If it is large, the\n"
            "  floor is what costs coverage here — not the taxonomy."
        )

    # ---- what a model could ever add -----------------------------------------------------
    torn = [r for r in rows if r.get("outcome") == pb.TORN]
    askable = [r for r in torn if r.get("shortlist")]
    print("\n-- what Phase 2 Tier A (the 3-way tie-break) could ever reach --")
    print(f"{'torn rows':<40}{len(torn):>7}")
    print(f"{'…askable (a shortlist was built)':<40}{len(askable):>7}")
    print(f"{'…NOT askable, so no model will see them':<40}{len(torn) - len(askable):>7}")
    print(
        f"\n  `worth_asking` needs at least BRANCH_MIN_KEYWORDS_FOR_MODEL keywords and a score at the\n"
        f"  floor. Most rows here fire exactly one keyword, so that constant — not the prompt — is\n"
        f"  what governs how far the tie-break can reach. Raising it is a measured decision; the\n"
        f"  askable count above is the measurement."
    )

    # ---- the model, split by the question it was asked -----------------------------------
    # Tier A and Tier B are never added together. "One of these 3 the keywords nominated" and "one of
    # all 77, with nothing anchoring it" are different questions, and an abstention means something
    # different in each. `decided_by` is what keeps them apart.
    touched = [r for r in rows if r.get("decided_by", "").startswith("model") or r.get("llm_refused")]
    if touched:
        print("\n-- the model, by the question it was asked --")
        print(f"{'':<24}{'tier A (3-way)':>16}{'tier B (77-way)':>17}")
        tiers = (pb.DECIDED_BY_MODEL_TIEBREAK, pb.DECIDED_BY_MODEL_DIRECT)
        for label, predicate in (
            ("named a field", lambda r, t: r.get("decided_by") == t and r.get("llm_branch")),
            ("said keine", lambda r, t: r.get("decided_by") == t and r.get("outcome") == pb.NONE_APPLY),
            ("said unsicher", lambda r, t: r.get("decided_by") == t and r.get("outcome") == pb.UNSURE),
        ):
            cells = "".join(f"{sum(1 for r in rows if predicate(r, t)):>17}" for t in tiers)
            print(f"{label:<24}{cells}")

        models = sorted({r.get("model_name") for r in touched if r.get("model_name")})
        print(f"\n  model(s): {', '.join(models) or '?'}")

        # ---- the calibration check that costs nothing ------------------------------------
        direct = [r for r in rows if r.get("decided_by") == pb.DECIDED_BY_MODEL_DIRECT]
        if direct:
            refused_direct = sum(
                1 for r in direct if r.get("outcome") in (pb.NONE_APPLY, pb.UNSURE)
            )
            print("\n-- did Tier B refuse often enough? --")
            print(f"{'rows it answered':<40}{len(direct):>7}")
            print(f"{'…of which keine or unsicher':<40}{refused_direct:>7}  "
                  f"{_pct(refused_direct, len(direct))}")
            print(
                "\n  2,294 of the 7,141 usable purposes — 32% — are holding, asset-management or\n"
                "  administrative boilerplate, measured independently of any model. So on a random\n"
                "  sample of the no_keyword rows something near a THIRD should come back refused.\n"
                "  Far below that and the model is under-refusing, which is the documented failure:\n"
                "  a confident field for a company whose register text says only that it\n"
                "  administers its own assets. Far above it and the prompt has gone timid."
            )

    failed = [r for r in rows if r.get("llm_refused")]
    if failed:
        reasons: dict[str, int] = {}
        for row in failed:
            head = str(row["llm_refused"]).split(":")[0]
            reasons[head] = reasons.get(head, 0) + 1
        print(f"\n{'model calls that BROKE (not a refusal)':<40}{len(failed):>7}")
        for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print(f"    {reason[:34]:<36}{count:>5}")
        print(
            "\n  These kept whatever the keywords said — `torn` or `no_keyword` — and are NEVER\n"
            "  `none_apply`. 'We got no answer' and 'the model says no field fits' are different\n"
            "  findings, and only the second one is about the company. `off-list` means the model\n"
            "  invented a field name; a lot of those means the 77-way question is too wide for it."
        )

    # ---- the free comparison nobody has had yet ------------------------------------------
    website_rows = {r["company_id"]: r for r in read_jsonl(BRANCHES_PATH) if r.get("company_id")}
    if website_rows:
        both = agree = 0
        examples: list[tuple[str, str, str]] = []
        for row in rows:
            other = website_rows.get(row.get("company_id"))
            if not other:
                continue
            mine = row.get("purpose_field")
            theirs = other.get("technologiefeld") or other.get("branch")
            if not mine or mine == pb.NONE or not theirs or theirs == pb.NONE:
                continue
            both += 1
            if mine == theirs:
                agree += 1
            elif len(examples) < 8:
                examples.append((row.get("company_name") or "", theirs, mine))
        print("\n-- where the two columns can be compared at all --")
        print(f"{'companies with BOTH columns filled':<40}{both:>7}")
        print(f"{'…the two agree':<40}{agree:>7}  {_pct(agree, both)}")
        print(f"{'…they disagree':<40}{both - agree:>7}  {_pct(both - agree, both)}")
        if examples:
            print("\n  a few disagreements (website field | register field):")
            for name, theirs, mine in examples:
                print(f"    {name[:34]:<36}{theirs[:24]:<26}{mine[:24]}")
        print(
            "\n  Disagreement is not error. A company registered to trade goods and now selling\n"
            "  software has two true answers, and this is the first time we can see how often that\n"
            "  happens. What it does NOT tell you is which column is right."
        )

    print(
        "\n\n  NOT KNOWN: whether any row above is correct. No purpose row has ever been\n"
        "  hand-checked, so this column has no precision figure — only counts. The model's\n"
        "  `llm_reason` per row is the cheapest proxy: read twenty and you will know whether it is\n"
        "  reading the register text or guessing from the company name."
    )




COMPARE_PATH = OUT_DIR / "three_columns_compared.csv"

COMPARE_COLUMNS = [
    "company_id", "company_name",
    # COLUMN 1 — what the company's own website says it works in.
    "col1_technologiefeld", "col1_outcome", "col1_decided_by",
    # COLUMN 2 — what it is registered as. A different taxonomy; never comparable to 1 or 3.
    "col2_wz_code", "col2_wz_division_title", "col2_wz_section_title",
    # COLUMN 3 — what the register says it was set up to do.
    "col3_purpose_field", "col3_outcome", "col3_decided_by", "col3_llm_reason",
    # The comparison itself, and the evidence behind it.
    "verdict", "how_many_columns", "purpose_text",
]

#: How columns 1 and 3 relate on one company. Only `agree` and `disagree` are judgements; the rest
#: say WHY no comparison was possible, which for most companies is the honest answer.
VERDICTS = (
    ("agree", "both named a Berlin field, the same one"),
    ("disagree", "both named a Berlin field, different ones"),
    ("only_website", "column 1 has a field, the register gave none"),
    ("only_register", "column 3 has a field, the website gave none"),
    ("neither", "no Berlin field from either source"),
)


def compare_columns(limit: int | None) -> None:
    """All three industry columns for one company, side by side. Free — reads the three files only.

    The point is that these are **three different questions**, and until they sit on one row nobody
    can see how they relate. What it can show:

    - where columns 1 and 3 both answered, and whether they agree. They share Berlin's 77 values, so
      this comparison is meaningful — and disagreement is a finding, not an error: a company
      registered to trade goods that now sells software has two true answers.
    - where only one of them answered, which is most companies, and which column reached ground the
      other could not.
    - what column 2 said, alongside, WITHOUT pretending it is comparable. The WZ classification is a
      different authority answering a different question in a different vocabulary; it is printed for
      context and never scored against the other two.

    Ordered worst-comparable first — `disagree`, then `agree`, then the single-column rows — because
    the disagreements are the only rows that can teach anything about either column's accuracy, and
    there are few enough to read by hand.
    """
    from combined_discovery import branch_classifier as bc

    # The literal a column carries when no field was established. Taken from the classifier rather
    # than spelled again here, so the two can never drift apart.
    none_field = bc.NONE

    branch_rows = {r["company_id"]: r for r in read_jsonl(BRANCHES_PATH) if r.get("company_id")}
    purpose_rows = {r["company_id"]: r for r in read_jsonl(PURPOSE_BRANCHES_PATH) if r.get("company_id")}
    if not branch_rows and not purpose_rows:
        raise SystemExit(
            f"nothing to compare: run `classify` and `classify-purpose` first "
            f"({BRANCHES_PATH.name}, {PURPOSE_BRANCHES_PATH.name})"
        )
    if not branch_rows:
        log.warning("%s is empty — columns 1 and 2 will be blank", BRANCHES_PATH.name)
    if not purpose_rows:
        log.warning("%s is empty — column 3 will be blank", PURPOSE_BRANCHES_PATH.name)

    # A row count far below the table is almost always a `--limit` left on an earlier run, which
    # rewrites the whole file rather than appending. Worth saying out loud: the comparison would
    # silently describe 100 companies while looking like it described the table.
    for name, rows in ((BRANCHES_PATH.name, branch_rows), (PURPOSE_BRANCHES_PATH.name, purpose_rows)):
        if rows and len(rows) < 1000:
            log.warning(
                "%s holds only %d rows — it was written with --limit. Re-run without one for the "
                "whole table; both commands are free.", name, len(rows)
            )

    def field(row: dict[str, Any] | None, *keys: str) -> str:
        """The first non-empty, non-NONE value among `keys`. NONE and blank both mean 'no answer'."""
        for key in keys:
            value = (row or {}).get(key) or ""
            if value and value != none_field:
                return str(value)
        return ""

    out: list[dict[str, Any]] = []
    for company_id in sorted(set(branch_rows) | set(purpose_rows)):
        b = branch_rows.get(company_id) or {}
        p = purpose_rows.get(company_id) or {}
        col1 = field(b, "technologiefeld", "branch")
        col2 = (b.get("wz_code") or "")
        col3 = field(p, "purpose_field")

        if col1 and col3:
            verdict = "agree" if col1 == col3 else "disagree"
        elif col1:
            verdict = "only_website"
        elif col3:
            verdict = "only_register"
        else:
            verdict = "neither"

        out.append({
            "company_id": company_id,
            "company_name": b.get("company_name") or p.get("company_name") or "",
            "col1_technologiefeld": col1 or none_field,
            "col1_outcome": b.get("outcome") or "",
            "col1_decided_by": b.get("decided_by") or "",
            "col2_wz_code": col2,
            "col2_wz_division_title": b.get("wz_division_title") or "",
            "col2_wz_section_title": b.get("wz_section_title") or "",
            "col3_purpose_field": col3 or none_field,
            "col3_outcome": p.get("outcome") or "",
            "col3_decided_by": p.get("decided_by") or "",
            "col3_llm_reason": p.get("llm_reason") or "",
            "verdict": verdict,
            "how_many_columns": sum(bool(x) for x in (col1, col2, col3)),
            "purpose_text": p.get("purpose_text") or "",
        })

    order = {name: i for i, (name, _) in enumerate(VERDICTS)}
    out.sort(key=lambda r: (order.get(r["verdict"], 9), r["company_name"]))
    if limit:
        out = out[:limit]

    COMPARE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with COMPARE_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COMPARE_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out)

    # ---- what the comparison says -------------------------------------------------------
    counts = collections.Counter(r["verdict"] for r in out)
    n = len(out)
    print(f"\n=== three industry columns over {n} companies ===")
    print(
        "\n  COL 1  Technologiefeld  what it WORKS IN     from its own website   needs a website\n"
        "  COL 2  WZ cluster       what it is REGISTERED AS  from its WZ code  a lookup, no judgement\n"
        "  COL 3  purpose_field    what it was SET UP TO DO  from the register  needs 40+ chars\n"
        "\n  Columns 1 and 3 share Berlin's 77 values, so they can be compared. Column 2 cannot be\n"
        "  compared with either — different authority, different vocabulary, different question."
    )

    print("\n-- how columns 1 and 3 relate --")
    for name, meaning in VERDICTS:
        print(f"{name:<16}{counts.get(name, 0):>7}  {_pct(counts.get(name, 0), n)}  {meaning}")

    comparable = counts.get("agree", 0) + counts.get("disagree", 0)
    if comparable:
        print(f"\n{'companies where BOTH answered':<40}{comparable:>7}")
        print(f"{'…and they agree':<40}{counts.get('agree', 0):>7}  "
              f"{_pct(counts.get('agree', 0), comparable)}")
        print(
            "\n  Read that percentage carefully: it is NOT an accuracy figure. Neither column is\n"
            "  ground truth, so a disagreement says the two sources describe the company\n"
            "  differently — which is often true of the company rather than false of the data."
        )

    print("\n-- what each column reached --")
    for label, key in (("column 1, Technologiefeld", "col1_technologiefeld"),
                       ("column 2, WZ code", "col2_wz_code"),
                       ("column 3, purpose_field", "col3_purpose_field")):
        got = sum(1 for r in out if r[key] and r[key] != none_field)
        print(f"{label:<40}{got:>7}  {_pct(got, n)}")
    none_at_all = sum(1 for r in out if r["how_many_columns"] == 0)
    print(f"{'no column at all':<40}{none_at_all:>7}  {_pct(none_at_all, n)}")

    disagreements = [r for r in out if r["verdict"] == "disagree"]
    if disagreements:
        print(f"\n-- every disagreement, worth reading by hand ({len(disagreements)}) --")
        print(f"  {'company':<26}{'website says':<26}{'register says':<26}decided by")
        for r in disagreements[:40]:
            print(f"  {r['company_name'][:24]:<26}{r['col1_technologiefeld'][:24]:<26}"
                  f"{r['col3_purpose_field'][:24]:<26}{r['col3_decided_by']}")
        if len(disagreements) > 40:
            print(f"  … and {len(disagreements) - 40} more, all in the CSV")

    print(f"\n  wrote {COMPARE_PATH}")
    print(
        "\n  The CSV carries `purpose_text` and `col3_llm_reason` side by side, so a model-decided\n"
        "  row can be judged against the text it was derived from without opening anything else.\n"
        "  Sort by `verdict`. NOT KNOWN: whether any value in any of the three columns is correct —\n"
        "  no row has ever been hand-checked, so there is no precision figure for any of them."
    )


def classify_report() -> None:
    """What happened. NOT whether it was right — that is `classify-labels-report`."""
    rows = read_jsonl(BRANCHES_PATH)
    if not rows:
        raise SystemExit(f"nothing in {BRANCHES_PATH}; run `classify` first")
    n = len(rows)

    print(f"\n=== {n} companies, two independent columns ===")

    # Which run this file is, spelled out. A keyword's weight depends on how many companies use it,
    # so the same company can carry different answers in two files and neither is wrong.
    versions = {r.get("taxonomy_version") for r in rows}
    corpora = {r.get("df_companies") for r in rows if r.get("df_companies")}
    print(f"\n  taxonomy+rule version   {', '.join(sorted(str(v) for v in versions if v)) or '?'}")
    print(f"  keyword weights over    {', '.join(str(c) for c in sorted(corpora)) or '?'} companies with text")
    if len(versions) > 1 or len(corpora) > 1:
        print("  !! this file mixes runs — the rows are not comparable; re-run `classify`")
    print(
        "\n  COLUMN 1  Technologiefeld — one of Berlin's 77 fields, or NONE. Only the words a\n"
        "            company published about itself may fill it.\n"
        "  COLUMN 2  WZ cluster — the official German industry classification the company is\n"
        "            registered under. A different question, by a different authority.\n"
        "\n  They are reported apart because they ARE apart. Neither is a check on the other."
    )

    print(f"\n\n########## COLUMN 1: Technologiefeld ##########\n")
    outcomes: dict[str, int] = {}
    for row in rows:
        outcomes[row.get("outcome") or "?"] = outcomes.get(row.get("outcome") or "?", 0) + 1
    order = ("assigned", "no_website", "torn", "no_keyword", "no_text")
    print("-- one field, or NONE for one of four different reasons --")
    for outcome in order:
        label = "a Berlin field" if outcome == "assigned" else f"NONE: {outcome}"
        print(f"{label:<34}{outcomes.get(outcome, 0):>7}  {_pct(outcomes.get(outcome, 0), n)}")
    for outcome, count in outcomes.items():
        if outcome not in order:
            print(f"{outcome:<34}{count:>7}  {_pct(count, n)}")
    print(
        "\n  `no_website` means the register text named a field and no website was ever found, so\n"
        "  the field was withheld. `torn` means keywords fired and two fields were too close to\n"
        "  separate. `no_keyword` means we read the company's text and not one keyword appeared.\n"
        "  `no_text` means we had nothing to read at all. Adding any two of those together turns a\n"
        "  blank into a claim — and none of them says the company has no technology field."
    )

    # The denominator that describes what this column actually does. A Technologiefeld now requires a
    # website, and a website was found for 902 of 14,443 companies — so measuring against the whole
    # table reports the website pipeline's coverage a second time and reads like failure.
    with_site = sum(1 for r in rows if r.get("website"))
    if with_site:
        print("\n-- measured against the companies this column can reach --")
        print(f"{'companies with a website':<34}{with_site:>7}  {_pct(with_site, n)} of the table")
        print(f"{'…given a Berlin field':<34}{outcomes.get('assigned', 0):>7}  "
              f"{_pct(outcomes.get('assigned', 0), with_site)} of those")
        if outcomes.get("assigned", 0) > with_site:
            # More fields than websites is arithmetically impossible under the rule, so this file was
            # written before it existed. Worth saying out loud: every number above is from the old
            # behaviour and none of it is comparable.
            print(
                "\n  !! more fields than websites — this file predates the website rule, so the\n"
                "     counts above are the OLD behaviour. Re-run `classify`."
            )
        print(
            "\n  Read the second line, not the first. A field requires a website, so the table-wide\n"
            "  percentage is really a report on website discovery, which has its own scorecard."
        )

    # What the website gate took away. Printed every run and on purpose: a rule whose cost is
    # invisible stops being a decision and becomes an assumption. These are answers the company's own
    # register text supported and no website confirmed.
    withheld = [r for r in rows if r.get("outcome") == "no_website" and r.get("withheld_branch")]
    if withheld:
        print("\n-- what the website gate withheld --")
        print(f"{'fields withheld for want of a website':<40}{len(withheld):>7}")
        counts: dict[str, int] = {}
        for row in withheld:
            counts[row["withheld_branch"]] = counts.get(row["withheld_branch"], 0) + 1
        for field, count in sorted(counts.items(), key=lambda kv: -kv[1])[:8]:
            print(f"    {field[:34]:<36}{count:>5}")
        print(
            "\n  A register purpose says what a company is PERMITTED to do, and much of it is\n"
            "  boilerplate — 'Handel mit Waren aller Art'. A website that answers is evidence it\n"
            "  trades. That is the trade this rule makes, and it is a big one: these would all have\n"
            "  been fields before it. The `no_website` rows in the labelling worksheet are the only\n"
            "  thing that can say whether withholding them was right — `classify-labels-report`."
        )

    assigned = [r for r in rows if r.get("outcome") == "assigned"]
    print("\n-- what decided it --")
    deciders: dict[str, int] = {}
    for row in assigned:
        deciders[row.get("decided_by") or "?"] = deciders.get(row.get("decided_by") or "?", 0) + 1
    for decider, count in sorted(deciders.items(), key=lambda kv: -kv[1]):
        print(f"{decider:<34}{count:>7}  {_pct(count, n)}")
    print("\n  `keywords` and `model` are the only two possible values here. A WZ code cannot")
    print("  appear, by construction — branch_classifier.classify() has no parameter for one.")

    print("\n-- which text the keywords came from --")
    from_source: dict[str, int] = {}
    for row in assigned:
        for source in (row.get("keywords") or {}).values():
            from_source[source] = from_source.get(source, 0) + 1
    for source, count in sorted(from_source.items(), key=lambda kv: -kv[1]):
        print(f"{source:<34}{count:>7}  keyword hits")

    # Did fetch-content earn its downloads? Counting decisions the homepage alone would not have
    # reached, the same way `band_without_judge` counts what the model changed.
    gained = [
        r for r in assigned
        if r.get("outcome_without_services") and r["outcome_without_services"] != "assigned"
    ]
    changed = [
        r for r in assigned
        if r.get("branch_without_services") and r["branch_without_services"] != r.get("branch")
    ]
    if any(r.get("outcome_without_services") for r in rows):
        print("\n-- did the downloaded services pages pay for themselves --")
        print(f"{'decided only thanks to them':<34}{len(gained):>7}")
        print(f"{'decided differently because of them':<34}{len(changed):>7}")
        print(
            "\n  The first number is what the ~1,813 downloads bought. If it is near zero the stage\n"
            "  should come out, not stay because it exists."
        )

    # How much evidence is behind an assignment, which is not the same question as how confident
    # the score is. Measured on the first full run: 807 of 992 assignments rested on ONE keyword.
    # That is not automatically wrong — `einzelhandel` (208 rows), `spedition` (39) and
    # `steuerberater` (21) each name an industry on their own — but it is where the wrong answers
    # live, and the two groups have to be labelled separately or one hides the other.
    # Every assignment comes from words the company published; there is no other kind. (This used
    # to filter out `decided_by == "wz"`, from the reverted version that let a register code fill
    # this column. The filter was hedging against something the signature now makes impossible.)
    single = [r for r in assigned if len(r.get("keywords") or {}) == 1]
    print("\n-- how much evidence is behind each assignment --")
    print(f"{'decided by ONE keyword':<34}{len(single):>7}  {_pct(len(single), max(len(assigned), 1))}")
    print(f"{'decided by two or more':<34}{len(assigned) - len(single):>7}")
    lone: dict[str, int] = {}
    for row in single:
        keyword = next(iter(row["keywords"]))
        lone[keyword] = lone.get(keyword, 0) + 1
    print("\n  the keywords carrying a whole assignment on their own, most common first:")
    for keyword, count in sorted(lone.items(), key=lambda kv: -kv[1])[:12]:
        print(f"    {keyword[:30]:<30}{count:>5}")
    print(
        "\n  Read that list as a to-do. A word that names an INDUSTRY belongs there; a word that\n"
        "  names a QUALITY does not. 'sicherheit' is the clear case: Berlin's sheet gives it to\n"
        "  Defence Tech, and on a German business page it means Arbeitssicherheit or\n"
        "  Datensicherheit. No measurement can tell those apart — 'sicherheit' appears on 1.8% of\n"
        "  our texts and 'einzelhandel' on 3.0%, and only one of them is decisive. This is a\n"
        "  judgement for a person, made by deleting the keyword from branch_taxonomy.yaml with a\n"
        "  comment saying why, NOT by adding a stop-list to the code."
    )

    # The list above says which keywords carry an assignment alone. It does NOT say which FIELD each
    # one carries, and that is the number that convicts a bad keyword. `sicherheit` produced 27 of
    # Defence Tech's 35 assignments on its own: not "a common keyword" but "a field that exists
    # because of one word". Reading it as a share per field is what makes the next one visible while
    # it is still eight rows instead of thirty-five.
    # Ranked on ALL the field's one-keyword rows, not just its commonest keyword. Ranking on the
    # commonest one has a blind spot, and it hid a real case: Wasserstoff took 19 of its 22
    # assignments from "netz" (10) and "speicher" (9), and neither reached the 50% a single-keyword
    # ranking looks for. A branch carried by two generic words is in exactly as much trouble as one
    # carried by one, so the share is the field's total and the words are listed beside it.
    print("\n-- fields resting on single keywords --")
    print("  What share of each field was decided by ONE keyword, and which words those were.")
    print("  High is a warning, not a verdict: 'windenergieanlagen' names an industry and carries")
    print("  100% of Windenergie correctly. 'netz' means 'online' and carried half of Wasserstoff.")
    lone_by_field: dict[str, dict[str, int]] = {}
    for row in single:
        field = row.get("technologiefeld") or row.get("branch") or "?"
        keyword = next(iter(row["keywords"]))
        lone_by_field.setdefault(field, {})
        lone_by_field[field][keyword] = lone_by_field[field].get(keyword, 0) + 1
    ranked_fields = []
    for field, count in branch_counts(assigned).items():
        if count < MIN_FIELD_SIZE_FOR_LONE_KEYWORD_WARNING:
            continue
        keywords = lone_by_field.get(field) or {}
        carried = sum(keywords.values())
        if not carried:
            continue
        top = ", ".join(
            f"{k} {n}" for k, n in sorted(keywords.items(), key=lambda kv: -kv[1])[:3]
        )
        ranked_fields.append((carried / count, count, carried, field, top))
    print(f"\n  {'share':>6} {'field':>6} {'lone':>5}   Technologiefeld / the words carrying it")
    for share, count, carried, field, top in sorted(ranked_fields, reverse=True)[:15]:
        print(f"  {share:>6.0%} {count:>6} {carried:>5}   {field[:26]:<28}{top[:44]}")
    print(
        "\n  Read the words, not the share. For each one ask the only question that matters: does\n"
        "  it name an INDUSTRY, or an activity every business does? Then check by reading the\n"
        "  companies it decided — `python scripts/run_combined_discovery.py csv` has them, and the\n"
        "  wrong ones are obvious ('Inspektion' -> Drohnen was two car dealerships and a repair\n"
        "  shop). A word that fails goes out of branch_taxonomy.yaml with the count in a comment,\n"
        "  the same way 'sicherheit' did. NOT into a stop-list in the code."
    )

    print("\n-- the 15 most common Technologiefelder --")
    for branch, count in sorted(branch_counts(assigned).items(), key=lambda kv: -kv[1])[:15]:
        print(f"{branch[:33]:<34}{count:>7}  {_pct(count, n)}")
    print(
        "\n  Defence Tech must not be near the top. If it is, a keyword is matching inside a German\n"
        "  compound again — 'Sicherheit' hides in Arbeitssicherheit — and that is the bug this\n"
        "  whole module is shaped around."
    )

    print("\n-- the 20 keywords that fired on the most companies --")
    fired: dict[str, int] = {}
    for row in rows:
        for keyword in row.get("keywords") or {}:
            fired[keyword] = fired.get(keyword, 0) + 1
    for keyword, count in sorted(fired.items(), key=lambda kv: -kv[1])[:20]:
        print(f"{keyword[:33]:<34}{count:>7}  {_pct(count, n)}")
    print(
        "\n  A keyword high on this list is worth almost nothing by construction: its weight falls\n"
        "  with the number of companies using it. That is measured, not listed by hand — do not\n"
        "  delete these from the taxonomy, and do not add a stop-list."
    )

    review = sum(1 for r in rows if r.get("needs_review"))
    print(f"\n{'flagged for a human to look at':<34}{review:>7}")

    # -----------------------------------------------------------------------------------------
    print(f"\n\n########## COLUMN 2: WZ cluster ##########\n")
    # -----------------------------------------------------------------------------------------
    # A lookup in the official classification, not a decision. Nothing here was judged by anybody,
    # so there is nothing to label and nothing to be right or wrong about — only present or absent.
    coded = [r for r in rows if r.get("wz_division")]
    print(f"{'companies with a WZ code':<34}{len(coded):>7}  {_pct(len(coded), n)}")
    print(f"{'…with no code at all':<34}{n - len(coded):>7}  {_pct(n - len(coded), n)}")
    divisions = {r["wz_division"] for r in coded}
    sections = {r.get("wz_section") for r in coded if r.get("wz_section")}
    print(f"{'distinct divisions present':<34}{len(divisions):>7}")
    print(f"{'distinct sections present':<34}{len(sections):>7}")
    untitled = sum(1 for r in coded if not r.get("wz_division_title"))
    if untitled:
        print(f"{'!! divisions with no title':<34}{untitled:>7}  wz_titles.yaml is older than the data")

    print("\n-- the 15 biggest divisions (Abteilungen) --")
    per_division: dict[tuple[str, str], int] = {}
    for row in coded:
        key = (row["wz_division"], row.get("wz_division_title") or "?")
        per_division[key] = per_division.get(key, 0) + 1
    for (code, title), count in sorted(per_division.items(), key=lambda kv: -kv[1])[:15]:
        print(f"  {code:<4}{title[:44]:<46}{count:>6}  {_pct(count, n)}")

    print("\n-- rolled up to sections (Abschnitte) --")
    per_section: dict[tuple[str, str], int] = {}
    for row in coded:
        key = (row.get("wz_section") or "?", (row.get("wz_section_title") or "?")[:44])
        per_section[key] = per_section.get(key, 0) + 1
    for (letter, title), count in sorted(per_section.items(), key=lambda kv: -kv[1]):
        # Printed as the classification writes them, in capitals. `.title()` mangles "IT-Beratung".
        print(f"  {letter:<4}{title[:44]:<46}{count:>6}  {_pct(count, n)}")

    # -----------------------------------------------------------------------------------------
    print(f"\n\n########## BOTH COLUMNS ##########\n")
    # -----------------------------------------------------------------------------------------
    both = sum(1 for r in rows if r.get("outcome") == "assigned" and r.get("wz_division"))
    field_only = sum(1 for r in rows if r.get("outcome") == "assigned" and not r.get("wz_division"))
    wz_only = sum(1 for r in rows if r.get("outcome") != "assigned" and r.get("wz_division"))
    neither = n - both - field_only - wz_only
    print(f"{'a Technologiefeld and a WZ cluster':<40}{both:>7}  {_pct(both, n)}")
    print(f"{'a Technologiefeld only':<40}{field_only:>7}  {_pct(field_only, n)}")
    print(f"{'a WZ cluster only':<40}{wz_only:>7}  {_pct(wz_only, n)}")
    print(f"{'neither':<40}{neither:>7}  {_pct(neither, n)}")
    print(
        "\n  This is coverage, NOT agreement. The two columns answer different questions, so they\n"
        "  cannot agree or disagree and nothing here should ever be reported as either. A company\n"
        "  registered under '68 Grundstücks- und Wohnungswesen' with the Technologiefeld NONE is\n"
        "  fully and correctly described by both columns."
    )
    print()


BRANCH_LABEL_COLUMNS = [
    "bucket", "company_id", "company_name", "website",
    "technologiefeld",
    # Empty except on `no_website` rows, where it is the field the register text claimed and no
    # website confirmed. `technologiefeld` stays NONE for those — this column is what you are being
    # asked to judge in that bucket, and without it the row says nothing.
    "withheld_branch",
    "score", "decided_by", "runner_up", "keyword_count", "keywords", "shortlist",
    # The company's own sentence around the deciding keyword — read THIS before opening a website.
    # For a `no_keyword` row it is the start of the text we read, so "nothing matched" can be checked
    # rather than believed.
    "evidence",
    "llm_reason",
    # The WZ cluster, as CONTEXT only. Do not label it — nobody guessed it, it is a lookup in the
    # official classification. It is here because knowing a company is registered under
    # "56 Gastronomie" helps a human judge whether "Gastgewerbe" is the right technology field.
    "wz_code", "wz_division_title", "wz_section_title",
    # Which run this row came out of. A label is an answer about a specific classification, and a
    # keyword's weight depends on how many companies use it — so the same company can carry a
    # different Technologiefeld in the next run, and a `yes` written about "Handel" says nothing
    # about a row that now reads "Logistik". Without this column that substitution is invisible.
    "taxonomy_version",
    # The two a human fills in. Everything to the left is context for the decision.
    #   label   yes | no | unclear — is `branch` the right industry for this company?
    #   notes   anything worth remembering. Never parsed, only read.
    "label", "notes",
    # Set by `classify-labels`, CLEARED by a human: `yes` when the row this label was written about
    # has since changed — a different Technologiefeld, or a different bucket, which means a
    # different QUESTION. The label is kept, because the human's work is not thrown away, and
    # `classify-labels-report` leaves it out of every number and says how many there are. The
    # `notes` column says what was originally judged, so re-labelling is a re-read, not a re-do.
    # Blanking this cell is the only thing that clears it, and it means "I have looked again".
    "stale",
]

BRANCH_LABEL_VALUES = ("yes", "no", "unclear")


def branch_label_buckets(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Six groups, because there are six different questions and one sample cannot answer them all.

    keyword_decided   the words said it. PRECISION of the whole column, and the main number here.
    model_decided     the model broke a tie. Was breaking it that way right?
    no_website        the register text named a field and no website was found, so it was withheld.
                      **Was the withheld field right?** This bucket is the price of the website gate.
                      Mostly `yes` here means the gate is throwing away good answers.
    torn              two branches too close to call. **Label these before paying for the model** —
                      they are exactly the rows it would be asked about, and if a human cannot
                      separate them from the same text, a 4B model will not either.
    no_keyword        we read text and no keyword appeared. Does this company have a branch anyway?
    no_text           we had nothing to read. THE CEILING: what better input would be buying.

    `no_website`, `torn`, `no_keyword` and `no_text` get their own buckets rather than one. Sampled
    together, 46% of the draw comes from `no_text`, which is the least informative of the four.
    """
    buckets: dict[str, list[dict[str, Any]]] = {
        "keyword_decided": [], "model_decided": [],
        "no_website": [], "torn": [], "no_keyword": [], "no_text": [],
    }
    for row in rows:
        if row.get("outcome") == "assigned":
            key = {"keywords": "keyword_decided", "model": "model_decided"}.get(
                row.get("decided_by") or "", "keyword_decided"
            )
            buckets[key].append(row)
        elif row.get("outcome") in buckets:
            buckets[row["outcome"]].append(row)
    return buckets


def label_staleness(
    was: Mapping[str, str], bucket: str, technologiefeld: str, field_key: str = "technologiefeld"
) -> tuple[bool, bool]:
    """Is a carried-over label still an answer to the question it was written about?

    `field_key` names the worksheet column holding the answer being judged, because there are now two
    worksheets and they do not share a vocabulary: column 1 writes `technologiefeld`, column 3 writes
    `purpose_field`. Defaulted, so column 1's caller and `check_branch_classifier.py` are unchanged.
    Getting this wrong is not a small bug — reading a key the worksheet does not have compares `""`
    against every real field name, so **every label goes stale on the second run** and quietly leaves
    the precision count. That is exactly what happened the first time column 3's worksheet ran.

    Returns `(moved, still_stale)`. `moved` means the row changed since it was judged and this is the
    first run to notice. `still_stale` means it was already flagged and nobody has cleared the flag.

    Pure, and separate from the file handling, because both halves of it were wrong once:

    1. A label used to be keyed on `(bucket, company_id)`, so a company moving from `no_keyword` to
       `keyword_decided` silently lost its label. It is keyed on the company now, and a move is
       reported instead of erasing the work.
    2. The flag used to be recomputed from scratch each run. Writing the row back makes `was` match
       the current state, so the second run said "unchanged" and the label rejoined the precision
       count with nobody having re-read it. **Stale is sticky.** Only a human blanking the cell
       clears it, and that is what "I have looked again" means.

    A changed bucket counts as much as a changed field: `keyword_decided` asks "is this field
    right", `no_text` asks "does any field fit at all". Those are different questions, and an answer
    to one is not an answer to the other.
    """
    labelled = bool((was.get("label") or "").strip())
    if not labelled:
        return False, False
    moved = (
        (was.get("bucket") or "") != bucket
        or (was.get(field_key) or "") != technologiefeld
    )
    was_flagged = (was.get("stale") or "").strip().lower() == "yes"
    return moved, was_flagged and not moved


def _stale_note(was: dict[str, str], field_key: str = "technologiefeld") -> str:
    """Say in the `notes` column what this label was originally about.

    Without it, `stale=yes` tells a human their answer no longer applies and not what it applied to,
    which makes re-labelling a re-do instead of a re-read. Their own note is kept in front.

    `field_key` names the answer column, for the same reason `label_staleness` takes one: two
    worksheets, two vocabularies. Read the wrong key and the trail says `keyword_decided/?` — it
    still flags the row, but it no longer says what the label was about, which is the entire job.
    """
    old_bucket = was.get("bucket") or "?"
    old_field = was.get(field_key) or "?"
    old_version = was.get("taxonomy_version") or "?"
    trail = (
        f"[labelled {was.get('label', '').strip()} when this was {old_bucket}/{old_field} "
        f"in run {old_version}]"
    )
    note = (was.get("notes") or "").strip()
    # Do not stack a trail on a trail: re-running this twice must not grow the cell.
    #
    # Split on the BRACKET, not on " [labelled " — with the leading space it only matched when a
    # human had written a note in front, so a row with no note of its own grew a second trail on
    # every run. Measured on column 3's first run: two identical trails in one cell.
    note = note.split("[labelled ")[0].strip()
    return f"{note} {trail}".strip()


def branch_labels(per_bucket: int = 10) -> None:
    """Write (or extend) the hand-labelling worksheet. Never overwrites a label already written.

    A label is keyed on the **company** and nothing else. It used to be keyed on the company *and
    its bucket*, which quietly threw work away: re-classify, a company moves from `no_keyword` to
    `keyword_decided`, and the row is rebuilt as if it had never been judged. Hand labels are the
    most expensive data in this project — 40 of them are the only thing that can say whether the
    Technologiefeld is right — so they are kept and, when what they describe has changed, marked
    `stale` rather than dropped or silently counted.

    The draw is `per_bucket` rows per bucket, ordered by company id. Company ids are UUIDs, so that
    ordering is arbitrary but **stable**: the same rows come up every time, and re-running this
    never swaps a labelled company for a fresh one.
    """
    rows = read_jsonl(BRANCHES_PATH)
    if not rows:
        raise SystemExit(f"nothing in {BRANCHES_PATH}; run `classify` first")
    buckets = branch_label_buckets(rows)
    version = next((r.get("taxonomy_version") for r in rows if r.get("taxonomy_version")), "")

    existing: dict[str, dict[str, str]] = {}
    if BRANCH_LABELS_PATH.exists():
        for old in read_label_csv(BRANCH_LABELS_PATH):
            existing[old.get("company_id") or ""] = old
        done = sum(1 for v in existing.values() if (v.get("label") or "").strip())
        log.info("%s has %d rows, %d labelled", BRANCH_LABELS_PATH.name, len(existing), done)

    out: list[dict[str, Any]] = []
    carried = stale_count = stale_kept = 0
    for bucket, entries in buckets.items():
        entries.sort(key=lambda r: r.get("company_id") or "")
        keep = entries[:per_bucket] + [
            e for e in entries[per_bucket:]
            if (existing.get(e.get("company_id") or "", {}).get("label") or "").strip()
        ]
        for row in keep:
            was = existing.get(row.get("company_id") or "", {})
            field_now = row.get("technologiefeld") or row.get("branch") or ""
            # Does this label still answer the question it was written about? See
            # `label_staleness` — the rule lives there so the guard can call it.
            moved, still = label_staleness(was, bucket, field_now)
            carried += bool((was.get("label") or "").strip())
            stale_count += moved
            stale_kept += still
            out.append({
                "bucket": bucket,
                "company_id": row.get("company_id"),
                "company_name": row.get("company_name"),
                "website": row.get("website") or "",
                "wz_code": row.get("wz_code") or "",
                "wz_division_title": row.get("wz_division_title") or "",
                "wz_section_title": row.get("wz_section_title") or "",
                "technologiefeld": field_now,
                "withheld_branch": row.get("withheld_branch") or "",
                "score": row.get("score") or "",
                "decided_by": row.get("decided_by") or row.get("outcome"),
                "runner_up": row.get("runner_up") or "",
                # How many keywords carried this row. 807 of the first run's 992 assignments
                # rested on one, so the two groups have to be labelled apart.
                "keyword_count": len(row.get("keywords") or {}),
                "keywords": ";".join(sorted(row.get("keywords") or {})),
                "shortlist": ";".join(row.get("shortlist") or []),
                "evidence": row.get("evidence") or "",
                "llm_reason": row.get("llm_reason") or "",
                "taxonomy_version": row.get("taxonomy_version") or version,
                # Carried over verbatim. This is the human's work.
                "label": was.get("label", ""),
                # The trail is written ONCE, when the row moves. Rewriting it on a later run would
                # describe the row's current state as its history, which is worse than no trail.
                "notes": _stale_note(was) if moved else was.get("notes", ""),
                "stale": "yes" if (moved or still) else "",
            })
        log.info("%-16s %d rows (%d available)", bucket, len(keep), len(entries))

    BRANCH_LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with BRANCH_LABELS_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=BRANCH_LABEL_COLUMNS, extrasaction="ignore",
                                delimiter=LABEL_CSV_DELIMITER)
        writer.writeheader()
        writer.writerows(out)

    print(f"\nWrote {len(out)} rows to {BRANCH_LABELS_PATH}")
    if carried:
        print(f"Carried {carried} label(s) over from the previous worksheet.")
    if stale_count:
        print(
            f"!! {stale_count} newly marked `stale`: the row changed since it was judged — a\n"
            "   different Technologiefeld, or a different bucket, which is a different question.\n"
            "   `notes` says what each one was."
        )
    if stale_kept:
        print(f"   {stale_kept} more were already stale and still are.")
    if stale_count or stale_kept:
        print(
            "   Stale labels are LEFT OUT of every number in classify-labels-report. Re-read the\n"
            "   row, fix `label` if it changed, then CLEAR the `stale` cell — that is how you say\n"
            "   you have looked again. Nothing else clears it."
        )
    print(f"Fill in `label` with one of: {', '.join(BRANCH_LABEL_VALUES)}")
    print("  keyword/model_decided    -> is `technologiefeld` right for this company?")
    print("  no_website               -> `withheld_branch` is what the company's REGISTER TEXT")
    print("                              claims and no website confirmed. Is that claim right?")
    print("                              `yes` here means the website rule cost a good answer.")
    print("  torn                     -> can YOU pick one of `shortlist` from the same text? If")
    print("                              not, no model can. Write which one in `notes`.")
    print("  no_keyword / no_text     -> look the company up. `yes` if some branch on the list")
    print("                              fits it, `no` if genuinely none does. Name it in `notes`.")
    print(f"\nThen: python {Path(__file__).name} classify-labels-report")


def branch_labels_report() -> None:
    """Read the worksheet back. The only thing here that says whether the answer was RIGHT."""
    if not BRANCH_LABELS_PATH.exists():
        raise SystemExit(f"no {BRANCH_LABELS_PATH}; run `classify-labels` first")
    rows = read_label_csv(BRANCH_LABELS_PATH)

    def tally(bucket: str) -> dict[str, int]:
        counts = {v: 0 for v in BRANCH_LABEL_VALUES}
        counts["unlabelled"] = 0
        counts["stale"] = 0
        for row in rows:
            if (row.get("bucket") or "") != bucket:
                continue
            value = (row.get("label") or "").strip().lower()
            # A stale label describes a row that has since changed — a different field, or a
            # different bucket, which is a different question. Counting it as `yes` or `no` would
            # be reporting an answer to a question nobody asked. It is set aside, and said out loud.
            if (row.get("stale") or "").strip().lower() == "yes":
                counts["stale"] += 1
                continue
            counts[value if value in counts else "unlabelled"] += 1
        return counts

    print(f"\n=== {len(rows)} rows in {BRANCH_LABELS_PATH.name} ===")
    versions = {(r.get("taxonomy_version") or "?") for r in rows}
    stale_total = sum(1 for r in rows if (r.get("stale") or "").strip().lower() == "yes")
    print(f"  classified by run   {', '.join(sorted(versions))}")
    if stale_total:
        print(
            f"  {stale_total} label(s) are STALE — the row changed after it was judged, so they are\n"
            "  left out of every number below. Re-read them; `notes` says what each one was."
        )
    for bucket, question in (
        ("keyword_decided", "the words on the page decided it"),
        ("model_decided", "the model broke a tie"),
    ):
        counts = tally(bucket)
        decided = counts["yes"] + counts["no"]
        print(f"\n-- {question} --")
        print(f"{'right':<38}{counts['yes']:>5}")
        print(f"{'wrong':<38}{counts['no']:>5}")
        print(f"{'unclear':<38}{counts['unclear']:>5}")
        print(f"{'not yet labelled':<38}{counts['unlabelled']:>5}")
        if decided:
            print(f"\n  PRECISION {counts['yes']}/{decided} = {100.0 * counts['yes'] / decided:.0f}%")
        else:
            print("\n  PRECISION unknown — nothing in this group is labelled yet")

    # The price of the website gate, in correctness rather than in count. `yes` here means the
    # register text was right and the rule threw a good answer away.
    gate = tally("no_website")
    decided = gate["yes"] + gate["no"]
    print("\n-- the website rule: was the withheld field right anyway? --")
    print(f"{'yes, the register text was right':<38}{gate['yes']:>5}")
    print(f"{'no, it was wrong':<38}{gate['no']:>5}")
    print(f"{'unclear':<38}{gate['unclear']:>5}")
    print(f"{'not yet labelled':<38}{gate['unlabelled']:>5}")
    if decided:
        print(f"\n  WITHHELD-BUT-RIGHT {gate['yes']}/{decided} = "
              f"{100.0 * gate['yes'] / decided:.0f}%")
        print("  Scale that across the 563 withheld rows before defending the rule. High means the")
        print("  register purpose is good evidence and requiring a website is costing real answers;")
        print("  low means the boilerplate argument holds and the rule is doing its job.")
    else:
        print("\n  UNKNOWN — nothing in this group is labelled, so the website rule is unmeasured.")

    torn = tally("torn")
    decided = torn["yes"] + torn["no"]
    print("\n-- the ties: could a human separate them from the same text? --")
    print(f"{'yes, one of the shortlist fits':<38}{torn['yes']:>5}")
    print(f"{'no, none of the shortlist fits':<38}{torn['no']:>5}")
    print(f"{'unclear':<38}{torn['unclear']:>5}")
    print(f"{'not yet labelled':<38}{torn['unlabelled']:>5}")
    if decided:
        print(f"\n  A human separated {torn['yes']}/{decided}. That is the ceiling for the tie-break")
        print("  model: it is asked the same question from the same text, with less to work with.")
        print("  If a person cannot do it, 52 minutes of model time will not do it either.")

    # The two kinds of blank, added together on purpose here and NOWHERE else: the question in this
    # section is "does a branch exist for this company", and for that one question it does not
    # matter whether we had no keyword or no text at all.
    no_keyword, no_text = tally("no_keyword"), tally("no_text")
    counts = {k: no_keyword[k] + no_text[k] for k in list(BRANCH_LABEL_VALUES) + ["unlabelled"]}
    decided = counts["yes"] + counts["no"]
    print("\n-- the ceiling: do the companies we gave no branch actually have one? --")
    print(f"{'a branch on the list fits (yes)':<38}{counts['yes']:>5}")
    print(f"{'no branch on the list fits (no)':<38}{counts['no']:>5}")
    print(f"{'unclear':<38}{counts['unclear']:>5}")
    print(f"{'not yet labelled':<38}{counts['unlabelled']:>5}")
    if decided:
        print(f"\n  MISSED {counts['yes']}/{decided} = {100.0 * counts['yes'] / decided:.0f}%")
        print("  of the blanks do have a branch. That is what better text or a model would buy.")
        print("  The rest are the Berlin/Osnabrück mismatch, and no classifier can fix those.")
    else:
        print("\n  CEILING unknown — this group decides whether anything more is worth building.")
    print()


# =====================================================================================
# COLUMN 3's hand-labelling worksheet
#
# The same machinery as column 1's above, on column 3's own file and vocabulary — `label_staleness`,
# `_stale_note` and `read_label_csv` are called, never copied, because each of them already records a
# bug it was written to fix and a second copy would have to re-earn all of them.
#
# Why column 3 needs its own: it has assigned a Berlin field to 1,163 companies and **not one has
# ever been checked**. Two of the numbers below decide whether the 553 Tier B calls (~10 s each) can
# be replaced by something cheaper — `model_77` precision, and whether `none_apply` is right, since a
# similarity ranking can never say "no field fits".
# =====================================================================================

#: Per-bucket draw. Weighted, not flat: `model_77` and `none_apply` are the two buckets the Tier B
#: decision rests on, and `no_keyword`/`no_purpose` hold 12,498 rows between them — sampled evenly,
#: most of the worksheet would be the two least informative groups.
PURPOSE_LABEL_DRAW = {
    "model_77": 25,
    "none_apply": 25,
    "keyword_decided": 15,
    "no_keyword": 15,
    "torn": 10,
    "model_3way": 10,
    "no_purpose": 10,
}

PURPOSE_LABEL_COLUMNS = [
    "bucket", "company_id", "company_name",
    "purpose_field",
    # The three candidates, flat rather than a JSON blob: the point of this file is a human sorting
    # and filtering in Excel, and a nested value cannot be filtered on.
    #
    # `cand_basis` is what stops the score column lying. `keyword_weight` means the number was
    # measured over the corpus; `model_rank` means a model put the fields in order and there is NO
    # number — the cell is empty, not zero. A zero would read as "scored nothing".
    "cand1_field", "cand1_score", "cand2_field", "cand2_score", "cand3_field", "cand3_score",
    "cand_basis",
    "outcome", "decided_by", "score", "keywords", "shortlist",
    # Empty except on `no_purpose` rows, where it is the field the register text WOULD have produced
    # had the 40-character floor not excluded it. What you are being asked to judge in that bucket.
    "withheld_by_floor",
    # The two that make labelling possible at all: the register's own words, and the model's stated
    # reason for what it did with them. Read them side by side before looking a company up.
    "purpose_text", "purpose_chars", "llm_reason", "llm_refused", "model_name",
    "taxonomy_version",
    "label", "notes", "stale",
]


def purpose_label_buckets(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Seven groups, because column 3 has seven outcomes and each asks a different question.

    keyword_decided  the register's words said it. PRECISION of the free path.
    model_3way       Tier A broke a keyword tie from 3 candidates. Was breaking it that way right?
    model_77         Tier B chose from all 77 where no keyword fired. **The expensive one** — 553
                     rows at ~10 s. Its precision decides whether a cheaper generator could replace it.
    torn             two fields too close to call. Label these before paying for Tier A: if a human
                     cannot separate them from the same text, a 4B model will not either.
    none_apply       a model read the purpose and said NO field fits. **Was it right?** This is the
                     one capability a similarity ranking cannot reproduce, so it is what a cheaper
                     approach would be giving up.
    no_keyword       we read a usable purpose and not one taxonomy word appeared. Does a field fit
                     anyway? These 5,196 rows are Tier B's target population.
    no_purpose       the register said nothing readable, or the 40-char floor excluded it. THE
                     CEILING: what better input would buy.

    `unsure` has no rows today but is a real outcome — it is folded in with `none_apply`, since both
    are a model declining, and the `outcome` column keeps them tellable apart. Silently dropping it
    would break the guarantee that the buckets cover all 14,443 rows.
    """
    from combined_discovery import purpose_branch as pb

    buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in PURPOSE_LABEL_DRAW}
    by_outcome = {
        pb.ASSIGNED_KEYWORDS: "keyword_decided",
        pb.ASSIGNED_MODEL_3WAY: "model_3way",
        pb.ASSIGNED_MODEL_77: "model_77",
        pb.TORN: "torn",
        pb.NONE_APPLY: "none_apply",
        pb.UNSURE: "none_apply",
        pb.NO_KEYWORD: "no_keyword",
        pb.NO_PURPOSE: "no_purpose",
    }
    for row in rows:
        bucket = by_outcome.get(row.get("outcome") or "")
        if bucket:
            buckets[bucket].append(row)
    return buckets


def _purpose_candidate_cells(row: dict[str, Any]) -> dict[str, Any]:
    """Flatten `candidates` into the six worksheet cells plus `cand_basis`.

    A missing rank leaves both its cells empty rather than writing a placeholder: a row where the
    model named one field and we do not know its second choice must not look like a row where the
    second choice was blank.
    """
    cells: dict[str, Any] = {}
    candidates = row.get("candidates") or []
    for rank in (1, 2, 3):
        entry = next((c for c in candidates if c.get("rank") == rank), None)
        cells[f"cand{rank}_field"] = (entry or {}).get("field") or ""
        score = (entry or {}).get("score")
        # `None` becomes empty, never 0. See the note on `cand_basis` in PURPOSE_LABEL_COLUMNS.
        cells[f"cand{rank}_score"] = "" if score is None else score
    cells["cand_basis"] = (candidates[0].get("basis") if candidates else "") or ""
    return cells


def purpose_labels(per_bucket: int | None = None) -> None:
    """Write (or extend) column 3's worksheet. Never overwrites a label a human has written.

    Keyed on the **company** alone, and stale-marked rather than dropped when what a label described
    has changed — both rules and both past bugs are `label_staleness`'s, which this calls.

    `per_bucket` flattens the weighted draw to one number for anyone who wants the simple version.
    The default is `PURPOSE_LABEL_DRAW`, which is deliberately uneven.
    """
    rows = read_jsonl(PURPOSE_BRANCHES_PATH)
    if not rows:
        raise SystemExit(f"nothing in {PURPOSE_BRANCHES_PATH}; run `classify-purpose` first")
    buckets = purpose_label_buckets(rows)
    version = next((r.get("taxonomy_version") for r in rows if r.get("taxonomy_version")), "")

    existing: dict[str, dict[str, str]] = {}
    if PURPOSE_LABELS_PATH.exists():
        for old in read_label_csv(PURPOSE_LABELS_PATH):
            existing[old.get("company_id") or ""] = old
        done = sum(1 for v in existing.values() if (v.get("label") or "").strip())
        log.info("%s has %d rows, %d labelled", PURPOSE_LABELS_PATH.name, len(existing), done)

    out: list[dict[str, Any]] = []
    carried = stale_count = stale_kept = 0
    for bucket, entries in buckets.items():
        # Ordered by company id: arbitrary but STABLE, so the same rows come up every run and
        # re-running never swaps a labelled company for a fresh one.
        entries.sort(key=lambda r: r.get("company_id") or "")
        want = per_bucket if per_bucket is not None else PURPOSE_LABEL_DRAW[bucket]
        keep = entries[:want] + [
            e for e in entries[want:]
            if (existing.get(e.get("company_id") or "", {}).get("label") or "").strip()
        ]
        for row in keep:
            was = existing.get(row.get("company_id") or "", {})
            field_now = row.get("purpose_field") or ""
            # `field_key="purpose_field"` is not optional. `label_staleness` defaults to column 1's
            # `technologiefeld`, which this worksheet does not have — reading it compares `""`
            # against every real field name, so every label goes stale on the second run and leaves
            # the precision count. That is what the first run of this command did.
            moved, still = label_staleness(was, bucket, field_now, field_key="purpose_field")
            carried += bool((was.get("label") or "").strip())
            stale_count += moved
            stale_kept += still
            entry = {
                "bucket": bucket,
                "company_id": row.get("company_id"),
                "company_name": row.get("company_name"),
                "purpose_field": field_now,
                "outcome": row.get("outcome"),
                "decided_by": row.get("decided_by") or "",
                "score": row.get("score") or "",
                "keywords": ";".join(sorted(row.get("keywords") or {})),
                "shortlist": ";".join(row.get("shortlist") or []),
                "withheld_by_floor": row.get("withheld_by_floor") or "",
                "purpose_text": row.get("purpose_text") or "",
                "purpose_chars": row.get("purpose_chars") or "",
                "llm_reason": row.get("llm_reason") or "",
                "llm_refused": row.get("llm_refused") or "",
                "model_name": row.get("model_name") or "",
                "taxonomy_version": row.get("taxonomy_version") or version,
                # Carried over verbatim. This is the human's work.
                "label": was.get("label", ""),
                # Written ONCE, when the row moves. `label_staleness` keeps the flag sticky, and
                # `_stale_note` never stacks a trail on a trail.
                "notes": (
                    _stale_note(was, field_key="purpose_field") if moved
                    else was.get("notes", "")
                ),
                "stale": "yes" if (moved or still) else "",
            }
            entry.update(_purpose_candidate_cells(row))
            out.append(entry)
        log.info("%-16s %d rows (%d available)", bucket, len(keep), len(entries))

    PURPOSE_LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Two separate things Excel needs, and both are needed: `utf-8-sig` for the BOM, or `Osnabrück`
    # renders as `OsnabrÃ¼ck`; and the semicolon delimiter, or all 25 columns land in column A.
    with PURPOSE_LABELS_PATH.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PURPOSE_LABEL_COLUMNS, extrasaction="ignore",
                                delimiter=LABEL_CSV_DELIMITER)
        writer.writeheader()
        writer.writerows(out)

    print(f"\nWrote {len(out)} rows to {PURPOSE_LABELS_PATH}")
    if carried:
        print(f"Carried {carried} label(s) over from the previous worksheet.")
    if stale_count:
        print(f"   {stale_count} label(s) went STALE — the row changed after it was judged.")
    if stale_kept:
        print(f"   {stale_kept} more were already stale and still are.")
    if stale_count or stale_kept:
        print(
            "   Stale labels are LEFT OUT of every number in classify-purpose-labels-report.\n"
            "   Re-read the row, fix `label`, then CLEAR the `stale` cell — that is how you say you\n"
            "   have looked again. Nothing else clears it."
        )
    print(f"Fill in `label` with one of: {', '.join(BRANCH_LABEL_VALUES)}")
    print("  keyword_decided          -> is `purpose_field` right, from `purpose_text` alone?")
    print("  model_3way / model_77    -> same question. `llm_reason` says why it chose;")
    print("                              `cand2_field`/`cand3_field` are what it ranked below.")
    print("  none_apply               -> the model said NO field fits. `yes` if that is right.")
    print("                              This is the one thing a similarity ranking cannot do.")
    print("  torn                     -> can YOU pick one of `shortlist` from the same text? If")
    print("                              not, no model can. Write which one in `notes`.")
    print("  no_keyword / no_purpose  -> does any Berlin field fit this company? Name it in `notes`.")
    print("\n  When you mark a row `no` but one of `cand2_field`/`cand3_field` IS right, write that")
    print("  field name in `notes` — the report counts those separately, and that number decides")
    print("  whether a cheap candidate generator could replace the model.")
    print(f"\nThen: python {Path(__file__).name} classify-purpose-labels-report")


def purpose_labels_report() -> None:
    """Read column 3's worksheet back. The only thing that says whether the answer was RIGHT."""
    if not PURPOSE_LABELS_PATH.exists():
        raise SystemExit(f"no {PURPOSE_LABELS_PATH}; run `classify-purpose-labels` first")
    rows = read_label_csv(PURPOSE_LABELS_PATH)

    def tally(*buckets: str) -> dict[str, int]:
        counts = {v: 0 for v in BRANCH_LABEL_VALUES}
        counts["unlabelled"] = 0
        counts["stale"] = 0
        for row in rows:
            if (row.get("bucket") or "") not in buckets:
                continue
            # A stale label describes a row that has since changed, so counting it as `yes` or `no`
            # would report an answer to a question nobody asked. Set aside, and said out loud.
            if (row.get("stale") or "").strip().lower() == "yes":
                counts["stale"] += 1
                continue
            value = (row.get("label") or "").strip().lower()
            counts[value if value in counts else "unlabelled"] += 1
        return counts

    def recovered(*buckets: str) -> tuple[int, int]:
        """Of the rows marked wrong, how many name a field that WAS at rank 2 or 3.

        The number the Tier B decision turns on. High means the ordering is nearly right and a cheap
        generator producing a shortlist would be enough; low means the candidate set itself is wrong
        and no amount of re-ranking helps.

        Matched case-insensitively against `notes`, because a human writes the field name in prose.
        """
        wrong = hit = 0
        for row in rows:
            if (row.get("bucket") or "") not in buckets:
                continue
            if (row.get("stale") or "").strip().lower() == "yes":
                continue
            if (row.get("label") or "").strip().lower() != "no":
                continue
            wrong += 1
            note = (row.get("notes") or "").casefold()
            lower = [(row.get("cand2_field") or "").casefold(),
                     (row.get("cand3_field") or "").casefold()]
            if note and any(field_name and field_name in note for field_name in lower):
                hit += 1
        return hit, wrong

    print(f"\n=== {len(rows)} rows in {PURPOSE_LABELS_PATH.name} ===")
    versions = {(r.get("taxonomy_version") or "?") for r in rows}
    stale_total = sum(1 for r in rows if (r.get("stale") or "").strip().lower() == "yes")
    print(f"  classified by run   {', '.join(sorted(versions))}")
    if stale_total:
        print(
            f"  {stale_total} label(s) are STALE — the row changed after it was judged, so they are\n"
            "  left out of every number below. Re-read them; `notes` says what each one was."
        )

    for bucket, question in (
        ("keyword_decided", "the register's own words decided it"),
        ("model_3way", "Tier A broke a keyword tie, from 3 candidates"),
        ("model_77", "Tier B chose from all 77 — the expensive call"),
    ):
        counts = tally(bucket)
        decided = counts["yes"] + counts["no"]
        print(f"\n-- {question} --")
        print(f"{'right':<38}{counts['yes']:>5}")
        print(f"{'wrong':<38}{counts['no']:>5}")
        print(f"{'unclear':<38}{counts['unclear']:>5}")
        print(f"{'not yet labelled':<38}{counts['unlabelled']:>5}")
        if decided:
            print(f"\n  PRECISION {counts['yes']}/{decided} = "
                  f"{100.0 * counts['yes'] / decided:.0f}%  (at rank 1)")
            hit, wrong = recovered(bucket)
            if wrong:
                print(f"  RANK-2/3 RECOVERY {hit}/{wrong} of the wrong ones named a field that was")
                print("  already at rank 2 or 3. High -> a cheap generator producing a shortlist")
                print("  would be enough. Low -> the candidate set itself is wrong.")
            else:
                print("  RANK-2/3 RECOVERY unknown — nothing in this group is labelled `no` yet.")
        else:
            print("\n  PRECISION unknown — nothing in this group is labelled yet")

    # The decline. Kept apart from the assignments because it is a different claim: not "is this
    # field right" but "is NO field right", which is the one answer a ranking cannot produce.
    decline = tally("none_apply")
    decided = decline["yes"] + decline["no"]
    print("\n-- the model declined: was 'no Berlin field fits' right? --")
    print(f"{'yes, no field fits':<38}{decline['yes']:>5}")
    print(f"{'no, a field does fit':<38}{decline['no']:>5}")
    print(f"{'unclear':<38}{decline['unclear']:>5}")
    print(f"{'not yet labelled':<38}{decline['unlabelled']:>5}")
    if decided:
        print(f"\n  DECLINE ACCURACY {decline['yes']}/{decided} = "
              f"{100.0 * decline['yes'] / decided:.0f}%")
        print("  Scale that across the 735 `none_apply` rows. A similarity ranking always returns a")
        print("  nearest field and can NEVER answer this, so a high number is the cost of replacing")
        print("  the model with one — and 1,272 of 6,298 active companies are holding shells.")
    else:
        print("\n  UNKNOWN — so the one thing a cheaper approach would give up is unmeasured.")

    torn = tally("torn")
    decided = torn["yes"] + torn["no"]
    print("\n-- the ties: could a human separate them from the same text? --")
    print(f"{'yes, one of the shortlist fits':<38}{torn['yes']:>5}")
    print(f"{'no, none of the shortlist fits':<38}{torn['no']:>5}")
    print(f"{'unclear':<38}{torn['unclear']:>5}")
    print(f"{'not yet labelled':<38}{torn['unlabelled']:>5}")
    if decided:
        print(f"\n  A human separated {torn['yes']}/{decided}. That is the ceiling for Tier A: it is")
        print("  asked the same question from the same text, with less to work with.")

    # The two kinds of blank, added together HERE and nowhere else: the question in this section is
    # "does a field exist for this company at all", and for that one question it does not matter
    # whether no keyword fired or there was no readable purpose.
    blanks = tally("no_keyword", "no_purpose")
    decided = blanks["yes"] + blanks["no"]
    print("\n-- the ceiling: do the companies we gave no field actually have one? --")
    print(f"{'a Berlin field fits (yes)':<38}{blanks['yes']:>5}")
    print(f"{'none fits (no)':<38}{blanks['no']:>5}")
    print(f"{'unclear':<38}{blanks['unclear']:>5}")
    print(f"{'not yet labelled':<38}{blanks['unlabelled']:>5}")
    if decided:
        print(f"\n  MISSED {blanks['yes']}/{decided} = {100.0 * blanks['yes'] / decided:.0f}% of the")
        print("  blanks do have a field. Over 5,196 `no_keyword` rows that is what better lexical")
        print("  coverage — compound splitting, lemmatisation — would be buying. Low means it would")
        print("  manufacture false positives instead.")
    else:
        print("\n  CEILING unknown — this group decides whether compound splitting is worth building.")
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
    p_run.add_argument(
        "--skip-coordinator", action="store_true",
        help=(
            "write the observations but do not resolve them. Nothing becomes a canonical website "
            "until pipeline_coordinator runs, so use this only when you intend to trigger it "
            "yourself -- in Airflow, for the dag_run record this direct call does not create."
        ),
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

    p_fetch = sub.add_parser(
        "fetch-content",
        help="download the /leistungen and /ueber-uns pages the cached homepages link to",
    )
    p_fetch.add_argument("--limit", type=int, default=None,
                         help="only the first N companies of the sample — use 100 first")
    p_fetch.add_argument("--workers", type=int, default=int(os.getenv("COMBINED_WORKERS", "8")))

    p_classify = sub.add_parser("classify", help="work out each company's branch")
    p_classify.add_argument("--limit", type=int, default=None)
    p_classify.add_argument(
        "--source", default="page,purpose",
        help=(
            "comma-separated: page, purpose. Default both. These are the texts the company itself "
            "published: `page` is its cached website, `purpose` the business purpose it registered. "
            "There is deliberately no `wz` source — a WZ code is a different classification and "
            "gets its own column, never the Technologiefeld."
        ),
    )
    p_classify.add_argument(
        "--llm", action="store_true",
        help=(
            "ask a local model to break the remaining ties, choosing from three shortlisted "
            "branches. About 10 seconds a page, so leave it off until the keyword numbers settle."
        ),
    )
    sub.add_parser("classify-report", help="print the branch scorecard")
    p_blabels = sub.add_parser(
        "classify-labels", help="write the branch labelling worksheet (merges with what is labelled)"
    )
    p_blabels.add_argument("--per-bucket", type=int, default=10,
                           help="how many companies per group (default 10, so about 40 rows)")
    sub.add_parser("classify-labels-report", help="read it back: is the branch actually right?")
    p_purpose = sub.add_parser(
        "classify-purpose",
        help="COLUMN 3: what the register says the company was set up to do. Free, no model.",
    )
    p_purpose.add_argument(
        "--limit", type=int, default=None,
        help="write the first N companies by id — a prefix of the full run. Keyword weights are "
             "always measured over the whole table, so a limit changes what is written, not what "
             "is counted.",
    )
    p_purpose.add_argument(
        "--tier", default="tiebreak",
        help="tiebreak (default) is Tier A: 3 candidates the keywords nominated, on `torn` rows. "
             "semantic is Tier B: all 77 fields on the `no_keyword` rows, where nothing anchors the "
             "answer to a word in the text. Comma-separate to run both.",
    )
    p_purpose.add_argument(
        "--company-id", action="append", default=None, metavar="ID",
        help="spend model calls on these companies only, jumping the queue. Accepts a unique id "
             "prefix and may be repeated. The queue is company_id order, which is NOT the handover "
             "CSV's order - so this is how you check a row you spotted in the spreadsheet without "
             "waiting. Every other row keeps its cached answer and the output file stays complete.",
    )
    p_purpose.add_argument(
        "--llm-limit", type=int, default=None,
        help="cap the NEW model calls this run. Cached answers do not count, so re-running the same "
             "command makes progress rather than repeating itself.",
    )
    p_purpose.add_argument(
        "--llm", action="store_true",
        help="turn the model on. Which question it is asked is set by --tier. Off by default, so "
             "the plain command stays free.",
    )
    sub.add_parser("classify-purpose-report", help="print the register-purpose scorecard")
    p_plabels = sub.add_parser(
        "classify-purpose-labels",
        help="COLUMN 3's hand-labelling worksheet (merges with what is already labelled)",
    )
    p_plabels.add_argument(
        "--per-bucket", type=int, default=None,
        help="flatten the weighted draw to N rows per bucket. The default is uneven on purpose — "
             "25 each for model_77 and none_apply, which are the two the Tier B decision rests on, "
             "and 10 for the 12,498 rows of blanks. About 110 rows in total.",
    )
    sub.add_parser(
        "classify-purpose-labels-report",
        help="read it back: is column 3 actually right? The only thing that measures precision.",
    )
    p_compare = sub.add_parser(
        "compare-columns",
        help="all three industry columns side by side, and where 1 and 3 disagree",
    )
    p_compare.add_argument(
        "--limit", type=int, default=None,
        help="first N rows of the comparison, disagreements first",
    )
    p_propose = sub.add_parser(
        "propose-keywords",
        help="ask the model what companies with NO keyword call themselves; writes a worksheet only",
    )
    p_propose.add_argument(
        "--limit", type=int, default=None,
        help="first N of the target companies, ordered by id — a prefix of the full run",
    )

    args = parser.parse_args()
    if args.command == "run":
        stages = {s.strip() for s in args.stages.split(",") if s.strip()}
        unknown = stages - set(ALL_STAGES)
        if unknown:
            raise SystemExit(f"unknown stage(s): {sorted(unknown)}")
        run(
            args.limit, stages, args.workers,
            skip_coordinator=args.skip_coordinator,
            refresh_osm=args.refresh_osm, force=args.force,
        )
    elif args.command == "report":
        report(args.limit)
    elif args.command == "labels":
        labels(args.limit, args.per_bucket)
    elif args.command == "labels-report":
        labels_report()
    elif args.command == "fetch-content":
        fetch_content(args.limit, args.workers)
    elif args.command == "classify":
        sources = {s.strip() for s in args.source.split(",") if s.strip()}
        unknown = sources - {"page", "purpose"}
        if unknown:
            # `wz` used to be accepted here and filled the Technologiefeld from the register's
            # industry code. It is refused on purpose now: that is a different classification
            # and it gets its own column. See combined_discovery/wz_cluster.py.
            raise SystemExit(
                f"unknown source(s): {sorted(unknown)}. The Technologiefeld is filled only from "
                "text the company published: page, purpose."
            )
        classify(args.limit, sources, args.llm)
    elif args.command == "classify-report":
        classify_report()
    elif args.command == "classify-labels":
        branch_labels(args.per_bucket)
    elif args.command == "classify-labels-report":
        branch_labels_report()
    elif args.command == "propose-keywords":
        propose_keywords(args.limit)
    elif args.command == "classify-purpose":
        tiers = {t.strip() for t in (args.tier or "").split(",") if t.strip()}
        unknown = tiers - {"tiebreak", "semantic"}
        if unknown:
            raise SystemExit(
                f"--tier does not know {sorted(unknown)}. Only `tiebreak` (3 candidates, on torn "
                f"rows) and `semantic` (all 77, on no_keyword rows) exist. They are different "
                f"questions and are reported apart."
            )
        classify_purpose(
            args.limit,
            use_llm=args.llm and "tiebreak" in tiers,
            semantic=args.llm and "semantic" in tiers,
            llm_budget=args.llm_limit,
            only=args.company_id,
        )
    elif args.command == "classify-purpose-report":
        classify_purpose_report()
    elif args.command == "classify-purpose-labels":
        purpose_labels(args.per_bucket)
    elif args.command == "classify-purpose-labels-report":
        purpose_labels_report()
    elif args.command == "compare-columns":
        compare_columns(args.limit)
    else:
        # `csv` lands here, and so does any subcommand added above without a branch of its own.
        # That silence is the trap: a new subcommand with no `elif` runs write_csv and looks like it
        # worked. If you add one, add its branch too.
        write_csv(args.limit)


if __name__ == "__main__":
    sys.exit(main())
