"""Save the Impressum pages of already-verified websites, so owner extraction can be fixed offline.

Why this exists. `scripts/ac_measurement.py` found a website it could prove belonged to the
company for 29 of 100 businesses. For 20 of those 29 it also pulled a phone number or an email
address off the Impressum — the legal contact page every German business website must have. But
it got a manager's name for only 7. The page was fetched and parsed either way, so the search
engine is not the problem: `url_discovery.imprint_extractor.extract_ceo_names` is.

Fixing a regex needs many real examples and many fast attempts. Re-running the measurement for
each attempt would cost search credits and take an hour. So this script fetches those Impressum
pages once, writes them to disk, and every later attempt reads from disk. It costs nothing —
no search API, no LLM, just plain HTTP to sites we already know about.

  collect   fetch and save the pages (run once, in the container so it can import the DAG code)
  shapes    print what follows each manager label, with the names masked — see below
  score     count how many saved pages extract_ceo_names currently finds a name on

Names on an Impressum are personal data. `shapes` therefore replaces every letter with X or x
and every digit with 9 before printing. "Geschäftsführer: Max Mustermann Amtsgericht" prints as
"Xxxxxxxxxxxxxxx: Xxx Xxxxxxxxx Xxxxxxxxxxx". That keeps the only things a regex actually keys
on — word count, capitalisation, punctuation, where the next label starts — and drops the
identities. The unmasked pages stay in /opt/airflow/data, which is gitignored.

Usage:

  docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-scheduler \
      python /opt/airflow/scripts/collect_imprint_fixtures.py collect
  python scripts/collect_imprint_fixtures.py shapes
  python scripts/collect_imprint_fixtures.py score
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("imprint_fixtures")


def _default_out_dir() -> Path:
    container = Path("/opt/airflow/data")
    if container.is_dir():
        return container
    return Path(__file__).resolve().parent.parent / "airflow" / "data"


OUT_DIR = Path(os.getenv("AC_OUT_DIR") or _default_out_dir())
FIXTURE_DIR = OUT_DIR / "imprint_fixtures"
INDEX_PATH = FIXTURE_DIR / "index.json"
WORKERS = int(os.getenv("FIXTURE_WORKERS", "6"))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Split on newlines ONLY — see the same note in ac_measurement.read_jsonl."""
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").split("\n"):
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _verified_sites() -> list[dict[str, str]]:
    """Every company that got a website we could prove, across all measured modes.

    Deduplicated by company id: the same company appears in each reg-mode file, and one copy of
    its Impressum is enough.
    """
    by_company: dict[str, dict[str, str]] = {}
    for path in sorted(OUT_DIR.glob("ac_stage1_*.jsonl")):
        for row in read_jsonl(path):
            path_a = row.get("path_a") or {}
            website = path_a.get("website")
            if not website or row["company_id"] in by_company:
                continue
            by_company[row["company_id"]] = {
                "company_id": row["company_id"],
                "company_name": row.get("company_name") or "",
                "website": website,
                # Only newer measurement runs record this; older cache files predate it.
                "imprint_url": path_a.get("imprint_url") or "",
                # What the current extractor managed, so `score` can show before vs after.
                "ceo_names_before": path_a.get("ceo_names") or [],
            }
    return list(by_company.values())


def run_collect() -> None:
    from imprint_probe import best_imprint

    sites = _verified_sites()
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    log.info("%d companies with a verified website; fetching their Impressum", len(sites))

    def fetch(site: dict[str, str]) -> dict[str, Any] | None:
        probed = best_imprint(site["website"])
        if not probed:
            log.warning("no Impressum reachable for %s", site["website"])
            return None
        html, imprint_url, extracted = probed
        name = f"{site['company_id']}.html"
        (FIXTURE_DIR / name).write_text(html, encoding="utf-8")
        return {
            **site,
            "file": name,
            "imprint_url": imprint_url,
            "extracted_keys_found": sorted(k for k, v in extracted.items() if v and k != "text_snippet"),
        }

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        saved = [r for r in pool.map(fetch, sites) if r]

    INDEX_PATH.write_text(json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("saved %d Impressum pages to %s", len(saved), FIXTURE_DIR)


def _mask(text: str) -> str:
    """Letters to X/x, digits to 9. Keeps structure, drops identity."""
    out = []
    for ch in text:
        if ch.isdigit():
            out.append("9")
        elif ch.isalpha():
            out.append("X" if ch.isupper() else "x")
        else:
            out.append(ch)
    return "".join(out)


def _load_index() -> list[dict[str, Any]]:
    if not INDEX_PATH.exists():
        raise SystemExit(f"no fixtures at {INDEX_PATH}; run `collect` inside the container first")
    return json.loads(INDEX_PATH.read_text(encoding="utf-8"))


def _add_dags_to_path() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow" / "dags"))


def run_shapes(window: int) -> None:
    _add_dags_to_path()
    from url_discovery.imprint_extractor import _CEO_LABELS, page_text

    label_re = re.compile(rf"(?:{_CEO_LABELS})", re.IGNORECASE)
    with_label = 0
    for entry in _load_index():
        html = (FIXTURE_DIR / entry["file"]).read_text(encoding="utf-8")
        # The real flattening step, so what is printed is exactly what CEO_RE sees.
        text = page_text(html)
        hits = list(label_re.finditer(text))
        if not hits:
            print(f"--- {entry['file'][:8]}: no manager label anywhere on the page")
            continue
        with_label += 1
        for match in hits[:2]:
            print(f"--- {entry['file'][:8]} (extractor currently finds {len(entry['ceo_names_before'])})")
            # \n printed as | so one fixture stays on one output line.
            print("    " + _mask(text[match.start():match.start() + window]).replace("\n", " | "))
    print(f"\n{with_label} pages have at least one manager label.")


def run_score() -> None:
    _add_dags_to_path()
    from url_discovery.imprint_extractor import extract_from_html

    total = found = 0
    reg = post = phone_mail = 0
    for entry in _load_index():
        html = (FIXTURE_DIR / entry["file"]).read_text(encoding="utf-8")
        out = extract_from_html(html)
        total += 1
        found += bool(out["ceo_names"])
        # Watched because the flattening change affects every extractor, not just names.
        reg += bool(out["register_number"])
        post += bool(out["postcode"])
        phone_mail += bool(out["phone"] or out["email"])
        # Print the COUNT, never the names.
        print(f"{entry['file'][:8]}  names before={len(entry['ceo_names_before'])} now={len(out['ceo_names'])}")
    print(f"\nowner name   {found}/{total}")
    print(f"register no. {reg}/{total}")
    print(f"postcode     {post}/{total}")
    print(f"phone/email  {phone_mail}/{total}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("collect", help="fetch and save the Impressum pages (needs the container)")
    shapes = sub.add_parser("shapes", help="print masked text after each manager label")
    shapes.add_argument("--window", type=int, default=140)
    sub.add_parser("score", help="count how many saved pages yield an owner name")

    args = parser.parse_args()
    if args.command == "collect":
        run_collect()
    elif args.command == "shapes":
        run_shapes(args.window)
    else:
        run_score()


if __name__ == "__main__":
    sys.exit(main())
