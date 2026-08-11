"""Measure what name + address alone can deliver: website URL, owner name, tokens per business.

Answers three questions that the DAGs, as they stand, cannot:

  1. For roughly what percentage of companies is a website / an owner name findable?
     The DAGs advance independent checkpoints, so their batches diverge and their hit rates
     are not comparable. This runs ONE fixed, deterministic sample through every variant.

  2. Which LLM is best? The shared runner deliberately pins extraction to one Mistral model
     so that providers can be compared. Here the axis is inverted: the search hits are
     fetched ONCE and cached, then replayed through each model. Same hits, same prompt,
     same parser — only the model varies.

  3. How token-intensive is one business? Recorded per company per model, not just summed
     per run.

The AC says "using only the name and address". Production does not: build_search_queries
puts the Handelsregister number into the query, and score_candidate awards its +100
auto-accept solely on a register-number match. `--reg-mode name_address` blanks the field
on the way in, so both effects disappear together; `with_reg` leaves production untouched.
Running both on the same companies is what shows the register number's actual worth.

Writes NOTHING to the database. Observations, checkpoints and canonical resolution are the
DAGs' business; a measurement that also mutated state could not be re-run.

  stage1  search + crawl + Impressum. Expensive (Exa spend, ~5 page fetches/company).
  stage2  extraction only, replayed over stage1's cached hits. Cheap, re-runnable per model.
  report  reads both stages and prints the comparison.

Usage (from the repo root, containers up):

  docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-scheduler \
      python /opt/airflow/scripts/ac_measurement.py stage1 --reg-mode name_address --limit 100
  docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-scheduler \
      python /opt/airflow/scripts/ac_measurement.py stage2 --reg-mode name_address \
      --model mistral:ministral-8b-latest
  docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-scheduler \
      python /opt/airflow/scripts/ac_measurement.py report

Output files land in /opt/airflow/data (gitignored) because they carry managing-director
names, which are personal data.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

# `requests`, psycopg2 and the DAG modules are imported inside the functions that need them,
# NOT here: `report` must run on the host straight against the output directory. Reading the
# cache through the bind mount is what produced the truncated reads in the first place.

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ac_measurement")


def _default_out_dir() -> Path:
    container = Path("/opt/airflow/data")
    if container.is_dir():
        return container
    return Path(__file__).resolve().parent.parent / "airflow" / "data"


OUT_DIR = Path(os.getenv("AC_OUT_DIR") or _default_out_dir())
MAX_CANDIDATES = int(os.getenv("AC_MAX_CANDIDATES", "5"))
STAGE1_WORKERS = int(os.getenv("AC_STAGE1_WORKERS", "4"))
STAGE2_WORKERS = int(os.getenv("AC_STAGE2_WORKERS", "2"))
ATTEMPTS = int(os.getenv("AC_EXTRACT_ATTEMPTS", "5"))
REG_MODES = ("name_address", "with_reg")

#: `--model` values are `<backend>:<model>`. Ollama speaks the OpenAI shape on /v1, so one
#: request builder covers all three backends — which is the point: a difference in the
#: numbers must come from the model, not from two different client implementations.
BACKENDS = {
    "mistral": (
        os.getenv("MISTRAL_API_BASE_URL", "https://api.mistral.ai/v1"),
        lambda: os.getenv("MISTRAL_API_KEY", ""),
    ),
    "ollama": (
        os.getenv("OLLAMA_OPENAI_BASE_URL", "http://host.docker.internal:11434/v1"),
        lambda: "ollama",  # any non-empty string; Ollama ignores the bearer token
    ),
}


def stage1_path(reg_mode: str) -> Path:
    return OUT_DIR / f"ac_stage1_{reg_mode}.jsonl"


def stage2_path(reg_mode: str, model: str) -> Path:
    slug = model.replace(":", "_").replace("/", "_")
    return OUT_DIR / f"ac_stage2_{reg_mode}__{slug}.jsonl"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a cache file, splitting on newlines ONLY.

    Not `splitlines()`: it also splits on U+2028, U+2029 and U+0085, and scraped German page
    text contains those raw. `json.dumps` has no reason to escape them — they are legal inside
    a JSON string — so one record gets cut in two and the half-record raises
    "Unterminated string". It reads exactly like a truncated write, which is the wrong thing to
    go fix. Writing is fine; only this split was ever broken.
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
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


# --------------------------------------------------------------------------------------
# sample
# --------------------------------------------------------------------------------------

#: ORDER BY md5(id) rather than random(): the same N companies every time, so a re-run and a
#: second variant measure the same population. ORDER BY id would bunch the sample into one
#: corner of whatever order the import happened to use.
SAMPLE_SQL = """
SELECT id::text, company_name, address, postcode, city, registration_number,
       legal_form, wz_code, business_purpose
FROM public.companies
WHERE company_name IS NOT NULL
  AND (address IS NOT NULL OR postcode IS NOT NULL)
ORDER BY md5(id::text)
LIMIT %s
"""


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
            id=r[0],
            company_name=r[1],
            address=r[2],
            postcode=r[3],
            city=r[4],
            registration_number=r[5],
            legal_form=r[6],
            wz_code=r[7],
            business_purpose=r[8],
        )
        for r in rows
    ]


# --------------------------------------------------------------------------------------
# stage 1 — search, crawl, Impressum. Mirrors search_discovery_runner.discover, minus the
# extraction call, plus the kept hits so stage 2 can replay them.
# --------------------------------------------------------------------------------------


def search_and_verify(company: Any) -> dict[str, Any]:
    from exa_search_discovery.exa_http import search as exa_search
    from imprint_probe import best_imprint
    from mistral_extract import as_str
    from search_common import filter_hits, is_own_site_candidate, origin_url, own_site_verdict
    from search_discovery_runner import _company_address
    from source_enrichment_common import build_search_queries
    from url_discovery.http_client import verify_url
    from url_discovery.scorer import AUTO_ACCEPT_SCORE, name_uniqueness, score_candidate

    queries = build_search_queries(company)
    address = _company_address(company)
    started = time.monotonic()

    response = exa_search(
        [queries["general"], queries["website"]],
        objective=f"Find the official website and Impressum of {company.company_name}, {address}",
    )
    kept, dropped = filter_hits(response.hits, company.company_name)

    candidates: list[dict[str, Any]] = []
    for hit in kept[:MAX_CANDIDATES]:
        verified = verify_url(hit.url)
        if not verified:
            continue
        extracted: dict[str, Any] = {}
        imprint_url = None
        probed = best_imprint(verified["final_url"])
        if probed:
            _html, imprint_url, extracted = probed
        candidates.append(
            {
                "url": hit.url,
                "final_url": verified["final_url"],
                "title": hit.title,
                "imprint_url": imprint_url,
                "extracted": extracted,
                "score": score_candidate(extracted, company),
                "verdict": own_site_verdict(verified["final_url"], company.company_name),
            }
        )
    candidates.sort(key=lambda c: c["score"], reverse=True)

    own_site = [
        c
        for c in candidates
        if is_own_site_candidate(
            c["final_url"], company.company_name, c["score"], AUTO_ACCEPT_SCORE
        )
    ]
    best = own_site[0] if own_site else None

    path_a: dict[str, Any] = {"website": None, "confidence": None, "verified_by": None,
                              "ceo_names": [], "phone": None, "email": None}
    if best and best["score"] >= 0:
        extracted = best["extracted"] or {}
        if best["score"] >= AUTO_ACCEPT_SCORE:
            confidence, verified_by = 1.0, "register_number_match"
        else:
            confidence = round(0.35 + 0.30 * name_uniqueness(company.company_name), 4)
            verified_by = "url_pattern"
        path_a = {
            "website": origin_url(best["final_url"]),
            "confidence": confidence,
            "verified_by": verified_by,
            # Production only writes these when the website cleared 0.35, so mirror that:
            # owner coverage from the Impressum is bounded by website coverage.
            "ceo_names": [n for n in (extracted.get("ceo_names") or []) if as_str(n)]
            if confidence >= 0.35
            else [],
            "phone": as_str(extracted.get("phone")) if confidence >= 0.35 else None,
            "email": as_str(extracted.get("email")) if confidence >= 0.35 else None,
        }

    return {
        "company_id": company.id,
        "company_name": company.company_name,
        "address": address,
        "registration_number": company.registration_number,
        "queries": queries,
        "hits_total": len(response.hits),
        "hits_kept": len(kept),
        "hits_dropped": dropped,
        "exa_cost_usd": response.cost_usd,
        "candidates_crawled": len(candidates),
        "rejected_as_third_party": len(candidates) - len(own_site),
        "path_a": path_a,
        # What stage 2 replays. Same join as the runner builds for its prompt.
        "kept_hits": [
            {"url": h.url, "title": h.title, "text": h.text} for h in kept if h.text.strip()
        ],
        "elapsed_s": round(time.monotonic() - started, 2),
    }


def run_stage1(reg_mode: str, limit: int) -> None:
    path = stage1_path(reg_mode)
    done = {row["company_id"] for row in read_jsonl(path)}
    companies = load_sample(limit)
    if reg_mode == "name_address":
        companies = [dataclasses.replace(c, registration_number=None) for c in companies]
    pending = [c for c in companies if c.id not in done]
    log.info("stage1 %s: %d companies, %d already cached, %d to run",
             reg_mode, len(companies), len(done), len(pending))
    if not pending:
        return

    total = len(pending)
    with ThreadPoolExecutor(max_workers=STAGE1_WORKERS) as pool:
        for index, (company, result) in enumerate(
            zip(pending, pool.map(_safe_stage1, pending)), start=1
        ):
            append_jsonl(path, result)
            log.info("[%d/%d] %s -> %s", index, total, company.company_name[:45],
                     (result.get("path_a") or {}).get("website") or result.get("error") or "nothing")


def _safe_stage1(company: Any) -> dict[str, Any]:
    try:
        return search_and_verify(company)
    except Exception as exc:  # noqa: BLE001
        log.warning("stage1 failed for %s: %s", company.id, exc)
        return {
            "company_id": company.id,
            "company_name": company.company_name,
            "error": f"{type(exc).__name__}: {exc}",
            "kept_hits": [],
            "path_a": {"website": None, "ceo_names": []},
            "exa_cost_usd": None,
        }


# --------------------------------------------------------------------------------------
# stage 2 — extraction only, one backend-agnostic request builder
# --------------------------------------------------------------------------------------


def chat_json(model_ref: str, prompt: str) -> tuple[dict[str, Any], int, int]:
    import requests

    from mistral_extract import EXTRACT_MAX_CHARS

    backend, _, model = model_ref.partition(":")
    if backend not in BACKENDS:
        raise ValueError(f"unknown backend {backend!r}; expected one of {sorted(BACKENDS)}")
    base_url, key_getter = BACKENDS[backend]
    api_key = key_getter()
    if not api_key:
        raise RuntimeError(f"no API key configured for backend {backend!r}")

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt[:EXTRACT_MAX_CHARS]}],
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "stream": False,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    timeout = float(os.getenv("AC_EXTRACT_TIMEOUT_S", "180"))

    # Retrying 429 is not politeness, it is correctness: mistral-small is rate-limited harder
    # than ministral-8b on the same account, so without this the larger model simply loses 60%
    # of its sample and looks worse at everything. Mirrors mistral_extract.extract_json.
    body = None
    for attempt in range(1, ATTEMPTS + 1):
        response = requests.post(
            f"{base_url.rstrip('/')}/chat/completions", headers=headers, json=payload, timeout=timeout
        )
        if response.status_code == 429 and attempt < ATTEMPTS:
            retry_after = response.headers.get("Retry-After")
            try:
                wait_s = min(float(retry_after), 30.0) if retry_after else 2.0 * attempt
            except ValueError:
                wait_s = 2.0 * attempt
            time.sleep(wait_s)
            continue
        response.raise_for_status()
        body = response.json()
        break
    if body is None:
        raise RuntimeError(f"{model_ref}: exhausted {ATTEMPTS} attempts against {base_url}")
    usage = body.get("usage") or {}
    content = body["choices"][0]["message"]["content"]
    return (
        json.loads(content),
        int(usage.get("prompt_tokens") or 0),
        int(usage.get("completion_tokens") or 0),
    )


def run_stage2(reg_mode: str, model_ref: str) -> None:
    from mistral_extract import as_confidence, as_str
    from search_discovery_runner import EXTRACTION_PROMPT

    source = read_jsonl(stage1_path(reg_mode))
    if not source:
        raise SystemExit(f"no stage1 output for {reg_mode}; run stage1 first")
    path = stage2_path(reg_mode, model_ref)
    done = {row["company_id"] for row in read_jsonl(path)}
    pending = [r for r in source if r["company_id"] not in done]
    log.info("stage2 %s / %s: %d cached companies, %d to extract",
             reg_mode, model_ref, len(done), len(pending))

    def extract(row: dict[str, Any]) -> dict[str, Any]:
        out = {"company_id": row["company_id"], "company_name": row.get("company_name"),
               "model": model_ref, "tokens_in": 0, "tokens_out": 0, "llm": None, "error": None}
        hits = row.get("kept_hits") or []
        if not hits:
            out["error"] = "no_hits"
            return out
        search_text = "\n\n---\n\n".join(
            f"SOURCE: {h['title']}\nURL: {h['url']}\n{h['text']}".strip() for h in hits
        )
        started = time.monotonic()
        try:
            parsed, tin, tout = chat_json(
                model_ref,
                EXTRACTION_PROMPT.format(
                    name=row["company_name"], address=row.get("address") or "", results=search_text
                ),
            )
        except Exception as exc:  # noqa: BLE001
            out["error"] = f"{type(exc).__name__}: {exc}"
            return out
        out.update(
            tokens_in=tin,
            tokens_out=tout,
            latency_s=round(time.monotonic() - started, 2),
            llm={
                "website": as_str(parsed.get("website")),
                "cluster": as_str(parsed.get("cluster")),
                "tech_field": as_str(parsed.get("tech_field")),
                "ceo_name_or_contact": as_str(parsed.get("ceo_name_or_contact")),
                "confidence": as_confidence(parsed.get("confidence")),
            },
        )
        return out

    total = len(pending)
    with ThreadPoolExecutor(max_workers=STAGE2_WORKERS) as pool:
        for index, result in enumerate(pool.map(extract, pending), start=1):
            append_jsonl(path, result)
            if index % 10 == 0 or index == total:
                log.info("[%d/%d] %s", index, total, model_ref)


# --------------------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------------------


def _pct(numerator: int, denominator: int) -> str:
    return f"{100.0 * numerator / denominator:5.1f}%" if denominator else "    -"


def _median(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) // 2


def run_report() -> None:
    print("\n=== Stage 1: search + Impressum verification (LLM-independent) ===")
    print(f"{'reg mode':<14} {'n':>4} {'website':>8} {'reg-match':>10} {'owner(imp)':>11} "
          f"{'phone/mail':>11} {'no hits':>8} {'Exa $':>8}")
    stage1: dict[str, list[dict[str, Any]]] = {}
    for reg_mode in REG_MODES:
        rows = read_jsonl(stage1_path(reg_mode))
        if not rows:
            continue
        stage1[reg_mode] = rows
        n = len(rows)
        website = sum(1 for r in rows if (r.get("path_a") or {}).get("website"))
        regmatch = sum(1 for r in rows
                       if (r.get("path_a") or {}).get("verified_by") == "register_number_match")
        owner = sum(1 for r in rows if (r.get("path_a") or {}).get("ceo_names"))
        contact = sum(1 for r in rows
                      if (r.get("path_a") or {}).get("phone") or (r.get("path_a") or {}).get("email"))
        nohits = sum(1 for r in rows if not r.get("kept_hits"))
        cost = sum(r.get("exa_cost_usd") or 0.0 for r in rows)
        print(f"{reg_mode:<14} {n:>4} {_pct(website, n):>8} {_pct(regmatch, n):>10} "
              f"{_pct(owner, n):>11} {_pct(contact, n):>11} {_pct(nohits, n):>8} {cost:>8.3f}")

    print("\n=== Stage 2: extraction, same hits replayed per model ===")
    print(f"{'reg mode':<14} {'model':<28} {'n':>4} {'owner':>7} {'website':>8} {'branch':>7} "
          f"{'fail':>6} {'tok in':>8} {'tok out':>8} {'median':>7}")
    stage2_rows: list[tuple[str, str, list[dict[str, Any]]]] = []
    for reg_mode in REG_MODES:
        for path in sorted(OUT_DIR.glob(f"ac_stage2_{reg_mode}__*.jsonl")):
            rows = read_jsonl(path)
            if not rows:
                continue
            model = rows[0].get("model") or path.stem
            stage2_rows.append((reg_mode, model, rows))
            n = len(rows)
            ok = [r for r in rows if r.get("llm")]
            owner = sum(1 for r in ok if (r["llm"] or {}).get("ceo_name_or_contact"))
            website = sum(1 for r in ok if (r["llm"] or {}).get("website"))
            branch = sum(1 for r in ok if (r["llm"] or {}).get("tech_field"))
            fail = sum(1 for r in rows if r.get("error") and r.get("error") != "no_hits")
            tin = [r["tokens_in"] for r in ok]
            tout = [r["tokens_out"] for r in ok]
            print(f"{reg_mode:<14} {model:<28} {n:>4} {_pct(owner, n):>7} {_pct(website, n):>8} "
                  f"{_pct(branch, n):>7} {fail:>6} "
                  f"{(sum(tin)//len(tin) if tin else 0):>8} "
                  f"{(sum(tout)//len(tout) if tout else 0):>8} {_median(tin):>7}")

    print("\n=== Combined: what the AC actually asks for (URL and owner, either path) ===")
    print(f"{'reg mode':<14} {'model':<28} {'URL any':>8} {'owner any':>10} {'both':>7}")
    for reg_mode, model, rows in stage2_rows:
        by_id = {r["company_id"]: r for r in rows}
        base = stage1.get(reg_mode) or []
        n = len(base)
        url_any = owner_any = both = 0
        for record in base:
            path_a = record.get("path_a") or {}
            llm = (by_id.get(record["company_id"]) or {}).get("llm") or {}
            has_url = bool(path_a.get("website") or llm.get("website"))
            has_owner = bool(path_a.get("ceo_names") or llm.get("ceo_name_or_contact"))
            url_any += has_url
            owner_any += has_owner
            both += has_url and has_owner
        print(f"{reg_mode:<14} {model:<28} {_pct(url_any, n):>8} {_pct(owner_any, n):>10} "
              f"{_pct(both, n):>7}")
    print()
    print("Note: 'URL any' and 'owner any' include unverified LLM reads. Production caps an")
    print("LLM-read website at 0.34 confidence, below the resolver's 0.35 threshold, so those")
    print("never become the canonical website on their own. The stage 1 'website' column is")
    print("the number that survives verification.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p1 = sub.add_parser("stage1", help="search + crawl + Impressum (expensive, cached)")
    p1.add_argument("--reg-mode", choices=REG_MODES, required=True)
    p1.add_argument("--limit", type=int, default=100)

    p2 = sub.add_parser("stage2", help="extraction only, replayed over cached hits")
    p2.add_argument("--reg-mode", choices=REG_MODES, required=True)
    p2.add_argument("--model", required=True, help="e.g. mistral:ministral-8b-latest")

    sub.add_parser("report", help="print the comparison")

    args = parser.parse_args()
    if args.command == "stage1":
        run_stage1(args.reg_mode, args.limit)
    elif args.command == "stage2":
        run_stage2(args.reg_mode, args.model)
    else:
        run_report()


if __name__ == "__main__":
    sys.exit(main())
