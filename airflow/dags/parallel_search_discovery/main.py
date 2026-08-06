"""Parallel web search → main's Impressum verification → observations.

Reference implementation for the search-API providers. Exa and Brave are structurally
identical; Parallel is built first only because it needs no API key (free, anonymous MCP) and
is therefore the one verifiable today.

Two paths per company, because a search engine and an LLM answer different questions:

  A  verified   search hit → Impressum → register-number match → confidence up to 1.0
     Reuses main's modules unchanged: url_discovery.http_client.verify_url,
     url_discovery.imprint_extractor.extract_from_html, url_discovery.scorer.score_candidate,
     and main's confidence ladder from url_discovery/main.py.

  B  inferred   the search snippets read by an LLM → cluster / tech_field, and website only
     when path A found none. Lower confidence by construction: nothing was verified against a
     page belonging to the company.

Writes observations ONLY. `pipeline_coordinator` owns is_canonical, company_canonical_field_
sources and companies — this DAG triggers it on completion, as every source DAG does.
"""
from __future__ import annotations

import json
import logging
import os
import sys as _sys, os as _os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

from parallel_http import ParallelQuotaExhausted, ParallelRateLimitError, search as provider_search

from imprint_probe import best_imprint
from mistral_extract import ExtractRateLimitError, as_confidence, as_str, extract_json, require_extract_api_key
from search_common import filter_hits, is_own_site_candidate, origin_url
from source_enrichment_common import (
    build_search_queries,
    ensure_source,
    fetch_companies_for_llm_enrichment,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)
from url_discovery.http_client import verify_url
from url_discovery.scorer import AUTO_ACCEPT_SCORE, MIN_STORE_SCORE, name_uniqueness, score_candidate


log = logging.getLogger(__name__)

SOURCE_KEY = "parallel_search_discovery"
SOURCE_NAME = "Parallel Search + Imprint Verification"
SOURCE_TYPE = "web_search"
ENABLED_FLAG = "ENABLE_PARALLEL_SEARCH_DISCOVERY"
PROVIDER = "parallel"

#: Candidates actually crawled per company. Each costs a verify_url plus up to a handful of
#: Impressum GETs, so this is the main cost knob. url_discovery uses 5.
MAX_CANDIDATES_TO_CRAWL = int(os.getenv("PARALLEL_MAX_CANDIDATES", "5"))
#: Ceiling for an unverified, LLM-read website. Must stay below both the resolver's 0.35
#: website threshold and main's pattern-only floor of 0.365, so a snippet guess can never
#: outrank page evidence. See the comment at its use site.
LLM_WEBSITE_CONF_CAP = float(os.getenv("LLM_WEBSITE_CONF_CAP", "0.34"))
PROGRESS_LOG_EVERY = int(os.getenv("PARALLEL_PROGRESS_LOG_EVERY", "10"))
CHECKPOINT_FILE = Path(
    os.getenv(
        "PARALLEL_DISCOVERY_CHECKPOINT_FILE",
        "/opt/airflow/data/parallel_search_discovery_checkpoint.json",
    )
)

EXTRACTION_PROMPT = """Extract facts about this German company from the search results below.
Use only what the results state. If a fact is not stated, return null — do not guess.
The results may mention OTHER companies with similar names; report only this one.

Company: {name}
Address: {address}

Return only JSON:
{{"website": string or null,
  "cluster": string or null,
  "tech_field": string or null,
  "ceo_name_or_contact": string or null,
  "confidence": number between 0 and 1}}

SEARCH RESULTS:
---
{results}
---"""


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not enabled.")
    require_extract_api_key()


def _ensure_checkpoint_parent() -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_checkpoint() -> dict[str, Any]:
    if not CHECKPOINT_FILE.exists():
        return {}
    try:
        return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning("Could not parse %s, starting fresh.", CHECKPOINT_FILE)
        return {}


def _save_checkpoint(last_company_id: str | None, processed_count: int) -> None:
    _ensure_checkpoint_parent()
    CHECKPOINT_FILE.write_text(
        json.dumps(
            {
                "last_company_id": last_company_id,
                "processed_count": processed_count,
                "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _delete_checkpoint() -> None:
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


def _company_address(company: Any) -> str:
    right = f"{company.postcode or ''} {company.city or ''}".strip()
    return ", ".join(part for part in [company.address or "", right] if part)


def _discover(company: Any) -> dict[str, Any]:
    """Search, verify, extract. Pure network + CPU — no DB access, no open transaction."""
    queries = build_search_queries(company)
    address = _company_address(company)

    response = provider_search(
        [queries["general"], queries["website"]],
        objective=f"Find the official website and Impressum of {company.company_name}, {address}",
    )
    kept, dropped = filter_hits(response.hits, company.company_name)

    result: dict[str, Any] = {
        "provider": PROVIDER,
        "queries": queries,
        "hits_total": len(response.hits),
        "hits_kept": len(kept),
        "hits_dropped": dropped,
        "provider_cost_usd": response.cost_usd,
        "candidates": [],
        "best": None,
        "llm": None,
        "tokens_in": 0,
        "tokens_out": 0,
    }
    if not kept:
        result["note"] = "no_matching_results"
        return result

    # ---- path A: verify candidates through main's Impressum + scorer -------------------
    for hit in kept[:MAX_CANDIDATES_TO_CRAWL]:
        verified = verify_url(hit.url)
        if not verified:
            continue
        entry: dict[str, Any] = {
            "url": hit.url,
            "final_url": verified["final_url"],
            "title": hit.title,
            "imprint_url": None,
            "extracted": {},
            "score": 0,
        }
        probed = best_imprint(verified["final_url"])
        if probed:
            _html, imprint_url, extracted = probed
            entry["imprint_url"] = imprint_url
            entry["extracted"] = extracted
        entry["score"] = score_candidate(entry["extracted"], company)
        result["candidates"].append(entry)

    if result["candidates"]:
        result["candidates"].sort(key=lambda c: c["score"], reverse=True)
        # Directories and third-party listings stay in `candidates` — their registry text is
        # worth extracting from — but must never become the company's own website. main's
        # scorer has no such notion, because its name-derived guesses can never be a directory.
        #
        # is_directory_host is a HARD exclusion, deliberately not overridable by the score. A
        # registry page contains the company's real HRB by definition, so score_candidate gives
        # it +100 and it "verifies" every time: `online-handelsregister...` was recorded as
        # yt energie GmbH's website at confidence 1.0. A register match proves the page is
        # ABOUT the company, not that it IS the company's site.
        #
        # host_matches_company, by contrast, IS overridable by a register match — a company
        # whose domain does not contain its name can still prove itself via the Impressum. That
        # escape hatch is what let creditsafe.com through, so is_own_site_candidate refuses it to
        # company-listing paths. Use the SAME predicate for the candidate_url/directory_url split
        # below — the resolver treats candidate_url as a website alias.
        own_site = [
            c
            for c in result["candidates"]
            if is_own_site_candidate(
                c["final_url"], company.company_name, c["score"], AUTO_ACCEPT_SCORE
            )
        ]
        result["best"] = own_site[0] if own_site else None
        result["best_rejected_as_third_party"] = len(result["candidates"]) - len(own_site)

    # ---- path B: the LLM reads the snippets -------------------------------------------
    search_text = "\n\n---\n\n".join(
        f"SOURCE: {h.title}\nURL: {h.url}\n{h.text}".strip() for h in kept if h.text.strip()
    )
    if search_text.strip():
        try:
            parsed, tokens_in, tokens_out = extract_json(
                EXTRACTION_PROMPT.format(name=company.company_name, address=address, results=search_text)
            )
            result["tokens_in"], result["tokens_out"] = tokens_in, tokens_out
            result["llm"] = {
                "website": as_str(parsed.get("website")),
                "cluster": as_str(parsed.get("cluster")),
                "tech_field": as_str(parsed.get("tech_field")),
                "ceo_name_or_contact": as_str(parsed.get("ceo_name_or_contact")),
                # Clamped: the raw value is a model's self-report and confidence is
                # NUMERIC(5,4) CHECK BETWEEN 0 AND 1, so an out-of-range number raises.
                "confidence": as_confidence(parsed.get("confidence")),
            }
        except ExtractRateLimitError:
            raise
        except Exception as exc:  # noqa: BLE001
            result["note"] = f"extract_failed:{type(exc).__name__}"
            log.warning("extraction failed for %s: %s", company.id, exc)
    else:
        result["note"] = "matching_results_had_no_text"
    return result


def _write_observations(cur: Any, *, company: Any, source_id: str, sync_run_id: str, result: dict[str, Any]) -> bool:
    """Write every observation for one company. Returns True if the source record was new."""
    best = result.get("best")
    best_score = best["score"] if best else None
    website_url = best["final_url"] if best and best["score"] >= 0 else None

    payload = {
        # "fetched", NOT dump_preview_payload's "planned": the selection query excludes only
        # non-'planned' records, so leaving it planned means the company is never marked done.
        "status": "fetched",
        "source_key": SOURCE_KEY,
        "provider": PROVIDER,
        "company_id": company.id,
        "company_name": company.company_name,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "result": result,
    }
    source_record_id, is_created = upsert_source_record(
        cur,
        company_id=company.id,
        source_id=source_id,
        external_id=f"{SOURCE_KEY}:{company.id}",
        external_url=website_url,
        source_company_name=company.company_name,
        source_payload=payload,
    )
    common = {
        "company_id": company.id,
        "source_record_id": source_record_id,
        "source_id": source_id,
        "sync_run_id": sync_run_id,
    }

    for field_name, query_key in (("search_query", "general"), ("website_search_query", "website")):
        insert_text_observation(
            cur,
            **common,
            field_name=field_name,
            value_text=result["queries"][query_key],
            metadata={"query_type": query_key, "provider": PROVIDER},
            confidence=1.0,
        )

    # ---- path A observations, using url_discovery/main.py's confidence ladder ----------
    website_conf: float | None = None
    for candidate in result.get("candidates") or []:
        if candidate["score"] < MIN_STORE_SCORE:
            continue
        # `candidate_url` is an ALIAS for the logical `website` field in
        # pipeline_coordinator.resolver.resolve_website (min conf 0.35), so anything written
        # under that name competes to become the canonical website. A directory must not:
        # `online-handelsregister...` register-matches by definition, giving candidate
        # confidence (100+50)/150 = 1.0, and it won canonical website for yt energie GmbH.
        # `directory_url` is a name the resolver does not consume — kept as queryable evidence,
        # out of contention. Anything that is not an own-site candidate goes there, not just
        # known directory hosts: heizoel24.de is neither a directory nor Ö. H. O. GmbH's site.
        is_own_site = is_own_site_candidate(
            candidate["final_url"], company.company_name, candidate["score"], AUTO_ACCEPT_SCORE
        )
        insert_text_observation(
            cur,
            **common,
            field_name="candidate_url" if is_own_site else "directory_url",
            value_text=candidate["final_url"],
            metadata={
                "provider": PROVIDER,
                "score": candidate["score"],
                "imprint_url": candidate["imprint_url"],
                "register_number": (candidate["extracted"] or {}).get("register_number"),
                "postcode": (candidate["extracted"] or {}).get("postcode"),
            },
            confidence=min(1.0, max(0.0, (candidate["score"] + 50) / 150)),
        )

    if best and best_score is not None and best_score >= 0:
        extracted = best["extracted"] or {}
        if best_score >= AUTO_ACCEPT_SCORE:
            website_conf = 1.0
            verified_by, extra = "register_number_match", {"register_number": extracted.get("register_number")}
        else:
            uniqueness = name_uniqueness(company.company_name)
            website_conf = round(0.35 + 0.30 * uniqueness, 4)
            verified_by, extra = "url_pattern", {"name_uniqueness": round(uniqueness, 3)}
        insert_text_observation(
            cur,
            **common,
            field_name="website",
            # The site, not the page search happened to rank. `matched_url` keeps that page.
            value_text=origin_url(best["final_url"]),
            metadata={
                "provider": PROVIDER,
                "score": best_score,
                "verified_by": verified_by,
                "matched_url": best["final_url"],
                **extra,
            },
            confidence=website_conf,
        )

        if website_conf >= 0.35:
            field_conf = round(website_conf * 0.95, 4)
            for field_name in ("phone", "email"):
                value = as_str(extracted.get(field_name))
                if value:
                    insert_text_observation(
                        cur,
                        **common,
                        field_name=field_name,
                        value_text=value,
                        metadata={"provider": PROVIDER, "source": "impressum", "imprint_url": best["imprint_url"]},
                        confidence=field_conf,
                    )
            for ceo in extracted.get("ceo_names") or []:
                name = as_str(ceo)
                if name:
                    insert_text_observation(
                        cur,
                        **common,
                        field_name="ceo_name_or_contact",
                        value_text=name,
                        metadata={"provider": PROVIDER, "source": "impressum", "imprint_url": best["imprint_url"]},
                        confidence=round(field_conf * 0.90, 4),
                    )

    # ---- path B observations: inferred, never verified against the company's own page ---
    llm = result.get("llm") or {}
    llm_conf = llm.get("confidence")
    for field_name in ("cluster", "tech_field", "ceo_name_or_contact"):
        value = llm.get(field_name)
        if value:
            insert_text_observation(
                cur,
                **common,
                field_name=field_name,
                value_text=value,
                metadata={"provider": PROVIDER, "verified_by": "llm_snippet"},
                confidence=llm_conf,
            )
    # A website the LLM read off a snippet was never checked against a page belonging to this
    # company, so it must not outrank path A. Left uncapped it does: the model self-reported
    # 0.95 for one company, beating path A's pattern-verified band of 0.365-0.65 outright.
    #
    # Capped below the resolver's 0.35 website threshold, so it is recorded as evidence and
    # shows up in provider comparisons, but never becomes the canonical website on its own.
    # cluster / tech_field / ceo_name_or_contact above keep the model's own confidence because
    # they have no verification path at all — there is nothing better for them to lose to.
    if llm.get("website") and website_conf is None:
        insert_text_observation(
            cur,
            **common,
            field_name="website",
            value_text=llm["website"],
            metadata={"provider": PROVIDER, "verified_by": "llm_snippet", "raw_confidence": llm_conf},
            confidence=min(llm_conf if llm_conf is not None else 0.30, LLM_WEBSITE_CONF_CAP),
        )
    return is_created


def run_parallel_search_discovery() -> None:
    checkpoint = _load_checkpoint()
    checkpoint_after_id = checkpoint.get("last_company_id")
    # Without min_quality_score: that branch of the helper mis-binds its parameters on main
    # and raises. This path correctly skips companies already at data_quality_score >= 0.90.
    companies = fetch_companies_for_llm_enrichment(SOURCE_KEY, after_id=checkpoint_after_id)
    if not companies and checkpoint_after_id:
        _delete_checkpoint()
        companies = fetch_companies_for_llm_enrichment(SOURCE_KEY, after_id=None)
    if not companies:
        log.info("No companies pending for %s.", SOURCE_KEY)
        return

    _require_enabled()
    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cur:
                source_id = ensure_source(
                    cur,
                    key=SOURCE_KEY,
                    name=SOURCE_NAME,
                    source_type=SOURCE_TYPE,
                    metadata={"dag_id": SOURCE_KEY, "provider": PROVIDER},
                )
                sync_run_id = start_sync_run(cur, source_id=source_id)

        created = updated = failed = processed = 0
        tokens_in = tokens_out = 0
        stopped_early = False
        last_completed_company_id = checkpoint_after_id

        for index, company in enumerate(companies, start=1):
            # Network first, outside any transaction: never hold one open across a search.
            try:
                result = _discover(company)
            except (ParallelQuotaExhausted, ParallelRateLimitError, ExtractRateLimitError) as exc:
                failed += 1
                stopped_early = True
                # Checkpoint the last COMPLETED company so this one is retried, not skipped.
                _save_checkpoint(last_completed_company_id, processed)
                log.warning("Stopping %s at %s (%s).", SOURCE_KEY, company.id, type(exc).__name__)
                break
            except Exception:
                failed += 1
                processed += 1
                log.exception("Discovery failed for company %s", company.id)
                last_completed_company_id = company.id
                _save_checkpoint(last_completed_company_id, processed)
                continue

            try:
                with connection:
                    with connection.cursor() as cur:
                        is_created = _write_observations(
                            cur,
                            company=company,
                            source_id=source_id,
                            sync_run_id=sync_run_id,
                            result=result,
                        )
                created += int(is_created)
                updated += int(not is_created)
                processed += 1
                tokens_in += result["tokens_in"]
                tokens_out += result["tokens_out"]
                last_completed_company_id = company.id
            except Exception:
                failed += 1
                processed += 1
                log.exception("Write failed for company %s", company.id)

            _save_checkpoint(last_completed_company_id, processed)
            best = result.get("best")
            log.info(
                "[%d/%d] %s | hits=%d kept=%d | score=%s | %s",
                index,
                len(companies),
                company.company_name[:40],
                result["hits_total"],
                result["hits_kept"],
                best["score"] if best else "-",
                (best["final_url"] if best else None) or "nothing",
            )
            if index % PROGRESS_LOG_EVERY == 0:
                log.info(
                    "%s progress: %d/%d (%d created, %d updated, %d failed, %d/%d tokens)",
                    SOURCE_KEY, index, len(companies), created, updated, failed, tokens_in, tokens_out,
                )

        with connection:
            with connection.cursor() as cur:
                finish_sync_run(
                    cur,
                    sync_run_id=sync_run_id,
                    records_seen=processed,
                    records_created=created,
                    records_updated=updated,
                    records_failed=failed,
                    status="partial" if stopped_early else None,
                )
        log.info(
            "%s finished: %d processed, %d created, %d updated, %d failed, %d in / %d out tokens",
            SOURCE_KEY, processed, created, updated, failed, tokens_in, tokens_out,
        )
        if not stopped_early:
            _delete_checkpoint()
    finally:
        connection.close()


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None

    with DAG(
        dag_id=SOURCE_KEY,
        description="Parallel web search, verified against the company's Impressum",
        schedule=None,
        start_date=datetime(2026, 8, 6, tzinfo=timezone.utc),
        catchup=False,
        tags=["sources", "parallel", "search", "discovery"],
    ) as dag:
        discover = PythonOperator(
            task_id="run_parallel_search_discovery",
            python_callable=run_parallel_search_discovery,
        )
        trigger_coordinator = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator",
            trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        discover >> trigger_coordinator
    return dag


dag = _make_dag()
