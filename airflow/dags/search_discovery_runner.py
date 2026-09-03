"""The per-company discovery flow, shared by every search-provider DAG.

Parallel, Exa and Brave differ only in how a list of pages is obtained. Everything after that
— filtering, Impressum verification, scoring, the confidence ladder, which field name a URL is
written under — must be byte-for-byte identical, for two reasons:

  1. It is what makes "which provider is best" a fair question. Same model, same prompt, same
     scoring; only the search step varies. Three copies of this logic would drift and the
     comparison would silently measure the drift instead of the providers.
  2. Drift between two copies of one rule is what caused the worst bug so far. The
     `candidate_url`/`directory_url` split used `is_directory_host` while the `website`
     observation used a stricter predicate, so candidates rejected as third-party still
     reached the resolver through the alias. One code path cannot disagree with itself.

A provider supplies a `ProviderSpec`; this module owns the rest.

Writes observations ONLY. `pipeline_coordinator` owns is_canonical,
company_canonical_field_sources and companies.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from imprint_probe import best_imprint
from mistral_extract import (
    ExtractRateLimitError,
    as_confidence,
    as_str,
    extract_json,
    require_extract_api_key,
)
from search_common import (
    SearchResponse,
    filter_hits,
    is_own_site_candidate,
    origin_url,
    own_site_verdict,
)
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
from url_discovery.scorer import (
    AUTO_ACCEPT_SCORE,
    MIN_STORE_SCORE,
    name_uniqueness,
    score_candidate,
)


log = logging.getLogger(__name__)

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


@dataclass
class ProviderSpec:
    """Everything that differs between providers."""

    source_key: str
    source_name: str
    provider: str
    #: (queries, objective) -> SearchResponse. Providers without an objective parameter accept
    #: and ignore it — see exa_http.search.
    search: Callable[..., SearchResponse]
    #: Rate-limit / quota exceptions that mean "stop, do not burn the rest of the batch".
    #: The run ends as `partial` and the checkpoint keeps the last COMPLETED company.
    transient_errors: tuple[type[BaseException], ...]
    #: Callable raising a clear error when the provider's key is missing.
    require_key: Callable[[], None]
    #: Refuse to run until a smoke script has proven this client against the provider's LIVE API.
    #:
    #: True by default, so a NEW provider is gated until somebody proves it and has to opt out on
    #: purpose. That default is the whole point: a wrong request shape does not raise, it returns
    #: an empty result list that reads exactly like "the search found nothing". The old Parallel
    #: REST call was malformed for weeks and every run looked like a provider that found little.
    #:
    #: Set False only once a smoke script has run against the live API, and say WHEN in a comment
    #: beside it — that comment is the only record of when the contract was last checked.
    smoke_required: bool = True
    source_type: str = "web_search"
    dag_description: str = ""
    dag_tags: tuple[str, ...] = ()
    env_prefix: str = ""

    def __post_init__(self) -> None:
        self.env_prefix = self.env_prefix or self.provider.upper()

    @property
    def enabled_flag(self) -> str:
        return f"ENABLE_{self.source_key.upper()}"

    def env(self, suffix: str, default: str) -> str:
        return os.getenv(f"{self.env_prefix}_{suffix}", default)

    @property
    def max_candidates(self) -> int:
        """Candidates crawled per company — the main cost knob. url_discovery uses 5."""
        return int(self.env("MAX_CANDIDATES", "5"))

    @property
    def progress_log_every(self) -> int:
        return int(self.env("PROGRESS_LOG_EVERY", "10"))

    @property
    def checkpoint_file(self) -> Path:
        return Path(
            self.env(
                "DISCOVERY_CHECKPOINT_FILE",
                f"/opt/airflow/data/{self.source_key}_checkpoint.json",
            )
        )

    @property
    def smoke_marker(self) -> Path:
        """Where a smoke script records that this client's live API contract holds.

        Beside the checkpoint rather than at a path of its own, for two reasons. It follows the
        checkpoint if that is ever relocated, and it needs no new environment variable — one absent
        from BOTH `.env.example` and `docker-compose.yml` cannot be set by editing `.env` alone.

        `/opt/airflow/data` is `./airflow/data` on the host (`docker-compose.yml`), which is what
        lets a script running on the host write a file this task can read.
        """
        return self.checkpoint_file.parent / f"{self.source_key}_smoke_ok.json"


#: Ceiling for an unverified, LLM-read website. Must stay below both the resolver's 0.35 website
#: threshold and main's pattern-only floor of 0.365, so a snippet guess can never outrank page
#: evidence. Shared across providers — a per-provider cap would skew the comparison.
LLM_WEBSITE_CONF_CAP = float(os.getenv("LLM_WEBSITE_CONF_CAP", "0.34"))


def require_smoke_check(spec: ProviderSpec) -> None:
    """Refuse to spend money through a client nobody has ever seen work.

    The ordering — prove the client, THEN run the batch — used to be human discipline, and it is
    the kind that gets skipped exactly when it matters, on the day the key finally arrives. It is
    cheap to enforce instead, because the host and this container share `/opt/airflow/data`.

    The one weakness is a STALE marker: the client changes, the marker from last month stays. So
    when the marker is present its contents are logged rather than silently accepted — the task log
    then says which day the shape was checked and what came back, and a reader can notice that the
    date predates the change they are debugging.
    """
    if not spec.smoke_required:
        return
    marker = spec.smoke_marker
    if not marker.exists():
        raise RuntimeError(
            f"{spec.provider} has never been checked against its live API, so this run is "
            f"refused: an empty result list from a malformed request is indistinguishable from "
            f"'found nothing', and a batch would spend the quota proving nothing.\n"
            f"Run  python scripts/smoke_{spec.provider}_client.py  on the host; it writes "
            f"{marker.name} once the response shape checks out.\n"
            f"Expected at {marker} (./airflow/data/{marker.name} on the host)."
        )
    try:
        recorded = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"{marker} is unreadable ({exc}). Re-run the smoke script.") from exc
    log.info(
        "%s smoke check passed %s: %s hits, response keys %s. If that predates the change you are "
        "debugging, re-run the smoke script before trusting this batch.",
        spec.provider,
        recorded.get("checked_at", "at an unrecorded time"),
        recorded.get("hits", "?"),
        recorded.get("response_keys", "?"),
    )


def require_enabled(spec: ProviderSpec) -> None:
    if os.getenv(spec.enabled_flag, "false").lower() != "true":
        raise RuntimeError(f"{spec.enabled_flag} is not enabled.")
    # Order of complaints, cheapest first: flag off, then key missing, then never proven.
    spec.require_key()
    require_smoke_check(spec)
    require_extract_api_key()


def _load_checkpoint(spec: ProviderSpec) -> dict[str, Any]:
    path = spec.checkpoint_file
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        log.warning("Could not parse %s, starting fresh.", path)
        return {}


def _save_checkpoint(spec: ProviderSpec, last_company_id: str | None, processed: int) -> None:
    path = spec.checkpoint_file
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "last_company_id": last_company_id,
                "processed_count": processed,
                "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _delete_checkpoint(spec: ProviderSpec) -> None:
    if spec.checkpoint_file.exists():
        spec.checkpoint_file.unlink()


def _company_address(company: Any) -> str:
    right = f"{company.postcode or ''} {company.city or ''}".strip()
    return ", ".join(part for part in [company.address or "", right] if part)


def discover(spec: ProviderSpec, company: Any) -> dict[str, Any]:
    """Search, verify, extract. Pure network + CPU — no DB access, no open transaction."""
    queries = build_search_queries(company)
    address = _company_address(company)

    response = spec.search(
        [queries["general"], queries["website"]],
        objective=(
            f"Find the official website and Impressum of {company.company_name}, {address}"
        ),
    )
    kept, dropped = filter_hits(response.hits, company.company_name)

    result: dict[str, Any] = {
        "provider": spec.provider,
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

    # ---- path A: verify candidates through main's Impressum + scorer -----------------------
    for hit in kept[: spec.max_candidates]:
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
        # is_own_site_candidate is the single predicate for that call; see its docstring for the
        # 6-false-positive / 0-true-positive measurement that settled its shape.
        own_site = [
            c
            for c in result["candidates"]
            if is_own_site_candidate(
                c["final_url"], company.company_name, c["score"], AUTO_ACCEPT_SCORE
            )
        ]
        result["best"] = own_site[0] if own_site else None
        result["best_rejected_as_third_party"] = len(result["candidates"]) - len(own_site)

    # ---- path B: the LLM reads the snippets ------------------------------------------------
    search_text = "\n\n---\n\n".join(
        f"SOURCE: {h.title}\nURL: {h.url}\n{h.text}".strip() for h in kept if h.text.strip()
    )
    if search_text.strip():
        try:
            parsed, tokens_in, tokens_out = extract_json(
                EXTRACTION_PROMPT.format(
                    name=company.company_name, address=address, results=search_text
                )
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
        # Brave lands here far more often than Exa: it returns snippets only, no page text.
        result["note"] = "matching_results_had_no_text"
    return result


def write_observations(
    spec: ProviderSpec,
    cur: Any,
    *,
    company: Any,
    source_id: str,
    sync_run_id: str,
    result: dict[str, Any],
) -> bool:
    """Write every observation for one company. Returns True if the source record was new."""
    best = result.get("best")
    best_score = best["score"] if best else None
    website_url = best["final_url"] if best and best["score"] >= 0 else None
    provider = spec.provider

    payload = {
        # "fetched", NOT dump_preview_payload's "planned": the selection query excludes only
        # non-'planned' records, so leaving it planned means the company is never marked done.
        "status": "fetched",
        "source_key": spec.source_key,
        "provider": provider,
        "company_id": company.id,
        "company_name": company.company_name,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "result": result,
    }
    source_record_id, is_created = upsert_source_record(
        cur,
        company_id=company.id,
        source_id=source_id,
        external_id=f"{spec.source_key}:{company.id}",
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
            metadata={"query_type": query_key, "provider": provider},
            confidence=1.0,
        )

    # ---- path A observations, using url_discovery/main.py's confidence ladder --------------
    website_conf: float | None = None
    for candidate in result.get("candidates") or []:
        if candidate["score"] < MIN_STORE_SCORE:
            continue
        # `candidate_url` is an ALIAS for the logical `website` field in
        # pipeline_coordinator.resolver.resolve_website (min conf 0.35), so anything written
        # under that name competes to become the canonical website. `directory_url` is a name
        # the resolver does not consume — queryable evidence, out of contention. The SAME
        # predicate decides this and `best` above; using a looser one here was the bug.
        verdict = own_site_verdict(candidate["final_url"], company.company_name)
        insert_text_observation(
            cur,
            **common,
            field_name="candidate_url" if verdict == "own_site" else "directory_url",
            value_text=candidate["final_url"],
            metadata={
                "provider": provider,
                "score": candidate["score"],
                # Why this URL did not qualify as the company's own site. A directory hit and a
                # name mismatch are different provider failure modes; keep them apart.
                "rejected_as": None if verdict == "own_site" else verdict,
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
            verified_by = "register_number_match"
            extra: dict[str, Any] = {"register_number": extracted.get("register_number")}
        else:
            uniqueness = name_uniqueness(company.company_name)
            website_conf = round(0.35 + 0.30 * uniqueness, 4)
            verified_by = "url_pattern"
            extra = {"name_uniqueness": round(uniqueness, 3)}
        insert_text_observation(
            cur,
            **common,
            field_name="website",
            # The site, not the page search happened to rank. `matched_url` keeps that page.
            value_text=origin_url(best["final_url"]),
            metadata={
                "provider": provider,
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
                        metadata={
                            "provider": provider,
                            "source": "impressum",
                            "imprint_url": best["imprint_url"],
                        },
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
                        metadata={
                            "provider": provider,
                            "source": "impressum",
                            "imprint_url": best["imprint_url"],
                        },
                        confidence=round(field_conf * 0.90, 4),
                    )

    # ---- path B observations: inferred, never verified against the company's own page ------
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
                metadata={"provider": provider, "verified_by": "llm_snippet"},
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
            metadata={"provider": provider, "verified_by": "llm_snippet", "raw_confidence": llm_conf},
            confidence=min(llm_conf if llm_conf is not None else 0.30, LLM_WEBSITE_CONF_CAP),
        )
    return is_created


def run_discovery(spec: ProviderSpec) -> None:
    """The task body. One provider, one batch of companies, observations written per company."""
    checkpoint = _load_checkpoint(spec)
    checkpoint_after_id = checkpoint.get("last_company_id")
    # Without min_quality_score: that branch of the helper mis-binds its parameters on main and
    # raises. This path correctly skips companies already at data_quality_score >= 0.90.
    companies = fetch_companies_for_llm_enrichment(spec.source_key, after_id=checkpoint_after_id)
    if not companies and checkpoint_after_id:
        _delete_checkpoint(spec)
        companies = fetch_companies_for_llm_enrichment(spec.source_key, after_id=None)
    if not companies:
        log.info("No companies pending for %s.", spec.source_key)
        return

    require_enabled(spec)
    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cur:
                source_id = ensure_source(
                    cur,
                    key=spec.source_key,
                    name=spec.source_name,
                    source_type=spec.source_type,
                    metadata={"dag_id": spec.source_key, "provider": spec.provider},
                )
                sync_run_id = start_sync_run(cur, source_id=source_id)

        created = updated = failed = processed = 0
        tokens_in = tokens_out = 0
        cost_usd = 0.0
        stopped_early = False
        last_completed_company_id = checkpoint_after_id

        for index, company in enumerate(companies, start=1):
            # Network first, outside any transaction: never hold one open across a search.
            try:
                result = discover(spec, company)
            except (*spec.transient_errors, ExtractRateLimitError) as exc:
                failed += 1
                stopped_early = True
                # Checkpoint the last COMPLETED company so this one is retried, not skipped.
                _save_checkpoint(spec, last_completed_company_id, processed)
                log.warning(
                    "Stopping %s at %s (%s).", spec.source_key, company.id, type(exc).__name__
                )
                break
            except Exception:
                failed += 1
                processed += 1
                log.exception("Discovery failed for company %s", company.id)
                last_completed_company_id = company.id
                _save_checkpoint(spec, last_completed_company_id, processed)
                continue

            try:
                with connection:
                    with connection.cursor() as cur:
                        is_created = write_observations(
                            spec,
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
                cost_usd += result.get("provider_cost_usd") or 0.0
                last_completed_company_id = company.id
            except Exception:
                failed += 1
                processed += 1
                log.exception("Write failed for company %s", company.id)

            _save_checkpoint(spec, last_completed_company_id, processed)
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
            if index % spec.progress_log_every == 0:
                log.info(
                    "%s progress: %d/%d (%d created, %d updated, %d failed, %d/%d tokens)",
                    spec.source_key, index, len(companies), created, updated, failed,
                    tokens_in, tokens_out,
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
            "%s finished: %d processed, %d created, %d updated, %d failed, "
            "%d in / %d out tokens, provider cost $%.4f",
            spec.source_key, processed, created, updated, failed, tokens_in, tokens_out, cost_usd,
        )
        if not stopped_early:
            _delete_checkpoint(spec)
    finally:
        connection.close()


def make_dag(spec: ProviderSpec, callable_: Callable[[], None]):  # noqa: ANN201
    """Build the DAG. Returns None outside Airflow so the module stays importable in scripts."""
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None

    with DAG(
        dag_id=spec.source_key,
        description=spec.dag_description or f"{spec.source_name}, verified against the Impressum",
        schedule=None,
        start_date=datetime(2026, 8, 6, tzinfo=timezone.utc),
        catchup=False,
        tags=list(spec.dag_tags) or ["sources", spec.provider, "search", "discovery"],
    ) as dag:
        discover_task = PythonOperator(
            task_id=f"run_{spec.source_key}",
            python_callable=callable_,
        )
        trigger_coordinator = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator",
            trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        discover_task >> trigger_coordinator
    return dag
