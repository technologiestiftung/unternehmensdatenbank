"""Turn one company's combined-discovery result into rows in the database.

Everything in this file is a write. The rest of the package deliberately writes nothing, so
that the same code can be measured on a sample without changing any state; this is the one
place that decides what a result looks like once stored.

Three vocabulary notes, because these words are invented by this codebase:

- An **observation** is one claim by one source about one field of one company — for example
  "`website` = `https://example.de`, and I am 0.71 sure". Sources never overwrite each other;
  they each add observations, and a separate DAG (`pipeline_coordinator`) later decides which
  observation wins.
- **Confidence** is the number between 0 and 1 that comes out of `confidence.py`. Only numbers
  at or above 0.35 can ever become a company's official website.
- A **source record** is the one row per company that says "this source looked at this company",
  with the raw detail attached as JSON.

## The field names, and why the split matters

A URL goes into one of three fields, and picking the wrong one causes the recurring bug in this
repo:

- `website` — we stand behind this. Confidence 0.35 or above.
- `candidate_url` — could be the company's own site, but the number is too low to promote.
- `directory_url` — this is a page *about* the company (a business directory, a registry, an
  insolvency tracker), not the company's own site.

`pipeline_coordinator` treats `candidate_url` as another name for `website` at a minimum
confidence of 0.35. It ignores `directory_url` entirely. So a directory page filed as a
`candidate_url` with a high enough number becomes a company's official website — which has
happened here before. `confidence.score` already refuses those URLs outright, giving them 0.0,
and this file files them under `directory_url` as well. Two independent guards, on purpose.

## Two rows every company gets, whatever happened

- `search_query` — an "we looked at this company" marker. `v_discovery_results.processed_by`
  and `v_discovery_scorecard.companies_tried` are both built by counting these, so a source
  that does not write one is invisible in the platform's own reporting. `url_discovery` does
  not write one, which is why it is missing from the scorecard despite having found more
  websites than any other source.
- `discovery_tier`, three of them, one per method — what each method did. The value is the
  method name; the metadata says whether it ran. Without these, "we asked OpenStreetMap and it
  had nothing" and "we never asked OpenStreetMap" look identical in the database, and the
  second one silently reads as evidence that a company has no website.

## What this does NOT write

`pipeline.discover` returns URLs and the evidence behind them. It does not return the phone
number, e-mail address or owner name off the Impressum, so this file cannot store them. The
older `url_discovery` DAG does store those, so the combined DAG is not yet a replacement for
it — it is a better answer to "what is this company's website" and no answer at all to "who
runs it".
"""
from __future__ import annotations

import logging
from typing import Any

from source_enrichment_common import (
    build_search_queries,
    insert_text_observation,
    now_iso,
    upsert_source_record,
)

#: The bar `pipeline_coordinator` applies before a website observation may become a company's
#: official website. Imported, never re-typed: two copies of a threshold drift apart.
from combined_discovery.confidence import WEBSITE_MIN_CONFIDENCE

log = logging.getLogger(__name__)

SOURCE_KEY = "combined_discovery"

#: Evidence names that mean "this is a page about the company, not the company's page".
_THIRD_PARTY_EVIDENCE = frozenset({"directory_host", "host_name_mismatch"})

#: Method name in a result -> the stage key used in the tier ledger.
_METHOD_TO_STAGE = {"url_pattern": "url", "osm": "osm", "local_llm": "llm"}

#: The three methods, and the name each one gets in the database.
TIER_NAMES = (("url", "url_pattern"), ("osm", "osm"), ("llm", "ollama_searxng"))


def field_name_for(answer: dict[str, Any]) -> str:
    """Which of the three URL fields this answer belongs in. See the header for why."""
    if set(answer.get("evidence") or []) & _THIRD_PARTY_EVIDENCE:
        return "directory_url"
    if (answer.get("confidence") or 0.0) >= WEBSITE_MIN_CONFIDENCE:
        return "website"
    return "candidate_url"


def _tier_metadata(
    result: dict[str, Any], stage: str, *, stages_run: set[str], skip_reason: str | None
) -> dict[str, Any]:
    """What one method did for one company, as stored metadata.

    `status` is one of:
      tried        the method ran
      not_reached  the method did not run, and `reason` says why
    `outcome`, only when it ran:
      found        it proposed at least one URL
      nothing      it ran and proposed nothing
      error        it raised; `error` carries the message
    """
    if stage not in stages_run:
        return {"status": "not_reached", "reason": skip_reason or "not_requested"}

    method = {"url": "url_pattern", "osm": "osm", "llm": "local_llm"}[stage]
    proposed = any(
        method in (answer.get("methods") or []) for answer in result.get("answers") or []
    )
    prefix = {"url": "url_patterns:", "osm": "osm:", "llm": "local_llm:"}[stage]
    error = next((e for e in result.get("errors") or [] if e.startswith(prefix)), None)
    if error:
        return {"status": "tried", "outcome": "error", "error": error}
    return {"status": "tried", "outcome": "found" if proposed else "nothing"}


def build_payload(
    company: Any,
    result: dict[str, Any],
    *,
    stages_run: set[str],
    skip_reasons: dict[str, str] | None = None,
) -> dict[str, Any]:
    """The raw JSON kept on the source record.

    `status` is `"fetched"`, never `"planned"`. The query that picks the next batch of companies
    skips a company only when its source record's status is something other than `"planned"`, so
    a source that leaves the status at `"planned"` re-processes the same companies on every run
    and never advances. Two DAGs in this repo do exactly that today.
    """
    skip_reasons = skip_reasons or {}
    return {
        "status": "fetched",
        "source_key": SOURCE_KEY,
        "company_id": company.id,
        "company_name": company.company_name,
        "generated_at": now_iso(),
        "tiers": {
            name: _tier_metadata(
                result, stage, stages_run=stages_run, skip_reason=skip_reasons.get(stage)
            )
            for stage, name in TIER_NAMES
        },
        "website": result.get("website"),
        "confidence": result.get("confidence_value"),
        "band": result.get("band"),
        "verified_by": result.get("verified_by"),
        "why": result.get("why"),
        "candidates_generated": result.get("candidates_generated"),
        "candidates_resolved": result.get("candidates_resolved"),
        "candidates_probed": result.get("candidates_probed"),
        "osm_objects_seen": result.get("osm_objects_seen"),
        "osm_id": result.get("osm_id"),
        "osm_match_score": result.get("osm_match_score"),
        "llm_website": result.get("llm_website"),
        "answers": result.get("answers") or [],
        "elapsed_s": result.get("elapsed_s"),
        "errors": result.get("errors") or [],
    }


def write_company_result(
    cur: Any,
    *,
    company: Any,
    result: dict[str, Any],
    source_id: str,
    sync_run_id: str,
    stages_run: set[str],
    skip_reasons: dict[str, str] | None = None,
) -> bool:
    """Store one company's result. Returns True when the source record was newly created.

    `result` is `dataclasses.asdict(pipeline.discover(...))`, so this function works the same on
    a live run and on a cached row read back from a measurement file.
    """
    payload = build_payload(company, result, stages_run=stages_run, skip_reasons=skip_reasons)
    website = result.get("website")
    confidence = result.get("confidence_value") or 0.0

    source_record_id, was_created = upsert_source_record(
        cur,
        company_id=company.id,
        source_id=source_id,
        external_id=f"{SOURCE_KEY}:{company.id}",
        # Only a URL we would stand behind goes on the record itself.
        external_url=website if confidence >= WEBSITE_MIN_CONFIDENCE else None,
        source_company_name=company.company_name,
        source_payload=payload,
    )

    # 1. The "we looked" marker. Written first, and unconditionally.
    insert_text_observation(
        cur,
        company_id=company.id,
        source_record_id=source_record_id,
        source_id=source_id,
        sync_run_id=sync_run_id,
        field_name="search_query",
        value_text=build_search_queries(company)["website"],
        metadata={
            "provider": SOURCE_KEY,
            "note": (
                "attempt marker. Two of the three methods do not use a search engine at all; "
                "this row exists so the company counts as looked-at in the platform's views."
            ),
        },
        confidence=1.0,
    )

    # 2. One row per method, so "never ran" is a value and not a missing row.
    for stage, name in TIER_NAMES:
        insert_text_observation(
            cur,
            company_id=company.id,
            source_record_id=source_record_id,
            source_id=source_id,
            sync_run_id=sync_run_id,
            field_name="discovery_tier",
            value_text=name,
            metadata=payload["tiers"][name],
            confidence=1.0,
        )

    # 3. Every URL any method proposed, filed by how much we trust it.
    for answer in result.get("answers") or []:
        url = answer.get("url")
        if not url:
            continue
        insert_text_observation(
            cur,
            company_id=company.id,
            source_record_id=source_record_id,
            source_id=source_id,
            sync_run_id=sync_run_id,
            field_name=field_name_for(answer),
            value_text=url,
            metadata={
                "provider": SOURCE_KEY,
                "tier": "+".join(
                    _METHOD_TO_STAGE.get(m, m) for m in (answer.get("methods") or [])
                ),
                "methods": answer.get("methods") or [],
                "verified_by": answer.get("verified_by"),
                "band": answer.get("band"),
                # `sorted`, not passed through. `pipeline.UrlAnswer.evidence` is a `set`, and
                # `psycopg2.extras.Json` cannot serialise one — so the contract this file's header
                # claims, `dataclasses.asdict(pipeline.discover(...))`, would raise here. The
                # normalised answers in `results.jsonl` already hold sorted lists
                # (`pipeline.py:815`), which is why feeding it the file is the tested path. Sorting
                # defensively costs nothing and makes both callers safe.
                "evidence": sorted(answer.get("evidence") or []),
                # Which methods actually ran for this company, so a reader can tell "OpenStreetMap
                # had nothing" from "OpenStreetMap was never asked" without joining to the tier
                # rows. Same reason those rows exist; this is the per-claim copy.
                "stages_run": sorted(stages_run),
                "why": answer.get("why"),
                "pattern": answer.get("pattern"),
                "matched_url": answer.get("imprint_url") or url,
                "imprint_url": answer.get("imprint_url"),
                "register_number_seen": answer.get("register_number_seen"),
                "is_winner": bool(website) and url == website,
            },
            confidence=answer.get("confidence") or 0.0,
        )

    # 4. The OpenStreetMap object, when one matched. Useful on its own: it is a stable handle
    #    for the business on the map, whether or not the map knew a website.
    if result.get("osm_id"):
        insert_text_observation(
            cur,
            company_id=company.id,
            source_record_id=source_record_id,
            source_id=source_id,
            sync_run_id=sync_run_id,
            field_name="osm_id",
            value_text=result["osm_id"],
            metadata={
                "provider": SOURCE_KEY,
                "match_score": result.get("osm_match_score"),
                "osm_website": result.get("osm_website"),
                "discovered_via": "overpass_api",
            },
            confidence=result.get("osm_match_score"),
        )

    return was_created
