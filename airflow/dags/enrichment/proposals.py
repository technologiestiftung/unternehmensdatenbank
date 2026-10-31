"""Websites another source proposes for a company, verified like a guessed domain.

Shared by `wikidata` and `common_crawl`. Each match is a source record whose payload lists the
proposed `websites`. `verify_batch` checks them with `pipeline.verify_proposed`, the same page
reading and `confidence.score` as a guessed domain, and files every answer as an observation of
that source. The page, not the match, proves a website.

A website drops to confidence 0 only when no record of the source for that company still proves it
(`proven_urls`): its page stopped naming the company, or the record now names another company.
Nothing is deleted. Register officers are loaded per batch as comparison keys, in memory only.
"""
from __future__ import annotations

import dataclasses
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from psycopg2.extras import Json

from enrichment import name_frequency, pipeline
from enrichment.observations import field_name_for
from enrichment.officers import load_officers
from source_enrichment_common import (
    CompanyCandidate,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
)

log = logging.getLogger(__name__)

URL_FIELDS = ["website", "candidate_url", "directory_url"]


def unproven(observations: list[tuple[str, str, str]], proven: set[tuple[str, str]],
             written: frozenset[str] = frozenset()) -> list[str]:
    """Ids of (id, company_id, url) observations to drop to 0: not just written, and no record of
    their company lists their URL as proven."""
    return [obs_id for obs_id, company_id, url in observations
            if obs_id not in written and (company_id, url) not in proven]


def demote_unproven(cur: Any, *, source_id: str, company_ids: list[str],
                    written: frozenset[str] = frozenset()) -> int:
    """Set these companies' URLs from this source to 0, except those just written and those a record
    of theirs still proves. Returns how many it demoted."""
    if not company_ids:
        return 0
    cur.execute(
        """
        SELECT id::text, company_id::text, value_text FROM public.company_field_observations
        WHERE source_id = %s AND company_id = ANY(%s::uuid[]) AND field_name = ANY(%s) AND confidence > 0
        """,
        (source_id, company_ids, URL_FIELDS),
    )
    observations = cur.fetchall()
    cur.execute(
        """
        SELECT company_id::text, jsonb_array_elements_text(source_payload -> 'proven_urls')
        FROM public.company_source_records
        WHERE source_id = %s AND company_id = ANY(%s::uuid[])
          AND jsonb_typeof(source_payload -> 'proven_urls') = 'array'
        """,
        (source_id, company_ids),
    )
    demote = unproven(observations, set(cur.fetchall()), written)
    if demote:
        cur.execute("UPDATE public.company_field_observations SET confidence = 0, updated_at = now()"
                    " WHERE id = ANY(%s::uuid[])", (demote,))
    return len(demote)


def verify_batch(*, source_id_of: Callable[[Any], str], provider: str, method: str, batch_size: int,
                 recheck_after_days: int, workers: int,
                 metadata_of: Callable[[dict[str, Any]], dict[str, Any]] = lambda payload: {}) -> int:
    """Verify one batch of the source's records not verified in `recheck_after_days`. Returns how
    many it checked: 0 skips the coordinator, a full batch starts the next run.

    `metadata_of(payload)` adds the source's own keys (Wikidata's `qid`) to each observation.
    """
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = source_id_of(cur)
            cur.execute(
                """
                SELECT r.id, r.company_id::text, r.source_payload
                FROM public.company_source_records r
                WHERE r.source_id = %s
                  AND (r.source_payload ->> 'verified_at' IS NULL
                       OR (r.source_payload ->> 'verified_at')::timestamptz < now() - %s * interval '1 day')
                ORDER BY r.source_payload ->> 'verified_at' NULLS FIRST, r.id
                LIMIT %s
                """,
                (source_id, recheck_after_days, batch_size),
            )
            records = cur.fetchall()
            if not records:
                log.info("Every %s proposal was verified in the last %d days.", provider, recheck_after_days)
                return 0
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = _companies(cur, [company_id for _, company_id, _ in records])
            officers = load_officers(cur, list(companies))
            cur.execute("SELECT company_name FROM public.v_companies_enriched")
            common = name_frequency.common_words([row[0] for row in cur.fetchall()])

        proven = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            jobs = {}
            for record_id, company_id, payload in records:
                company = dataclasses.replace(companies[company_id], officers=officers.get(company_id, frozenset()))
                jobs[pool.submit(pipeline.verify_proposed, company, payload["websites"],
                                 method=method, common=common)] = (record_id, company_id, payload)
            for job in as_completed(jobs):
                record_id, company_id, payload = jobs[job]
                try:
                    answers = job.result()
                except Exception:  # noqa: BLE001  one bad page must not discard the batch
                    log.exception("Verifying %s websites failed for %s; retried later", provider, company_id)
                    continue
                proven += any(field_name_for(a) == "website" for a in answers)
                with connection, connection.cursor() as cur:
                    write_answers(cur, company_id=company_id, record_id=record_id, source_id=source_id,
                                  sync_run_id=sync_run_id, provider=provider, extra=metadata_of(payload),
                                  answers=answers)

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(records), records_created=proven,
                            records_updated=0, records_failed=0)
        log.info("Verified %d %s proposals: %d websites proven.", len(records), provider, proven)
        return len(records)
    finally:
        connection.close()


def _companies(cur: Any, company_ids: list[str]) -> dict[str, CompanyCandidate]:
    cur.execute(
        """
        SELECT id::text, company_name, address, postcode, city, registration_number, legal_form
        FROM public.v_companies_enriched WHERE id = ANY(%s::uuid[])
        """,
        (company_ids,),
    )
    return {row[0]: CompanyCandidate(*row) for row in cur.fetchall()}


def write_answers(cur: Any, *, company_id: str, record_id: str, source_id: str, sync_run_id: str,
                  provider: str, answers: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> None:
    """Every verified URL as an observation, filed like discovery's; earlier ones no record of this
    company proves any more drop to 0. Only a URL filed as `website` counts as proven."""
    written = set()
    for answer in answers:
        written.add(str(insert_text_observation(
            cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
            sync_run_id=sync_run_id, field_name=field_name_for(answer), value_text=answer["url"],
            confidence=answer["confidence"],
            metadata={
                "provider": provider, **(extra or {}), "methods": answer["methods"],
                "verified_by": answer["verified_by"], "band": answer["band"],
                "evidence": sorted(answer["evidence"]), "why": answer["why"],
                "matched_url": answer["imprint_url"] or answer["url"], "imprint_url": answer["imprint_url"],
                "register_number_seen": answer["register_number_seen"],
            },
        )))
    cur.execute(
        "UPDATE public.company_source_records SET source_payload = source_payload || jsonb_build_object("
        "'verified_at', now(), 'proven_urls', %s), last_synced_at = now() WHERE id = %s",
        (Json(sorted({a["url"] for a in answers if field_name_for(a) == "website"})), record_id),
    )
    demote_unproven(cur, source_id=source_id, company_ids=[company_id], written=frozenset(written))
