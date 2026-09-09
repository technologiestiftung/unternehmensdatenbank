from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import Json


log = logging.getLogger(__name__)

PG_DSN = os.getenv(
    "PG_DSN",
    "host=postgres port=5432 dbname=unternehmensdatenbank user=app password=app",
)
SOURCE_ENRICHMENT_BATCH_SIZE = int(os.getenv("SOURCE_ENRICHMENT_BATCH_SIZE", "50"))
SOURCE_ENRICHMENT_SELECTION_WINDOW = int(os.getenv("SOURCE_ENRICHMENT_SELECTION_WINDOW", "250"))

#: Skip companies the register says no longer exist. Off by default, and the default is the whole
#: point of the flag: `companies.register_status` is `erloschen` for 7,673 of the 14,443 Osnabrück
#: rows (53.1%) and `in Liquidation` for 472 more, so switching it on removes 56.4% of the work — and
#: with it 36 of the 902 websites ever discovered, 4.0%. That trade is good but it is a CHANGE, and
#: every number published in `docs/` was measured over the unfiltered table. Turn it on, measure once,
#: write the new baseline down, and only then argue about the default.
#:
#: `aktiv` alone counts as active: a company in liquidation is winding down, and its 7 websites are
#: not worth 472 companies of work. Decided 2026-08-27.
ACTIVE_ONLY = os.getenv("DISCOVERY_ACTIVE_ONLY", "").strip().lower() in {"1", "true", "yes"}

#: A whole SQL fragment, interpolated into the candidate queries below — **NOT** a bound parameter,
#: and it must not be turned into one. It is either the empty string or this one fixed literal; the
#: only input is an environment variable choosing between them, and no value from a row, a request or
#: a user reaches it. A `%s` placeholder cannot be empty and cannot carry `AND <col> = <val>`, so
#: binding it would either fail to parse or silently filter on something else. The real arguments in
#: those queries stay `%s`, as they must.
_ACTIVE_ONLY_SQL = "AND c.register_status = 'aktiv'" if ACTIVE_ONLY else ""

#: Skip companies that already HAVE a resolved website. Off by default, same reasoning as above:
#: it changes which companies every published number was measured over.
#:
#: Why it is needed at all. The candidate queries already skip a company THIS source has seen, and
#: skip anything at `data_quality_score >= 0.90`. Neither catches the case that costs money: on
#: 2026-09-01, importing `combined_discovery` took the active companies with a website from 155 to
#: 949, and their mean `data_quality_score` was **0.340** — nowhere near the 0.90 gate. So a paid
#: provider would have re-searched all 949, 15.1% of the 6,298 active companies, to rediscover
#: URLs a DNS lookup had already found. Tradineo GmbH is the worked example: url_pattern had
#: `tradineo.com` at 0.99 with a register-number match, and Brave paid to find it again.
#:
#: It asks `company_canonical_field_sources` rather than counting observations, and the difference
#: is load-bearing: an observation is a CLAIM, and a company can hold half a dozen rejected ones.
#: Only a canonical row means the resolver actually settled on a website for this company.
#:
#: **Two ways a website can already be known, and the flag now covers both.** The second is
#: `northdata_export.website` — 3,283 URLs the register export shipped. Once
#: `import_northdata_websites.py` has run and `pipeline_coordinator` has resolved them, the
#: canonical check above already covers every one of those companies and the export predicate skips
#: nobody: measured 2026-09-03, the two together leave the same **3,109** active companies as the
#: canonical check alone. Its whole value is the window before that — a Berlin export sitting in the
#: table unimported, where a paid provider would bill for URLs already on disk. The export predicate
#: alone is strictly WORSE than the canonical one (3,271 companies, because 228 canonical websites
#: came from discovery and are in no export), so it is an addition and never a replacement.
#:
#: Leave it OFF while measuring which provider is best on equal terms, and turn it ON for
#: production runs, where re-finding a known website is pure cost.
SKIP_RESOLVED_WEBSITE = os.getenv("DISCOVERY_SKIP_RESOLVED_WEBSITE", "").strip().lower() in {
    "1",
    "true",
    "yes",
}

#: Interpolated, not bound — see the note on `_ACTIVE_ONLY_SQL`, which applies here word for word.
#:
#: **`NOT EXISTS`, and it may never become `NOT IN`.** `northdata_export.company_id` is nullable and
#: 13 rows are NULL today — export rows that matched no company. `x NOT IN (SELECT col …)` evaluates
#: to UNKNOWN for every row the moment that subquery yields a single NULL, so the predicate would
#: return **zero companies**, in all six candidate queries at once, with nothing logged. The
#: `website IS NOT NULL` filter excludes all 13 today, so `NOT IN` would work here and then break on
#: the first export matched by a different key. Same silent-total-filter failure that
#: `run_combined_discovery.load_sample` raises `SystemExit` over for `DISCOVERY_ACTIVE_ONLY`;
#: `NOT EXISTS` is simply immune to it.
_SKIP_RESOLVED_WEBSITE_SQL = (
    """AND NOT EXISTS (
                    SELECT 1 FROM public.company_canonical_field_sources ccfs
                    WHERE ccfs.company_id = c.id AND ccfs.field_name = 'website'
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM public.northdata_export nde
                    WHERE nde.company_id = c.id AND nde.website IS NOT NULL
                  )"""
    if SKIP_RESOLVED_WEBSITE
    else ""
)
LEGAL_SUFFIX_RE = re.compile(
    r"\b(gmbh(?:\s*&\s*co\.\s*kg)?|ug|ag|eg|kg|ohg|gbr|se|kgaa|mbh|e\. ?v\.?)\b",
    re.IGNORECASE,
)
LOW_SIGNAL_KEYWORDS = (
    "vermögensverwaltungs",
    "beteiligungs",
    "holding",
    "grundbesitz",
    "verwaltung eigenen vermögens",
    "verwaltung eigener vermögenswerte",
)
HIGH_SIGNAL_KEYWORDS = (
    "software",
    "technik",
    "technologie",
    "engineering",
    "ingenieur",
    "reise",
    "tourismus",
    "bau",
    "immobil",
    "apotheke",
    "handel",
    "dienst",
    "marketing",
    "restaurant",
    "hotel",
    "logistik",
)


@dataclass
class CompanyCandidate:
    id: str
    company_name: str
    address: str | None
    postcode: str | None
    city: str | None
    registration_number: str | None
    legal_form: str | None = None
    wz_code: str | None = None
    business_purpose: str | None = None
    branch_description: str | None = None


@dataclass
class CompanyUrlCandidate(CompanyCandidate):
    source_url: str = ""


def get_connection() -> Any:
    return psycopg2.connect(PG_DSN)


def fetch_companies_without_source(source_key: str, limit: int = SOURCE_ENRICHMENT_BATCH_SIZE) -> list[CompanyCandidate]:
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            # `_ACTIVE_ONLY_SQL` is interpolated, not bound: it is a fixed SQL fragment chosen by an
            # env var, never a value, and a `%s` placeholder can be neither empty nor a predicate.
            # See its definition at the top of this module before "fixing" this.
            cur.execute(
                f"""
                SELECT
                  c.id,
                  c.company_name,
                  c.address,
                  c.postcode,
                  c.city,
                  c.registration_number,
                  c.legal_form,
                  c.wz_code,
                  c.business_purpose,
                  c.metadata ->> 'branch_description' as branch_description
                FROM public.companies c
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM public.company_source_records csr
                  JOIN public.sources s ON s.id = csr.source_id
                  WHERE csr.company_id = c.id
                    AND s.key = %s
                    AND COALESCE(csr.source_payload ->> 'status', '') <> 'planned'
                )
                {_ACTIVE_ONLY_SQL}
                {_SKIP_RESOLVED_WEBSITE_SQL}
                ORDER BY c.updated_at DESC, c.company_name ASC
                LIMIT %s
                """,
                (source_key, limit * SOURCE_ENRICHMENT_SELECTION_WINDOW),
            )
            return _rank_company_candidates([CompanyCandidate(*row) for row in cur.fetchall()], limit)
    finally:
        connection.close()


def fetch_companies_without_source_after_id(
    source_key: str,
    *,
    after_id: str | None = None,
    limit: int = SOURCE_ENRICHMENT_BATCH_SIZE,
) -> list[CompanyCandidate]:
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            # Both branches interpolate `_ACTIVE_ONLY_SQL` rather than binding it — see the note at
            # its definition; a bound parameter cannot be an empty string or a predicate.
            if after_id:
                cur.execute(
                    f"""
                    SELECT
                      c.id,
                      c.company_name,
                      c.address,
                      c.postcode,
                      c.city,
                      c.registration_number,
                      c.legal_form,
                      c.wz_code,
                      c.business_purpose,
                      c.metadata ->> 'branch_description' as branch_description
                    FROM public.companies c
                    WHERE c.id > %s
                      AND NOT EXISTS (
                        SELECT 1
                        FROM public.company_source_records csr
                        JOIN public.sources s ON s.id = csr.source_id
                        WHERE csr.company_id = c.id
                          AND s.key = %s
                          AND COALESCE(csr.source_payload ->> 'status', '') <> 'planned'
                      )
                      {_ACTIVE_ONLY_SQL}
                      {_SKIP_RESOLVED_WEBSITE_SQL}
                    ORDER BY c.id ASC
                    LIMIT %s
                    """,
                    (after_id, source_key, limit * SOURCE_ENRICHMENT_SELECTION_WINDOW),
                )
            else:
                cur.execute(
                    f"""
                    SELECT
                      c.id,
                      c.company_name,
                      c.address,
                      c.postcode,
                      c.city,
                      c.registration_number,
                      c.legal_form,
                      c.wz_code,
                      c.business_purpose,
                      c.metadata ->> 'branch_description' as branch_description
                    FROM public.companies c
                    WHERE NOT EXISTS (
                      SELECT 1
                      FROM public.company_source_records csr
                      JOIN public.sources s ON s.id = csr.source_id
                      WHERE csr.company_id = c.id
                        AND s.key = %s
                        AND COALESCE(csr.source_payload ->> 'status', '') <> 'planned'
                    )
                    {_ACTIVE_ONLY_SQL}
                    {_SKIP_RESOLVED_WEBSITE_SQL}
                    ORDER BY c.id ASC
                    LIMIT %s
                    """,
                    (source_key, limit * SOURCE_ENRICHMENT_SELECTION_WINDOW),
                )
            return _rank_company_candidates([CompanyCandidate(*row) for row in cur.fetchall()], limit)
    finally:
        connection.close()


def _score_company_candidate(company: CompanyCandidate) -> tuple[int, str, str]:
    score = 0
    searchable_text = " ".join(
        part for part in [company.company_name, company.business_purpose, company.branch_description] if part
    ).lower()

    if company.address and company.postcode and company.city:
        score += 3
    if company.registration_number:
        score += 2
    if company.wz_code:
        score += 2
    if company.business_purpose and len(company.business_purpose) >= 30:
        score += 3
    if company.branch_description:
        score += 2

    if company.legal_form and company.legal_form.lower() not in {"e. v.", "e.v.", "egbr", "gbr"}:
        score += 1

    if any(keyword in searchable_text for keyword in HIGH_SIGNAL_KEYWORDS):
        score += 4
    if any(keyword in searchable_text for keyword in LOW_SIGNAL_KEYWORDS):
        score -= 5

    return score, company.company_name.casefold(), company.id


def _rank_company_candidates(companies: list[CompanyCandidate], limit: int) -> list[CompanyCandidate]:
    ranked = sorted(companies, key=_score_company_candidate, reverse=True)
    return ranked[:limit]


def ensure_source(cur: Any, *, key: str, name: str, source_type: str, metadata: dict[str, Any]) -> str:
    cur.execute(
        """
        INSERT INTO public.sources (
          key,
          name,
          source_type,
          metadata,
          created_at,
          updated_at
        )
        VALUES (%s, %s, %s, %s, now(), now())
        ON CONFLICT (key)
        DO UPDATE SET
          name = EXCLUDED.name,
          source_type = EXCLUDED.source_type,
          metadata = public.sources.metadata || EXCLUDED.metadata,
          updated_at = now()
        RETURNING id
        """,
        (key, name, source_type, Json(metadata)),
    )
    return cur.fetchone()[0]


def delete_planned_source_records(cur: Any, *, company_id: str, source_key: str) -> None:
    cur.execute(
        """
        DELETE FROM public.company_source_records csr
        USING public.sources s
        WHERE csr.source_id = s.id
          AND csr.company_id = %s
          AND s.key = %s
          AND COALESCE(csr.source_payload ->> 'status', '') = 'planned'
        """,
        (company_id, source_key),
    )


def start_sync_run(cur: Any, *, source_id: str, sync_type: str = "manual") -> str:
    cur.execute(
        """
        INSERT INTO public.source_sync_runs (
          source_id,
          sync_type,
          status,
          started_at,
          created_at,
          updated_at
        )
        VALUES (%s, %s, 'running', now(), now(), now())
        RETURNING id
        """,
        (source_id, sync_type),
    )
    return cur.fetchone()[0]


def finish_sync_run(
    cur: Any,
    *,
    sync_run_id: str,
    records_seen: int,
    records_created: int,
    records_updated: int,
    records_failed: int,
    status: str | None = None,
) -> None:
    final_status = status or ("success" if records_failed == 0 else "partial")
    cur.execute(
        """
        UPDATE public.source_sync_runs
        SET
          status = %s,
          finished_at = now(),
          records_seen = %s,
          records_created = %s,
          records_updated = %s,
          records_failed = %s,
          updated_at = now()
        WHERE id = %s
        """,
        (
            final_status,
            records_seen,
            records_created,
            records_updated,
            records_failed,
            sync_run_id,
        ),
    )


def upsert_source_record(
    cur: Any,
    *,
    company_id: str,
    source_id: str,
    external_id: str,
    external_url: str | None,
    source_company_name: str,
    source_payload: dict[str, Any],
) -> tuple[str, bool]:
    cur.execute(
        """
        INSERT INTO public.company_source_records (
          company_id,
          source_id,
          external_id,
          external_url,
          source_company_name,
          source_payload,
          first_seen_at,
          last_seen_at,
          last_synced_at,
          is_deleted_in_source,
          is_active,
          created_at,
          updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, now(), now(), now(), false, true, now(), now())
        ON CONFLICT (source_id, external_id)
        DO UPDATE SET
          company_id = EXCLUDED.company_id,
          external_url = EXCLUDED.external_url,
          source_company_name = EXCLUDED.source_company_name,
          source_payload = EXCLUDED.source_payload,
          last_seen_at = now(),
          last_synced_at = now(),
          is_deleted_in_source = false,
          is_active = true,
          updated_at = now()
        RETURNING id, xmax = 0
        """,
        (
            company_id,
            source_id,
            external_id,
            external_url,
            source_company_name,
            Json(source_payload),
        ),
    )
    row = cur.fetchone()
    return row[0], bool(row[1])


def insert_text_observation(
    cur: Any,
    *,
    company_id: str,
    source_record_id: str,
    source_id: str,
    sync_run_id: str,
    field_name: str,
    value_text: str,
    metadata: dict[str, Any] | None = None,
    confidence: float | None = None,
) -> str:
    """Write one claim by one source about one text field of one company. Returns the row's id.

    An append for every field except `website` and `candidate_url`, which V17 gave a unique key so a
    re-run can update rather than write a second copy. Without that, a source re-writing what it
    already wrote had to DELETE first, and `company_canonical_field_sources` cascades off these rows —
    which un-published 558 websites for four days on 2026-09-03.

    **Newest write wins**, unconditionally. Not highest-confidence-wins: a re-score has to be able to
    DEMOTE, and this repo has already been bitten by a change that could only ever promote
    (`scripts/replay_judge_gate.py` exists because of it). A conditional `DO UPDATE ... WHERE` would
    also return no row when it declined to fire, and the `RETURNING id` below would then raise.

    Two things it deliberately does not refresh on conflict: `observed_at` and `source_sync_run_id`
    keep the values from the first write, so a row updated by a later run still points at the run that
    created it. Add them to the SET list if run attribution ever matters more than the original
    timestamp.

    The `WHERE` in the conflict target is not optional — it is how Postgres infers a *partial* index.
    A row in any other field satisfies no predicate, finds no arbiter index, and inserts as before.
    """
    cur.execute(
        """
        INSERT INTO public.company_field_observations (
          company_id,
          company_source_record_id,
          source_id,
          source_sync_run_id,
          field_name,
          value_text,
          observed_at,
          confidence,
          metadata,
          created_at,
          updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s, now(), now())
        ON CONFLICT (company_id, field_name, source_id, value_text)
          WHERE field_name IN ('website', 'candidate_url')
        DO UPDATE SET
          confidence = EXCLUDED.confidence,
          metadata   = EXCLUDED.metadata,
          updated_at = now()
        RETURNING id
        """,
        (
            company_id,
            source_record_id,
            source_id,
            sync_run_id,
            field_name,
            value_text,
            confidence,
            Json(metadata or {}),
        ),
    )
    return cur.fetchone()[0]


def insert_number_observation(
    cur: Any,
    *,
    company_id: str,
    source_record_id: str,
    source_id: str,
    sync_run_id: str,
    field_name: str,
    value_number: float,
    metadata: dict[str, Any] | None = None,
    confidence: float | None = None,
) -> str:
    cur.execute(
        """
        INSERT INTO public.company_field_observations (
          company_id,
          company_source_record_id,
          source_id,
          source_sync_run_id,
          field_name,
          value_number,
          observed_at,
          confidence,
          metadata,
          created_at,
          updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s, now(), now())
        RETURNING id
        """,
        (
            company_id,
            source_record_id,
            source_id,
            sync_run_id,
            field_name,
            value_number,
            confidence,
            Json(metadata or {}),
        ),
    )
    return cur.fetchone()[0]


def normalize_company_name_for_search(value: str) -> str:
    simplified = value.lower().replace("&", " und ")
    simplified = LEGAL_SUFFIX_RE.sub(" ", simplified)
    simplified = re.sub(r"[^0-9a-zäöüß]+", " ", simplified, flags=re.IGNORECASE)
    simplified = re.sub(r"\s+", " ", simplified).strip()
    return simplified


def fetch_companies_with_latest_text_observation(
    observation_field_name: str,
    target_source_key: str,
    limit: int = SOURCE_ENRICHMENT_BATCH_SIZE,
) -> list[CompanyUrlCandidate]:
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            # Interpolated, not bound — see `_ACTIVE_ONLY_SQL` at the top of this module.
            cur.execute(
                f"""
                WITH latest_observation AS (
                  SELECT DISTINCT ON (o.company_id)
                    o.company_id,
                    o.value_text
                  FROM public.company_field_observations o
                  WHERE o.field_name = %s
                    AND o.value_text IS NOT NULL
                    AND trim(o.value_text) <> ''
                  ORDER BY o.company_id, o.is_canonical DESC, o.observed_at DESC, o.created_at DESC
                )
                SELECT
                  c.id,
                  c.company_name,
                  c.address,
                  c.postcode,
                  c.city,
                  c.registration_number,
                  lo.value_text
                FROM public.companies c
                JOIN latest_observation lo ON lo.company_id = c.id
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM public.company_source_records csr
                  JOIN public.sources s ON s.id = csr.source_id
                  WHERE csr.company_id = c.id
                    AND s.key = %s
                    AND COALESCE(csr.source_payload ->> 'status', '') <> 'planned'
                )
                {_ACTIVE_ONLY_SQL}
                {_SKIP_RESOLVED_WEBSITE_SQL}
                ORDER BY c.updated_at DESC, c.company_name ASC
                LIMIT %s
                """,
                (observation_field_name, target_source_key, limit),
            )
            return [CompanyUrlCandidate(*row) for row in cur.fetchall()]
    finally:
        connection.close()


def fetch_companies_for_llm_enrichment(
    source_key: str,
    *,
    after_id: str | None = None,
    limit: int = SOURCE_ENRICHMENT_BATCH_SIZE,
    min_quality_score: float | None = None,
    max_quality_score: float = 0.90,
) -> list[CompanyCandidate]:
    """
    Like fetch_companies_without_source_after_id but additionally skips companies
    whose data_quality_score is already at or above max_quality_score (default 0.90).
    Use this for expensive LLM enrichment so fully-covered companies are not
    re-processed. Set min_quality_score to prioritise the least-enriched companies.
    """
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            id_clause = "AND c.id > %s" if after_id else ""
            params: list = []
            if after_id:
                params.append(after_id)
            params += [source_key, max_quality_score, limit * SOURCE_ENRICHMENT_SELECTION_WINDOW]
            if min_quality_score is not None:
                quality_clause = (
                    "AND (c.data_quality_score IS NULL OR c.data_quality_score BETWEEN %s AND %s)"
                )
                params.insert(-1, min_quality_score)
                params.insert(-1, max_quality_score)
            else:
                quality_clause = "AND (c.data_quality_score IS NULL OR c.data_quality_score < %s)"
            cur.execute(
                f"""
                SELECT
                  c.id,
                  c.company_name,
                  c.address,
                  c.postcode,
                  c.city,
                  c.registration_number,
                  c.legal_form,
                  c.wz_code,
                  c.business_purpose,
                  c.metadata ->> 'branch_description' as branch_description
                FROM public.companies c
                WHERE true
                  {id_clause}
                  AND NOT EXISTS (
                    SELECT 1
                    FROM public.company_source_records csr
                    JOIN public.sources s ON s.id = csr.source_id
                    WHERE csr.company_id = c.id
                      AND s.key = %s
                      AND COALESCE(csr.source_payload ->> 'status', '') <> 'planned'
                  )
                  {quality_clause}
                  {_ACTIVE_ONLY_SQL}
                  {_SKIP_RESOLVED_WEBSITE_SQL}
                ORDER BY c.id ASC
                LIMIT %s
                """,
                params,
            )
            return _rank_company_candidates(
                [CompanyCandidate(*row) for row in cur.fetchall()], limit
            )
    finally:
        connection.close()


#: Whether to put the Handelsregister number — the ID a German company gets in the commercial
#: register, like "HRB 217094" — into the search query.
#:
#: Off, because measuring it said so. On 100 Osnabrück companies, searching WITH the number
#: returned more results (only 17 companies found nothing, versus 29 without it) but fewer
#: usable ones: a website we could actually prove for 21 companies, versus 25 for the identical
#: pipeline searching without it. The extra results are register aggregators, insolvency
#: trackers and business directories, because those are the pages that quote register numbers.
#: The search gets pulled toward pages *about* the company and away from the company's own site.
#:
#: This does NOT stop url_discovery.scorer.score_candidate from using the register number to
#: confirm a page it has already downloaded. The two uses are separable and only the query side
#: is turned off here. See docs/website-and-owner-discovery-results.md.
SEARCH_QUERY_INCLUDE_REGISTER_NUMBER = (
    os.getenv("SEARCH_QUERY_INCLUDE_REGISTER_NUMBER", "false").lower() == "true"
)


def build_search_queries(
    company: CompanyCandidate, *, include_registration_number: bool | None = None
) -> dict[str, str]:
    """Build the search queries for one company.

    `include_registration_number` defaults to the SEARCH_QUERY_INCLUDE_REGISTER_NUMBER env var
    (off). Pass it explicitly only to measure the difference — scripts/ac_measurement.py does.
    """
    if include_registration_number is None:
        include_registration_number = SEARCH_QUERY_INCLUDE_REGISTER_NUMBER
    exact_name = f'"{company.company_name}"'
    normalized = normalize_company_name_for_search(company.company_name)
    normalized_name = f'"{normalized}"' if normalized and normalized != company.company_name.lower() else ""
    location = " ".join(part for part in [company.postcode or "", company.city or ""] if part).strip()
    address = company.address or location
    registration_number = (
        f'"{company.registration_number}"'
        if company.registration_number and include_registration_number
        else ""
    )

    general_variants = [exact_name]
    if normalized_name:
        general_variants.append(normalized_name)
    if registration_number:
        general_variants.append(registration_number)

    general_query = " OR ".join(general_variants)

    return {
        "general": f'({general_query}) "{address}"'.strip(),
        "website": f'({general_query}) "{location}"'.strip(),
        "kununu": f'({general_query}) "{location}" site:kununu.com'.strip(),
        "linkedin": f'({general_query}) "{location}" site:linkedin.com/company'.strip(),
    }


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def log_and_skip_if_disabled(flag_name: str) -> bool:
    enabled = os.getenv(flag_name, "false").lower() == "true"
    if not enabled:
        log.info("%s is disabled; DAG run will no-op until configured.", flag_name)
    return enabled


def dump_preview_payload(company: CompanyCandidate, source_key: str) -> dict[str, Any]:
    return {
        "status": "planned",
        "source_key": source_key,
        "company_id": company.id,
        "company_name": company.company_name,
        "queries": build_search_queries(company),
        "generated_at": now_iso(),
    }
