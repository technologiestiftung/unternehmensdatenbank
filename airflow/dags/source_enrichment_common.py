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
            cur.execute(
                """
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
            if after_id:
                cur.execute(
                    """
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
                    ORDER BY c.id ASC
                    LIMIT %s
                    """,
                    (after_id, source_key, limit * SOURCE_ENRICHMENT_SELECTION_WINDOW),
                )
            else:
                cur.execute(
                    """
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
            cur.execute(
                """
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


def build_search_queries(company: CompanyCandidate) -> dict[str, str]:
    exact_name = f'"{company.company_name}"'
    normalized = normalize_company_name_for_search(company.company_name)
    normalized_name = f'"{normalized}"' if normalized and normalized != company.company_name.lower() else ""
    location = " ".join(part for part in [company.postcode or "", company.city or ""] if part).strip()
    address = company.address or location
    registration_number = f'"{company.registration_number}"' if company.registration_number else ""

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
