"""
Canonical resolution and quality scoring for the 6 target enrichment fields.

Architecture: enriched field values live in company_field_observations and are
promoted through company_canonical_field_sources (one row per logical field per
company). The companies table holds only identity + pipeline metadata.
Application code queries through the v_companies_enriched view.

Logical field names → observation field_name aliases:
  website       ← candidate_url, website
  cluster       ← cluster
  tech_field    ← tech_field
  ceo_contact   → company_managers (not canonical field sources)
  revenue       ← revenue
  employee_count← employee_count, number_of_employees

Source priority: lower integer wins. Cross-source agreement bonus: +0.10 per
additional source agreeing on the same value, capped at 1.0.

Quality score weights (sum = 1.0):
  website        0.30
  cluster        0.20
  tech_field     0.20
  ceo_contact    0.15  (presence in company_managers)
  revenue        0.10
  employee_count 0.05
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

_WEBSITE_MIN_CONF = 0.35
_FIELD_MIN_CONF = 0.50

_TMP_TABLE = "_cfw_winners"


def _ensure_tmp(cur: Any) -> None:
    cur.execute(
        f"""
        CREATE TEMP TABLE IF NOT EXISTS {_TMP_TABLE} (
          company_id UUID,
          obs_id     UUID,
          source_id  UUID
        ) ON COMMIT DELETE ROWS
        """
    )
    cur.execute(f"TRUNCATE {_TMP_TABLE}")


def _apply_canonical(
    cur: Any,
    *,
    logical_field: str,
    obs_field_names: list[str],
    value_col: str,
    min_confidence: float,
    cast_as: str | None = None,
) -> int:
    """
    Resolve the best observation for each company across obs_field_names aliases,
    mark it as canonical (is_canonical = true on the observation), and record
    the winner in company_canonical_field_sources under logical_field.

    Returns the number of company_canonical_field_sources rows upserted.
    """
    _ensure_tmp(cur)

    # --- Step 1: find winners, load into temp table ---
    value_expr = f"o.{value_col}" + (f"::{cast_as}" if cast_as else "")
    not_null_check = f"o.{value_col} IS NOT NULL"

    cur.execute(
        f"""
        INSERT INTO {_TMP_TABLE} (company_id, obs_id, source_id)
        WITH candidates AS (
          SELECT
            o.company_id,
            o.id                              AS obs_id,
            o.source_id,
            {value_expr}                      AS val,
            COALESCE(o.confidence, 0.5)       AS conf,
            s.priority
          FROM public.company_field_observations o
          JOIN public.sources s ON s.id = o.source_id
          WHERE o.field_name = ANY(%s)
            AND {not_null_check}
            AND COALESCE(o.confidence, 0) >= %s
        ),
        agreement AS (
          SELECT
            company_id,
            val,
            COUNT(DISTINCT source_id)                                         AS n_sources,
            MAX(conf)                                                          AS best_conf,
            MIN(priority)                                                      AS best_priority,
            (array_agg(obs_id    ORDER BY conf DESC NULLS LAST, obs_id))[1]   AS best_obs_id,
            (array_agg(source_id ORDER BY conf DESC NULLS LAST, obs_id))[1]   AS best_source_id
          FROM candidates
          GROUP BY company_id, val
        ),
        winners AS (
          SELECT DISTINCT ON (company_id)
            company_id,
            best_obs_id    AS obs_id,
            best_source_id AS source_id
          FROM agreement
          ORDER BY
            company_id,
            best_priority ASC,
            LEAST(1.0, best_conf + (n_sources - 1) * 0.10) DESC,
            val
        )
        SELECT company_id, obs_id, source_id FROM winners
        """,
        (obs_field_names, min_confidence),
    )
    n_winners = cur.rowcount

    if n_winners == 0:
        return 0

    # --- Step 2: clear old is_canonical for all aliases on affected companies ---
    cur.execute(
        f"""
        UPDATE public.company_field_observations
        SET is_canonical = false, updated_at = now()
        WHERE field_name = ANY(%s)
          AND is_canonical = true
          AND company_id IN (SELECT company_id FROM {_TMP_TABLE})
        """,
        (obs_field_names,),
    )

    # --- Step 3: mark winner observations as canonical ---
    cur.execute(
        f"""
        UPDATE public.company_field_observations
        SET is_canonical = true, updated_at = now()
        WHERE id IN (SELECT obs_id FROM {_TMP_TABLE})
          AND NOT is_canonical
        """
    )

    # --- Step 4: upsert company_canonical_field_sources ---
    cur.execute(
        f"""
        INSERT INTO public.company_canonical_field_sources
          (company_id, field_name, source_id, company_field_observation_id,
           selected_at, metadata, created_at, updated_at)
        SELECT
          w.company_id,
          %s,
          w.source_id,
          w.obs_id,
          now(),
          '{{}}'::jsonb,
          now(),
          now()
        FROM {_TMP_TABLE} w
        ON CONFLICT (company_id, field_name)
        DO UPDATE SET
          source_id                    = EXCLUDED.source_id,
          company_field_observation_id = EXCLUDED.company_field_observation_id,
          selected_at                  = now(),
          updated_at                   = now()
        """,
        (logical_field,),
    )
    return cur.rowcount


def resolve_website(cur: Any) -> int:
    return _apply_canonical(
        cur,
        logical_field="website",
        obs_field_names=["candidate_url", "website"],
        value_col="value_text",
        min_confidence=_WEBSITE_MIN_CONF,
    )


def resolve_cluster(cur: Any) -> int:
    return _apply_canonical(
        cur,
        logical_field="cluster",
        obs_field_names=["cluster"],
        value_col="value_text",
        min_confidence=_FIELD_MIN_CONF,
    )


def resolve_tech_field(cur: Any) -> int:
    return _apply_canonical(
        cur,
        logical_field="tech_field",
        obs_field_names=["tech_field"],
        value_col="value_text",
        min_confidence=_FIELD_MIN_CONF,
    )


def resolve_revenue(cur: Any) -> int:
    return _apply_canonical(
        cur,
        logical_field="revenue",
        obs_field_names=["revenue"],
        value_col="value_number",
        min_confidence=_FIELD_MIN_CONF,
        cast_as="NUMERIC",
    )


def resolve_employee_count(cur: Any) -> int:
    return _apply_canonical(
        cur,
        logical_field="employee_count",
        obs_field_names=["employee_count", "number_of_employees"],
        value_col="value_number",
        min_confidence=_FIELD_MIN_CONF,
        cast_as="INTEGER",
    )


def resolve_ceo_contact(cur: Any) -> int:
    """Upsert best ceo_name_or_contact observation into company_managers."""
    cur.execute(
        """
        WITH best AS (
          SELECT DISTINCT ON (o.company_id)
            o.company_id,
            o.value_text AS full_name
          FROM public.company_field_observations o
          JOIN public.sources s ON s.id = o.source_id
          WHERE o.field_name = 'ceo_name_or_contact'
            AND o.value_text IS NOT NULL
            AND trim(o.value_text) <> ''
            AND COALESCE(o.confidence, 0) >= %s
          ORDER BY o.company_id, s.priority ASC, o.confidence DESC NULLS LAST
        )
        INSERT INTO public.company_managers
          (company_id, full_name, role, is_current, metadata, created_at, updated_at)
        SELECT b.company_id, b.full_name, 'Geschäftsführer', true, '{}'::jsonb, now(), now()
        FROM best b
        ON CONFLICT DO NOTHING
        """,
        (_FIELD_MIN_CONF,),
    )
    return cur.rowcount


def compute_quality_scores(cur: Any) -> int:
    """
    Recompute data_quality_score for every company from company_canonical_field_sources
    and company_managers (for ceo_contact). Binary presence per field, weighted sum.
    """
    cur.execute(
        """
        UPDATE public.companies c
        SET
          data_quality_score = ROUND((
            CASE WHEN EXISTS (
              SELECT 1 FROM public.company_canonical_field_sources
              WHERE company_id = c.id AND field_name = 'website'
            ) THEN 0.30 ELSE 0 END
            +
            CASE WHEN EXISTS (
              SELECT 1 FROM public.company_canonical_field_sources
              WHERE company_id = c.id AND field_name = 'cluster'
            ) THEN 0.20 ELSE 0 END
            +
            CASE WHEN EXISTS (
              SELECT 1 FROM public.company_canonical_field_sources
              WHERE company_id = c.id AND field_name = 'tech_field'
            ) THEN 0.20 ELSE 0 END
            +
            CASE WHEN EXISTS (
              SELECT 1 FROM public.company_managers m
              WHERE m.company_id = c.id AND m.is_current = true
              LIMIT 1
            ) THEN 0.15 ELSE 0 END
            +
            CASE WHEN EXISTS (
              SELECT 1 FROM public.company_canonical_field_sources
              WHERE company_id = c.id AND field_name = 'revenue'
            ) THEN 0.10 ELSE 0 END
            +
            CASE WHEN EXISTS (
              SELECT 1 FROM public.company_canonical_field_sources
              WHERE company_id = c.id AND field_name = 'employee_count'
            ) THEN 0.05 ELSE 0 END
          )::NUMERIC, 3),
          canonical_updated_at = now(),
          updated_at = now()
        WHERE true
        """
    )
    return cur.rowcount


def report_field_coverage(cur: Any) -> dict[str, Any]:
    """Return per-field fill rates (via canonical tables) and quality score distribution."""
    cur.execute(
        """
        SELECT
          COUNT(*)                                                          AS total,
          COUNT(CASE WHEN data_quality_score >= 0.90 THEN 1 END)           AS tier_complete,
          COUNT(CASE WHEN data_quality_score >= 0.50
                      AND data_quality_score <  0.90 THEN 1 END)           AS tier_partial,
          COUNT(CASE WHEN data_quality_score <  0.50
                      OR  data_quality_score IS NULL  THEN 1 END)          AS tier_minimal,
          ROUND(AVG(data_quality_score)::NUMERIC, 3)                       AS avg_score
        FROM public.companies
        """
    )
    row = cur.fetchone()
    total, tier_complete, tier_partial, tier_minimal, avg_score = row

    cur.execute(
        """
        SELECT field_name, COUNT(DISTINCT company_id) AS cnt
        FROM public.company_canonical_field_sources
        WHERE field_name = ANY(ARRAY['website','cluster','tech_field','revenue','employee_count'])
        GROUP BY field_name
        """
    )
    field_counts: dict[str, int] = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute(
        """
        SELECT COUNT(DISTINCT company_id)
        FROM public.company_managers
        WHERE is_current = true
        """
    )
    ceo_count: int = cur.fetchone()[0]

    def pct(n: int) -> str:
        return f"{n / total * 100:.1f}%" if total else "0%"

    fields = {
        f: {"filled": field_counts.get(f, 0), "pct": pct(field_counts.get(f, 0))}
        for f in ("website", "cluster", "tech_field", "revenue", "employee_count")
    }
    fields["ceo_contact"] = {"filled": ceo_count, "pct": pct(ceo_count)}

    return {
        "total_companies": total,
        "fields": fields,
        "tiers": {
            "complete (≥0.90)": tier_complete,
            "partial  (≥0.50)": tier_partial,
            "minimal  (<0.50)": tier_minimal,
        },
        "avg_quality_score": float(avg_score or 0),
    }
