"""
Canonical resolution and quality scoring for the 6 target enrichment fields.

Architecture: enriched field values live in company_field_observations and are
promoted through company_canonical_field_sources (one row per logical field per
company). The companies table holds only identity + pipeline metadata.
Application code queries through the v_companies_enriched view.

Logical field names → observation field_name aliases:
  website        ← candidate_url, website
  cluster        ← cluster
  tech_field     ← tech_field
  ceo_contact    → company_managers (not canonical field sources)
  revenue        ← revenue
  employee_count ← employee_count, number_of_employees
  technologiefeld← technologiefeld        (Berlin's 77 fields, website-gated)
  purpose_field  ← purpose_field          (the same 77, from the register purpose)
  activity_keywords ← activity_keywords   (product/service words from the website)

`email` and `phone` (the `website_contacts` DAG) are resolved by nothing until a privacy review
clears them: a resolved value reaches the app and `v_field_provenance`.

Two field names are resolved by NOTHING and must stay that way: `technologiefeld_withheld` and
`purpose_field_withheld`, which hold the fields the website rule and the length floor refused to
publish. Naming either one in an `obs_field_names` array publishes every withheld row at once.
`directory_url` is the third name in that category, for third-party pages.

`cluster`/`tech_field` and `technologiefeld`/`purpose_field` are FOUR different questions and must
never be merged or coalesced -- the first two are free text an LLM typed, the last two are a
controlled vocabulary. A WZ code may never fill `technologiefeld`.

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

    # A company whose published value no longer has any qualifying observation loses it. Without
    # this, a re-score that drops a website below the bar (or any value whose confidence falls) left
    # the old value published for good, because this function only ever wrote winners. Only the
    # pointer goes: the observation stays, un-canonicalised, as every observation does.
    cur.execute(
        f"""
        WITH stale AS (
          DELETE FROM public.company_canonical_field_sources c
          WHERE c.field_name = %s
            AND NOT EXISTS (SELECT 1 FROM {_TMP_TABLE} w WHERE w.company_id = c.company_id)
          RETURNING c.company_field_observation_id
        )
        UPDATE public.company_field_observations o
        SET is_canonical = false, updated_at = now()
        WHERE o.id IN (SELECT company_field_observation_id FROM stale) AND o.is_canonical
        """,
        (logical_field,),
    )

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
    """Upsert every ceo_name_or_contact name above the bar into company_managers, with its provenance.

    **Every name, not one per company.** A company often has two or three Geschäftsführer; taking only
    the best name per company kept 37 of the 67 names spaCy read on the Berlin sample. Each distinct
    `(company, name)` is written once, from the best observation of it: lowest source priority, then
    highest confidence, then the newest.

    **Each name keeps where it came from**: `company_field_observation_id` and `source_id` (V3), so
    the app can say which source read it, when, and from which Impressum. On a repeat run the pointer
    moves to whichever observation is now best.

    Idempotent through `uq_company_managers_company_name` on `(company_id, full_name)`, which the
    clause below names. Naming it rather than relying on inference means a migration that drops the
    constraint breaks this statement loudly. Before that key existed, every run appended a fresh
    copy of every name (9,195 rows for 1,047 names at worst).

    Still deliberate: no batch and no changed-companies filter. It re-runs over every qualifying
    observation, which is cheap now that repeats collapse, so `rowcount` counts inserts and updates.

    **A name whose observation is gone goes too.** The FK sets the pointer to NULL when a source
    deletes what it read (spaCy does when a company's website changes); before this, those names
    stayed in the app: 16 read off the wrong page of rustlerschriever.com on 2026-10-28.
    """
    cur.execute("DELETE FROM public.company_managers WHERE company_field_observation_id IS NULL")
    removed = cur.rowcount
    cur.execute(
        """
        WITH best AS (
          SELECT DISTINCT ON (o.company_id, o.value_text)
            o.company_id,
            o.value_text AS full_name,
            o.id AS observation_id,
            o.source_id
          FROM public.company_field_observations o
          JOIN public.sources s ON s.id = o.source_id
          WHERE o.field_name = 'ceo_name_or_contact'
            AND o.value_text IS NOT NULL
            AND trim(o.value_text) <> ''
            AND COALESCE(o.confidence, 0) >= %s
          ORDER BY o.company_id, o.value_text, s.priority ASC, o.confidence DESC NULLS LAST, o.observed_at DESC
        )
        INSERT INTO public.company_managers
          (company_id, full_name, role, is_current, metadata, company_field_observation_id, source_id,
           created_at, updated_at)
        SELECT b.company_id, b.full_name, 'Geschäftsführer', true, '{}'::jsonb, b.observation_id,
               b.source_id, now(), now()
        FROM best b
        ON CONFLICT ON CONSTRAINT uq_company_managers_company_name DO UPDATE SET
          company_field_observation_id = EXCLUDED.company_field_observation_id,
          source_id = EXCLUDED.source_id,
          updated_at = now()
        WHERE public.company_managers.company_field_observation_id
              IS DISTINCT FROM EXCLUDED.company_field_observation_id
        """,
        (_FIELD_MIN_CONF,),
    )
    return removed + cur.rowcount


def resolve_technologiefeld(cur: Any) -> int:
    """Berlin's 77-field taxonomy, from words the company published on its own site.

    No alias list, and there must never be one. `technologiefeld_withheld` holds the 535 fields the
    website rule refused to publish, and it is a field name **nothing resolves** -- the same trick
    `directory_url` uses. Adding it here, or to any other `obs_field_names` array, would publish
    every withheld row at once and no threshold change could take it back.

    `branch_classifier` is currently the only source writing this name, so there is no priority
    contest and no agreement bonus; the resolution is a formality that exists so the column reaches
    `v_companies_enriched` through the same path as every other field.
    """
    return _apply_canonical(
        cur,
        logical_field="technologiefeld",
        obs_field_names=["technologiefeld"],
        value_col="value_text",
        min_confidence=_FIELD_MIN_CONF,
    )


def resolve_purpose_field(cur: Any) -> int:
    """The same 77 fields, from the register's Gegenstand. A different question, and no website gate.

    Same rule as above: `purpose_field_withheld` -- the 17 fields the 40-character floor cost -- gets
    no resolver, ever.

    The confidence policy behind these rows has its lowest value at 0.52, deliberately above
    `_FIELD_MIN_CONF`. It used to be 0.50, exactly on the bar, where 4,095 rows promoted only because
    the filter reads `>=`. `enrichment/branch_observations.py` asserts the gap at import.
    """
    return _apply_canonical(
        cur,
        logical_field="purpose_field",
        obs_field_names=["purpose_field"],
        value_col="value_text",
        min_confidence=_FIELD_MIN_CONF,
    )


def resolve_activity_keywords(cur: Any) -> int:
    """Product and service keywords the company's own website states (`website_keywords`)."""
    return _apply_canonical(
        cur,
        logical_field="activity_keywords",
        obs_field_names=["activity_keywords"],
        value_col="value_text",
        min_confidence=_FIELD_MIN_CONF,
    )


def compute_quality_scores(cur: Any) -> int:
    """
    Recompute data_quality_score for every company from company_canonical_field_sources
    and company_managers (for ceo_contact). Binary presence per field, weighted sum.

    **`technologiefeld` and `purpose_field` are deliberately absent from these weights.** The six
    below sum to 1.00, so adding two more means either giving them no weight -- nothing moves and
    every stored score stays valid -- or re-weighting all eight, which invalidates all 14,443 stored
    `data_quality_score` values and shifts the tier histogram. `test_spend_guard` pins the ceiling at
    0.850 with 0 companies at the 0.90 spend guard, so a re-weighting would fail that test for a
    reason having nothing to do with the industry columns.

    The choice is open, not settled: whoever wants these two to count towards data quality should
    make it as its own change, with the new histogram measured and the pinned numbers moved in the
    same commit.
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
        WHERE field_name = ANY(ARRAY['website','cluster','tech_field','revenue','employee_count',
                                     'technologiefeld','purpose_field'])
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

    # This tuple and the SQL ARRAY above are the same list written twice, and they MUST move
    # together. Widen one and not the other and this function under-reports with no error: the SQL
    # counts a field the Python never prints, or the Python prints a field the SQL never counted and
    # it silently reads 0.
    fields = {
        f: {"filled": field_counts.get(f, 0), "pct": pct(field_counts.get(f, 0))}
        for f in ("website", "cluster", "tech_field", "revenue", "employee_count",
                  "technologiefeld", "purpose_field")
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
