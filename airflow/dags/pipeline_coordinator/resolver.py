"""
Canonical resolution: which observation each field of each company shows.

Architecture: every shown value lives in company_field_observations and is promoted through
company_canonical_field_sources (one row per logical field per company). The companies table holds
identity only. Application code queries through the v_companies_enriched view.

Logical field names → observation field_name aliases:
  website           ← candidate_url, website
  ceo_contact       → company_managers (not canonical field sources)
  revenue           ← revenue
  employee_count    ← employee_count, number_of_employees
  technology_field  ← technology_field  (Berlin's 77 fields in English, website-gated)
  purpose_field     ← purpose_field     (the same 77, from the register purpose)
  activity_keywords ← activity_keywords (product/service words from the website)
  register_announcement ← register_announcement (latest kind of court announcement; a hint, not the status)
  REGISTER_FIELDS   name, legal_form, address, postcode, city, register_status, ... (V3)
  deeptech_class, is_ai (only corrections from the app fill them so far)

`email` and `phone` (the `website_contacts` DAG) are stored, never resolved: a resolved value
reaches the app and `v_field_provenance`.

Two field names are resolved by NOTHING and must stay that way: `technology_field_withheld` and
`purpose_field_withheld`, which hold the fields the website rule and the length floor refused to
publish. Naming either one in an `obs_field_names` array publishes every withheld row at once.
`directory_url` is the third name in that category, for third-party pages.

`technology_field` and `purpose_field` are two different questions and must never be merged or
coalesced. A WZ code may never fill `technology_field`. (`cluster` and `tech_field`, free text an
LLM typed, were removed with #47.)

Source priority: lower integer wins. Cross-source agreement bonus: +0.10 per
additional source agreeing on the same value, capped at 1.0.
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

_WEBSITE_MIN_CONF = 0.35
_FIELD_MIN_CONF = 0.50

_TMP_TABLE = "_cfw_winners"

#: Years after which a value counts half when ranked against another value of the same field (#49,
#: decided 2026-10-29; starting values, to be measured once Fusionbase overlaps with OffeneRegister).
#: The age runs from when the SOURCE read the value (`source_updated_at`, else `observed_at`). Decay
#: only ranks: the bar filters on the stored confidence, so an old value with nothing newer stays
#: shown, and the app says when it was read. A field not listed does not decay; `established_at` never.
HALF_LIVES: dict[str, float] = {
    "website": 3,
    "address": 5, "postcode": 5, "city": 5,
    "register_status": 2,
    "name": 10, "legal_form": 10,
}

#: The source key of people's corrections (`feedback_observations`). A correction stands until another
#: source has a qualifying value it read AFTER the latest vote behind the correction (#49).
PEOPLE_SOURCE = "user_feedback"


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
    # With the register fields (V3) this holds up to 296,897 winners. Without indexes, one stale row
    # estimate (statistics from before a bulk load) made the planner scan all of it once per published
    # row: the stale-pointer DELETE for `name` ran over 10 minutes on a fresh copy. Indexed, a bad
    # estimate costs a lookup per row.
    cur.execute(f"CREATE INDEX IF NOT EXISTS {_TMP_TABLE}_company ON {_TMP_TABLE} (company_id)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS {_TMP_TABLE}_obs ON {_TMP_TABLE} (obs_id)")
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

    Returns the number of company_canonical_field_sources rows that got a new winner. An unchanged
    winner is not written again, so `selected_at` is when the value was chosen, not the last run.
    """
    _ensure_tmp(cur)

    # --- Step 1: find winners, load into temp table ---
    value_expr = f"o.{value_col}" + (f"::{cast_as}" if cast_as else "")
    half_life = HALF_LIVES.get(logical_field)
    not_null_check = f"o.{value_col} IS NOT NULL"

    cur.execute(
        f"""
        INSERT INTO {_TMP_TABLE} (company_id, obs_id, source_id)
        WITH qualifying AS (
          SELECT
            o.company_id,
            o.id                              AS obs_id,
            o.source_id,
            {value_expr}                      AS val,
            -- Ranked by age: halves every `half_life` years since the source read it.
            COALESCE(o.confidence, 0.5) * CASE WHEN %(half_life)s::float IS NULL THEN 1 ELSE power(
              0.5, GREATEST(0, extract(epoch FROM now() - COALESCE(o.source_updated_at, o.observed_at)))
                   / 31557600.0 / %(half_life)s::float) END AS conf,
            COALESCE(o.source_updated_at, o.observed_at) AS read_at,
            s.key = %(people)s                AS from_people,
            s.priority
          FROM public.company_field_observations o
          JOIN public.sources s ON s.id = o.source_id
          WHERE o.field_name = ANY(%(fields)s)
            AND {not_null_check}
            AND COALESCE(o.confidence, 0) >= %(min_confidence)s
        ),
        candidates AS (
          -- A correction gives way to a source value read after the vote behind it.
          SELECT * FROM qualifying c
          WHERE NOT (c.from_people AND EXISTS (
            SELECT 1 FROM qualifying n
            WHERE n.company_id = c.company_id AND NOT n.from_people AND n.read_at > c.read_at))
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
        {"fields": obs_field_names, "min_confidence": min_confidence, "half_life": half_life,
         "people": PEOPLE_SOURCE},
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

    # A winner that is already published needs nothing more. Without this, steps 2-4 cleared every
    # winner and set it again on every run: about 5 million rows once the nine register fields (V3)
    # went through here, for a run in which nothing had changed.
    cur.execute(
        f"""
        DELETE FROM {_TMP_TABLE} w
        USING public.company_canonical_field_sources c
        WHERE c.company_id = w.company_id AND c.field_name = %s AND c.company_field_observation_id = w.obs_id
        """,
        (logical_field,),
    )
    if n_winners == cur.rowcount:
        return 0

    # --- Step 2: clear old is_canonical for all aliases on companies with a new winner ---
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


def resolve_technology_field(cur: Any) -> int:
    """Berlin's 77-field taxonomy, from words the company published on its own site.

    No alias list, and there must never be one. `technology_field_withheld` holds the 535 fields the
    website rule refused to publish, and it is a field name **nothing resolves** -- the same trick
    `directory_url` uses. Adding it here, or to any other `obs_field_names` array, would publish
    every withheld row at once and no threshold change could take it back.

    `branch_classifier` is currently the only source writing this name, so there is no priority
    contest and no agreement bonus; the resolution is a formality that exists so the column reaches
    `v_companies_enriched` through the same path as every other field.
    """
    return _apply_canonical(
        cur,
        logical_field="technology_field",
        obs_field_names=["technology_field"],
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


def resolve_register_announcement(cur: Any) -> int:
    """The latest register announcement (`register_announcements`). Its DAG gives the latest of each
    company confidence 1.0 and every older one 0, so only the latest clears the bar."""
    return _apply_canonical(
        cur,
        logical_field="register_announcement",
        obs_field_names=["register_announcement"],
        value_col="value_text",
        min_confidence=_FIELD_MIN_CONF,
    )


#: What the register says about a company, one observation field each (V3). `v_companies_enriched`
#: reads the winners under the old `companies` column names; `name` is `company_name` there.
REGISTER_FIELDS = (
    "name", "legal_form", "address", "postcode", "city",
    "register_status", "established_at", "business_purpose", "wz_code",
)


def resolve_register_fields(cur: Any) -> int:
    """Name, address, status and the other register facts. Until V3 they were plain columns that
    nothing could compete with; now a second register or a user's correction can win them."""
    return sum(
        _apply_canonical(
            cur, logical_field=field, obs_field_names=[field], value_col="value_text",
            min_confidence=_FIELD_MIN_CONF,
        )
        for field in REGISTER_FIELDS
    )


def resolve_deeptech_and_ai(cur: Any) -> int:
    """`deeptech_class` and `is_ai`. The profile has shown both since V1, but no pipeline writes them
    yet; until one does, only people's corrections (`feedback_observations`) fill them."""
    return sum(
        _apply_canonical(cur, logical_field=field, obs_field_names=[field], value_col="value_text",
                         min_confidence=_FIELD_MIN_CONF)
        for field in ("deeptech_class", "is_ai")
    )


#: The fields the coordinator reports coverage for, one list for the SQL and the Python.
COVERAGE_FIELDS = ("website", "revenue", "employee_count", "technology_field", "purpose_field")


def report_field_coverage(cur: Any) -> dict[str, Any]:
    """Per-field fill rates, from the canonical table, and the number of companies."""
    cur.execute("SELECT COUNT(*) FROM public.companies WHERE merged_into IS NULL")
    total = cur.fetchone()[0]
    cur.execute(
        """
        SELECT field_name, COUNT(DISTINCT company_id)
        FROM public.company_canonical_field_sources
        WHERE field_name = ANY(%s)
        GROUP BY field_name
        """,
        (list(COVERAGE_FIELDS),),
    )
    field_counts: dict[str, int] = dict(cur.fetchall())
    cur.execute("SELECT COUNT(DISTINCT company_id) FROM public.company_managers WHERE is_current")
    ceo_count: int = cur.fetchone()[0]

    def pct(n: int) -> str:
        return f"{n / total * 100:.1f}%" if total else "0%"

    fields = {f: {"filled": field_counts.get(f, 0), "pct": pct(field_counts.get(f, 0))} for f in COVERAGE_FIELDS}
    fields["ceo_contact"] = {"filled": ceo_count, "pct": pct(ceo_count)}
    return {"total_companies": total, "fields": fields}
