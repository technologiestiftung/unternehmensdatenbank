-- The register status of a company: is it still a going concern, or is it gone?
--
-- The Northdata export has always carried this as its `Status` column and it has always been
-- imported — but only into `metadata`, as `metadata ->> 'status'`, where nothing read it. That was
-- expensive. Over the Osnabrück table of 14,443 companies:
--
--     erloschen         7,673   53.1%
--     aktiv             6,298   43.6%
--     in Liquidation      472    3.3%
--
-- so more than half of every URL guess, page download, judge call and model tie-break this project
-- has paid for went to companies that no longer exist. They returned 36 of the 902 discovered
-- websites (4.0%) — and a dissolved company's domain is exactly the kind that gets resold to
-- somebody else, so those 36 are the false-positive-prone kind the discovery rules work hardest to
-- reject.
--
-- Named `register_status`, not `status`, deliberately. `company_source_records.source_payload ->>
-- 'status'` (compared against 'planned') and `source_sync_runs.status` already exist and mean
-- something completely different. A bare `c.status` sitting next to those in one query is the next
-- recurring bug in this repo, and the longer name costs nothing.

ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS register_status TEXT;

-- The backfill needs no re-import: every row already carries the value in `metadata`. NULLIF because
-- an empty string is not a status, and the WHERE keeps this migration idempotent-ish — a row already
-- filled by a later import is not overwritten by the raw metadata behind it.
UPDATE public.companies
   SET register_status = NULLIF(metadata ->> 'status', '')
 WHERE register_status IS NULL;

CREATE INDEX IF NOT EXISTS companies_register_status_idx
  ON public.companies (register_status);

-- Deliberately NO check constraint. Berlin arrives as another Northdata CSV and may well carry a
-- fourth value; a failing import is worse than an unexpected string, and an unknown value shows up
-- immediately in a GROUP BY. The three values seen so far are documented instead:
COMMENT ON COLUMN public.companies.register_status IS
  'Register status as the source states it. Osnabrück: aktiv 6298, erloschen 7673, '
  'in Liquidation 472. NULL means the source did not say — note that the discovery filter is '
  '= ''aktiv'', which EXCLUDES NULL, so an import that fails to map this column yields no work at '
  'all rather than too much. Backfilled from metadata->>''status'', which is kept as the raw record.';

-- The view has to be repeated in full: CREATE OR REPLACE VIEW only tolerates a new column APPENDED
-- at the end of the select list, so `register_status` goes after employee_count_source rather than
-- next to wz_code where it belongs logically. Body taken from
-- `SELECT pg_get_viewdef('public.v_companies_enriched'::regclass, true)` — checked identical to V10,
-- since V11 and V12 redefine only v_discovery_results and v_discovery_scorecard.
CREATE OR REPLACE VIEW public.v_companies_enriched AS
SELECT
  c.id,
  c.company_name,
  c.normalized_name,
  c.legal_form,
  c.business_purpose,
  c.address,
  c.established_at,
  c.registration_number,
  c.wz_code,
  c.data_quality_score,
  c.canonical_updated_at,
  c.data_last_updated_at,
  c.metadata,
  c.created_at,
  c.updated_at,
  obs_website.value_text          AS website_url,
  obs_cluster.value_text          AS cluster,
  obs_tech.value_text             AS tech_field,
  obs_revenue.value_number        AS revenue,
  obs_emp.value_number::INTEGER   AS employee_count,
  obs_website.confidence          AS website_confidence,
  obs_cluster.confidence          AS cluster_confidence,
  obs_tech.confidence             AS tech_field_confidence,
  obs_revenue.confidence          AS revenue_confidence,
  obs_emp.confidence              AS employee_count_confidence,
  src_website.key                 AS website_source,
  src_cluster.key                 AS cluster_source,
  src_tech.key                    AS tech_field_source,
  src_revenue.key                 AS revenue_source,
  src_emp.key                     AS employee_count_source,
  c.register_status
FROM public.companies c
LEFT JOIN public.company_canonical_field_sources cfs_website
       ON cfs_website.company_id = c.id AND cfs_website.field_name = 'website'
LEFT JOIN public.company_field_observations obs_website
       ON obs_website.id = cfs_website.company_field_observation_id
LEFT JOIN public.sources src_website ON src_website.id = cfs_website.source_id
LEFT JOIN public.company_canonical_field_sources cfs_cluster
       ON cfs_cluster.company_id = c.id AND cfs_cluster.field_name = 'cluster'
LEFT JOIN public.company_field_observations obs_cluster
       ON obs_cluster.id = cfs_cluster.company_field_observation_id
LEFT JOIN public.sources src_cluster ON src_cluster.id = cfs_cluster.source_id
LEFT JOIN public.company_canonical_field_sources cfs_tech
       ON cfs_tech.company_id = c.id AND cfs_tech.field_name = 'tech_field'
LEFT JOIN public.company_field_observations obs_tech
       ON obs_tech.id = cfs_tech.company_field_observation_id
LEFT JOIN public.sources src_tech ON src_tech.id = cfs_tech.source_id
LEFT JOIN public.company_canonical_field_sources cfs_revenue
       ON cfs_revenue.company_id = c.id AND cfs_revenue.field_name = 'revenue'
LEFT JOIN public.company_field_observations obs_revenue
       ON obs_revenue.id = cfs_revenue.company_field_observation_id
LEFT JOIN public.sources src_revenue ON src_revenue.id = cfs_revenue.source_id
LEFT JOIN public.company_canonical_field_sources cfs_emp
       ON cfs_emp.company_id = c.id AND cfs_emp.field_name = 'employee_count'
LEFT JOIN public.company_field_observations obs_emp
       ON obs_emp.id = cfs_emp.company_field_observation_id
LEFT JOIN public.sources src_emp ON src_emp.id = cfs_emp.source_id;

COMMENT ON VIEW public.v_companies_enriched IS
  'Canonical company view joining through company_canonical_field_sources. '
  'Application code should query this view, not companies directly.';
