-- Pipeline metadata column on companies (data_quality_score).
-- Enriched field values (website, cluster, tech_field, revenue, employee_count)
-- are NOT stored on companies — they live in company_canonical_field_sources
-- and are accessed through the v_companies_enriched view below.

ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS data_quality_score NUMERIC(4, 3)
    CONSTRAINT companies_data_quality_score_check
      CHECK (data_quality_score IS NULL OR (data_quality_score BETWEEN 0 AND 1));

CREATE INDEX IF NOT EXISTS companies_data_quality_score_idx
  ON public.companies (data_quality_score NULLS FIRST);

-- Speed up canonical resolution and view lookups
CREATE INDEX IF NOT EXISTS company_canonical_field_sources_company_field_idx
  ON public.company_canonical_field_sources (company_id, field_name);

-- Application view: join companies through canonical pointers to get enriched fields
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
  src_emp.key                     AS employee_count_source
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
