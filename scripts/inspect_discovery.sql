-- Inspect what the search-provider DAGs wrote. Read-only.
--
--   Get-Content scripts\inspect_discovery.sql -Raw | docker compose exec -T postgres psql -U app -d unternehmensdatenbank -f -
--
-- NOTE on `rejected_as`: NULL means EITHER "this URL qualified as the company's own site" OR
-- "this row was written before the field existed". Query 5 distinguishes them via
-- `metadata ? 'rejected_as'` — absent key = old row. Only re-run providers have it.

\echo '=== 1. Which URLs were refused as the company website, and why ==================='
SELECT c.company_name,
       o.value_text,
       o.confidence,
       o.metadata->>'rejected_as'    AS rejected_as,
       o.metadata->>'score'          AS score,
       o.metadata->>'register_number' AS register_no
FROM public.company_field_observations o
JOIN public.companies c ON c.id = o.company_id
WHERE o.field_name = 'directory_url'
  AND o.metadata->>'rejected_as' IS NOT NULL
ORDER BY o.confidence DESC, c.company_name;

\echo ''
\echo '=== 2. The registercheck.de case: refused at confidence 1.0 by PATH, not host ===='
-- A register-number match scores +120, i.e. confidence 1.0. registercheck. is NOT in
-- search_common.DIRECTORY_HOSTS, so only the /companies/ path marker stopped it becoming the
-- canonical website. Compare against scripts/check_own_site_filter.py.
SELECT c.company_name,
       o.field_name,
       o.value_text,
       o.confidence,
       o.metadata->>'rejected_as' AS rejected_as,
       o.metadata->>'register_number' AS register_no
FROM public.company_field_observations o
JOIN public.companies c ON c.id = o.company_id
WHERE o.value_text ILIKE '%registercheck%'
   OR o.value_text ILIKE '%creditsafe%'
   OR o.value_text ILIKE '%insolvenz%'
   OR o.value_text ILIKE '%ceginformacio%';

\echo ''
\echo '=== 3. Which field name / logic produced each value ============================='
-- field_name says WHICH field. metadata->>verified_by says HOW:
--   register_number_match -> 1.0        url_pattern -> 0.365-0.65        llm_snippet -> <=0.34
SELECT s.key AS provider,
       o.field_name,
       coalesce(o.metadata->>'verified_by', '(derived)') AS logic,
       count(*) AS n,
       min(o.confidence) AS min_conf,
       max(o.confidence) AS max_conf
FROM public.company_field_observations o
JOIN public.sources s ON s.id = o.source_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

\echo ''
\echo '=== 4. Canonical websites after pipeline_coordinator ============================'
SELECT c.company_name,
       o.value_text,
       o.confidence,
       o.metadata->>'verified_by' AS logic,
       s.key AS provider
FROM public.company_canonical_field_sources cf
JOIN public.companies c ON c.id = cf.company_id
JOIN public.company_field_observations o ON o.id = cf.company_field_observation_id
JOIN public.sources s ON s.id = cf.source_id
WHERE cf.field_name = 'website'
ORDER BY o.confidence DESC;

\echo ''
\echo '=== 5. Do these rows even have the rejected_as field? ==========================='
-- has_field = f means the row predates the field, so its NULL says nothing. Re-run that
-- provider before comparing rejection reasons across providers.
SELECT s.key AS provider,
       o.field_name,
       (o.metadata ? 'rejected_as') AS has_field,
       count(*) AS n
FROM public.company_field_observations o
JOIN public.sources s ON s.id = o.source_id
WHERE o.field_name IN ('candidate_url', 'directory_url')
GROUP BY 1, 2, 3
ORDER BY 1, 2;

\echo ''
\echo '=== 6. Which companies did each provider actually cover? ========================'
-- Providers advance independent checkpoints, so their batches DIVERGE. Any overlap is
-- incidental — this is why the DAGs cannot be used to compare providers directly.
SELECT c.company_name,
       bool_or(s.key = 'exa_search_discovery')      AS exa,
       bool_or(s.key = 'parallel_search_discovery') AS parallel,
       bool_or(s.key = 'brave_search_discovery')    AS brave
FROM public.company_source_records r
JOIN public.sources s ON s.id = r.source_id
JOIN public.companies c ON c.id = r.company_id
GROUP BY c.company_name
ORDER BY 2 DESC, 3 DESC, 1;
