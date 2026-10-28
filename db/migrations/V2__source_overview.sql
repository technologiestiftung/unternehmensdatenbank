-- What each source has collected, for the app's sources page. Counts only, never a value.
--
-- Two questions per source: how much did it deliver (records, companies, values per field), and
-- how much of that does the app show (the coordinator's winners). A source can store a lot and show
-- nothing: `email` and `phone` from `website_contacts` wait for a privacy review, and keywords publish
-- only from 500 companies on. Without the split that looks like a broken source.
--
-- The counts are stored, like the company profile (V5 in git history): counting 202,281 observations
-- on every page load is wasteful, and they only change when data changes. `refresh_company_profile()`
-- now refreshes them too, so they are as fresh as the profile the app shows. The latest run per
-- source is read live from source_sync_runs (a few hundred rows): `website_contacts` does not trigger
-- the coordinator, and its run should show anyway.

-- 1. Per source: records, companies it covers (records and linked grants), companies it created.
--    One grouped pass: a count per source took 5.9 s of every refresh, this takes 1.5 s (296,486
--    register records, 2026-10-28).
CREATE MATERIALIZED VIEW public.source_stats_mv AS
WITH covered AS (
    SELECT r.source_id, r.company_id, true AS is_record, (r.match_method = 'created') AS created
    FROM public.company_source_records r
    UNION ALL
    SELECT s.id, l.company_id, false, false
    FROM public.company_funding_links l
    JOIN public.company_fundings f ON f.fkz = l.fkz
    JOIN public.sources s ON s.key = f.source
), per_source AS (
    SELECT source_id,
           count(*) FILTER (WHERE is_record) AS records,
           count(*) FILTER (WHERE created) AS companies_created,
           count(DISTINCT company_id) AS companies
    FROM covered
    GROUP BY source_id
)
SELECT
    s.key AS source_key,
    COALESCE(p.records, 0) AS records,
    COALESCE(p.companies_created, 0) AS companies_created,
    COALESCE(p.companies, 0) AS companies,
    now() AS computed_at
FROM public.sources s
LEFT JOIN per_source p ON p.source_id = s.id
WITH DATA;

CREATE UNIQUE INDEX source_stats_mv_key ON public.source_stats_mv (source_key);

-- 2. Per source and field: values stored, companies, companies whose shown value came from it.
--    "Shown" is the coordinator's choice (is_canonical), or a manager name in company_managers.
--    Grants are not observations; they count as the field `fundings` of the source that delivered them.
CREATE MATERIALIZED VIEW public.source_field_stats_mv AS
SELECT
    s.key AS source_key,
    o.field_name,
    count(*) AS observations,
    count(DISTINCT o.company_id) AS companies,
    count(DISTINCT o.company_id) FILTER (WHERE o.is_canonical OR m.id IS NOT NULL) AS companies_shown,
    round(avg(o.confidence), 3) AS avg_confidence,
    max(o.observed_at) AS last_observed_at
FROM public.company_field_observations o
JOIN public.sources s ON s.id = o.source_id
LEFT JOIN public.company_managers m ON m.company_field_observation_id = o.id AND m.is_current
GROUP BY s.key, o.field_name
UNION ALL
SELECT
    f.source,
    'fundings',
    count(DISTINCT f.fkz),
    count(DISTINCT l.company_id),
    count(DISTINCT l.company_id),
    NULL::numeric,
    max(f.last_seen_at)
FROM public.company_fundings f
LEFT JOIN public.company_funding_links l ON l.fkz = f.fkz
GROUP BY f.source
WITH DATA;

CREATE UNIQUE INDEX source_field_stats_mv_key ON public.source_field_stats_mv (source_key, field_name);

-- 3. What the app reads. last_run_status is the newest run's stored status, so a run killed by a
--    restart can show `running` until the next run starts. runs_interrupted counts the runs still
--    `running` although a newer run has started since: a worker or container restart leaves its row
--    behind (11 for website_discovery on 2026-10-28). The history itself is not changed.
CREATE VIEW public.v_source_overview AS
SELECT
    s.key AS source_key,
    s.name,
    s.source_type,
    s.base_url,
    COALESCE(st.records, 0) AS records,
    COALESCE(st.companies, 0) AS companies,
    COALESCE(st.companies_created, 0) AS companies_created,
    run.started_at AS last_run_started_at,
    run.finished_at AS last_run_finished_at,
    run.status AS last_run_status,
    run.records_seen AS last_run_seen,
    run.records_created AS last_run_created,
    run.records_updated AS last_run_updated,
    run.records_failed AS last_run_failed,
    (SELECT count(*) FROM public.source_sync_runs r WHERE r.source_id = s.id) AS runs,
    (SELECT count(*) FROM public.source_sync_runs r
      WHERE r.source_id = s.id AND r.status = 'running'
        AND EXISTS (SELECT 1 FROM public.source_sync_runs n
                    WHERE n.source_id = r.source_id AND n.started_at > r.started_at)) AS runs_interrupted,
    st.computed_at AS counts_as_of
FROM public.sources s
LEFT JOIN public.source_stats_mv st ON st.source_key = s.key
LEFT JOIN LATERAL (
    SELECT r.started_at, r.finished_at, r.records_seen, r.records_created, r.records_updated,
           r.records_failed, r.status
    FROM public.source_sync_runs r
    WHERE r.source_id = s.id
    ORDER BY r.started_at DESC
    LIMIT 1
) run ON true;

COMMENT ON VIEW public.v_source_overview IS
  'One row per source for the app''s sources page: records, companies, companies created, and its latest run. Counts as of counts_as_of (refresh_company_profile()); the run is live.';

CREATE VIEW public.v_source_fields AS
SELECT source_key, field_name, observations, companies, companies_shown, avg_confidence, last_observed_at
FROM public.source_field_stats_mv;

COMMENT ON VIEW public.v_source_fields IS
  'Per source and field: values stored, companies, and companies whose shown value came from this source. Counts only; no value is exposed.';

-- 4. Refreshed with the profile, wherever that is refreshed (the coordinator, the imports).
CREATE OR REPLACE FUNCTION public.refresh_company_profile() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public', 'pg_temp'
    AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.company_profile_mv;
  ANALYZE public.company_profile_mv;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.source_stats_mv;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.source_field_stats_mv;
END
$$;

GRANT SELECT ON public.source_stats_mv, public.source_field_stats_mv, public.v_source_overview,
    public.v_source_fields TO ${reader_role};
GRANT SELECT ON public.source_stats_mv, public.source_field_stats_mv, public.v_source_overview,
    public.v_source_fields TO ${writer_role};
