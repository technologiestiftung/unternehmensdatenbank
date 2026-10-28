-- The company profile, computed once and stored, so every list filter is fast, including by source.
--
-- `v_company_profile` computed each company's columns on read. Unfiltered that is fine (a page of 50
-- in 0.65 s), but a selective filter makes Postgres walk the companies in name order, computing each
-- profile until it has 50 hits: 5 to 45 seconds to filter by source over 296,486 companies.
-- Computing every profile once takes about 3 seconds, so the profile is now a materialized view,
-- refreshed by `refresh_company_profile()` after the data changes (the coordinator calls it at the
-- end of every run, and every source DAG triggers the coordinator). `v_company_profile` keeps its
-- name and columns and reads the stored rows, so Hasura and the app do not change.

-- 1. Which source each shown value came from, one row per company, field and source.
CREATE VIEW public.v_company_sources AS
SELECT cfs.company_id, cfs.field_name, s.key AS source_key
FROM public.company_canonical_field_sources cfs
JOIN public.sources s ON s.id = cfs.source_id
UNION ALL
SELECT m.company_id, 'management', s.key
FROM public.company_managers m
JOIN public.sources s ON s.id = m.source_id
WHERE m.is_current
UNION ALL
SELECT r.company_id, 'register', s.key
FROM public.company_source_records r
JOIN public.sources s ON s.id = r.source_id AND s.source_type = 'register'
UNION ALL
SELECT l.company_id, 'fundings', 'foerderkatalog'
FROM public.company_funding_links l;

COMMENT ON VIEW public.v_company_sources IS
  'Which source each shown value of a company came from (company_id, field_name, source_key).';

-- 2. The live definition keeps working under a new name; the stored profile is built from it.
ALTER VIEW public.v_company_profile RENAME TO v_company_profile_live;

CREATE MATERIALIZED VIEW public.company_profile_mv AS
SELECT p.*, COALESCE(src.source_keys, '{}'::text[]) AS source_keys
FROM public.v_company_profile_live p
LEFT JOIN (
    SELECT company_id, array_agg(DISTINCT source_key ORDER BY source_key) AS source_keys
    FROM public.v_company_sources
    GROUP BY company_id
) src ON src.company_id = p.id;

-- Unique on id so it can be refreshed without locking readers out.
CREATE UNIQUE INDEX company_profile_mv_id ON public.company_profile_mv (id);
CREATE INDEX company_profile_mv_name ON public.company_profile_mv (name);
CREATE INDEX company_profile_mv_technologiefeld ON public.company_profile_mv (technologiefeld);
CREATE INDEX company_profile_mv_legal_form ON public.company_profile_mv (legal_form);
CREATE INDEX company_profile_mv_register_status ON public.company_profile_mv (register_status);
CREATE INDEX company_profile_mv_wz_code ON public.company_profile_mv (wz_code text_pattern_ops);
CREATE INDEX company_profile_mv_website ON public.company_profile_mv (name) WHERE website IS NOT NULL;
CREATE INDEX company_profile_mv_fundings ON public.company_profile_mv (name) WHERE funding_count > 0;
CREATE INDEX company_profile_mv_source_keys ON public.company_profile_mv USING gin (source_keys);

-- 3. The name the app and Hasura use.
CREATE VIEW public.v_company_profile AS SELECT * FROM public.company_profile_mv;

COMMENT ON VIEW public.v_company_profile IS
  'The target record per company, as of the last refresh_company_profile(). The live definition is v_company_profile_live.';

-- 4. Refresh. Runs as the owner, because only the owner of a materialized view may refresh it;
--    the writer (DAGs) may call it.
--    ANALYZE afterwards: without fresh statistics Postgres misjudges how rare a source is and a
--    filter by source took 1.8 s instead of 0.3 ms.
CREATE FUNCTION public.refresh_company_profile() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = public, pg_temp
    AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.company_profile_mv;
  ANALYZE public.company_profile_mv;
END
$$;

REVOKE EXECUTE ON FUNCTION public.refresh_company_profile() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.refresh_company_profile() TO ${writer_role};
GRANT SELECT ON public.v_company_sources, public.company_profile_mv, public.v_company_profile,
    public.v_company_profile_live TO ${reader_role};
