-- Wipe everything one search provider wrote, so a corrected run starts clean.
--
-- Needed because the discovery DAGs skip companies that already have a
-- company_source_records row for their source: without this, a re-run picks up where the
-- previous one stopped instead of re-doing the companies whose observations were wrong.
--
-- Set the provider key, then run inside a transaction and check the counts before COMMIT:
--   docker compose exec -T postgres psql -U app -d unternehmensdatenbank \
--     -v key=parallel_search_discovery -f /dev/stdin < scripts/reset_provider_observations.sql
--
-- NOTE company_managers cannot be scoped by source — it has no source_id column (see the
-- duplicate-manager finding on main) — so the delete below removes ALL manager rows. That is
-- correct only while this pipeline is their sole writer. Check before running it.

BEGIN;

\set provider_key :key

DELETE FROM public.company_canonical_field_sources
WHERE source_id = (SELECT id FROM public.sources WHERE key = :'provider_key');

DELETE FROM public.company_field_observations
WHERE source_id = (SELECT id FROM public.sources WHERE key = :'provider_key');

DELETE FROM public.company_source_records
WHERE source_id = (SELECT id FROM public.sources WHERE key = :'provider_key');

DELETE FROM public.source_sync_runs
WHERE source_id = (SELECT id FROM public.sources WHERE key = :'provider_key');

DELETE FROM public.company_managers;

-- Recomputed by pipeline_coordinator on its next run.
UPDATE public.companies SET data_quality_score = NULL WHERE data_quality_score IS NOT NULL;

SELECT
    (SELECT count(*) FROM public.company_field_observations)      AS observations_left,
    (SELECT count(*) FROM public.company_canonical_field_sources) AS canonical_left,
    (SELECT count(*) FROM public.company_source_records)          AS source_records_left,
    (SELECT count(*) FROM public.companies)                       AS companies_kept;

-- Replace with COMMIT once the counts look right.
ROLLBACK;
