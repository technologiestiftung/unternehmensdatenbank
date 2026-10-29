-- Queries that work against the schema as it stands (V1__baseline).
--
-- WHY THIS FILE CARRIES NO EXPECTED OUTPUT. Every number in this repo's documentation has gone
-- stale at least once, and a comment saying "should return 3,529" is a number nobody re-measures.
-- The "Pinned by ..." notes below name constants of the integration tests that pinned these counts
-- on the Osnabrück example data. Those tests were removed with that data on 2026-10-28; the queries
-- still work, the constants no longer exist.
--
-- HOW TO RUN THESE. From the repo root, on the host:
--
--     docker compose exec -T postgres psql -U app -d unternehmensdatenbank -f - < docs/core-queries-current.sql
--
-- or paste one at a time into pgAdmin (http://localhost:5051) or
-- `psql "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app"`.
--
-- Vocabulary, once, because the rest of the file assumes it:
--   observation  - one claim about one field from one source. Never deleted, only un-canonicalised.
--   canonical    - the single winning claim per (company, field), chosen by `pipeline_coordinator`.
--   priority     - `sources.priority`, LOWER WINS, and it is sorted BEFORE confidence.


-- ================================================================================================
-- 1. Canonical websites per source.
--
-- The coverage number for the whole project. Pinned by CANONICAL_WEBSITES and CANONICAL_TOTAL in
-- tests/integration/test_ground_truth_discovery.py.
--
-- COUNT THESE THROUGH `company_canonical_field_sources`, never by grouping on the observation's own
-- `field_name`. The resolver treats `candidate_url` as an alias for `website`, so a company whose
-- winning claim is stored as `candidate_url` still has a canonical website. Grouping on the
-- observation name splits one source's total across two names and reads like lost websites -- the
-- first version of this project's baseline undercounted Brave by 3 for exactly that reason.
-- ================================================================================================

SELECT
  s.key                             AS source,
  s.priority,
  count(*)                          AS canonical_websites
FROM public.company_canonical_field_sources cf
JOIN public.company_field_observations o  ON o.id = cf.company_field_observation_id
JOIN public.company_source_records r      ON r.id = o.company_source_record_id
JOIN public.sources s                     ON s.id = r.source_id
WHERE cf.field_name = 'website'
GROUP BY 1, 2
ORDER BY canonical_websites DESC;

-- The alias itself, if you need to prove it still works. Pinned by
-- CANONICAL_BY_OBSERVATION_NAME. A canonical `website` row whose observation is named
-- `candidate_url` is correct, not a bug.

SELECT
  o.field_name                      AS stored_as,
  count(*)                          AS canonical_websites
FROM public.company_canonical_field_sources cf
JOIN public.company_field_observations o  ON o.id = cf.company_field_observation_id
WHERE cf.field_name = 'website'
GROUP BY 1
ORDER BY 2 DESC;


-- ================================================================================================
-- 2. The view an application should query, and the four industry-ish columns side by side.
--
-- `public.v_companies_enriched` resolves every enriched field through the canonical table.
-- `website_url` is a VIEW column here -- it is not a column of `companies` and never will be.
--
-- THE TWO COLUMNS BELOW ANSWER TWO DIFFERENT QUESTIONS AND MUST NEVER BE MERGED OR COALESCED:
--   technology_field  ONE of Berlin's 77 taxonomy fields. Requires a website somebody fetched.
--   purpose_field     ONE of the same 77, from the register purpose, with NO website requirement
--
-- `technology_field` and `purpose_field` are filled by the classification write path
-- (`enrichment/branch_observations.py`). See docs/branch-classification.md.
-- ================================================================================================

SELECT
  company_name,
  register_status,
  website_url,
  round(website_confidence, 4)      AS website_confidence,
  website_source,
  technology_field,                 -- NULL until the write path lands
  purpose_field                     -- NULL until the write path lands
FROM public.v_companies_enriched
WHERE website_url IS NOT NULL
ORDER BY website_confidence DESC NULLS LAST
LIMIT 25;

-- Field coverage across the whole table, in one row.

SELECT
  count(*)                          AS companies,
  count(website_url)                AS with_website,
  count(technology_field)           AS with_technology_field,
  count(purpose_field)              AS with_purpose_field
FROM public.v_companies_enriched;


-- ================================================================================================
-- 3. `company_managers` -- the duplicate check, and the constraint that makes it hold.
--
-- A bug fixed before the baseline squash, worth understanding before you trust this table. `resolve_ceo_contact` ended in
-- `ON CONFLICT DO NOTHING` with no target, which can only suppress a conflict on a constraint that
-- exists -- and the only one was the primary key on a `gen_random_uuid()` column, which never
-- collides. So every `pipeline_coordinator` run appended a full copy of every name it could
-- resolve, and the coordinator runs after every discovery DAG.
--
-- The two numbers must be EQUAL. Pinned by MANAGER_DISTINCT and
-- test_company_managers_no_duplicates, as an equality rather than a ceiling.
-- ================================================================================================

SELECT
  count(*)                                        AS total_rows,
  count(DISTINCT (company_id, full_name))         AS distinct_managers
FROM public.company_managers;

-- The constraint itself. One row, or the equality above is luck rather than a guarantee.

SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conname = 'uq_company_managers_company_name';


-- ================================================================================================
-- 4. Industry columns 1 and 3 -- WHERE THE BUCKETS ACTUALLY LIVE.
--
-- The buckets (why a company has no field) were computed by the old discovery CLI into files, and
-- only the assigned fields reach the database. The Technologiefeld DAG that replaces it (issue #18)
-- keeps the buckets in Postgres. `NONE` is the sentinel
-- for "no field", not an empty string, and the four reasons for it must never be added together.
--
-- NOTE: `classify` now needs Postgres running. It takes its "does this company have a website
-- somebody actually fetched" gate from `v_companies_enriched` rather than from its own result file.
--
-- Column 2 -- the WZ cluster -- IS in the database, because it is a lookup on a register code and
-- needs no judgement at all. That one you can query:
-- ================================================================================================

SELECT
  left(wz_code, 2)                  AS wz_division,
  count(*)                          AS companies
FROM public.v_companies_enriched
WHERE wz_code IS NOT NULL AND wz_code <> ''
GROUP BY 1
ORDER BY companies DESC
LIMIT 20;

-- How many companies have a WZ code at all. Division = wz_code[:2], and the titles are WZ 2025 --
-- `70.10.4` and `62.10.9` do not exist in WZ 2008, so a 2008 table mismatches silently.

SELECT
  count(*)                                                          AS companies,
  count(*) FILTER (WHERE wz_code IS NOT NULL AND wz_code <> '')      AS with_wz_code
FROM public.v_companies_enriched;


-- ================================================================================================
-- 5. `verified_by` over the canonical websites -- how much of the coverage is actually checked.
--
-- This is the honesty query. `metadata->>'verified_by'` says WHICH LOGIC produced a website;
-- `field_name` only says which field it is about.
--
--   register_number_match  the page carried the company's register number. The strongest evidence.
--   page_content_match     the page named the company, no register number found
--   url_pattern            an Impressum was fetched, nothing matched
--   method_agreement       two independent methods produced the same URL
--   NULL                   nobody fetched anything. These are the register export's own URLs,
--                          winning on priority 50 alone.
--
-- Expect the NULL bucket to be the largest by far. That is the price of the export outranking
-- everything, and it is intended -- see the priority section in the vault's
-- `Confidence and Resolution`.
-- ================================================================================================

SELECT
  coalesce(o.metadata->>'verified_by', '(none - never fetched)')  AS verified_by,
  count(*)                                                        AS canonical_websites,
  round(avg(o.confidence), 4)                                     AS mean_confidence
FROM public.company_canonical_field_sources cf
JOIN public.company_field_observations o ON o.id = cf.company_field_observation_id
WHERE cf.field_name = 'website'
GROUP BY 1
ORDER BY canonical_websites DESC;


-- ================================================================================================
-- 6. The plumbing: sources, priorities, and what has been claimed per field.
--
-- `sources.priority` is the resolver's first sort key and LOWER WINS. It is sorted BEFORE
-- confidence, so a priority difference is absolute: nothing at 100 can beat anything at 50, at any
-- confidence. Pinned by SOURCE_PRIORITIES.
-- ================================================================================================

SELECT key, name, source_type, priority
FROM public.sources
ORDER BY priority, key;

-- Every observation name in the table, with how many rows carry it. Useful for spotting a field
-- name nothing resolves: `directory_url` is deliberately one of those -- it holds third-party pages
-- so they stay queryable as evidence while being out of contention for `website`. Same trick is
-- reserved for `technology_field_withheld` and `purpose_field_withheld`.
--
-- Observations are never deleted, only un-canonicalised, so these counts may rise and never fall.
-- Pinned as a FLOOR by WEBSITE_OBSERVATION_FLOOR for the two website-ish names.

SELECT
  field_name,
  count(*)                                        AS observations,
  count(*) FILTER (WHERE is_canonical)            AS canonical,
  count(DISTINCT company_id)                      AS companies
FROM public.company_field_observations
GROUP BY 1
ORDER BY observations DESC;

-- The four resolver invariants, all of which must return 0. Pinned by test_resolver_invariants.
-- If any is non-zero, `pipeline_coordinator` has a bug and the canonical table is lying.

SELECT
  (SELECT count(*) FROM public.company_canonical_field_sources cf
     JOIN public.company_field_observations o ON o.id = cf.company_field_observation_id
    WHERE cf.field_name = 'website' AND o.confidence < 0.35)     AS website_below_the_bar,
  (SELECT count(*) FROM public.company_canonical_field_sources cf
     JOIN public.company_field_observations o ON o.id = cf.company_field_observation_id
    WHERE cf.field_name = 'website' AND o.field_name = 'directory_url')
                                                                  AS directory_promoted_to_website,
  (SELECT count(*) FROM (SELECT company_id, field_name
                           FROM public.company_field_observations
                          WHERE is_canonical GROUP BY 1, 2 HAVING count(*) > 1) t)
                                                                  AS two_canonical_for_one_field,
  (SELECT count(*) FROM public.company_canonical_field_sources cf
     JOIN public.company_field_observations o ON o.id = cf.company_field_observation_id
    WHERE NOT o.is_canonical)                                     AS pointer_to_noncanonical_row;

-- The register status split, because it changes how every other number should be read: over half
-- the table is dissolved. Pinned nowhere -- it comes straight from the import.

SELECT coalesce(register_status, '(null)') AS register_status, count(*)
FROM public.v_companies_enriched
GROUP BY 1
ORDER BY 2 DESC;
