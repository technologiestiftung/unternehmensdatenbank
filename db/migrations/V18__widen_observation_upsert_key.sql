-- V18: extend the V17 upsert key to the six fields `combined_discovery/observations.py` writes.
--
-- WHY. V17 keyed `website` and `candidate_url`, which was all the import bridge needed. Step 3 of
-- the unification switches that bridge to `observations.py`, which writes four more field names:
-- `directory_url`, `discovery_tier`, `search_query` and `osm_id`. Unkeyed, a second run appends
-- roughly five extra rows per company -- about 58,000 over the full table -- which is precisely the
-- class of problem V17 and the upsert existed to remove.
--
-- WHY THESE SIX AND NOT THE WHOLE TABLE. 71 duplicate groups remain in fields nothing is writing
-- yet: `ceo_name_or_contact` 28, `email` 11, `phone` 9, `website_search_query` 8, `cluster` 3. Each
-- deserves its own look before its duplicates are thrown away -- the CEO names most of all, sitting
-- next to a `company_managers` table holding 7,167 rows for 1,047 distinct names. A table-wide
-- unique index would have to delete all of them today, and a failed Flyway migration blocks every
-- later one until the history is repaired by hand.
--
-- Note the asymmetry this leaves: `search_query` is keyed and `website_search_query` is not, because
-- `observations.py` writes the first and not the second. The provider DAGs write both.
--
-- WHY THE SIX ARE SAFE UNDER A VALUE-INCLUSIVE KEY.
--   `discovery_tier`   three rows per company, values `url_pattern` / `osm` / `ollama_searxng`
--   `directory_url`    multi-valued, up to 5 per company/source, distinct URLs
--   `candidate_url`    multi-valued, same shape
--   `search_query`     one row, one value
--   `osm_id`           one row, one value
--   `website`          one row per source
-- Keyed on the value as well, a repeat of the same claim updates and a new claim inserts.
--
-- WHAT THIS COSTS, BEYOND V17'S TRADE. `search_query` and `directory_url` are written by the
-- provider DAGs too, so this changes their behaviour as well: a DAG re-running over a company it
-- has already seen now updates those rows instead of appending. For an unchanged value that history
-- was a duplicate; for a changed one a new row still appears.

-- ---------------------------------------------------------------------------------------------
-- 1. Dedupe the 12 groups that remain in the two affected fields: 8 `search_query`, 4
--    `directory_url`. Same rule as V17 -- keep the highest confidence, NULLs last, id as the
--    deterministic tie-break.
-- ---------------------------------------------------------------------------------------------

DELETE FROM public.company_field_observations
WHERE field_name IN ('directory_url', 'discovery_tier', 'search_query', 'osm_id')
  AND id NOT IN (
    SELECT DISTINCT ON (company_id, field_name, source_id, value_text) id
    FROM public.company_field_observations
    WHERE field_name IN ('directory_url', 'discovery_tier', 'search_query', 'osm_id')
    ORDER BY company_id, field_name, source_id, value_text,
             confidence DESC NULLS LAST, id
  );

-- ---------------------------------------------------------------------------------------------
-- 2. Replace V17's two-field index with the six-field one. Dropped and recreated rather than
--    added alongside: two overlapping partial unique indexes on the same columns would both be
--    valid arbiters, and which one `ON CONFLICT` infers is not something to leave to chance.
--
--    `insert_text_observation` names this exact predicate in its conflict target. The two must be
--    kept in step -- a row whose field_name is outside the list satisfies no predicate, finds no
--    arbiter index, and inserts as it always did.
-- ---------------------------------------------------------------------------------------------

DROP INDEX IF EXISTS public.uq_obs_company_field_source_value;

CREATE UNIQUE INDEX uq_obs_company_field_source_value
    ON public.company_field_observations (company_id, field_name, source_id, value_text)
    WHERE field_name IN (
        'website', 'candidate_url', 'directory_url',
        'discovery_tier', 'search_query', 'osm_id'
    );

COMMENT ON INDEX public.uq_obs_company_field_source_value IS
    'Upsert key for the six fields combined_discovery/observations.py writes, so a re-run updates '
    'instead of appending. Partial: 71 duplicate groups remain in ceo_name_or_contact, email, '
    'phone, website_search_query and cluster, and each needs its own review first. Inserts must '
    'repeat this predicate to infer the index -- see insert_text_observation.';
