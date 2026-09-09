-- V17: give website observations a key an upsert can use.
--
-- WHY. `scripts/import_combined_discovery.py` has to DELETE this source's previous `website`
-- observations before writing, because nothing in the table stops it writing a second copy of a row
-- it already wrote. `company_canonical_field_sources` cascades off those observations, so between the
-- DELETE and the next `pipeline_coordinator` run every website that source had won is un-published.
-- That happened on 2026-09-03 and coverage sat at 2,955 instead of 3,513 for four days, because the
-- Brave runs that followed failed and a failed task never reaches the coordinator.
--
-- With a unique key the importer can upsert instead, and there is no window at all.
--
-- WHY THE KEY INCLUDES value_text. `candidate_url` is legitimately multi-valued: `url_discovery`
-- writes one row per verified candidate, up to 5 for a single company/source pair today. A key of
-- (company_id, field_name, source_id) would collapse those five into one and destroy the candidate
-- list. Keyed on the value as well, a re-run of the same URL updates and a new URL inserts, which is
-- what "one observation per claim" means.
--
-- WHY IT IS PARTIAL. 120 duplicate (company_id, field_name, source_id, value_text) groups exist in
-- this table. Only 49 are in the two fields this migration is about — 26 `candidate_url` and 23
-- `website`. The other 71 are in `ceo_name_or_contact` (28), `email` (11), `phone` (9),
-- `website_search_query` (8), `search_query` (8), `directory_url` (4) and `cluster` (3). An
-- unqualified unique index would fail on those, and a failed Flyway migration blocks every later one
-- until somebody repairs the history by hand. Each of those fields deserves its own look before its
-- duplicates are thrown away — the 28 duplicate CEO names in particular, next to a
-- `company_managers` table holding 7,167 rows for 1,047 distinct names.
--
-- WHAT THIS COSTS. The append-only history of `website` and `candidate_url` for a repeated URL. A
-- provider DAG that re-runs over the same company and finds the same URL now updates its row instead
-- of adding a second one, so "what did Brave say last month about this URL" stops being answerable
-- from this table. That is the trade: idempotency for one source's history of an unchanged answer. A
-- *different* URL still inserts a new row, so a genuinely changed answer keeps its predecessor.

-- ---------------------------------------------------------------------------------------------
-- 1. Dedupe the 49 groups, keeping the highest-confidence row in each.
--
-- `confidence DESC NULLS LAST` then `id` — a NULL confidence loses to any number, and the tie-break
-- on id is there only to make the choice deterministic rather than arbitrary. Both the survivor set
-- and the delete are scoped to the two field names, so no row outside them is touched.
-- ---------------------------------------------------------------------------------------------

DELETE FROM public.company_field_observations
WHERE field_name IN ('website', 'candidate_url')
  AND id NOT IN (
    SELECT DISTINCT ON (company_id, field_name, source_id, value_text) id
    FROM public.company_field_observations
    WHERE field_name IN ('website', 'candidate_url')
    ORDER BY company_id, field_name, source_id, value_text,
             confidence DESC NULLS LAST, id
  );

-- ---------------------------------------------------------------------------------------------
-- 2. The key itself.
--
-- `value_text` is NOT NULL on all 16,986 rows in this table today, so no group is silently split by
-- NULL-distinctness. That does not hold for `insert_number_observation`, whose rows leave
-- `value_text` NULL — nothing calls it yet, and when something does it needs its own key on
-- `value_number`, not this one.
-- ---------------------------------------------------------------------------------------------

CREATE UNIQUE INDEX IF NOT EXISTS uq_obs_company_field_source_value
    ON public.company_field_observations (company_id, field_name, source_id, value_text)
    WHERE field_name IN ('website', 'candidate_url');

COMMENT ON INDEX public.uq_obs_company_field_source_value IS
    'Upsert key for website/candidate_url observations, so a re-run updates instead of appending. '
    'Partial: 71 duplicate groups remain in other fields and each needs its own review first. '
    'Inserts must name the predicate to infer this index: '
    'ON CONFLICT (company_id, field_name, source_id, value_text) '
    'WHERE field_name IN (''website'', ''candidate_url'').';
