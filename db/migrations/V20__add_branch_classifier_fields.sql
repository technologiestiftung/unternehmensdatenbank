-- V20: reserve the two industry-classification columns in the database. SCHEMA ONLY.
--
-- WHY. Columns 1 and 3 of the industry classification live only in two files:
-- `branches.jsonl` holds 403 Technologiefelder and `purpose_branches.jsonl` holds 1,200
-- `purpose_field` values. No migration defined them, no observation carried them, the resolver had
-- no logical field for either, and `v_companies_enriched` could not show them. Anyone consuming the
-- platform saw `website`, `cluster` and `tech_field` and could not reach either industry column
-- except by opening a file on somebody's laptop.
--
-- WHAT THIS DOES NOT DO. It writes no rows. All six new view columns read NULL for all 14,443
-- companies, because the write path -- `classify` and `classify-purpose` emitting observations, plus
-- the two `resolve_*` wrappers the coordinator would need -- lands after the Berlin import. The
-- point of splitting it that way: both columns' VALUES are Osnabrück figures that Berlin replaces
-- wholesale, so migrating the shape now and the data later avoids pinning counts that are about to
-- be thrown away, and lets the fixed confidences below be judged against the population that will
-- actually be served.

-- ---------------------------------------------------------------------------------------------
-- 1. The source that will write them.
--
-- A source of its own rather than reusing `combined_discovery`. `sources.priority` governs EVERY
-- field a source writes, so sharing the row would couple the Technologiefeld's resolution order to
-- any future change of the website priority -- and that priority has already been edited once, by
-- `import_northdata_websites.py`, as a side effect of an import.
--
-- `name` is NOT NULL with no default, which is why it is spelled out here. `key` is UNIQUE, so
-- `ON CONFLICT` keeps this migration safe on a restored or re-baselined database.
--
-- The confidences in the metadata are a POLICY, not a measurement, and the metadata says so
-- because the row is where a reader will look. These columns have no calibrated confidence:
-- `score` is an unbounded keyword-weight sum (3.467 to 36.643 over column 1's 403 rows) and 590 of
-- column 3's 1,200 assigned rows have no score at all, because Tier B asks a model to choose from
-- all 77 fields with no keyword to anchor it. `company_field_observations.confidence` is
-- NUMERIC(5,4) CHECK (0..1) and the resolver excludes NULL, so a number has to be chosen. These
-- three are ordered by how much evidence stands behind them, and the real score travels in
-- `metadata.score` where a later calibration can find it.
-- ---------------------------------------------------------------------------------------------

INSERT INTO public.sources (key, name, source_type, priority, metadata, created_at, updated_at)
VALUES (
  'branch_classifier',
  'Branch Classifier (Berlin 77-field taxonomy)',
  'classifier',
  100,
  jsonb_build_object(
    'writes', jsonb_build_array(
      'technologiefeld', 'purpose_field',
      'technologiefeld_withheld', 'purpose_field_withheld'
    ),
    'confidence_basis', 'fixed_by_decided_by_v20',
    'confidence_by_decided_by', jsonb_build_object(
      'keywords', 0.60, 'model_tiebreak', 0.55, 'model_direct', 0.50
    ),
    'note', 'Confidence is policy, not measurement: these columns score by unbounded keyword '
            'weight and 590 of 1,200 purpose fields have no score at all. The real score is in '
            'each observation metadata.score. See docs/branch-classification.md.',
    'writes_nothing_yet', 'V20 is schema only; the write path lands after the Berlin import.'
  ),
  now(),
  now()
)
ON CONFLICT (key) DO NOTHING;

-- ---------------------------------------------------------------------------------------------
-- 2. The upsert key, widened from six field names to ten.
--
-- Without this the four new names are not idempotent: a second `classify` run would append a
-- duplicate row per company rather than updating in place, which is what V17 and V18 existed to
-- stop. `technologiefeld_withheld` and `purpose_field_withheld` are included because they are
-- written the same way -- see the note on them below.
--
-- **`insert_text_observation` in `airflow/dags/source_enrichment_common.py` repeats this predicate
-- verbatim in its `ON CONFLICT (...) WHERE ...` clause, and the two must stay identical.** Postgres
-- infers a partial index from the predicate, so a name present here and absent there cannot be
-- upserted at all. Both were widened in the same commit as this migration.
-- ---------------------------------------------------------------------------------------------

DROP INDEX IF EXISTS public.uq_obs_company_field_source_value;

CREATE UNIQUE INDEX uq_obs_company_field_source_value
    ON public.company_field_observations (company_id, field_name, source_id, value_text)
    WHERE field_name IN (
        'website', 'candidate_url', 'directory_url',
        'discovery_tier', 'search_query', 'osm_id',
        'technologiefeld', 'purpose_field',
        'technologiefeld_withheld', 'purpose_field_withheld'
    );

COMMENT ON INDEX public.uq_obs_company_field_source_value IS
    'Upsert key for the ten field names combined_discovery/observations.py and the branch '
    'classifier write, so a re-run updates instead of appending. Partial: 71 duplicate groups '
    'remain in ceo_name_or_contact, email, phone, website_search_query and cluster, and each needs '
    'its own review first. insert_text_observation repeats this predicate and must match it exactly.';

-- ---------------------------------------------------------------------------------------------
-- 3. The two withheld field names, which nothing will ever resolve.
--
-- `technologiefeld_withheld` will hold the 535 fields the website rule withholds -- keywords found
-- a field, no verified website exists, so it is kept and not published -- and
-- `purpose_field_withheld` the 17 that column 3's 40-character floor costs.
--
-- They are separate FIELD NAMES rather than a low confidence on purpose, reusing the pattern
-- `directory_url` already established: no `resolve_*` function names them, so no threshold change
-- can ever publish them by accident. A low-confidence row in the real field would be one edit away
-- from release. There is nothing to create here -- a field name needs no DDL -- but the decision
-- belongs in the migration that reserves the other two, because that is where somebody will look
-- for it.

-- ---------------------------------------------------------------------------------------------
-- 4. The view, with six columns appended.
--
-- APPENDED, and that word is load-bearing. `CREATE OR REPLACE VIEW` may only add columns at the
-- end; it cannot reorder or retype an existing one. Two views depend on this one --
-- `v_website_provenance` (V15) reads it as `FROM public.v_companies_enriched v` and
-- `v_company_full` (V16) as `LEFT JOIN public.v_companies_enriched e` -- so the alternative was
-- `DROP ... CASCADE` and recreating all three. Neither dependent uses `v.*` or `e.*`; both name
-- their columns explicitly, so appending changes neither one's output. Checked before writing this,
-- because a star-select in either would have made this a silent change of shape rather than a safe
-- addition.
--
-- Everything above `register_status` is V13's text unchanged.
-- ---------------------------------------------------------------------------------------------

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
  c.register_status,
  -- NEW in V20. NULL for every company until the write path lands.
  obs_tfeld.value_text            AS technologiefeld,
  obs_tfeld.confidence            AS technologiefeld_confidence,
  src_tfeld.key                   AS technologiefeld_source,
  obs_purpose.value_text          AS purpose_field,
  obs_purpose.confidence          AS purpose_field_confidence,
  src_purpose.key                 AS purpose_field_source
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
LEFT JOIN public.sources src_emp ON src_emp.id = cfs_emp.source_id
-- NEW in V20: the same three-join shape as every field above.
LEFT JOIN public.company_canonical_field_sources cfs_tfeld
       ON cfs_tfeld.company_id = c.id AND cfs_tfeld.field_name = 'technologiefeld'
LEFT JOIN public.company_field_observations obs_tfeld
       ON obs_tfeld.id = cfs_tfeld.company_field_observation_id
LEFT JOIN public.sources src_tfeld ON src_tfeld.id = cfs_tfeld.source_id
LEFT JOIN public.company_canonical_field_sources cfs_purpose
       ON cfs_purpose.company_id = c.id AND cfs_purpose.field_name = 'purpose_field'
LEFT JOIN public.company_field_observations obs_purpose
       ON obs_purpose.id = cfs_purpose.company_field_observation_id
LEFT JOIN public.sources src_purpose ON src_purpose.id = cfs_purpose.source_id;

COMMENT ON VIEW public.v_companies_enriched IS
    'The view applications should query. THREE industry-ish columns live here and they are three '
    'different questions -- getting that wrong was the expensive mistake in this project. '
    'tech_field: free-text LLM output from the search providers, 508 rows, no controlled '
    'vocabulary, whatever the model felt like typing. technologiefeld: one of Berlin''s 77 '
    'taxonomy fields, requires a website somebody actually fetched, written by branch_classifier. '
    'cluster: the same kind of free text as tech_field. They must never be merged or coalesced, '
    'and a WZ code may never fill technologiefeld. purpose_field is a fourth question again: what '
    'the register says the company was set up to do, with no website requirement at all. '
    'See docs/branch-classification.md and docs/purpose-classification.md.';
