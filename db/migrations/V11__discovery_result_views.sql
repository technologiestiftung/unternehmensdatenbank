-- Two read-only views for looking at discovery results in pgAdmin.
--
-- What problem they solve: the answers the pipeline produces are spread over four tables and
-- a JSONB column. v_companies_enriched (V10) joins the winning website through
-- company_canonical_field_sources, but it drops the three things you need to judge a result:
--
--   1. the owner's name, which resolve_ceo_contact writes to company_managers, not to
--      company_canonical_field_sources
--   2. how the website was established, which lives in
--      company_field_observations.metadata->>'verified_by'
--   3. which search provider found it, in metadata->>'provider'
--
-- Point 2 is the important one. "verified_by" has three values and they mean very different
-- things:
--
--   register_number_match  we downloaded the page, found its Impressum -- the legal contact
--                          page every German business site must have -- and the commercial
--                          register number on it matched ours. Confidence 1.0.
--   url_pattern            we downloaded the page and the Impressum was consistent with the
--                          company, but no register number confirmed it. Confidence 0.365-0.65.
--   llm_snippet            a language model read some search results and told us a URL.
--                          Nothing was checked. Capped at 0.34 confidence on purpose, which is
--                          below the 0.35 the resolver needs, so it never becomes the official
--                          answer on its own.
--
-- Measured on 100 companies, the first two together find a website for 29 of them, while
-- counting llm_snippet as well would claim 40. Reporting 40 would be dishonest. These views
-- keep the two apart so nobody has to guess which number they are looking at.
--
-- Views only -- no table or column is changed here, so nothing existing can break.

-- ---------------------------------------------------------------------------------------
-- One row per company. Every company, including the ones where nothing was found: a view
-- that hides the misses would make coverage look better than it is.
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_discovery_results AS
SELECT
  c.id,
  c.company_name,
  c.address,
  c.postcode,
  c.city,
  c.registration_number,
  c.data_quality_score,

  -- The website the pipeline actually stands behind. NULL means nothing cleared 0.35.
  obs_web.value_text                        AS website_url,
  obs_web.confidence                        AS website_confidence,
  obs_web.metadata ->> 'verified_by'        AS website_verified_by,
  obs_web.metadata ->> 'provider'           AS website_provider,
  src_web.key                               AS website_source,
  -- The exact page that was checked, which is often a subpage of website_url.
  obs_web.metadata ->> 'matched_url'        AS website_matched_url,

  -- An unchecked model guess, shown ONLY when there is no verified website. Useful as a lead
  -- to follow by hand; not an answer. Always read alongside website_url being NULL.
  guess.value_text                          AS unverified_website_guess,
  guess.metadata ->> 'provider'             AS unverified_website_provider,

  owner.value_text                          AS owner_name,
  owner.confidence                          AS owner_confidence,
  -- 'impressum' = read off the company's own legal page. 'llm_snippet' = a model's reading of
  -- a search result, never checked against the company's own site.
  COALESCE(owner.metadata ->> 'source', owner.metadata ->> 'verified_by')
                                            AS owner_found_via,
  owner.metadata ->> 'provider'             AS owner_provider,
  owner.metadata ->> 'imprint_url'          AS owner_imprint_url,

  -- Which discovery DAGs have looked at this company at all. Empty means none have run on it,
  -- which is a different thing from "we looked and found nothing".
  COALESCE(seen.source_keys, ARRAY[]::TEXT[]) AS processed_by

FROM public.companies c

-- The canonical website: whichever observation pipeline_coordinator promoted.
LEFT JOIN public.company_canonical_field_sources cfs_web
       ON cfs_web.company_id = c.id AND cfs_web.field_name = 'website'
LEFT JOIN public.company_field_observations obs_web
       ON obs_web.id = cfs_web.company_field_observation_id
LEFT JOIN public.sources src_web
       ON src_web.id = cfs_web.source_id

-- Best model guess, only surfaced when nothing was verified.
LEFT JOIN LATERAL (
  SELECT o.value_text, o.metadata
  FROM public.company_field_observations o
  WHERE o.company_id = c.id
    AND o.field_name = 'website'
    AND o.metadata ->> 'verified_by' = 'llm_snippet'
    AND cfs_web.id IS NULL
  ORDER BY o.confidence DESC NULLS LAST, o.observed_at DESC
  LIMIT 1
) guess ON TRUE

-- The owner. Taken straight from the observations rather than from company_managers, because
-- company_managers keeps only the name -- it does not record which observation it came from,
-- so joining it could not tell a checked Impressum from a model's guess. Selection mirrors
-- pipeline_coordinator.resolver.resolve_ceo_contact: lowest source priority first, then
-- highest confidence, minimum confidence 0.50.
LEFT JOIN LATERAL (
  SELECT o.value_text, o.confidence, o.metadata
  FROM public.company_field_observations o
  JOIN public.sources s ON s.id = o.source_id
  WHERE o.company_id = c.id
    AND o.field_name = 'ceo_name_or_contact'
    AND o.value_text IS NOT NULL
    AND trim(o.value_text) <> ''
    AND COALESCE(o.confidence, 0) >= 0.50
  ORDER BY s.priority ASC, o.confidence DESC NULLS LAST
  LIMIT 1
) owner ON TRUE

-- Every source that wrote a search_query observation for this company. The runner writes one
-- per company unconditionally, so it is the honest record of "we tried".
LEFT JOIN LATERAL (
  SELECT array_agg(DISTINCT s.key ORDER BY s.key) AS source_keys
  FROM public.company_field_observations o
  JOIN public.sources s ON s.id = o.source_id
  WHERE o.company_id = c.id AND o.field_name = 'search_query'
) seen ON TRUE;

COMMENT ON VIEW public.v_discovery_results IS
  'One row per company: the website and owner discovery found, and how each was established. '
  'website_verified_by = register_number_match or url_pattern means the page was downloaded '
  'and checked; llm_snippet means a model guessed it and nothing was checked. '
  'unverified_website_guess is populated only when website_url is NULL. '
  'processed_by lists the discovery DAGs that have run on this company -- an empty array '
  'means nobody has looked yet, which is not the same as looking and finding nothing.';


-- ---------------------------------------------------------------------------------------
-- One row per discovery source. This is "what percent of companies can we find this for?"
-- as a live query instead of a number written down in a markdown file.
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_discovery_scorecard AS
WITH per_source AS (
  SELECT
    s.id   AS source_id,
    s.key  AS source_key,
    s.name AS source_name,
    s.priority,
    -- Denominator: companies this source actually ran on. The runner writes a search_query
    -- observation for every company it processes, whether or not it found anything, so this
    -- counts attempts and not successes. Using the successes as the denominator would make
    -- every source look perfect.
    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name = 'search_query')                       AS companies_tried,

    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name = 'website'
                AND o.metadata ->> 'verified_by'
                    IN ('register_number_match', 'url_pattern'))         AS website_verified,
    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name = 'website'
                AND o.metadata ->> 'verified_by' = 'register_number_match')
                                                                         AS website_register_match,
    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name = 'website'
                AND o.metadata ->> 'verified_by' = 'llm_snippet')        AS website_guess_only,

    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name = 'ceo_name_or_contact'
                AND o.metadata ->> 'source' = 'impressum')               AS owner_from_impressum,
    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name = 'ceo_name_or_contact'
                AND o.metadata ->> 'verified_by' = 'llm_snippet')        AS owner_from_llm,

    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name IN ('phone', 'email'))                  AS phone_or_email,
    -- Pages that mention the company but are not its own site: register aggregators,
    -- directories, insolvency trackers. Written under a field name the resolver ignores, so
    -- they can never become the canonical website. A high number here means the search is
    -- being pulled toward pages *about* the company instead of the company's own page.
    COUNT(DISTINCT o.company_id)
      FILTER (WHERE o.field_name = 'directory_url')                      AS hit_directories_only
  FROM public.sources s
  LEFT JOIN public.company_field_observations o ON o.source_id = s.id
  GROUP BY s.id, s.key, s.name, s.priority
)
SELECT
  source_key,
  source_name,
  priority,
  companies_tried,
  website_verified,
  ROUND(100.0 * website_verified   / NULLIF(companies_tried, 0), 1) AS website_verified_pct,
  website_register_match,
  website_guess_only,
  ROUND(100.0 * website_guess_only / NULLIF(companies_tried, 0), 1) AS website_guess_only_pct,
  owner_from_impressum,
  ROUND(100.0 * owner_from_impressum / NULLIF(companies_tried, 0), 1) AS owner_verified_pct,
  owner_from_llm,
  ROUND(100.0 * owner_from_llm     / NULLIF(companies_tried, 0), 1) AS owner_guess_pct,
  phone_or_email,
  hit_directories_only
FROM per_source
WHERE companies_tried > 0
ORDER BY website_verified_pct DESC NULLS LAST, source_key;

COMMENT ON VIEW public.v_discovery_scorecard IS
  'One row per discovery source, answering "for what percent of companies does this source '
  'find a website / an owner?". companies_tried is the denominator: companies the source ran '
  'on, counted from the search_query observation it writes for every company whether it '
  'succeeds or not. website_verified_pct counts only websites whose page was downloaded and '
  'checked; website_guess_only_pct counts unchecked model guesses, which never become the '
  'official answer. Sources that have never run are omitted.';
