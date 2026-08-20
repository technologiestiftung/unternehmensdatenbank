-- Add provenance to v_discovery_results: when a fact was found, and the raw score behind it.
--
-- V11 showed WHAT was found and HOW it was established, but not WHEN, and not the evidence
-- tally that produced the confidence. Both had to be fetched with a hand-written join against
-- company_field_observations every time, which is exactly what a view is for.
--
-- "when" matters because these numbers move. The owner-extraction fix on 11 August 2026 more
-- than doubled how many managers we read off Impressum pages, so a row found before that date
-- and a row found after are not comparable. Without a timestamp there is no way to tell them
-- apart.
--
-- "score" matters because confidence hides the reasoning. Confidence is derived from score:
--
--   score >= 100                     -> confidence 1.0
--   score 0..99                      -> confidence 0.35 + 0.30 * name_uniqueness (0.365..0.65)
--   score < 0                        -> no website row is written at all
--
-- and score itself is just a tally: +100 if the register number on the page matches ours, +20
-- if the postcode matches, -200 if the page shows a DIFFERENT register number. Seeing 120
-- rather than 1.0000 tells you both checks passed, which 1.0000 alone does not.
--
-- Views only. No table or column changes.

DROP VIEW IF EXISTS public.v_discovery_results;

CREATE VIEW public.v_discovery_results AS
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
  -- The evidence tally the confidence was derived from. See the header for the scale.
  (obs_web.metadata ->> 'score')::INT       AS website_score,
  obs_web.metadata ->> 'provider'           AS website_provider,
  src_web.key                               AS website_source,
  -- The exact page that was downloaded and checked, often a subpage of website_url.
  obs_web.metadata ->> 'matched_url'        AS website_matched_url,
  obs_web.observed_at                       AS website_found_at,

  -- An unchecked model guess, shown ONLY when there is no verified website. A lead to follow
  -- by hand, not an answer.
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
  owner.observed_at                         AS owner_found_at,

  -- Which discovery DAGs have looked at this company at all. Empty means none have run on it,
  -- which is a different thing from "we looked and found nothing".
  COALESCE(seen.source_keys, ARRAY[]::TEXT[]) AS processed_by,
  seen.last_tried_at                          AS last_tried_at

FROM public.companies c

LEFT JOIN public.company_canonical_field_sources cfs_web
       ON cfs_web.company_id = c.id AND cfs_web.field_name = 'website'
LEFT JOIN public.company_field_observations obs_web
       ON obs_web.id = cfs_web.company_field_observation_id
LEFT JOIN public.sources src_web
       ON src_web.id = cfs_web.source_id

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

-- The owner. Taken from the observations rather than from company_managers, because that table
-- keeps only the name -- it does not record which observation it came from, so joining it could
-- not tell a checked Impressum from a model's guess. Selection mirrors
-- pipeline_coordinator.resolver.resolve_ceo_contact: lowest source priority first, then highest
-- confidence, minimum confidence 0.50.
LEFT JOIN LATERAL (
  SELECT o.value_text, o.confidence, o.metadata, o.observed_at
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
-- per company unconditionally, so it is the honest record of "we tried", and its timestamp is
-- the honest record of "when we last tried".
LEFT JOIN LATERAL (
  SELECT array_agg(DISTINCT s.key ORDER BY s.key) AS source_keys,
         max(o.observed_at)                       AS last_tried_at
  FROM public.company_field_observations o
  JOIN public.sources s ON s.id = o.source_id
  WHERE o.company_id = c.id AND o.field_name = 'search_query'
) seen ON TRUE;

COMMENT ON VIEW public.v_discovery_results IS
  'One row per company: the website and owner discovery found, how each was established, when, '
  'and the evidence score behind it. website_verified_by = register_number_match or url_pattern '
  'means the page was downloaded and checked; llm_snippet means a model guessed and nothing was '
  'checked. website_score is the raw tally: +100 register number matched, +20 postcode matched, '
  '-200 the page showed a different register number. unverified_website_guess is populated only '
  'when website_url is NULL. processed_by empty means nobody has looked at this company yet, '
  'which is not the same as looking and finding nothing. Compare website_found_at against a '
  'known fix date before treating old and new rows as equivalent.';
