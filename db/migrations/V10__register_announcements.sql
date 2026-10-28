-- The latest register announcement of each company, shown as a hint next to its register status.
--
-- The DAG `register_announcements` reads Berlin's announcements (Registerbekanntmachungen) from
-- handelsregister.de once a week: deletion announcements, mergers and conversions, new documents.
-- The coordinator publishes the latest per company as `register_announcement`; its date comes from the
-- observation's metadata. A deletion announcement is a hint, never a change of `register_status`: the
-- court deletes later, or not.
--
-- V10: V9 is the vote-delete migration renumbered in #33.

-- 1. The live profile gains the two columns at the end; every earlier column stays as it was.
CREATE OR REPLACE VIEW public.v_company_profile_live AS
SELECT e.id,
    e.company_name AS name,
    e.employee_count,
    e.revenue,
    COALESCE(fund.funding_count, 0::bigint) AS funding_count,
    fund.funding_total_eur,
    e.wz_code,
    e.established_at,
    e.address,
    c.postcode,
    c.city,
    e.legal_form,
    e.cluster,
    e.technologiefeld,
    e.website_url AS website,
    mgr.managers AS management,
    e.business_purpose,
    obs_deeptech.value_text AS deeptech_class,
    obs_deeptech.confidence AS deeptech_confidence,
    obs_ai.value_text::boolean AS is_ai,
    obs_amazon.value_text AS amazon_effect,
    e.register_status,
    GREATEST(e.data_last_updated_at, e.canonical_updated_at) AS updated_at,
    obs_keywords.value_text AS activity_keywords,
    obs_announce.value_text AS register_announcement,
    (obs_announce.metadata ->> 'published_on')::date AS register_announcement_date
FROM public.v_companies_enriched e
    JOIN public.companies c ON c.id = e.id
    LEFT JOIN LATERAL (SELECT count(*) AS funding_count, sum(f.amount_eur) AS funding_total_eur
                       FROM public.v_company_fundings f
                       WHERE f.company_id = e.id) fund ON true
    LEFT JOIN LATERAL (SELECT string_agg(DISTINCT cm.full_name, '; '::text ORDER BY cm.full_name) AS managers
                       FROM public.company_managers cm
                       WHERE cm.company_id = e.id AND cm.is_current) mgr ON true
    LEFT JOIN public.company_canonical_field_sources cfs_deeptech
           ON cfs_deeptech.company_id = e.id AND cfs_deeptech.field_name = 'deeptech_class'
    LEFT JOIN public.company_field_observations obs_deeptech ON obs_deeptech.id = cfs_deeptech.company_field_observation_id
    LEFT JOIN public.company_canonical_field_sources cfs_ai
           ON cfs_ai.company_id = e.id AND cfs_ai.field_name = 'is_ai'
    LEFT JOIN public.company_field_observations obs_ai ON obs_ai.id = cfs_ai.company_field_observation_id
    LEFT JOIN public.company_canonical_field_sources cfs_amazon
           ON cfs_amazon.company_id = e.id AND cfs_amazon.field_name = 'amazon_effect'
    LEFT JOIN public.company_field_observations obs_amazon ON obs_amazon.id = cfs_amazon.company_field_observation_id
    LEFT JOIN public.company_canonical_field_sources cfs_keywords
           ON cfs_keywords.company_id = e.id AND cfs_keywords.field_name = 'activity_keywords'
    LEFT JOIN public.company_field_observations obs_keywords ON obs_keywords.id = cfs_keywords.company_field_observation_id
    LEFT JOIN public.company_canonical_field_sources cfs_announce
           ON cfs_announce.company_id = e.id AND cfs_announce.field_name = 'register_announcement'
    LEFT JOIN public.company_field_observations obs_announce ON obs_announce.id = cfs_announce.company_field_observation_id;

-- 2. Rebuilt to carry the columns, as in V8: the stored profile froze `p.*` when it was created.
DROP VIEW public.v_company_profile;
DROP MATERIALIZED VIEW public.company_profile_mv;

CREATE MATERIALIZED VIEW public.company_profile_mv AS
SELECT p.*, COALESCE(src.source_keys, '{}'::text[]) AS source_keys
FROM public.v_company_profile_live p
LEFT JOIN (
    SELECT company_id, array_agg(DISTINCT source_key ORDER BY source_key) AS source_keys
    FROM public.v_company_sources
    GROUP BY company_id
) src ON src.company_id = p.id;

CREATE UNIQUE INDEX company_profile_mv_id ON public.company_profile_mv (id);
CREATE INDEX company_profile_mv_name ON public.company_profile_mv (name);
CREATE INDEX company_profile_mv_technologiefeld ON public.company_profile_mv (technologiefeld);
CREATE INDEX company_profile_mv_legal_form ON public.company_profile_mv (legal_form);
CREATE INDEX company_profile_mv_register_status ON public.company_profile_mv (register_status);
CREATE INDEX company_profile_mv_wz_code ON public.company_profile_mv (wz_code text_pattern_ops);
CREATE INDEX company_profile_mv_website ON public.company_profile_mv (name) WHERE website IS NOT NULL;
CREATE INDEX company_profile_mv_fundings ON public.company_profile_mv (name) WHERE funding_count > 0;
CREATE INDEX company_profile_mv_source_keys ON public.company_profile_mv USING gin (source_keys);

CREATE VIEW public.v_company_profile AS SELECT * FROM public.company_profile_mv;

COMMENT ON VIEW public.v_company_profile IS
  'The target record per company, as of the last refresh_company_profile(). The live definition is v_company_profile_live.';

GRANT SELECT ON public.company_profile_mv, public.v_company_profile, public.v_company_profile_live
    TO ${reader_role};

-- 3. Users can vote on it like on every other shown field (V3).
ALTER TABLE public.extraction_feedback
    DROP CONSTRAINT extraction_feedback_field_name_check,
    ADD CONSTRAINT extraction_feedback_field_name_check CHECK (field_name IN (
        'name', 'address', 'legal_form', 'register_status', 'wz_code', 'established_at',
        'employee_count', 'revenue', 'fundings', 'cluster', 'technologiefeld', 'website',
        'management', 'business_purpose', 'deeptech', 'is_ai', 'amazon_effect', 'activity_keywords',
        'register_announcement'
    ));
