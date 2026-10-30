-- The register number lives in one place: `company_identifiers` (scheme `register`).
--
-- `companies.registration_number` held the same number a second time, with "Amtsgericht " in front
-- (296,897 of 296,897 companies, 2026-10-30). The two could disagree, and only the identifiers can
-- hold a second number: a change of legal form gets a new one, which `register_announcements` adds
-- (14 companies). The column kept the old number.
--
-- The views keep their column `registration_number` in the same format, now the company's NEWEST
-- register identifier, so the app and every reader of `v_companies_enriched` stay as they were.
-- Newest = stored last, and among numbers stored together the higher one: Berlin's court numbers
-- count up. Two companies got both their numbers in one run (2026-10-28); without the second rule a
-- random id picked HRB 210032 over HRB 291998.
-- Same column names and types, so `company_profile_mv` keeps working; its stored values change on
-- the next `refresh_company_profile()`.

-- 1. A number only the column holds becomes an identifier (none on 2026-10-30). One that another
--    company already holds is a conflict, never overwritten: the migration stops instead.
INSERT INTO public.company_identifiers (company_id, scheme, value)
SELECT id, 'register', regexp_replace(registration_number, '^Amtsgericht ', '')
FROM public.companies
WHERE registration_number IS NOT NULL
ON CONFLICT (scheme, value) DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM public.companies c
    WHERE c.registration_number IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM public.company_identifiers i
                      WHERE i.company_id = c.id AND i.scheme = 'register'
                        AND i.value = regexp_replace(c.registration_number, '^Amtsgericht ', ''))
  ) THEN
    RAISE EXCEPTION 'A register number in companies belongs to another company in company_identifiers';
  END IF;
END
$$;

-- 2. The two views that read the column read the identifiers instead.
CREATE OR REPLACE VIEW public.v_companies_enriched AS
 SELECT c.id,
    reg.company_name,
    public.normalize_company_name(reg.company_name) AS normalized_name,
    reg.legal_form,
    reg.business_purpose,
    reg.address,
    reg.established_at,
    ( SELECT 'Amtsgericht ' || i.value
           FROM public.company_identifiers i
          WHERE i.company_id = c.id AND i.scheme = 'register'
          ORDER BY i.created_at DESC, (substring(i.value FROM '(\d+)\D*$'))::bigint DESC NULLS LAST, i.id DESC
         LIMIT 1) AS registration_number,
    reg.wz_code,
    c.data_last_updated_at,
    c.metadata,
    c.created_at,
    c.updated_at,
    obs_website.value_text AS website_url,
    obs_revenue.value_number AS revenue,
    (obs_emp.value_number)::integer AS employee_count,
    obs_website.confidence AS website_confidence,
    obs_revenue.confidence AS revenue_confidence,
    obs_emp.confidence AS employee_count_confidence,
    src_website.key AS website_source,
    src_revenue.key AS revenue_source,
    src_emp.key AS employee_count_source,
    reg.register_status,
    obs_tfield.value_text AS technology_field,
    obs_tfield.confidence AS technology_field_confidence,
    src_tfield.key AS technology_field_source,
    obs_purpose.value_text AS purpose_field,
    obs_purpose.confidence AS purpose_field_confidence,
    src_purpose.key AS purpose_field_source,
    reg.postcode,
    reg.city,
    c.merged_into
   FROM public.companies c
     LEFT JOIN ( SELECT cfs.company_id,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'name') AS company_name,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'legal_form') AS legal_form,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'business_purpose') AS business_purpose,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'address') AS address,
            (max(o.value_text) FILTER (WHERE cfs.field_name = 'established_at'))::date AS established_at,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'wz_code') AS wz_code,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'register_status') AS register_status,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'postcode') AS postcode,
            max(o.value_text) FILTER (WHERE cfs.field_name = 'city') AS city
           FROM public.company_canonical_field_sources cfs
             JOIN public.company_field_observations o ON o.id = cfs.company_field_observation_id
          WHERE cfs.field_name IN ('name', 'legal_form', 'address', 'postcode', 'city',
                                   'register_status', 'established_at', 'business_purpose', 'wz_code')
          GROUP BY cfs.company_id) reg ON reg.company_id = c.id
     LEFT JOIN public.company_canonical_field_sources cfs_website ON cfs_website.company_id = c.id AND cfs_website.field_name = 'website'
     LEFT JOIN public.company_field_observations obs_website ON obs_website.id = cfs_website.company_field_observation_id
     LEFT JOIN public.sources src_website ON src_website.id = cfs_website.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_revenue ON cfs_revenue.company_id = c.id AND cfs_revenue.field_name = 'revenue'
     LEFT JOIN public.company_field_observations obs_revenue ON obs_revenue.id = cfs_revenue.company_field_observation_id
     LEFT JOIN public.sources src_revenue ON src_revenue.id = cfs_revenue.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_emp ON cfs_emp.company_id = c.id AND cfs_emp.field_name = 'employee_count'
     LEFT JOIN public.company_field_observations obs_emp ON obs_emp.id = cfs_emp.company_field_observation_id
     LEFT JOIN public.sources src_emp ON src_emp.id = cfs_emp.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_tfield ON cfs_tfield.company_id = c.id AND cfs_tfield.field_name = 'technology_field'
     LEFT JOIN public.company_field_observations obs_tfield ON obs_tfield.id = cfs_tfield.company_field_observation_id
     LEFT JOIN public.sources src_tfield ON src_tfield.id = cfs_tfield.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_purpose ON cfs_purpose.company_id = c.id AND cfs_purpose.field_name = 'purpose_field'
     LEFT JOIN public.company_field_observations obs_purpose ON obs_purpose.id = cfs_purpose.company_field_observation_id
     LEFT JOIN public.sources src_purpose ON src_purpose.id = cfs_purpose.source_id;

CREATE OR REPLACE VIEW public.v_field_provenance AS
 SELECT cfs.company_id,
    cfs.field_name,
    COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text) AS value,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE((o.metadata ->> 'verified_by'), (o.metadata ->> 'decided_by'), s.source_type) AS method,
    o.confidence,
    COALESCE((o.metadata -> 'evidence'), (o.metadata -> 'keywords')) AS evidence,
    COALESCE((o.metadata ->> 'imprint_url'), (o.metadata ->> 'matched_url')) AS link,
    COALESCE(o.source_updated_at, o.observed_at) AS observed_at,
    cfs.selected_at
   FROM public.company_canonical_field_sources cfs
     JOIN public.company_field_observations o ON o.id = cfs.company_field_observation_id
     JOIN public.sources s ON s.id = cfs.source_id
UNION ALL
 SELECT m.company_id,
    'management'::text AS field_name,
    m.full_name AS value,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE((o.metadata ->> 'model_version'), s.source_type) AS method,
    o.confidence,
    (o.metadata -> 'evidence') AS evidence,
    (o.metadata ->> 'imprint_url') AS link,
    COALESCE(o.source_updated_at, o.observed_at) AS observed_at,
    m.updated_at AS selected_at
   FROM public.company_managers m
     LEFT JOIN public.company_field_observations o ON o.id = m.company_field_observation_id
     LEFT JOIN public.sources s ON s.id = m.source_id
  WHERE m.is_current
UNION ALL
 SELECT l.company_id,
    'fundings'::text AS field_name,
    f.fkz AS value,
    f.source AS source_key,
    COALESCE(s.name, f.source) AS source_name,
        CASE l.match_method
            WHEN 'register_number' THEN 'linked by register number'
            ELSE 'linked by recipient name and city'
        END AS method,
    NULL::numeric AS confidence,
    NULL::jsonb AS evidence,
    NULL::text AS link,
    f.first_seen_at AS observed_at,
    f.last_seen_at AS selected_at
   FROM public.company_funding_links l
     JOIN public.company_fundings f ON f.fkz = l.fkz
     LEFT JOIN public.sources s ON s.key = f.source
UNION ALL
 SELECT r.company_id,
    'registration_number'::text AS field_name,
    'Amtsgericht ' || r.value AS value,
    s.key AS source_key,
    s.name AS source_name,
    'register'::text AS method,
    NULL::numeric AS confidence,
    NULL::jsonb AS evidence,
    NULL::text AS link,
    r.created_at AS observed_at,
    r.created_at AS selected_at
   FROM ( SELECT DISTINCT ON (i.company_id) i.company_id, i.value, i.source_id, i.created_at
           FROM public.company_identifiers i
          WHERE i.scheme = 'register'
          -- The same order as v_companies_enriched, so both show the same number.
          ORDER BY i.company_id, i.created_at DESC, (substring(i.value FROM '(\d+)\D*$'))::bigint DESC NULLS LAST, i.id DESC) r
     LEFT JOIN public.sources s ON s.id = r.source_id;

-- 3. Nothing reads the column any more.
ALTER TABLE public.companies DROP COLUMN registration_number;
