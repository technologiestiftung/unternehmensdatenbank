-- Register fields become observations; `companies` keeps identity only (issue #18, step 3).
--
-- Before: name, legal form, address, postcode, city, register status, founding date, purpose and WZ
-- code were columns of `companies`, written once by the import. The coordinator never saw them, so a
-- second source (Fusionbase) or a user's correction had no way to compete with them.
--
-- After: each is an observation of the source that delivered it, and the coordinator picks the
-- winner like every other field (priority, then confidence). `v_companies_enriched` reads the winners
-- under the old column names, so every reader and the app's columns stay as they were.
--
-- `companies` keeps: id, registration_number (an identifier), metadata, the pipeline timestamps,
-- data_quality_score and merged_into. Dropped with no replacement: normalized_name (computed from the
-- name where matching needs it), district and locality (empty, read by nothing).

-- 1. The upsert key covers the register fields, so a re-import updates instead of appending.
--    `insert_text_observation` in source_enrichment_common.py repeats this predicate verbatim.
DROP INDEX public.uq_obs_company_field_source_value;
CREATE UNIQUE INDEX uq_obs_company_field_source_value
    ON public.company_field_observations (company_id, field_name, source_id, value_text)
    WHERE field_name IN (
        'website', 'candidate_url', 'directory_url',
        'discovery_tier', 'search_query', 'osm_id',
        'technologiefeld', 'purpose_field',
        'technologiefeld_withheld', 'purpose_field_withheld',
        'name', 'legal_form', 'address', 'postcode', 'city',
        'register_status', 'established_at', 'business_purpose', 'wz_code'
    );

-- 2. Which source record speaks for each company's columns: its OffeneRegister record, else the
--    register announcement that created it (411 companies on 2026-10-29).
CREATE TEMP TABLE _register_record ON COMMIT DROP AS
SELECT DISTINCT ON (r.company_id)
       r.company_id, r.id AS record_id, r.source_id,
       COALESCE((r.source_payload #>> '{register,retrieved_at}')::timestamptz, r.first_seen_at) AS observed_at
FROM public.company_source_records r
JOIN public.sources s ON s.id = r.source_id
WHERE s.key = 'offeneregister' OR (s.key = 'registerbekanntmachungen' AND r.match_method = 'created')
ORDER BY r.company_id, (s.key = 'offeneregister') DESC, r.first_seen_at;

-- A company with values but no record to hang them on would lose them when the columns go.
DO $$
DECLARE orphans integer;
BEGIN
  SELECT count(*) INTO orphans
  FROM public.companies c
  WHERE NOT EXISTS (SELECT 1 FROM _register_record rr WHERE rr.company_id = c.id);
  IF orphans > 0 THEN
    RAISE EXCEPTION '% companies have no register source record; their columns would be lost', orphans;
  END IF;
END
$$;

-- 3. The columns' values as observations, already published: one source per field, so the
--    coordinator would pick the same rows.
INSERT INTO public.company_field_observations
  (company_id, company_source_record_id, source_id, field_name, value_text, observed_at, confidence,
   is_canonical, metadata)
SELECT c.id, rr.record_id, rr.source_id, f.field_name, f.value, rr.observed_at, 1.0, true, f.metadata
FROM public.companies c
JOIN _register_record rr ON rr.company_id = c.id
CROSS JOIN LATERAL (VALUES
  ('name', c.company_name, '{}'::jsonb),
  ('legal_form', c.legal_form, '{"decided_by": "read from the end of the registered name"}'::jsonb),
  ('address', c.address, '{}'::jsonb),
  ('postcode', c.postcode, '{}'::jsonb),
  ('city', c.city, '{}'::jsonb),
  ('register_status', c.register_status, '{}'::jsonb),
  ('established_at', c.established_at::text, '{}'::jsonb),
  ('business_purpose', c.business_purpose, '{}'::jsonb),
  ('wz_code', c.wz_code, '{}'::jsonb)
) f(field_name, value, metadata)
WHERE f.value IS NOT NULL;

INSERT INTO public.company_canonical_field_sources
  (company_id, field_name, source_id, company_field_observation_id, selected_at)
SELECT o.company_id, o.field_name, o.source_id, o.id, now()
FROM public.company_field_observations o
WHERE o.is_canonical
  AND o.field_name IN ('name', 'legal_form', 'address', 'postcode', 'city',
                       'register_status', 'established_at', 'business_purpose', 'wz_code');

-- Matching by name and city (register announcements, grant links) used companies.normalized_name.
CREATE INDEX company_field_observations_name_key_idx
    ON public.company_field_observations (public.normalize_company_name(value_text))
    WHERE field_name = 'name' AND is_canonical;

-- 4. The views read the published values under the old names. Same columns, same types, so the
--    stored profile (company_profile_mv) and Hasura are untouched. Three columns are appended.
CREATE OR REPLACE VIEW public.v_companies_enriched AS
 SELECT c.id,
    reg.company_name,
    public.normalize_company_name(reg.company_name) AS normalized_name,
    reg.legal_form,
    reg.business_purpose,
    reg.address,
    reg.established_at,
    c.registration_number,
    reg.wz_code,
    c.data_quality_score,
    c.canonical_updated_at,
    c.data_last_updated_at,
    c.metadata,
    c.created_at,
    c.updated_at,
    obs_website.value_text AS website_url,
    obs_cluster.value_text AS cluster,
    obs_tech.value_text AS tech_field,
    obs_revenue.value_number AS revenue,
    (obs_emp.value_number)::integer AS employee_count,
    obs_website.confidence AS website_confidence,
    obs_cluster.confidence AS cluster_confidence,
    obs_tech.confidence AS tech_field_confidence,
    obs_revenue.confidence AS revenue_confidence,
    obs_emp.confidence AS employee_count_confidence,
    src_website.key AS website_source,
    src_cluster.key AS cluster_source,
    src_tech.key AS tech_field_source,
    src_revenue.key AS revenue_source,
    src_emp.key AS employee_count_source,
    reg.register_status,
    obs_tfeld.value_text AS technologiefeld,
    obs_tfeld.confidence AS technologiefeld_confidence,
    src_tfeld.key AS technologiefeld_source,
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
     LEFT JOIN public.company_canonical_field_sources cfs_cluster ON cfs_cluster.company_id = c.id AND cfs_cluster.field_name = 'cluster'
     LEFT JOIN public.company_field_observations obs_cluster ON obs_cluster.id = cfs_cluster.company_field_observation_id
     LEFT JOIN public.sources src_cluster ON src_cluster.id = cfs_cluster.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_tech ON cfs_tech.company_id = c.id AND cfs_tech.field_name = 'tech_field'
     LEFT JOIN public.company_field_observations obs_tech ON obs_tech.id = cfs_tech.company_field_observation_id
     LEFT JOIN public.sources src_tech ON src_tech.id = cfs_tech.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_revenue ON cfs_revenue.company_id = c.id AND cfs_revenue.field_name = 'revenue'
     LEFT JOIN public.company_field_observations obs_revenue ON obs_revenue.id = cfs_revenue.company_field_observation_id
     LEFT JOIN public.sources src_revenue ON src_revenue.id = cfs_revenue.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_emp ON cfs_emp.company_id = c.id AND cfs_emp.field_name = 'employee_count'
     LEFT JOIN public.company_field_observations obs_emp ON obs_emp.id = cfs_emp.company_field_observation_id
     LEFT JOIN public.sources src_emp ON src_emp.id = cfs_emp.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_tfeld ON cfs_tfeld.company_id = c.id AND cfs_tfeld.field_name = 'technologiefeld'
     LEFT JOIN public.company_field_observations obs_tfeld ON obs_tfeld.id = cfs_tfeld.company_field_observation_id
     LEFT JOIN public.sources src_tfeld ON src_tfeld.id = cfs_tfeld.source_id
     LEFT JOIN public.company_canonical_field_sources cfs_purpose ON cfs_purpose.company_id = c.id AND cfs_purpose.field_name = 'purpose_field'
     LEFT JOIN public.company_field_observations obs_purpose ON obs_purpose.id = cfs_purpose.company_field_observation_id
     LEFT JOIN public.sources src_purpose ON src_purpose.id = cfs_purpose.source_id;

CREATE OR REPLACE VIEW public.v_company_profile_live AS
 SELECT e.id,
    e.company_name AS name,
    e.employee_count,
    e.revenue,
    COALESCE(fund.funding_count, (0)::bigint) AS funding_count,
    fund.funding_total_eur,
    e.wz_code,
    e.established_at,
    e.address,
    e.postcode,
    e.city,
    e.legal_form,
    e.cluster,
    e.technologiefeld,
    e.website_url AS website,
    mgr.managers AS management,
    e.business_purpose,
    obs_deeptech.value_text AS deeptech_class,
    obs_deeptech.confidence AS deeptech_confidence,
    (obs_ai.value_text)::boolean AS is_ai,
    obs_amazon.value_text AS amazon_effect,
    e.register_status,
    GREATEST(e.data_last_updated_at, e.canonical_updated_at) AS updated_at,
    obs_keywords.value_text AS activity_keywords,
    obs_announce.value_text AS register_announcement,
    ((obs_announce.metadata ->> 'published_on'))::date AS register_announcement_date
   FROM public.v_companies_enriched e
     LEFT JOIN LATERAL ( SELECT count(*) AS funding_count,
            sum(f.amount_eur) AS funding_total_eur
           FROM public.v_company_fundings f
          WHERE f.company_id = e.id) fund ON true
     LEFT JOIN LATERAL ( SELECT string_agg(DISTINCT cm.full_name, '; ' ORDER BY cm.full_name) AS managers
           FROM public.company_managers cm
          WHERE cm.company_id = e.id AND cm.is_current) mgr ON true
     LEFT JOIN public.company_canonical_field_sources cfs_deeptech ON cfs_deeptech.company_id = e.id AND cfs_deeptech.field_name = 'deeptech_class'
     LEFT JOIN public.company_field_observations obs_deeptech ON obs_deeptech.id = cfs_deeptech.company_field_observation_id
     LEFT JOIN public.company_canonical_field_sources cfs_ai ON cfs_ai.company_id = e.id AND cfs_ai.field_name = 'is_ai'
     LEFT JOIN public.company_field_observations obs_ai ON obs_ai.id = cfs_ai.company_field_observation_id
     LEFT JOIN public.company_canonical_field_sources cfs_amazon ON cfs_amazon.company_id = e.id AND cfs_amazon.field_name = 'amazon_effect'
     LEFT JOIN public.company_field_observations obs_amazon ON obs_amazon.id = cfs_amazon.company_field_observation_id
     LEFT JOIN public.company_canonical_field_sources cfs_keywords ON cfs_keywords.company_id = e.id AND cfs_keywords.field_name = 'activity_keywords'
     LEFT JOIN public.company_field_observations obs_keywords ON obs_keywords.id = cfs_keywords.company_field_observation_id
     LEFT JOIN public.company_canonical_field_sources cfs_announce ON cfs_announce.company_id = e.id AND cfs_announce.field_name = 'register_announcement'
     LEFT JOIN public.company_field_observations obs_announce ON obs_announce.id = cfs_announce.company_field_observation_id;

-- The register branch is gone: register facts are published observations now, covered by the first.
-- Postcode and city appear as fields of their own.
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
    o.observed_at,
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
    o.observed_at,
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
     LEFT JOIN public.sources s ON s.key = f.source;

CREATE OR REPLACE FUNCTION public.refresh_company_funding_links() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  linked integer;
BEGIN
  DELETE FROM public.company_funding_links;
  INSERT INTO public.company_funding_links (fkz, company_id, match_method)
  SELECT f.fkz, i.company_id, 'register_number'
  FROM public.company_fundings f
  JOIN public.company_identifiers i
    ON i.scheme = 'register' AND i.value = 'Berlin (Charlottenburg) ' || f.recipient_register
  WHERE f.recipient_register IS NOT NULL;

  INSERT INTO public.company_funding_links (fkz, company_id)
  WITH unique_names AS (
    SELECT c.normalized_name, lower(c.city) AS city, (array_agg(c.id))[1] AS company_id
    FROM public.v_companies_enriched c
    WHERE c.city IS NOT NULL
    GROUP BY c.normalized_name, lower(c.city)
    HAVING count(*) = 1
  )
  SELECT f.fkz, u.company_id
  FROM public.company_fundings f
  JOIN unique_names u ON u.normalized_name = f.recipient_normalized_name AND u.city = lower(f.city)
  WHERE f.recipient_register IS NULL;

  SELECT count(*) INTO linked FROM public.company_funding_links;
  RETURN linked;
END
$$;

-- 5. The columns go. Their indexes go with them.
ALTER TABLE public.companies
  DROP COLUMN company_name,
  DROP COLUMN normalized_name,
  DROP COLUMN legal_form,
  DROP COLUMN business_purpose,
  DROP COLUMN address,
  DROP COLUMN established_at,
  DROP COLUMN wz_code,
  DROP COLUMN postcode,
  DROP COLUMN city,
  DROP COLUMN district,
  DROP COLUMN locality,
  DROP COLUMN register_status;

COMMENT ON TABLE public.companies IS
  'Company identity: a random UUID, the register number, merged_into. Every shown value (name, address, status, ...) is an observation; read the published one through v_companies_enriched.';
