-- Profile cleanup, user tags, the register number, English names (issue #47, decided 2026-10-29).
--
-- Ships together with the matching unternehmensdaten-app release: it renames and drops columns the
-- app reads, which docs/release.md otherwise forbids within one release (decided for #47).
--
--   removed   cluster, tech_field (free text an LLM typed), amazon_effect (never filled),
--             data_quality_score and canonical_updated_at (weighted the removed columns; rewrote the
--             whole stored profile on every coordinator run), the 65,587 `search_query` markers
--   renamed   technologiefeld -> technology_field (columns, observation fields, feedback key)
--   English   the 77 field names (`name_en` in branch_taxonomy.yaml) and register_status
--             (aktiv/erloschen/in Liquidation -> active/dissolved/in_liquidation)
--   added     tags and company_tags (people tag companies), registration_number in the profile

-- 1. Values in English. The mapping is branch_taxonomy.yaml's `name_en`.
CREATE TEMP TABLE _field_en (de text PRIMARY KEY, en text NOT NULL) ON COMMIT DROP;
INSERT INTO _field_en VALUES
  ('5G- bzw. 6G Anwendungen', '5G/6G Applications'),
  ('Additive Fertigung', 'Additive Manufacturing'),
  ('Advanced Materials', 'Advanced Materials'),
  ('Architekturmarkt', 'Architecture Market'),
  ('Automobilwirtschaft', 'Automotive Industry'),
  ('Autonomes, vernetzes Fahren', 'Autonomous and Connected Driving'),
  ('Bahnindustrie', 'Rail Industry'),
  ('Batterietechnologien', 'Battery Technologies'),
  ('Bauindustrie', 'Construction Industry'),
  ('Bioproduktion', 'Bioproduction'),
  ('Biotechnologie', 'Biotechnology'),
  ('Buchmarkt', 'Book Market'),
  ('Carbon Management', 'Carbon Management'),
  ('Chemische Industrie', 'Chemical Industry'),
  ('Cybersicherheit', 'Cybersecurity'),
  ('Defence Tech', 'Defence Tech'),
  ('Designwirtschaft', 'Design Industry'),
  ('Dienstleistung', 'Services'),
  ('Digital Health', 'Digital Health'),
  ('Drohnen', 'Drones'),
  ('Elektroindustrie', 'Electrical Industry'),
  ('Elektromobilität', 'Electromobility'),
  ('Energiespeicher', 'Energy Storage'),
  ('Ernährungsindustrie', 'Food Industry'),
  ('Fernwärme', 'District Heating'),
  ('Filmwirtschaft', 'Film Industry'),
  ('FinTech', 'FinTech'),
  ('Foodtech', 'Foodtech'),
  ('Freiberufler', 'Freelancers'),
  ('Games', 'Games'),
  ('Gastgewerbe', 'Hospitality'),
  ('Geothermie', 'Geothermal Energy'),
  ('Gesundheitsversorgung & Pflege', 'Healthcare & Nursing'),
  ('GovTech', 'GovTech'),
  ('Gummi- und Kunststoffverarbeitende Industrie', 'Rubber and Plastics Processing Industry'),
  ('Halbleitertechnologien', 'Semiconductor Technologies'),
  ('Handel', 'Trade'),
  ('Handwerk', 'Skilled Crafts'),
  ('Immersive Technologien (VR, AR, XR)', 'Immersive Technologies (VR, AR, XR)'),
  ('Kreislaufwirtschaft', 'Circular Economy'),
  ('Kunstmarkt', 'Art Market'),
  ('Künstliche Intelligenz', 'Artificial Intelligence'),
  ('Ladeinfrastruktur', 'Charging Infrastructure'),
  ('Logistik', 'Logistics'),
  ('Markt für darstellende Künste', 'Performing Arts Market'),
  ('Maschinen- und Anlagenbau', 'Mechanical and Plant Engineering'),
  ('Medizintechnik', 'Medical Technology'),
  ('Messe- und Kongresswirtschaft', 'Trade Fair and Congress Industry'),
  ('Metallindustrie', 'Metal Industry'),
  ('Mikroelektronik', 'Microelectronics'),
  ('Mobilitätssoftware', 'Mobility Software'),
  ('Modewirtschaft', 'Fashion Industry'),
  ('Musikwirtschaft', 'Music Industry'),
  ('Nachhaltige Wirtschaft', 'Sustainable Economy'),
  ('Neue Mobilitätsangebote', 'New Mobility Services'),
  ('Papier- und Druckindustrie', 'Paper and Printing Industry'),
  ('Pharma & Wirkstoffe', 'Pharma & Active Ingredients'),
  ('Pharmaindustrie', 'Pharmaceutical Industry'),
  ('Photonik', 'Photonics'),
  ('Pressemarkt', 'Press Market'),
  ('Quantentechnologien', 'Quantum Technologies'),
  ('Raumfahrt', 'Space'),
  ('Rechenzentrenbetreiber', 'Data Centre Operators'),
  ('Recycling', 'Recycling'),
  ('Robotik', 'Robotics'),
  ('Rundfunkwirtschaft', 'Broadcasting Industry'),
  ('Sicherheits- und Verteidigungsindustrie', 'Security and Defence Industry'),
  ('Solarenergie', 'Solar Energy'),
  ('Soziale Unternehmen', 'Social Enterprises'),
  ('Stromnetz', 'Power Grid'),
  ('Telekommunikationsanbieter', 'Telecommunications Providers'),
  ('Tourismus', 'Tourism'),
  ('Wasserstoff', 'Hydrogen'),
  ('Wassertechnologien', 'Water Technologies'),
  ('Web3, Blockchain, DLT', 'Web3, Blockchain, DLT'),
  ('Werbemarkt', 'Advertising Market'),
  ('Windenergie', 'Wind Energy');

UPDATE public.company_field_observations o SET value_text = f.en, updated_at = now()
FROM _field_en f
WHERE o.field_name IN ('technologiefeld', 'technologiefeld_withheld', 'purpose_field', 'purpose_field_withheld')
  AND o.value_text = f.de;
UPDATE public.company_field_observations SET field_name = 'technology_field', updated_at = now()
WHERE field_name = 'technologiefeld';
UPDATE public.company_field_observations SET field_name = 'technology_field_withheld', updated_at = now()
WHERE field_name = 'technologiefeld_withheld';
UPDATE public.company_canonical_field_sources SET field_name = 'technology_field' WHERE field_name = 'technologiefeld';

CREATE TEMP TABLE _status_en (de text PRIMARY KEY, en text NOT NULL) ON COMMIT DROP;
INSERT INTO _status_en VALUES ('aktiv', 'active'), ('erloschen', 'dissolved'), ('in Liquidation', 'in_liquidation');
UPDATE public.company_field_observations o SET value_text = s.en, updated_at = now()
FROM _status_en s WHERE o.field_name = 'register_status' AND o.value_text = s.de;

-- Votes follow, so a vote still matches the value it was cast on. Votes on removed fields go.
DELETE FROM public.extraction_feedback WHERE field_name IN ('cluster', 'amazon_effect');
ALTER TABLE public.extraction_feedback DROP CONSTRAINT extraction_feedback_field_name_check;
UPDATE public.extraction_feedback SET field_name = 'technology_field' WHERE field_name = 'technologiefeld';
UPDATE public.extraction_feedback e SET value = COALESCE((SELECT en FROM _field_en WHERE de = e.value), e.value),
       correction = COALESCE((SELECT en FROM _field_en WHERE de = e.correction), e.correction)
WHERE e.field_name = 'technology_field';
UPDATE public.extraction_feedback e SET value = COALESCE((SELECT en FROM _status_en WHERE de = e.value), e.value),
       correction = COALESCE((SELECT en FROM _status_en WHERE de = e.correction), e.correction)
WHERE e.field_name = 'register_status';
ALTER TABLE public.extraction_feedback ADD CONSTRAINT extraction_feedback_field_name_check CHECK (field_name IN (
  'name', 'address', 'legal_form', 'register_status', 'wz_code', 'established_at', 'employee_count',
  'revenue', 'fundings', 'technology_field', 'website', 'management', 'business_purpose', 'deeptech',
  'is_ai', 'activity_keywords', 'register_announcement', 'registration_number'));

UPDATE public.sources
SET metadata = jsonb_set(metadata, '{writes}',
    '["technology_field", "purpose_field", "technology_field_withheld", "purpose_field_withheld"]')
WHERE key = 'branch_classifier';

-- 2. The two foreign keys onto observations get their index. Without them every deleted observation
--    scanned both tables completely: deleting the markers below ran over 8 minutes on the Berlin set,
--    and every source that replaces its observations (manager_ner on a new website) paid the same.
CREATE INDEX company_canonical_field_sources_observation_idx
    ON public.company_canonical_field_sources (company_field_observation_id);
CREATE INDEX company_managers_observation_idx ON public.company_managers (company_field_observation_id);

-- The `search_query` markers go; "looked at" is the source record (enrichment/observations.py).
DELETE FROM public.company_field_observations WHERE field_name = 'search_query';

-- The upsert key, renamed and without search_query. `insert_text_observation` in
-- source_enrichment_common.py repeats this predicate verbatim.
DROP INDEX public.uq_obs_company_field_source_value;
CREATE UNIQUE INDEX uq_obs_company_field_source_value
    ON public.company_field_observations (company_id, field_name, source_id, value_text)
    WHERE field_name IN (
        'website', 'candidate_url', 'directory_url',
        'discovery_tier', 'osm_id',
        'technology_field', 'purpose_field',
        'technology_field_withheld', 'purpose_field_withheld',
        'name', 'legal_form', 'address', 'postcode', 'city',
        'register_status', 'established_at', 'business_purpose', 'wz_code'
    );

-- 3. The view chain, rebuilt without the removed columns.
DROP VIEW public.v_company_profile;
DROP MATERIALIZED VIEW public.company_profile_mv;
DROP VIEW public.v_company_profile_live;
DROP VIEW public.v_companies_enriched;

ALTER TABLE public.companies DROP COLUMN data_quality_score, DROP COLUMN canonical_updated_at;

CREATE VIEW public.v_companies_enriched AS
 SELECT c.id,
    reg.company_name,
    public.normalize_company_name(reg.company_name) AS normalized_name,
    reg.legal_form,
    reg.business_purpose,
    reg.address,
    reg.established_at,
    c.registration_number,
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

COMMENT ON VIEW public.v_companies_enriched IS
  'The company as applications should read it: the published value of every field, register facts included (V3). technology_field and purpose_field are two different questions (one of Berlin''s 77 fields in English, from published words vs. from the register purpose) and must never be merged; a WZ code may never fill technology_field.';

CREATE VIEW public.v_company_profile_live AS
 SELECT e.id,
    e.company_name AS name,
    e.registration_number,
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
    e.technology_field,
    e.website_url AS website,
    mgr.managers AS management,
    e.business_purpose,
    obs_deeptech.value_text AS deeptech_class,
    obs_deeptech.confidence AS deeptech_confidence,
    (obs_ai.value_text)::boolean AS is_ai,
    e.register_status,
    GREATEST(e.data_last_updated_at, chosen.selected_at) AS updated_at,
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
     -- When a shown value last changed. selected_at is when a winner was chosen (#46).
     LEFT JOIN LATERAL ( SELECT max(cfs.selected_at) AS selected_at
           FROM public.company_canonical_field_sources cfs
          WHERE cfs.company_id = e.id) chosen ON true
     LEFT JOIN public.company_canonical_field_sources cfs_deeptech ON cfs_deeptech.company_id = e.id AND cfs_deeptech.field_name = 'deeptech_class'
     LEFT JOIN public.company_field_observations obs_deeptech ON obs_deeptech.id = cfs_deeptech.company_field_observation_id
     LEFT JOIN public.company_canonical_field_sources cfs_ai ON cfs_ai.company_id = e.id AND cfs_ai.field_name = 'is_ai'
     LEFT JOIN public.company_field_observations obs_ai ON obs_ai.id = cfs_ai.company_field_observation_id
     LEFT JOIN public.company_canonical_field_sources cfs_keywords ON cfs_keywords.company_id = e.id AND cfs_keywords.field_name = 'activity_keywords'
     LEFT JOIN public.company_field_observations obs_keywords ON obs_keywords.id = cfs_keywords.company_field_observation_id
     LEFT JOIN public.company_canonical_field_sources cfs_announce ON cfs_announce.company_id = e.id AND cfs_announce.field_name = 'register_announcement'
     LEFT JOIN public.company_field_observations obs_announce ON obs_announce.id = cfs_announce.company_field_observation_id;

COMMENT ON VIEW public.v_company_profile_live IS
  'The target record per company, read by the app through Hasura (via company_profile_mv). Tags and comments are separate tables. deeptech_class and is_ai are filled only by corrections from the app so far.';

CREATE MATERIALIZED VIEW public.company_profile_mv AS
 SELECT p.*,
    COALESCE(src.source_keys, '{}'::text[]) AS source_keys
   FROM public.v_company_profile_live p
     LEFT JOIN ( SELECT v_company_sources.company_id,
            array_agg(DISTINCT v_company_sources.source_key ORDER BY v_company_sources.source_key) AS source_keys
           FROM public.v_company_sources
          GROUP BY v_company_sources.company_id) src ON src.company_id = p.id
  WITH DATA;

CREATE UNIQUE INDEX company_profile_mv_id ON public.company_profile_mv USING btree (id);
CREATE INDEX company_profile_mv_fundings ON public.company_profile_mv USING btree (name) WHERE (funding_count > 0);
CREATE INDEX company_profile_mv_legal_form ON public.company_profile_mv USING btree (legal_form);
CREATE INDEX company_profile_mv_name ON public.company_profile_mv USING btree (name);
CREATE INDEX company_profile_mv_register_status ON public.company_profile_mv USING btree (register_status);
CREATE INDEX company_profile_mv_source_keys ON public.company_profile_mv USING gin (source_keys);
CREATE INDEX company_profile_mv_technology_field ON public.company_profile_mv USING btree (technology_field);
CREATE INDEX company_profile_mv_website ON public.company_profile_mv USING btree (name) WHERE (website IS NOT NULL);
CREATE INDEX company_profile_mv_wz_code ON public.company_profile_mv USING btree (wz_code text_pattern_ops);

CREATE VIEW public.v_company_profile AS SELECT * FROM public.company_profile_mv;
COMMENT ON VIEW public.v_company_profile IS
  'The target record per company, as of the last refresh_company_profile(). The live definition is v_company_profile_live.';

GRANT SELECT, INSERT, DELETE, UPDATE ON public.v_companies_enriched, public.v_company_profile_live,
    public.company_profile_mv, public.v_company_profile TO ${writer_role};
GRANT SELECT ON public.v_companies_enriched, public.v_company_profile_live,
    public.company_profile_mv, public.v_company_profile TO ${reader_role};

-- 4. The register number in the ⓘ: it is identity, not an observation, so its row comes from the
--    company and the register identifier's source.
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
     LEFT JOIN public.sources s ON s.key = f.source
UNION ALL
 SELECT c.id AS company_id,
    'registration_number'::text AS field_name,
    c.registration_number AS value,
    s.key AS source_key,
    s.name AS source_name,
    'register'::text AS method,
    NULL::numeric AS confidence,
    NULL::jsonb AS evidence,
    NULL::text AS link,
    i.created_at AS observed_at,
    i.created_at AS selected_at
   FROM public.companies c
     JOIN public.company_identifiers i ON i.company_id = c.id AND i.scheme = 'register'
       AND c.registration_number = 'Amtsgericht ' || i.value
     LEFT JOIN public.sources s ON s.id = i.source_id
  WHERE c.registration_number IS NOT NULL;

-- 5. Tags. People write them through Hasura (role `user`, which reaches the database as the
--    reader login); permissions in hasura/metadata. created_by and added_by come from the JWT.
CREATE TABLE public.tags (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    name text NOT NULL CHECK (btrim(name) <> '' AND length(name) <= 60),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX tags_name_key ON public.tags (lower(btrim(name)));
COMMENT ON TABLE public.tags IS
  'Tags people give companies in the app. Free text, unique ignoring case and surrounding spaces. created_by is a Keycloak user id.';

CREATE TABLE public.company_tags (
    company_id uuid NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    tag_id uuid NOT NULL REFERENCES public.tags(id) ON DELETE CASCADE,
    added_by text NOT NULL,
    added_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, tag_id)
);
CREATE INDEX company_tags_tag_idx ON public.company_tags (tag_id);
COMMENT ON TABLE public.company_tags IS
  'Which company has which tag, who added it and when. A person removes only tags they added (Hasura); an admin role for the rest comes later.';

GRANT SELECT, INSERT, DELETE, UPDATE ON public.tags, public.company_tags TO ${writer_role};
GRANT SELECT, INSERT ON public.tags TO ${reader_role};
GRANT SELECT, INSERT, DELETE ON public.company_tags TO ${reader_role};

COMMENT ON TABLE public.companies IS
  'Company identity: a random UUID, the register number, merged_into. Every shown value is an observation; read the published one through v_companies_enriched.';
