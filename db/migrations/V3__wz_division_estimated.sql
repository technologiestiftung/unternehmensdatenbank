-- An estimated WZ division per company, from our own text classifier (#158, second step of #140).
--
-- `wz_division_estimated` holds two digits ('62'), a WZ 2025 division (`wz_divisions`). It is an ESTIMATE from the
-- company's website text, never the official `wz_code`: its own field, its own source (`wz_textcat`),
-- shown in the app as "WZ (geschätzt)". The official code, where one exists, stays in `wz_code`.
--
-- This migration lets the field be upserted, corrected, listed as a claim and shown:
--   1. the observation upsert index (repeated verbatim in `insert_text_observation`, same commit);
--   2. `extraction_feedback`'s field check, so the app can vote on it;
--   3. `v_company_claims`' allow-list;
--   4. a profile column: `v_company_profile_live`, then `company_profile_mv` rebuilt (it froze p.*),
--      and with it the two views that read it, `v_company_profile` and `v_review_results`.
-- `v_field_provenance` and the history need nothing: they read every canonical field.

DROP INDEX public.uq_obs_company_field_source_value;
CREATE UNIQUE INDEX uq_obs_company_field_source_value ON public.company_field_observations USING btree (company_id, field_name, source_id, value_text) WHERE (field_name = ANY (ARRAY['website'::text, 'candidate_url'::text, 'directory_url'::text, 'discovery_tier'::text, 'osm_id'::text, 'technology_field'::text, 'purpose_field'::text, 'technology_field_withheld'::text, 'purpose_field_withheld'::text, 'name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text, 'founding_year'::text, 'wz_division_estimated'::text]));

ALTER TABLE public.extraction_feedback DROP CONSTRAINT extraction_feedback_field_name_check;
ALTER TABLE public.extraction_feedback ADD CONSTRAINT extraction_feedback_field_name_check CHECK ((field_name = ANY (ARRAY['name'::text, 'address'::text, 'legal_form'::text, 'register_status'::text, 'wz_code'::text, 'established_at'::text, 'established_year'::text, 'employee_count'::text, 'revenue'::text, 'fundings'::text, 'technology_field'::text, 'website'::text, 'management'::text, 'business_purpose'::text, 'deeptech'::text, 'is_ai'::text, 'activity_keywords'::text, 'register_announcement'::text, 'registration_number'::text, 'wz_division_estimated'::text])));

CREATE OR REPLACE VIEW public.v_company_claims AS
 SELECT o.id,
    o.company_id,
        CASE o.field_name
            WHEN 'ceo_name_or_contact'::text THEN 'management'::text
            WHEN 'candidate_url'::text THEN 'website'::text
            WHEN 'deeptech_class'::text THEN 'deeptech'::text
            WHEN 'founding_year'::text THEN 'established_year'::text
            ELSE o.field_name
        END AS field_key,
    COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text) AS value,
    o.confidence,
    (o.is_canonical OR (EXISTS ( SELECT 1
           FROM public.company_managers m
          WHERE ((m.company_field_observation_id = o.id) AND m.is_current)))) AS is_shown,
    COALESCE((o.metadata ->> 'verified_by'::text), (o.metadata ->> 'decided_by'::text)) AS method,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE(o.source_updated_at, o.observed_at) AS read_at
   FROM (public.company_field_observations o
     JOIN public.sources s ON ((s.id = o.source_id)))
  WHERE (o.field_name = ANY (ARRAY['name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text, 'website'::text, 'candidate_url'::text, 'technology_field'::text, 'purpose_field'::text, 'activity_keywords'::text, 'register_announcement'::text, 'ceo_name_or_contact'::text, 'employee_count'::text, 'revenue'::text, 'deeptech_class'::text, 'is_ai'::text, 'founding_year'::text, 'wz_division_estimated'::text]));

CREATE OR REPLACE VIEW public.v_company_profile_live AS
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
    ((obs_announce.metadata ->> 'published_on'::text))::date AS register_announcement_date,
    COALESCE(
        CASE
            WHEN (obs_year.value_text ~ '^\d{4}$'::text) THEN (obs_year.value_text)::integer
            ELSE NULL::integer
        END, (EXTRACT(year FROM e.established_at))::integer) AS established_year,
    obs_wz_est.value_text AS wz_division_estimated
   FROM (((((((((((((((public.v_companies_enriched e
     LEFT JOIN LATERAL ( SELECT count(*) AS funding_count,
            sum(f.amount_eur) AS funding_total_eur
           FROM public.v_company_fundings f
          WHERE (f.company_id = e.id)) fund ON (true))
     LEFT JOIN LATERAL ( SELECT string_agg(DISTINCT cm.full_name, '; '::text ORDER BY cm.full_name) AS managers
           FROM public.company_managers cm
          WHERE ((cm.company_id = e.id) AND cm.is_current)) mgr ON (true))
     LEFT JOIN LATERAL ( SELECT max(cfs.selected_at) AS selected_at
           FROM public.company_canonical_field_sources cfs
          WHERE (cfs.company_id = e.id)) chosen ON (true))
     LEFT JOIN public.company_canonical_field_sources cfs_deeptech ON (((cfs_deeptech.company_id = e.id) AND (cfs_deeptech.field_name = 'deeptech_class'::text))))
     LEFT JOIN public.company_field_observations obs_deeptech ON ((obs_deeptech.id = cfs_deeptech.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_ai ON (((cfs_ai.company_id = e.id) AND (cfs_ai.field_name = 'is_ai'::text))))
     LEFT JOIN public.company_field_observations obs_ai ON ((obs_ai.id = cfs_ai.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_keywords ON (((cfs_keywords.company_id = e.id) AND (cfs_keywords.field_name = 'activity_keywords'::text))))
     LEFT JOIN public.company_field_observations obs_keywords ON ((obs_keywords.id = cfs_keywords.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_announce ON (((cfs_announce.company_id = e.id) AND (cfs_announce.field_name = 'register_announcement'::text))))
     LEFT JOIN public.company_field_observations obs_announce ON ((obs_announce.id = cfs_announce.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_year ON (((cfs_year.company_id = e.id) AND (cfs_year.field_name = 'founding_year'::text))))
     LEFT JOIN public.company_field_observations obs_year ON ((obs_year.id = cfs_year.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_wz_est ON (((cfs_wz_est.company_id = e.id) AND (cfs_wz_est.field_name = 'wz_division_estimated'::text))))
     LEFT JOIN public.company_field_observations obs_wz_est ON ((obs_wz_est.id = cfs_wz_est.company_field_observation_id)));

-- CASCADE drops v_company_profile and v_review_results; both are created again below.
DROP MATERIALIZED VIEW public.company_profile_mv CASCADE;

CREATE MATERIALIZED VIEW public.company_profile_mv AS
 SELECT p.id,
    p.name,
    p.registration_number,
    p.employee_count,
    p.revenue,
    p.funding_count,
    p.funding_total_eur,
    p.wz_code,
    p.established_at,
    p.address,
    p.postcode,
    p.city,
    p.legal_form,
    p.technology_field,
    p.website,
    p.management,
    p.business_purpose,
    p.deeptech_class,
    p.deeptech_confidence,
    p.is_ai,
    p.register_status,
    p.updated_at,
    p.activity_keywords,
    p.register_announcement,
    p.register_announcement_date,
    COALESCE(src.source_keys, '{}'::text[]) AS source_keys,
    p.established_year,
    p.wz_division_estimated
   FROM (public.v_company_profile_live p
     LEFT JOIN ( SELECT v_company_sources.company_id,
            array_agg(DISTINCT v_company_sources.source_key ORDER BY v_company_sources.source_key) AS source_keys
           FROM public.v_company_sources
          GROUP BY v_company_sources.company_id) src ON ((src.company_id = p.id)))
  WITH DATA;

CREATE INDEX company_profile_mv_established_year ON public.company_profile_mv USING btree (established_year);
CREATE INDEX company_profile_mv_fundings ON public.company_profile_mv USING btree (name) WHERE (funding_count > 0);
CREATE UNIQUE INDEX company_profile_mv_id ON public.company_profile_mv USING btree (id);
CREATE INDEX company_profile_mv_legal_form ON public.company_profile_mv USING btree (legal_form);
CREATE INDEX company_profile_mv_name ON public.company_profile_mv USING btree (name);
CREATE INDEX company_profile_mv_register_status ON public.company_profile_mv USING btree (register_status);
CREATE INDEX company_profile_mv_source_keys ON public.company_profile_mv USING gin (source_keys);
CREATE INDEX company_profile_mv_technology_field ON public.company_profile_mv USING btree (technology_field);
CREATE INDEX company_profile_mv_website ON public.company_profile_mv USING btree (name) WHERE (website IS NOT NULL);
CREATE INDEX company_profile_mv_wz_code ON public.company_profile_mv USING btree (wz_code text_pattern_ops);
CREATE INDEX company_profile_mv_wz_division_estimated ON public.company_profile_mv USING btree (wz_division_estimated text_pattern_ops);

CREATE VIEW public.v_company_profile AS
 SELECT company_profile_mv.id,
    company_profile_mv.name,
    company_profile_mv.registration_number,
    company_profile_mv.employee_count,
    company_profile_mv.revenue,
    company_profile_mv.funding_count,
    company_profile_mv.funding_total_eur,
    company_profile_mv.wz_code,
    company_profile_mv.established_at,
    company_profile_mv.address,
    company_profile_mv.postcode,
    company_profile_mv.city,
    company_profile_mv.legal_form,
    company_profile_mv.technology_field,
    company_profile_mv.website,
    company_profile_mv.management,
    company_profile_mv.business_purpose,
    company_profile_mv.deeptech_class,
    company_profile_mv.deeptech_confidence,
    company_profile_mv.is_ai,
    company_profile_mv.register_status,
    company_profile_mv.updated_at,
    company_profile_mv.activity_keywords,
    company_profile_mv.register_announcement,
    company_profile_mv.register_announcement_date,
    company_profile_mv.source_keys,
    company_profile_mv.established_year,
    company_profile_mv.wz_division_estimated
   FROM public.company_profile_mv;

COMMENT ON VIEW public.v_company_profile IS 'The target record per company, as of the last refresh_company_profile(). The live definition is v_company_profile_live. established_year is the canonical founding_year, else the year of established_at (V6).';

CREATE VIEW public.v_review_results AS
 WITH verdicts AS (
         SELECT s.stratum,
            f.company_id,
            f.field_name,
            f.value,
                CASE sign(((count(*) FILTER (WHERE (f.verdict = 'correct'::text)) - count(*) FILTER (WHERE (f.verdict = 'wrong'::text))))::double precision)
                    WHEN 1 THEN 'correct'::text
                    WHEN '-1'::integer THEN 'wrong'::text
                    ELSE NULL::text
                END AS verdict
           FROM ((public.review_sample s
             JOIN public.extraction_feedback f ON (((f.company_id = s.company_id) AND (f.value <> ''::text))))
             JOIN public.company_profile_mv p ON ((p.id = f.company_id)))
          WHERE
                CASE f.field_name
                    WHEN 'name'::text THEN (f.value = p.name)
                    WHEN 'address'::text THEN (f.value = p.address)
                    WHEN 'legal_form'::text THEN (f.value = p.legal_form)
                    WHEN 'register_status'::text THEN (f.value = p.register_status)
                    WHEN 'wz_code'::text THEN (f.value = p.wz_code)
                    WHEN 'established_at'::text THEN (f.value = (p.established_at)::text)
                    WHEN 'established_year'::text THEN (f.value = (p.established_year)::text)
                    WHEN 'employee_count'::text THEN
                    CASE
                        WHEN (f.value ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((f.value)::numeric = (p.employee_count)::numeric)
                        ELSE NULL::boolean
                    END
                    WHEN 'revenue'::text THEN
                    CASE
                        WHEN (f.value ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((f.value)::numeric = p.revenue)
                        ELSE NULL::boolean
                    END
                    WHEN 'fundings'::text THEN (EXISTS ( SELECT 1
                       FROM public.company_funding_links l
                      WHERE ((l.company_id = f.company_id) AND (l.fkz = f.value))))
                    WHEN 'technology_field'::text THEN (f.value = p.technology_field)
                    WHEN 'website'::text THEN (f.value = p.website)
                    WHEN 'management'::text THEN (f.value = ANY (string_to_array(p.management, '; '::text)))
                    WHEN 'business_purpose'::text THEN (f.value = p.business_purpose)
                    WHEN 'deeptech'::text THEN (f.value = p.deeptech_class)
                    WHEN 'is_ai'::text THEN (f.value = (p.is_ai)::text)
                    WHEN 'activity_keywords'::text THEN (f.value = ANY (string_to_array(p.activity_keywords, '; '::text)))
                    WHEN 'register_announcement'::text THEN (f.value = p.register_announcement)
                    WHEN 'registration_number'::text THEN (f.value = p.registration_number)
                    WHEN 'wz_division_estimated'::text THEN (f.value = p.wz_division_estimated)
                    ELSE NULL::boolean
                END
          GROUP BY s.stratum, f.company_id, f.field_name, f.value
        )
 SELECT verdicts.stratum,
    verdicts.field_name,
    count(DISTINCT verdicts.company_id) AS companies_judged,
    count(*) FILTER (WHERE (verdicts.verdict = 'correct'::text)) AS correct,
    count(*) FILTER (WHERE (verdicts.verdict = 'wrong'::text)) AS wrong,
    round(((count(*) FILTER (WHERE (verdicts.verdict = 'correct'::text)))::numeric / (NULLIF(count(*) FILTER (WHERE (verdicts.verdict IS NOT NULL)), 0))::numeric), 3) AS "precision"
   FROM verdicts
  GROUP BY verdicts.stratum, verdicts.field_name;

COMMENT ON VIEW public.v_review_results IS 'Hand-checked precision per field and stratum of the values shown now, from the app''s votes on the review sample: a vote counts only while its value is what company_profile_mv shows; one verdict per value by majority (ties left out). Missing values (a vote on an empty field) are not counted here.';


GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_profile_mv TO ${writer_role};
GRANT SELECT ON TABLE public.company_profile_mv TO ${reader_role};
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_company_profile TO ${writer_role};
GRANT SELECT ON TABLE public.v_company_profile TO ${reader_role};
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_review_results TO ${writer_role};
GRANT SELECT ON TABLE public.v_review_results TO ${reader_role};
