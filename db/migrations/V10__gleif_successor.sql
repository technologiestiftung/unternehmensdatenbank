-- The company a dissolved one went into, from GLEIF (#167).
--
-- GLEIF's `SuccessorEntity` names the LEI of the entity a merged or restructured one went into. The LEI
-- issuer records it from the restructuring, so it is proof, not a guess from a similar name or address
-- (which `docs/data-model.md` rules out). The `gleif` DAG writes it as the register field
-- `successor_lei`; the coordinator resolves it like the others, so `v_field_provenance` and the history
-- need nothing. A successor is a different company: `companies.merged_into` (a duplicate) stays untouched.
--
--   1. the observation upsert index (repeated verbatim in `insert_text_observation`, same commit);
--   2. `v_company_successor`, the company page's link, only where we hold the successor's LEI.
-- No profile column: one view, as `v_company_emails` (V6), spares rebuilding `company_profile_mv`.

DROP INDEX public.uq_obs_company_field_source_value;
CREATE UNIQUE INDEX uq_obs_company_field_source_value ON public.company_field_observations USING btree (company_id, field_name, source_id, value_text) WHERE (field_name = ANY (ARRAY['website'::text, 'candidate_url'::text, 'directory_url'::text, 'discovery_tier'::text, 'osm_id'::text, 'technology_field'::text, 'purpose_field'::text, 'technology_field_withheld'::text, 'purpose_field_withheld'::text, 'name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text, 'founding_year'::text, 'wz_division_estimated'::text, 'successor_lei'::text]));

CREATE VIEW public.v_company_successor AS
SELECT DISTINCT ON (cfs.company_id)
       cfs.company_id,
       successor.company_id AS successor_id,
       p.name AS successor_name,
       o.value_text AS successor_lei,
       s.name AS source,
       COALESCE(o.source_updated_at, o.observed_at) AS observed_at
  FROM public.company_canonical_field_sources cfs
  JOIN public.company_field_observations o ON o.id = cfs.company_field_observation_id
  JOIN public.sources s ON s.id = cfs.source_id
  JOIN public.company_identifiers successor ON successor.scheme = 'lei' AND successor.value = o.value_text
  JOIN public.company_profile_mv p ON p.id = successor.company_id
 WHERE cfs.field_name = 'successor_lei'
   AND successor.company_id <> cfs.company_id
 ORDER BY cfs.company_id, successor.created_at;

COMMENT ON VIEW public.v_company_successor IS 'The company a dissolved one went into, from GLEIF''s successor LEI (#167). Only where that LEI belongs to one of our companies; the name is the successor''s as of the last refresh_company_profile().';

GRANT SELECT ON TABLE public.v_company_successor TO ${reader_role};
GRANT SELECT ON TABLE public.v_company_successor TO ${writer_role};
