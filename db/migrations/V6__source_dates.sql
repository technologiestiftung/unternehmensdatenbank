-- When the SOURCE read a value, for every value we already have (issue #49).
--
-- `source_updated_at` is the source's date, `observed_at` ours. The coordinator ranks values by the
-- source's date (`resolver.HALF_LIVES`), and a correction from the app gives way to a source value
-- read after the vote. The register import of manager names wrote the import date (2026-10-28) into
-- observed_at, so 156,853 names from the 2017-2019 register looked new; the sources set the date
-- themselves from now on (`insert_text_observation(source_updated_at=...)`).

-- OffeneRegister manager names: the date the register was read (2017-06-12 to 2019-01-31). The
-- register fields need nothing: V3 already wrote that date into their observed_at.
UPDATE public.company_field_observations o
SET source_updated_at = (r.source_payload #>> '{register,retrieved_at}')::timestamptz
FROM public.company_source_records r
JOIN public.sources s ON s.id = r.source_id AND s.key = 'offeneregister'
WHERE o.company_source_record_id = r.id
  AND o.field_name = 'ceo_name_or_contact'
  AND o.source_updated_at IS NULL
  AND r.source_payload #>> '{register,retrieved_at}' IS NOT NULL;

-- Register announcements: the day the court published it.
UPDATE public.company_field_observations o
SET source_updated_at = (r.source_payload ->> 'published_on')::date
FROM public.company_source_records r
JOIN public.sources s ON s.id = r.source_id AND s.key = 'registerbekanntmachungen'
WHERE o.company_source_record_id = r.id
  AND o.source_updated_at IS NULL
  AND r.source_payload ->> 'published_on' IS NOT NULL;

-- The ⓘ says when the source read a value ("read 2018"), not when we stored it.
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
