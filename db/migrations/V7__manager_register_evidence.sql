-- The ⓘ of a manager name shows whether the register confirms it.
--
-- spaCy's names now carry `evidence` in their observation's metadata: `["register_officer"]` when
-- the name is one of the company's officers in the register, `[]` when it is not (see
-- `manager_ner.main._mark_register_officers`). Which officer matched is never stored. The
-- management part of v_field_provenance passed NULL for evidence; it now passes the observation's.
-- Same columns as V6; only that one line changed.
CREATE OR REPLACE VIEW public.v_field_provenance AS
SELECT
    cfs.company_id,
    cfs.field_name,
    COALESCE(o.value_text, o.value_number::text, o.value_date::text) AS value,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE(o.metadata->>'verified_by', o.metadata->>'decided_by', s.source_type) AS method,
    o.confidence,
    COALESCE(o.metadata->'evidence', o.metadata->'keywords') AS evidence,
    COALESCE(o.metadata->>'imprint_url', o.metadata->>'matched_url') AS link,
    o.observed_at,
    cfs.selected_at
FROM public.company_canonical_field_sources cfs
JOIN public.company_field_observations o ON o.id = cfs.company_field_observation_id
JOIN public.sources s ON s.id = cfs.source_id

UNION ALL
SELECT
    m.company_id,
    'management',
    m.full_name,
    s.key,
    s.name,
    COALESCE(o.metadata->>'model_version', s.source_type),
    o.confidence,
    o.metadata->'evidence',
    o.metadata->>'imprint_url',
    o.observed_at,
    m.updated_at
FROM public.company_managers m
LEFT JOIN public.company_field_observations o ON o.id = m.company_field_observation_id
LEFT JOIN public.sources s ON s.id = m.source_id
WHERE m.is_current

UNION ALL
SELECT
    c.id,
    f.field_name,
    f.value,
    s.key,
    s.name,
    f.method,
    NULL::numeric,
    NULL::jsonb,
    NULL::text,
    (r.source_payload #>> '{register,retrieved_at}')::timestamp with time zone,
    r.last_synced_at
FROM public.companies c
JOIN public.company_source_records r ON r.company_id = c.id
JOIN public.sources s ON s.id = r.source_id AND s.source_type = 'register'
CROSS JOIN LATERAL (VALUES
    ('name', c.company_name, 'register'),
    ('address', c.address, 'register'),
    ('legal_form', c.legal_form, 'read from the end of the registered name'),
    ('register_status', c.register_status, 'register'),
    ('wz_code', c.wz_code, 'register'),
    ('business_purpose', c.business_purpose, 'register'),
    ('established_at', c.established_at::text, 'register')
) AS f(field_name, value, method)
WHERE f.value IS NOT NULL

UNION ALL
SELECT
    l.company_id,
    'fundings',
    f.fkz,
    f.source,
    COALESCE(s.name, f.source),
    CASE l.match_method
      WHEN 'register_number' THEN 'linked by register number'
      ELSE 'linked by recipient name and city'
    END,
    NULL::numeric,
    NULL::jsonb,
    NULL::text,
    f.first_seen_at,
    f.last_seen_at
FROM public.company_funding_links l
JOIN public.company_fundings f ON f.fkz = l.fkz
LEFT JOIN public.sources s ON s.key = f.source;
