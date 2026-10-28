-- A second grant source: Berlin's own Zuwendungsdatenbank, linked by register number.
--
-- `company_fundings` held only the Förderkatalog des Bundes, linked to a company by name and city.
-- The Berlin Zuwendungsdatenbank (Senatsverwaltung für Finanzen, 55,454 grants 2021–2025 on
-- 2026-10-28) names each recipient's register entry (`hrb_084522`), so its grants are linked
-- exactly: 18,421 of them to 3,861 of our companies. The DAG `berlin_zuwendungen` loads it.
--
-- 1. Each grant says which source it came from, and Berlin grants keep their register entry and
--    policy area. The Förderkatalog rows keep the default.
ALTER TABLE public.company_fundings
    ADD COLUMN source text DEFAULT 'foerderkatalog' NOT NULL,
    ADD COLUMN recipient_register text,
    ADD COLUMN policy_area text;

COMMENT ON TABLE public.company_fundings IS
  'Grants, one row per grant: the Förderkatalog des Bundes (fkz = Förderkennzeichen) and the Berlin Zuwendungsdatenbank (fkz = ''berlin-'' || its id). company_funding_links links them to companies.';
COMMENT ON COLUMN public.company_fundings.recipient_register IS
  'The recipient''s register entry as in company_identifiers, without the court (''HRB 84522''). Set only by sources that name it; such a grant is linked by it and never by name.';

-- Both grant sources as rows, so the app's source filter and the ⓘ can name them.
INSERT INTO public.sources (key, name, source_type, base_url) VALUES
    ('foerderkatalog', 'Förderkatalog des Bundes', 'fundings', 'https://foerderportal.bund.de/foekat/'),
    ('berlin_zuwendungen', 'Zuwendungsdatenbank des Landes Berlin', 'fundings',
     'https://www.berlin.de/sen/finanzen/service/zuwendungsdatenbank/')
ON CONFLICT (key) DO NOTHING;

-- 2. Link by register number where the grant names one, by name and city otherwise. A register
--    number that matches none of our companies stays unlinked: it is not ours, whatever the name says.
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
    FROM public.companies c
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

-- 3. The source filter: a grant counts for the source it came from. Same columns as V5.
CREATE OR REPLACE VIEW public.v_company_sources AS
SELECT cfs.company_id, cfs.field_name, s.key AS source_key
FROM public.company_canonical_field_sources cfs
JOIN public.sources s ON s.id = cfs.source_id
UNION ALL
SELECT m.company_id, 'management', s.key
FROM public.company_managers m
JOIN public.sources s ON s.id = m.source_id
WHERE m.is_current
UNION ALL
SELECT r.company_id, 'register', s.key
FROM public.company_source_records r
JOIN public.sources s ON s.id = r.source_id AND s.source_type = 'register'
UNION ALL
SELECT l.company_id, 'fundings', f.source
FROM public.company_funding_links l
JOIN public.company_fundings f ON f.fkz = l.fkz;

-- 4. The ⓘ: each grant names its source and how it was linked. Same columns as V3; only the last
--    part changed.
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
    NULL::jsonb,
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
