-- Feedback on every field the app shows, and where every shown value came from.
--
-- 1. extraction_feedback accepted votes on three fields. Now on every field of the company view,
--    under the same keys the app uses, and also on an EMPTY field: `value = ''` means "nothing was
--    shown", and a `wrong` vote on it carries the missing value as its correction.
-- 2. company_managers kept only the name. Now each name points at the observation and source it
--    came from, so the app can say who read it, when, and from which Impressum.
-- 3. v_field_provenance: one row per company, field and shown value, with source, method,
--    confidence, evidence, a link and dates. Read by the app's info button.

-- ------------------------------------------------------------------------------------------------
-- 1. Feedback on every shown field
-- ------------------------------------------------------------------------------------------------
-- The keys are the app's column keys (src/lib/columns.tsx). `comment` and `updated_at` are left
-- out on purpose: one is already user input, the other a system date.
ALTER TABLE public.extraction_feedback
    DROP CONSTRAINT extraction_feedback_field_name_check,
    ADD CONSTRAINT extraction_feedback_field_name_check CHECK (field_name IN (
        'name', 'address', 'legal_form', 'register_status', 'wz_code', 'established_at',
        'employee_count', 'revenue', 'fundings', 'cluster', 'technologiefeld', 'website',
        'management', 'business_purpose', 'deeptech', 'is_ai', 'amazon_effect'
    )),
    DROP CONSTRAINT extraction_feedback_value_check,
    -- Empty is allowed: a vote on a field that showed nothing.
    ADD CONSTRAINT extraction_feedback_value_check CHECK (length(value) <= 500),
    -- A vote on an empty field can only say "something is missing", and must say what.
    ADD CONSTRAINT extraction_feedback_empty_needs_correction CHECK (
        value <> '' OR (verdict = 'wrong' AND correction IS NOT NULL)
    );

COMMENT ON COLUMN public.extraction_feedback.value IS
  'The value shown when the vote was cast; '''' when the field showed nothing. For `fundings` it is the grant''s Förderkennzeichen (FKZ), one vote per grant.';

-- ------------------------------------------------------------------------------------------------
-- 2. Where each manager name came from
-- ------------------------------------------------------------------------------------------------
ALTER TABLE public.company_managers
    ADD COLUMN company_field_observation_id uuid
        REFERENCES public.company_field_observations(id) ON DELETE SET NULL,
    ADD COLUMN source_id uuid REFERENCES public.sources(id) ON DELETE SET NULL;

-- Backfill the names written before this column existed: the best observation of the same name.
UPDATE public.company_managers m
SET company_field_observation_id = best.id, source_id = best.source_id
FROM (
    SELECT DISTINCT ON (o.company_id, o.value_text) o.company_id, o.value_text, o.id, o.source_id
    FROM public.company_field_observations o
    JOIN public.sources s ON s.id = o.source_id
    WHERE o.field_name = 'ceo_name_or_contact'
    ORDER BY o.company_id, o.value_text, s.priority, o.confidence DESC NULLS LAST, o.observed_at DESC
) best
WHERE best.company_id = m.company_id AND best.value_text = m.full_name
  AND m.company_field_observation_id IS NULL;

-- ------------------------------------------------------------------------------------------------
-- 3. Provenance of every shown value
-- ------------------------------------------------------------------------------------------------
-- Field keys match the app and extraction_feedback. Register facts come from the company's register
-- record and say when the register was read. Nothing personal is exposed: no officer names, no raw
-- register text, only source, method, dates and links.
CREATE VIEW public.v_field_provenance AS
-- Values the coordinator chose from observations: website, technologiefeld, cluster, revenue, ...
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
-- Every manager name, with the observation it was read from.
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
-- Register facts, as of the register snapshot the company came from.
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
-- Each grant, and how it was linked to the company.
SELECT
    fu.company_id,
    'fundings',
    fu.fkz,
    'foerderkatalog',
    'Förderkatalog des Bundes',
    'linked by recipient name and city',
    NULL::numeric,
    NULL::jsonb,
    NULL::text,
    fu.first_seen_at,
    fu.last_seen_at
FROM public.v_company_fundings fu;

COMMENT ON VIEW public.v_field_provenance IS
  'Where every value the app shows came from: source, method, confidence, evidence, link and dates, one row per company, field and value. No personal data beyond the shown value.';

GRANT SELECT ON public.v_field_provenance TO ${reader_role};
