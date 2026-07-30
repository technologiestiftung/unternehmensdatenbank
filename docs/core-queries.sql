-- Core query catalog for the Unternehmensdatenbank model.
-- Parameter placeholders use PostgreSQL-style $1, $2, ... notation.

-- 1. Create a company
INSERT INTO public.companies (
  company_name,
  normalized_name,
  legal_form,
  business_purpose,
  address,
  established_at,
  share_capital,
  registration_number,
  website_url,
  funding,
  cluster,
  revenue,
  revenue_yoy,
  revenue_current_year,
  employee_count,
  wz_code,
  tags,
  metadata,
  extra_data,
  canonical_updated_at,
  data_last_updated_at
)
VALUES (
  'Muster GmbH',
  public.normalize_company_name('Muster GmbH'),
  'GmbH',
  'Software development',
  'Berlin, Germany',
  '2020-05-01',
  25000.00,
  'HRB 12345 B',
  'https://muster.de',
  'Seed',
  'AI',
  1200000.00,
  0.15,
  1400000.00,
  18,
  '62.01',
  ARRAY['startup', 'berlin'],
  '{}'::jsonb,
  '{}'::jsonb,
  now(),
  now()
)
RETURNING *;

-- 2. Upsert company by registration number
INSERT INTO public.companies (
  company_name,
  normalized_name,
  registration_number,
  legal_form,
  address,
  website_url,
  data_last_updated_at,
  canonical_updated_at
)
VALUES (
  'Muster GmbH',
  public.normalize_company_name('Muster GmbH'),
  'HRB 12345 B',
  'GmbH',
  'Berlin, Germany',
  'https://muster.de',
  now(),
  now()
)
ON CONFLICT (registration_number)
DO UPDATE SET
  company_name = EXCLUDED.company_name,
  normalized_name = EXCLUDED.normalized_name,
  legal_form = EXCLUDED.legal_form,
  address = EXCLUDED.address,
  website_url = EXCLUDED.website_url,
  data_last_updated_at = now(),
  canonical_updated_at = now()
RETURNING *;

-- 3. Insert source
INSERT INTO public.sources (
  key,
  name,
  source_type,
  base_url,
  priority,
  default_sync_interval_minutes
)
VALUES (
  'handelsregister',
  'Handelsregister',
  'registry',
  'https://example.org',
  10,
  1440
)
RETURNING *;

-- 4. Start sync run
INSERT INTO public.source_sync_runs (
  source_id,
  sync_type,
  status
)
VALUES (
  $1,
  'incremental',
  'running'
)
RETURNING *;

-- $1 = source UUID

-- 5. Finish sync run
UPDATE public.source_sync_runs
SET
  status = 'success',
  finished_at = now(),
  records_seen = 100,
  records_created = 20,
  records_updated = 70,
  records_failed = 10
WHERE id = $1
RETURNING *;

-- 6. Update source sync timestamps
UPDATE public.sources
SET
  last_synced_at = now(),
  next_sync_at = now() + interval '1 day'
WHERE id = $1
RETURNING *;

-- 7. Upsert company source record
INSERT INTO public.company_source_records (
  company_id,
  source_id,
  external_id,
  external_url,
  source_company_name,
  source_payload,
  first_seen_at,
  last_seen_at,
  last_synced_at
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6::jsonb,
  now(),
  now(),
  now()
)
ON CONFLICT (source_id, external_id)
DO UPDATE SET
  company_id = EXCLUDED.company_id,
  external_url = EXCLUDED.external_url,
  source_company_name = EXCLUDED.source_company_name,
  source_payload = EXCLUDED.source_payload,
  last_seen_at = now(),
  last_synced_at = now(),
  is_deleted_in_source = false,
  is_active = true
RETURNING *;

-- Parameters:
-- $1 company_id
-- $2 source_id
-- $3 external_id
-- $4 external_url
-- $5 source_company_name
-- $6 raw json

-- 8. Insert field observation: text
INSERT INTO public.company_field_observations (
  company_id,
  company_source_record_id,
  source_id,
  source_sync_run_id,
  field_name,
  value_text,
  observed_at,
  source_updated_at,
  confidence,
  metadata
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  'website_url',
  'https://muster.de',
  now(),
  now(),
  0.95,
  '{}'::jsonb
)
RETURNING *;

-- 9. Insert field observation: number
INSERT INTO public.company_field_observations (
  company_id,
  company_source_record_id,
  source_id,
  source_sync_run_id,
  field_name,
  value_number,
  observed_at,
  confidence
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  'employee_count',
  18,
  now(),
  0.90
)
RETURNING *;

-- 10. Insert field observation: date
INSERT INTO public.company_field_observations (
  company_id,
  company_source_record_id,
  source_id,
  source_sync_run_id,
  field_name,
  value_date,
  observed_at,
  confidence
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  'established_at',
  '2020-05-01',
  now(),
  0.99
)
RETURNING *;

-- 11. Insert manager
INSERT INTO public.company_managers (
  company_id,
  full_name,
  role,
  email,
  phone,
  is_current,
  metadata
)
VALUES (
  $1,
  'Max Mustermann',
  'Geschäftsführer',
  'max@muster.de',
  '+49 30 123456',
  true,
  '{}'::jsonb
)
RETURNING *;

-- 12. Get full canonical company
SELECT *
FROM public.companies
WHERE id = $1;

-- 13. Get company plus managers
SELECT
  c.*,
  COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'full_name', m.full_name,
        'role', m.role,
        'email', m.email,
        'phone', m.phone,
        'is_current', m.is_current
      )
    ) FILTER (WHERE m.id IS NOT NULL),
    '[]'::jsonb
  ) AS managers
FROM public.companies c
LEFT JOIN public.company_managers m
  ON m.company_id = c.id
WHERE c.id = $1
GROUP BY c.id;

-- 14. Find company by registration number
SELECT *
FROM public.companies
WHERE registration_number = $1;

-- 15. Find company by name
SELECT *
FROM public.companies
WHERE normalized_name = public.normalize_company_name($1)
ORDER BY company_name;

-- 16. Get all source records for a company
SELECT
  csr.*,
  s.key AS source_key,
  s.name AS source_name,
  s.priority
FROM public.company_source_records csr
JOIN public.sources s
  ON s.id = csr.source_id
WHERE csr.company_id = $1
ORDER BY s.priority ASC, csr.last_synced_at DESC;

-- 17. Get all field observations for a company
SELECT
  o.*,
  s.key AS source_key,
  s.name AS source_name
FROM public.company_field_observations o
JOIN public.sources s
  ON s.id = o.source_id
WHERE o.company_id = $1
ORDER BY o.field_name, o.observed_at DESC;

-- 18. Get observations for one field
SELECT
  o.*,
  s.key AS source_key,
  s.name AS source_name,
  s.priority
FROM public.company_field_observations o
JOIN public.sources s
  ON s.id = o.source_id
WHERE o.company_id = $1
  AND o.field_name = 'website_url'
ORDER BY
  o.is_canonical DESC,
  s.priority ASC,
  o.observed_at DESC;

-- 19. Get current canonical source per field
SELECT
  ccfs.field_name,
  s.key AS source_key,
  s.name AS source_name,
  ccfs.selected_at,
  o.value_text,
  o.value_number,
  o.value_date,
  o.value_jsonb
FROM public.company_canonical_field_sources ccfs
JOIN public.sources s
  ON s.id = ccfs.source_id
LEFT JOIN public.company_field_observations o
  ON o.id = ccfs.company_field_observation_id
WHERE ccfs.company_id = $1
ORDER BY ccfs.field_name;

-- 20. Mark one observation as canonical
BEGIN;

UPDATE public.company_field_observations
SET is_canonical = false
WHERE company_id = $1
  AND field_name = $2
  AND is_canonical = true;

UPDATE public.company_field_observations
SET is_canonical = true
WHERE id = $3;

COMMIT;

-- Parameters:
-- $1 company_id
-- $2 field_name
-- $3 observation_id

-- 21. Update canonical company field from winner
UPDATE public.companies
SET
  website_url = $2,
  canonical_updated_at = now(),
  data_last_updated_at = now()
WHERE id = $1
RETURNING *;

-- 22. Upsert canonical field source pointer
INSERT INTO public.company_canonical_field_sources (
  company_id,
  field_name,
  source_id,
  company_field_observation_id,
  selected_at,
  metadata
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  now(),
  '{}'::jsonb
)
ON CONFLICT (company_id, field_name)
DO UPDATE SET
  source_id = EXCLUDED.source_id,
  company_field_observation_id = EXCLUDED.company_field_observation_id,
  selected_at = EXCLUDED.selected_at,
  metadata = EXCLUDED.metadata
RETURNING *;

-- 23. Resolve latest best observation for a field
SELECT
  o.id,
  o.company_id,
  o.field_name,
  o.value_text,
  o.value_number,
  o.value_date,
  o.value_jsonb,
  o.observed_at,
  o.confidence,
  s.id AS source_id,
  s.key AS source_key,
  s.priority
FROM public.company_field_observations o
JOIN public.sources s
  ON s.id = o.source_id
WHERE o.company_id = $1
  AND o.field_name = $2
ORDER BY
  s.priority ASC,
  o.confidence DESC NULLS LAST,
  o.source_updated_at DESC NULLS LAST,
  o.observed_at DESC
LIMIT 1;

-- 24. Latest observation per field for one company
SELECT DISTINCT ON (o.field_name)
  o.field_name,
  o.id AS observation_id,
  o.value_text,
  o.value_number,
  o.value_date,
  o.value_jsonb,
  o.observed_at,
  s.key AS source_key,
  s.priority
FROM public.company_field_observations o
JOIN public.sources s
  ON s.id = o.source_id
WHERE o.company_id = $1
ORDER BY
  o.field_name,
  s.priority ASC,
  o.confidence DESC NULLS LAST,
  o.observed_at DESC;

-- 25. Rebuild all canonical fields for one company
WITH winner AS (
  SELECT
    o.id AS observation_id,
    o.company_id,
    o.field_name,
    o.value_text,
    o.source_id
  FROM public.company_field_observations o
  JOIN public.sources s
    ON s.id = o.source_id
  WHERE o.company_id = $1
    AND o.field_name = 'website_url'
    AND o.value_text IS NOT NULL
  ORDER BY
    s.priority ASC,
    o.confidence DESC NULLS LAST,
    o.source_updated_at DESC NULLS LAST,
    o.observed_at DESC
  LIMIT 1
)
UPDATE public.companies c
SET
  website_url = w.value_text,
  canonical_updated_at = now(),
  data_last_updated_at = now()
FROM winner w
WHERE c.id = w.company_id
RETURNING c.*, w.observation_id, w.source_id;

-- 26. Soft-delete source records no longer present
UPDATE public.company_source_records
SET
  is_deleted_in_source = true,
  is_active = false,
  updated_at = now()
WHERE source_id = $1
  AND external_id = ANY($2::text[]);

-- $2 is an array of external ids.

-- 27. Show stale companies
SELECT *
FROM public.companies
WHERE data_last_updated_at IS NULL
   OR data_last_updated_at < now() - interval '90 days'
ORDER BY data_last_updated_at NULLS FIRST;

-- 28. Show stale source records
SELECT
  csr.*,
  s.key AS source_key
FROM public.company_source_records csr
JOIN public.sources s
  ON s.id = csr.source_id
WHERE csr.last_synced_at < now() - interval '30 days'
ORDER BY csr.last_synced_at ASC;

-- 29. Show failed sync runs
SELECT
  ssr.*,
  s.key AS source_key,
  s.name AS source_name
FROM public.source_sync_runs ssr
JOIN public.sources s
  ON s.id = ssr.source_id
WHERE ssr.status IN ('failed', 'partial')
ORDER BY ssr.started_at DESC;

-- 30. Update extra_data
UPDATE public.companies
SET
  extra_data = extra_data || jsonb_build_object(
    'linkedin_url', 'https://linkedin.com/company/muster'
  )
WHERE id = $1
RETURNING *;

-- 31. Query by tag
SELECT *
FROM public.companies
WHERE tags @> ARRAY['startup']::text[];

-- 32. Query by JSONB metadata
SELECT *
FROM public.companies
WHERE metadata ->> 'import_status' = 'verified';

-- 33. One transaction for full ingestion
BEGIN;

WITH sync_run AS (
  INSERT INTO public.source_sync_runs (source_id, sync_type, status)
  VALUES ($1, 'incremental', 'running')
  RETURNING id, source_id
),
company_upsert AS (
  INSERT INTO public.companies (
    company_name,
    normalized_name,
    registration_number,
    canonical_updated_at,
    data_last_updated_at
  )
  VALUES (
    $2,
    public.normalize_company_name($2),
    $3,
    now(),
    now()
  )
  ON CONFLICT (registration_number)
  DO UPDATE SET
    company_name = EXCLUDED.company_name,
    normalized_name = EXCLUDED.normalized_name,
    canonical_updated_at = now(),
    data_last_updated_at = now()
  RETURNING id
),
source_record AS (
  INSERT INTO public.company_source_records (
    company_id,
    source_id,
    external_id,
    external_url,
    source_company_name,
    source_payload,
    first_seen_at,
    last_seen_at,
    last_synced_at
  )
  SELECT
    cu.id,
    sr.source_id,
    $4,
    $5,
    $2,
    $6::jsonb,
    now(),
    now(),
    now()
  FROM company_upsert cu
  CROSS JOIN sync_run sr
  ON CONFLICT (source_id, external_id)
  DO UPDATE SET
    company_id = EXCLUDED.company_id,
    external_url = EXCLUDED.external_url,
    source_company_name = EXCLUDED.source_company_name,
    source_payload = EXCLUDED.source_payload,
    last_seen_at = now(),
    last_synced_at = now(),
    is_deleted_in_source = false
  RETURNING id, company_id, source_id
)
INSERT INTO public.company_field_observations (
  company_id,
  company_source_record_id,
  source_id,
  source_sync_run_id,
  field_name,
  value_text,
  observed_at,
  confidence
)
SELECT
  sr.company_id,
  sr.id,
  sr.source_id,
  sy.id,
  'website_url',
  $7,
  now(),
  0.95
FROM source_record sr
CROSS JOIN sync_run sy;

COMMIT;

-- 34. Most useful read query for the app
SELECT
  c.*,
  COALESCE(
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'id', m.id,
          'full_name', m.full_name,
          'role', m.role,
          'email', m.email,
          'phone', m.phone,
          'is_current', m.is_current
        )
        ORDER BY m.full_name
      )
      FROM public.company_managers m
      WHERE m.company_id = c.id
    ),
    '[]'::jsonb
  ) AS managers,
  COALESCE(
    (
      SELECT jsonb_object_agg(
        ccfs.field_name,
        jsonb_build_object(
          'source_id', s.id,
          'source_key', s.key,
          'source_name', s.name,
          'selected_at', ccfs.selected_at
        )
      )
      FROM public.company_canonical_field_sources ccfs
      JOIN public.sources s
        ON s.id = ccfs.source_id
      WHERE ccfs.company_id = c.id
    ),
    '{}'::jsonb
  ) AS canonical_field_sources
FROM public.companies c
WHERE c.id = $1;

-- 35. Sync lifecycle summary
-- For every incoming field from a source:
-- 1. upsert or match company
-- 2. upsert source record
-- 3. insert field observation
-- 4. resolve winner
-- 5. update canonical company
-- 6. upsert canonical field source pointer
