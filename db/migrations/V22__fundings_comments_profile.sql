-- V22: Förderungen, user comments, and the one view the app reads.
--
-- WHY
--
-- The target record for every company has twenty columns (docs/target-columns.md). Fourteen
-- already exist in `v_companies_enriched`. This migration adds what was missing:
--
-- 1. `company_fundings` -- one row per grant from the Förderkatalog des Bundes, keyed by the
--    Förderkennzeichen (FKZ). The `foerderkatalog` DAG fills it monthly.
-- 2. `company_comments` -- what users write in the app. The only table the reader role may write,
--    because Hasura connects as the reader and the app has no backend of its own.
-- 3. Four reserved observation fields, read the same three-join way as every other field:
--    `deeptech_class`, `is_ai` (text 'true'/'false'), `amazon_effect` (meaning not defined yet --
--    reserved so the column exists, nothing writes it). The confidence of the DeepTech
--    classification is the observation's own `confidence`, not a field of its own.
-- 4. `v_company_profile` -- the target record, one row per company.
--
-- FUNDINGS ARE NOT LINKED BY A STORED company_id
--
-- The Förderkatalog has no register number, only a recipient name and a city. A stored link would
-- freeze whatever matching rule wrote it. `v_company_fundings` matches on every read instead:
-- normalised name equal AND same city AND that name unique in that city. An ambiguous name links
-- to nobody rather than to the wrong company. Measured on the Förderkatalog export of 2026-10-01:
-- 19,470 grants, 2,994 recipients, many of them universities and institutes, not companies -- so
-- most grants are expected to stay unlinked. Locally it links nothing at all, because the local
-- companies are Osnabrück and every recipient is in Berlin.

CREATE TABLE IF NOT EXISTS public.company_fundings (
  fkz TEXT PRIMARY KEY,
  recipient_name TEXT NOT NULL,
  recipient_normalized_name TEXT GENERATED ALWAYS AS (public.normalize_company_name(recipient_name)) STORED,
  city TEXT,
  amount_eur NUMERIC(14, 2),
  period_from DATE,
  period_to DATE,
  topic TEXT,
  ministry TEXT,
  funding_type TEXT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS company_fundings_recipient_idx
  ON public.company_fundings (recipient_normalized_name, lower(city));

COMMENT ON TABLE public.company_fundings IS
  'Grants from the Förderkatalog des Bundes, one row per Förderkennzeichen. Not linked to '
  'companies here; v_company_fundings does the matching on read. See V22.';

CREATE OR REPLACE VIEW public.v_company_fundings AS
WITH unique_names AS (
  SELECT normalized_name, lower(city) AS city, (array_agg(id))[1] AS company_id
  FROM public.companies
  WHERE city IS NOT NULL
  GROUP BY normalized_name, lower(city)
  HAVING count(*) = 1
)
SELECT u.company_id, f.*
FROM public.company_fundings f
JOIN unique_names u
  ON u.normalized_name = f.recipient_normalized_name
 AND u.city = lower(f.city);

CREATE TABLE IF NOT EXISTS public.company_comments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  author_id TEXT NOT NULL,
  body TEXT NOT NULL CHECK (length(trim(body)) BETWEEN 1 AND 5000),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS company_comments_company_idx
  ON public.company_comments (company_id, created_at DESC);

COMMENT ON COLUMN public.company_comments.author_id IS
  'Keycloak `sub` of the writer, set by Hasura from the JWT, never by the client.';

GRANT INSERT ON public.company_comments TO ${reader_role};

CREATE OR REPLACE VIEW public.v_company_profile AS
SELECT
  e.id,
  e.company_name                         AS name,
  e.employee_count,
  e.revenue,
  coalesce(fund.funding_count, 0)        AS funding_count,
  fund.funding_total_eur,
  e.wz_code,
  e.established_at,
  e.address,
  c.postcode,
  c.city,
  e.legal_form,
  e.cluster,
  e.technologiefeld,
  e.website_url                          AS website,
  mgr.managers                           AS management,
  e.business_purpose,
  obs_deeptech.value_text                AS deeptech_class,
  obs_deeptech.confidence                AS deeptech_confidence,
  obs_ai.value_text::BOOLEAN             AS is_ai,
  obs_amazon.value_text                  AS amazon_effect,
  e.register_status,
  greatest(e.data_last_updated_at, e.canonical_updated_at) AS updated_at
FROM public.v_companies_enriched e
JOIN public.companies c ON c.id = e.id
LEFT JOIN LATERAL (
  SELECT count(*) AS funding_count, sum(f.amount_eur) AS funding_total_eur
  FROM public.v_company_fundings f
  WHERE f.company_id = e.id
) fund ON TRUE
LEFT JOIN LATERAL (
  SELECT string_agg(DISTINCT cm.full_name, '; ' ORDER BY cm.full_name) AS managers
  FROM public.company_managers cm
  WHERE cm.company_id = e.id AND cm.is_current
) mgr ON TRUE
LEFT JOIN public.company_canonical_field_sources cfs_deeptech
       ON cfs_deeptech.company_id = e.id AND cfs_deeptech.field_name = 'deeptech_class'
LEFT JOIN public.company_field_observations obs_deeptech
       ON obs_deeptech.id = cfs_deeptech.company_field_observation_id
LEFT JOIN public.company_canonical_field_sources cfs_ai
       ON cfs_ai.company_id = e.id AND cfs_ai.field_name = 'is_ai'
LEFT JOIN public.company_field_observations obs_ai
       ON obs_ai.id = cfs_ai.company_field_observation_id
LEFT JOIN public.company_canonical_field_sources cfs_amazon
       ON cfs_amazon.company_id = e.id AND cfs_amazon.field_name = 'amazon_effect'
LEFT JOIN public.company_field_observations obs_amazon
       ON obs_amazon.id = cfs_amazon.company_field_observation_id;

COMMENT ON VIEW public.v_company_profile IS
  'The target record per company, read by the app through Hasura. Comments are a separate table '
  '(company_comments). deeptech_class, is_ai and amazon_effect are NULL until something writes them.';
