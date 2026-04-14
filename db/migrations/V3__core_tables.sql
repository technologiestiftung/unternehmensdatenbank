CREATE TABLE IF NOT EXISTS public.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_name TEXT NOT NULL,
  normalized_name TEXT NOT NULL,
  legal_form TEXT,
  business_purpose TEXT,
  address TEXT,
  founded_at DATE,
  share_capital NUMERIC(18, 2),
  registration_number TEXT,
  website_url TEXT,
  funding TEXT,
  cluster TEXT,
  revenue NUMERIC(18, 2),
  revenue_yoy NUMERIC(12, 6),
  revenue_current_year NUMERIC(18, 2),
  employee_count INTEGER,
  wz_code TEXT,
  tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  extra_data JSONB NOT NULL DEFAULT '{}'::JSONB,
  canonical_updated_at TIMESTAMPTZ,
  data_last_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT companies_registration_number_key UNIQUE (registration_number),
  CONSTRAINT companies_employee_count_check CHECK (employee_count IS NULL OR employee_count >= 0)
);

CREATE TABLE IF NOT EXISTS public.sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  key TEXT NOT NULL,
  name TEXT NOT NULL,
  source_type TEXT NOT NULL,
  base_url TEXT,
  priority INTEGER NOT NULL DEFAULT 100,
  default_sync_interval_minutes INTEGER NOT NULL DEFAULT 1440,
  last_synced_at TIMESTAMPTZ,
  next_sync_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT sources_key_key UNIQUE (key),
  CONSTRAINT sources_priority_check CHECK (priority >= 0),
  CONSTRAINT sources_default_sync_interval_minutes_check CHECK (default_sync_interval_minutes > 0)
);

CREATE TABLE IF NOT EXISTS public.source_sync_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES public.sources(id) ON DELETE CASCADE,
  sync_type TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  records_seen INTEGER NOT NULL DEFAULT 0,
  records_created INTEGER NOT NULL DEFAULT 0,
  records_updated INTEGER NOT NULL DEFAULT 0,
  records_failed INTEGER NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT source_sync_runs_sync_type_check CHECK (sync_type IN ('full', 'incremental', 'backfill', 'manual')),
  CONSTRAINT source_sync_runs_status_check CHECK (status IN ('pending', 'running', 'success', 'failed', 'partial', 'cancelled')),
  CONSTRAINT source_sync_runs_records_seen_check CHECK (records_seen >= 0),
  CONSTRAINT source_sync_runs_records_created_check CHECK (records_created >= 0),
  CONSTRAINT source_sync_runs_records_updated_check CHECK (records_updated >= 0),
  CONSTRAINT source_sync_runs_records_failed_check CHECK (records_failed >= 0)
);

CREATE TABLE IF NOT EXISTS public.company_source_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  source_id UUID NOT NULL REFERENCES public.sources(id) ON DELETE CASCADE,
  external_id TEXT NOT NULL,
  external_url TEXT,
  source_company_name TEXT,
  source_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_deleted_in_source BOOLEAN NOT NULL DEFAULT FALSE,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT company_source_records_source_external_key UNIQUE (source_id, external_id)
);

CREATE TABLE IF NOT EXISTS public.company_field_observations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  company_source_record_id UUID NOT NULL REFERENCES public.company_source_records(id) ON DELETE CASCADE,
  source_id UUID NOT NULL REFERENCES public.sources(id) ON DELETE CASCADE,
  source_sync_run_id UUID REFERENCES public.source_sync_runs(id) ON DELETE SET NULL,
  field_name TEXT NOT NULL,
  value_text TEXT,
  value_number NUMERIC,
  value_date DATE,
  value_jsonb JSONB,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_updated_at TIMESTAMPTZ,
  confidence NUMERIC(5, 4),
  is_canonical BOOLEAN NOT NULL DEFAULT FALSE,
  metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT company_field_observations_confidence_check CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  CONSTRAINT company_field_observations_has_value_check CHECK (
    num_nonnulls(value_text, value_number, value_date, value_jsonb) >= 1
  )
);

CREATE TABLE IF NOT EXISTS public.company_managers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  full_name TEXT NOT NULL,
  role TEXT NOT NULL,
  email TEXT,
  phone TEXT,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.company_canonical_field_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  field_name TEXT NOT NULL,
  source_id UUID NOT NULL REFERENCES public.sources(id) ON DELETE CASCADE,
  company_field_observation_id UUID NOT NULL REFERENCES public.company_field_observations(id) ON DELETE CASCADE,
  selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT company_canonical_field_sources_company_field_key UNIQUE (company_id, field_name)
);

COMMENT ON TABLE public.companies IS 'Canonical company records consumed by the application.';
COMMENT ON TABLE public.sources IS 'Registry of upstream sources with sync scheduling metadata.';
COMMENT ON TABLE public.source_sync_runs IS 'Execution log for source synchronization runs.';
COMMENT ON TABLE public.company_source_records IS 'One raw source record per external company identity and source.';
COMMENT ON TABLE public.company_field_observations IS 'Raw field-level evidence collected from upstream source records.';
COMMENT ON TABLE public.company_managers IS 'Canonical manager contacts attached to a company.';
COMMENT ON TABLE public.company_canonical_field_sources IS 'Selected winning source and observation for each canonical company field.';
