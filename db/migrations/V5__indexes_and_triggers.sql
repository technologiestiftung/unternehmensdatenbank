CREATE INDEX IF NOT EXISTS companies_normalized_name_idx
  ON public.companies (normalized_name);

CREATE INDEX IF NOT EXISTS companies_data_last_updated_at_idx
  ON public.companies (data_last_updated_at);

CREATE INDEX IF NOT EXISTS companies_tags_gin_idx
  ON public.companies
  USING GIN (tags);

CREATE INDEX IF NOT EXISTS companies_metadata_gin_idx
  ON public.companies
  USING GIN (metadata);

CREATE INDEX IF NOT EXISTS companies_extra_data_gin_idx
  ON public.companies
  USING GIN (extra_data);

CREATE INDEX IF NOT EXISTS sources_next_sync_at_idx
  ON public.sources (next_sync_at);

CREATE INDEX IF NOT EXISTS source_sync_runs_source_started_at_idx
  ON public.source_sync_runs (source_id, started_at DESC);

CREATE INDEX IF NOT EXISTS source_sync_runs_status_started_at_idx
  ON public.source_sync_runs (status, started_at DESC);

CREATE INDEX IF NOT EXISTS company_source_records_company_id_idx
  ON public.company_source_records (company_id);

CREATE INDEX IF NOT EXISTS company_source_records_last_synced_at_idx
  ON public.company_source_records (last_synced_at);

CREATE INDEX IF NOT EXISTS company_source_records_active_idx
  ON public.company_source_records (source_id, is_active, is_deleted_in_source);

CREATE INDEX IF NOT EXISTS company_field_observations_company_field_idx
  ON public.company_field_observations (company_id, field_name, observed_at DESC);

CREATE INDEX IF NOT EXISTS company_field_observations_source_record_idx
  ON public.company_field_observations (company_source_record_id);

CREATE INDEX IF NOT EXISTS company_field_observations_sync_run_idx
  ON public.company_field_observations (source_sync_run_id);

CREATE INDEX IF NOT EXISTS company_field_observations_resolution_idx
  ON public.company_field_observations (
    company_id,
    field_name,
    source_id,
    confidence DESC,
    source_updated_at DESC,
    observed_at DESC
  );

CREATE UNIQUE INDEX IF NOT EXISTS company_field_observations_single_canonical_idx
  ON public.company_field_observations (company_id, field_name)
  WHERE is_canonical;

CREATE INDEX IF NOT EXISTS company_managers_company_id_idx
  ON public.company_managers (company_id);

CREATE INDEX IF NOT EXISTS company_canonical_field_sources_source_idx
  ON public.company_canonical_field_sources (source_id);

DROP TRIGGER IF EXISTS companies_touch_updated_at ON public.companies;
CREATE TRIGGER companies_touch_updated_at
BEFORE UPDATE ON public.companies
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS sources_touch_updated_at ON public.sources;
CREATE TRIGGER sources_touch_updated_at
BEFORE UPDATE ON public.sources
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS source_sync_runs_touch_updated_at ON public.source_sync_runs;
CREATE TRIGGER source_sync_runs_touch_updated_at
BEFORE UPDATE ON public.source_sync_runs
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS company_source_records_touch_updated_at ON public.company_source_records;
CREATE TRIGGER company_source_records_touch_updated_at
BEFORE UPDATE ON public.company_source_records
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS company_field_observations_touch_updated_at ON public.company_field_observations;
CREATE TRIGGER company_field_observations_touch_updated_at
BEFORE UPDATE ON public.company_field_observations
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS company_managers_touch_updated_at ON public.company_managers;
CREATE TRIGGER company_managers_touch_updated_at
BEFORE UPDATE ON public.company_managers
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();

DROP TRIGGER IF EXISTS company_canonical_field_sources_touch_updated_at ON public.company_canonical_field_sources;
CREATE TRIGGER company_canonical_field_sources_touch_updated_at
BEFORE UPDATE ON public.company_canonical_field_sources
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();
