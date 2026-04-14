CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.sources (
  id BIGSERIAL PRIMARY KEY,
  slug TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON SCHEMA app IS 'Application schema for Unternehmensdatenbank.';
COMMENT ON TABLE app.sources IS 'Registry of upstream data sources for Airflow-managed sync jobs.';
