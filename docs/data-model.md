# Data Model

The database model is built around one core idea:

1. store a canonical company record in `public.companies`
2. keep every upstream payload in `public.company_source_records`
3. keep every field-level fact in `public.company_field_observations`
4. choose winners per field and record the chosen source in `public.company_canonical_field_sources`

## Tables

### `public.companies`

The canonical company profile used by the application.

Important fields:

- `id`: UUID primary key
- `company_name`: latest canonical company name
- `normalized_name`: normalized search key generated with `public.normalize_company_name(...)`
- `registration_number`: unique registry key when available
- `postcode`, `city`, `district`, `locality`: location fields used for search and heuristic deduplication
- `metadata`: system-level structured metadata
- `extra_data`: flexible application-facing enrichment
- `canonical_updated_at`: when canonical values were last resolved
- `data_last_updated_at`: when business data was last refreshed

### `public.sources`

Registry of upstream systems.

Important fields:

- `key`: stable code such as `handelsregister`
- `source_type`: registry, website, api, manual, etc.
- `priority`: lower wins during canonical resolution
- `default_sync_interval_minutes`: scheduling hint for Airflow
- `last_synced_at` and `next_sync_at`: operational sync timestamps

### `public.source_sync_runs`

Operational audit table for sync executions.

Important fields:

- `sync_type`: `full`, `incremental`, `backfill`, `manual`
- `status`: `pending`, `running`, `success`, `failed`, `partial`, `cancelled`
- counters for `records_seen`, `records_created`, `records_updated`, `records_failed`

### `public.company_source_records`

Exactly one current row per `(source_id, external_id)`.

This is the source-specific identity layer that keeps:

- the source payload
- the upstream external identifier
- the source URL
- last-seen and last-synced timestamps
- soft-delete state from the upstream source

### `public.company_field_observations`

Field-level evidence collected from upstream records.

Each row stores:

- the company
- the source record
- the source and sync run that produced it
- the field name
- one typed value column
- confidence and timestamps
- whether the observation is currently canonical

Only one observation per `(company_id, field_name)` may have `is_canonical = true`.

### `public.company_canonical_field_sources`

Pointer table recording which source won each canonical field.

This is what lets the application answer:

- where did the canonical website come from?
- which source currently owns `employee_count`?
- when was that decision made?

### `public.company_managers`

Canonical manager/contact records attached to the company.

## Resolution lifecycle

For each incoming company row from a source:

1. create or update a `source_sync_runs` row
2. upsert a canonical company identity in `companies`
3. upsert the upstream record in `company_source_records`
4. insert one or more `company_field_observations`
5. resolve the winning observation for each field
6. update canonical columns in `companies`
7. upsert the winning source pointer in `company_canonical_field_sources`

## Query groups

The canonical query catalog lives in [core-queries.sql](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/docs/core-queries.sql:1).

The most important groups are:

- writes for canonical company rows
- writes for source records and observations
- reads for canonical company views
- resolution queries for choosing winners
- operational queries for stale data and failed syncs

## Migration strategy

- Keep migrations forward-only
- Never edit an applied migration in shared environments
- Add new schema changes under `db/migrations`
- Run Flyway before Airflow workers interact with changed schema
