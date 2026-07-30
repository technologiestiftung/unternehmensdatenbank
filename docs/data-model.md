# Data Model

## ELI5

Think of the database like this:

1. one table stores the "best current company card"
2. one table stores "where did we see this company?"
3. one table stores "what exact facts did that source claim?"
4. one table stores "which source won for each field?"

If that is all you remember, the model will make sense.

## The Simple Story

Imagine three websites talk about the same company:

- source A says the website is `example.com`
- source B says the website is `example.org`
- source C has no website but has a better address

We do **not** want to lose any of that.

So we store:

- the current best company row in `public.companies`
- the raw source row in `public.company_source_records`
- each individual claim like `website_url = example.com` in `public.company_field_observations`
- the final "winner" for each field in `public.company_canonical_field_sources`

That is the whole design.

## Tables

### `public.companies`

This is the table your app reads most of the time.

It is the "current best version" of a company.

If someone asks:

- "show me the company"
- "what is the current website?"
- "what is the current address?"

this is the table they usually mean.

Important fields:

- `id`: UUID primary key
- `company_name`: the current chosen display name
- `normalized_name`: simplified name used for matching and search
- `registration_number`: strongest legal identifier when we have one
- `address`: current chosen address
- `legal_form`: current chosen legal form
- `established_at`: current chosen establishment date when we know it
- `wz_code`: current chosen business classification code
- `business_purpose`: canonical company description / purpose when we know it
- `postcode`, `city`, `district`, `locality`: location fields for search and rough deduplication
- `metadata`: technical/system metadata
- `canonical_updated_at`: when the winning values were last recalculated
- `data_last_updated_at`: when business data last changed

Short version:

- `companies` = "best current company profile"

### `public.sources`

This is the address book of upstream systems.

Examples:

- `handelsregister`
- `berlin_gewerbedaten`
- `northdata`
- `manual_import`

Important fields:

- `key`: stable code such as `handelsregister`
- `source_type`: registry, website, api, manual, etc.
- `priority`: lower wins during canonical resolution
- `default_sync_interval_minutes`: scheduling hint for Airflow
- `last_synced_at` and `next_sync_at`: operational sync timestamps

Short version:

- `sources` = "which systems do we import from?"

### `public.source_sync_runs`

This is the run log.

Every time a source import starts, we create one row here.

It answers questions like:

- "did yesterday's import fail?"
- "how many records did we see?"
- "when did the run finish?"

Important fields:

- `sync_type`: `full`, `incremental`, `backfill`, `manual`
- `status`: `pending`, `running`, `success`, `failed`, `partial`, `cancelled`
- counters for `records_seen`, `records_created`, `records_updated`, `records_failed`

Short version:

- `source_sync_runs` = "logbook of import jobs"

### `public.company_source_records`

Exactly one current row per `(source_id, external_id)`.

This is the raw company row from one source.

If `berlin_gewerbedaten` has company `12345`, that source row lives here.

It keeps:

- the source payload
- the upstream external identifier
- the source URL
- last-seen and last-synced timestamps
- soft-delete state from the upstream source

This table answers:

- "what did this source say about this company?"
- "what is the raw payload we imported?"

Short version:

- `company_source_records` = "one company as seen by one source"

### `public.company_field_observations`

This is the most important table to understand.

Each row is one claim about one field.

Examples:

- company X, field `website_url`, value `https://example.com`, source `handelsregister`
- company X, field `employee_count`, value `18`, source `manual_import`
- company X, field `address`, value `Berlin, ...`, source `berlin_gewerbedaten`

Each row stores:

- the company
- the source record
- the source and sync run that produced it
- the field name
- one typed value column
- confidence and timestamps
- whether the observation is currently canonical

Only one observation per `(company_id, field_name)` may have `is_canonical = true`.

Short version:

- `company_field_observations` = "all the individual facts we ever saw"

### `public.company_canonical_field_sources`

This table records the winner for each field.

This is what lets the application answer:

- where did the canonical website come from?
- which source currently owns `employee_count`?
- when was that decision made?

Example:

- company X
- field `website_url`
- winning source `handelsregister`
- winning observation row `abc-123`

Short version:

- `company_canonical_field_sources` = "which fact won, and from where?"

### `public.company_managers`

This stores people linked to the company after they are accepted into the canonical profile.

Examples:

- managing director
- owner
- board member

Short version:

- `company_managers` = "people attached to the final company profile"

## How One Import Flows Through The Tables

Let’s say one source row comes in.

1. We log the import job in `source_sync_runs`.
2. We ensure the source exists in `sources`.
3. We find or create the company in `companies`.
4. We store the raw source row in `company_source_records`.
5. We split the source row into individual facts and store them in `company_field_observations`.
6. We decide which fact wins for each field.
7. We write the winner back into `companies`.
8. We record the winning source in `company_canonical_field_sources`.

So:

- `companies` is the summary
- `company_source_records` is the raw row
- `company_field_observations` is the evidence
- `company_canonical_field_sources` is the explanation for why the summary looks the way it does

## If You Want The Mental Model In One Sentence

`companies` is the answer, `company_field_observations` is the proof.

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

The canonical query catalog lives in [core-queries.sql](core-queries.sql).

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
