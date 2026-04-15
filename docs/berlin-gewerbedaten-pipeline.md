# Berlin Gewerbedaten Pipeline

This repo contains an Airflow-native version of the Berlin Gewerbedaten importer under [airflow/dags/berlin_gewerbedaten/main.py](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/airflow/dags/berlin_gewerbedaten/main.py:1).

It does two things on each DAG run:

1. fetch and enrich Berlin Gewerbedaten into a persistent JSONL export
2. load that export into the local Postgres schema used by this repository

## DAG

- DAG id: `berlin_gewerbedaten_to_json`
- schedule: `@daily`
- catchup: `false`

Tasks:

- `fetch_and_export`
- `load_json_to_postgres`

## Runtime paths

The Airflow containers mount [airflow/data](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/airflow/data:1) at `/opt/airflow/data`.

Defaults:

- `OUTPUT_FILE=/opt/airflow/data/gewerbedaten.jsonl`
- `ADRESSEN_CACHE_FILE=/opt/airflow/data/adressen_cache.json`

## Python dependencies

The Airflow services now build from [airflow/Dockerfile](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/airflow/Dockerfile:1) and install [airflow/requirements.txt](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/airflow/requirements.txt:1), which includes:

- `requests`
- `pyproj`
- `rapidfuzz`
- `scipy`
- `numpy`
- `psycopg2-binary`

## Database loading strategy

The loader targets the schema created by Flyway migrations in [db/migrations](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/db/migrations:1).

For each imported record it:

1. ensures a `sources` row for `berlin_gewerbedaten`
2. creates a `source_sync_runs` row
3. resolves an existing company by source record, then by `normalized_name + postcode` when a business name exists
4. upserts a `companies` row without holding the whole dataset in memory
4. upserts a `company_source_records` row keyed by `(source_id, external_id)`
5. inserts `company_field_observations`
6. marks canonical observations for `company_name`, `address`, and `legal_form`

Risk controls now built into the pipeline:

- the exporter writes JSONL incrementally instead of building one giant in-memory list
- the loader commits in batches instead of one giant transaction
- public Overpass fallback is disabled by default
- name lookup is disabled by default and can be enabled explicitly when local Overpass is available

Remaining caveat:

- the Gewerbedaten source still does not provide a stable registry number, so identity matching is heuristic until another authoritative source is joined in
- when no business name can be matched from Overpass, the loader uses `berlin_gewerbedaten:<ref_no>` as a placeholder canonical company name

## Environment variables

The Docker Compose Airflow services already inject the important defaults from [.env](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/.env:1):

- `OUTPUT_FILE`
- `ADRESSEN_CACHE_FILE`
- `PG_DSN`
- `MAX_RECORDS`
- `WFS_PAGE_SIZE`
- `SKIP_GEOCODE`
- `SKIP_NAME_LOOKUP`
- `ADRESSEN_MAX_DIST_M`
- `OVERPASS_URL`
- `ALLOW_PUBLIC_OVERPASS_FALLBACK`
- `OVERPASS_RADIUS`
- `OVERPASS_DELAY_S`
- `NAME_MATCH_MIN_SCORE`
- `NOMINATIM_URL`
- `NOMINATIM_DELAY_S`
- `DB_COMMIT_BATCH_SIZE`

## Useful commands

Rebuild the Airflow image after DAG or dependency changes:

```bash
docker compose build airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

Restart the Airflow stack:

```bash
docker compose up -d airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

Inspect DAG import/runtime logs:

```bash
docker compose logs -f airflow-apiserver airflow-scheduler airflow-dag-processor
```

## Local iteration

You can still run the fetch/export step directly without Airflow:

```bash
python3 airflow/dags/berlin_gewerbedaten/main.py
```

That keeps the imported pipeline easy to debug outside the scheduler.
