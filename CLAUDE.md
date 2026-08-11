# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## How to write the docs in this repo

Every `.md` file here is written for a smart 18-year-old who knows how to code but has never
seen this project, German company registers, or Airflow. Follow these rules when you add or
edit documentation:

1. Explain the thing before you use its name. "Impressum — the legal contact page every German
   business website must have" comes before you start saying "the Impressum".
2. Short sentences. One idea each. If a sentence needs a comma to survive, split it.
3. Say what actually happens, not what the code is called. "The score drops by 200" beats
   "a penalty is applied".
4. Numbers over adjectives. Not "coverage is low" — "we find a website for 29 out of 100".
5. Always answer "so what". After a fact, say what it means for the person reading.
6. No unexplained jargon: canonical, observation, resolver, confidence, checkpoint. Each of
   these is a made-up word in this codebase. Define it the first time it appears in a file.
7. Keep the honest caveat. If a number came from 100 companies, say so, and say what that
   means for trusting it.

## What this is

Local development scaffold for a Postgres-backed company data platform. Airflow orchestrates data pipelines. Flyway owns all schema migrations. Two Postgres databases share one server: `airflow` (Airflow metadata) and `unternehmensdatenbank` (application data).

## Stack

- Postgres 16 (port 5433 by default)
- Airflow 3.2.0 (Airflow 3 API — `airflow.providers.standard`, not `airflow.operators`)
- Flyway 12 for SQL-first, forward-only migrations
- Python DAGs with psycopg2 for direct DB writes

## Common commands

```bash
# Full stack startup in correct order
./scripts/dev-up.sh

# Start just Postgres
docker compose up -d postgres

# Run pending migrations
docker compose --profile tools run --rm flyway

# Check migration state
docker compose --profile tools run --rm flyway info

# Start Airflow (after postgres + migrations)
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer

# Rebuild Airflow image (after changing airflow/Dockerfile or requirements.txt)
docker compose build

# Open Airflow CLI shell
docker compose --profile debug run --rm airflow-cli bash

# Stop stack
docker compose down

# Destroy volumes
docker compose down -v
```

Optional services (not started by default):
```bash
docker compose --profile geo up -d overpass       # local Overpass for Berlin
docker compose --profile geocoding up -d nominatim # local Nominatim
```

## Access

- Airflow UI: http://localhost:8080 (user/pass: `airflow`/`airflow`)
- pgAdmin: http://localhost:5051 (email: `admin@example.com`, pass: `admin`)
- Postgres: `localhost:5433`, app DB: `unternehmensdatenbank`, user/pass: `app`/`app`

## Environment setup

```bash
cp .env.example .env
```

All DAGs are **disabled by default** via env flags. Enable them explicitly:
- `ENABLE_MISTRAL_SEARCH_DISCOVERY=true`
- `ENABLE_KUNUNU_ENRICHMENT=true`
- `ENABLE_LINKEDIN_ENRICHMENT=true`
- `ENABLE_BERLIN_GEWERBEDATEN_DAG=true` (legacy, disabled by default)

## Architecture: Data model

The core design is a multi-source canonical resolution pattern:

1. `public.companies` — the current "best" company row consumed by the application
2. `public.company_source_records` — one raw row per `(source_id, external_id)`
3. `public.company_field_observations` — individual field-level claims from sources (e.g. `website_url = example.com` from `handelsregister`)
4. `public.company_canonical_field_sources` — records which observation "won" for each field

Supporting tables: `public.sources` (registry of upstream systems with priority), `public.source_sync_runs` (import job log), `public.company_managers`.

**Resolution lifecycle** for each incoming record:
1. Upsert `sources` row
2. Create `source_sync_runs` row
3. Upsert company identity in `companies`
4. Upsert raw row in `company_source_records`
5. Insert `company_field_observations` (one per field)
6. Promote winning observation (`is_canonical = true`) and update `companies`
7. Upsert `company_canonical_field_sources` with the winning observation

Source priority: lower `priority` integer wins during canonical resolution.

## Architecture: DAGs

There are four DAGs, all in `airflow/dags/`:

**Active (source enrichment):**
- `mistral_search_discovery/main.py` — URL discovery via Mistral web search
- `kununu_enrichment/main.py` — employer review data from Kununu
- `linkedin_enrichment/main.py` — company profiles from LinkedIn
- Shared helpers: `source_enrichment_common.py`

All three enrichment DAGs use `schedule=None` and currently scaffold search payloads without performing live remote calls. The next step is wiring live Mistral API calls, then consuming discovered URLs in the Kununu and LinkedIn DAGs.

**Legacy (disabled by default):**
- `berlin_gewerbedaten/main.py` — fetches the Berlin Gewerbedaten WFS, enriches with addresses (KDTree spatial index over 500k Berlin address points) and OSM POI names (Overpass + rapidfuzz), writes JSONL, then loads into Postgres in batches of 250.

## Architecture: Migrations

All schema changes go in `db/migrations/` as `V{N}__description.sql`. Migrations are forward-only and immutable once applied. Flyway runs as a one-shot Docker service (`--profile tools`); it does not run automatically with `docker compose up`. Run migrations before starting Airflow workers whenever schema changes land.

## Utility scripts

`scripts/ac_measurement.py` measures what the pipeline can actually find when it only has a
company name and address: website, owner name, and tokens spent per business. It writes nothing
to the database, caches each stage so re-runs are free, and reuses the DAGs' own filtering,
scoring and prompt so the numbers describe production behaviour. Results and the method are in
[docs/website-and-owner-discovery-results.md](docs/website-and-owner-discovery-results.md).

Headline, measured on 100 Osnabrück companies: a **verified** website for 29%, a verified owner
name for 7%. Including the Handelsregister number in the search makes website discovery *worse*
(21% vs 29%) because it pulls results toward register aggregators. `ministral-8b-latest` and
`mistral-small-latest` are statistically indistinguishable at this task, so the cheap one wins.
About 1,850 input tokens per business.

`scripts/prepare_company_import.py` converts a Northdata-format CSV export into a clean CSV ready for bulk import into `public.companies`. Run it standalone:

```bash
python scripts/prepare_company_import.py input.csv output.csv
```

The script handles malformed rows with commas in quoted fields (`Gegenstand` column) by detecting and repairing the column offset.
