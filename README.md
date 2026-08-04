# Unternehmensdatenbank

Local development scaffold for a Postgres-backed company data platform. The goal is to enrich a dataset of ~400k Berlin companies with structured fields (website, cluster, tech field, CEO contact, revenue, employee count) by running a cost-efficient waterfall of discovery pipelines.

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Postgres | 16 | App database + Airflow metadata (two separate DBs on one server) |
| Airflow | 3.2.0 | Pipeline orchestration (Airflow 3 API) |
| Flyway | 12 | Forward-only SQL migrations |
| Python | 3.12 | DAG logic, psycopg2 for DB writes |

## Data model

The core pattern is **multi-source canonical resolution**: raw observations from each source are promoted to a single winning value per field per company.

```
companies                             ← identity + pipeline metadata only
  └─ company_source_records           ← one raw record per (source, external_id)
       └─ company_field_observations  ← one row per field claim, with confidence
            └─ company_canonical_field_sources  ← winning observation per field
```

**`companies`** holds register fields (name, legal form, address, WZ code, established_at) and `data_quality_score`. It does **not** store enriched values directly.

**`company_field_observations`** stores every claim from every source. Each row has:
- `field_name` — the logical field being claimed (e.g. `candidate_url`, `website`, `cluster`)
- `value_text` / `value_number` / `value_date` / `value_jsonb`
- `confidence` — 0.0–1.0, source- and name-uniqueness-aware
- `is_canonical` — true on the single winning observation per (company, field)

**`company_canonical_field_sources`** has one row per logical field per company, pointing at the winning observation. Written by `pipeline_coordinator`.

**`v_companies_enriched`** — the view applications should query. Joins through the canonical table to expose `website_url`, `cluster`, `tech_field`, `revenue`, `employee_count` alongside identity fields, confidence scores, and winning source key.

### Source priority (lower = more trusted)

| Priority | Source |
|----------|--------|
| 10 | handelsregister |
| 20 | url_discovery |
| 30 | osm_discovery |
| 50 | mistral_search_discovery, claude_search_discovery |
| 60 | ollama_search_discovery |

When multiple sources agree on the same value, each additional agreeing source adds +0.10 to the effective confidence (capped at 1.0).

## Enrichment pipeline

The pipeline is a **waterfall**: free sources run first over all companies; expensive LLM sources run only on companies with `data_quality_score < 0.50`.

```
url_discovery          (free, fast — URL pattern matching + imprint verification)
       │
       └─► pipeline_coordinator  ← auto-triggered on every source DAG completion

osm_discovery          (free, local Overpass — Berlin geodata only)
       │
       └─► pipeline_coordinator

ollama_search_discovery  (free, local LLM via Ollama + SearXNG web search)
       │
       └─► pipeline_coordinator

mistral_search_discovery  (paid, best web search quality)
       │
       └─► pipeline_coordinator

claude_search_discovery   (paid, used for structured field extraction)
       │
       └─► pipeline_coordinator
```

Every source DAG ends with a `TriggerDagRunOperator` that fires `pipeline_coordinator` automatically. The coordinator resolves canonical fields, recomputes `data_quality_score`, and logs field coverage.

### `data_quality_score` field weights

| Field | Weight |
|-------|--------|
| website | 0.30 |
| cluster | 0.20 |
| tech_field | 0.20 |
| ceo_contact | 0.15 |
| revenue | 0.10 |
| employee_count | 0.05 |

Companies with score < 0.50 are the priority LLM enrichment targets.

## DAG directory

See [`airflow/dags/README.md`](airflow/dags/README.md) for a full description of each DAG and shared module.

## Quickstart

```bash
cp .env.example .env
./scripts/dev-up.sh
```

This starts Postgres, runs all Flyway migrations, builds the Airflow image, and brings up all Airflow services. Then open http://localhost:8080 (airflow / airflow), unpause the DAGs you want, and trigger them.

To import a Northdata CSV export into the companies table:

```bash
python scripts/prepare_company_import.py input.csv output.csv
```

## Common commands

```bash
# Full stack startup in correct order
./scripts/dev-up.sh

# Run pending migrations
docker compose --profile tools run --rm flyway

# Check migration state
docker compose --profile tools run --rm flyway info

# Rebuild Airflow image (after changing Dockerfile or requirements.txt)
docker compose build

# Open Airflow CLI shell
docker compose --profile debug run --rm airflow-cli bash

# Stop stack
docker compose down

# Destroy volumes (wipes all data)
docker compose down -v

# Optional: local Overpass for Berlin geodata
docker compose --profile geo up -d overpass

# Optional: local Nominatim for geocoding
docker compose --profile geocoding up -d nominatim
```

## Access

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| pgAdmin | http://localhost:5051 | admin@example.com / admin |
| Postgres | localhost:5433 | app / app — db: unternehmensdatenbank |
| SearXNG | http://localhost:8888 | — |
| Overpass (optional) | http://localhost:12345/api/interpreter | — |
| Nominatim (optional) | http://localhost:8081/reverse | — |

## Environment flags

All DAGs are disabled by default. Enable them in `.env`:

```
ENABLE_URL_DISCOVERY=true
ENABLE_OSM_DISCOVERY=true               # requires local Overpass (Berlin only)
ENABLE_MISTRAL_SEARCH_DISCOVERY=true    # requires MISTRAL_API_KEY
ENABLE_CLAUDE_SEARCH_DISCOVERY=true     # requires ANTHROPIC_API_KEY
ENABLE_OLLAMA_SEARCH_DISCOVERY=true     # requires local Ollama + SearXNG
ENABLE_BERLIN_GEWERBEDATEN_DAG=true     # legacy loader, off by default
```

## Migrations

All schema changes go in `db/migrations/` as `V{N}__description.sql`. Migrations are **forward-only and immutable** once applied. Flyway runs as a one-shot Docker service and does not start automatically with `docker compose up`.
