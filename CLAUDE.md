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

- Airflow UI: http://localhost:8082 (user/pass: `airflow`/`airflow`) — the port comes from
  `AIRFLOW_PORT`, default `8082`. **Not 8080.** The container listens on 8080 internally, which
  is what `docker ps` shows, and other Airflow projects on this machine take 8080 on the host —
  logging in there gives "invalid login" because it is a different instance with different users.
- pgAdmin: http://localhost:5051 (email: `admin@example.com`, pass: `admin`)
- Postgres: `localhost:5433`, app DB: `unternehmensdatenbank`, user/pass: `app`/`app`

## Environment setup

```bash
cp .env.example .env
```

All DAGs are **disabled by default** via env flags, checked at task runtime rather than at
parse time. Enable the ones you want: `ENABLE_EXA_SEARCH_DISCOVERY`,
`ENABLE_PARALLEL_SEARCH_DISCOVERY`, `ENABLE_BRAVE_SEARCH_DISCOVERY`,
`ENABLE_MISTRAL_SEARCH_DISCOVERY`, `ENABLE_CLAUDE_SEARCH_DISCOVERY`,
`ENABLE_OLLAMA_SEARCH_DISCOVERY`, `ENABLE_URL_DISCOVERY`, `ENABLE_OSM_DISCOVERY`,
`ENABLE_BERLIN_GEWERBEDATEN_DAG` (legacy).

`.env` is not regenerated from `.env.example` automatically and is missing many of its keys.
That is usually harmless — `docker-compose.yml` supplies a default for every one of them — but
a variable absent from **both** files cannot be changed by editing `.env` alone.

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

All in `airflow/dags/`, all `schedule=None` and triggered by hand or by another DAG — **except
`berlin_gewerbedaten`, which is `@daily`**. That one runs by itself once its flag is `true` and
it is unpaused; every other flag can be turned on with nothing happening until you trigger. Full
detail per DAG is in [airflow/dags/README.md](airflow/dags/README.md).

**Search-provider discovery** — `exa_search_discovery`, `parallel_search_discovery`,
`brave_search_discovery`. These three share `search_discovery_runner.py` verbatim: same model,
same prompt, same scoring, only the search step differs. That is what makes "which provider is
best" a fair question, so do not fork the flow to fit a provider.

**LLM-hosted-search discovery** — `mistral_search_discovery`, `claude_search_discovery` use the
provider's own server-side web search. `ollama_search_discovery` pairs a local SearXNG with a
local model.

**No-API discovery** — `url_discovery` guesses URLs from the company name and checks them.
`osm_discovery` matches against a local Overpass (Berlin data only).

**Coordinator** — `pipeline_coordinator` is the only thing that decides which observation wins a
field. Source DAGs write observations and nothing else. Every source DAG triggers it on
completion.

**Legacy** — `berlin_gewerbedaten` fetches the Berlin Gewerbedaten WFS, enriches with addresses
(KDTree over 500k Berlin address points) and OSM POI names (Overpass + rapidfuzz), writes JSONL,
loads into Postgres in batches of 250. Disabled by default; does not chain into
`pipeline_coordinator`.

Two facts that have misled people repeatedly:

- **`website_url` is a view column, not a table column.** V9 dropped it from `companies`.
  Enriched fields live in `company_canonical_field_sources` and are read through
  `public.v_companies_enriched`.
- **A register number on a page proves the page is *about* a company, never that it *is* that
  company's site.** Registry pages, insolvency trackers and directories all quote them.
  `search_common.is_own_site_candidate` is the single predicate for that call; using a looser
  one anywhere else is the recurring bug in this repo.

## Running discovery and looking at the results

Runbook: [docs/running-discovery.md](docs/running-discovery.md) — which DAGs are runnable with
the current keys, the enable-flag + unpause + trigger sequence, and the SQL for each common
question.

Results live in `public.v_discovery_results` (one row per company) and
`public.v_discovery_scorecard` (one row per source, with coverage percentages) — migration V11.
Read `website_verified_by` before trusting a row: `register_number_match` and `url_pattern` mean
the page was downloaded and checked, `llm_snippet` means a model guessed and nothing was
checked.

A DAG needs **two** switches to run: its `ENABLE_*` env flag set to `true` (checked at task
runtime, so a missing flag fails the task rather than hiding the DAG) **and** unpausing in
Airflow. One run processes `SOURCE_ENRICHMENT_BATCH_SIZE` companies (default 50) and checkpoints
to `airflow/data/<dag_id>_checkpoint.json`, so re-triggering continues rather than restarting.

## Architecture: Migrations

All schema changes go in `db/migrations/` as `V{N}__description.sql`. Migrations are forward-only and immutable once applied. Flyway runs as a one-shot Docker service (`--profile tools`); it does not run automatically with `docker compose up`. Run migrations before starting Airflow workers whenever schema changes land.

## Utility scripts

`scripts/ac_measurement.py` measures what the pipeline can actually find when it only has a
company name and address: website, owner name, and tokens spent per business. It writes nothing
to the database, caches each stage so re-runs are free, and reuses the DAGs' own filtering,
scoring and prompt so the numbers describe production behaviour. Results and the method are in
[docs/website-and-owner-discovery-results.md](docs/website-and-owner-discovery-results.md).

Headline, measured on 100 Osnabrück companies: a **verified** website for 29%, a verified owner
name for 15%. Including the Handelsregister number in the search makes website discovery *worse*
(21% vs 25% on identical scoring) because it pulls results toward register aggregators — so it
is now left out by default. `ministral-8b-latest` and `mistral-small-latest` are statistically
indistinguishable at this task, so the cheap one wins. About 1,850 input tokens per business.

Two companion scripts, both free to run — no search API, no LLM:

- `scripts/check_own_site_filter.py` — 21 real URLs with hand-checked verdicts, guarding
  `is_own_site_candidate` against letting a directory page become a company's website.
- `scripts/check_imprint_owner_extraction.py` — 19 Impressum layouts guarding owner-name
  extraction. `scripts/collect_imprint_fixtures.py` rebuilds the page corpus it was written
  from; its `shapes` output masks every letter and digit, so layouts can be studied without
  exposing anyone's name.

Run both after touching `search_common.py`, `imprint_probe.py` or
`url_discovery/imprint_extractor.py`. Neither needs the database.

**Git Bash caveat:** `docker compose exec -e PYTHONPATH=/opt/airflow/dags` fails under Git Bash,
which rewrites the path into `C:/Program Files/Git/opt/airflow/dags` on the way into the
container. The failure looks like an unrelated `ModuleNotFoundError`. Use PowerShell for these,
or prefix with `MSYS_NO_PATHCONV=1`.

`scripts/prepare_company_import.py` converts a Northdata-format CSV export into a clean CSV ready for bulk import into `public.companies`. Run it standalone:

```bash
python scripts/prepare_company_import.py input.csv output.csv
```

The script handles malformed rows with commas in quoted fields (`Gegenstand` column) by detecting and repairing the column offset.
