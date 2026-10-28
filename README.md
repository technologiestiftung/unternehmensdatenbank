# Unternehmensdatenbank

Local development scaffold for a Postgres-backed German company data platform. The goal is to fill in the missing fields of a register export — website, industry field, CEO contact, revenue, employee count — cheaply, and to record *why* every value was chosen.

**Everything here runs on example data.** The final dataset, Berlin companies probably delivered quarterly by the vendor Fusionbase ([docs/target-columns.md](docs/target-columns.md)), has not arrived yet. Until it does, the database holds **14,443 companies from a Northdata export of the Osnabrück region**, mostly a name and an address. Osnabrück is the **benchmark**, not the target: every number in this repo is an Osnabrück measurement that Berlin will replace. Over half the table (7,673 of 14,443) is `erloschen`, meaning dissolved, which is worth knowing before reading any coverage figure.

Current website coverage: **3,529 of 14,443**. Where they came from is the priority table below.

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Postgres | 15 + PostGIS + pgvector | App databases (prod + staging) + Airflow metadata, as on the Berlin Data Hub |
| Airflow | 3.2.0 | Pipeline orchestration (Airflow 3 API) |
| Flyway | 12 | Forward-only SQL migrations |
| Python | 3.12 | DAG logic, psycopg2 for DB writes |
| Keycloak · Hasura · RustFS (S3) | 26.8 · 2.51 · 1.0 | Data Hub emulation, profile `datahub` — see [docs/datahub-emulation.md](docs/datahub-emulation.md) |

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

⚠️ **Four columns in that view look like the same question and are not.** `cluster` and `tech_field` are free text an LLM typed, with no controlled vocabulary. `technologiefeld` is one of Berlin's 77 taxonomy fields and requires a website somebody actually fetched. `purpose_field` is one of the same 77 taken from the register purpose, with no website requirement at all. **They must never be merged or coalesced**, and a WZ code may never fill `technologiefeld`. `technologiefeld` and `purpose_field` were reserved by V20 and are now filled — written by `scripts/import_branch_classification.py` and published by `pipeline_coordinator`. Their counts are pinned as `CANONICAL_OTHER_FIELDS` in `tests/integration/test_ground_truth_discovery.py`; no row of either has been hand-checked.

### Source priority (lower = more trusted)

Measured from the `sources` table, 2026-09-09. Pinned in `tests/integration/test_ground_truth_discovery.py`.

| Priority | Source key | Canonical websites |
|----------|------------|--------------------|
| **50** | `northdata_export` | **2,815** |
| 100 | `combined_discovery` | 552 |
| 100 | `brave_search_discovery` | 85 |
| 100 | `exa_search_discovery` | 39 |
| 100 | `url_discovery` | 37 |
| 100 | `parallel_search_discovery` | 1 |
| 100 | `branch_classifier` | 0 — it writes no URL; it fills the two industry columns |
| | | **3,529** |

Those seven keys are the whole table. **Priority is sorted before confidence, so it is absolute**: nothing at 100 beats anything at 50, at any confidence. That is why 2,815 export URLs nobody has ever fetched outrank pages that were downloaded and register-checked — intended, because a URL filed with the register is better evidence than a domain guessed from a company name. Among the six sources at 100, confidence alone decides.

When multiple sources agree on the same value, each additional agreeing source adds +0.10 to the effective confidence (capped at 1.0).

## Enrichment pipeline

**Five pipelines, not a waterfall.** They are independent: each has its own source key, its own record of which companies it has handled, and its own checkpoint file. Running one does not stop another from working the same company — they meet only in `pipeline_coordinator`, which picks one winner per field.

```
P1  northdata_export        priority 50 · free · 2,815 websites
    scripts/import_northdata_export.py, then import_northdata_websites.py
    The register CSV. Biggest single win by a wide margin, and the only
    source at priority 50, so it outranks everything below.
         │
P2  brave_ / exa_ / parallel_search_discovery    priority 100 · PAID · 85 / 39 / 1
    A search API, then fetch the page and check its Impressum.
    All three share airflow/dags/search_discovery_runner.py VERBATIM —
    only the search step differs, which is what makes "which provider is
    best" a fair question. Do not fork the flow to fit a provider.
         │
P3  combined_discovery      priority 100 · free · 552 websites
    scripts/run_combined_discovery.py — a SCRIPT, not a DAG.
    Guess ~20 domains from the name → DNS → fetch → match OpenStreetMap →
    a LOCAL model reads the page and says whose it is.
    943 websites of its own, 886 of them high-confidence.
         │
P4  url_discovery           priority 100 · free · 37 websites
    Guesses domains from the name inside Airflow, verifies the Impressum.
         │
P5  branch_classifier       priority 100 · free · 0 rows so far
    The 77-field Berlin industry taxonomy. Reserved by V20; the write
    path lands after the Berlin import.
         │
         └─────────────►  pipeline_coordinator
                          the ONLY thing that decides which claim wins a
                          field. Source pipelines write observations and
                          nothing else. Runs automatically — P1–P4 all
                          trigger it themselves when they finish.
```

**Not deployed:** `mistral_search_discovery`, `claude_search_discovery` and `ollama_search_discovery` exist as code and have **never written a row** — they have no entry in the `sources` table at all. Mistral and Claude use the provider's own server-side search, which is why they cannot share P2's runner. Ollama pairs a local SearXNG with a local model, and SearXNG's upstream engines serve a CAPTCHA to this network, so it finds nothing. `osm_discovery` is Berlin-only and needs a local Overpass; combined discovery's `osm` stage covers the same ground for Osnabrück.

Each source DAG ends with a `TriggerDagRunOperator`; P3, being a script, calls the Airflow API itself. The coordinator resolves canonical fields, recomputes `data_quality_score`, and logs field coverage.

### `data_quality_score` field weights

| Field | Weight |
|-------|--------|
| website | 0.30 |
| cluster | 0.20 |
| tech_field | 0.20 |
| ceo_contact | 0.15 |
| revenue | 0.10 |
| employee_count | 0.05 |

### The spend guard has never fired

Discovery skips companies whose `data_quality_score` is already **≥ 0.90**, so money is not spent on companies that are complete. Measured 2026-09-09: the highest score in the table is **0.850** and **0 companies** reach the guard, because no company has enough fields filled to sum past 0.85. So the guard is currently unreachable and skips nobody. 13,969 of 14,443 companies score under 0.50.

Weights sum to 1.00 and re-weighting invalidates every score already stored, so it is not a free knob.

## DAG directory

See [`airflow/dags/README.md`](airflow/dags/README.md) for a full description of each DAG and shared module.

## Quickstart

```bash
cp .env.example .env
./scripts/dev-up.sh
```

This writes a fresh Airflow Fernet key into `.env` if the value there is empty (never overwriting one), starts Postgres, runs all Flyway migrations, builds the Airflow image, and brings up all Airflow services. Then open http://localhost:8082 (airflow / airflow), unpause the DAGs you want, and trigger them.

For which DAGs to run and how to read what they found, see [docs/running-discovery.md](docs/running-discovery.md).

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
| Airflow UI | http://localhost:8082 | airflow / airflow |
| pgAdmin | http://localhost:5051 | admin@example.com / admin |
| Postgres | localhost:5433 | app / app (owner), writer / writer (DAGs), reader / reader (Hasura) — dbs: unternehmensdatenbank, unternehmensdatenbank_staging |
| Hasura (`datahub`) | http://localhost:8090/console | admin secret local-admin-secret |
| Keycloak (`datahub`) | http://localhost:8180/admin | admin / admin · app user dev / dev |
| S3 / RustFS (`datahub`) | http://localhost:9001 | localaccess / localsecret |
| SearXNG | http://localhost:8888 | — |
| Overpass (optional) | http://localhost:12345/api/interpreter | — |
| Nominatim (optional) | http://localhost:8081/reverse | — |

## Environment flags

All DAGs are disabled by default, and a DAG needs **two** switches: its `ENABLE_*` flag *and* unpausing in the Airflow UI. The flag is read at task **runtime**, so a disabled DAG still appears in the UI and its task raises `RuntimeError: <FLAG> is not enabled` rather than skipping — which means a missing flag fails loudly instead of hiding the DAG.

```
# Live
ENABLE_URL_DISCOVERY=true
ENABLE_FOERDERKATALOG=true              # @monthly, Förderkatalog des Bundes, free
ENABLE_BRAVE_SEARCH_DISCOVERY=true      # requires BRAVE_API_KEY, costs money
ENABLE_EXA_SEARCH_DISCOVERY=true        # requires EXA_API_KEY, costs money
ENABLE_PARALLEL_SEARCH_DISCOVERY=true   # anonymous MCP, hard ~20-search quota per process

# Never written a row — no entry in the sources table
ENABLE_MISTRAL_SEARCH_DISCOVERY=true    # requires MISTRAL_API_KEY
ENABLE_CLAUDE_SEARCH_DISCOVERY=true     # requires ANTHROPIC_API_KEY
ENABLE_OLLAMA_SEARCH_DISCOVERY=true     # local Ollama + SearXNG; SearXNG is CAPTCHA-blocked here
ENABLE_OSM_DISCOVERY=true               # requires local Overpass (Berlin only)
ENABLE_BERLIN_GEWERBEDATEN_DAG=true     # legacy loader, and the only @daily DAG
```

One run processes `SOURCE_ENRICHMENT_BATCH_SIZE` companies (default 50) and checkpoints to `airflow/data/<dag_id>_checkpoint.json`, so re-triggering continues rather than restarts.

`.env` is **not** regenerated from `.env.example`, and `.env.example` is loaded by nothing. A variable must also appear in `x-airflow-common.environment` in `docker-compose.yml`, because the containers do not inherit `.env` — one absent from **both** cannot be changed by editing `.env` alone. Env is baked in at container start: after editing `.env`, run `docker compose up -d` on the Airflow services. A restart is not enough.

## Migrations

All schema changes go in `db/migrations/` as `V{N}__description.sql`, currently **V1–V22**. Migrations are **forward-only and immutable** once applied — never edit an applied file, add a new one. Flyway runs as a one-shot Docker service under `--profile tools` and does **not** start with `docker compose up`, so run migrations before starting Airflow whenever schema changes land.

The four most recent, because they changed behaviour and not just shape:

| | |
|---|---|
| **V17** | deduped observations and added the upsert key, so a re-run updates in place instead of appending |
| **V18** | widened that key from two field names to six |
| **V19** | deduped `company_managers` 9,195 → 1,047 and added the unique constraint its `ON CONFLICT` had always needed |
| **V20** | added the `branch_classifier` source and reserved `technologiefeld` / `purpose_field` in `v_companies_enriched`. Schema only at the time; the write path landed later and both columns are now served |
| **V21** | grants for the Data Hub's three roles: `writer` (DAGs) may write rows but not DDL, `reader` (Hasura) may only read. Role names are Flyway placeholders |
| **V22** | `company_fundings`, `company_comments` and `v_company_profile`, the target record per company ([docs/target-columns.md](docs/target-columns.md)) |

## Tests and guards

```bash
pytest -q                                                  # 21 tests
pytest -m "not integration"                                # the subset needing neither disk nor Docker
python tests/integration/test_ground_truth_columns.py      # also runs standalone, without pytest
python scripts/check_branch_classifier.py                  # one of 7 guard scripts
```

`tests/` is the pytest tree — `unit/` for tests needing nothing but the code, `integration/` for tests reading the artifacts on disk or the database. Alongside it are **7 guard scripts** in `scripts/` (`check_*.py` plus `replay_judge_gate.py`), all free: no API, no model, no database, no network. A `PostToolUse` hook runs five of them after an edit under `combined_discovery/`, `scripts/` or `tests/`.

The two ground-truth modules assert **counts, never correctness** — not one row of the industry classification has been hand-labelled, so there is no precision figure to assert. A failure means "these numbers moved, find out why". They also run standalone without pytest, which is deliberate: pytest is host-only and absent from `airflow/requirements.txt`.

**They pin one particular copy of the example data**: the database and the files under `airflow/data/combined_discovery/` on the machine where the discovery and classification runs were made (14,443 companies, 3,529 canonical websites). A machine with a different or partial copy fails them with messages like `companies got=7449 want=14443`. That is not a code regression, it just means "not that copy". The pre-commit hook runs these tests, so on such a machine it blocks every commit. To get the pinned copy, take a `pg_dump` of `unternehmensdatenbank` and the `airflow/data/combined_discovery/` folder from that machine, restore both here, and run Flyway. When the final Berlin data arrives, the pinned numbers are replaced wholesale, following the rule in [docs/guards-and-tests.md](docs/guards-and-tests.md).
