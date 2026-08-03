# Unternehmensdatenbank

Local development scaffold for a Postgres-backed company data platform with Airflow for orchestration and Flyway for schema migrations.

## Stack

- Postgres `16` for both Airflow metadata and the application database
- Airflow `3.2.0` for local DAG development
- Flyway `12` for SQL-first schema migrations

## Why this setup

- Airflow orchestrates syncs and workflows
- Flyway owns schema changes
- Postgres is split into two databases on the same local server:
  - `airflow` for Airflow's metadata
  - `unternehmensdatenbank` for the actual application schema

That separation keeps Airflow infrastructure state away from the domain data model.

## Repository structure

```text
.
├── airflow/
│   ├── config/
│   ├── dags/
│   └── plugins/
├── db/
│   └── migrations/
├── postgres/
│   └── init/
└── docker-compose.yml
```

## Quickstart

1. Create a local env file:

```bash
cp .env.example .env
```

Or do the whole startup sequence in one command:

```bash
./scripts/dev-up.sh
```

2. Start Postgres:

```bash
docker compose up -d postgres
```

3. Run database migrations:

```bash
docker compose --profile tools run --rm flyway
```

4. Initialize Airflow metadata and create the local admin user:

```bash
docker compose up airflow-init
```

5. Start Airflow:

```bash
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

## Access

- Airflow UI/API: http://localhost:8082
- pgAdmin: http://localhost:5051
- Overpass API: http://localhost:12345/api/interpreter when started with `--profile geo`
- Nominatim: http://localhost:8081/reverse when started with `--profile geocoding`
- Airflow username: `airflow`
- Airflow password: `airflow`
- pgAdmin email: `admin@example.com`
- pgAdmin password: `admin`
- Postgres host: `localhost`
- Postgres port: `5433` by default, configurable via `POSTGRES_PORT`
- App database: `unternehmensdatenbank`

## Common commands

Start the full local stack in the safe order:

```bash
./scripts/dev-up.sh
```

Run pending migrations:

```bash
docker compose --profile tools run --rm flyway
```

Inspect Flyway migration state:

```bash
docker compose --profile tools run --rm flyway info
```

Open an Airflow CLI shell:

```bash
docker compose --profile debug run --rm airflow-cli bash
```

Open pgAdmin logs:

```bash
docker compose logs -f pgadmin
```

Start local Overpass for Berlin:

```bash
docker compose --profile geo up -d overpass
docker compose logs -f overpass
```

Start local Nominatim:

```bash
docker compose --profile geocoding up -d nominatim
docker compose logs -f nominatim
```

Stop the stack:

```bash
docker compose down
```

Remove containers and volumes:

```bash
docker compose down -v
```

## Documentation

- [Data model](docs/data-model.md)
- [Core queries](docs/core-queries.sql)
- [Geospatial services](docs/geospatial.md)
- [Berlin Gewerbedaten pipeline](docs/berlin-gewerbedaten-pipeline.md)
- [Source enrichment DAGs](docs/source-enrichment-dags.md)

## Notes

- pgAdmin is preconfigured with a local server entry pointing at the `postgres` container.
- Overpass and Nominatim are optional heavy services and are intentionally not part of the default `./scripts/dev-up.sh` startup path.
- Airflow now builds from a local Dockerfile so the Berlin Gewerbedaten DAG dependencies are installed inside the scheduler containers.
- The Berlin Gewerbedaten pipeline now streams to JSONL and loads Postgres in batches instead of holding the full dataset in memory.
- Name enrichment is disabled by default and public Overpass fallback is disabled by default; enable them explicitly when you have local Overpass available.
- The Berlin Gewerbedaten DAG is disabled by default. The active direction is source-specific enrichment via Mistral search discovery, Kununu, and LinkedIn DAGs.
- Active discovery providers in the repo are `mistral_search_discovery` and `claude_search_discovery`. Kununu and LinkedIn are treated as discovered URL fields, not separate runnable DAGs.
- Flyway is defined as a one-shot tool service. It does not run automatically with `docker compose up`.
- Flyway connection settings come from the same `.env` values as the rest of the stack, so local credentials only need to be changed in one place.
- The initial migration only creates a minimal `app.sources` table so the migration pipeline has a real baseline.
- Add future schema changes as new immutable SQL files under [db/migrations](db/migrations).
