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

- Airflow UI/API: http://localhost:8080
- pgAdmin: http://localhost:5051
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

Stop the stack:

```bash
docker compose down
```

Remove containers and volumes:

```bash
docker compose down -v
```

## Documentation

- [docs/data-model.md](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/docs/data-model.md:1)
- [docs/core-queries.sql](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/docs/core-queries.sql:1)

## Notes

- pgAdmin is preconfigured with a local server entry pointing at the `postgres` container.
- Flyway is defined as a one-shot tool service. It does not run automatically with `docker compose up`.
- Flyway connection settings come from the same `.env` values as the rest of the stack, so local credentials only need to be changed in one place.
- The initial migration only creates a minimal `app.sources` table so the migration pipeline has a real baseline.
- Add future schema changes as new immutable SQL files under [db/migrations](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/db/migrations).
