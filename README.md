# Unternehmensdatenbank

A Postgres database of Berlin companies, filled from several sources, with every value traceable to
the source that delivered it. Runs on the Berlin Data Hub (CIVITAS/CORE); this repo holds the schema,
the Airflow DAGs and a local copy of the hub's services. The app that reads it is
[unternehmensdaten-app](https://github.com/technologiestiftung/unternehmensdaten-app).

**The data is example data.** Today: **296,486 companies registered in Berlin** from OffeneRegister,
a snapshot of the Handelsregister taken 2017–2019 (142,571 were active then). The final source is
the vendor Fusionbase, which has not delivered yet. Which of the 20 target columns each source
covers: [docs/target-columns.md](docs/target-columns.md). Where this is going: issue #18.

## Stack

| | version | role |
|---|---|---|
| Postgres | 15 + PostGIS + pgvector | the data, as on the Data Hub |
| Airflow | 3.2.0 | pipelines |
| Flyway | 12 | schema migrations, shipped to the hub as an image ([docs/release.md](docs/release.md)) |
| Python | 3.12 | DAG code |
| Keycloak · Hasura · RustFS (S3) | 26.8 · 2.51 · 1.0 | the hub's services, profile `datahub` ([docs/datahub-emulation.md](docs/datahub-emulation.md)) |

## How a value is stored

```
companies                             one row per company: identity and register facts
  └─ company_source_records           the whole record a source delivered, per company and source
       └─ company_field_observations  one row per field claim, with a confidence
            └─ company_canonical_field_sources   the winning observation per field
```

No source overwrites another. Every source writes observations; `pipeline_coordinator` picks the
winner per field, **source priority first, then confidence**. The app reads the result through the
view `v_company_profile`. Details: [docs/data-model.md](docs/data-model.md).

## Quickstart

```bash
cp .env.example .env
./scripts/dev-up.sh
```

`dev-up.sh` writes a fresh Airflow Fernet key into `.env` if the value there is empty, starts
Postgres, runs the migrations (staging first, then prod), and starts Airflow. Then open
http://localhost:8082 (airflow / airflow).

To load the Berlin companies, follow "Loading the OffeneRegister Berlin set" in
[docs/data-model.md](docs/data-model.md).

## Common commands

```bash
docker compose --profile tools run --rm flyway          # pending migrations ( | flyway info )
docker compose build                                    # after Dockerfile or requirements changes
docker compose --profile debug run --rm airflow-cli bash
docker compose --profile datahub up -d                  # Keycloak, Hasura, S3
docker compose --profile geo up -d overpass             # optional: local Overpass for Berlin
docker compose down                                     # add -v to destroy all data
```

## Access

| service | URL | login |
|---|---|---|
| Airflow | http://localhost:8082 | airflow / airflow |
| pgAdmin | http://localhost:5051 | admin@example.com / admin |
| Postgres | localhost:**5433** | app / app (owner), writer / writer (DAGs), reader / reader (Hasura). Databases: `unternehmensdatenbank` (the Berlin example data), `unternehmensdatenbank_staging` (empty) |
| Hasura | http://localhost:8090/console | admin secret `local-admin-secret` |
| Keycloak | http://localhost:8180/admin | admin / admin · app user dev / dev |
| S3 (RustFS) | http://localhost:9001 | localaccess / localsecret |
| Overpass (optional) | http://localhost:12345/api/interpreter | — |

All are local defaults; the hub issues its own.

## DAGs

[airflow/dags/README.md](airflow/dags/README.md). A DAG needs **two** switches: its `ENABLE_*` flag
in `.env` *and* unpausing it in Airflow. The flag is read when the task runs, so a disabled DAG
still shows up and fails with a clear message instead of hiding.

## Migrations

`db/migrations/`: `V1__baseline.sql` is the whole schema as of 2026-10-28 (the earlier history squashed
before anything was deployed, details in [data-model](docs/data-model.md#migration-strategy)). Forward-only from
here. Flyway runs as a one-shot service (`--profile tools`), not with `docker compose up`.

## Checks

```bash
scripts/free_checks.sh    # ruff, the unit tests, the seven rule guards; no database, no network
```

The same script runs in CI (`.github/workflows/checks.yml`), in no-mistakes, and in the Claude
hooks. What each guard defends: [docs/guards-and-tests.md](docs/guards-and-tests.md).
