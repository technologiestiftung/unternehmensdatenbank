# Unternehmensdatenbank

A Postgres database of Berlin companies, filled from several sources, every value traceable to its
source. It runs on the Berlin Data Hub (CIVITAS/CORE); this repo holds the schema, the Airflow DAGs
and a local copy of the hub's services. The app that reads it:
[unternehmensdaten-app](https://github.com/technologiestiftung/unternehmensdaten-app).

**The data is example data:** 296,486 Berlin companies from OffeneRegister, a Handelsregister
snapshot from 2017–2019 (142,571 active then). The final source is Fusionbase, not delivered yet.
Which source fills which column: [docs/target-columns.md](docs/target-columns.md). The plan: issue #18.

## Stack

| | version | role |
|---|---|---|
| Postgres | 15 + PostGIS + pgvector | the data |
| Airflow | 3.2.0 | pipelines |
| Flyway | 12 | migrations, shipped as an image ([release](docs/release.md)) |
| Python | 3.12 | DAG code |
| Keycloak · Hasura · RustFS (S3) | 26.8 · 2.51 · 1.0 | the hub's services, profile `datahub` ([emulation](docs/datahub-emulation.md)) |

## How a value is stored

```
companies                             one row per company
  └─ company_source_records           the whole record a source delivered
       └─ company_field_observations  one row per field claim, with a confidence
            └─ company_canonical_field_sources   the winning observation per field
```

Sources only write observations. `pipeline_coordinator` picks the winner per field, **priority
first, then confidence**. The app reads `v_company_profile`. Details: [data-model](docs/data-model.md).

## Quickstart

```bash
cp .env.example .env
./scripts/dev-up.sh     # Fernet key, Postgres, migrations, Airflow
```

Airflow: http://localhost:8082 (airflow / airflow). To load the Berlin companies, see "Loading the
OffeneRegister Berlin set" in [data-model](docs/data-model.md).

```bash
docker compose --profile tools run --rm flyway          # pending migrations ( | flyway info )
docker compose build                                    # after Dockerfile or requirements changes
docker compose --profile debug run --rm airflow-cli bash
docker compose --profile datahub up -d                  # Keycloak, Hasura, S3
docker compose down                                     # add -v to destroy all data
```

## Access (local defaults)

| service | URL | login |
|---|---|---|
| Airflow | http://localhost:8082 | airflow / airflow |
| pgAdmin | http://localhost:5051 | admin@example.com / admin |
| Postgres | localhost:**5433** | app / app (owner), writer / writer (DAGs), reader / reader (Hasura) |
| Hasura | http://localhost:8090/console | admin secret `local-admin-secret` |
| Keycloak | http://localhost:8180/admin | admin / admin · app user dev / dev |
| S3 (RustFS) | http://localhost:9001 | localaccess / localsecret |

Database: `unternehmensdatenbank` (the Berlin data).

## DAGs, migrations, checks

- **DAGs:** [airflow/dags/README.md](airflow/dags/README.md). Each starts paused; unpause it
  in Airflow to run it.
- **Migrations:** `db/migrations/`, a `V1__baseline.sql` plus later steps ([data-model](docs/data-model.md),
  "Migration strategy"). Forward-only.
  Flyway runs as a one-shot service (`--profile tools`), not with `docker compose up`.
- **Checks:** `scripts/free_checks.sh` (ruff, unit tests, seven guards; no database, no network).
  CI, no-mistakes and the Claude hooks run the same script. See [guards-and-tests](docs/guards-and-tests.md).
