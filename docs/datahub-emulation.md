# Running the Berlin Data Hub locally

The production home of this project is the **Berlin Data Hub** (data-hub.berlin): a Kubernetes
platform built on CIVITAS/CORE and run by the Technologiestiftung with a service provider. We do not
deploy there ourselves. We hand over DAGs, migrations and a Docker image for the app, and the hub
team runs them.

So that "works on my machine" means "works on the hub", this repo runs the same pieces locally.

## What the hub runs, and what stands in for it here

| On the hub | Locally | Where |
|---|---|---|
| Postgres **15** with PostGIS and pgvector | the same, built from `postgres:15` | `postgres/Dockerfile` |
| one database per environment | `unternehmensdatenbank` (prod) and `unternehmensdatenbank_staging` | `postgres/init/01-create-databases.sh` |
| three logins per database: owner, writer, reader | `app`, `writer`, `reader` | same file; grants at the end of the baseline migration |
| Airflow (add-on, default 3.3.2) | Airflow 3.2.0, as before | `docker-compose.yml` |
| Keycloak 26 (login, realm `tsb-prd`) | Keycloak 26.8.1, realm `datahub` | `keycloak/datahub-realm.json` |
| Hasura v2 (GraphQL over Postgres) | Hasura v2.51.3 | `hasura/metadata/` |
| MinIO (S3) | RustFS — MinIO no longer publishes images; same S3 API | `s3` service |
| APISIX gateway | **not emulated** — the app talks to Hasura directly | — |

**How sure we are about the hub column.** Postgres 15 is the CIVITAS/CORE default
(`pg_version: "15"`) and the Airflow version is the default of its Airflow add-on, both read on
2026-10-23. The three-login scheme, Hasura and the Keycloak version come from a planning document,
not from the hub team. The hub's own wiki (wiki.data-hub.berlin) needs a login and was not read.
**Ask the hub team** before relying on any of them: datahub@ts.berlin.

## Start it

```bash
cp .env.example .env                                   # first time only
./scripts/dev-up.sh                                    # Postgres, both migrations, Airflow
docker compose --profile datahub up -d                 # Keycloak, Hasura, S3
```

| | URL | login |
|---|---|---|
| Hasura console | http://localhost:8090/console | admin secret `local-admin-secret` |
| Hasura GraphQL | http://localhost:8090/v1/graphql | a Keycloak token |
| Keycloak admin | http://localhost:8180/admin | `admin` / `admin` |
| App user | — | `dev` / `dev`, realm `datahub` |
| S3 API / console | http://localhost:9000 · http://localhost:9001 | `localaccess` / `localsecret` |

All of these passwords are local-only defaults. The hub issues its own.

## The three database roles

| role | used by | may |
|---|---|---|
| `app` (owner) | Flyway, the host scripts in `scripts/` | everything, including DDL |
| `writer` | every Airflow DAG (`PG_DSN` in docker-compose.yml) | read and write rows, TEMP tables; **no DDL** |
| `reader` | Hasura, so the app | SELECT everything; INSERT into `company_comments` only |

Checked 2026-10-27: `writer` gets `permission denied for schema public` on `CREATE TABLE`, and
`reader` gets `permission denied for table companies` on `UPDATE`.

The role **names** reach the migrations as Flyway placeholders (`${writer_role}`,
`${reader_role}`), so the hub can use its own naming without a new migration.

## Staging and prod

Both databases get every migration. Migrate staging first:

```bash
APP_DB_NAME=unternehmensdatenbank_staging docker compose --profile tools run --rm flyway
docker compose --profile tools run --rm flyway         # prod
```

Airflow and Hasura point at prod by default. To point them at staging, set
`APP_DB_NAME=unternehmensdatenbank_staging` in `.env` and recreate the containers.

## Hasura

`hasura/metadata/` is applied on every Hasura start (the `cli-migrations-v3` image), so the GraphQL
API is defined in git. Five objects are exposed, all to the role `user`:

- `companies` — `v_company_profile`, read-only, with the relations `comments`, `fundings`,
  `feedback` and `provenance`
- `comments` — `company_comments`: select all; insert with `company_id` and `body` only. Hasura
  sets `author_id` from the token's `sub`. A client that sends `author_id` gets a validation error.
  There is no update and no delete.
- `fundings` — `v_company_fundings`, read-only
- `feedback` — `extraction_feedback`: votes and corrections on any shown field; a user may change
  or delete only their own
- `provenance` — `v_field_provenance`, read-only: where each shown value came from

Requests without a token are refused. Tokens come from Keycloak. Every realm role becomes a Hasura
role, and the default role is `user`.

Try it from a shell:

```bash
TOKEN=$(curl -s -d grant_type=password -d client_id=unternehmensdaten-app \
  -d client_secret=local-dev-secret -d username=dev -d password=dev \
  http://localhost:8180/realms/datahub/protocol/openid-connect/token | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" \
  -d '{"query":"{ companies(limit: 3) { name city funding_count } }"}' \
  http://localhost:8090/v1/graphql
```

## Moving an existing Postgres 16 volume to 15

The old stack ran Postgres 16 on the volume `postgres-data`. Version 15 cannot open a 16 data
directory, so the new stack uses `postgres15-data`. **The old volume is left alone.** To carry its
data over:

```bash
docker run --rm -v unternehmensdatenbank_postgres-data:/var/lib/postgresql/data -d --name pg16 postgres:16
docker exec pg16 pg_dump -U postgres unternehmensdatenbank > /tmp/udb.sql   # plain SQL; 15 cannot read 16's -Fc
docker stop pg16
(echo 'SET ROLE app;'; cat /tmp/udb.sql) | docker compose exec -T postgres psql -q -U postgres -d unternehmensdatenbank
docker compose --profile tools run --rm flyway
```

The Airflow metadata database (DAG run history) is not carried over, and `airflow-init` creates a
fresh one. Once the copy is checked, `docker volume rm unternehmensdatenbank_postgres-data` frees
the space.

## Known differences from the hub

- **DAG dependencies.** The hub's own DAGs (gitlab.com/tsb-udp/airflow-dags) install their packages
  per task with `@task.virtualenv(requirements=...)`. Ours rely on the packages baked into
  `airflow/Dockerfile`. On the hub that means asking for our packages in the shared image, or moving
  to `@task.virtualenv`.
- **Executor.** The hub's Airflow runs CeleryExecutor, so tasks run on different workers with no
  shared disk. The checkpoint files under `airflow/data/` will not survive there. Locally it is
  LocalExecutor.
- **DAG prefix.** The hub's Airflow add-on can restrict DAG visibility by DAG-id prefix per team.
  Whether we need one is an open question for the hub team.
