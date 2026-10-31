# Running the Berlin Data Hub locally

The project's home is the **Berlin Data Hub** (data-hub.berlin), a Kubernetes platform on CIVITAS/CORE.
We do not deploy there ourselves: we hand over DAGs, migrations and images, and the hub team runs
them. This repo runs the same pieces locally, so "works here" means "works on the hub".

| On the hub | Locally | Where |
|---|---|---|
| Postgres **15** with PostGIS and pgvector | the same, from `postgres:15` | `postgres/Dockerfile` |
| one database per environment | `unternehmensdatenbank` (one environment locally) | `postgres/init/01-create-databases.sh` |
| owner, writer, reader login per database | `app`, `writer`, `reader` | same file; grants in the baseline |
| Airflow (add-on, default 3.3.2) | Airflow 3.3.2 | `docker-compose.yml` |
| Keycloak 26 (realm `tsb-prd`) | Keycloak 26.8.1, realm `datahub` | `keycloak/datahub-realm.json` |
| Hasura v2 | Hasura v2.51.3 | `hasura/metadata/` |
| MinIO (S3) | RustFS, same S3 API | `s3` service |
| APISIX gateway | not emulated; the app talks to Hasura directly | — |

Postgres 15 and the Airflow version are CIVITAS/CORE defaults (read 2026-10-23). The three logins,
Hasura and the Keycloak version come from a planning document, not the hub team. Ask before relying
on them: datahub@ts.berlin.

## Start it

```bash
./scripts/dev-up.sh                        # Postgres, both migrations, Airflow
docker compose --profile datahub up -d     # Keycloak, Hasura
docker compose --profile local-s3 up -d    # S3 (RustFS); leave it off where another S3 is set
```

| | URL | login |
|---|---|---|
| Hasura console / GraphQL | http://localhost:8090/console · /v1/graphql | admin secret `local-admin-secret` · a Keycloak token |
| Keycloak admin | http://localhost:8180/admin | `admin` / `admin` |
| App user | — | `dev` / `dev`, realm `datahub` |
| S3 API / console | http://localhost:9000 · :9001 | `localaccess` / `localsecret` |

## The three database roles

| role | used by | may |
|---|---|---|
| `app` (owner) | Flyway, the host scripts | everything, including DDL |
| `writer` | every DAG (`PG_DSN`) | read and write rows, TEMP tables; **no DDL** |
| `reader` | Hasura, so the app | SELECT; INSERT on comments, INSERT/UPDATE on votes |

The role **names** reach the migrations as Flyway placeholders (`${writer_role}`, `${reader_role}`),
so the hub can name them its own way.

**After a migration or a change in `hasura/metadata`, restart Hasura:** `docker compose restart hasura`.
Hasura reads the columns of every table and view once and keeps them, and the `cli-migrations` image
applies `hasura/metadata` only when it starts. Two failures of the app on 2026-10-30 came from this:
the company list failed with `database query error` (`column ... cluster does not exist`, Hasura still
offered the columns V5 dropped), and the company page with `field 'history' not found` (the
relationship from #59 was in git, not in the running Hasura). A `reload_metadata` call fixes the
first only: it re-reads the database, not the files.

## Hasura

`hasura/metadata/` is applied on every Hasura start, so the GraphQL API lives in git. Exposed to the
role `user` (every Keycloak realm role becomes a Hasura role; requests without a token are refused):

- `companies`: `v_company_profile`, read-only, with `comments`, `fundings`, `feedback`, `provenance`
- `comments`: `company_comments`; insert `company_id` and `body` only, Hasura sets `author_id`
  from the token. No update, no delete.
- `fundings`: `v_company_fundings`, read-only
- `feedback`: `extraction_feedback`, votes and corrections; a user changes only their own
- `provenance`: `v_field_provenance`, where each shown value came from
- `source_overview` / `source_fields`: counts per source, for the app's `/sources` page

```bash
TOKEN=$(curl -s -d grant_type=password -d client_id=unternehmensdaten-app \
  -d client_secret=local-dev-secret -d username=dev -d password=dev \
  http://localhost:8180/realms/datahub/protocol/openid-connect/token | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" \
  -d '{"query":"{ companies(limit: 3) { name city funding_count } }"}' http://localhost:8090/v1/graphql
```

## Known differences from the hub

- **Packages.** Locally tasks run in Airflow's own Python with the packages from `airflow/Dockerfile`;
  on the hub as pods from our pipelines image ([release](release.md)).
- **Executor.** The hub runs CeleryExecutor (no shared disk between workers); locally LocalExecutor.
  Files under `airflow/data/` will not survive there.
- **DAG prefix.** The hub can restrict DAG visibility by DAG-id prefix per team. Open question.

## The play server

A second emulation runs on TSB's play server with docker compose: `technologiestiftung/ds-root-server`,
branch `play`, folders `udb-postgres`, `udb-keycloak`, `udb-hasura`, `udb-airflow`, `udb-app`. It
uses the official images only: Airflow pulls the DAGs from this repo with a `GitDagBundle`, and each
task runs as a container from the pipelines image (`pipeline_runner` `docker`, see
[release](release.md)).
