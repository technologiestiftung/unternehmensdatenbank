# Releases: how the schema and the pipelines reach the Data Hub

The app and this repo share one database on the hub. This repo owns the **schema** (`db/migrations/`).
The hub does not run Flyway for us; it runs an **image** we build, before the app starts.

## What is a migration

| kind | example | goes where |
|---|---|---|
| schema | a new table or column | a migration |
| reference data the code needs | `sources` rows with their priority | a migration |
| a one-off data fix | deduplicating rows a bug wrote twice | a migration |
| business data | Fusionbase, Förderkatalog, enrichment, votes | **never**: the DAGs and the app write it |

The test: if an empty database needs it to work, it is a migration. If it changes every quarter, it
is a pipeline.

## The migration image

`db/Dockerfile`: Flyway 12 with `db/migrations/` inside. It runs `flyway migrate` and exits. Run it
as the app's **initContainer** on every start: a second run applies nothing, and Flyway's lock keeps
two pods from colliding. Configured by environment only:

| variable | value | secret? |
|---|---|---|
| `FLYWAY_URL` | `jdbc:postgresql://<host>:5432/<database>` | no |
| `FLYWAY_USER` / `FLYWAY_PASSWORD` | the database's **owner** login | password yes |
| `FLYWAY_PLACEHOLDERS_WRITER_ROLE` | the hub's name for the writer login | no |
| `FLYWAY_PLACEHOLDERS_READER_ROLE` | the hub's name for the reader login | no |

```yaml
initContainers:
  - name: migrations
    image: ghcr.io/technologiestiftung/unternehmensdatenbank-migrations:1.0.0
    envFrom:
      - secretRef:
          name: unternehmensdatenbank-flyway   # the variables above
```

**Test locally:** `scripts/test_migration_image.sh` runs the image twice against an empty Postgres 15
with role names that are not ours: the first run applies everything, the second nothing, and the
writer can insert where the reader cannot update. CI runs it too.

## The pipelines image

The hub's Airflow cannot install our packages (spaCy, bs4 ...). So every task runs as a
**KubernetesPodOperator** pod from `ghcr.io/technologiestiftung/unternehmensdatenbank-pipelines`
(`airflow/pipelines.Dockerfile`: the DAG packages and `airflow/requirements.txt`, no Airflow). A pod
runs one task; its exit code is the task's result, its log the task's log.

```
Airflow (the hub's)     reads our DAG files from git (the DAG bundle), keeps the schedules
  └─ task due → pod     python -m run_task website_discovery.main:run_discover
                        env from a Kubernetes secret: PG_DSN (writer) and DAG settings
```

`airflow/dags/pipeline_task.py` builds each task; Airflow **variables** decide how:

| variable | local | hub |
|---|---|---|
| `pipeline_runner` | unset: `PythonOperator` | `kubernetes` |
| `pipeline_image` | — | `…-pipelines:main` (staging), `…-pipelines:X.Y.Z` (production) |
| `pipeline_namespace` | — | from the hub team |
| `pipeline_secret` | — | the secret with `PG_DSN` and the DAG settings |

A third runner, `pipeline_runner = docker`, is for the play server, which emulates the hub with
docker compose and the official Airflow image: the same image and `run_task` argument, but as a
Docker container instead of a pod. It joins the network named by `pipeline_network` and gets the
worker's own `PG_DSN` and DAG settings instead of a secret (`airflow/dags/pipeline_task.py`).

A pod shares no disk, so state between runs belongs in Postgres. Not solved yet: `manager_ner_train`
keeps its spaCy models on disk (`MANAGER_NER_MODEL_DIR`) and needs S3 or a volume on the hub.

```bash
docker build -f airflow/pipelines.Dockerfile -t udb-pipelines airflow
docker run --rm -e PG_DSN="host=host.docker.internal port=5433 dbname=unternehmensdatenbank user=writer password=writer" \
  udb-pipelines pipeline_coordinator.main:run_pipeline_coordinator
```

## Staging and production

| what you do | workflow | image tags | environment |
|---|---|---|---|
| open a pull request | `ci.yml`: free checks, migration image test | none | — |
| merge to `main` | `ci.yml`, then both images | `:main`, `:sha-<commit>` | **staging** |
| **Actions → Create release** on `main` (patch / minor / major) | `release.yml`: checks, tag `vX.Y.Z`, GitHub Release, images after approval | `:X.Y.Z`, `:X.Y` | **production** |

No `:latest`, and tags are never overwritten: a hotfix is a new patch release.

- **Order matters.** An app release that reads a new column needs its migration first. Release this
  repo first and hand the hub both versions together.
- **Forward-only.** Rolling the app back does not roll the schema back, so a migration must keep the
  schema readable by the previous app: add a column, never rename or drop one in the same release.
- **One-time repo settings:** required reviewers on the `production` environment; protect `main`.

## Open

- **Hasura metadata** (`hasura/metadata/`): applied by the Hasura image on start locally. Whether the
  hub applies it, or we do with `hasura metadata apply`, is open.
