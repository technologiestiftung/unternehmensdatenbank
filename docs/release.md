# Releases: how the schema and the pipelines reach the Data Hub

The app (repo `unternehmensdaten-app`) and this repo share one database on the Berlin Data Hub.
This repo owns its **schema**: the Flyway migrations in `db/migrations/`. The hub does not run
Flyway for us. It runs an **image** we build, before the app starts.

## What a migration is, and what it is not

| kind | example | goes where |
|---|---|---|
| schema | a new table or column, e.g. `V2__add_something.sql` | a migration |
| reference data the code needs | `sources` rows with their priority | a migration |
| a one-off data fix | deduplicating rows a bug wrote twice | a migration |
| business data | Fusionbase, Förderkatalog, enrichment, users' votes | **never** a migration: the DAGs and the app write it |

The test: if a fresh, empty database needs it to work, it is a migration. If it changes every
quarter, it is a pipeline. The Osnabrück example data never goes to the hub.

## The migration image

`db/Dockerfile`: Flyway 12 with `db/migrations/` copied in. It runs `flyway migrate` and exits.

- **Run it on every app start, as an initContainer.** Kubernetes starts the app only once it exits
  with 0. Flyway records what it applied, so a second run applies nothing, and it takes a lock,
  so two pods starting at once do not collide.
- **Configured by environment only:**

| variable | value | secret? |
|---|---|---|
| `FLYWAY_URL` | `jdbc:postgresql://<host>:5432/<database>` | no |
| `FLYWAY_USER` | the database's **owner** login | no |
| `FLYWAY_PASSWORD` | its password | **yes** |
| `FLYWAY_PLACEHOLDERS_WRITER_ROLE` | the hub's name for the writer login | no |
| `FLYWAY_PLACEHOLDERS_READER_ROLE` | the hub's name for the reader login | no |

The migrations never name the writer or reader login; the baseline grants to whatever these two say.

In the app's Deployment:

```yaml
initContainers:
  - name: migrations
    image: ghcr.io/technologiestiftung/unternehmensdatenbank-migrations:1.0.0
    envFrom:
      - secretRef:
          name: unternehmensdatenbank-flyway   # the five variables above
```

**Test it locally:** `scripts/test_migration_image.sh` builds the image and runs it twice against
an empty Postgres 15 set up like the hub's, with role names that are not ours. The first run must
apply every migration, the second none, and the writer must be able to insert where the reader
cannot update. Needs Docker only. CI runs the same script.

## The pipelines image

The hub's Airflow cannot install our packages (spaCy, psycopg2, bs4 ...), and it offers no custom
Airflow image. So every DAG task runs as a **KubernetesPodOperator** pod from our own image
`ghcr.io/technologiestiftung/unternehmensdatenbank-pipelines` (`airflow/pipelines.Dockerfile`): the
DAG packages and `airflow/requirements.txt`, no Airflow. A pod runs one task and exits; its exit code
is the task's result, its log is the task's log in the Airflow UI.

```
Airflow (the hub's)            reads our DAG files from git (the DAG bundle), keeps the schedules
  └─ task due → pod            image unternehmensdatenbank-pipelines:<tag>
       python -m run_task website_discovery.main:run_discover
       environment from the Kubernetes secret: PG_DSN (writer login), ENABLE_* flags
```

The DAG files are the same everywhere. `airflow/dags/pipeline_task.py` builds each task, and
Airflow **variables** decide how:

| variable | local | hub |
|---|---|---|
| `pipeline_runner` | unset: `PythonOperator`, as before | `kubernetes` |
| `pipeline_image` | — | `…-pipelines:main` on staging, `…-pipelines:X.Y.Z` on production |
| `pipeline_namespace` | — | from the hub team |
| `pipeline_secret` | — | the Kubernetes secret with `PG_DSN` and the `ENABLE_*` flags |

A third runner, `pipeline_runner = docker`, is for the play server, which emulates the hub with
docker compose and the official Airflow image: the same pipelines image and `run_task` argument, but
as a Docker container instead of a pod. It joins the network named by `pipeline_network` and gets
the worker's own `PG_DSN` and DAG settings instead of a secret. Details in
`airflow/dags/pipeline_task.py`.

Schedules (`@monthly`, cron strings) and the `TriggerDagRunOperator` chains stay in Airflow; only the
work moves into the pod. A pod shares no disk, so state between runs belongs in Postgres.

**Test it locally:**

```bash
docker build -f airflow/pipelines.Dockerfile -t udb-pipelines airflow
docker run --rm -e PG_DSN="host=host.docker.internal port=5433 dbname=unternehmensdatenbank user=writer password=writer" \
  udb-pipelines pipeline_coordinator.main:run_pipeline_coordinator
```

It is built and tagged together with the migration image (table below). Not solved yet: the spaCy
models of `manager_ner_train` live on disk (`MANAGER_NER_MODEL_DIR`) and need S3 or a volume on the
hub; until then a pod trains and forgets.

## Staging and production

The same flow as the app:

| what you do | workflow | image | environment |
|---|---|---|---|
| open a pull request | `ci.yml`: the free checks and the migration image test | none | none |
| merge to `main` | `ci.yml`: the same, then both images are built | `:main`, `:sha-<commit>` | **staging** |
| **Actions → Create release → Run workflow** on `main`, pick patch / minor / major | `release.yml`: checks again, tags `vX.Y.Z`, publishes a GitHub Release, builds both images after approval | `:X.Y.Z`, `:X.Y` | **production** |

Images: `ghcr.io/technologiestiftung/unternehmensdatenbank-migrations` and `…-pipelines`. No `:latest`: with staging
and production on one platform it would name neither. Tags are never overwritten, so a hotfix
is a new patch release from `main`.

**Order matters.** A release of the app that reads a new column needs the migration that adds
it. Release this repo first and give the hub both versions together, or the app starts against
the old schema.

**Migrations are forward-only.** Rolling the app back does not roll the schema back. A migration
must therefore leave the schema readable by the app version before it: add a column, never
rename or drop one in the same release that stops using it.

### One-time repo settings

1. Settings → Environments → `production` → required reviewers: whoever approves a production
   release. Without reviewers the images are built straight after the tag.
2. Settings → Branches → protect `main` (pull requests only).

## Not covered yet

- **Hasura metadata** (`hasura/metadata/`): locally the Hasura image applies it on start. Whether
  the hub applies it, or we do with `hasura metadata apply`, is open.
