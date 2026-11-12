# Releases: how the schema and the pipelines reach the Data Hub

The app and this repo share one database on the hub. This repo owns the **schema** (`db/migrations/`).
The hub does not run Flyway for us; it runs an **image** we build, before the app starts.

## What is a migration

| kind | example | goes where |
|---|---|---|
| schema | a new table or column | a migration |
| reference data the code needs | Technologiefelder, their keywords, WZ divisions | a migration |
| a source | its `sources` row, priority and text | **never**: its DAG writes them with `ensure_source` (`priority`, `SourceDetails`) on every run |
| a one-off data fix | deduplicating rows a bug wrote twice | a migration |
| business data | OffeneRegister, Förderkatalog, enrichment, votes | **never**: the DAGs and the app write it |

The test: if an empty database needs it to work and no DAG writes it, it is a migration. If it
changes every quarter, it is a pipeline.

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
writer can insert where the reader cannot update. It also checks the taxonomy tables (#84): 77 fields,
544 active and 27 removed keywords, 87 WZ divisions in 22 sections, and that the writer cannot insert
a keyword. CI runs it too.

## The pipelines image

The hub's Airflow cannot install our packages (spaCy, bs4 ...). So every task runs as a
**KubernetesPodOperator** pod from `ghcr.io/technologiestiftung/unternehmensdatenbank-pipelines`
(`airflow/pipelines.Dockerfile`: the DAG packages, `airflow/requirements.txt` and the local classifier's `airflow/requirements-model.txt` with CPU-only PyTorch and its baked-in weights, no Airflow). A pod
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
worker's own `PG_DSN` and DAG settings instead of a secret (`airflow/dags/pipeline_task.py`). Every
task pulls the image first, so a merge to main reaches play without a manual `docker pull`.
Airflow reaches Docker through a proxy that cuts a connection quiet for 10 minutes, so `run_task` logs
"still running" every 5 minutes: before that, a training run that printed nothing for 20 minutes was
failed while its container kept running (a 409 on removal, 2026-11-12).

A pod shares no disk, so state between runs belongs in Postgres, and files a later run needs in S3:
the company corpus (#61) and the trained models (`enrichment.model_store`, under `models/<name>/` in the
corpus bucket). `manager_ner_train` publishes every run's metrics there and a promoted model with its
`current.json`; `manager_ner_extract` downloads the current version into `MANAGER_NER_MODEL_DIR`, which
is only a working copy. The hub needs the bucket and the `CORPUS_S3_*` settings for both.

```bash
docker build -f airflow/pipelines.Dockerfile -t udb-pipelines airflow
docker run --rm -e PG_DSN="host=host.docker.internal port=5433 dbname=unternehmensdatenbank user=writer password=writer" \
  udb-pipelines pipeline_coordinator.main:run_pipeline_coordinator
```

### S3 on the hub (#85)

- **The settings reach a pod only through `pipeline_secret`.** A pod gets that secret as its
  environment, not the worker's settings. Put these keys next to `PG_DSN`:

  | key | value |
  |---|---|
  | `CORPUS_S3_ENDPOINT` | `host:port`, without `https://` |
  | `CORPUS_S3_ACCESS_KEY`, `CORPUS_S3_SECRET_KEY` | the key for the bucket |
  | `CORPUS_S3_BUCKET` | default `company-corpus` |
  | `CORPUS_S3_SECURE` | `true` when the endpoint uses TLS |

- **Create the bucket before the first run.** The code creates it only when it is missing, which a key
  scoped to one bucket usually may not do. The key needs, on that bucket only, the policy actions
  `s3:ListBucket` (the check that the bucket exists), `s3:GetObject` (also covers HEAD on objects) and
  `s3:PutObject`. The code never lists objects and never deletes.
- **Private, no public read.** The bucket holds Impressum pages in full, with names, emails and
  phone numbers. A privacy review comes before any of it leaves local development (#18).
- **Check it right after deploying.** In a pod or container of the pipelines image, from the DAGs
  folder: `python -m enrichment.corpus`. It writes `healthcheck/ping` and reads it back; it prints
  `company corpus: OK`, or the S3 error (`SignatureDoesNotMatch`, `AccessDenied`, ...) and exits 1.
- **A wrong key is logged, once.** A store that fails behaves like an empty one, so a DAG does not
  stop over its cache. Each S3 error is logged once per task run as a warning; a wrong key, a missing
  permission or a missing bucket also switches the store off for the rest of the run. Model reads and
  writes (`model_store`) raise instead, so the model DAGs fail.

### When a task fails

A task fails as a pod (or container) whose run function raised. Before it exits, `run_task` logs
one line:

```
pipeline_task_failed target=foerderkatalog.main:run_foerderkatalog last_try=true error=HTTPError: 503 Server Error …
```

Only the error's type and the first 200 characters of its message's first line, never a page's text.
`last_try=false` means Airflow will run the task again (twice, 10 minutes apart); `true` means no
retry follows. `pipeline_task` hands it to the pod as the environment variable `PIPELINE_LAST_TRY`.

**Play:** Alloy ships every container's log to Loki, the task containers too. An alert in Grafana
(folder `host-server-alerts`, contact point `tsb-slack-ds-infra-alerts`) on this query sends a Slack
message to #ds-infra-alerts:

```
sum by (target) (count_over_time({source="<play's HOST_HOSTNAME>"} |= "pipeline_task_failed" |= "last_try=true" | regexp `target=(?P<target>\S+)` [15m]))
```

with the condition "above 0". The rule is created by hand in the Grafana UI, like the other rules
in `ds-root-server/grafana/alerts/`.

**Hub:** open. Which monitoring reads the pods' logs there is a question for the hub team.

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

## Keycloak roles

The realm needs only the role `user` (everyone). Every logged-in person may also start, rename and delete
review rounds on the app's Prüfen page (#190): a separate role for that is not set up yet (decided
2026-11-11). **Never name a role `admin`:** Hasura treats a role of that name as its superuser and checks
no permission for it.

## Open

- **S3 on the hub** (#85), questions for the hub team (datahub@ts.berlin): who creates the bucket and
  the key? Is the store MinIO, at which endpoint, with TLS? Is there encryption at rest, a backup, a
  retention or deletion policy? Can pods reach it from our namespace?

- **Hasura metadata** (`hasura/metadata/`): applied by the Hasura image on start locally. Whether the
  hub applies it, or we do with `hasura metadata apply`, is open.
