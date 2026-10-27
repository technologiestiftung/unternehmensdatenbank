# Releases: how the schema reaches the Data Hub

The app (repo `unternehmensdaten-app`) and this repo share one database on the Berlin Data Hub.
This repo owns its **schema**: the Flyway migrations in `db/migrations/`. The hub does not run
Flyway for us. It runs an **image** we build, before the app starts.

## What a migration is, and what it is not

| kind | example | goes where |
|---|---|---|
| schema | `V23__extraction_feedback.sql` | a migration |
| reference data the code needs | `sources` rows with their priority | a migration |
| a one-off data fix | V17's dedup, V19's `company_managers` | a migration |
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

The migrations never name the writer or reader login; V21 grants to whatever these two say.

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

## Staging and production

The same flow as the app:

| what you do | workflow | image | environment |
|---|---|---|---|
| open a pull request | `ci.yml`: the free checks and the migration image test | none | none |
| merge to `main` | `ci.yml`: the same, then the image is built | `:main`, `:sha-<commit>` | **staging** |
| **Actions → Create release → Run workflow** on `main`, pick patch / minor / major | `release.yml`: checks again, tags `vX.Y.Z`, publishes a GitHub Release, builds the image after approval | `:X.Y.Z`, `:X.Y` | **production** |

Image: `ghcr.io/technologiestiftung/unternehmensdatenbank-migrations`. No `:latest`: with staging
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
   release. Without reviewers the image is built straight after the tag.
2. Settings → Branches → protect `main` (pull requests only).

## Not covered yet

- **Hasura metadata** (`hasura/metadata/`): locally the Hasura image applies it on start. Whether
  the hub applies it, or we do with `hasura metadata apply`, is open.
- **Airflow**: the DAGs could come straight from this repo (GitDagBundle, `airflow/dags`), but
  their Python packages (spaCy, rapidfuzz, pyproj, scipy) are not in the upstream image. Whether
  we get our own Airflow, and so our own image, is asked of the hub team. Until then no Airflow
  image is built here.
