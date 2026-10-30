# CLAUDE.md

This file is a map: one line per rule that cost somebody real time, plus a link to the detail.

- **Target design and next PRs: issue #18.** Read it before changing the schema, adding a source, or
  building a DAG.
- **The app** is `technologiestiftung/unternehmensdaten-app` (Next.js, reads through Hasura with the
  user's Keycloak token). This repo owns what it reads: migrations, `hasura/metadata/`,
  `keycloak/datahub-realm.json`.
- **Docs:** [docs/README.md](docs/README.md). The module docstrings are the real documentation;
  `airflow/dags/enrichment/__init__.py` maps the library.

Every `.md` is written for a smart 18-year-old who has never seen this project, German registers or
Airflow: explain before naming, short sentences, **numbers not adjectives**, and keep the caveat (if
a number came from 1,000 companies, say so).

## What this is

A Postgres database of **Berlin** companies from several sources, every value traceable. Airflow
orchestrates, Flyway owns all schema, Python writes with psycopg2. It runs on the Berlin Data Hub
(CIVITAS/CORE); this repo emulates the hub locally ([datahub-emulation](docs/datahub-emulation.md)).

**The data is example data:** 296,486 Berlin companies from OffeneRegister (a 2017–2019 snapshot;
142,571 active then). The final source is Fusionbase (not delivered yet). The numbers in the rule
docs were measured on the removed Osnabrück data; the rules still apply.

Postgres 15 + PostGIS (port **5433**) · Airflow 3.2.0 (Airflow 3 API: `airflow.providers.standard`,
not `airflow.operators`) · Flyway 12.

## Commands and access

```bash
./scripts/dev-up.sh                                 # full stack, correct order
docker compose --profile tools run --rm flyway      # pending migrations ( | flyway info )
docker compose build                                # after Dockerfile/requirements changes
scripts/free_checks.sh                              # every free check, as CI runs them
```

- Airflow UI **http://localhost:8082** (`airflow`/`airflow`). **Not 8080**: another Airflow on the
  developer's machine owns it.
- Postgres `localhost:5433`, `app`/`app`. Databases `unternehmensdatenbank` (the data) and
  `unternehmensdatenbank_staging` (empty); Airflow and Hasura use `APP_DB_NAME` from `.env`.
  **Port 5432 is a different Postgres.**
- Profiles: `debug`, `datahub` (Keycloak, Hasura, S3).
- **Three DB roles, as on the hub:** `app` owns the schema (Flyway, host scripts), DAGs run as
  `writer` (rows only, no DDL), Hasura as `reader`. A DAG that needs DDL is wrong, not the grant.
- `cp .env.example .env`; `dev-up.sh` fills in a Fernet key. Every DAG starts **paused** (Airflow's
  default); unpausing it in Airflow is the only switch.

## Architecture

`sources` → `company_source_records` (the whole record) → `company_field_observations` (one row per
field claim, with a confidence) → `pipeline_coordinator` → `company_canonical_field_sources` → views
(`v_companies_enriched`, `v_company_profile`). Detail: [data-model](docs/data-model.md).

- **`pipeline_coordinator` alone decides which observation wins.** Sources write observations, then
  trigger it.
- **Priority sorts before confidence, so it is absolute:** nothing at priority 100 beats anything at
  50. Within one priority, confidence decides, **weighed by age** (`resolver.HALF_LIVES`, #49): the
  age runs from when the source read the value (`source_updated_at`), never from when we stored it.
- **`website_url` is a view column**, not a table column; enriched fields live in
  `company_canonical_field_sources`, read through `v_companies_enriched`.
- **`companies` is identity only (V3).** Name, address, status and the other register facts are
  observations too (`resolver.REGISTER_FIELDS`), shown by `v_companies_enriched` under the old column
  names. Never add a value column to `companies`: nothing could compete with it.
- **Company identity is a random UUID, never the name** (32,170 Berlin companies have a former name).
  Matching: identifiers first, fuzzy second; `match_method`/`match_confidence` on the source record.
  Merging never deletes (`companies.merged_into`).
- **The history is append-only** (`company_field_history`, #54): written only by
  `record_company_history()` in the coordinator, never updated or deleted. `v_company_claims` is an
  allow-list: a new field the app shows needs its name there, and email, phone and `_withheld` never.
- **`company_source_records` is not exposed through Hasura.** OffeneRegister's full record (officers,
  raw address) sits there.

## DAGs

[airflow/dags/README.md](airflow/dags/README.md).

- **`email` and `phone` are stored, never resolved.** A resolved value reaches the app and
  `v_field_provenance`.
- **A correction is a source, shown only when people agree.** `feedback_observations` writes
  `user_feedback` (priority 50): one person 0.30, never shown; two 0.60. A field no other source
  fills takes the first correction (0.60). Counts only, never who.
- **A register announcement is a hint, never the status.** `register_announcement` stands next to
  `register_status`. The DAG reads only the result list, never a detail page.
- **A keyword is never a person's name:** `activity_keywords.is_candidate` drops anything
  `imprint_extractor.looks_like_person_name` accepts.
- **Import DAG modules by package name** (`from enrichment.confidence import …`). A bare import
  after a `sys.path` insert once handed one package another's module.
- **On the hub, tasks are KubernetesPodOperator pods** from our pipelines image, with no shared disk:
  state belongs in Postgres (`pipeline_state`), never in `airflow/data/`. Build a task with
  `pipeline_task(task_id, run_function)`, never a bare `PythonOperator` ([release](docs/release.md)).

## Website discovery rules (`enrichment/`)

Read [website-verification](docs/website-verification.md) before touching `confidence.py`,
`own_site.py`, or anything that reads a page.

- **A register number on a page proves the page is *about* a company, not that it *is* its site.**
  `own_site.is_own_site_candidate` is the single predicate; a looser one elsewhere is the recurring bug.
- **"A real business site" is not "THAT company's site."** Impressum, VAT number and own-domain email
  may add score, never decide. Only `confidence.identifies_company` does: own register number, a
  Geschäftsführer who is a register officer, or name AND postcode. The Impressum alone once promoted
  190 wrong websites.
- **Never grow a list of bad domains or words.** Genericness is measured
  (`name_frequency.common_words`, `common_email_domains`).
- **`enrichment/observations.py` is the write path for discovery.** `sorted()` the `evidence` set
  before `Json()`.
- Search-engine discovery was measured and removed; a new search source must feed the same
  verification and `confidence.py`, as `wikidata` does (`pipeline.verify_proposed`). The OpenStreetMap step is off (`WEBSITE_DISCOVERY_OSM=false`).

## Industry classification: THREE columns, never mixed

| | Technologiefeld (`technology_field`) | WZ cluster | purpose_field |
|---|---|---|---|
| question | what field does it **work in**? | what is it **registered as**? | what was it **set up to do**? |
| from | words the company published | its official WZ code | the register's business purpose |
| values | one of Berlin's 77 in English (`name_en`), or `NONE` | 76 divisions in 20 sections | one of the same 77, or `NONE` |

Details: [branch-classification](docs/branch-classification.md),
[purpose-classification](docs/purpose-classification.md). OffeneRegister has no WZ code and no
purpose, so for Berlin only the Technologiefeld can be filled today.

1. **A WZ code may never fill the Technologiefeld.** `classify()` has no WZ parameter; a guard checks it.
2. **A Technologiefeld requires a downloaded website**, and the gate is Postgres, not a file.
3. **`NONE` has four reasons kept apart:** `no_website`, `torn`, `no_keyword`, `no_text`.
4. **Never match a keyword inside a German compound.** `BRANCH_HYPHEN_IS_BOUNDARY` is **off**, measured.
5. **A keyword wrong for its field is deleted from the YAML with the count in a comment**, never a
   stop-list in code.
6. **Keywords decide; the spaCy classifier only fills gaps**, promoted only when it beats the
   keywords on companies people voted on.
7. **No row of any column is hand-checked yet.** Agreement is not correctness.

## Migrations

`V1__baseline.sql` is the whole schema as of 2026-10-28 (squashed while nothing was deployed), plus
`V2__source_overview.sql`, `V3__register_fields_as_observations.sql`, `V4__user_feedback_source.sql`, `V5__profile_cleanup_tags_english.sql` and `V6__source_dates.sql`. A database migrated before the squash needs its Flyway history reset once
([data-model](docs/data-model.md), "Migration strategy"). **Forward-only:** never edit a migration
that ran anywhere shared. Flyway runs under `--profile tools`, not with `docker compose up`; on the
hub as the app's initContainer ([release](docs/release.md)).

- **The company profile is stored.** `v_company_profile` reads `company_profile_mv`; anything that
  changes companies or published values ends with `refresh_company_profile()` (the coordinator does).
  A filter on the live view took up to 45 s.
- **A new profile column means rebuilding `company_profile_mv`:** it froze `p.*`, so
  `CREATE OR REPLACE` on `v_company_profile_live` alone never reaches the app.
- **Grant links are a table**, refreshed by `refresh_company_funding_links()`. Linking on read made
  the company list take 71 s.
- **The sources page reads stored counts** (`V2`), refreshed by `refresh_company_profile()`. Counts
  only, never a value.
- **A new shown field needs its key in feedback and `v_field_provenance`.**
- **The observation upsert key is a partial unique index (`uq_obs_company_field_source_value`), and
  `insert_text_observation` repeats its predicate verbatim in `ON CONFLICT (...) WHERE`.** Widen one,
  widen the other, same commit.

## Checks

`scripts/free_checks.sh`: ruff, unit tests, seven guards. No database, network or model. CI,
no-mistakes and both Claude hooks run it ([guards-and-tests](docs/guards-and-tests.md)).

1. **Import the rule a guard checks, never restate it.**
2. **Assert on the syntax tree, not the source text**, when the forbidden thing is discussed in a
   docstring (a plain `in` test found `Handel` inside `Handelsregister`).

**Shipping:** with `no-mistakes` installed, validate and open the PR with `/no-mistakes`; otherwise
the normal branch + PR flow.

**Git Bash:** `docker compose exec -e PYTHONPATH=/opt/airflow/dags` breaks (path rewritten to
`C:/Program Files/Git/...`). Use PowerShell or `MSYS_NO_PATHCONV=1`.
