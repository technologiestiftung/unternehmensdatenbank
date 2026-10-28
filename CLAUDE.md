# CLAUDE.md

Guidance for Claude Code working in this repository.

## Where the detail lives

This file is a map. It holds only what has cost somebody real time when they got it wrong; detail
goes to the repo docs and this file keeps the one-line rule plus the link.

- **The target design and the order of the next PRs: issue #18.** Read it before changing the schema,
  adding a source, or building a DAG.
- **The app** lives in its own repo, `technologiestiftung/unternehmensdaten-app` (Next.js, reads through
  Hasura with the user's Keycloak token). This repo owns what it reads: migrations, `hasura/metadata/`,
  `keycloak/datahub-realm.json`.
- **Docs:** [docs/README.md](docs/README.md) is the index. The module docstrings are long on purpose
  and are the real documentation; `airflow/dags/enrichment/__init__.py` maps the library.

Every `.md` here is written for a smart 18-year-old who has never seen this project, German company
registers, or Airflow: explain a thing before naming it, short sentences, **numbers not adjectives**,
and keep the caveat — if a number came from 1,000 companies, say so.

## What this is

A Postgres database of **Berlin** companies, filled from several sources, every value traceable.
Airflow orchestrates, Flyway owns all schema, Python writes with psycopg2. It runs on the Berlin Data
Hub (CIVITAS/CORE); this repo emulates the hub locally ([datahub-emulation](docs/datahub-emulation.md)).

**The data is example data.** `unternehmensdatenbank` holds 296,486 companies registered in
Berlin from OffeneRegister (a Handelsregister snapshot from 2017–2019; 142,571 active then). The final
source is Fusionbase (not delivered yet). The Osnabrück example data was removed on 2026-10-28; the
rules in the docs still apply, the numbers measured on it do not.

Postgres 15 + PostGIS (port **5433**) · Airflow 3.2.0 (Airflow 3 API — `airflow.providers.standard`,
not `airflow.operators`) · Flyway 12.

## Commands and access

```bash
./scripts/dev-up.sh                                 # full stack, correct order
docker compose --profile tools run --rm flyway      # pending migrations ( | flyway info )
docker compose build                                # after Dockerfile/requirements changes
scripts/free_checks.sh                              # every free check, as CI runs them
```

- Airflow UI **http://localhost:8082** (`airflow`/`airflow`). **Not 8080**: another Airflow on the
  developer's machine owns that port, and logging in there gives "invalid login".
- Postgres `localhost:5433`, user `app`/`app`. Databases: `unternehmensdatenbank` (the Berlin example
  data) and `unternehmensdatenbank_staging` (empty). Airflow and Hasura use the one named by
  `APP_DB_NAME` in `.env`. **Port 5432 on this machine is a different
  Postgres** — a tool pointed there sees an empty or foreign database.
- Optional profiles: `geo` (local Overpass for OSM matching), `debug`, `datahub` (Keycloak, Hasura, S3).

**Three DB roles, as on the hub:** `app` owns the schema (Flyway, host scripts), DAGs run as `writer`
(rows only, no DDL), Hasura as `reader`. A DAG that needs DDL is wrong, not the grant.

## Environment

`cp .env.example .env`; `dev-up.sh` fills in a Fernet key. All DAGs are **disabled by default** via
`ENABLE_*` flags checked at task runtime, so a missing flag fails the task rather than hiding the
DAG. A DAG needs **two** switches: its flag *and* unpausing in Airflow.

## Architecture

Multi-source canonical resolution, detail in [data-model](docs/data-model.md):
`sources` → `company_source_records` (the whole record a source delivered) →
`company_field_observations` (one row per field claim, with a confidence) →
`pipeline_coordinator` → `company_canonical_field_sources` → views (`v_companies_enriched`,
`v_company_profile` for the app).

- **`pipeline_coordinator` is the only thing that decides which observation wins a field.** Sources
  write observations and nothing else, then trigger it.
- **Priority is sorted before confidence, so it is absolute** (decided again 2026-10-28): nothing at
  priority 100 beats anything at 50, at any confidence. Within one priority, confidence decides.
- **`website_url` is a view column, not a table column.** Enriched fields live in
  `company_canonical_field_sources`, read through `v_companies_enriched`.
- **Company identity is a random UUID, never the name.** Names repeat and change (32,170 Berlin
  companies have a former name). Matching across sources is by identifiers first, fuzzy second (#18).
- **OffeneRegister's full record, officers and raw address included, sits in its source record.**
  That is personal data: `company_source_records` is not exposed through Hasura, and nothing of it
  leaves local development without a privacy review (TSB rules).

## DAGs

[airflow/dags/README.md](airflow/dags/README.md): `pipeline_coordinator`, `manager_ner_extract` /
`manager_ner_train` (spaCy, retrained from the app's votes), `foerderkatalog` and `berlin_zuwendungen` (grants, `@monthly`), and the
library `enrichment/`. `berlin_gewerbedaten` was removed on 2026-10-28: Berlin's open trade data is
anonymised (no name, no street, no register number), so it cannot be linked to a company. Website discovery and the Technologiefeld have no DAG yet; they are rebuilt on
the library next (#18).

- DAG packages import their own modules **by package name** (`from enrichment.confidence import …`).
  A bare `from scorer import` after a `sys.path` insert once handed one package another's module.
- On the hub, DAG tasks will run as **KubernetesPodOperator** pods with our own image (#18), so code
  must not rely on a shared disk: state belongs in Postgres, not in `airflow/data/` files.

## Website discovery rules (`enrichment/`)

Read [website-verification](docs/website-verification.md) before touching `confidence.py`,
`own_site.py`, or anything that reads a page.

- **A register number on a page proves the page is *about* a company, never that it *is* that
  company's site.** Registry pages, insolvency trackers and directories all quote them.
  `own_site.is_own_site_candidate` is the single predicate for that call; a looser one anywhere else
  is the recurring bug.
- **"A real business site" is not "THAT company's site."** Every German business site has an
  Impressum, a VAT number and a contact email at its own domain. That evidence may add score but
  never makes a page the company's website: only `confidence.identifies_company` does, and it asks
  for the own register number, a Geschäftsführer who is a register officer, or name AND postcode
  together. Letting the Impressum alone count once promoted 190 wrong websites.
- **Never grow a list of bad domains or words.** Genericness is *measured*:
  `name_frequency.common_words` counts company names, `common_email_domains` counts mapped businesses.
- **`enrichment/observations.py` is the write path for discovery results.** Its `evidence` set must be
  `sorted()` before `Json()`: a live `set` is not serialisable.
- Search-engine discovery (paid APIs, SearXNG, Ollama) was measured and removed; a new search source
  must feed the same verification and `confidence.py`, never a scorer of its own.

## Industry classification — THREE columns, never mixed

| | Technologiefeld | WZ cluster | purpose_field |
|---|---|---|---|
| question | what field does it **work in**? | what is it **registered as**? | what was it **set up to do**? |
| from | words the company published | its official WZ code | the register's business purpose |
| values | one of Berlin's 77, or `NONE` | 76 divisions in 20 sections | one of the same 77, or `NONE` |

Details: [branch-classification](docs/branch-classification.md),
[purpose-classification](docs/purpose-classification.md). OffeneRegister has no WZ code and no
purpose, so for Berlin only the Technologiefeld can be filled today.

1. **A WZ code may never fill the Technologiefeld.** `classify()` has no WZ parameter; the guard fails
   if one reappears.
2. **A Technologiefeld requires a website somebody downloaded**, and the gate is Postgres, not a file.
3. **`NONE` has four reasons kept apart**: `no_website`, `torn`, `no_keyword`, `no_text`. Never add two.
4. **Never match a keyword inside a German compound.** `BRANCH_HYPHEN_IS_BOUNDARY` is **off**, measured.
5. **A keyword wrong for its field is deleted from the YAML with the count in a comment**, never a
   stop-list in code.
6. **Keywords decide; the spaCy text classifier only fills gaps**, and is promoted only when it beats
   the keywords on companies people have voted on.
7. **No row of any column is hand-checked yet.** Agreement is not correctness.

## Migrations — V1 baseline, V2 identifiers

`V1__baseline.sql` is the whole schema as of 2026-10-28 (24 earlier migrations squashed while nothing
was deployed); `V2` adds identifiers, matching and pipeline state ([data-model](docs/data-model.md)).
From here on forward-only: never edit a migration that has run anywhere shared. Flyway is a one-shot
service under `--profile tools` and does **not** run with `docker compose up`. On the hub it runs as
the app's initContainer from the migration image ([release](docs/release.md)).

- **Matching:** a source record finds its company through `company_identifiers` first, a fuzzy
  score second, and records `match_method`/`match_confidence` on its source record. Merging never
  deletes (`companies.merged_into`).
- **Pipeline state** goes into `pipeline_state`, never a file: the hub's pods share no disk.
- **The company profile is stored (V5).** `v_company_profile` reads `company_profile_mv`; anything
  that changes companies or their published values must end with `refresh_company_profile()`
  (the coordinator does). A filter on the live view took up to 45 s.
- **Grant links (V4)** are a table, refreshed by `refresh_company_funding_links()` after grants or
  companies load. Never link grants on read again: it made the company list take 71 s.
- **Feedback and provenance (V3):** users can vote on every shown field, and `v_field_provenance`
  says where every shown value came from. A new shown field needs its key in both.

**The observation upsert key is a partial unique index (`uq_obs_company_field_source_value`), and
`insert_text_observation` in `source_enrichment_common.py` repeats its predicate verbatim in its
`ON CONFLICT (...) WHERE`.
The two must stay identical** — Postgres infers the partial index from the predicate. Widen one,
widen the other, same commit.

## Checks

`scripts/free_checks.sh` is the one list: ruff, the unit tests, and seven rule guards. No database,
no network, no model. CI, no-mistakes and both Claude hooks run it (the edit hook runs its guard
lines, the commit hook the whole script). What each guard defends:
[guards-and-tests](docs/guards-and-tests.md). Two rules that keep being got wrong:

1. **Import the rule a guard checks, never restate it** — a check that rebuilds the logic agrees with
   itself no matter what the real code does.
2. **Assert on the syntax tree, not the source text**, when the forbidden thing is discussed in a
   docstring. A plain `in` test once found `Handel` inside `Handelsregister`.

**Shipping:** if `no-mistakes` is installed (`command -v no-mistakes`), validate and open the PR with
`/no-mistakes`. Without it, the normal branch + PR flow. It is optional for contributors.

**Git Bash caveat:** `docker compose exec -e PYTHONPATH=/opt/airflow/dags` fails under Git Bash,
which rewrites the path to `C:/Program Files/Git/opt/airflow/dags`. Use PowerShell, or prefix
`MSYS_NO_PATHCONV=1`.
