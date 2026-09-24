# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## Where the detail lives

**This file is a map, and it is 326 lines against a 300-line target.** It holds only what has cost
somebody real time when they got it wrong. When a section wants to grow, the material goes to the
repo docs or the vault and this file keeps the one-line rule plus the link — that is how the guard
and test detail left for [guards-and-tests](docs/guards-and-tests.md), and how the industry
classification detail left for `docs/branch-classification.md` and `docs/purpose-classification.md`
on 2026-09-24 (each has a dated "State on 2026-09-24" section carrying what moved).

- **Repo docs:** [branch-classification](docs/branch-classification.md) ·
  [purpose-classification](docs/purpose-classification.md) ·
  [running-branch-classification](docs/running-branch-classification.md) ·
  [combined-discovery-verification](docs/combined-discovery-verification.md) (why a page is judged
  to belong to a company) · [running-discovery](docs/running-discovery.md) ·
  [data-model](docs/data-model.md) · [guards-and-tests](docs/guards-and-tests.md) ·
  [core-queries-current.sql](docs/core-queries-current.sql) ·
  [airflow/dags/README.md](airflow/dags/README.md). The module docstrings are long on purpose and
  are the real documentation.
  **`docs/core-queries.sql` is the OLD catalog and is largely unrunnable** — most of it addresses
  columns V9 dropped, and its query 3 would create a source at priority 10 that outranks everything
  real. Read the warning at its top; use `core-queries-current.sql`.
- **Obsidian vault** at `C:\Users\TuPhan\TSB_Projects_vaults\unternhemensdatenbank` (local, not
  checked in): `Unternehmensdatenbank` is the index, then `Website Discovery Runbook`, `Branch
  Classification`, `Combined Discovery`, `Confidence and Resolution`, `Data Model`, `Discovery DAGs`,
  `URL Guessing`, `Local Model Setup`, `Search Provider Comparison`, `Local Dev Runbook`, `Repo
  Conventions and Gotchas`. **The code always wins a disagreement**, and the vault is not in git, so
  a vault edit is never part of a commit.

Every `.md` here is written for a smart 18-year-old who has never seen this project, German company
registers, or Airflow: explain a thing before naming it, short sentences, **numbers not adjectives**,
and keep the caveat — if a number came from 100 companies, say so. Full rules: vault `Repo
Conventions and Gotchas`.

## What this is

Local scaffold for a Postgres-backed German company-data platform. Airflow orchestrates, Flyway owns
all schema, Python DAGs write with psycopg2. Two databases on one server: `airflow` (metadata) and
`unternehmensdatenbank` (application data). `public.companies` holds 14,443 companies from a
Northdata export of the Osnabrück region, mostly name + address; the work is filling the missing
fields cheaply and traceably. **Osnabrück is a benchmark — the real data is Berlin, arriving as
another Northdata CSV.**

Postgres 16 (port 5433), Airflow 3.2.0 (Airflow 3 API — `airflow.providers.standard`, not
`airflow.operators`), Flyway 12.

## Commands and access

```bash
./scripts/dev-up.sh                                 # full stack, correct order
docker compose --profile tools run --rm flyway      # pending migrations ( | flyway info )
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
docker compose build                                # after Dockerfile/requirements changes
docker compose down                                 # add -v to destroy volumes
```

Optional profiles: `geo` (overpass), `geocoding` (nominatim), `llm` (searxng, ollama), `debug`.

- Airflow UI **http://localhost:8082** (`airflow`/`airflow`), from `AIRFLOW_PORT`. **Not 8080** — the
  container listens on 8080 internally, which is what `docker ps` shows, and another Airflow on this
  machine owns 8080 on the host. Logging in there gives "invalid login".
- pgAdmin http://localhost:5051 (`admin@example.com`/`admin`) · Postgres `localhost:5433`, db
  `unternehmensdatenbank`, `app`/`app`

## Environment

`cp .env.example .env`. All DAGs are **disabled by default** via `ENABLE_*` flags checked at task
runtime, so a missing flag fails the task rather than hiding the DAG. `.env` is not regenerated from
`.env.example` and is missing many keys — usually harmless, since `docker-compose.yml` defaults every
one, but a variable absent from **both** cannot be changed by editing `.env` alone.

A DAG needs **two** switches: its `ENABLE_*` flag *and* unpausing in Airflow. One run processes
`SOURCE_ENRICHMENT_BATCH_SIZE` companies (default 50) and checkpoints to
`airflow/data/<dag_id>_checkpoint.json`, so re-triggering continues rather than restarts.

## Architecture: data model

Multi-source canonical resolution — full detail in `docs/data-model.md`.

## Architecture: five pipelines, seven sources

They are **independent**, not a waterfall: own source key, own record of which companies it has
handled, own checkpoint. They meet only in `pipeline_coordinator`. Counts measured 2026-09-09 and
pinned in `tests/integration/test_ground_truth_discovery.py`.

| | pipeline | priority | canonical websites | cost |
|---|---|---|---|---|
| P1 | `northdata_export` — the register CSV | **50** | **2,815** | free |
| P2 | `brave_` / `exa_` / `parallel_search_discovery` | 100 | 85 / 39 / 1 | **paid** |
| P3 | `combined_discovery` — a script, not a DAG | 100 | 552 | free |
| P4 | `url_discovery` | 100 | 37 | free |
| P5 | `branch_classifier` — the 77-field taxonomy | 100 | 0 websites; both industry columns published | free |
| | | | **3,529** | |

**Priority is sorted before confidence, so it is absolute** — nothing at 100 beats anything at 50 at
any confidence. That is why 2,815 export URLs nobody has fetched outrank downloaded, register-checked
pages. Intended: a URL filed with the register beats a guessed domain. Among the six at 100,
confidence alone decides, which is what keeps P2's provider comparison fair.

**`mistral_`/`claude_`/`ollama_search_discovery` are code that has never written a row** — no
`sources` entry exists for any of them. Do not count them as pipelines.

**Everything triggers the coordinator itself.** P1–P4 all do, P3 by calling the Airflow API from the
script. There is no manual trigger step any more; `--skip-coordinator` opts out when measuring.

## Architecture: DAGs

All in `airflow/dags/`, all `schedule=None` — **except `berlin_gewerbedaten`, which is `@daily`** and
runs by itself once its flag is true and it is unpaused. Per-DAG detail in `airflow/dags/README.md`.

- `exa_/parallel_/brave_search_discovery` share `search_discovery_runner.py` **verbatim** — same
  model, prompt and scoring, only the search step differs. That is what makes "which provider is
  best" a fair question, so **do not fork the flow to fit a provider.** `mistral_/claude_` use the
  provider's own server-side search; `ollama_` pairs a local SearXNG with a local model.
- `url_discovery` guesses URLs from the name; `osm_discovery` matches a local Overpass (Berlin only).
- **`pipeline_coordinator` is the only thing that decides which observation wins a field.** Source
  DAGs write observations and nothing else, then trigger it.

## Combined discovery — a script, not a DAG

`airflow/dags/combined_discovery/`, driven by `scripts/run_combined_discovery.py`. No API money,
resumable. Two stage names get confused: **`llm` searches** (SearXNG, CAPTCHA-blocked on this
network, finds nothing) and **`judge` reads** pages already downloaded (Ollama only, no search
engine).

**Which subcommands touch the database — this changed on 2026-09-08 and the old "writes nothing to
the database" is now wrong for two of them:**

| subcommand | database |
|---|---|
| `run` | **writes** observations via `observations.py`, then **triggers `pipeline_coordinator`** |
| `classify` | **reads** it — the verified-website gate comes from `v_companies_enriched` |
| `classify-purpose`, `report`, `csv`, `labels`, `fetch-content` | nothing, still free and offline |

So `run` and `classify` both need Docker up. `--skip-coordinator` suppresses the trigger when you
are only measuring.

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"   # required; the default qwen2.5:7b is not pulled
python scripts/run_combined_discovery.py run --limit 1000 --stages url,osm,judge
python scripts/run_combined_discovery.py report --limit 1000   # then: csv | labels | labels-report
```

**943 websites** over the full table — **886 `high`, 56 `medium`, 1 `low`**. Read
[combined-discovery-verification](docs/combined-discovery-verification.md) before touching
`confidence.py`, the judge gate, or anything that reads a page. Three rules that keep being broken:

1. A pre-filter for the judge is a **veto only** — cheap evidence may cancel a model call, never
   replace one. Its value is its rejections, and they land on convincing-looking rows.
2. Counting lost *discoveries* hides lost *rejections*. `replay_judge_gate.py` fails on both.
3. **Scoring** changes replay offline from `results.jsonl`; **extraction** changes need the cached
   pages (`COMBINED_PAGE_CACHE_DIR`). Warm, a company costs 0.8 s — the run is model-bound.

Ollama runs native on Windows with `OLLAMA_IGPU_ENABLE=1`, **74× faster** than the Docker one: vault
`Local Model Setup`.

## Industry classification — THREE columns, and they must never be mixed

| | Technologiefeld | WZ cluster | purpose_field |
|---|---|---|---|
| the question | what field does it **work in**? | what is it **registered as**? | what was it **set up to do**? |
| from | words the company published | its official WZ code | `business_purpose`, the register |
| values | one of Berlin's 77, or `NONE` | 76 divisions in 20 sections | one of the same 77, or `NONE` |
| filled for | **403** — 36.2% of the 1,113 with a fetched website | **8,483** (58.7%) | **1,200** — 16.8% of the 7,141 usable purposes |
| requires | **a website**; no site, no field | nothing but the code | a purpose over 40 chars |
| judgement | keyword weights, thresholds | **none** — a lookup | the same weights, plus a 3-way tie-break |
| file | `branches.jsonl` | (same file) | `purpose_branches.jsonl` |

Every number, every deleted keyword and the reasoning:
[branch-classification](docs/branch-classification.md) ·
[purpose-classification](docs/purpose-classification.md) · vault `Branch Classification`.
Commands, in order: [running-branch-classification](docs/running-branch-classification.md) —
`check_branch_classifier.py` and `check_purpose_branch.py` are free and run first.

1. **A WZ code may never fill the Technologiefeld.** `classify()` has no WZ parameter at all, and the
   guard fails if one reappears.
2. **A Technologiefeld requires a website, and the gate is Postgres** (`load_verified_websites()`), not
   the result file — and deliberately not the 3,529 canonical URLs, 2,815 of which were never fetched.
3. **`NONE` is normal, with four reasons kept apart**: `no_website`, `torn`, `no_keyword`, `no_text`.
   **Never add two together.** `outcome` says whether there is a field, `decided_by` says who.
4. **Division = `wz_code[:2]`**, section = the letter above it, from `wz_titles.yaml` — **WZ 2025**; a
   WZ 2008 table mismatches silently.
5. **Never match a keyword inside a German compound.** `BRANCH_HYPHEN_IS_BOUNDARY` is **off**, measured.
6. **A keyword wrong for its field is deleted from the YAML with the count in a comment**, never a
   stop-list in the code. Ask whether it is generic in **German** or only in Osnabrück.
7. **Keywords decide; the model only breaks ties, from three options**, and `think` stays off. The
   current file has `llm_branch` empty on all 14,443 rows: `decided_by` is `keywords` for all 403.
8. **`classify` makes zero web requests** but opens **one database connection** for the gate, so Docker
   must be up. `fetch-content` is the only part that downloads.
9. **Column 3 has no website gate — that is its point**, and `purpose_branch` never calls `classify()`.
   Own vocabulary (`assigned_keywords`, not `assigned`), **eight** outcomes summing to 14,443,
   **4,705 fields** after the Tier B run of 2026-09-10. A **failed** model call is never `none_apply`,
   and `unsure` is not `none_apply` either.

Not known: **whether any row of either column is right** — none is hand-checked. `classify-labels`
writes the worksheet; only `classify-labels-report` reports precision.

## Facts that have misled people repeatedly

- **`website_url` is a view column, not a table column.** V9 dropped it from `companies`. Enriched
  fields live in `company_canonical_field_sources`, read through `public.v_companies_enriched`.
- **The import carries 3,283 websites, and `import_northdata_websites.py` is the only thing that
  reads them.** For months nothing did — the `Status` mistake again, one field over. Discovery had
  found 1,136; **2,375 more sat in the import for companies it never found**. That script writes them
  as `website` observations at **priority 50, beating all six discovery sources**, so it also
  replaces **184** canonical websites where the two disagree (137 of them `combined_discovery`, mean
  confidence 0.95). `--dry-run` prints that list; undo is `UPDATE public.sources SET priority = 200
  WHERE key = 'northdata_export'` plus one `pipeline_coordinator` run, because the resolver only
  un-canonicalises a displaced observation and never deletes it. The export's `status` is English
  (`active`) where `register_status` is German (`aktiv`), and the website is at `extras[].items[]`
  `id='url'`, not a top-level field: [data-model](docs/data-model.md).
- **53% of `companies` is `erloschen`** — dissolved. V13 promoted the register's `Status` into
  `companies.register_status` (`aktiv` 6,298 / `erloschen` 7,673 / `in Liquidation` 472); it had been
  in `metadata` since the import, unread. Every published number — 943 websites, 886 `high`, 403
  fields — was measured over the dead ones, which supplied 4.0% of the 902 websites of 2026-08-14,
  the last time that split was counted.
  `DISCOVERY_ACTIVE_ONLY=1` skips them and is **off**: the limit applies *after* the filter, so a
  limited run gets a **different, not smaller** sample (`--limit 200` → 200 live, overlapping the
  unfiltered 200 by 78). Keywords still score all 14,443; only a **new** model call is withheld, and a
  cached verdict still applies. The manual Berlin `\copy` **must map `register_status`** or every row
  is NULL and an active-only run does nothing. [data-model](docs/data-model.md).
- **A register number on a page proves the page is *about* a company, never that it *is* that
  company's site.** Registry pages, insolvency trackers and directories all quote them.
  `search_common.is_own_site_candidate` is the single predicate for that call; a looser one anywhere
  else is the recurring bug here. It reappeared in the judge, where a directory scored 0.805.
- **"A real business site" is not "THAT company's site."** Every German business site has an
  Impressum, a VAT number and a contact email at its own domain, so none identifies one company. That
  evidence may add score but must stay out of `confidence._VERIFYING_EVIDENCE`, or it lifts the 0.34
  cap and promotes pages naming nobody — how 190 wrong websites got promoted.
- **Never grow a list of bad domains or words.** Genericness is *measured*:
  `name_frequency.common_words` counts company names, `common_email_domains` counts mapped
  businesses. The most common email domain here is `osnanet.de`, a local ISP no hand-written list
  would contain.
- **`combined_discovery/observations.py` IS the live write path — this entry used to say the
  opposite.** It read *"has no importer, the live write path is `import_combined_discovery.py`'s own
  SQL, editing `observations.py` changes nothing"*, and that was true until 2026-09-08. It is now
  exactly inverted: `write_company_result` in `observations.py` does the writing, and
  `import_combined_discovery.py` is a thin bridge that calls it. **Editing `observations.py` changes
  the production write path for P3.** Its `evidence` set must be `sorted()` before it is handed to
  `Json()`, because a live `set` is not serialisable and the first execution of this module failed on
  exactly that.
- Two more live in the industry section above, because that is where they bite: a **WZ code is not a
  technology field** (rule 1), and a **word naming a quality is not a word naming an industry**
  (rule 6).

## Migrations — currently V1–V20

Schema changes go in `db/migrations/` as `V{N}__description.sql`, forward-only and immutable once
applied. Flyway is a one-shot service under `--profile tools`; it does **not** run with
`docker compose up`. Run migrations before starting Airflow whenever schema changes land.

The four most recent fixed live bugs. Each migration's own header has the full reasoning:

| | |
|---|---|
| **V17** | deduped 49 observation groups, added the upsert key so a re-run updates instead of appending. **Partial** — 71 groups remain in `ceo_name_or_contact`, `email`, `phone`, `website_search_query`, `cluster` |
| **V18** | widened that key from two field names to six |
| **V19** | `company_managers` **9,195 → 1,047** + `uq_company_managers_company_name`. `ON CONFLICT DO NOTHING` with no target can only catch a constraint that exists, and the only one was a PK on a `gen_random_uuid()` column, so every coordinator run appended a full copy of every name |
| **V20** | the `branch_classifier` source; `technologiefeld`/`purpose_field` reserved in the view; `technologiefeld_withheld`/`purpose_field_withheld` as names **nothing resolves** (the `directory_url` trick, so no threshold change publishes them). Schema only at the time; the write path landed later and both columns are now served |

**V20 widened the upsert key to ten field names, and `insert_text_observation` in
`source_enrichment_common.py` repeats that predicate verbatim in its `ON CONFLICT (...) WHERE`
clause. The two must stay identical** — Postgres infers the partial index from the predicate, so a
name in one and not the other cannot be upserted at all. Widen one, widen the other, same commit.

## Guards and tests

**Eight guards** in `scripts/` check the *rules*; **17 pytest tests** in `tests/` check the *numbers
those rules last produced*. All the guards are free — no API, no model, no database, no network.
Two hooks, tracked in `.claude/` since 2026-09-24 and wired from `.claude/settings.json`: after an
edit, `guard_after_edit.py` runs **ruff** on any `.py` and the six guards under `combined_discovery/`,
`scripts/` or `tests/`; before a `git commit`, `run_checks_before_commit.py` runs ruff on the staged
files, the six guards and the ten Postgres tests — **Docker down blocks the commit**. `ruff.toml`
holds the default rule set only; E501 is off on purpose.

Neither kind checks whether the answers are **correct** — nothing here does, because not one row of
the three industry columns has ever been hand-labelled. A failure means "these numbers moved, find
out why".

Which guard defends what, the hook's two traps, and the rule about changing a pinned number:
**[guards-and-tests](docs/guards-and-tests.md)**. Four things from it that keep being got wrong:

1. **Import the rule a guard checks, never restate it** — a check that rebuilds the logic agrees with
   itself no matter what the real code does.
2. **Assert on the syntax tree, not the source text**, when the forbidden thing is discussed in a
   docstring. A plain `in` test once found **`Handel` inside `Handelsregister`**.
3. **Count canonical websites *through* `company_canonical_field_sources`**, never by grouping on the
   observation's own `field_name` — that splits Brave's 85 into 82 + 3.
4. **`WEBSITE_OBSERVATION_FLOOR` is a floor, not an equality.** Observations are never deleted, only
   un-canonicalised, so it may rise and must never fall.

`scripts/archive/` holds **eight trial scripts** kept for their numbers, not for running. **Each
needs a `parents[2]` path fix first** — they resolve the repo root relative to themselves, and moving
them one level deeper broke it. See `scripts/archive/README.md`.

## Known deferred work

Decided, not forgotten; each for a stated reason.

1. ✅ **Write path for `technologiefeld`/`purpose_field` — DONE, and both columns are published.**
   `scripts/import_branch_classification.py` reads both JSONL files and writes through
   `combined_discovery/branch_observations.py`; `classify` stays compute-only. Both `resolve_*`
   wrappers exist and all five hardcoded field sites are widened — the quality-score weights
   deliberately are **not**, so no stored score moved. Counts: `CANONICAL_OTHER_FIELDS` and
   `BRANCH_CLASSIFIER_OBSERVATIONS` in `tests/integration/test_ground_truth_discovery.py`; the
   emitter gates on the **outcome**, never the value, or `'NONE'` becomes a canonical field.
   What remains is the data, not the code: the values are Osnabrück figures Berlin replaces
   wholesale, and not one row of either column is hand-checked (item 5).
2. **Full `search_discovery_runner.py` scoring unification** — P2 has a 2-signal scorer, P3 has 25
   evidence constants. *After Berlin*: Brave costs money to re-run, and **P2 stored no page evidence,
   so its rows cannot be re-scored offline** the way P3's can from `results.jsonl`.
3. **`fetch-content` over the 2,672 canonical URLs with no cached page** — free, ~2,700 requests.
4. **`docs/core-queries.sql`** — warned and flagged, not yet cut down; working queries are in
   `docs/core-queries-current.sql`. And `import_northdata_websites.py` ~:303 is a stale log line.
5. **No row of any of the three industry columns is hand-labelled**, so no precision figure exists
   for any of them. `classify-labels` / `classify-labels-report` is the cheapest way to start.

`scripts/archive/ac_measurement.py` measures what a name and address alone yield — 29% verified websites, 15% owners
— and adding the Handelsregister number makes it *worse* (21% vs 25%) by pulling results toward
register aggregators ([results](docs/website-and-owner-discovery-results.md)).
`prepare_company_import.py` converts a Northdata export, repairing rows whose quoted `Gegenstand`
contains commas.

**Git Bash caveat:** `docker compose exec -e PYTHONPATH=/opt/airflow/dags` fails under Git Bash,
which rewrites the path to `C:/Program Files/Git/opt/airflow/dags` on the way in. It surfaces as an
unrelated `ModuleNotFoundError`. Use PowerShell, or prefix `MSYS_NO_PATHCONV=1`.
