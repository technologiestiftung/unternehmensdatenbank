# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## Where the detail lives

**This file is a map and stays under 200 lines.** It holds only what has cost somebody real time
when they got it wrong. When a section wants to grow, the material goes to the repo docs or the
vault and this file keeps the one-line rule plus the link.

- **Repo docs:** [branch-classification](docs/branch-classification.md) ·
  [purpose-classification](docs/purpose-classification.md) ·
  [running-branch-classification](docs/running-branch-classification.md) ·
  [combined-discovery-verification](docs/combined-discovery-verification.md) (why a page is judged
  to belong to a company) · [running-discovery](docs/running-discovery.md) ·
  [data-model](docs/data-model.md) · [core-queries.sql](docs/core-queries.sql) ·
  [airflow/dags/README.md](airflow/dags/README.md). The module docstrings are long on purpose and
  are the real documentation.
- **Obsidian vault** at `C:\Users\TuPhan\TSB_Projects_vaults\unternhemensdatenbank` (local, not
  checked in): `Unternehmensdatenbank` is the index, then `Branch Classification`, `Combined
  Discovery`, `Confidence and Resolution`, `Data Model`, `Discovery DAGs`, `URL Guessing`, `Local
  Model Setup`, `Search Provider Comparison`, `Local Dev Runbook`, `Repo Conventions and Gotchas`.
  **The code always wins a disagreement.**

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

1. `public.companies` — the current best row the application consumes
2. `public.company_source_records` — one raw row per `(source_id, external_id)`
3. `public.company_field_observations` — individual field-level claims from sources
4. `public.company_canonical_field_sources` — which observation won each field

Supporting: `public.sources` (lower `priority` wins), `public.source_sync_runs`,
`public.company_managers`. Per record: source → sync run → identity → raw row → observations →
promote the winner (`is_canonical = true`) → canonical field source.

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
writes nothing to the database, resumable. Two stage names get confused: **`llm` searches** (SearXNG,
CAPTCHA-blocked on this network, finds nothing) and **`judge` reads** pages already downloaded
(Ollama only, no search engine).

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"   # required; the default qwen2.5:7b is not pulled
python scripts/run_combined_discovery.py run --limit 1000 --stages url,osm,judge
python scripts/run_combined_discovery.py report --limit 1000   # then: csv | labels | labels-report
```

**902 websites** over the full table, 842 of them `high`. Read
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
| filled for | **489** — 54% of the 902 with a website | **8,483** (58.7%) | **610** — 8.5% of the 7,141 usable purposes |
| requires | **a website**; no site, no field | nothing but the code | a purpose over 40 chars |
| judgement | keyword weights, thresholds | **none** — a lookup | the same weights, plus a 3-way tie-break |
| file | `branches.jsonl` | (same file) | `purpose_branches.jsonl` |

Every number, every deleted keyword and the reasoning:
[branch-classification](docs/branch-classification.md) ·
[purpose-classification](docs/purpose-classification.md) · vault `Branch Classification`.
Commands: [running-branch-classification](docs/running-branch-classification.md).

```powershell
python scripts/check_branch_classifier.py                                # free, first
python scripts/check_purpose_branch.py                                   # free, column 3
python scripts/run_combined_discovery.py fetch-content                  # once, downloads
python scripts/run_combined_discovery.py classify --source page,purpose  # 2 min, no network
python scripts/run_combined_discovery.py classify-report
python scripts/run_combined_discovery.py classify-purpose                # 13 s, no network
python scripts/run_combined_discovery.py classify-purpose-report
```

1. **A WZ code may never fill the Technologiefeld.** Mapping one onto the other gave 2,632 companies
   a field nothing they published supported, and flattened 22 sections and 87 divisions to do it.
   `classify()` has **no WZ parameter at all**, and the guard fails if one reappears.
2. **A Technologiefeld requires a website.** A register purpose says what a company is *permitted* to
   do and much of it is boilerplate; a live website is evidence it trades. Price: **909 → 346**, 563
   withheld and **kept** in `withheld_branch`, never deleted. `has_website` defaults to `False`.
3. **`NONE` is normal, with four reasons kept apart**: `no_website` 563, `torn` 129, `no_keyword`
   6,558, `no_text` 6,704. **Never add two together.** And `assigned` (489) does not mean the model
   assigned it — `outcome` says whether there is a field, `decided_by` says who: keywords 346,
   model 143.
4. **Division = `wz_code[:2]`**, section = the letter above it, from `wz_titles.yaml` — 2,008 codes,
   **WZ 2025**. `70.10.4` and `62.10.9` do not exist in WZ 2008, so a 2008 table mismatches silently.
5. **Never match a keyword inside a German compound.** Allowing it made Defence Tech the biggest
   field in Osnabrück while clear winners *fell* 338 → 316. Hyphenated compounds still get through;
   `BRANCH_HYPHEN_IS_BOUNDARY` closes that and is **off**, measured.
6. **A keyword wrong for its field is deleted from the YAML with the count in a comment** — 27 have
   gone that way, never a stop-list in the code. Ask whether it is generic in **German** or only in
   Osnabrück: `clubs` scored just as badly here and was kept, because in Berlin a Club is a venue.
7. **Keywords decide; the model only breaks ties, from three options.** Run once, 2026-08-25: 172
   rows, 29 min, **143 answered, 26 refused, 3 invented a field name** and were discarded by the
   off-list check. It can never create a field from nothing — only convert `torn` → `assigned` — so it
   reaches none of the 6,558 `no_keyword` ones. `think` stays off: with thinking on it spends the whole
   120-token budget reasoning and returns nothing. The 26 refusals are the best list of bad keywords
   we have.
8. **`classify` makes zero web requests**, two minutes over 14,443 companies — which is what makes
   arguing with the taxonomy cheap. `fetch-content` is the only part that downloads.

9. **Column 3 has no website gate — that is its point**, and `purpose_branch` never calls
   `classify()` (the guard asserts it on the *syntax tree*, since the docstring discusses both).
   Own file, own vocabulary: `assigned_keywords` not `assigned`. Seven outcomes summing to 14,443.
   A **failed** model call is never `none_apply`, and the 40-char floor costs 17 real fields — kept
   in `withheld_by_floor`, `PURPOSE_MIN_CHARS` is the knob. Numbers:
   [purpose-classification](docs/purpose-classification.md).

Not known: **whether any of the 489 are right** — no row is hand-checked, though the model's
`llm_reason` per row is a cheap proxy and reads well. `classify-labels` writes a
50-row worksheet in six buckets; only `classify-labels-report` reports precision. **Excel saves that
worksheet with semicolons** on a German locale; the reader sniffs the delimiter, so a round trip is
safe. The same is true of column 3, which has **no** labelling worksheet yet.

## Facts that have misled people repeatedly

- **`website_url` is a view column, not a table column.** V9 dropped it from `companies`. Enriched
  fields live in `company_canonical_field_sources`, read through `public.v_companies_enriched`.
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
- Two more live in the industry section above, because that is where they bite: a **WZ code is not a
  technology field** (rule 1), and a **word naming a quality is not a word naming an industry**
  (rule 6).

## Migrations

Schema changes go in `db/migrations/` as `V{N}__description.sql`, forward-only and immutable once
applied. Flyway is a one-shot service under `--profile tools`; it does **not** run with
`docker compose up`. Run migrations before starting Airflow whenever schema changes land.

## Utility scripts

Six guards, all free — no API, no model, no database, no network. Run them after touching
`search_common.py`, `imprint_probe.py`, `url_discovery/imprint_extractor.py`, or anything in
`combined_discovery/`. A `PostToolUse` hook in `.claude/settings.local.json` runs three automatically
after an edit under `combined_discovery/` or `scripts/`.

- `check_own_site_filter.py` · 21 real URLs guarding `is_own_site_candidate`
- `check_imprint_owner_extraction.py` · 19 Impressum layouts guarding owner extraction
- `check_combined_discovery.py` · URL guessing, the confidence ladder, the judge's gate, JSON-LD
- `check_branch_classifier.py` · the compound rule and hyphen knob, keyword weights, the WZ lookup,
  the `content-index` key, label staleness, and the model's answer validation — **imported** from
  `branch_llm.verdict_from_answer`, never restated
- `check_purpose_branch.py` · 154 checks on column 3: the seven outcomes stay disjoint, the website
  gate and WZ code cannot get in (asserted on the **syntax tree**, since the docstring discusses
  both), the floor keeps what it takes, and a failed model call never becomes `none_apply`
- `replay_judge_gate.py` · re-runs the real gate over `results.jsonl`, failing if a change loses a
  discovery **or a demotion**

`ac_measurement.py` measures what a name and address alone yield — 29% verified websites, 15% owners
— and adding the Handelsregister number makes it *worse* (21% vs 25%) by pulling results toward
register aggregators ([results](docs/website-and-owner-discovery-results.md)).
`prepare_company_import.py` converts a Northdata export, repairing rows whose quoted `Gegenstand`
contains commas.

**Git Bash caveat:** `docker compose exec -e PYTHONPATH=/opt/airflow/dags` fails under Git Bash,
which rewrites the path to `C:/Program Files/Git/opt/airflow/dags` on the way in. It surfaces as an
unrelated `ModuleNotFoundError`. Use PowerShell, or prefix `MSYS_NO_PATHCONV=1`.
