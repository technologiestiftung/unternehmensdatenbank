# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## Where the detail lives

This file is a map, deliberately short. The depth is in two places:

- **In the repo:** [docs/running-discovery.md](docs/running-discovery.md) (runbook),
  [docs/combined-discovery-verification.md](docs/combined-discovery-verification.md) (how a page is
  judged to belong to a company, and the measurement behind every rule),
  [docs/data-model.md](docs/data-model.md), [docs/core-queries.sql](docs/core-queries.sql),
  [airflow/dags/README.md](airflow/dags/README.md), and the module docstrings — which are long on
  purpose and are the real documentation.
- **In the maintainer's Obsidian vault** at `C:\Users\TuPhan\TSB_Projects_vaults\unternhemensdatenbank`
  (local, not checked in): `Unternehmensdatenbank` is the index, then `Confidence and Resolution`,
  `Data Model`, `Discovery DAGs`, `Combined Discovery`, `Local Model Setup`, `Search Provider
  Comparison`, `Local Dev Runbook`, `Repo Conventions and Gotchas`. Read them when available; the
  code always wins a disagreement.

## How to write the docs in this repo

Every `.md` here is for a smart 18-year-old who codes but has never seen this project, German
company registers, or Airflow.

1. Explain a thing before using its name. "Impressum — the legal contact page every German business
   website must have" comes before you start saying "the Impressum".
2. Short sentences, one idea each. If a sentence needs a comma to survive, split it.
3. Say what happens, not what the code is called. "The score drops by 200" beats "a penalty is applied".
4. Numbers, not adjectives. Not "coverage is low" — "we find a website for 29 out of 100".
5. Answer "so what" after every fact.
6. Define the invented words on first use in each file: canonical, observation, resolver, confidence,
   checkpoint.
7. Keep the honest caveat. If a number came from 100 companies, say so.

## What this is

Local development scaffold for a Postgres-backed German company-data platform. Airflow orchestrates,
Flyway owns all schema, Python DAGs write to Postgres with psycopg2. Two databases share one server:
`airflow` (metadata) and `unternehmensdatenbank` (application data). `public.companies` holds 14,443
companies from a Northdata export of the Osnabrück region, mostly name + address. The work is filling
in the missing fields cheaply and traceably.

Stack: Postgres 16 (port 5433), Airflow 3.2.0 (Airflow 3 API — `airflow.providers.standard`, not
`airflow.operators`), Flyway 12, Python DAGs.

## Common commands

```bash
./scripts/dev-up.sh                                    # full stack, correct order
docker compose up -d postgres
docker compose --profile tools run --rm flyway         # run pending migrations
docker compose --profile tools run --rm flyway info    # migration state
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
docker compose build                                   # after Dockerfile/requirements changes
docker compose --profile debug run --rm airflow-cli bash
docker compose down                                    # add -v to destroy volumes
```

Optional: `--profile geo up -d overpass`, `--profile geocoding up -d nominatim`,
`--profile llm up -d searxng ollama`.

## Access

- Airflow UI: http://localhost:8082 (`airflow`/`airflow`), from `AIRFLOW_PORT`. **Not 8080** — the
  container listens on 8080 internally, which is what `docker ps` shows, and another Airflow on
  this machine owns 8080 on the host. Logging in there gives "invalid login".
- pgAdmin: http://localhost:5051 (`admin@example.com`/`admin`)
- Postgres: `localhost:5433`, db `unternehmensdatenbank`, `app`/`app`

## Environment

`cp .env.example .env`. All DAGs are **disabled by default** via `ENABLE_*` flags checked at task
runtime, so a missing flag fails the task rather than hiding the DAG. `.env` is not regenerated
from `.env.example` and is missing many keys — usually harmless, since `docker-compose.yml`
defaults every one, but a variable absent from **both** cannot be changed by editing `.env` alone.

A DAG needs **two** switches: its `ENABLE_*` flag *and* unpausing in Airflow. One run processes
`SOURCE_ENRICHMENT_BATCH_SIZE` companies (default 50) and checkpoints to
`airflow/data/<dag_id>_checkpoint.json`, so re-triggering continues rather than restarts.

## Architecture: data model

Multi-source canonical resolution. Full detail in `docs/data-model.md`.

1. `public.companies` — the current best row the application consumes
2. `public.company_source_records` — one raw row per `(source_id, external_id)`
3. `public.company_field_observations` — individual field-level claims from sources
4. `public.company_canonical_field_sources` — which observation won each field

Supporting: `public.sources` (lower `priority` wins), `public.source_sync_runs`,
`public.company_managers`. Per incoming record: upsert source → create sync run → upsert company
identity → upsert raw row → insert observations → promote the winner (`is_canonical = true`) →
upsert the canonical field source.

## Architecture: DAGs

All in `airflow/dags/`, all `schedule=None` and triggered by hand or by another DAG — **except
`berlin_gewerbedaten`, which is `@daily`** and will run by itself once its flag is true and it is
unpaused. Per-DAG detail in `airflow/dags/README.md`.

- **Search-provider discovery** — `exa_search_discovery`, `parallel_search_discovery`,
  `brave_search_discovery` share `search_discovery_runner.py` verbatim: same model, same prompt,
  same scoring, only the search step differs. That is what makes "which provider is best" a fair
  question, so do not fork the flow to fit a provider.
- **LLM-hosted search** — `mistral_search_discovery`, `claude_search_discovery` use the provider's
  own server-side web search. `ollama_search_discovery` pairs a local SearXNG with a local model.
- **No-API** — `url_discovery` guesses URLs from the name and checks them. `osm_discovery` matches
  a local Overpass (Berlin data only).
- **Coordinator** — `pipeline_coordinator` is the only thing that decides which observation wins a
  field. Source DAGs write observations and nothing else, then trigger it.
- **Legacy** — `berlin_gewerbedaten` fetches the Berlin Gewerbedaten WFS, enriches with addresses and
  OSM POI names, writes JSONL, loads in batches of 250. Disabled; never chains into the coordinator.

## Combined discovery — a script, not a DAG

`airflow/dags/combined_discovery/`, driven by `scripts/run_combined_discovery.py`. Costs no API
money, writes nothing to the database, resumable. Four stages, and the last two are constantly
confused: **`llm` searches** (SearXNG, CAPTCHA-blocked on this network, finds nothing) and
**`judge` reads** pages the earlier stages already downloaded (Ollama only, no search engine).

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"   # required on the host; the default qwen2.5:7b is not pulled
python scripts/run_combined_discovery.py run --limit 1000 --stages url,osm,judge
python scripts/run_combined_discovery.py report --limit 1000   # then: csv | labels | labels-report
```

Measured on 1,000 Osnabrück companies (2026-08-14): a website for **53 (5.3%)** — 52 `high`, 38 proven
by a register number on the page. The paid search path measured **29% verified** on 100 companies, and
that gap is *not* a verification problem: for 665 of the 954 companies with no website we had already
downloaded a page and refused it. Only **289 never yielded a live page at all.**

Read [docs/combined-discovery-verification.md](docs/combined-discovery-verification.md) before
changing `confidence.py`, the judge gate, or anything that reads a page. In short:

1. A pre-filter for the judge is a **veto only** — cheap evidence may cancel a model call, never
   replace one. Its value is its rejections, and they land on convincing-looking rows.
2. Counting lost *discoveries* hides lost *rejections*. `scripts/replay_judge_gate.py` fails on both.
3. **Scoring** changes replay offline from `results.jsonl`; **extraction** changes need the pages —
   that is `COMBINED_PAGE_CACHE_DIR`. Warm, the median company costs 0.8 s: the run is model-bound.
4. `report` counts what happened; only `labels-report` says whether it was **right**.

Local model setup — native Windows Ollama with `OLLAMA_IGPU_ENABLE=1`, which is **74× faster** than
the Docker one because the host had paged the WSL2 VM's memory to disk: vault `Local Model Setup`.

## Industry classification — TWO columns, and they must never be mixed

| | Technologiefeld | WZ cluster |
|---|---|---|
| the question | what field does it **work in**? | what is it **registered as**? |
| from | words the company published | its official WZ code |
| values | one of Berlin's 77, or `NONE` | 76 divisions in 20 sections |
| filled for | **974** companies (6.7%) | **8,483** (58.7%) |
| judgement | keyword weights, thresholds | **none** — a lookup |

Full detail and every number: [docs/branch-classification.md](docs/branch-classification.md).
Command-by-command: [docs/running-branch-classification.md](docs/running-branch-classification.md).

```powershell
python scripts/check_branch_classifier.py                                # free, first
python scripts/run_combined_discovery.py fetch-content                   # once, 1,567 pages
python scripts/run_combined_discovery.py classify --source page,purpose   # 2 min, no network
python scripts/run_combined_discovery.py classify-report
```

1. **A WZ code may never fill the Technologiefeld.** An earlier version mapped one onto the other and
   gave 2,632 companies a technology field that nothing they published supported, while flattening 22
   sections and 87 divisions of good official classification to do it. `classify()` now has **no WZ
   parameter at all**, and the guard fails if one reappears. Same class of error as "a register number
   proves a page is *about* a company, never that it *is* its site".
2. **`NONE` is the normal Technologiefeld and not a failure**, with three reasons kept apart: `torn`
   400, `no_keyword` 6,365, `no_text` 6,704. Never add two of them together.
3. **The WZ cluster needs no mapping and no human.** Division = `wz_code[:2]`, section = the letter
   above it, both from `wz_titles.yaml` (2,008 codes, **WZ 2025** — `70.10.4` and `62.10.9` do not
   exist in WZ 2008, so a 2008 table mismatches silently).
4. **Never match a keyword inside a German compound.** Allowing it took `sicherheit` from 90 pages to
   139 — it hides in *Arbeitssicherheit* — and made Defence Tech the biggest field in Osnabrück, while
   clear winners *fell* 338 → 316.
5. **Read the homepage, not the Impressum** (68% carry a keyword against 44%) — and better still the
   pages it links to. 644 of 766 homepages link to `/leistungen` or `/ueber-uns`; fetching them gave
   **136** companies a field nothing else found.
6. **Keywords decide; the model only breaks ties, from three options.** Asked to pick from all 77 it
   files everything under "Dienstleistung". 10.3 s a page, so it is off by default.
7. **`classify` makes zero web requests** and takes two minutes over all 14,443 companies. That is
   what makes arguing with the taxonomy cheap; `fetch-content` is the only part that downloads.

Not yet known: **whether the Technologiefeld is right.** 974 companies have one and no row is
hand-checked. `classify-labels` writes the 40-row worksheet; only `classify-labels-report` reports
precision. The WZ column needs no labelling — nobody guessed it.

## Six facts that have misled people repeatedly

- **`website_url` is a view column, not a table column.** V9 dropped it from `companies`. Enriched
  fields live in `company_canonical_field_sources` and are read through `public.v_companies_enriched`.
- **A register number on a page proves the page is *about* a company, never that it *is* that
  company's site.** Registry pages, insolvency trackers and directories all quote them.
  `search_common.is_own_site_candidate` is the single predicate for that call; a looser one anywhere
  else is the recurring bug here. It reappeared in the judge, where a directory scored 0.805.
- **An official industry code says what a company is REGISTERED AS, never which technology field
  it WORKS IN.** The two are separate columns — Technologiefeld and WZ cluster — and mapping one
  onto the other gave 2,632 companies a field nothing they published supported. Structurally
  prevented now: `branch_classifier.classify()` has no WZ parameter.
- **A word that names a quality is not a word that names an industry, and no measurement separates
  them.** `sicherheit` appears in 1.8% of our texts, `einzelhandel` in 3.0%; only one is decisive.
  Berlin's sheet filed `Sicherheit` under Defence Tech, and it produced 27 of that branch's 35
  assignments alone — a bed shop, a lift company, an estate agent, a dental laboratory. The fix is
  deleting the keyword from `branch_taxonomy.yaml` with the number in a comment. It is **not** a
  stop-list in the code: that is the next bullet, and the two are easy to confuse.
- **Never grow a list of bad domains or words.** Genericness is *measured* from the data —
  `name_frequency.common_words` counts company names, `common_email_domains` counts mapped
  businesses. The most common email domain in Osnabrück is `osnanet.de`, a local ISP no
  hand-written list would ever contain.
- **"A real business site" is not "THAT company's site."** Every German business site has an
  Impressum, a VAT number and a contact email at its own domain, so none of them identifies one
  company. That class of evidence may add score but must stay out of
  `confidence._VERIFYING_EVIDENCE`, or it lifts the 0.34 cap and promotes pages naming nobody — which
  is how 190 wrong websites got promoted and then removed by a model one at a time.

## Migrations

All schema changes go in `db/migrations/` as `V{N}__description.sql`. Forward-only and immutable
once applied. Flyway is a one-shot service under `--profile tools`; it does **not** run with
`docker compose up`. Run migrations before starting Airflow whenever schema changes land.

## Utility scripts

`scripts/ac_measurement.py` measures what the pipeline finds from a name and address alone, writing
nothing to the database and caching every stage. Method, model comparison and full numbers:
[docs/website-and-owner-discovery-results.md](docs/website-and-owner-discovery-results.md). On 100
companies: a **verified** website for 29%, an owner for 15%, ~1,850 input tokens each. Adding the
Handelsregister number makes it *worse* (21% vs 25%) — it pulls results toward register aggregators.

Three guard scripts, all free — no API, no model, no database, no network:

- `scripts/check_own_site_filter.py` — 21 real URLs, guarding `is_own_site_candidate`
- `scripts/check_imprint_owner_extraction.py` — 19 Impressum layouts guarding owner extraction
  (`scripts/collect_imprint_fixtures.py` rebuilds the corpus; its `shapes` output masks every letter
  and digit so layouts can be studied without exposing anyone's name)
- `scripts/check_combined_discovery.py` — URL guessing, the confidence ladder, the judge's limits and
  gate, what `_page_evidence` takes off a page, postcodes, and JSON-LD including malformed shapes

- `scripts/check_branch_classifier.py` — the compound-word rule, the keyword weights, the
  longest-prefix WZ lookup, and the services-page link finder

Run them after touching `search_common.py`, `imprint_probe.py`,
`url_discovery/imprint_extractor.py`, or anything in `combined_discovery/`.

`scripts/replay_judge_gate.py` belongs with them: equally free, and the only way to check a gate or
scoring change against real data without a full run. It re-runs the real gate (imported, never
restated) over `results.jsonl` and fails if the change loses a discovery **or a demotion**.

`scripts/prepare_company_import.py input.csv output.csv` converts a Northdata CSV export into a
clean import, repairing rows whose quoted `Gegenstand` field contains commas.

**Git Bash caveat:** `docker compose exec -e PYTHONPATH=/opt/airflow/dags` fails under Git Bash,
which rewrites the path to `C:/Program Files/Git/opt/airflow/dags` on the way into the container.
It surfaces as an unrelated `ModuleNotFoundError`. Use PowerShell, or prefix `MSYS_NO_PATHCONV=1`.
