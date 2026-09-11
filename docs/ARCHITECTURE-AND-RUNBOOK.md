# Architecture and runbook

**The front door. Start here.** This file answers four questions and nothing else: what the
architecture is, what the five pipelines are, which command does what, and which traps cost hours.

If you want to know *why* a decision was made — why a keyword was deleted, why the judge caps a
score instead of disqualifying a page, why Docker's Ollama was 74× slower than the native one — this
file links out. The reasoning lives in the Obsidian vault at
`C:\Users\TuPhan\TSB_Projects_vaults\unternhemensdatenbank`, which is **not in git**.

---

## 0. How to read this file

### 0.1 What this is, and what it is not

| | |
|---|---|
| **This file** | what and how. Architecture, commands, task index, gotchas. Committed, reviewed with the code. |
| **`CLAUDE.md`** | the rules a model must not break, plus a link here. |
| **`docs/*` detail files** | depth on one subject. Each linked below, each carrying its own status. |
| **The vault** | reasoning, measurement history, the arguments behind the numbers. Local only, not in git. |

### 0.2 THE CROSS-REFERENCE RULE

**This file points at enforceable truth. It does not restate it.** Read this before editing
anything below.

The precedent is `docs/core-queries-current.sql`, which opens with the same argument and is the only
doc in this repo that has not gone stale. Six clauses:

1. **Every number names the test constant that pins it, never the value.** Write "canonical websites
   per source: `CANONICAL_WEBSITES` in `tests/integration/test_ground_truth_discovery.py`" — never
   the count itself. A number in prose is a number nobody re-measures. Every count in this repo's
   documentation has gone stale at least once, and ten files are stale on exactly this.
2. **Every "how to run" names the command; `--help` is the contract.** This file groups the
   subcommands and says which need Docker. It does not copy their flags.
   **One live exception, recorded so it can be cited safely:** `run_combined_discovery.py`'s module
   docstring — which is what `--help` prints — still claims it "Writes NOTHING to the database".
   That stopped being true on 2026-09-08. `--help` is trustworthy for the subcommand list and the
   flags, and not yet for write behaviour. See §2.8.
3. **Every rule names the guard that enforces it.** "A keyword may not match inside a German
   compound — `scripts/check_branch_classifier.py`." A rule with no guard is a wish; say so.
4. **Every dated measurement carries its date, its n, and a link to the reasoning.** "29 verified
   websites per 100 companies, measured 11 Aug 2026 —
   [website-and-owner-discovery-results.md](website-and-owner-discovery-results.md)."
5. **Reasoning links out to the vault.** Do not summarise it here; a summary is a copy.
6. **When a number moves, the constant moves in the same commit as the change that moved it, with
   the reason in the message. Never edit a pinned number to make a red test green.**

> The test of any edit to this file: **does it add a pointer, or does it add a copy?** A copy will be
> wrong within a month.

### 0.3 Status of every other doc — read this before trusting one

Audited 2026-09-10 by reading all 32 files against the code. **Currency splits by author intent, not
by location:** the vault and the two ground-truth test modules were re-measured on 2026-09-09/10 and
are current. The repo `docs/` tree mostly stopped being maintained around 2026-08-26, and the
provider READMEs around 2026-08-11.

**CURRENT — trust these**

| file | covers |
|---|---|
| [guards-and-tests.md](guards-and-tests.md) | the 8 guards, the 17 tests, which 6 the edit hook runs |
| [core-queries-current.sql](core-queries-current.sql) | **the queries that work**, each naming the constant that pins it |
| [combined-discovery-verification.md](combined-discovery-verification.md) | why a page is judged to belong to a company (numbers scoped to a dated 1,000-company run) |
| [website-and-owner-discovery-results.md](website-and-owner-discovery-results.md) | a dated measurement, correctly scoped |
| [geospatial.md](geospatial.md) | optional Overpass + Nominatim |
| [README.md](README.md) | the docs index; it already flags two of its own entries |
| `../README.md` | project, stack, the priority table, the five pipelines. Two defects: its `flyway info` command cannot work (§6.7), and its quickstart stops at "unpause the DAGs you want" |
| `../scripts/archive/README.md` | the 8 kept-for-numbers scripts and the `parents[2]` fix they need |
| vault: `Confidence and Resolution`, `Data Model`, `Branch Classification`, `Combined Discovery`, `Repo Conventions and Gotchas`, `Local Dev Runbook`, `Website Discovery Runbook`, `Local Model Setup`, `Discovery DAGs`, `URL Guessing`, `Unternehmensdatenbank` | the real documentation, 11 of 13 notes current |

**STALE — the file says so itself, so it is safe**

| file | why |
|---|---|
| [core-queries.sql](core-queries.sql) | ⚠️ **DANGEROUS, labelled.** Written against V1–V8. Its **query 3 inserts a source at priority 10**, which would outrank every real source and re-order the resolver for every field. Read queries 4–35; run nothing |
| [source-enrichment-dags.md](source-enrichment-dags.md) | names Mistral and Claude as the active DAGs; both have never written a row. Superseded by `../airflow/dags/README.md` |
| vault `Search Provider Comparison` | banner says HISTORICAL — its benchmark script is deleted and its truth pool is now the answer key |
| vault `Enrichment Cascade Spike` | banner says both scripts it names are gone |

**STALE — no warning on the file. These will mislead you.**

| file | the specific wrong claim |
|---|---|
| `../airflow/dags/README.md` | states the `resolve_ceo_contact` manager-duplication bug as **live**. **V19 fixed it** and a test now asserts total == distinct. Its coordinator field-alias table has 6 rows and never mentions `technologiefeld` / `purpose_field` / V20 |
| [data-model.md](data-model.md) | ⚠️ **also dangerous.** Uses the observation name `website_url` at :28, :184, :217 — see §6.4, a query on that name returns zero rows rather than an error. Says nothing reads `public.northdata_export` (`import_northdata_websites.py` does). Migration list stops before V15. Points at `core-queries.sql` as "the canonical query catalog" |
| [running-discovery.md](running-discovery.md) | "checked 11 August 2026". Says Brave has no key; Ollama is 0.38 tok/s; the best quality score is 0.700; one run is 50 companies. **Never mentions P3 at all.** Its §3 view guide and §4 query cookbook are still useful |
| [running-branch-classification.md](running-branch-classification.md) | every expected-output block predates Tier B. Its `classify-purpose` block omits two outcomes that now hold over 1,300 rows. Its command sequence is still correct |
| [branch-classification.md](branch-classification.md) | prints the pre-2026-09-08 buckets and `decided_by keywords 346, model 143`. Live is `keywords 403, model 0`. **Vault `Branch Classification` is the current version of this file** |
| [purpose-classification.md](purpose-classification.md) | says `assigned_model_77` is `0`, "not run". Tier B has since run |
| [berlin-gewerbedaten-pipeline.md](berlin-gewerbedaten-pipeline.md) | links `/Users/jonathanhaudenschild/…`, another machine's absolute path |
| `../airflow/dags/brave_search_discovery/README.md` | "Never executed against the live API… no `BRAVE_API_KEY` has ever existed here." Brave is now the **best-performing paid provider** by canonical websites |
| `../airflow/dags/exa_search_discovery/README.md` | "Never executed against the live API." Exa has run and is the one provider with a measured per-company cost |
| `../airflow/dags/ollama_search_discovery/README.md` | describes kununu/linkedin extraction; never says the DAG has written **zero** rows because SearXNG's upstream engines CAPTCHA this network |
| `../airflow/dags/mistral_search_discovery/README.md`, `claude_search_discovery/README.md`, `berlin_gewerbedaten/README.md` | 7-line stubs whose only link is a foreign absolute path; none says its DAG is dead |
| `../airflow/data/README.md` | says the directory holds Berlin Gewerbedaten files. It now holds every checkpoint, `results.jsonl`, both classification JSONLs, the page cache and the handover CSV |

Fixing those is tracked in §5.7.

### 0.4 Vocabulary, once

Defined before use, because this codebase invented most of these words.

- **Observation** — one claim by one source about one field of one company. "`website` =
  `example.de`, from `exa_search_discovery`, confidence 0.65." Sources only ever write observations.
  They never overwrite each other.
- **Canonical** — the one observation that won for a field, and therefore the answer the application
  sees. Only `pipeline_coordinator` decides this.
- **Confidence** — a number from 0 to 1 on an observation. Stored as `NUMERIC(5,4)`.
- **Resolver** — the code inside `pipeline_coordinator` that picks the winner per field.
- **Checkpoint** — a JSON file under `airflow/data/` holding where a run stopped, so re-triggering
  continues instead of restarting.
- **Impressum** — the legal contact page every German business website must have. It usually carries
  the address, the managing director's name and the company's court-register number, which makes it
  the one page that can *prove* a site belongs to a specific company.
- **Technologiefeld** — one of Berlin's 77 technology and application fields. A controlled
  vocabulary. Not the same thing as `tech_field`; see §1.7, which exists because confusing them was
  the most expensive mistake in this project.

---

## 1. Architecture overview

### 1.1 What this is

A local development scaffold for a Postgres-backed German company-data platform. Airflow
orchestrates, Flyway owns all schema, Python DAGs write with psycopg2. Two databases on one server:
`airflow` (metadata) and `unternehmensdatenbank` (application data).

`public.companies` holds a Northdata export of the **Osnabrück** region — mostly name and address.
The whole active effort is filling the missing fields cheaply and traceably.

**Osnabrück is the benchmark, not the target.** The real data is Berlin, arriving as another
Northdata CSV, and every count in this repo is an Osnabrück measurement that Berlin replaces
wholesale. Over half the table is `erloschen` — dissolved — which is worth knowing before reading any
coverage figure. Counts and the register-status split: `TOTAL_COMPANIES` in both test modules, and
the `register_status` query in [core-queries-current.sql](core-queries-current.sql).

Postgres 16 (host port 5433), Airflow 3.2.0 (Airflow 3 API — `airflow.providers.standard`, **not**
`airflow.operators`), Flyway 12, Python 3.12.

### 1.2 The data model in one line

> `companies` is the answer. `company_field_observations` is the proof.

Several sources can disagree about a company's website. The design refuses to lose any of them: each
claim is stored separately, one claim per field wins, and the winner is recorded along with *why*.

```
companies                              identity + pipeline metadata only
  └─ company_source_records            one raw row per (source_id, external_id)
       └─ company_field_observations   ONE ROW = ONE CLAIM ABOUT ONE FIELD
            └─ company_canonical_field_sources    which observation won, from which source
```

Supporting tables: `public.sources` (lower `priority` wins), `public.source_sync_runs` (the import
logbook — `partial` is the *normal* outcome of a quota-limited provider run), `public.company_managers`.

**`v_companies_enriched` is the view applications should query.** It exposes `website_url`,
`cluster`, `tech_field`, `revenue`, `employee_count` and the two V20 industry columns as view
columns resolved from the observations. `companies` does **not** store enriched values — V9 dropped
those columns.

Depth: vault `Data Model` (current) · [data-model.md](data-model.md) (**stale**, see §0.3).

### 1.3 The five pipelines

**They are independent, not a waterfall.** Each has its own source key, its own record of which
companies it has handled, and its own checkpoint. Running one does not stop another working the same
company. They meet in exactly one place: `pipeline_coordinator`.

| | pipeline | source key | priority | cost | writes | triggers the coordinator? |
|---|---|---|---|---|---|---|
| **P1** | the register export | `northdata_export` | **50** | free | `website` observations | **no — the only manual trigger left** |
| **P2** | paid search + Impressum check | `brave_` / `exa_` / `parallel_search_discovery` | 100 | **money** | `website`, `candidate_url`, `directory_url`, `phone`, `email`, `ceo_name_or_contact`, `cluster`, `tech_field` | yes, `TriggerDagRunOperator` |
| **P3** | combined discovery — **a script, not a DAG** | `combined_discovery` | 100 | free | `website`, `candidate_url`, `directory_url`, `discovery_tier`, `search_query`, `osm_id` | yes, calls it directly |
| **P4** | url_discovery — name → domain, in Airflow | `url_discovery` | 100 | free | `candidate_url`, `directory_url`, `website`, `phone`, `email`, `ceo_name_or_contact` | yes |
| **P5** | the 77-field Berlin taxonomy | `branch_classifier` | 100 | free | `technologiefeld`, `purpose_field`, and their two `_withheld` names | yes, unless `--skip-coordinator` |

Per-source canonical website counts: `CANONICAL_WEBSITES` and `CANONICAL_TOTAL` in
`tests/integration/test_ground_truth_discovery.py`. Priorities: `SOURCE_PRIORITIES` in the same file.

**Not deployed.** `mistral_search_discovery`, `claude_search_discovery` and
`ollama_search_discovery` exist as code and have **never written a row** — they have no entry in the
`sources` table at all. Mistral and Claude use the provider's own server-side search, so they cannot
share P2's runner. Ollama pairs a local SearXNG with a local model, and SearXNG's upstream engines
serve a CAPTCHA to this network. `osm_discovery` needs a local Overpass holding Berlin only; P3's
`osm` stage covers the same ground for Osnabrück. Do not count any of them as a pipeline.

Depth: vault `Discovery DAGs` · `../airflow/dags/README.md` (**stale** on one point, see §0.3).

### 1.4 The one diagram — where each artefact lives

No other diagram in this project shows the file/database split, and that split is the thing people
get wrong.

```
 SOURCES                             WRITES OBSERVATIONS         PUBLISHED BY
 ──────────────────────────────────  ─────────────────────────   ────────────────────────
 P1  northdata_export      prio  50 ──┐
     register CSV · free               │
 P2  brave / exa / parallel prio 100 ──┤
     search API · PAID                 │   company_field_         pipeline_coordinator
 P3  combined_discovery     prio 100 ──┤   observations       ──► the ONLY thing that
     script · free · local model       │   [ POSTGRES ]           picks a winner
 P4  url_discovery          prio 100 ──┤                                │
     name → domain · free              │                                ▼
 P5  branch_classifier      prio 100 ──┘                          company_canonical_
     77-field taxonomy · free                                     field_sources
                                                                        │
                                                                        ▼
 FILES  [ all gitignored — they carry managers' names ]            v_companies_enriched
 ─────────────────────────────────────────────                    [ what the app and
 airflow/data/combined_discovery/                                   Hasura read ]
   results.jsonl            ← run              ─── replay ──►
   branches.jsonl           ← classify         ── import_branch ──►
   purpose_branches.jsonl   ← classify-purpose ──  _classification ──►
   pages/…                  ← fetch-content    (the page cache)
   combined_discovery_urls.csv ← csv  [ url = P3's own find
                                        db_website = what the DB serves — §6.2 ]
 airflow/data/
   <source_key>_checkpoint.json  ← every P2 / P4 run

 Both classification files reach Postgres through
 scripts/import_branch_classification.py -> branch_observations.py, and both
 columns are PUBLISHED: the emitter's row counts are pinned as
 BRANCH_CLASSIFIER_OBSERVATIONS and what the view serves as
 CANONICAL_OTHER_FIELDS, both in test_ground_truth_discovery.py. The two
 _withheld field names are written and deliberately never resolved. See §5.1.
```

`airflow/data/**` is gitignored **on purpose**: the register data carries managing-director names,
which are personal data. Never force-add it. Same for `output/` and `*.csv` repo-wide.

### 1.5 Priority is absolute

The resolver orders candidates by `sources.priority` ascending, then by confidence plus a
cross-source agreement bonus descending, then by the value as a tiebreak.

**Priority is sorted before confidence, so it is absolute.** Nothing at 100 beats anything at 50, at
any confidence. That is why the register export's URLs — which nobody has ever fetched — outrank
pages that were downloaded and register-checked.

**This is intended.** A URL a company filed with the register is better evidence than a domain
guessed from its name. Two consequences worth holding on to:

- **Among the six sources at 100, confidence alone decides**, which is what keeps P2's provider
  comparison fair.
- **The undo is one `UPDATE`**, because the resolver only un-canonicalises a displaced observation
  and never deletes it:
  `UPDATE public.sources SET priority = 200 WHERE key = 'northdata_export';` plus one coordinator
  run. `import_northdata_websites.py --dry-run` prints exactly what would move.

Depth: vault `Confidence and Resolution` — **read it before changing any threshold, weight or field
name.** Several of its rules exist because a specific wrong value reached `companies` in production
data.

### 1.6 `pipeline_coordinator` is the publish switch

**Nothing is visible to the application until the coordinator runs.** Source pipelines write
observations and nothing else; only the coordinator may write `is_canonical`,
`company_canonical_field_sources` or `companies`.

Who triggers it, and this is scattered across three files everywhere else:

| | |
|---|---|
| P2, P4 | automatically, `TriggerDagRunOperator` at the end of the DAG |
| P3 | automatically, a **direct call** to `run_pipeline_coordinator` — no Airflow needed, and therefore no `dag_run` row in the UI. `--skip-coordinator` opts out |
| `import_combined_discovery.py` | automatically, same direct call |
| **P1's `import_northdata_websites.py`** | **no. This is the one step that still needs a manual trigger** |
| **a hand-run DAG** (`docker compose exec … python -c "…"`) | **no.** It bypasses the operator, so nothing becomes canonical. Trigger it yourself afterwards |
| a **failed** task | **no.** A failed task never reaches the trigger. After any failure, check a coordinator run has happened before trusting a count |

Safe to run by hand at any time, and idempotent since V19.

### 1.7 Five industry-ish columns, and the naming hazard

`v_companies_enriched` carries five columns that look like the same question and are not. **Merging
or coalescing any two of them was the most expensive mistake in this project.**

| column | the question | filled from | vocabulary | needs a website? |
|---|---|---|---|---|
| `tech_field` | — | whatever a search-provider LLM typed | **none** — free text | no |
| `cluster` | — | same | **none** — free text | no |
| `technologiefeld` | what field does it **work in**? | words the company published | one of Berlin's **77**, or `NONE` | **yes** — no site, no field |
| `wz_code` | what is it **registered as**? | its official WZ code | 76 divisions in 20 sections | no |
| `purpose_field` | what was it **set up to do**? | `business_purpose`, the register | one of the **same 77**, or `NONE` | **no — that is its point** |

Four rules, each of which has already been broken once:

1. **A WZ code may never fill the Technologiefeld.** Mapping one onto the other gave thousands of
   companies a field nothing they published supported. `classify()` has no WZ parameter at all, and
   `scripts/check_branch_classifier.py` fails if one reappears.
2. **`technologiefeld` and `purpose_field` are never a fallback for each other.** They answer
   different questions. Where both are filled they agree less than half the time — which is a finding
   about the companies, not an error.
3. **`NONE` is a value, not a blank**, and its reasons are counted apart. "We had no text to read" is
   not evidence that a company has no technology field. Never add two reason buckets together.
4. **`assigned` does not mean a model assigned it.** `outcome` says whether there is a field;
   `decided_by` says who.

Counts and buckets: `BRANCH_OUTCOMES`, `PURPOSE_OUTCOMES` in
`tests/integration/test_ground_truth_columns.py`. Depth: vault `Branch Classification` (current) ·
[branch-classification.md](branch-classification.md) and
[purpose-classification.md](purpose-classification.md) (**both stale**, see §0.3).

---

## 2. End-to-end runbook

Fresh database to results, in order. **The shell is PowerShell**, from the repo root. Every
long-running command should be run bare in a terminal — see §6.12.

### 2.0 Prerequisites — bring the stack up

Order matters: **Postgres → Flyway → `airflow-init` → Airflow services.** Starting Airflow against
an unmigrated database is the classic mistake; it surfaces as
`column c.data_quality_score does not exist`.

```powershell
cp .env.example .env          # first time only
./scripts/dev-up.sh           # does all four, in order
```

`dev-up.sh`'s closing banner prints **port 8080, which is wrong** — see §6.6.

Manual equivalent:

```powershell
docker compose up -d postgres
docker compose --profile tools run --rm flyway      # migrate. Takes NO arguments — see §6.7
docker compose up airflow-init
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

| service | URL | credentials |
|---|---|---|
| **Airflow UI** | **http://localhost:8082** (`AIRFLOW_PORT`) | `airflow` / `airflow` |
| pgAdmin | http://localhost:5051 | `admin@example.com` / `admin` |
| Postgres | `localhost:5433` → container `5432` | db `unternehmensdatenbank`, `app` / `app` |
| SearXNG | http://localhost:8888 | — |
| Overpass | http://localhost:12345/api/interpreter | profile `geo` |
| Nominatim | http://localhost:8081/reverse | profile `geocoding` |

Depth: vault `Local Dev Runbook`.

### 2.1 Step 0 — get companies into the table

**This is the prerequisite with no script, and it is documented nowhere else except a stale file.**

```powershell
python scripts/prepare_company_import.py input.csv output.csv
```

It converts a Northdata export, repairing rows whose quoted `Gegenstand` contains commas, and fixing
double-encoded umlaut mojibake. Its output columns, in order:

```
company_name  normalized_name  registration_number  legal_form  business_purpose
wz_code  address  established_at  postcode  city  register_status  metadata_json
```

**There is no loader script in this repo — the prepared CSV goes into Postgres by hand, and the
`\copy` column list MUST map `register_status`.** The discovery filter is `= 'aktiv'`, which excludes
`NULL`, so if you leave that column out every row lands NULL and an active-only run finds nothing to
do at all. `prepare_company_import.py:56-58` carries the same warning at the column definition.

### 2.2 Step 1 — P1, the register export. Free, and the biggest single win.

Only needed when a new export lands. Osnabrück is already imported.

```powershell
python scripts/import_northdata_export.py "C:/path/to/export.jsonl"   # land it in public.northdata_export
python scripts/import_northdata_websites.py --dry-run                 # ** READ THIS OUTPUT **
python scripts/import_northdata_websites.py                           # write website observations
```

Then **trigger `pipeline_coordinator` in the Airflow UI once.** This is the one step that does not
trigger it itself (§1.6).

**The dry run is not a formality.** It prints the list of canonical websites the import is about to
**replace** — the export sits at priority 50 and wins every contest it enters (§1.5) — and that list
is your only chance to see them. When it prints `0 pure gain` and `no canonical website would
change`, the export is already applied.

⚠️ **These URLs are nobody's evidence.** Nothing fetched them. Filter on `verified` in
`v_website_provenance`, not on confidence — the export's flat confidence sits *above* several
genuinely verified discovery findings, so ordering by confidence mixes them back together.

`import_northdata_websites.py` is the only thing that reads the export's `website` column. Note the
export's `status` is English (`active`) where `register_status` is German (`aktiv`), and the website
lives at `extras[].items[]` with `id='url'`, not as a top-level field.

Depth: vault `Website Discovery Runbook`.

### 2.3 Step 2 — P3, combined discovery. Free.

Guess ~20 domains from the name, resolve them by DNS, fetch what answers, match OpenStreetMap, and
let a **local** model read the pages and say whose they are. No API money, nothing leaves the machine.

**Set the model first. This is not optional — see §6.1.**

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"
$env:DISCOVERY_SKIP_RESOLVED_WEBSITE = "true"     # or you re-discover what you already have
python scripts/run_combined_discovery.py run --limit 100 --stages url,osm,judge    # try small first
python scripts/run_combined_discovery.py run --limit 20000 --stages url,osm,judge  # any limit above
                                                                # the table size is a full pass
```

Stages: `url` (domain guessing), `osm` (the city snapshot), `llm` (**dead here** — it searches via
SearXNG, which is CAPTCHA-blocked on this network), `judge` (a local model **reads** pages the others
already downloaded; needs no search engine). `llm` and `judge` are constantly confused. **Only
`judge` is worth switching on.**

`run` **writes observations to Postgres itself and then triggers the coordinator.** There is no
separate import step. `--skip-coordinator` suppresses the trigger when you are only measuring.

Resumable: one JSON line per company in `results.jsonl`, so a re-run continues. `--force` re-runs the
whole sample. The judge is ~31 s a call and is roughly two-thirds of a run's wall clock; everything
else is under a second a company once the cache is warm.

**`run --limit` *is* a prefix.** The sample is `ORDER BY md5(id::text) LIMIT n` — the same companies
every time — so `--limit 50` is the first 50 of `--limit 1000` and a second run measures the same
population. This is the opposite of `classify --limit`; see §6.14 for why.

**What to check:** the log line naming how many companies were withheld for already having a website.
If it says 0 and you expected thousands, the environment variable did not reach the script — host
scripts never read `.env`, which configures the containers only.

Depth: vault `Combined Discovery`, `URL Guessing`, `Local Model Setup` ·
[combined-discovery-verification.md](combined-discovery-verification.md) — **read that one before
touching `confidence.py`, the judge gate, or anything that reads a page.**

### 2.4 Step 3 — P4, `url_discovery`. Free, in Airflow.

**A DAG needs two switches** (§6.8): its `ENABLE_*` flag in `.env`, *and* unpausing in Airflow.

```powershell
# 1. in .env
ENABLE_URL_DISCOVERY=true

# 2. recreate the containers — env is baked in at start, a restart is not enough
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer

# 3. unpause and trigger
docker compose exec airflow-scheduler airflow dags unpause url_discovery
docker compose exec airflow-scheduler airflow dags trigger url_discovery
```

One run handles `SOURCE_ENRICHMENT_BATCH_SIZE` companies (default 50) and checkpoints to
`airflow/data/url_discovery_checkpoint.json`. Trigger again for the next batch.

Optional and worth it on a full pass: `DISCOVERY_ACTIVE_ONLY=true` skips dissolved companies, which
removes more than half the work for almost no loss. **It is not a subset** — see §6.15.

### 2.5 Step 4 — P2, the paid providers. The only step that costs money.

Same two switches. Brave has the best measured result of the three; Exa is the only one that reports
its own cost; Parallel's anonymous access is a hard quota of roughly 20 searches per process, not a
rate limit.

```powershell
# in .env
ENABLE_BRAVE_SEARCH_DISCOVERY=true        # requires BRAVE_API_KEY
DISCOVERY_SKIP_RESOLVED_WEBSITE=true      # do not pay to re-find what you have
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
docker compose exec airflow-scheduler airflow dags unpause brave_search_discovery
docker compose exec airflow-scheduler airflow dags trigger brave_search_discovery
```

**A new provider is refused until somebody has watched it work.** `ProviderSpec.smoke_required`
defaults to `True` and the task will not start until `airflow/data/<source_key>_smoke_ok.json`
exists. Write it by running `scripts/smoke_<provider>_client.py` on the host. The reason: a wrong
request shape does not raise — it returns an empty result list, which reads exactly like "the search
found nothing". That failure hid in the Parallel REST client for weeks.

**Watch the run from the log file, not the UI.** The DAG-run detail page holds a database
transaction open per request and about fifteen loads of it exhaust the connection pool; task
heartbeats then time out and the supervisor **kills your running task with no traceback**.

```powershell
Get-Content -Wait "airflow\logs\dag_id=brave_search_discovery\run_id=<the run id>\task_id=run_brave_search_discovery\attempt=1.log"
```

Two more things about a paid run, both of which cost real time once. **The `[1/N]` in the log is not
the batch size** — it is how many companies the query returned, which the batch size only trims, so a
small N means the pool shrank rather than something breaking. And **a `402 Payment Required` does not
stop the run**; let it finish anyway, because the checkpoint is deleted only on completion and
deleting it is what returns the failed companies to the pool.

Depth: vault `Discovery DAGs`, `Website Discovery Runbook`, and `Search Provider Comparison`
(historical, but its sizing table and "report two numbers, never one" still hold).

### 2.6 Step 5 — the classification columns

Postgres must be up: both `classify` and `classify-purpose` read `public.companies`, and `classify`
also reads the verified-website gate. Neither makes a web request or a model call without `--llm`.

```powershell
python scripts/check_branch_classifier.py                                # free, always first
python scripts/check_purpose_branch.py                                   # free, column 3

python scripts/run_combined_discovery.py fetch-content                   # once. THE ONLY DOWNLOADER
python scripts/run_combined_discovery.py classify --source page,purpose   # ~2 min, no network
python scripts/run_combined_discovery.py classify-report

python scripts/run_combined_discovery.py classify-purpose                 # seconds, no network
python scripts/run_combined_discovery.py classify-purpose-report

python scripts/run_combined_discovery.py compare-columns                  # all three side by side
```

`--source` accepts `page` and `purpose`. **`--source wz` is deliberately refused** (§1.7 rule 1).

`fetch-content` reads the homepages already in the cache, finds their links to `/leistungen`,
`/ueber-uns`, `/produkte` and similar, and downloads them under a cap per category and per company.
Running it again downloads nothing — everything is cached. `--limit 100` means 100 companies *that
have a website*, not the first 100 of the table.

`classify` deletes and rewrites `branches.jsonl` on every run: it is a full re-score, not an
increment, because every keyword weight may have moved. That is also why **`--limit` is not a
subset** — see §6.14.

⚠️ **These two commands stop at the file, and that is the whole design.** Neither writes to Postgres.
Getting the answers into `v_companies_enriched.technologiefeld` and `.purpose_field` is a separate
step, `scripts/import_branch_classification.py` — §3's task index and §5.1. Both columns are
published today, so a reader wanting the values should query the view; the JSONL files are where you
go to ask *why* a company got the field it did.

Depth: vault `Branch Classification` · [running-branch-classification.md](running-branch-classification.md)
(**stale numbers, correct command order**).

### 2.7 Step 6 — verify

```powershell
python scripts/check_own_site_filter.py
python scripts/check_imprint_owner_extraction.py
python scripts/check_combined_discovery.py
python scripts/check_branch_classifier.py
python scripts/check_purpose_branch.py
python scripts/check_osm_match.py
python scripts/check_branch_observations.py
python scripts/replay_judge_gate.py

pytest -q                        # 17 tests; the discovery module needs Postgres up
```

All eight guards are free — no API, no model, no database, no network — and take a second or two each.
**Neither guards nor tests check whether the answers are correct**; nothing in this repo does, because
not one row of the three industry columns has ever been hand-labelled. A failure means "these numbers
moved, find out why".

Depth: [guards-and-tests.md](guards-and-tests.md).

### 2.8 Which steps write to the database, and which write files

| command | Postgres | files |
|---|---|---|
| `import_northdata_export.py` | **writes** `public.northdata_export` | — |
| `import_northdata_websites.py` | **writes** observations at priority 50 | — |
| P2 / P4 DAGs | **write** observations, then trigger the coordinator | the checkpoint JSON |
| `run_combined_discovery.py run` | **writes** observations, then triggers the coordinator | `results.jsonl` |
| `import_combined_discovery.py` | **writes** observations, then triggers the coordinator | — |
| `classify` | **reads** — `public.companies` plus the verified-website gate | `branches.jsonl` |
| `classify-purpose` | **reads** — `public.companies.business_purpose` | `purpose_branches.jsonl` |
| `fetch-content` | nothing | the page cache. **The only step that downloads** |
| `report`, `csv`, `*-report`, `*-labels`, `*-labels-report`, `compare-columns` | nothing | their own output |
| `pipeline_coordinator` | **writes** `is_canonical`, `company_canonical_field_sources`, `companies` | — |

Two corrections this table records, because both are wrong elsewhere in the repo:

1. **`classify-purpose` reads Postgres.** `CLAUDE.md`'s subcommand table lists it under "nothing,
   still free and offline". It calls `load_company_facts()` at `run_combined_discovery.py:2355`,
   which opens a psycopg2 connection. What is true — and what got mis-stated as "offline" — is that
   it makes no web request and no model call without `--llm`.
2. **"Offline" and "free" are different claims.** `report`, `csv`, `fetch-content` and the label
   commands need no database at all. `classify` and `classify-purpose` are free, need no network and
   no model, and still cannot start without Postgres.

The `run_combined_discovery.py` module docstring — printed by `--help` — still says the script
"Writes NOTHING to the database… Storing results is the DAGs' job." That has been false since
2026-09-08. Tracked in §5.7.

---

## 3. Task index — "I want to X, run Y"

### 3.1 The table

| I want to… | run | writes | needs |
|---|---|---|---|
| load a new city's companies | `prepare_company_import.py in.csv out.csv`, then a hand-written `\copy` — **§2.1**, and it must map `register_status` | `public.companies` | Postgres |
| land a new register export | `import_northdata_export.py <export.jsonl>` (`--dry-run` available) | `public.northdata_export` | Postgres |
| publish the export's websites | `import_northdata_websites.py --dry-run` **first**, then without | observations | Postgres, **then a manual coordinator trigger** |
| convert an export to a spreadsheet | `northdata_jsonl_to_table.py <file> -o out.xlsx` | a file | — |
| discover websites, free | `run_combined_discovery.py run --stages url,osm,judge` | `results.jsonl` + observations + resolves | Postgres, Ollama, `OLLAMA_MODEL` |
| discover websites, paid | unpause `brave_search_discovery` — **§2.5** | observations + resolves | Postgres, Airflow, `BRAVE_API_KEY` |
| discover websites, free, in Airflow | unpause `url_discovery` — **§2.4** | observations + resolves | Postgres, Airflow |
| re-import an existing `results.jsonl` without re-running discovery | `import_combined_discovery.py` (`--dry-run`, `--limit`, `--skip-coordinator`) | observations + resolves | Postgres |
| download the `/leistungen` and `/ueber-uns` pages | `run_combined_discovery.py fetch-content` | the page cache | network. **The only downloader** |
| compute tech fields | `classify --source page,purpose` **and** `classify-purpose` | `branches.jsonl`, `purpose_branches.jsonl` | Postgres |
| read the tech-field scorecards | `classify-report`, `classify-purpose-report` | — | the files |
| compare all three industry columns | `compare-columns` | — | both files |
| get a handover CSV | `run_combined_discovery.py csv` | `combined_discovery_urls.csv` | Postgres. **Read `db_website`, not `url` — §6.2** |
| find out whether any of it is *right* | `classify-labels` / `classify-purpose-labels`, fill in `label`, then the matching `*-labels-report` | the worksheets | — |
| ask the model what unclassified companies call themselves | `propose-keywords` | a worksheet only | Ollama |
| sync tech fields to Postgres | `import_branch_classification.py` (`--dry-run`, `--limit`, `--skip-coordinator`) | observations, and resolves unless skipped | Postgres |
| rebuild the Berlin taxonomy from a new spreadsheet | `import_branch_taxonomy.py <csv> --out C:\temp\new.yaml` — **never overwrite the live file**, it carries hand edits and comments | a YAML to diff | — |
| rebuild the WZ titles | `import_wz_titles.py <wz.xlsx>` | `wz_titles.yaml` | — |
| verify nothing broke | the 8 guards, then `pytest -q` — **§2.7** | — | Postgres for 10 of the 17 tests |
| query the database | [core-queries-current.sql](core-queries-current.sql). **Never `core-queries.sql`** — §0.3 | — | Postgres |
| resolve observations by hand | trigger `pipeline_coordinator` in the UI, or call `run_pipeline_coordinator` directly | canonical fields | Postgres |
| reset one provider's data | `reset_provider_observations.sql` — **and delete the checkpoint JSON, which it does not clear. §6.13** | deletes rows | Postgres |
| prove a new search provider works | `smoke_brave_client.py` / `smoke_exa_client.py` on the host | the `_smoke_ok.json` marker | the provider's key |
| bring the stack up | `./scripts/dev-up.sh` | — | Docker |

### 3.2 The script census — 19 files in `scripts/`

| group | files |
|---|---|
| **importers and runners (8)** | `run_combined_discovery.py` · `import_combined_discovery.py` · `import_northdata_export.py` · `import_northdata_websites.py` · `import_branch_taxonomy.py` · `import_wz_titles.py` · `prepare_company_import.py` · `northdata_jsonl_to_table.py` |
| **guards (7)** — free, no API, no model, no DB, no network | `check_own_site_filter.py` · `check_imprint_owner_extraction.py` · `check_combined_discovery.py` · `check_branch_classifier.py` · `check_purpose_branch.py` · `check_osm_match.py` · `replay_judge_gate.py` |
| **smoke tests (2)** | `smoke_brave_client.py` · `smoke_exa_client.py` |
| **other (2)** | `dev-up.sh` · `reset_provider_observations.sql` |

Plus **8 in `scripts/archive/`**, kept for their numbers rather than for running. ⚠️ **Each computes
the repo root one level above itself, which resolves to `scripts/` from there — fix that to
`parents[2]` before running any**, or it fails on import or, worse, writes output into the wrong
directory. Any doc giving a bare `python scripts/<name>.py` for one of these is stale; the path needs
`archive/`. See `../scripts/archive/README.md`.

### 3.3 The 16 `run_combined_discovery.py` subcommands

Grouped. **`--help` is the contract for the flags; this is only the grouping.**

| group | subcommands |
|---|---|
| **do work** | `run` · `fetch-content` · `classify` · `classify-purpose` · `propose-keywords` |
| **report** | `report` · `classify-report` · `classify-purpose-report` · `compare-columns` |
| **write a labelling worksheet** | `labels` · `classify-labels` · `classify-purpose-labels` |
| **read a worksheet back** — the only things that measure precision | `labels-report` · `classify-labels-report` · `classify-purpose-labels-report` |
| **hand over** | `csv` |

Flags worth knowing before you read `--help`:

- `run --stages url,osm,llm,judge` (default `url,osm`) · `--force` · `--skip-coordinator` ·
  `--workers` · `--refresh-osm`
- `classify --source page,purpose` (default both; `wz` refused) · `--llm`
- `classify-purpose --tier tiebreak|semantic` (comma-separate for both) · `--llm` ·
  `--llm-limit N` (caps **new** calls; cached answers are free and do not count) ·
  `--company-id <prefix>` (repeatable — spends calls on named companies, jumping the queue, because
  the queue is `company_id` order while the handover CSV sorts by confidence)

---

## 4. Current state snapshot

**There are no counts in this section, deliberately.** Every number in this repo's documentation has
gone stale at least once; ten files are stale on exactly that. Each row below names the constant that
**fails a test** when the number moves. If a query disagrees with its constant, run the test — it
will tell you which of the two is wrong.

### 4.1 Where each number is pinned

`tests/integration/test_ground_truth_discovery.py` — 10 tests, **needs Postgres**:

| what | constant |
|---|---|
| companies in the table | `TOTAL_COMPANIES` |
| canonical websites per source, and the total | `CANONICAL_WEBSITES`, `CANONICAL_TOTAL` |
| source priorities — the thing that decides who wins | `SOURCE_PRIORITIES` |
| the `candidate_url` → `website` alias holding | `CANONICAL_BY_OBSERVATION_NAME`, `CANONICAL_BRAVE_SPLIT` |
| the other canonical fields | `CANONICAL_OTHER_FIELDS` |
| website-ish observations | `WEBSITE_OBSERVATION_FLOOR` — **a floor, not an equality.** Observations are never deleted, only un-canonicalised, so it may rise and **must never fall** |
| managers, total == distinct | `MANAGER_DISTINCT` |
| the quality-score ceiling and the spend guard | `test_spend_guard` |
| V20's six view columns present, and empty | `V20_VIEW_COLUMNS`, `test_v20_columns_exist_and_are_empty` |

`tests/integration/test_ground_truth_columns.py` — 7 tests, free, in the edit hook:

| what | constant |
|---|---|
| column 1's five outcome buckets, summing to the table | `BRANCH_OUTCOMES` |
| what the website gate withheld | `BRANCH_WITHHELD`, `BRANCH_WITH_WEBSITE` |
| column 3's seven outcome buckets, summing to the table | `PURPOSE_OUTCOMES` |
| column 3's total fields, and what the length floor cost | `PURPOSE_FIELDS`, `PURPOSE_WITHHELD_BY_FLOOR` |
| both columns scored against the same taxonomy | `TAXONOMY_VERSION` |
| `branches.jsonl` not stale against `results.jsonl` | `test_column1_not_stale` |

**Count canonical websites *through* `company_canonical_field_sources`, never by grouping on the
observation's own `field_name`.** The resolver aliases `website ← [candidate_url, website]`, so a
company whose winning claim is stored as `candidate_url` still has a canonical website. Grouping on
the observation name splits one provider's total into two and reads like lost websites; the first
version of that baseline was wrong for exactly this reason.

### 4.2 How to re-measure

```powershell
pytest -q                                                     # everything
python tests/integration/test_ground_truth_columns.py         # standalone, no pytest, no Docker
python tests/integration/test_ground_truth_discovery.py       # standalone, needs Docker
python scripts/run_combined_discovery.py classify-report       # column 1 and 2 buckets
python scripts/run_combined_discovery.py classify-purpose-report  # column 3 buckets
```

Both test modules run **two ways and both must keep working**: under pytest, and as a plain script.
**pytest is host-only and deliberately absent from `airflow/requirements.txt`**, which is why
`import pytest` is optional and a built-in runner takes over when it is missing — it keeps the checks
dependency-free and able to run inside the Airflow image. Do not "simplify" that by adding the
dependency.

Per-source SQL and the four resolver invariants: [core-queries-current.sql](core-queries-current.sql).

### 4.3 What is not known — kept honest

- **No row of any of the three industry columns has ever been hand-labelled**, so **no precision
  figure exists for any of them**. `classify-labels` / `classify-purpose-labels` and their
  `*-labels-report` commands are the only things that would produce one, and they are the cheapest
  unstarted task in the project.
- **Not one of the register export's websites has ever been fetched.** They carry `verified: false`,
  and nothing has measured them. So the headline website count means one confident number and one
  hopeful one — filter on `verified`.
- **Nobody has established the discovery ceiling.** We cannot distinguish "we failed to find it"
  from "this company has no website", which makes every coverage number uninterpretable in absolute
  terms. Settling it needs ~35 companies labelled by hand.
- **A labelling worksheet exists for two of the three columns** and holds a small number of labels.
  Fifty hand labels are a sanity check, not a precision measurement — the confidence interval on 50
  rows is about ±14 points.

---

## 5. Known gaps and deferred work

Decided, not forgotten. Each for a stated reason.

### 5.1 ✅ The tech-field write path — landed and published

**Done, and kept here because §5 is where a reader looks for it.** Both industry columns are served
by `v_companies_enriched`; the counts are `CANONICAL_OTHER_FIELDS` and the emitter's row counts are
`BRANCH_CLASSIFIER_OBSERVATIONS`, both in `tests/integration/test_ground_truth_discovery.py`. Do not
copy either figure into prose — §0.2 clause 1.

The shape, for anyone tracing it: `classify` / `classify-purpose` write the two JSONL files and stop,
`scripts/import_branch_classification.py` reads them and writes observations through
`airflow/dags/combined_discovery/branch_observations.py`, and `pipeline_coordinator` promotes the
winners. `classify` staying compute-only is deliberate — its `--limit` is not a subset (§6.14), so a
write reachable only through the finished file cannot land a partially-scored population.

Three decisions worth knowing before changing any of it, each guarded by
`scripts/check_branch_observations.py`:

- **The emitter gates on the OUTCOME, never on the value.** Every unassigned row in both files
  carries the literal string `"NONE"`, tens of thousands of them, and a filter written against the
  value would make `'NONE'` a canonical industry field for thousands of companies. `ASSIGNED_OUTCOMES`
  is imported from `purpose_branch`, never restated, so a new outcome cannot silently slip in.
- **`technologiefeld_withheld` and `purpose_field_withheld` are written and resolved by nothing.**
  The `directory_url` trick: a field name no `resolve_*` function reads cannot be published by any
  threshold change. `test_resolver_invariants` asserts no withheld row ever reaches a canonical
  pointer.
- **The quality-score weights were left alone**, so no stored `data_quality_score` moved and the
  ceiling `test_spend_guard` pins is unchanged. Giving the two columns weight is a separate, measured
  decision — `resolver.compute_quality_scores` says so in place.

**What is still open is the data, not the code.** The values are Osnabrück figures that Berlin
replaces wholesale, and not one row of either column has been hand-checked — see §5.6, which is the
cheapest unstarted task in the project.

### 5.2 Full scoring unification — after Berlin

P2 has a 2-signal scorer; P3 has 25 evidence constants. Deferred because Brave costs money to re-run
and **P2 stored no page evidence, so its rows cannot be re-scored offline** the way P3's can from
`results.jsonl`.

### 5.3 Hasura — greenfield

Nothing is wired yet. **Recommendation: a plain view over `v_companies_enriched`**, not a second
resolution layer — the resolver is already the single place that decides a winner, and a second one
would be the "one predicate per decision" rule broken at the API boundary.

Both industry columns are published and ready to serve — §5.1. What is not settled is whether they SHOULD be exposed yet: not one row of either is hand-checked (§5.6).

### 5.4 `fetch-content` over the canonical URLs with no cached page

Free, a few thousand requests. Most canonical URLs came from the register export and none has been
fetched, so their words have never been read.

### 5.5 `docs/core-queries.sql` not yet cut down

Warned and flagged (§0.3), not yet reduced. Working queries are in
[core-queries-current.sql](core-queries-current.sql).

### 5.6 No hand-labelled baseline for any industry column

See §4.3. The cheapest unstarted task, and the one gating every quality claim in the project.

### 5.7 Documentation debt this file creates

Tracked here so it is not forgotten. Each is a small, separate commit:

1. **Add a status banner to the ten unlabelled-stale files** listed in §0.3, in the style
   `core-queries.sql` and the two vault notes already use. Cheapest, highest-value.
2. **Delete the V19 "Known bug" paragraph** in `../airflow/dags/README.md`, and add
   `technologiefeld` / `purpose_field` to its coordinator field-alias table.
3. **Fix the three foreign absolute paths** (`/Users/jonathanhaudenschild/…`) in the mistral, claude
   and berlin_gewerbedaten READMEs and in [berlin-gewerbedaten-pipeline.md](berlin-gewerbedaten-pipeline.md).
4. **Fix `run_combined_discovery.py`'s module docstring** so `--help` can be cited as truth for write
   behaviour (§0.2 clause 2).
5. **Fix `CLAUDE.md`'s `classify-purpose` cell** — it reads Postgres (§2.8).
6. **Remove `flyway info`** from the root `README.md`'s Common commands; it cannot work (§6.7).
7. **Cut `CLAUDE.md` to a map under 300 lines**, pointing here. Its industry-classification section is
   the standing debt its own header names.

---

## 6. Gotchas — the traps that cost hours

Symptom first, because a symptom is what you have.

### 6.1 `OLLAMA_MODEL` — the model name silently loses paid-for answers

**Symptom.** A free re-run reports thousands of rows as `no_keyword` and logs `model answers applied
from cache: 0`. Or a run dies at the first model call after appearing to start fine.

**Cause.** `OLLAMA_MODEL` defaults in code to a model that is **not pulled** on this machine, and
`require_ollama` validates only the model's **base name** — so a wrong-but-plausible name passes the
check and then fails at the first real call. Worse: the answer cache key includes the model name, so a
wrong name makes every cached answer **unreachable rather than absent**. That silently dropped 1,285
paid-for model answers once and reported them as if the model had never spoken.

**Fix.** Set it explicitly, every time, before any command that may call a model:

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"
```

`.env` carries `OLLAMA_BASE_URL=http://host.docker.internal:11434` for the **containers**; the
scripts run on the host, where that name does not resolve, and `run_combined_discovery.py` rewrites
it to `localhost` on the way in.

Ollama must also run **natively on Windows with `OLLAMA_IGPU_ENABLE=1`** — it finds the integrated
GPU through Vulkan and then *drops* it unless that variable is set. Native is 74× faster than the
Docker one, and the reason is host memory over-commitment, not the hardware. Full diagnosis and the
`prompt-eval : generation` ratio that detects it: vault `Local Model Setup`.

### 6.2 `db_website` vs `url` in the handover CSV

**Symptom.** The handover CSV appears to show far fewer websites than the database has.

**Cause.** `csv` emits **both**. `url` is combined discovery's *own* find. `db_website` is the
canonical website the application actually serves, from every source including the register export.
Because the export supplies the large majority of canonical websites (§1.5), reading `url` as "the
company's website" undercounts by roughly a factor of four.

**Fix.** Read `db_website`, with `db_website_source`, `db_website_confidence` and `agrees_with_db`
beside it. `url` answers a narrower question: what did P3 find on its own.

### 6.3 `tech_field` vs `technologiefeld` vs `cluster` vs `purpose_field` vs `wz_code`

Five columns, five different questions, and they must never be merged or coalesced. Full table and
the four rules: **§1.7.**

### 6.4 The observation is named `website`, never `website_url`

**Symptom.** A query returns zero rows and no error.

**Cause.** `website_url` is a **view** column of `v_companies_enriched`. The observation `field_name`
is `website`, aliased with `candidate_url`. A query filtering `field_name = 'website_url'` returns
zero rows rather than failing — which is worse than failing, because it reads as "we found nothing".
Four queries in `core-queries.sql` and three passages in [data-model.md](data-model.md) do exactly
this.

**Fix.** Query `v_companies_enriched` for the resolved answer; query `company_field_observations`
for the evidence, on `field_name IN ('website','candidate_url')`. And do not add enriched columns
back to `companies` expecting the coordinator to fill them — V9 dropped them on purpose.

### 6.5 The coordinator is the publish switch

**Symptom.** A run reports finding websites and `v_companies_enriched` shows none.

**Cause.** Observations are written and unresolved. See **§1.6** for who triggers it and who does
not — in particular a hand-run DAG, P1's website import, and any failed task.

### 6.6 Port 8082, not 8080

**Symptom.** The Airflow UI shows the wrong DAGs, or login fails with "invalid login".

**Cause.** The mapping is `${AIRFLOW_PORT:-8082}:8080`. `docker ps` shows `8080/tcp`, which is the
port *inside* the container. On this machine host port 8080 belongs to a **different project's**
Airflow — different instance, different users. `dev-up.sh`'s banner prints 8080 and is wrong.

**Fix.** http://localhost:8082. Confirm with `docker compose ps --format "{{.Service}} {{.Ports}}"`.

### 6.7 `flyway info` with an argument cannot work

**Symptom.** *"Unable to connect… Configure the url, user and password."*

**Cause.** `docker compose run` with a command argument **overrides** the service's `command:`,
discarding the baked-in `-url/-user/-password`. The command in the root `README.md` was never valid.

**Fix.** Use the argument-free form, which migrates:
`docker compose --profile tools run --rm flyway`. Flyway is a one-shot service under
`--profile tools` and **does not run with `docker compose up`**.

### 6.8 A DAG needs two switches, and the flag is read at task runtime

**Symptom.** A DAG appears in the UI and its task raises `RuntimeError: <FLAG> is not enabled.`

**Cause.** Two independent locks: the `ENABLE_*` flag in `.env`, **and** unpausing in Airflow. The
flag is checked when the task **runs**, not when the file is parsed — which is deliberate, so a
missing flag fails loudly instead of hiding the DAG. A missing API key raises the same way.

**One exception:** `berlin_gewerbedaten` is the only DAG with a schedule (`@daily`). Turning its flag
on and unpausing it is enough to make it run by itself. Leave it `false`.

Check what actually arrived without printing the secret:

```powershell
docker compose exec -T airflow-scheduler python -c "import os; k=os.getenv('BRAVE_API_KEY',''); print('key:', ('set, %d chars' % len(k)) if k else 'MISSING'); print('flag:', os.getenv('ENABLE_BRAVE_SEARCH_DISCOVERY'))"
```

### 6.9 `.env` is what runs; `.env.example` is loaded by nothing

**Symptom.** A setting has no effect.

**Cause.** Compose reads `.env`. `.env.example` is committed and read by nothing, so setting a flag
there changes nothing. A variable must **also** appear in `x-airflow-common.environment` in
`docker-compose.yml`, because the containers do not inherit `.env` — one absent from **both** cannot
be changed by editing `.env` alone. `.env` is not regenerated from `.env.example` and is missing many
keys, which is usually harmless because compose defaults every one.

**Fix.** Env is baked in at container start: after editing `.env`, run `docker compose up -d` on the
Airflow services. **A restart is not enough.**

### 6.10 Host DSN vs container DSN

Inside Docker: `host=postgres port=5432`. From Windows: `host=localhost port=5433`. **Host scripts
never read `.env`** — they set a host-shaped `PG_DSN` default themselves, and it has to be set before
`source_enrichment_common` is imported, because that module reads `PG_DSN` at import time. That is why
several imports live inside functions rather than at module top level.

### 6.11 Git Bash rewrites `/opt/...`

**Symptom.** An unrelated `ModuleNotFoundError`.

**Cause.** `docker compose exec -e PYTHONPATH=/opt/airflow/dags` under Git Bash arrives as
`C:/Program Files/Git/opt/airflow/dags`.

**Fix.** Use PowerShell, or prefix `MSYS_NO_PATHCONV=1`, or write `//opt/...`.

### 6.12 A pipe hides a long run completely

**Symptom.** A 20-minute job looks like a hung process with a 0-byte log.

**Cause.** `python -u long_script.py | tail -20` buffers the whole run; nothing appears until the
process exits.

**Fix.** Run long jobs bare, with `-u`, in a terminal. **Never pipe a run through `tail`.** The repo
enforces this: `.claude/hooks/block_long_python.py` refuses the model-invoking and downloading
commands outright so a person runs them by hand.

### 6.13 `reset_provider_observations.sql` does not clear the checkpoint

**Symptom.** A "clean" re-run resumes *past* the companies you just deleted.

**Cause.** The script clears the database; `airflow/data/<source_key>_checkpoint.json` survives. It
also deletes **all** `company_managers` rows, because that table has no `source_id` to scope by, and
nulls `data_quality_score`, which the coordinator recomputes.

**Fix.** Delete the checkpoint JSON too. Dry run first — the script ends in `ROLLBACK` and prints the
counts; swap it for `COMMIT` to apply. Command form: vault `Local Dev Runbook`.

### 6.14 `--limit` on `classify` is not a subset

**Symptom.** A limited run and a full run give the **same company different answers**, and two
`branches.jsonl` files look identical but are not comparable.

**Cause.** A keyword's weight depends on **how many companies use that word**, so the scoring corpus
is whatever is in scope. `--limit 1000` produces different weights, not fewer rows of the same
answer. `classify` records `scope_companies` and `df_companies` on every row for exactly this reason,
and `classify-report` prints `!! this file mixes runs` when a file holds rows from two.

**Contrast:** `fetch-content --limit` *is* a prefix, and `classify-purpose --limit` changes what is
written but not what is counted. Only `classify` has this property.

### 6.15 `DISCOVERY_ACTIVE_ONLY` is not a subset either

**Cause.** The batch limit applies **after** the filter, so a limited run with the flag on is that
many **live** companies — different companies, not fewer. The saving appears on a full pass over the
table, not on a limited one.

Two more things about it: **nothing in this repo's published numbers was measured with it on**, so
compare a filtered run only against another filtered run. And in `classify` it withholds only a
**new** model call — keywords still score every company, and a verdict already in the cache still
applies, so both classification files keep one row per company and every outcome count still sums to
the table.

### 6.16 A register number on a page proves the page is *about* a company, never that it *is* its site

**The single most recurrent bug in this project — it has arrived through three different doors.**
Registries, credit bureaux, insolvency trackers and business directories all print register numbers,
because describing companies is their business, so they match perfectly every time.

`search_common.own_site_verdict` / `is_own_site_candidate` is the **single** predicate allowed to
make that call. A looser one anywhere else is the recurring failure. Two traps in the same family:

- **"A real business site" is not "THAT company's site."** Every German business site has an
  Impressum, a VAT number and a contact email at its own domain, so none of those identifies one
  company. That evidence may add score but must stay out of the verifying-evidence set.
- **Never grow a list of bad domains or words.** Genericness is *measured* from the company table
  itself, because the most common email domain here is a **local ISP** no hand-written list of
  national providers would contain — and the next town's would differ.

Guard: `scripts/check_own_site_filter.py`, real URLs a provider actually returned, non-zero exit on
mismatch. **Run it after touching any of this.** Depth: vault `Confidence and Resolution`, Part 4 —
the nine pages that tried, and the two rules that were measured rather than guessed.

### 6.17 Excel changes the delimiter and does not tell you

**Symptom.** A label worksheet opens fine and has every row, but a reader sees the whole header as
one column.

**Cause.** On a German locale Excel saves CSV with `;` instead of `,`.

**Fix.** Already handled — the readers sniff the delimiter and log which one they found, so a round
trip is safe. `is ';'-delimited (Excel does this)` in the log is the fix working, not a problem.
Related: always write CSV exports with `| Out-File -Encoding utf8`, never plain `>`, or the umlauts
land in the system codepage.

### 6.18 Never edit a pinned number to make a red test green

If you change a weight, a threshold or the taxonomy **on purpose**, the pinned numbers are *supposed*
to move: re-measure with the `*-report` subcommands and edit the frozen block **in the same commit as
the change**, with the reason in the message. Diagnose first — a number that moved for a reason you
cannot name is a bug, not a stale baseline. §0.2 clause 6.

Two guard rules in the same spirit, both learned by writing a guard that broke them:

1. **Import the rule you are checking; never restate it.** A check that rebuilds the logic agrees
   with itself no matter what the real code does.
2. **Assert on the syntax tree, not the source text**, when the forbidden thing is *discussed* in a
   docstring. A plain substring test once found **`Handel` inside `Handelsregister`**.

Depth: [guards-and-tests.md](guards-and-tests.md).
