# DAGs

Every DAG here uses `schedule=None` **except `berlin_gewerbedaten`, which is `@daily`**. So all
but that one run only when you trigger them. Every source DAG triggers `pipeline_coordinator`
when it finishes, via `TriggerDagRunOperator`; the coordinator triggers nothing, so there is no
chain beyond one step.

Two independent locks stop a DAG running: its `ENABLE_*` env flag must be `true`, **and** it
must be unpaused in Airflow. The flag is checked when the task runs, not when the file is
parsed, so a disabled DAG still appears in the UI and fails with a clear message instead of
vanishing. Turning a flag on is safe for everything except `berlin_gewerbedaten`, where the flag
plus unpausing is enough to make it run by itself once a day.

Schema changes belong in `db/migrations/`, not here.

Two words you need before anything else:

- **observation** — one claim about one field of one company, from one source. "`website` =
  `example.de`, from `exa_search_discovery`, confidence 0.65". Sources write observations and
  nothing else. They never overwrite each other.
- **canonical** — the one observation that won for a field, and is therefore the answer the
  application sees. Only `pipeline_coordinator` decides this. A source DAG that wrote directly
  to `companies` would make the other sources' claims unrecoverable.

---

## Shared modules

### `source_enrichment_common.py`

Database access and company selection, used by every source DAG.

- `get_connection()` — psycopg2 connection to the app database
- `ensure_source(cur, key, name, source_type, metadata)` — upsert into `sources`
- `start_sync_run` / `finish_sync_run` — log one run's lifecycle in `source_sync_runs`
- `upsert_source_record` — upsert into `company_source_records`, returns `(id, was_created)`
- `insert_text_observation(cur, ..., confidence)` — insert into `company_field_observations`
- `fetch_companies_without_source_after_id(source_key, after_id, limit)` — checkpoint-based
  batch fetch; returns companies this source has not processed yet
- `fetch_companies_for_llm_enrichment(source_key, ...)` — same, but also skips companies whose
  `data_quality_score` is already at or above 0.90, so expensive sources are not spent on
  companies that are already complete
- `build_search_queries(company, *, include_registration_number=None)` — the search strings.
  The Handelsregister number is left OUT by default, because putting it in makes discovery
  worse: measured 21 provable websites per 100 companies with it, 25 without. The extra results
  it brings back are register aggregators and directories. Override with
  `SEARCH_QUERY_INCLUDE_REGISTER_NUMBER=true` or the keyword argument.
- `CompanyCandidate` — `id, company_name, address, postcode, city, registration_number,
  legal_form, wz_code, business_purpose`

### `search_discovery_runner.py`

The whole per-company flow for every search provider: search, filter, download, check the
Impressum, score, ask the model, write observations. A provider supplies a `ProviderSpec` — a
source key, a `search` callable and its rate-limit exception types — and this module does the
rest.

Sharing it is not tidiness. It is what makes "which provider is best" answerable: same model,
same prompt, same scoring, only the search step differs. Three copies would drift, and the
comparison would then measure the drift. Drift between two copies of one rule also caused the
worst bug in this repo so far — see `search_common.is_own_site_candidate`.

Two routes run per company, and keeping them apart is the point:

- **route A** downloads each candidate page and checks its Impressum. Confidence 1.0 on a
  register-number match, 0.365–0.65 on a pattern match.
- **route B** shows the search snippets to a model and asks for the fields. Nothing is checked,
  so a website from this route is capped at `LLM_WEBSITE_CONF_CAP` (0.34) — below the 0.35 the
  resolver requires, so a guess can never become the canonical website on its own.

### `search_common.py`

Deciding what a URL is.

- `filter_hits(hits, company_name)` — drop results that are not about this company
- `is_own_site_candidate(url, company_name, score, auto_accept)` — is this the company's OWN
  site, as opposed to a page about it? A register number on a page proves the page is *about*
  the company, never that it *is* the company. Registry pages, insolvency trackers and
  directories all quote register numbers.
- `own_site_verdict(url, company_name)` — the same decision, but says why it failed. Drives
  whether a URL is written as `candidate_url` (competes to become the website) or
  `directory_url` (a field name the resolver ignores, kept as evidence).
- `origin_url(url)` — the site, not the page that happened to rank

`scripts/check_own_site_filter.py` is the regression gate: 21 real URLs with hand-checked
verdicts.

### `imprint_probe.py`

An **Impressum** is the legal contact page every German business website must have. It usually
carries the address, the managing director's name and the register number, which makes it the
one page that can prove a site belongs to a specific company.

`best_imprint(base_url)` tries the usual paths (`/impressum/`, `/imprint`, …) and returns the
most informative one — a register number beats a VAT id beats a named person. Returns `None`
when nothing could be fetched; never raises on network problems.

Parsing lives in `url_discovery/imprint_extractor.py`. `page_text()` keeps one line per HTML
element, which `extract_ceo_names()` depends on: the name pattern stops at a line break. Gate:
`scripts/check_imprint_owner_extraction.py`.

### `mistral_extract.py`

One model call: text in, strict JSON out. Deliberately separate from the search step so a
provider can be swapped without touching extraction. Mistral because it is EU-hosted and the
Impressum text being sent carries managing-director names, which are personal data.

`as_str()` and `as_confidence()` are the boundary guards. Models return the string `"null"`,
and dicts where a string was asked for; both used to reach the database.

### `searxng_http.py`

Thin wrapper around the local SearXNG instance.

**There are two copies of this file** — this one, and `ollama_search_discovery/searxng_http.py`.
They are not identical: the copy inside the package waits `SEARXNG_REQUEST_DELAY_S` (10 seconds
by default) between requests, and this one does not. `ollama_search_discovery` puts its own
directory first on `sys.path`, so it gets the copy with the delay, and nothing currently imports
the one at this level. Delete one of them before the difference bites someone.

---

## Source DAGs

Each has its own README with env vars and provider quirks.

| DAG | What it does | Needs |
|---|---|---|
| [`exa_search_discovery/`](exa_search_discovery/README.md) | Exa web search + Impressum check | `EXA_API_KEY` |
| [`parallel_search_discovery/`](parallel_search_discovery/README.md) | Parallel Search MCP + Impressum check | key optional |
| [`brave_search_discovery/`](brave_search_discovery/README.md) | Brave Search + Impressum check | `BRAVE_API_KEY` |
| [`mistral_search_discovery/`](mistral_search_discovery/README.md) | Mistral's own hosted web search | `MISTRAL_API_KEY` |
| [`claude_search_discovery/`](claude_search_discovery/README.md) | Anthropic's hosted web search tool | `ANTHROPIC_API_KEY` |
| [`ollama_search_discovery/`](ollama_search_discovery/README.md) | SearXNG + a local model, no API cost | Ollama + SearXNG |
| `url_discovery/` | Guesses URLs from the company name, then checks them | nothing |
| `osm_discovery/` | Matches companies to OpenStreetMap businesses | local Overpass |

The first three share `search_discovery_runner` verbatim, which is what makes them comparable.

### A new provider is refused until somebody has watched it work

A search client that has never run against its provider's live API cannot be trusted to fail
loudly. A wrong request shape does not raise — it returns an empty result list, and an empty result
list reads exactly like "the search found nothing". The Parallel REST call was malformed on that
basis for weeks, and every run looked like a provider that simply found little.

So `ProviderSpec.smoke_required` defaults to `True`, and `require_enabled()` refuses to start the
batch until `./airflow/data/<source_key>_smoke_ok.json` exists. A smoke script on the host writes
that file after one real call, recording the date, the hit count and the response's top-level keys;
the task then logs those back, so a marker left over from before a change is visible rather than
silently accepted.

To clear the gate for a provider, write `scripts/smoke_<provider>_client.py` — copy
`smoke_brave_client.py`, which is the fullest of them — run it, and read what it prints. Set
`smoke_required=False` only for a client already proven in production, and put the date and the
evidence in a comment beside it: that comment is the only record of when the contract was last
checked.

### `url_discovery/`

Builds candidate URLs from the company name and legal form (slug variants, common TLDs,
subdomains), fetches each one, parses its Impressum, and scores the match. No search engine
involved, so it costs nothing.

**Confidence**: register number matches → `1.0`. Postcode matches, nothing contradicts → `0.55`
to `0.85`. URL pattern only, no Impressum → `0.35 + 0.30 × name_uniqueness`, where uniqueness
comes from slug length, word count and whether every word is a generic term. "Handel" scores
low; a specific three-word name scores high.

**Watch out**: `scorer.py:77` takes **200 points** off for a register number that does not
match, and anything below zero is discarded. An Impressum can legitimately show a different
number from ours — a group parent, or a page listing several companies. Measured to cost about
4 verified websites per 100 companies. Not yet fixed.

**Writes**: `candidate_url` for every verified candidate, `website` for the best one.

### `osm_discovery/`

Queries the local Overpass API for businesses matching each company name inside its postcode or
city, with fuzzy name matching (rapidfuzz).

> The local Overpass container holds Berlin only. On the Osnabrück dataset this DAG finds
> nothing.

**Writes**: `osm_id`, `candidate_url` (the OSM `website` tag), `phone`.

---

## Coordinator DAG

### `pipeline_coordinator/`

Turns observations into answers. Resolves every pending observation into a canonical field,
recomputes `data_quality_score` for every company, and logs field coverage before and after.
Triggered automatically at the end of every source DAG, and safe to trigger by hand at any
time — it is idempotent.

**Per logical field**

1. Find the best observation across all field names that mean the same thing (`candidate_url`
   and `website` both mean `website`)
2. Add 0.10 for each additional source agreeing on the same value, capped at 1.0
3. Break ties by source priority — lower integer wins
4. Mark the winner `is_canonical = true`
5. Upsert `company_canonical_field_sources`

**Field names and their aliases**

| Logical field | Observation field names | Min confidence |
|---|---|---|
| `website` | `candidate_url`, `website` | 0.35 |
| `cluster` | `cluster` | 0.50 |
| `tech_field` | `tech_field` | 0.50 |
| `revenue` | `revenue` | 0.50 |
| `employee_count` | `employee_count`, `number_of_employees` | 0.50 |
| `ceo_contact` | `ceo_name_or_contact` → written to `company_managers` | 0.50 |

`directory_url` is deliberately absent from this table. It is where a URL goes when it mentions
the company but is not the company's own site, and the resolver never reads it.

**Known bug**: `resolve_ceo_contact` inserts into `company_managers` with `ON CONFLICT DO
NOTHING`, but that table has no unique constraint on `(company_id, full_name)`, so nothing ever
conflicts and every run adds duplicate rows. It also does not record which observation a name
came from, so a name read off an Impressum cannot be told apart from one a model guessed.
`v_discovery_results` works around this by reading observations directly.

**Files**: `resolver.py` (all the SQL), `main.py` (DAG + coverage logging)

---

## Looking at the results

`public.v_discovery_results` and `public.v_discovery_scorecard` (migration V11) show what these
DAGs have found, per company and per source. Open them in pgAdmin. Always read
`website_verified_by` first: `register_number_match` and `url_pattern` mean the page was
downloaded and checked, `llm_snippet` means a model guessed and nothing was checked.

Measured coverage and the method behind it:
[docs/website-and-owner-discovery-results.md](../../docs/website-and-owner-discovery-results.md).

---

## Legacy

### `berlin_gewerbedaten/`

Fetches the Berlin Gewerbedaten WFS, enriches with addresses (KDTree over 500k address points)
and OSM POI names (Overpass + rapidfuzz), writes JSONL, bulk-loads into Postgres. Does not chain
into `pipeline_coordinator`.

> **The only DAG here with a schedule.** It is `@daily`, not `schedule=None`. Set
> `ENABLE_BERLIN_GEWERBEDATEN_DAG=true` and unpause it, and it runs every day on its own,
> downloading the Berlin dataset and writing into `companies`. Every other flag in `.env` can be
> turned on without anything happening until you press trigger. This one cannot. Leave it
> `false` unless you actually want Berlin data.
