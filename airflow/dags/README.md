# DAGs

All DAGs use `schedule=None` and are triggered manually or by another DAG. Every source DAG automatically triggers `pipeline_coordinator` on completion via `TriggerDagRunOperator`.

Schema changes belong in `db/migrations/`, not here.

---

## Shared modules

### `source_enrichment_common.py`

Shared helpers used by all source enrichment DAGs.

**DB helpers**
- `get_connection()` — psycopg2 connection to the app database
- `ensure_source(cur, key, name, type, priority)` — upsert into `sources`
- `start_sync_run` / `finish_sync_run` — log a sync run lifecycle in `source_sync_runs`
- `upsert_source_record` — upsert into `company_source_records`
- `insert_text_observation(cur, ..., confidence)` — insert into `company_field_observations`

**Company fetching**
- `fetch_companies_without_source_after_id(source_key, after_id, limit)` — checkpoint-based batch fetch; returns companies not yet processed by this source
- `fetch_companies_for_llm_enrichment(source_key, ..., max_quality_score=0.90)` — same but also skips companies above the quality threshold, used to gate expensive LLM sources

**Types**: `CompanyCandidate` namedtuple — `id, company_name, normalized_name, registration_number, postcode, city, address, wz_code`

### `searxng_http.py`

Thin wrapper around the local SearXNG instance. Used by `ollama_search_discovery`.

---

## Source DAGs

### `url_discovery/`

Generates candidate URLs from company name + legal form (slug variants, common TLDs, subdomain patterns), fetches each candidate, parses `/impressum` for register number and postcode, and scores the match.

**Confidence scoring**
- Register number match in imprint → `1.0`
- Partial verification (postcode match, no conflict) → `0.55–0.85` scaled by name uniqueness
- URL pattern only, no imprint → `0.35 + 0.30 × name_uniqueness`
- Name uniqueness is computed from slug length, word count, and generic-word detection — a specific multi-word name gets higher base confidence than a single common word like "Handel"

**Observations written**: `candidate_url` (all verified candidates), `website` (best candidate only)

**Key env vars**
```
ENABLE_URL_DISCOVERY=true
URL_DISCOVERY_BATCH_SIZE=10000
URL_DISCOVERY_CONCURRENCY=5
URL_DISCOVERY_REQUEST_DELAY_S=0.5
URL_DISCOVERY_CHECKPOINT_FILE=/opt/airflow/data/url_discovery_checkpoint.json
```

**Files**: `candidate_generator.py`, `http_client.py`, `imprint_extractor.py`, `scorer.py`, `main.py`

---

### `osm_discovery/`

Queries the local Overpass API for businesses matching each company name within its postcode or city area, using fuzzy name matching (rapidfuzz) to score candidates.

> **Note**: the local Overpass container only has Berlin data. OSM discovery finds nothing for non-Berlin datasets.

**Observations written**: `osm_id`, `candidate_url` (OSM website tag), `phone`

**Key env vars**
```
ENABLE_OSM_DISCOVERY=true
OSM_OVERPASS_URL=http://overpass/api/interpreter
OSM_MIN_MATCH_SCORE=0.65
```

**Files**: `overpass_client.py`, `scorer.py`, `main.py`

---

### `mistral_search_discovery/`

Uses Mistral's web-search-enabled API to search for each company and extract structured fields. Only runs on companies where `data_quality_score < 0.90` and the company hasn't been processed by this source.

**Observations written**: `website`, `cluster`, `tech_field`, `ceo_name_or_contact`, `revenue`, `employee_count`

**Key env vars**
```
ENABLE_MISTRAL_SEARCH_DISCOVERY=true
MISTRAL_API_KEY=...
MISTRAL_MODEL=mistral-small-latest
```

**Files**: `mistral_http.py`, `main.py`

---

### `claude_search_discovery/`

Same structure as `mistral_search_discovery`, using Anthropic's API with web search tool use.

**Key env vars**
```
ENABLE_CLAUDE_SEARCH_DISCOVERY=true
ANTHROPIC_API_KEY=...
```

**Files**: `claude_http.py`, `main.py`

---

### `ollama_search_discovery/`

Free/local equivalent of the paid LLM search DAGs. Queries SearXNG for each company, feeds top results into a local Ollama model, and extracts structured fields.

**Key env vars**
```
ENABLE_OLLAMA_SEARCH_DISCOVERY=true
OLLAMA_BASE_URL=http://host.docker.internal:11434
OLLAMA_MODEL=llama3.2:latest
SEARXNG_BASE_URL=http://searxng:8080
```

**Files**: `main.py`

---

## Coordinator DAG

### `pipeline_coordinator/`

Resolves all pending observations into canonical field sources, recomputes `data_quality_score` for every company, and logs field coverage before and after. Triggered automatically at the end of every source DAG; can also be triggered manually at any time (idempotent).

**Resolution logic per logical field**
1. Find the best observation across all alias field names (e.g. `candidate_url` and `website` both resolve to the logical field `website`)
2. Apply cross-source agreement bonus: +0.10 per additional source agreeing on the same value (capped at 1.0)
3. Source priority breaks ties — lower `priority` integer wins
4. Mark winning observation `is_canonical = true`
5. Upsert `company_canonical_field_sources` with the logical field name

**Logical fields and their observation aliases**

| Logical field | Observation field names | Min confidence |
|---------------|------------------------|----------------|
| `website` | `candidate_url`, `website` | 0.35 |
| `cluster` | `cluster` | 0.50 |
| `tech_field` | `tech_field` | 0.50 |
| `revenue` | `revenue` | 0.50 |
| `employee_count` | `employee_count`, `number_of_employees` | 0.50 |
| `ceo_contact` | `ceo_name_or_contact` → written to `company_managers` | 0.50 |

**Files**: `resolver.py` (all resolution + scoring SQL), `main.py` (DAG + coverage logging)

---

## Legacy

### `berlin_gewerbedaten/`

Fetches the Berlin Gewerbedaten WFS, enriches with addresses (KDTree over 500k address points) and OSM POI names (Overpass + rapidfuzz), writes JSONL, bulk-loads into Postgres. Disabled by default (`ENABLE_BERLIN_GEWERBEDATEN_DAG=false`). Does not chain into `pipeline_coordinator`.
