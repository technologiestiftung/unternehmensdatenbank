# Source Enrichment DAGs

The source-enrichment direction is centered on provider-specific discovery DAGs that write comparable results into the same database model.

## Active DAGs

- `mistral_search_discovery`
- `claude_search_discovery`

Files:

- [airflow/dags/mistral_search_discovery/main.py](../airflow/dags/mistral_search_discovery/main.py)
- [airflow/dags/claude_search_discovery/main.py](../airflow/dags/claude_search_discovery/main.py)
- shared helpers: [airflow/dags/source_enrichment_common.py](../airflow/dags/source_enrichment_common.py)
- low-level Mistral HTTP client: [airflow/dags/mistral_http.py](../airflow/dags/mistral_http.py)
- low-level Claude HTTP client: [airflow/dags/claude_http.py](../airflow/dags/claude_http.py)

## What they do today

Current state:

- `mistral_search_discovery` is now a live integration
- `claude_search_discovery` is now a live integration

`mistral_search_discovery` currently:

1. selects companies missing a completed Mistral discovery source record
2. calls Mistral Conversations API with the `web_search` tool
3. asks for JSON containing:
   - `website`
   - `ceo_name_or_contact`
   - `tech_field`
   - `cluster`
   - `kununu_url`
   - `linkedin_url`
4. stores the raw discovery payload in `company_source_records`
5. stores query and discovered-field observations in `company_field_observations`

`claude_search_discovery` currently:

1. selects companies missing a completed Claude discovery source record
2. calls Anthropic Messages API with the server-side web search tool
3. asks for the same JSON field set as Mistral:
   - `website`
   - `ceo_name_or_contact`
   - `tech_field`
   - `cluster`
   - `kununu_url`
   - `linkedin_url`
   - `candidate_urls`
4. stores the raw discovery payload in `company_source_records`
5. stores query and discovered-field observations in `company_field_observations`

## Why this shape

- `mistral_search_discovery` and `claude_search_discovery` are parallel upstream URL and metadata discovery layers
- Kununu and LinkedIn are still represented as fields to discover, not as separate active DAGs

That separation keeps:

- retries independent
- credentials independent
- failure domains small
- downstream enrichment easy to pause per source

## Default behavior

All three DAGs are manual-safe and disabled by default:

- `schedule=None`
- disabled by default through env flags

Relevant env vars in `.env`:

- `ENABLE_MISTRAL_SEARCH_DISCOVERY`
- `ENABLE_CLAUDE_SEARCH_DISCOVERY`
- `SOURCE_ENRICHMENT_BATCH_SIZE`
- `MISTRAL_API_KEY`
- `MISTRAL_MODEL`
- `MISTRAL_SEARCH_TOOL`
- `MISTRAL_DISCOVERY_MAX_RESULTS`
- `MISTRAL_REQUEST_DELAY_S`
- `MISTRAL_HTTP_TIMEOUT_S`
- `MISTRAL_HTTP_MAX_RETRIES`
- `MISTRAL_RETRY_MAX_WAIT_S`
- `MISTRAL_PROGRESS_LOG_EVERY`
- `MISTRAL_DISCOVERY_CHECKPOINT_FILE`
- `ANTHROPIC_API_KEY`
- `CLAUDE_MODEL`
- `CLAUDE_WEB_SEARCH_TOOL_TYPE`
- `CLAUDE_API_BASE_URL`
- `CLAUDE_HTTP_TIMEOUT_S`
- `CLAUDE_HTTP_MAX_RETRIES`
- `CLAUDE_REQUEST_DELAY_S`
- `CLAUDE_RETRY_MAX_WAIT_S`
- `CLAUDE_MAX_TOKENS`
- `CLAUDE_WEB_SEARCH_MAX_USES`
- `CLAUDE_MAX_PAUSE_TURNS`
- `CLAUDE_DISCOVERY_MAX_RESULTS`
- `CLAUDE_PROGRESS_LOG_EVERY`
- `CLAUDE_DISCOVERY_CHECKPOINT_FILE`

## Legacy DAG

The old Berlin Gewerbedaten DAG is still in the repo, but disabled by default:

- [airflow/dags/berlin_gewerbedaten/main.py](../airflow/dags/berlin_gewerbedaten/main.py)
- env flag: `ENABLE_BERLIN_GEWERBEDATEN_DAG=false`

## Current operational reality

- the Mistral DAG is live
- the Claude DAG is live
- the account is rate-limited, so large sweeps must be chunked and resumed
- discovered `kununu_url` and `linkedin_url` values can still be stored as observations now, even though there are no separate DAGs for them

## What else full integrations need

Besides env vars, full integrations need:

1. Mistral access

- a valid `MISTRAL_API_KEY`
- use of Mistral Conversations or Agents with `web_search`, because official docs say `web_search` is supported in the Conversations API and Agents API, not Chat Completions

2. Claude access

- a valid `ANTHROPIC_API_KEY`
- support for the Anthropic Messages API server-side web search tool
- continuation handling for `pause_turn`

3. Kununu scraping hardening

- stable HTML selectors
- retry and timeout handling
- conservative rate limiting
- handling for CloudFront or bot blocking when direct fetches are refused

4. LinkedIn runtime

- Playwright installed in the runtime image
- a persistent authenticated session file, or controlled credential login flow
- slower, more conservative request pacing than Kununu

5. Shared matching rules

- how to map one discovered page back to one `companies` row
- conflict resolution if multiple candidate pages are found
- confidence thresholds before writing canonical fields

6. Source field mapping

- which extracted source fields become:
  - raw `company_source_records`
  - `company_field_observations`
  - canonical company columns

7. Ops and monitoring

- per-source retry policy
- per-source pause/disable switch
- logs for failed selectors / auth expiration
- metrics for created vs updated source records
