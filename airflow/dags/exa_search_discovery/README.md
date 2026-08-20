# exa_search_discovery

Exa web search → Impressum verification → field observations.

## Status

**Never executed against the live API.** `EXA_API_KEY` has not been present in this environment,
so the DAG parses, imports and builds its `ProviderSpec`, and that is the whole extent of what
has been verified. The client is a port of a previously written `exa_http.py` that was also never
run. Treat the first real run as contract verification, not as a measurement.

The DAG refuses to start without the key: `require_exa_api_key()` runs in the preflight.

## What makes Exa different

- **Page contents arrive with the search results** (nested under `contents` in the same POST), so
  path B has real page text to read instead of snippets. This is the opposite end of the
  spectrum from Brave, and it matters most for owner extraction, where the
  Geschäftsführer line lives in the Impressum body.
- **It reports `costDollars`.** `SearchResponse.cost_usd` carries it through and
  `run_discovery` sums it into the finish log, making Exa the one provider whose spend is
  measured rather than estimated.

Two schema facts older Exa clients get wrong, both already handled in `exa_http.py`:

- `type` no longer accepts `neural` or `keyword`. The enum is
  `instant | fast | auto | deep-lite | deep | deep-reasoning`; "neural" survives only as a cost
  bucket in the response and in the deprecated `resolvedSearchType`.
- Results have **no `score` field** — only `highlightScores`, parallel to `highlights`.

## Flow and observations

Identical to [`parallel_search_discovery`](../parallel_search_discovery/README.md) — the flow
lives in [`search_discovery_runner.py`](../search_discovery_runner.py) and is shared verbatim.
See that README for the full field/confidence table. Only `spec.search` differs.

## Key env vars

| Var | Default | Notes |
|---|---|---|
| `ENABLE_EXA_SEARCH_DISCOVERY` | `false` | checked at task runtime |
| `EXA_API_KEY` | *(empty)* | **required** |
| `EXA_CONTENT_MODE` | `highlights` | `highlights` \| `text` \| `both` — see below |
| `EXA_SEARCH_TYPE` | `auto` | not `neural`/`keyword` |
| `EXA_NUM_RESULTS` | `8` | per query, and two queries run per company |
| `EXA_MAX_CANDIDATES` | `5` | candidates crawled per company |
| `MISTRAL_API_KEY` | *(empty)* | **required** — path B extraction |

`EXA_CONTENT_MODE` is the one knob likely to change a comparison result. Exa's own guidance is
highlights-only for LLM workflows — query-relevant excerpts keep token usage predictable, and
they call combining modes an antipattern. But highlights are *query-biased*, so the managing
director line may not survive them. If you switch it to `text` for Exa, that is no longer a
same-input comparison against the other providers; say so when reporting.

## Files

| File | Role |
|---|---|
| `main.py` | `ProviderSpec` + the task callable |
| `exa_http.py` | `POST /search` client, `contents` payload, `costDollars` extraction |
