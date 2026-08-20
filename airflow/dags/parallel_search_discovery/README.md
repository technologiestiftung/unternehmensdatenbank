# parallel_search_discovery

Parallel web search → Impressum verification → field observations.

Reference implementation of the three search-provider DAGs, and the only one runnable without an
API key: Parallel's MCP endpoint serves anonymous requests.

## Status

**Verified end to end.** Repeated runs against the live API have produced canonical websites
through `pipeline_coordinator`, including register-verified matches at confidence 1.0.

## Flow

The per-company flow lives in [`search_discovery_runner.py`](../search_discovery_runner.py) and
is shared **verbatim** with Exa and Brave. Only the search call differs. That is deliberate: it
is what makes "which provider is best" a fair question, and three copies of the scoring logic
would drift until the comparison measured the drift instead of the providers.

```
parallel_http.search(queries, objective)
  → search_common.filter_hits            drop results about other companies
  → path A  verify_url → imprint_probe.best_imprint → scorer.score_candidate
            → website / candidate_url / phone / email / ceo_name_or_contact
  → path B  mistral_extract over the filtered result text
            → cluster / tech_field, and website only when path A found none
  → TriggerDagRunOperator → pipeline_coordinator
```

## Observations written

| field_name | Path | Confidence |
|---|---|---|
| `website` | A, register number matches `companies.registration_number` | `1.0` |
| `website` | A, Impressum fetched but no register match | `0.35 + 0.30 × name_uniqueness` |
| `website` | B, LLM read it off search text | capped at `LLM_WEBSITE_CONF_CAP` (0.34) |
| `candidate_url` | A, plausibly the company's own site | `(score + 50) / 150` |
| `directory_url` | A, anything else — **the resolver ignores this name** | `(score + 50) / 150` |
| `phone`, `email` | A, from the Impressum | `website_conf × 0.95` |
| `ceo_name_or_contact` | A Impressum, or B | `× 0.95 × 0.90`, or the model's own |
| `cluster`, `tech_field` | B | the model's own, clamped to [0, 1] |
| `search_query`, `website_search_query` | audit trail | `1.0` |

`metadata->>'verified_by'` records which of those rungs produced a value:
`register_number_match`, `url_pattern`, `llm_snippet`.

## The quota, and why runs end as `partial`

Anonymous access is a **hard quota, not a rate limit** — measured at roughly 20 searches per
process, after which every request 429s regardless of how long you wait. `Retry-After` is
honoured, but retrying a quota wall only burns wall-clock: an early flat 5s retry loop wasted
~6.6 minutes before `PARALLEL_QUOTA_GIVEUP_AFTER` existed.

So a 50-company batch stops at ~20 with `source_sync_runs.status = 'partial'`, and the
checkpoint holds the last **completed** company so the interrupted one is retried, not skipped.
Any batch larger than ~20 needs `PARALLEL_API_KEY`.

## Key env vars

| Var | Default | Notes |
|---|---|---|
| `ENABLE_PARALLEL_SEARCH_DISCOVERY` | `false` | checked at task runtime |
| `PARALLEL_API_KEY` | *(empty)* | optional — only raises the quota |
| `PARALLEL_QUOTA_GIVEUP_AFTER` | `3` | consecutive 429s before stopping the run |
| `PARALLEL_MAX_CANDIDATES` | `5` | candidates crawled per company — the cost knob |
| `MISTRAL_API_KEY` | *(empty)* | **required** — path B extraction |

## Files

| File | Role |
|---|---|
| `main.py` | `ProviderSpec` + the task callable. Thin by design. |
| `parallel_http.py` | MCP client (Streamable HTTP JSON-RPC), session and quota state |

`web_search` requires **both** `objective` and `search_queries`. Sending neither correctly is
how the original REST call failed silently for weeks, reported as "search found nothing".
