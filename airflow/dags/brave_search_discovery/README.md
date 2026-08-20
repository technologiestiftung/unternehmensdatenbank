# brave_search_discovery

Brave web search → Impressum verification → field observations.

## Status

**Never executed against the live API, and the client was written from the OpenAPI schema
rather than from an observed response.** No `BRAVE_API_KEY` has ever existed here. Brave
publishes no complete end-to-end JSON example, so the first real run doubles as contract
verification.

If the first run returns suspiciously empty result lists, suspect a **request-shape mismatch**
before concluding "Brave found nothing". That exact failure hid inside the old Parallel REST call
for weeks: the request was malformed, the exception was swallowed, and every run reported zero
results as though the search had simply failed to find anything.

The DAG refuses to start without the key: `require_brave_api_key()` runs in the preflight.

## Brave is not a like-for-like competitor

Both of these are structural, not bugs, and both must be stated when reporting results:

- **Snippets only.** `description` plus up to five `extra_snippets` — there is no page text. Full
  text is a separate product (`/res/v1/llm/context`). Path A is unaffected, because
  `imprint_probe` fetches pages itself; **path B is materially handicapped.** When Brave extracts
  fewer owners or clusters than Exa, part of the cause is that it supplied less text to extract
  from, not that its ranking was worse.
- **`q` is capped at 400 characters / 50 words**, below what `build_search_queries()` produces for
  a long German company name plus address. `brave_http._truncate_query` truncates client-side, so
  Brave searches on a shorter query than the other providers receive.

## Flow and observations

Identical to [`parallel_search_discovery`](../parallel_search_discovery/README.md) — the flow
lives in [`search_discovery_runner.py`](../search_discovery_runner.py) and is shared verbatim.
See that README for the full field/confidence table. Only `spec.search` differs.

Expect `result["note"] == "matching_results_had_no_text"` far more often here than for Exa; that
is the snippet limitation surfacing, and it means path B was skipped entirely for that company.

## Key env vars

| Var | Default | Notes |
|---|---|---|
| `ENABLE_BRAVE_SEARCH_DISCOVERY` | `false` | checked at task runtime |
| `BRAVE_API_KEY` | *(empty)* | **required** — subscription token, sent as `X-Subscription-Token` |
| `BRAVE_COUNT` | `10` | 1–20; higher is rejected |
| `BRAVE_COUNTRY` / `BRAVE_SEARCH_LANG` | `DE` / `de` | |
| `BRAVE_MAX_CANDIDATES` | `5` | candidates crawled per company |
| `MISTRAL_API_KEY` | *(empty)* | **required** — path B extraction |

The free Search plan grants $5/month of credits, but signup requires a card on file.

## Files

| File | Role |
|---|---|
| `main.py` | `ProviderSpec` + the task callable |
| `brave_http.py` | `GET /res/v1/web/search` client, query truncation, snippet merging |
