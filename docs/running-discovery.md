# How to run discovery and look at what it found

A runbook. Which jobs you can run, how to start them, and which query answers which question.

Two words first:

- **DAG** — one job in Airflow. All of ours are triggered by hand; none run on a schedule.
- **observation** — one claim about one company from one source, e.g. "website = `example.de`,
  found by Exa, confidence 0.65". Discovery jobs only ever write observations. A separate job
  decides which claim wins.

---

## 1. What you can run today

Depends on which API keys are filled in. Checked on 11 August 2026:

| DAG | Runnable now? | Why |
|---|---|---|
| `exa_search_discovery` | **yes** | `EXA_API_KEY` and `MISTRAL_API_KEY` are set |
| `url_discovery` | **yes** | guesses URLs from the company name, needs no key at all |
| `mistral_search_discovery` | **yes** | `MISTRAL_API_KEY` is set |
| `parallel_search_discovery` | partly | works without a key, but anonymous use stops after roughly 20 searches per run |
| `brave_search_discovery` | no | `BRAVE_API_KEY` is not in `.env` |
| `claude_search_discovery` | no | `ANTHROPIC_API_KEY` is empty |
| `ollama_search_discovery` | no | needs a GPU; measured 0.38 tokens/second on this machine |
| `osm_discovery` | pointless | the local map server holds Berlin only, and the data is Osnabrück |

**Start with `exa_search_discovery`.** It is the one every published number came from, so its
results are the ones you can compare against.

`url_discovery` is the free one. It costs nothing because it never searches — it guesses URLs
from the company name and checks them. Run it first on a big batch if you want coverage without
spending anything.

---

## 2. Running one

Four steps. Steps 1 and 2 are one-time per DAG.

### Step 1 — switch the DAG on

Each DAG refuses to do anything unless its own flag says `true`. This is a safety catch: a job
that costs money should not start because someone clicked the wrong button.

Edit `.env`:

```
ENABLE_EXA_SEARCH_DISCOVERY=true
```

Then restart the Airflow containers so they pick the change up:

```bash
docker compose up -d airflow-scheduler airflow-dag-processor airflow-apiserver airflow-triggerer
```

The flag names are `ENABLE_` plus the DAG name in capitals: `ENABLE_URL_DISCOVERY`,
`ENABLE_MISTRAL_SEARCH_DISCOVERY`, and so on.

### Step 1b — optional: skip the companies that no longer exist

**7,673 of the 14,443 companies in this table are `erloschen`** — dissolved — and 472 more are `in
Liquidation`. Between them they yielded 36 of the 902 websites ever found, 4.0%. One flag makes every
discovery DAG ignore them:

```
DISCOVERY_ACTIVE_ONLY=true
```

It applies to every DAG at once, because they all pick their companies through the same five queries
in `source_enrichment_common.py`. Restart the Airflow containers as above for it to take effect.

Two warnings, both learned the hard way:

1. **It is not a subset.** The batch limit applies *after* the filter, so a run of 50 companies with
   the flag on is 50 **live** companies — different companies, not fewer. Measured on the sampling
   order used by the combined-discovery script, `--limit 200` unfiltered holds 78 live companies while
   `--limit 200` filtered holds 200, overlapping by exactly those 78. The saving appears on a full
   pass over the table (14,443 → 6,298), not on a limited one.
2. **Nothing in this repo's published numbers was measured with it on.** Compare a filtered run only
   against another filtered run.

### Step 2 — unpause it in Airflow

Separate from the flag. Airflow will not start a paused DAG even if the flag is true.

```bash
docker compose exec airflow-scheduler airflow dags unpause exa_search_discovery
```

Check which are paused:

```bash
docker compose exec airflow-scheduler airflow dags list
```

The `is_paused` column: `True` means it will not run.

### Step 3 — trigger it

```bash
docker compose exec airflow-scheduler airflow dags trigger exa_search_discovery
```

Or open **http://localhost:8082** (`airflow` / `airflow`) and press the play button.

> The port is `8082`, not `8080`. `docker ps` shows `8080/tcp` for the Airflow containers, but
> that is the port *inside* the container; the host port comes from `AIRFLOW_PORT`. If another
> Airflow project is running on this machine it will usually have taken 8080, and logging in
> there fails with "invalid login" — different instance, different users. Confirm with:
>
> ```bash
> docker compose ps --format "{{.Service}} {{.Ports}}"
> ```

**One run handles 50 companies**, set by `SOURCE_ENRICHMENT_BATCH_SIZE`. It picks companies it
has not seen before, skipping any whose `data_quality_score` is already 0.90 or higher, so
money is not spent on companies that are already complete.

Trigger it again to do the next 50. It remembers where it stopped, in a checkpoint file under
`airflow/data/`. To start over, delete that file:

```bash
rm airflow/data/exa_search_discovery_checkpoint.json
```

### Step 4 — nothing

`pipeline_coordinator` runs automatically when the discovery job finishes. It is what turns
observations into answers. You do not need to trigger it.

> **Do not trigger `pipeline_coordinator` by hand more often than necessary.** It has a bug:
> every run adds duplicate rows to `company_managers`, because the table has no unique
> constraint for its `ON CONFLICT DO NOTHING` to catch. It is otherwise safe to re-run.

---

## 2b. Do they interfere with each other? No. What order should you run them in?

### They are independent

Each DAG keeps its own record of which companies it has handled: its own source key, its own
checkpoint file, its own observations. Running `exa_search_discovery` does not stop
`parallel_search_discovery` from running on the same company, and neither overwrites the other.

They meet in one place only: `pipeline_coordinator`, which picks a winner per field. Two sources
that independently produce the same value give that value **+0.10 confidence**, capped at 1.0.
Ties are broken by source priority, lower number first. So running more sources is never
destructive; the worst case is that you paid for a claim that lost.

### Recommended order

1. **`url_discovery`** — free. Guesses URLs from the company name and checks them. Run this on
   everything first.
2. **`exa_search_discovery`** — the paid one that all published numbers came from. About
   $0.014 per company.
3. **A second search provider** if you want the agreement bonus, or to compare providers.

### The order does NOT save you money, and here is why

There is a cost guard in `fetch_companies_for_llm_enrichment`: paid sources skip any company
whose `data_quality_score` has already reached **0.90**. The idea is that once a company is
nearly complete, there is no point paying to look again.

**That guard can never fire.** The score is a weighted sum of six fields:

| Field | Weight | Does anything write it today? |
|---|---|---|
| `website` | 0.30 | yes |
| `cluster` | 0.20 | yes |
| `tech_field` | 0.20 | yes |
| `ceo_contact` | 0.15 | yes |
| `revenue` | 0.10 | **no** |
| `employee_count` | 0.05 | **no** |

Nothing in the pipeline produces revenue or employee counts, so the ceiling is **0.85**, and
0.85 is below 0.90. Measured on the current database: the best-scoring company sits at 0.700 and
**zero** companies are above the threshold.

So a paid DAG will re-process a company that already has a perfectly good website, found for
free by `url_discovery` an hour earlier. Running the cheap ones first buys you coverage, not
savings.

Two ways to fix it, neither done yet:

- lower the threshold to 0.60, so a company with a website plus an industry is left alone; or
- select on "has no canonical website yet" instead of on the quality score, which is what you
  actually mean when the goal is finding websites.

Until then, if you want to stop paying to re-find websites you already have, restrict the batch
by hand before running a paid DAG, or accept the duplication.

## 3. Where the results are

### The two views to use

Both are in the `unternehmensdatenbank` database. In pgAdmin (http://localhost:5051,
`admin@example.com` / `admin`): Schemas → public → Views → right-click → View/Edit Data.

| View | One row per | Use it for |
|---|---|---|
| `v_discovery_results` | company | "what did we find for this company?" |
| `v_discovery_scorecard` | source | "what percent does this provider find?" |

### The one column that matters most

`website_verified_by` in `v_discovery_results`. Three values:

| Value | Meaning | Trust |
|---|---|---|
| `register_number_match` | we downloaded the page and its register number matched ours | highest |
| `url_pattern` | we downloaded the page, the Impressum fitted, no register number confirmed it | good |
| `llm_snippet` | an AI read a search result and named a URL, nothing was checked | **none** |

An **Impressum** is the legal contact page every German business website must have. It is what
lets us prove a site belongs to a specific company.

A row with `llm_snippet` is a lead to follow up, not an answer. It is deliberately capped at
0.34 confidence, below the 0.35 the pipeline requires, so it never becomes the official website.

---

## 4. Queries for common questions

Run them in pgAdmin's Query Tool, or from the shell:

```bash
docker compose exec -T postgres psql -U app -d unternehmensdatenbank -c "<the query>"
```

**How is each provider doing?**

```sql
SELECT * FROM public.v_discovery_scorecard;
```

Read `website_verified` and `owner_from_impressum`. Ignore the `_guess` and `_llm` columns
unless you are specifically studying what the AI claimed. `companies_tried` is the denominator —
a 66% success rate over 3 companies means nothing.

**What did we find, for the companies we have looked at?**

```sql
SELECT company_name, postcode, website_url, website_verified_by, owner_name, owner_found_via
FROM public.v_discovery_results
WHERE cardinality(processed_by) > 0
ORDER BY website_url NULLS LAST;
```

**Only the results I can actually stand behind:**

```sql
SELECT company_name, website_url, website_confidence, owner_name
FROM public.v_discovery_results
WHERE website_verified_by IN ('register_number_match', 'url_pattern')
ORDER BY website_confidence DESC;
```

**Which companies did we try and fail on?** Useful for judging whether the failure is ours or
whether the business simply has no website.

```sql
SELECT company_name, address, postcode, processed_by
FROM public.v_discovery_results
WHERE cardinality(processed_by) > 0 AND website_url IS NULL
LIMIT 50;
```

**How much of the whole database is covered?**

```sql
SELECT
  COUNT(*)                                               AS companies,
  COUNT(*) FILTER (WHERE cardinality(processed_by) > 0)  AS looked_at,
  COUNT(*) FILTER (WHERE website_url IS NOT NULL)        AS have_website,
  COUNT(*) FILTER (WHERE owner_name IS NOT NULL)         AS have_owner
FROM public.v_discovery_results;
```

**Did the last run actually work?** Every run is logged, whether it succeeded or not.

```sql
SELECT s.key, r.status, r.started_at, r.finished_at,
       r.records_seen, r.records_created, r.records_failed
FROM public.source_sync_runs r
JOIN public.sources s ON s.id = r.source_id
ORDER BY r.started_at DESC
LIMIT 10;
```

`status = 'partial'` means it stopped early, almost always because the provider rate-limited us.
Trigger it again; it resumes from its checkpoint.

**Raw evidence for one company**, including the pages that were rejected:

```sql
SELECT o.field_name, o.value_text, o.confidence, o.metadata
FROM public.company_field_observations o
JOIN public.companies c ON c.id = o.company_id
WHERE c.company_name ILIKE '%part of the name%'
ORDER BY o.field_name;
```

`field_name = 'directory_url'` is a page that mentions the company but is not its website — a
register aggregator or a business directory. Kept as evidence, never used as an answer.

---

## 5. When the numbers look wrong

| What you see | What it means |
|---|---|
| A view is empty | No discovery DAG has finished yet. Check `source_sync_runs`. |
| `companies_tried` is tiny | Only a couple of runs have happened. 50 companies per run. |
| A percentage looks great | Check `companies_tried` first. 2 out of 3 is not 67%. |
| `owner_from_impressum` is 0 | Results predate the 11 Aug 2026 extraction fix. Re-run the DAG. |
| Lots of `hit_directories_only` | The search is drifting toward pages *about* the company. |
| DAG missing from Airflow | `airflow dags list-import-errors` |
| DAG fails instantly | Its `ENABLE_` flag is not `true`, or its API key is empty. Read the task log. |

---

## 6. What this pipeline can realistically find

Measured on 100 Osnabrück companies. Do not expect better than this:

- a website you can prove: **29 in 100**
- an owner name you can prove: **15 in 100**
- roughly 1,850 tokens and about $0.014 of search per company

68 of those 100 produced no verified website at all. Many small German businesses genuinely do
not have one, so a number well below 100% is expected rather than a failure.

Method and full numbers: [website-and-owner-discovery-results.md](website-and-owner-discovery-results.md).
