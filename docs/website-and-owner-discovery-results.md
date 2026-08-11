# Can AI find a company's website and owner, given only its name and address?

We measured this on 11 August 2026. This page is the answer, the numbers behind it, and how
to reproduce them.

**Short version:** for about 3 in 10 companies we find a website we can actually prove belongs
to them. For about 1 in 3 we get an owner name, but most of those come from an AI reading a
search result, not from a document we checked. The Handelsregister number — which we assumed
would help — makes things *worse*. The two Mistral models we compared are equally good, so use
the cheap one.

---

## Some words you need first

This project invented a few words. Here is what they mean.

- **Impressum** — German law requires every business website to have a legal contact page. It
  usually lists the address, the managing director's name, and the company's register number.
  It is the single most useful page on a German company website, because it lets us *prove* a
  site belongs to a specific company.
- **Handelsregister number** (`registration_number` in the database) — the ID a German company
  gets in the commercial register, like `HRB 217094`. Every official record about the company
  carries it.
- **Verified vs. guessed** — a *verified* website means we downloaded the page, found its
  Impressum, and the details matched the company we were looking for. A *guessed* website means
  an AI read some search results and told us a URL. We store both, but we trust them very
  differently.
- **Confidence** — a number from 0 to 1 we attach to every fact we discover. The pipeline only
  promotes a website to "this is the company's real website" above 0.35. An AI guess is capped
  at 0.34, deliberately, so a guess can never win against a checked page.

## What we tested

- **100 companies**, picked from the 14,443 Osnabrück records in the database. The pick is
  deterministic (`ORDER BY md5(id)`), so every variant below ran on the *exact same* 100
  companies. That matters — otherwise you are comparing two different sets of companies and
  calling it a comparison.
- **Search engine:** Exa. It was the only provider with a working API key.
- **Two input modes:**
  - `name_address` — we blank out the Handelsregister number, so the system truly only knows
    the name and the address. This is what the requirement asked for.
  - `with_reg` — the register number is included, which is what the code does today.
- **Two AI models** for reading the search results: `ministral-8b-latest` and
  `mistral-small-latest`. Both are Mistral, both run in the EU.

## How the system tries to find a website

There are two independent routes, and telling them apart is the whole point.

**Route A — check it yourself.** Search, take the top 5 results, actually download each page,
look for its Impressum, and compare what it says against what we know. If the register number
matches, confidence is 1.0. If only the postcode matches, confidence lands between 0.365 and
0.65 depending on how distinctive the company name is. A page that turns out to be a business
directory gets thrown out even if it mentions the company perfectly — a directory page is
*about* the company, it is not the company's website.

**Route B — ask the AI.** Paste the search results into a Mistral model and ask it for the
website, the owner, and the industry. Fast and cheap, but nothing was checked, so it is capped
at 0.34 confidence and never becomes the official answer on its own.

---

## Result 1: the register number makes discovery worse

Route A only, since this route does not involve an AI at all.

| Input | website (verified) | owner from Impressum | phone or email | search found nothing useful |
|---|---|---|---|---|
| **name + address only** | **29 / 100** | **7 / 100** | 20 / 100 | 29 / 100 |
| with register number | 21 / 100 | 3 / 100 | 14 / 100 | 17 / 100 |

Because both ran on the same 100 companies, we can compare company by company:

- 18 companies: both modes found the website
- **11 companies: only name + address found it**
- 3 companies: only the register number version found it
- 68 companies: neither found anything

An exact McNemar test on the 11-vs-3 split gives a two-sided *p* of **0.057**. In plain terms:
the effect points clearly in one direction, but with only 100 companies we cannot rule out luck
at the usual 5% bar. Treat it as "strongly suggestive", not "proven".

**Why does it happen?** Adding the register number to the search query gets you *more* results
— the "found nothing" rate drops from 29% to 17%. But those extra results are Handelsregister
aggregator sites, insolvency trackers, and company directories, because those are the pages
that quote register numbers. The search gets pulled toward pages *about* the company and away
from the company's own site. Both modes had exactly 176 crawled pages rejected as third-party
directories.

**So what:** the "only name and address" constraint in the requirement is not a limitation we
have to work around. It is the better setting. Nobody needs to go find register numbers first.

## Result 2: the two Mistral models are interchangeable

Same 100 companies, same search results, same prompt — only the model changed. This is a fair
test precisely because the search step ran once and was cached, then replayed.

| Input | Model | owner found | website found | industry found | tokens in | tokens out |
|---|---|---|---|---|---|---|
| name + address | ministral-8b | 28 / 100 | 28 / 100 | 18 / 100 | 2,014 | 67 |
| name + address | mistral-small | 32 / 100 | 27 / 100 | 15 / 100 | 2,036 | 48 |
| with register no. | ministral-8b | 40 / 100 | 31 / 100 | 19 / 100 | 1,840 | 67 |
| with register no. | mistral-small | 37 / 100 | 29 / 100 | 19 / 100 | 1,840 | 48 |

Comparing the models company by company, every difference is noise:

| Comparison | 8b only | small only | *p* |
|---|---|---|---|
| owner, name + address | 6 | 10 | 0.45 |
| website, name + address | 6 | 5 | 1.00 |
| owner, with register no. | 12 | 9 | 0.66 |
| website, with register no. | 6 | 4 | 0.75 |

**So what:** `ministral-8b-latest` is the right default, which is what the code already does
(`mistral_extract.py:31`). The bigger model is not better at this job. This job is mechanical —
copy facts out of text — and does not need a smarter model.

One practical note: `mistral-small` is rate-limited harder on our account. Our first run lost
60 of 100 companies to HTTP 429 errors before we added retry-with-backoff. If you compare
models without retries, you are measuring rate limits, not quality.

## Result 3: how many tokens one business costs

Per company, per extraction call:

- **input: ~1,850 tokens** (median 1,846; range 250 to 4,459)
- **output: ~50–70 tokens**

Output is tiny because we ask for a small JSON object. Input dominates, and it is driven by how
much page text the search engine hands back.

Scaling to all 14,443 Osnabrück companies, once:

- roughly **27 million input tokens** and 0.9 million output tokens
- Exa search: measured at **$0.014 per company** → about **$200**
- Mistral: check the current price list before quoting a figure. We measured tokens, not euros.

## What this means for the requirement

The requirement asked: using only name and address, automate finding the business URL and the
owner's name.

| What you asked for | What we can deliver | How much to trust it |
|---|---|---|
| Business URL | **29%** | High — we downloaded the page and matched its Impressum |
| Business URL | 40% | Low — includes AI guesses we never checked |
| Owner name | **7%** | High — read off a verified Impressum |
| Owner name | 31–34% | Low — mostly an AI reading a search snippet |
| Both, same company | ~20–22% | Mixed |

Read the bold rows as the honest answer. **29% for websites, 7% for owner names.**

Two things drag the owner number down, and both are fixable:

1. We only extract the owner from an Impressum *after* we already accepted the website. So
   owner coverage can never exceed website coverage on the trustworthy route.
2. Exa is configured with `EXA_CONTENT_MODE=highlights`, which returns short excerpts chosen to
   match the search query. The line naming the Geschäftsführer often is not in the excerpt.
   Switching to `text` would likely raise owner recall. This has not been measured yet.

Also worth knowing: 68 of 100 companies produced no verified website by either route. A lot of
small German businesses genuinely do not have a website, so a ceiling well below 100% is
expected. We have not measured how much of that 68 is "no website exists" versus "we failed to
find it", and that is the single most useful next measurement.

## Reproducing this

The harness is `scripts/ac_measurement.py`. It writes nothing to the database on purpose — it
needs to re-run and compare, and a script that also changed state could not do that. It reuses
the pipeline's own filtering, scoring and prompt rather than reimplementing them, so the numbers
describe the real system.

It runs in three stages. Stage 1 is expensive and cached; stages 2 and 3 are cheap and repeatable.

```bash
R="docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-scheduler \
   python /opt/airflow/scripts/ac_measurement.py"

# 1. search + download + check Impressum. Costs Exa money. Cached, so safe to re-run.
$R stage1 --reg-mode name_address --limit 100
$R stage1 --reg-mode with_reg     --limit 100

# 2. AI extraction, replayed over the cached search results. No search cost.
$R stage2 --reg-mode name_address --model mistral:ministral-8b-latest
$R stage2 --reg-mode name_address --model mistral:mistral-small-latest

# 3. print the tables above. Runs on the host too, no container needed.
python scripts/ac_measurement.py report
```

Output files land in `airflow/data/` and are gitignored, because they contain managing-director
names — that is personal data and must not be committed.

Adding another model is one flag: `--model ollama:llama3.2:latest` routes to a local Ollama via
its OpenAI-compatible endpoint. Ollama was not running when we measured, so a local model is
still untested.

## What we did not test

- **Other search providers.** Only Exa had a working key. Parallel, Brave and Claude are
  implemented but unkeyed, so "which search provider is best" is still open.
- **A local model.** Ollama was down.
- **`EXA_CONTENT_MODE=text`.** The likeliest single improvement to owner recall.
- **Whether "no website found" means "no website exists".** The most valuable thing to check
  next, because it tells you whether 29% is a failure or close to the real ceiling.
