# Can AI find a company's website and owner, given only its name and address?

First measured on 10–11 August 2026, then re-measured on 11 August 2026 after two fixes this
page led to. It is the answer, the numbers behind it, and how to reproduce them.

**Short version:** for about 3 in 10 companies we find a website we can actually prove belongs
to them. For about 1 in 6 we get an owner name we can prove — up from 1 in 14, after fixing the
code that reads names off the page. The Handelsregister number, which we assumed would help,
makes things worse in *both* places it is used. The two Mistral models we compared are equally
good, so use the cheap one.

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
- **Three input modes.** The Handelsregister number has two separate jobs, and these modes turn
  them on and off independently:

  | mode | number in the search query? | number used to score a downloaded page? |
  |---|---|---|
  | `name_address` | no | no |
  | `with_reg` | yes | yes |
  | `scorer_only` | no | yes |

  `name_address` is what the requirement asked for. `with_reg` was what the code used to do.
  `scorer_only` is what the code does now, and it exists because the first two change both jobs
  at once and so cannot say which one mattered.
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

## Result 1: the register number makes discovery worse, in both places

Route A only, since this route does not involve an AI at all. The owner column shows the number
after the extraction fix described in Result 2; the number before it is in brackets.

| Input | website (verified) | owner from Impressum | phone or email | search found nothing useful |
|---|---|---|---|---|
| **name + address only** | **29 / 100** | **17 / 100** (was 7) | 20 / 100 | 29 / 100 |
| out of query, still scoring | 25 / 100 | 14 / 100 (was 5) | 16 / 100 | 29 / 100 |
| with register number | 21 / 100 | 13 / 100 (was 3) | 14 / 100 | 17 / 100 |

### The query effect: 21 → 25

Compare the bottom two rows. They differ only in whether the register number went into the
search query. Taking it out is worth 4 more websites per 100.

Adding the number to the query gets you *more* results — the "found nothing" rate drops from
29% to 17%. But those extra results are Handelsregister aggregator sites, insolvency trackers,
and company directories, because those are the pages that quote register numbers. The search
gets pulled toward pages *about* the company and away from the company's own site.

An exact McNemar test on the original 29-vs-21 comparison gives a two-sided *p* of **0.057**.
With only 100 companies we cannot rule out luck at the usual 5% bar. Treat the direction as
well supported and the exact size as rough.

### The scoring effect: 25 → 29, and it is a bug

Compare the top two rows. They ran the *identical* search query — byte for byte, checked for
all 100 companies. The only difference is whether `score_candidate` was allowed to compare the
register number on the downloaded page against ours. Letting it do so cost 4 websites.

That is backwards from what you would expect, so we checked it directly. Exa does not return
identical results for identical queries — only 51 of the 100 companies got the same pages back
in both runs. Restricting the comparison to those 51:

- 2 companies: the website was found without the register-number scoring, and lost with it
- 0 companies: the other way round

The mechanism is in `airflow/dags/url_discovery/scorer.py:77`. A register number that does
**not** match takes **200 points** off, and anything below zero is discarded. So a company's own
website is thrown away whenever its Impressum shows a register number different from the one we
hold — which happens legitimately: the site belongs to a group parent, or the page lists several
companies and the extractor picked the wrong number.

**So what:** the "only name and address" constraint in the requirement is not a limitation we
have to work around. It is the better setting. The query half of this is fixed — the register
number is out of the search query by default now
(`SEARCH_QUERY_INCLUDE_REGISTER_NUMBER=false`). The −200 penalty is **not** fixed and is the
next thing worth doing, because it is worth roughly the same 4 companies per 100.

## Result 2: owner names, 7 per 100 → 17 per 100

This was the weakest number in the first measurement, and the explanation first published here
was wrong. It blamed Exa's `EXA_CONTENT_MODE=highlights` setting. That cannot be the cause: the
verified owner comes from route A, which downloads the Impressum itself over plain HTTP. Exa's
content setting only shapes the snippets handed to the AI in route B.

The real cause was in our own parser. Of the 31 companies whose website we had verified, we
saved the Impressum page and counted what came off it:

- 26 pages gave up a phone number or an email address
- only 7 gave up a manager's name

The page was fetched and parsed either way. Two bugs in
`airflow/dags/url_discovery/imprint_extractor.py`:

1. **The line breaks were thrown away before the search for a name.** The HTML-to-text step
   joined every element with a space. The name-finding pattern is written to stop at a line
   break, so with no line breaks left it ran on for 70 characters past the label and grabbed
   whatever followed. The filter afterwards then rejected the whole run for having too many
   words. Worse, that 70-character run often swallowed the *next* label, so a page reading
   "Vertreten durch / Geschäftsführer: Anna Beispiel" was never even offered the second label.
2. **The whole line had to look like a name.** Real Impressum lines carry on past it — "Erika
   Beispiel Diplom-Kauffrau eingetragen beim Amtsgericht Osnabrück HRB 12345" is one line — so
   a name that was plainly there got discarded.

Now the text keeps one line per HTML element, and the parser takes the *leading* run of
capitalised words, stopping at the first thing that cannot be part of a name: a lowercase word,
a number, a street, a legal form, a job qualification. It also looks at the line *after* the
label, because German Impressums routinely put the label and the name in separate elements.

Result on those 31 saved pages: **19 now yield a name, up from 7.** Four of the remaining 12
have no manager label anywhere on the page, so they were never findable this way.

Precision was treated as more important than recall, because a wrong owner is worse than no
owner — nothing downstream can tell the difference. Company names, leading acronyms, street
names and single words are all rejected. `scripts/check_imprint_owner_extraction.py` is the
regression gate: 19 cases, one per page layout found in the sample, with invented names.

We confirmed the change touches nothing else: register number, VAT id, postcode, phone and
email come back byte-identical on all 31 pages.

## Result 3: the two Mistral models are interchangeable

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

## Result 4: how many tokens one business costs

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
| Owner name | **17%** | High — read off a verified Impressum |
| Owner name | 31–34% | Low — mostly an AI reading a search snippet |

Read the bold rows as the honest answer. **29% for websites, 17% for owner names.**

The owner number can never exceed the website number on the trustworthy route, because we only
read the Impressum of a site we already accepted. Raising website coverage raises both. The −200
scoring penalty in Result 1 is the cheapest remaining win.

Also worth knowing: 68 of 100 companies produced no verified website by either route. A lot of
small German businesses genuinely do not have a website, so a ceiling well below 100% is
expected. We have not measured how much of that 68 is "no website exists" versus "we failed to
find it", and that is the single most useful next measurement.

## Looking at results in pgAdmin

Two database views show what the live pipeline has found, as opposed to what this one-off
measurement found. Open pgAdmin at http://localhost:5051, then the `unternehmensdatenbank`
database → Schemas → public → Views.

- **`v_discovery_results`** — one row per company: the website, how it was established, the
  owner, and which search provider found them. It includes companies where nothing was found,
  because a view that hid the misses would make coverage look better than it is. The
  `processed_by` column distinguishes "no discovery job has run on this company yet" from "we
  looked and found nothing".
- **`v_discovery_scorecard`** — one row per search provider, with the same percentages this
  page reports, computed live. `companies_tried` is the denominator and counts attempts, not
  successes.

Read `website_verified_by` before trusting a row. `register_number_match` and `url_pattern`
mean the page was downloaded and checked. `llm_snippet` means an AI guessed it and nothing was
checked.

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
$R stage1 --reg-mode scorer_only  --limit 100
$R stage1 --reg-mode with_reg     --limit 100

# 2. AI extraction, replayed over the cached search results. No search cost.
$R stage2 --reg-mode name_address --model mistral:ministral-8b-latest
$R stage2 --reg-mode name_address --model mistral:mistral-small-latest

# 3. print the tables above. Runs on the host too, no container needed.
python scripts/ac_measurement.py report
```

If you are on Git Bash, run these through PowerShell instead. Git Bash rewrites
`/opt/airflow/dags` into a Windows path on its way into the container, and the import fails with
a confusing `ModuleNotFoundError`.

For owner-extraction work there is a second, free harness:

```bash
# save the Impressum pages of every company with a verified website (no API cost)
$R:collect_imprint_fixtures.py collect
# study the layouts with every letter and digit masked, so no names are exposed
python scripts/collect_imprint_fixtures.py shapes
# count how many saved pages currently yield a name
python scripts/collect_imprint_fixtures.py score
# the regression gate
python scripts/check_imprint_owner_extraction.py
```

Output files land in `airflow/data/` and are gitignored, because they contain managing-director
names — that is personal data and must not be committed.

Adding another model is one flag: `--model ollama:qwen2.5:1.5b` routes to the local Ollama
container (`docker compose --profile llm up -d ollama`) via its OpenAI-compatible endpoint.
`--limit N` cuts the sample to the first N companies, which a local model needs to be
measurable at all.

## What we did not test

- **Other search providers.** Only Exa had a working key. Parallel, Brave and Claude are
  implemented but unkeyed, so "which search provider is best" is still open.
- **`EXA_CONTENT_MODE=text`.** It cannot affect the verified owner number — that was the
  mistake corrected in Result 2 — but it should raise route B's owner rate, at the cost of more
  input tokens per company. Worth measuring as a separate question.
- **Whether "no website found" means "no website exists".** The most valuable thing to check
  next, because it tells you whether 29% is a failure or close to the real ceiling.
- **Removing the −200 register-mismatch penalty.** Worth about 4 companies per 100 on the
  evidence in Result 1, but changing the scoring ladder affects the whole confidence model, so
  it needs its own measurement.
