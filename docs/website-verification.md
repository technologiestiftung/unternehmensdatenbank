# How website discovery decides a page belongs to a company

> **Status 2026-10-28.** Written on the Osnabrück example data, which has been removed. The rules
> here still hold; the numbers were measured on that data and Berlin replaces them. The commands
> of the old discovery CLI (`run_combined_discovery.py`) and the Ollama stages are gone; the code
> lives in `airflow/dags/enrichment/`, and the DAGs that run it are being rebuilt (issue #18).

Combined discovery guesses domains from a company's name, downloads what answers, and decides whether
each page is that company's own website. This file is about the deciding — the part that turned out to
matter far more than the finding.

Everything here was measured on the same 1,000 Osnabrück companies on 2026-08-14.

Words used below, defined once:

- **candidate** — a URL some stage proposed for a company.
- **evidence** — a named fact about a candidate, like `company_name_on_page`. `confidence.py` turns a
  set of these into one number between 0 and 1.
- **the judge** — a local language model that reads a page already downloaded and says whether it
  belongs to the company. It never searches and never proposes URLs.
- **promotable** — scored at or above 0.35, the bar for being reported as a website.

## The result

A website for **53 of 1,000 companies (5.3%)**: 52 in the `high` band, 38 proven by a court-register
number printed on the page. Up from 47 that morning — and the entire gain came from reading pages more
carefully, not from finding more of them.

For contrast, the paid search path measured 29% verified on 100 companies. The gap is not a
verification problem: for **665 of the 954 companies with no website we had already downloaded a page
and refused it.** Only 289 never yielded a live page at all, and those are the ones no amount of
better reading can reach.

## Over half the companies were never worth asking about

Measured 27 August 2026, when `register_status` was promoted out of `metadata` into its own column.
The register says whether a company still exists, and for this table it mostly says no:

| | `aktiv` | `erloschen` | `in Liquidation` |
|---|---|---|---|
| companies | 6,298 (43.6%) | **7,673 (53.1%)** | 472 (3.3%) |
| of the 902 websites found | 866 | 29 | 7 |
| of the 842 in the `high` band | 813 | 22 | 7 |
| Technologiefeld `assigned` (346) | 335 | 9 | 2 |
| purpose-column model answers (567) | 335 | 232 | — |

So **56.4% of every URL guess, page download, judge call and tie-break went to a company that no
longer trades**, and returned 4.0% of the websites. `DISCOVERY_ACTIVE_ONLY=1` skips them. For Berlin it
is on (`.env.example` and docker-compose); the code default stays off. It changes which companies a
limited run selects, so the numbers above are the *unfiltered* baseline and a filtered run is only
comparable to another filtered run.

Two things worth noticing beyond the runtime. First, the hit rate on `in Liquidation` companies is the
same as on live ones — all 7 of their websites are `high` — which is what you would expect of a
business that is winding down but still trading; they are counted as inactive anyway, because 472
companies of work is not worth 7 websites. Second, a dissolved company's domain is the classic
resold-domain case, so those 29 `erloschen` websites deserve more suspicion than their confidence
scores carry. Nobody has labelled them.

## The bottleneck is now the model, not the network

With the page cache warm the median company costs **0.8 seconds**, down from 10.1. The remaining ~69
minutes per 1,000 companies is ~281 Ollama calls queueing on one GPU. Fetching is effectively free.
Nothing gets faster now except by making fewer judge calls or using a faster model.

## What can be replayed offline, and what cannot

`results.jsonl` stores each candidate's `methods` and `evidence`, which is everything
`confidence.score` reads. So **a scoring change can be re-measured in seconds** with no network, no
model and no database. `scripts/replay_judge_gate.py` did it, and printed whether re-scoring still
reproduced every stored value; it was removed with the old CLI on 2026-10-28.

**An extraction change cannot** be replayed from `results.jsonl` — that needs the pages themselves,
which is what the page cache is for. Scores are frozen at write time and `report`/`csv` read the
stored values, so a scoring change never applies retroactively to an old file.

### Pages are cached, so extraction changes are free to try

`COMBINED_PAGE_CACHE_DIR`, default on for the script and off in Airflow — a DAG must see today's web,
and a cache would let it report a page that died last month as a current website.

Raw HTML and status only. Whether a page looks parked, what postcode is on it, what its register
number is — all recomputed on read, because caching a *conclusion* would freeze the code the cache
exists to let you change. About 150–300 MB per 1,000 companies, gzipped, under the gitignored
`airflow/data/`. That gitignore matters for more than tidiness: cached Impressum HTML carries
managing-director names.

## The judge gate — where two thirds of the runtime went

The judge was ~69% of the run's wall clock and answered `unrelated` for 743 of 767 candidates.
`pipeline._worth_judging` now also requires the page to carry **at least one fact naming the
company**: its name, its city, its postcode, or either of the last two in the page's structured data.
Measured over the stored run, 510 of 767 calls went to pages carrying none of those, and **not one
website was ever rescued from such a page.**

The gate is a **veto only**. There is deliberately no rule that skips the judge because a candidate
looks convincing — the judge's main product is its rejections, and those land on exactly the
convincing-looking candidates. Cheap evidence may cancel a model call; it may never stand in for one.

### The first version of that gate was not safe

It removed 514 calls and lost no discoveries — and lost **190 demotions**, meaning 190 wrong websites
came back. Those candidates had reached the promotable band on `base + host_is_exact_name +
imprint_found` alone, with nothing on the page naming the company, because `imprint_found` counted as
verification. Every German business site has an Impressum, so finding one says the page is *a*
business, never *which* one.

Removing `IMPRINT_FOUND` from `confidence._VERIFYING_EVIDENCE` left promotable candidates unchanged at
49 with an identical `verified_by` breakdown, and dropped the forced demotions from 190 to 0. **The
model had been paid 190 times to clean up a scoring bug.** It keeps its +0.08 weight; only the
cap-lift is gone.

The lesson generalises, and it is the first thing to check when a gate looks free: counting only lost
*discoveries* hides lost *rejections*. `replay_judge_gate.py` failed on either.

## Do not ask the model what a regex can answer

- A register number printed on a page with **no findable Impressum** used to reach the score only via
  the judge. `_page_evidence` reads it directly now, behind the same host check as everything else.
- `phone`, `email` and `vat_id` were being extracted and thrown away. The phone is now scored against
  the number OpenStreetMap holds for the business — real identity evidence, since a phone number is
  not a word anybody could guess. The other two are scored as the weak "this is a real business site"
  class that cannot lift the cap.
- **JSON-LD**, the schema.org block search engines read, is parsed by
  `enrichment/structured_data.py`. It is the only place a site states its own name as *data*,
  which matters because own-site detection otherwise rests entirely on matching the domain string
  against the company name.

### All five registers, and every entry on the page

Germany has five court registers and only two were being read. `HRA`/`HRB` is the Handelsregister; an
`e. V.` gets `VR`, a cooperative `eG` gets `GnR`, a `Partnerschaft mbB` gets `PR`.

The bigger defect was reading only the **first** entry on a page. A holding company's Impressum
routinely prints `HRA 5121` and `HRB 16630`, and when the company's own entry was the second one, the
page counted as printing *a different company's* register number — which disqualifies. Fixing that
alone recovered **5 websites at 0.99 with hard proof**: Felix Schoeller, HunTac, Vela Dare,
Radio-Elektro Mense, intan media services.

Two rules keep the comparison sound, both guarded:

1. **The register has to match, not just the number.** The five registers number independently, so
   `VR 213220` and `HRB 213220` are different entities. A page carrying only a `VR` number when this
   company is an `HRB` is therefore *neither* a match nor a mismatch — a mismatch disqualifies, and
   that is too strong a conclusion to draw from an unrelated register.
2. **Reading an entry off the page text needs the whole entry, never the bare digits.**
   Bäko-Osnabrück is `GnR 205`, and searching a page for "205" finds a price or a house number. That
   briefly awarded hard proof on a coincidence while this was being built.

### Structured data can lift the name refusal

A company's website does not have to contain the company's name. `HOST_NAME_MISMATCH` exists because a
*guess* that missed the name landed on somebody else — but JSON-LD is not a guess and owes nothing to
the domain string, so it lifts that refusal (`confidence._LIFTED_BY`). Never for a `company_listing`
path: a listing's structured data names the company it is listing, which is this repo's oldest trap
wearing a new hat.

Two consequences, each of which was a bug first:

- **A lifted refusal must not skip the judge.** `_worth_judging` asks
  `confidence.standing_disqualifiers` rather than testing for `HOST_NAME_MISMATCH` directly, or the
  rows the lift just rescued would silently never be read.
- **A lifted refusal must not stop the page being read.** `_page_evidence` used to return the moment
  it saw a name mismatch, so `thebigbash.de` and `gmh-gruppe.de` scored ~0.55 on a guess while the
  company's own HRB number sat unread further down the same page. Both now score 0.99.

### A brand before the legal form, and a number with a space in it

"3pc GmbH Neue Kommunikation" is a brand, `3pc`, then a description, and its website is 3pc.de
(issue #30). Three things stopped us finding it: `3pc` is shorter than the 4 letters a guessed stem
needs, the host check only counts name words of 4+ letters, and its Impressum prints `HRB 794 28`,
which was read as `HRB 794`: a different company's number, a disqualifier.

- `url_patterns.brand_stem` takes the words before a legal form that has more name after it. It is
  guessed first, and a host that *equals* it (never one that merely contains it) carries the name.
  19,119 of 142,571 active companies have such a brand. In a sample of 200, discovery proved 14
  websites, 6 of them through the brand.
- `extract_register_numbers` also reads `HRB 794 28` as `HRB 79428`, next to the plain reading. Only
  a tail of 1–3 digits is joined, so a postcode after the entry never is, and the joined number counts
  only if it is exactly the company's own.

Both changes only add: no published website was lost (234 re-checked on 2026-10-28; 4 fail the page
check with and without the change).

## A model's stated reason is not evidence

`cf-selbsthilfe-osnabrueck.de` was accepted in one run because the judge reported *"the Impressum
explicitly states ... register number VR 2217"*, and rejected in the next. Neither `VR` nor `2217`
appears anywhere in the 45,000 characters of either cached page. The model was told that number in its
prompt and echoed it as if it had read it.

`llm_judge.flags_for` already refuses to credit a number that is not on the page — but that guard
protects the *evidence*, not the free-text reason, and not the verdict itself. Do not read a
`judge_reason` as a fact about a page without checking the page.

The judge emits evidence flags, never a score, and its limits are enforced in code rather than asked
for in the prompt: it is not counted as a "method", its register numbers are verified against the page
before they count, it can never produce a register *mismatch*, and it cannot promote a page on its
own — but its rejection **caps** the score at 0.34, because a plain penalty let 5 of 11 promoted
websites be pages it had explicitly rejected.

## `report` counts; only `labels-report` scores

Nothing in `report` can say whether the pipeline is **right**. "53 websites" is not a score without
knowing how many of the 53 are real, and comparing a new version against the model's opinion only
measures agreement with the thing you are trying to replace.

`labels` writes a worksheet of ~60 companies in three groups and a human fills in one column:

| group | question it answers |
|---|---|
| websites we claim | **precision** — how many of the 53 are real? |
| candidates that were rejected | were the rejections right, or are real sites being thrown away? |
| companies we found nothing for | **the ceiling** — do they have websites at all? |

The ceiling group is the one that decides strategy. If most of those companies do have a website, 5.3%
is a recall problem worth paying a search API for. If they do not, buying search is wasted money.

The file is never overwritten — `labels` merges and keeps every label already written, including rows
that fall outside the current sample — and nothing in the pipeline may write to the `label` column. A
measuring instrument the measured thing can adjust is not one.

## Free web search does not work from this network

Not a configuration problem, and not fixable by slowing down. Self-hosted SearXNG answers for about a
dozen queries and then its upstream engines refuse. From SearXNG's own diagnostics:

```
['duckduckgo', 'CAPTCHA']                        <- rejection (93 of 97 errors)
['startpage',  'Suspended: CAPTCHA']             <- rejection
['brave',      'Suspended: too many requests']   <- rate limit
['google cse', 'Suspended: too many requests']   <- rate limit
```

Wikipedia also returned HTTP 429, and its API is generous — good evidence the egress IP is shared and
already flagged, so this is inherited reputation rather than anything the pipeline did.

Three traps worth knowing before repeating the experiment:

1. **`local_llm.availability()` gates on one control query**, so it cannot see a volume-dependent
   block. It reported the stage ON, and then 20 consecutive companies returned `no_search_results`.
2. **SearXNG suspends an engine after failures** — its defaults are 3600 s for a CAPTCHA, 180 s for a
   429, and 15 days for a Cloudflare CAPTCHA. So one burst costs an hour of silence, and pushing
   harder risks the 15-day tier.
3. **Never draw a conclusion from a search sweep without an interleaved control query.** A
   30-company test of "does search reach the companies URL guessing cannot" returned zero useful hits
   and looked like a real answer. It was measuring the CAPTCHA block.

Separately, and independent of the block: `source_enrichment_common.build_search_queries` emits
`("Name GmbH" OR "name") "Street 1, 49084 Osnabrück"`. Scraped engines return **0 hits for a quoted
street address** even when unblocked, where a plain `name + city` query works. That shape suits the
paid APIs it was written for, which measured 29% verified — so do not "fix" it globally. A SearXNG
path would need its own simpler query.

The 289 unreachable companies therefore need an authenticated search API. `exa_search_discovery`,
`brave_search_discovery` and `parallel_search_discovery` already exist for that; Brave's *API* is a
different product from the scraped `search.brave.com` that gets rate-limited here.

## Not yet measured on real pages

The phone/email/VAT evidence and the anchored postcode regex are covered by offline cases in
`scripts/check_website_discovery.py` but have not yet decided a run. Everything else on this page is
measured against the stored 1,000-company results and its cached pages.
