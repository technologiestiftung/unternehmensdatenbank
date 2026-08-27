# Column 3: what the register says the company was set up to do

Every company registered in Germany files a **Gegenstand des Unternehmens** — one or two sentences
saying what the company is for. It is written by a notary, it describes what the company is
*permitted* to do, and it is filed once, at registration, and rarely changed. It arrives here as
`public.companies.business_purpose`.

This column turns that text into `purpose_field`: one of Berlin's 77 technology fields, or a recorded
non-answer. Run on 2026-08-26 over 14,443 companies.

Read [branch-classification](branch-classification.md) first if you have not. This document assumes
you know what a Technologiefeld is and why it requires a website.

---

## Why this is a third column rather than more coverage in the second

On 2026-08-25 the Technologiefeld was deliberately narrowed: a field requires a website. The argument
was that a register purpose says what a company is *permitted* to do, and much of it is boilerplate,
while a live website is evidence the company actually trades. That rule cost 563 answers and it was a
considered price.

Filling the same column from register purposes would have silently reversed that decision. So the
register gets its own column, in its own file, with its own outcome names. The two can then be
compared — which is a measurement nobody had before — instead of being merged into a number that
means neither thing.

**They are never a check on each other.** A company registered to trade goods that now sells software
has two true answers. Where they disagree, that is a finding about the company, not an error.

---

## What it produces

| | |
|---|---|
| companies | 14,443 |
| purpose empty | 6,902 |
| purpose a 1–40 character stub | 400 |
| **`no_purpose`** | **7,302** (50.6%) |
| purpose usable — over 40 characters, 251 on average | **7,141** (49.4%) |
| …of those, containing at least one taxonomy keyword | **660** (9.2%) |

Seven outcomes. **They sum to 14,443 exactly, and no two may ever be added together.**

| outcome | count | what it means |
|---|---|---|
| `assigned_keywords` | **596** | one field beat both thresholds on keywords alone |
| `assigned_model_3way` | **14** | was `torn`; a model chose among the 3 candidates the keywords nominated |
| `assigned_model_77` | 0 | not run — this is Tier B, see below |
| `none_apply` | **3** | a model read it and said no Berlin field fits |
| `unsure` | 0 | a model read it and said the text does not say enough |
| `torn` | **47** | keywords fired, top two too close, and no model verdict |
| `no_keyword` | **6,481** | we read it and not one of the 500 keywords appeared |
| `no_purpose` | **7,302** | empty, whitespace, or 40 characters or fewer. Never read |

**610 fields**, 8.5% of the usable purposes. `decided_by` splits them: **keywords 596, model_tiebreak
14**.

### The four `NONE` reasons are four different claims

- `no_purpose` — the register said nothing we could read. Says nothing about the company.
- `no_keyword` — we read it and none of Berlin's words appeared. Says the taxonomy has no word for
  this business, which is a fact about the taxonomy.
- `torn` — two fields were too close. Says our weights could not separate them.
- `none_apply` — a model read it and concluded no Berlin field fits. **The only one of the four that
  is a claim about the company.**

`unsure` is a fifth: the model read it and could not tell. Kept apart from `none_apply` because "no
field fits this company" and "this text is too thin" are different findings.

---

## How a field is decided

Keywords only, unless `--llm` is passed. The scoring is `branch_classifier`'s, reused rather than
reimplemented — same `normalize`, same `_pattern_for`, same `keyword_weight`, same `MIN_SCORE` 2.0 and
`MIN_MARGIN` 1.5.

```
idf    = log(max(n_companies, 1) / (1 + df))       # 0.0 if idf <= 0
weight = idf * (1.0 + 0.5 * (words - 1)) / claimants
```

Two things differ from the website pass, and both matter:

1. **Document frequency is computed over purpose texts only**, with `n_companies` = 7,141. Register
   prose is a different vocabulary from homepage copy; weighting it by how often a word appears on
   websites would rank the wrong words.
2. **The stubs are matched but excluded from the frequencies.** Otherwise the length floor would be
   shifting the scores of the very rows it claims to have no opinion about.

### The thresholds were calibrated on website text, and they turn out to be fine

Purposes are 251 characters against several thousand for a homepage, so every score sits lower. It was
worth checking whether the floor was throwing away answers. It is not:

| | |
|---|---|
| rows that scored anything | 660 |
| …lowest / median / highest | 1.93 / 5.54 / 18.93 |
| …of those, the ones that won a field | 596 |
| …lowest / median / highest | **3.48** / 5.54 / 18.93 |
| scored something but under the floor | **18** |

Every winner cleared 3.48, comfortably above the 2.0 floor, and only 18 rows fell under it. **The
floor is not what limits this column — the taxonomy is.** 6,481 companies contain no word for any
industry Berlin's list names.

### 94.5% of fields rest on a single keyword

563 of the 596 keyword-decided rows fired exactly one keyword. That is not a bug — register prose is
short — but it is the reason this column deserves less trust than the website column, where four
different texts can agree with each other.

---

## The length floor costs 17 real fields, and they are kept

A purpose of 40 characters or fewer is never read. The stated reason was that a stub names no trade
specifically enough to classify. **Measured, that reason is wrong.**

| field it cost | rows | shortest text |
|---|---|---|
| Gastgewerbe | 6 | `Gastronomie` (11 chars) |
| Handel | 4 | `Textil-Einzelhandel` (19) |
| Gesundheitsversorgung & Pflege | 3 | `Pflegedienst` (12) |
| Logistik | 2 | `Betrieb einer Spedition` (24) |
| Musikwirtschaft | 1 | `Die Tätigkeit als Label und Musikverlag.` (40) |
| Telekommunikationsanbieter | 1 | `der Ausbau der Glasfaser Infrastruktur` (38) |

A short register purpose is often the **least** ambiguous one there is. "Gastronomie" names a trade and
nothing else; the 2,294 long ones about *Verwaltung eigenen Vermögens* name none.

The floor still applies — but the field it would have produced is kept in `withheld_by_floor` and
printed by `classify-purpose-report`, the same way the website gate keeps `withheld_branch`. **A veto
whose rejections are never counted cannot be told apart from a veto that never fires.**
`PURPOSE_MIN_CHARS` is the knob; lowering it to 10 would recover all 17.

---

## The model: two tiers, both off by default

### Tier A — the 3-way tie-break (`--llm`), run 2026-08-26

Only `torn` rows, and only those where the keywords built a shortlist. The model sees **3 candidates
plus `keine` plus `unsicher`** — never 77.

**Its reach is 17 companies, and that number is set by a constant, not by the prompt.**
`worth_asking` requires at least `BRANCH_MIN_KEYWORDS_FOR_MODEL` (2) distinct keywords, and **608 of
the 660 companies with any purpose keyword have exactly one**. So of 64 torn rows only 17 were ever
askable. If you want Tier A to reach further, that constant is the thing to argue with — and lowering
it means asking a model to break one-word-against-one-word ties, which is a coin flip in a confident
voice.

Result: 17 calls, 2 minutes 38 seconds on `qwen3.5:4b` (~9 s a call), **14 fields, 3 `keine`, 0
`unsicher`, 0 failures.** Re-running reports 0 model calls, all 17 served from cache.

All three refusals were genuine boilerplate, which is the prompt's boilerplate instruction working:

```
Holding mit Verwaltungsaufgaben.
Beteiligungsgesellschaft mit reinem Verwaltungszweck.
Verwaltung und Dienstleistungen für gemeinnützigen Verbund.
```

And the assignments track the register text rather than the company name:

```
Logistik              Spedition, Lagerung und Kontraktlogistik im Fokus.
Pharma & Wirkstoffe   Entwicklung und Verkauf von Arzneimitteln und Diagnostika.
Soziale Unternehmen   Betrieb von Werkstätten für behinderte Menschen.
Mobilitätssoftware    Unternehmen ist primär Software-Agentur für Mobilität.
```

3 of 17 is an 18% refusal rate, in line with the 15% measured on the website tie-break — so the known
under-refusal has not been fixed by the prompt, merely not made worse. On this sample it did not
matter, because all 17 rows had real keyword evidence behind them.

### Tier B — the 77-way ask, on the 6,481 rows keywords cannot reach

The 6,481 `no_keyword` rows are the only place real coverage is left, and reaching them means asking a
model to choose from all 77 with no keyword to anchor the answer. Every row it decides is marked
`decided_by = model_direct` and is the least trustworthy value in the column: nothing ties it to a word
anybody wrote.

`branch_llm`'s docstring warns against this, on measured grounds — asked from all 77 about a *web
page*, `qwen3.5:4b` reached for the vaguest label available and called a drain-cleaning firm
"Dienstleistung". **On register purposes it behaves better**, and the reason is worth stating: a
purpose is 251 formulaic characters written to describe one activity, where a homepage is 6,000
characters of marketing about everything a company wants to be associated with. The 77-way question is
much narrower when the input is narrow.

Cost: about **10 seconds a call** on `qwen3.5:4b`, so all 6,481 rows is roughly **18 hours**. Use
`--llm-limit` to spend a bounded number of new calls per run; cached answers do not count against the
budget, so re-running the same command continues rather than repeats.

#### The hosted alternative

Costed for `claude-haiku-4-5` via the **Message Batches API** (50% off, and a one-time backfill has no
latency requirement): ~700 input tokens a call (the 77-name block measures 1,483 characters ≈ 500
tokens), ~10 output, 6,481 calls → 4.5M input, 65K output → **$2.43 batched**, $4.86 synchronous.

Three things to get right when it is built:

1. **Prompt caching will silently do nothing.** The constant prefix is ~600 tokens and the minimum
   cacheable prefix is ~1024. Below that it fails to cache with no error. The Batches discount is the
   substitute.
2. **Batch results arrive in any order — key by `custom_id`**, which should be the company id.
3. **Do not pre-filter the boilerplate.** At $2.43 the saving is under a dollar, and the independent
   2,294-row boilerplate count is worth far more as a **calibration check**: if `keine` comes back on
   roughly a third of rows, the prompt is working. If it comes back on 10%, the model is
   under-refusing by the same factor of three and the column should not be trusted.
4. **The cache is shared, keyed by model.** `purpose_cache_key` puts the model name in the key and both
   backends write the `purpose-llm-direct` namespace, so a local run and a hosted run coexist: the
   hosted pass pays only for rows the local one has not answered, and both answers stay readable side
   by side for comparison.

---

## Commands

```powershell
python scripts/check_purpose_branch.py                          # free, first. 154 checks
python scripts/run_combined_discovery.py classify-purpose       # 13 s, no network, no model
python scripts/run_combined_discovery.py classify-purpose-report

$env:OLLAMA_MODEL = "qwen3.5:4b"
# Tier A, the 3-way tie-break: 17 calls, about 3 minutes
python scripts/run_combined_discovery.py classify-purpose --llm

# Tier B, the 77-way ask, bounded. ~10 s a call, so 200 calls is about 35 minutes.
# Re-run to continue: cached answers are free and do not count against the budget.
python scripts/run_combined_discovery.py classify-purpose --llm --tier tiebreak,semantic --llm-limit 200
```

`--tier` takes `tiebreak` (the default), `semantic`, or both comma-separated. `--llm` alone does
nothing without a tier, and an unknown tier name is refused rather than ignored.

`--limit N` writes the first N companies by id. Keyword weights are **always** measured over the whole
table, so a limit changes what is written, not what is counted — which is what makes `--limit 50` a
true prefix of the full run rather than a different sample.

Output: `airflow/data/combined_discovery/purpose_branches.jsonl`, one row per company, plus seven
`purpose_*` columns in the handover CSV.

---

## The rules that must not be broken

1. **`purpose_branch` never calls `branch_classifier.classify()`.** That function applies the website
   gate, which would withhold every answer this column exists to produce — and passing
   `has_website=True` to get around it would be lying to the function about what was checked. The
   guard asserts this against the **parsed syntax tree**, not the source text, because the module
   docstring discusses `classify()` and `has_website` at length and a substring search finds the
   prose.
2. **A failed model call is never `none_apply`.** A timeout, an unreachable model, unparseable output
   or an invented field name all leave the row's original outcome and set `llm_refused`. "The model
   says no field fits" is a finding about the company; "the model never answered" is a finding about
   our infrastructure.
3. **Abstentions record which tier produced them.** `none_apply` from 3 options means "none of these
   three"; from 77 it means "none of Berlin's 77". Without `decided_by` those are the same row and
   they are not the same claim.
4. **No compound matching.** Measured on this data, relaxing it fires `epa` inside *Reparatur* on 124
   companies, `its` inside *Kreditsicherung* on 303, and `ai` on 144 — noise that would outrank every
   real hit. The cost of keeping the rule is that German inflection blocks real words too:
   `restaurant` does not match *Restaurants*, `spedition` does not match *Speditionsgeschäft*. Of the
   6,481 rows where nothing fired, only 1,058 contain a taxonomy word blocked purely this way, and
   the commonest of those are the false hits above.
5. **The two files stay separate.** `branches.jsonl` and `purpose_branches.jsonl`. `classify-purpose`
   has never written the first, and `classify-report` still prints 489 / 563 / 129 / 6,558 / 6,704.
6. **The outcome vocabularies are disjoint.** `assigned_keywords`, not `assigned`. A downstream `if`
   must not be able to confuse a register answer with a website answer.

---

## Not known

**Whether any of the 610 fields is correct.** No purpose row has ever been hand-checked, so this
column has no precision figure — only counts. There is no labelling worksheet for it yet, which is the
obvious next piece of work and costs nothing but somebody's afternoon.

The cheapest proxy available today is the 14 `llm_reason` strings quoted above, and they read well.
That is evidence about 14 rows out of 610.

### Where the two columns disagree

48 companies have both a Technologiefeld and a `purpose_field`. **31 agree (64.6%), 17 disagree.**

```
Stadtteilauto OS GmbH        Elektromobilität      | Neue Mobilitätsangebote
Heinicke-electronic GmbH     Batterietechnologien  | Handel
Solanergie GmbH              Solarenergie          | Handel
Appfahrt GmbH                Dienstleistung        | Mobilitätssoftware
Kajoni Energie GmbH          Nachhaltige Wirtschaft| Windenergie
```

The pattern in those five is worth noticing: the register tends toward the generic (`Handel`) where
the website is specific, and the website tends toward the generic (`Dienstleistung`) where the
register is specific. Neither column is right; they are answering different questions. 48 is a small
sample and this observation is not a finding.
