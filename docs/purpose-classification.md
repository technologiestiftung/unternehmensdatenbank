# Column 3: what the register says the company was set up to do

Every registered German company files a **Gegenstand des Unternehmens**: one or two sentences, written
by a notary, saying what it is *permitted* to do. It is filed at registration and rarely changed. It
arrives as `companies.business_purpose`.

`airflow/dags/enrichment/purpose_branch.py` turns that text into `purpose_field`: one of Berlin's 77
fields, or a recorded non-answer. Read [branch-classification](branch-classification.md) first; this
file assumes you know what a Technologiefeld is.

> The numbers below were measured on the Osnabrück example data (14,443 companies, 2026-08/09), which
> has been removed. OffeneRegister has no business purpose, so for Berlin this column cannot be filled
> today.

## Why a third column

The Technologiefeld requires a downloaded website. Filling it from register purposes would quietly undo
that rule. So the register gets its own column with its own outcome names, and the two can be compared
instead of merged.

**They are never a check on each other.** A company registered to trade goods that now sells software
has two true answers. On 48 companies with both, 31 agreed and 17 did not: the register tends to say
`Handel` where the website is specific, and the other way round.

## Outcomes

Eight outcomes. Every company has exactly one, **and no two are ever added together.**

| outcome | meaning | Osnabrück count |
|---|---|---|
| `assigned_keywords` | one field won on keywords alone | 596 |
| `assigned_model_3way` | was `torn`; a model chose among the 3 the keywords nominated | 14 |
| `assigned_model_77` | was `no_keyword`; a model chose from all 77. The least traceable value | 4,095 |
| `none_apply` | a model read it and said no Berlin field fits | 2,225 |
| `unsure` | a model read it and said the text does not say enough | 5 |
| `torn` | keywords fired, top two too close | 47 |
| `no_keyword` | read, and none of the 500 keywords appeared | 159 |
| `no_purpose` | empty, or 40 characters or fewer. Never read | 7,302 |

The model stages ran with the old CLI (local `qwen3.5:4b` via Ollama) and are removed from the repo; the
outcomes stay in the vocabulary so stored rows remain readable.

The four `NONE` reasons are four different claims. `no_purpose` says nothing about the company.
`no_keyword` says the taxonomy has no word for it. `torn` says our weights could not separate two
fields. Only `none_apply` is a claim about the company. `unsure` is kept apart from `none_apply`
because "no field fits" and "the text is too thin" are different findings.

## How a field is decided

The keyword scoring is `branch_classifier`'s, reused: same normalizing, same pattern, same
`MIN_SCORE` 2.0 and `MIN_MARGIN` 1.5. Two differences:

1. **Document frequency is counted over purpose texts only.** Register prose is a different vocabulary
   from homepage copy.
2. **Stubs are matched but left out of the frequencies**, so the length floor does not shift the scores
   of the rows it ignores.

Purposes are short (251 characters on average), but the thresholds are not what limits this column:
every winner scored at least 3.48. **The taxonomy is the limit.** 94.5% of keyword-decided fields rest
on a single keyword, so this column deserves less trust than the website one.

## The length floor

A purpose of 40 characters or fewer is not read (`PURPOSE_MIN_CHARS`). Measured, that cost 17 real
fields: a short purpose like "Gastronomie" is often the least ambiguous one there is. The field it would
have produced is kept in `withheld_by_floor`, because **a veto whose rejections are never counted cannot
be told apart from one that never fires.** Lowering the floor to 10 recovers all 17.

## Rules that must not be broken

1. **`purpose_branch` never calls `branch_classifier.classify()`.** That function applies the website
   gate, which would withhold every answer this column exists for. The guard checks the parsed syntax
   tree, not the source text, because the docstring discusses `classify()` at length.
2. **A failed model call is never `none_apply`.** A timeout, unparseable output or an invented field
   name keeps the original outcome and sets `llm_refused`.
3. **Abstentions record which tier produced them.** `none_apply` from 3 options means "none of these
   three"; from 77 it means "none of Berlin's 77". `decided_by` keeps them apart.
4. **No compound matching.** Relaxing it fired `epa` inside *Reparatur* on 124 companies and `its`
   inside *Kreditsicherung* on 303. The cost: `restaurant` does not match *Restaurants*.
5. **The outcome vocabularies are disjoint from the Technologiefeld's.** `assigned_keywords`, not
   `assigned`, so code can never confuse a register answer with a website answer.

## Not known yet

**Whether any field is correct.** A worksheet of 50 labels exists (41 yes, 8 no), but no row is
verified as ground truth, so there is no precision figure.
