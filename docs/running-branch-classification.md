# How to run the industry classification, and what you should see

Every command here is run from the repo root on Windows, in PowerShell. Concepts and the reasoning
behind the numbers are in [branch-classification.md](branch-classification.md); this file is just the
order to type things in.

Two columns come out of this, and they are separate on purpose:

- **Technologiefeld** — one of Berlin's 77 technology and application fields, or `NONE`. Filled only
  from words the company published about itself.
- **WZ cluster** — the official German industry classification the company is registered under.
  Derived from its WZ code with no judgement at all.

## 0. What has to be running

| | needed for | how to start it |
|---|---|---|
| Postgres | everything (it reads `public.companies`) | `docker compose up -d postgres` |
| nothing else | steps 1–5 | — |
| Ollama | only the optional step 7 | it runs natively on Windows |

Airflow is **not** involved. This is a script, it writes nothing to the database, and it can be
stopped and re-run at any point.

## 1. The guards — free, and always first

```powershell
python scripts/check_branch_classifier.py
python scripts/check_purpose_branch.py
python scripts/check_combined_discovery.py
```

No network, no database, no model, about a second each. **Expect `all checks passed`** twice and
`all combined-discovery checks passed`. If the first one fails, stop: it means either a keyword now
matches inside a German compound word, or something has tried to let a WZ code fill the
Technologiefeld. Both are bugs this project has already paid for once. If the second fails, either
the website gate has leaked into column 3 — which would withhold every register answer — or a failed
model call is being recorded as a refusal.

## 2. Fetch the pages that actually describe the businesses — once

```powershell
python scripts/run_combined_discovery.py fetch-content
```

This is **the only step that touches the internet.** It reads the homepages already on disk, finds
the links they offer to `/leistungen`, `/ueber-uns`, `/produkte` and similar, and downloads them under
a cap of 2 per category and 4 per company.

Expect, over the 902 companies with a website:

```
companies in scope                          902
no homepage in the cache                    136
homepage but no services-style link         188
pages chosen                               1598
…that came back                            1567
…that would not load                         31
```

About **2 minutes 51 seconds** at eight threads. Running it again downloads **nothing** — everything
is cached. To try it small first, use `--limit 100`, which means 100 companies *that have a website*,
not the first 100 of the table.

Two things about that cache, both learned on 2026-08-24:

1. **`pages chosen` is now a list of pages that all get read.** It used to fetch two per category and
   read one, throwing away 32% of what it downloaded.
2. **The remembered choice of pages carries the rules that made it.** Change
   `COMBINED_CONTENT_MAX_PER_CATEGORY`, `COMBINED_CONTENT_MAX_PER_COMPANY` or a category pattern in
   `content_pages.CATEGORIES`, and the affected companies are re-examined. That costs no bandwidth —
   the homepage is on disk and the pages are cached under their own URLs — so a first run after such a
   change should still report **0 pages fetched** unless the new rules pick pages nobody has fetched
   before. If it downloads thousands, the rules changed more than you meant them to.

## 3. Classify — two minutes, no network at all

```powershell
python scripts/run_combined_discovery.py classify --source page,purpose
```

Reads every company's text from disk, counts which Berlin keywords appear, weights them, and attaches
the WZ cluster to every row. Expect:

```
77 Technologiefelder, 500 keywords, taxonomy c154c74cf003 | 87 WZ divisions in 22 sections
14443 companies in scope
7739 companies have text to read
235 of 500 keywords appear at least once
Wrote 14443 companies to …\combined_discovery\branches.jsonl
```

Roughly **2 minutes 30 seconds**, and **zero web requests**. That is what makes it cheap to edit
`branch_taxonomy.yaml` and simply run this again.

`--source` accepts `page` and `purpose`, which are the two kinds of text a company publishes about
itself. **`--source wz` is deliberately refused** — a WZ code is a different classification and gets
its own column, never the Technologiefeld.

## 4. Read the scorecard

```powershell
python scripts/run_combined_discovery.py classify-report
```

It prints three blocks. **Column 1, Technologiefeld:**

```
a Berlin field                        489    3.4%
NONE: no_website                      563    3.9%
NONE: torn                            129    0.9%
NONE: no_keyword                     6558   45.4%
NONE: no_text                        6704   46.4%
```

`NONE` is the normal answer, not a failure — Berlin's list is a technology taxonomy and most of
Osnabrück is trades and restaurants. The four reasons are counted apart on purpose: *"we had no text
to read"* is not evidence that a company has no technology field.

**`no_website` is the expensive one.** A Technologiefeld requires a website, so 563 fields the
register text named are withheld. The report prints them, and the block below the outcomes gives the
denominator that actually describes this column: 489 of the **902 companies with a website**, which
is 54% — not 3.4% of a table most of which this column can never reach.

**`assigned` is not "the model assigned it."** `outcome` says whether a company got a field;
`decided_by` says who — `keywords` 346, `model` 143, and empty for every non-assigned row. The model
can only ever convert `torn` → `assigned`; it can never create a field from nothing.

Also in this block: which text the keywords came from (`purpose` 625 hits, `services` 519, `homepage`
226, `imprint` 49), whether the downloaded services pages paid for themselves (**157** companies got a
field only because of them), the 15 most common fields, and — worth reading every time — the keywords
that carried a whole assignment on their own, and what the website rule withheld — 563
fields the register text named and no website confirmed.

**Column 2, WZ cluster:**

```
companies with a WZ code             8483   58.7%
…with no code at all                 5960   41.3%
distinct divisions present             76
distinct sections present              20
```

then the 15 biggest divisions and the section rollup. Nothing here was decided by anybody, so there is
nothing to argue with — only present or absent.

**Both columns** ends with coverage: 810 companies have both, 99 a Technologiefeld only, 7,673 a WZ
cluster only, 5,861 neither. That is coverage and **not** agreement: the two columns answer different
questions and cannot agree or disagree.

### Four things in that report that mean "go and fix something"

1. **`Defence Tech` near the top of the field list** — a keyword is matching inside a German compound
   again.
2. **`!! divisions with no title`** — `wz_titles.yaml` is older than the data. Rebuild it:
   `python scripts/import_wz_titles.py <the WZ .xlsx>`.
3. **`!! this file mixes runs`** — `branches.jsonl` holds rows from two different runs, and they are
   not comparable, because a keyword's weight depends on how many companies use it. Re-run `classify`.
4. **A field near the top of *fields resting on single keywords* whose words name an activity rather
   than an industry.** That is the shape `sicherheit` had before it was deleted, and 26 more keywords
   went the same way on 2026-08-24 — `instandhaltung` (26 of Bahnindustrie's 29 assignments, none of
   them a railway), `netz` (which is how German says "online"), `app`, `galerie`, `social media`,
   `inspektion`. Check a suspect by reading the companies it decided; the wrong ones are obvious.
   Then delete it from `branch_taxonomy.yaml` with the count in a comment — never a stop-list in the
   code. And ask whether the word is generic in GERMAN or only in Osnabrück: `clubs` scored just as
   badly here and was kept, because in Berlin a Club really is a music venue.

## 4b. Optional: is the hyphen hole worth closing?

The keyword rule stops a word hiding inside a glued German compound (*Arbeitssicherheit*) and not
inside a hyphenated one, so `Kfz-Zulassung` fires `zulassung` and lands a car-registration office in
**Pharma & Wirkstoffe**. Closing it also stops *E-Commerce-Lösungen* matching `e-commerce`, so it is a
trade. Measure it, do not guess:

```powershell
python scripts/run_combined_discovery.py classify --source page,purpose      # the knob is off
python scripts/run_combined_discovery.py classify-report > before.txt
$env:BRANCH_HYPHEN_IS_BOUNDARY = "1"
python scripts/run_combined_discovery.py classify --source page,purpose
python scripts/run_combined_discovery.py classify-report > after.txt
Remove-Item Env:\BRANCH_HYPHEN_IS_BOUNDARY
```

Four minutes, no network. Compare exactly three things, the same three that settled the
glued-compound rule:

1. companies carrying **at least one keyword** (`no_keyword` falling means more matched);
2. the **per-keyword company counts** for `app`, `galerie`, `zulassung`, `makler`;
3. companies with a **clear winner** — the `a Berlin field` line.

If the clear-winner count **falls**, the hole is doing less harm than closing it would: leave it and
delete the individual keywords instead. If it rises, flip the default and put the table in
`docs/branch-classification.md`. Either way the run's `taxonomy+rule version` line at the top of the
report changes, so the two files can never be confused.

## 4c. Optional: stop spending model time on companies that no longer exist

`register_status` says whether the register still lists a company. Over this table, **7,673 of 14,443
are `erloschen`** — dissolved — and 472 more are `in Liquidation`. `DISCOVERY_ACTIVE_ONLY=1` stops any
**new** model call being spent on them:

```powershell
$env:DISCOVERY_ACTIVE_ONLY = "1"
python scripts/run_combined_discovery.py classify --source page,purpose
python scripts/run_combined_discovery.py classify-purpose
Remove-Item Env:\DISCOVERY_ACTIVE_ONLY
```

What it does and does not do, because the difference matters:

1. **Keywords still score every company.** Both files still hold one row per company, all 14,443, so
   every outcome count still sums to the table. The keyword pass is free — it is the model that costs
   minutes.
2. **A verdict already in the cache is still applied.** Only a new call is withheld. A re-run
   therefore never rewrites the file as though the model had never spoken, which would drop the
   counts with nothing to explain why.
3. **Column 3 is where the money is.** A register purpose outlives the company that filed it, so 232
   of the 567 model answers in `purpose_branches.jsonl` — 41% — went to a dead company. Column 1
   needs a website and dead companies mostly have none, so there it saves 11 calls of 346.
4. **Rows the model is not asked about stay exactly as the keywords left them** — `torn` or
   `no_keyword`, which is what a refused or unreachable model already produces. No new outcome, no
   count moving for a reason nobody can see.
5. `register_status` is written on every row of both files whether the flag is on or not, so any count
   in either report can be sliced by it after the fact.

## 5. The spreadsheet to hand over

```powershell
python scripts/run_combined_discovery.py csv
```

Writes `airflow/data/combined_discovery/combined_discovery_urls.csv`. The two column groups sit side
by side and never mix:

| column | what it holds |
|---|---|
| `technologiefeld` | a Berlin field, or the literal `NONE`. Never blank, never anything else. |
| `technologiefeld_reason` | `assigned`, `torn`, `no_keyword` or `no_text` |
| `technologiefeld_keywords` | the words that decided it, so any row can be argued with |
| `technologiefeld_text_source` | `services`, `homepage`, `imprint` or `purpose` |
| `wz_code` | the official code the company is registered under |
| `wz_division` / `wz_division_title` | e.g. `56` / `Gastronomie` |
| `wz_section` / `wz_section_title` | e.g. `I` / `GASTGEWERBE` |

A useful sanity read: sort by `technologiefeld` and look at a few rows against their `wz_division`.
They will often say different things about the same company, and that is correct.

## 6. The only step that says whether any of this is RIGHT

Everything above counts what happened. None of it says whether the Technologiefeld is correct, because
that needs a person.

```powershell
python scripts/run_combined_discovery.py classify-labels
```

Writes 50 rows to `airflow/data/combined_discovery/branch_labels_todo.csv` in six groups of ten:
`keyword_decided`, `model_decided`, `no_website`, `torn`, `no_keyword`, `no_text`. Fill in the
**`label`** column with `yes`, `no` or `unclear`, and use `notes` freely.

> **Excel will change the delimiter and not tell you.** On a German locale it saves CSV with `;`
> instead of `,`. The file still opens and still has every row, so nothing looks wrong — but a
> comma-based reader then sees the whole header as one column. That already happened here, and
> `classify-labels-report` would have reported nothing at all rather than failing. The reader now
> sniffs the delimiter and logs which one it found, so an Excel round trip is safe. If you see
> `is ';'-delimited (Excel does this)` in the log, that is the fix working, not a problem.

- `keyword_decided` — is `technologiefeld` right for this company? This is the precision number.
- `no_website` — `withheld_branch` is what the company's **register text** claims and no website
  confirmed. Is that claim right? `yes` here means requiring a website cost a good answer, and 563
  answers were withheld, so this bucket is how you find out whether that rule was worth it.
- `torn` — can *you* pick one of `shortlist` from the same text? **Do this group first:** if a person
  cannot, the model in step 7 cannot either, and you have saved yourself 52 minutes.
- `no_keyword` / `no_text` — look the company up. `yes` if some field on Berlin's list does fit it,
  `no` if genuinely none does. This is the ceiling.

The `wz_*` columns are in the file as **context, not for labelling** — nobody guessed them. Knowing a
company is registered under `56 Gastronomie` helps you judge whether `Gastgewerbe` is right.

**Do this after a `classify` run, not before one.** A label is an answer about one classification. A
keyword's weight depends on how many companies use the word, so the next run can give the same
company a different field — and `yes, Handel is right` says nothing about a row that now reads
`Logistik`.

Re-running merges, and it keeps a label even when the row moved between groups. It never overwrites
what a person wrote. When the row a label describes **has** changed, that label is marked in the
`stale` column and `notes` records what it was originally about:

```
stale  yes
notes  my note [labelled yes when this was keyword_decided/Handel in run 4d7e5d81c795]
```

Three things follow from that, and they are the whole point of the column:

1. A stale label is **left out of every number** in the report. Precision from an answer to a
   question nobody asked is worse than no precision.
2. The flag is **sticky**. Re-running does not clear it, because re-running has not re-read anything.
3. **You** clear it: re-read the row, fix `label` if the answer changed, then blank the `stale` cell.
   That is the only thing that clears it, and it means "I have looked again".

Then:

```powershell
python scripts/run_combined_discovery.py classify-labels-report
```

Its first lines say which run(s) the labels describe and how many are stale.

## 7. Optional: the local model, for the ties only

Only worth it if step 6 says the `torn` rows are separable by a human.

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"
python scripts/run_combined_discovery.py classify --source page --llm
python scripts/run_combined_discovery.py classify-report
```

**10.3 seconds per page** and 302 rows qualify, so about **52 minutes**, paid once — answers are cached
against the page and the taxonomy version. The model only ever chooses between the **three** fields
the keywords shortlisted, plus "keine"; an answer that was not on the menu is thrown away, and "keine"
flags the row for a human.

## 7b. Optional: a text classifier for the rows no keyword reached

The keyword rows are the seed; the app's votes on `technologiefeld` correct it (a correction
replaces a label, a "wrong" without one removes it). The classifier then fills only `no_keyword`
rows that have a verified website, and is judged only against human votes. Reasoning:
`airflow/dags/combined_discovery/branch_textcat.py`.

```bash
python scripts/run_combined_discovery.py classify                  # the seed: branches.jsonl
python scripts/train_branch_textcat.py                             # train, measure, promote if better
python scripts/run_combined_discovery.py classify --textcat        # fill the gaps
```

- Training needs at least 50 labelled companies (`BRANCH_TEXTCAT_MIN_EXAMPLES`).
- It promotes a model by itself only when at least 20 held-out companies carry a human label
  (`BRANCH_TEXTCAT_MIN_HUMAN_HOLDOUT`) and the classifier is at least as accurate there as the
  keywords. Without those labels, `--promote` is your decision, and `metrics.json` records it.
- Models: `airflow/data/combined_discovery/models/branch_textcat/<version>/` with `metrics.json`;
  `current.json` names the one in use.
- Each row records `textcat_branch` and `textcat_score` whenever the classifier was asked, also
  below the threshold (`BRANCH_TEXTCAT_MIN_SCORE`, 0.6).
- With `--textcat` the pinned numbers in `tests/integration/` move on purpose: re-measure and
  update them in the same commit (docs/guards-and-tests.md, "Changing a pinned number").

## 8. When Berlin's list changes

```powershell
python scripts/import_branch_taxonomy.py "path\to\ALLE_Abfrage_Labels.csv" --out C:\temp\new.yaml
```

Writes somewhere else so you can `git diff` it against the live `branch_taxonomy.yaml` — the live file
carries hand edits and comments that a straight overwrite would destroy. Then edit the live file by
hand, re-run the guard and step 3, and compare the report.

Editing a keyword costs one `classify` run: two minutes, no downloads.

## 9. Column 3 — what the register says, 13 seconds and no network

A separate column from a separate source: `business_purpose`, the *Gegenstand des Unternehmens*. It
needs no website, so it reaches thousands of companies the Technologiefeld cannot. Full numbers and
reasoning: [purpose-classification](purpose-classification.md).

```powershell
python scripts/run_combined_discovery.py classify-purpose
python scripts/run_combined_discovery.py classify-purpose-report
```

Expect, over 14,443 companies:

```
assigned_keywords       596
torn                     64
no_keyword             6481
no_purpose             7302     <- 6,902 empty plus 400 stubs of 40 chars or fewer
TOTAL                 14443
```

**The total must be exactly 14,443**, and `classify-report` must still print 489 / 563 / 129 / 6,558 /
6,704 afterwards. This column cannot move the Technologiefeld, and that check is how you know.

The tie-break is optional and reaches 17 companies — `worth_asking` needs two keywords and 608 of the
660 rows with any keyword have exactly one:

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"
python scripts/run_combined_discovery.py classify-purpose --llm     # 17 calls, about 3 minutes
```

Expect `assigned_model_3way 14, none_apply 3, torn 47` and, on a second run,
**`model calls 0, served from cache 17`**. If the second run asks again, the cache key is broken.

## Everything that gets written, and where

| file | written by | in git? |
|---|---|---|
| `airflow/data/combined_discovery/branches.jsonl` | `classify` | no, gitignored |
| `…/purpose_branches.jsonl` | `classify-purpose` | no |
| `…/combined_discovery_urls.csv` | `csv` | no |
| `…/branch_labels_todo.csv` | `classify-labels`, then you | no |
| `…/pages/content/…` | `fetch-content` | no |
| `airflow/dags/combined_discovery/branch_taxonomy.yaml` | you, by hand | **yes** |
| `airflow/dags/combined_discovery/wz_titles.yaml` | `import_wz_titles.py` | **yes** |

Nothing here writes to Postgres. It only reads `public.companies`.
