# Which industry is each company in?

We have 14,443 companies, mostly just a name and an address. This is about filling in one more
field: the industry. Everything here is free — no paid API, and the language model is optional and
runs on the laptop.

## The words

- **Branch** (German *Branche*) — an industry plus the technology field it sits in. "Bahnindustrie".
  "Gastgewerbe". "Additive Fertigung". Berlin's economic-development people keep a list of 77.
- **Keyword** — a word that suggests a company is in a branch. "Schienenfahrzeuge" suggests
  Bahnindustrie. The Berlin list gives each branch a handful: 527 distinct words in total, 5 per
  branch in the middle case and 35 for the busiest.
- **WZ code** — the official German industry code, *Klassifikation der Wirtschaftszweige*. `56.1` is
  restaurants, `62.10.9` software, `64.2` holding companies. Somebody else already did this work and
  the Northdata export brought it along for 8,483 of our companies.
- **The page cache** — every web page the discovery pipeline downloaded, gzipped on disk. It exists
  so that changing *what we read out of a page* costs minutes instead of hours.

Where things live:

| | |
|---|---|
| Berlin's branch list | `airflow/dags/combined_discovery/branch_taxonomy.yaml` |
| WZ code → branch | `airflow/dags/combined_discovery/wz_to_branch.yaml` |
| the matcher | `airflow/dags/combined_discovery/branch_classifier.py` |
| fetching better pages | `airflow/dags/combined_discovery/content_pages.py` |
| the optional tie-break | `airflow/dags/combined_discovery/branch_llm.py` |
| the commands | `scripts/run_combined_discovery.py classify …` |
| the free guard | `scripts/check_branch_classifier.py` |

## Why the lists are YAML files and not database tables

Three reasons, and they are the same three every time this question comes up.

1. **A human edits them, often.** 277 of the 527 keywords never matched a single company here. Those
   lines will be argued about. In YAML a changed keyword is a changed line in `git diff`. The same
   list in a spreadsheet cell is 35 comma-separated words on one line, and a diff of that line tells
   you nothing.
2. **YAML takes a comment, and the comment is the point.** "Sicherheit removed: it produced 27 of
   this branch's 35 assignments and they were a bed shop, a lift company and an estate agent." That
   sentence has nowhere to live in a table row, and without it somebody adds the keyword back.
3. **A table cannot be reproduced.** Schema changes here are forward-only and immutable, so every
   edit to the list would need its own migration — or the list becomes a table people edit directly,
   and then last month's classification can never be re-run.

If a web interface ever needs the lists, load the YAML into a table at that point. The YAML stays the
source of truth.

## Three stages, in the order they should be built

### Stage A — the WZ code

The cheapest signal in the project. One SQL query, no downloads, no model, and it reaches companies
nothing else can:

| | |
|---|---|
| companies with a WZ code | **8,483** of 14,443 |
| distinct codes | 756 |
| covered by the 25 most common codes | 3,563 (42%) |
| covered by the 100 most common | **5,845 (69%)** |
| have a code but **no** describing text at all | **1,943** |

So a hand-written mapping of 100 codes answers for 5,845 companies. That is six times what the whole
website path produces.

**Matched by longest prefix**, because the codes arrive at three different depths — `64.2`, `64.21`,
`94.99` and `94.99.9` all occur in the same column. A company's `94.99.9` falls back to the entry for
`94.99`. Exact matching alone would leave most of the table unmapped and look like the mapping was
unfinished.

**"No branch applies" is written down explicitly**, and this matters more than it sounds. The biggest
codes are holding companies (508), membership organisations (383), property management (369) and
landlords (189). Berlin's list was built for a technology economy and has no home for any of them.
Writing `null` records that as a decision. Leaving it blank invites somebody to file 508 holding
companies under Finanzwirtschaft to make a coverage number look better.

Only **3 of the 78** rows in Berlin's sheet even hint at a WZ code, so nothing can be converted
automatically. `scripts/wz_branch_worksheet.py write` prints the 100 codes with company counts and
three example names each, for a person to fill in; `apply` reads it back and refuses to write a
branch name that is not in the taxonomy.

### Stage B0 — get the right pages first

The pipeline downloads two pages per company: the homepage, and the **Impressum** — the legal contact
page every German business website must have. Neither is where a business describes what it does. The
Impressum is a name, an address and a register number. Measured over the 699 companies where we have
both:

| | homepage | Impressum |
|---|---|---|
| average characters | 6,386 | 5,245 |
| at least one keyword | **474 (68%)** | 311 (44%) |
| total keyword hits | **1,130** | 569 |

So the homepage wins, and the Impressum still earns its place: on 72 companies it contributes a
keyword the homepage lacks.

But the real text is on `/leistungen`, `/ueber-uns` and `/produkte` — and **the homepage already
links there.** Finding those links costs nothing, because the homepage is on disk:

| | |
|---|---|
| cached homepages | 766 of 902 |
| …that link to a services or about-style page | **644 (84%)** |

Fetching them, capped at 2 per category and 4 per company, was **1,598 pages chosen and 1,567
fetched** for all 902 companies, in 2 minutes 51 seconds at eight threads. And it paid:

| on the 79 companies measured first | |
|---|---|
| now carry at least one keyword | **73 (92%)**, against the homepage's 68% |
| assigned a branch | 44 |
| …**only** because of these pages | **39** |

Over all 902: **136 companies got a branch only from these pages** and 18 got a different one.

Two things to keep in mind about that. First, **follow the links, never guess the paths**: a fixed
list asking for `/leistungen` gets a 404 from every site that calls it `/de/unternehmen/leistungen`.
The repo already learned this from the other direction — `url_discovery.http_client.fetch_impressum`
follows the homepage's own links and the fixed path list is only its fallback. Second, **more text is
not monotonically more answers.** Richer pages spread keywords over more branches, so companies that
had one keyword and a clear winner now have four keywords and a contest: assignments stayed flat at
about 990 while ties rose from 360 to 400. Whether that is worse or better is a precision question,
and precision is not known yet — see the end.

### Stage B — count the keywords, weighted

Read the company's text — services pages first, then homepage, then Impressum, then the register's
`business_purpose` — and see which branch's keywords appear.

**A keyword matches only with a word boundary on both sides. Never inside a German compound.** German
glues nouns together, so `Batterierecycling` really does sit inside `Batterierecyclinganlage`, and
allowing that looks like free coverage. It is not. Measured: pages with at least one keyword rose from
61% to 65%, but `sicherheit` went from 90 companies to 139 — it hides inside *Arbeitssicherheit* and
*Sicherheitstechnik* — and **Defence Tech became the most common industry in Osnabrück.** Companies
with a clear winner *fell*, 338 to 316. More matches, worse answers. `scripts/check_branch_classifier.py`
pins this shut.

Then two weights, both **measured from the data** rather than chosen:

1. **How many companies use the word at all.** `log(companies / (1 + how many use it))`. A word on
   500 of 7,739 companies is worth almost nothing; a word on 3 is worth a lot.
2. **How many branches claim the word.** `schienenfahrzeuge` belongs to Bahnindustrie alone and
   carries its full value. `sensorik` is claimed by seven branches and gives each a seventh.

A multi-word keyword scores higher on top of that: "european train control system" appearing is far
stronger evidence than "steuerung" appearing.

This is the same rule the repo already follows twice — `name_frequency.common_words` counts words in
company names instead of listing them, and `common_email_domains` counts mail providers instead of
listing them. Genericness is measured, never hand-written.

**Not deciding is an answer, and there are five of them.** From the full run:

| | | |
|---|---|---|
| `assigned` | 974 | one branch cleared the floor and beat the runner-up |
| `torn` | 400 | keywords fired, two branches too close to separate |
| `no_keyword` | 6,365 | we read text and not one keyword appeared |
| `no_text` | 6,704 | we had nothing to read |
| `no_branch_applies` | 0 so far | the WZ code says holding company. Needs Stage A filled in |

**Never add any two of those together.** `no_text` is not evidence that a company has no industry,
and `no_branch_applies` is an answer rather than a gap. This is the same distinction the website side
draws between `no_result` and `never_tried`, for the same reason: collapsing them turns a blank into
a claim.

Where the keywords came from, counting hits on assigned companies: `purpose` 722, `services` 566,
`homepage` 320, `imprint` 72. The register's own description of the business is the single most
productive source, and it costs one SQL query.

### Stage C — a local model, only for the ties

`branch_llm.py` asks `qwen3.5:4b` to choose, and everything about it is a restriction:

- **Three branches plus "keine", never 77.** Asked to pick from all 77, the model reached for the
  vaguest label available — a drain-cleaning firm came back as "Dienstleistung". A 77-way choice from
  a 4B model is a guess in a confident voice, and this repo has the precedent: a whole paragraph of
  prompt could not stop the same model failing the Brücklyn case in `llm_judge`.
- **Only where there is something to arbitrate**: at least two keywords fired and the leader is above
  the score floor. One keyword against one keyword is a coin flip, and paying ten seconds to flip it
  buys nothing.
- **`think` off, `temperature` 0.** With thinking on, the model spent its entire output budget on
  reasoning tokens and returned an **empty string** three times out of three.
- **`num_ctx` set explicitly.** Ollama's own default is small and it truncates a longer prompt
  *silently* — a ~2,400-token prompt reported 1,546 prompt tokens, and the part cut off was the page
  text at the end. The model would have answered confidently about a page it never saw.
- **An answer that was not on the menu decides nothing.** It is logged and discarded.
- **"keine" flags the row for a human.** The model read the page and rejected all three, which is not
  the same as no keyword firing.

Cost, measured on this laptop: **10.3 s a page** on `qwen3.5:4b`, 18 s on `qwen3.5:9b`. 302 rows
currently qualify, so about **52 minutes**, once — every answer is cached against the page and the
taxonomy's version hash, so editing a keyword only re-asks the rows whose shortlist changed.

It is off by default and should stay off until the labels below say the ties are worth breaking.

## The one thing no measurement can decide

`sicherheit` appears in 1.8% of our texts. `einzelhandel` appears in 3.0%. `einzelhandel` names an
industry; `sicherheit` names a quality, and on a German business page it means *Arbeitssicherheit* or
*Datensicherheit*. **Document frequency cannot tell them apart, and neither can any other property of
this corpus.**

Berlin's sheet lists `Sicherheit` under Defence Tech. Left in, it produced 27 of that branch's 35
assignments on its own: a bed shop, a lift company, an estate agent, a transport firm, a home-help
service, a dental laboratory. It was removed from `branch_taxonomy.yaml` on 2026-08-20 with the
measurement written next to it. Defence Tech keeps 14 keywords that do name the industry.

Two rules follow, and they pull in opposite directions on purpose:

1. **Do not add a stop-list to the code.** A word being common is already handled by the arithmetic,
   measured rather than opined. `classify-report` prints the keywords that fired on the most
   companies precisely so nobody feels the need.
2. **Do delete a keyword that is wrong for its branch** — in the YAML, with a comment saying what it
   cost. That is a judgement about German, not about this data set, and a person has to make it.

`classify-report` has a section built for exactly this decision: the keywords that carried a whole
assignment on their own. 777 of the 974 assignments rest on one keyword, and the list is mostly
sound — `einzelhandel` (206), `spedition` (40), `gastronomie` (40), `windenergieanlagen` (26),
`steuerberater` (24). Read it as a to-do list.

## Running it

Free, and they must pass before anything else:

```powershell
python scripts/check_branch_classifier.py
python scripts/check_combined_discovery.py
```

Stage A, once, with a person in the middle:

```powershell
python scripts/wz_branch_worksheet.py write --top 100
# fill in the `branch` column of airflow/data/combined_discovery/wz_branch_todo.csv
python scripts/wz_branch_worksheet.py apply
```

Stage B0 — the only part that touches the network. It pays once and caches:

```powershell
python scripts/run_combined_discovery.py fetch-content --limit 100   # trial: ~190 pages, 35 s
python scripts/run_combined_discovery.py fetch-content               # all: 1,567 pages, 2m51s
```

Stage B, and the report. **No web requests at all** — about two minutes for all 14,443 companies,
which is what makes editing the taxonomy cheap:

```powershell
python scripts/run_combined_discovery.py classify --source wz,page,purpose
python scripts/run_combined_discovery.py classify-report
python scripts/run_combined_discovery.py csv       # branch columns join into the handover CSV
```

Stage C, only when the labels justify it:

```powershell
$env:OLLAMA_MODEL = "qwen3.5:4b"
python scripts/run_combined_discovery.py classify --source page --llm
```

## What is not known yet

**Nobody has checked whether any of this is right.** Every number above counts what happened. Not one
says whether the branch is correct, because that needs a human looking at companies:

```powershell
python scripts/run_combined_discovery.py classify-labels        # 40 rows, six groups
# fill in the `label` column by hand
python scripts/run_combined_discovery.py classify-labels-report
```

The `torn` group is the one to fill in first. Those are the rows the model would be asked about, from
the same text, and if a person cannot separate `Filmwirtschaft` from `Freiberufler` for Kotte & Cie.
then 52 minutes of model time will not either.

And the honest caveat over all of it: **the list is Berlin's, the companies are Osnabrück's.** 277 of
527 keywords match nothing here, and the branches that come out on top are Handel, Gastgewerbe,
Freiberufler and Logistik — not Quantum or 5G. Pointing a technology taxonomy built for Berlin at a
Lower Saxony town full of trades and restaurants has a ceiling, and no classifier can lift it. Better
input can: the WZ codes, and the pages the businesses write about themselves.
