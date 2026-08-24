# Two columns: Technologiefeld and WZ cluster

We have 14,443 companies, mostly just a name and an address. This is about adding two industry
columns. Everything here is free — no paid API, and the language model is optional and runs on the
laptop.

**They are two columns because they are two different questions**, and the single most expensive
mistake made here was answering one with the other.

| | Technologiefeld | WZ cluster |
|---|---|---|
| the question | which technology or application field does this company **work in**? | what is it **registered as**? |
| who decides | Berlin's economic-development list of 77 fields | the Statistisches Bundesamt |
| filled from | words the company published about itself | the company's official WZ code |
| possible values | one of the 77, or `NONE` | 76 divisions in 20 sections |
| companies with a value | **974** (6.7%) | **8,483** (58.7%) |
| any judgement involved | yes: keyword weights, thresholds | **none** — it is a lookup |

Neither is a check on the other. A company registered under *68 Grundstücks- und Wohnungswesen* whose
Technologiefeld is `NONE` is fully and correctly described by both columns.

## The words

- **Technologiefeld** — short for Berlin's column *Branche / Technologie- und Anwendungsfeld*. 77
  values: "Bahnindustrie", "Quantentechnologien", "Gastgewerbe", "Handwerk". A company has one of
  those or it has `NONE`.
- **Keyword** — a word that suggests a Technologiefeld. "Schienenfahrzeuge" suggests Bahnindustrie.
  The Berlin list gives each field a handful; 526 distinct words in total.
- **WZ code** — the official German industry code, *Klassifikation der Wirtschaftszweige*. `56.11.1`
  is "Restaurants mit Bedienung". The Northdata export brought one along for 8,483 companies.
- **Division** (*Abteilung*) — the first two characters of a WZ code, e.g. `56 Gastronomie`. 87 exist,
  76 occur here. **Section** (*Abschnitt*) — the letter above it, e.g. `I GASTGEWERBE`. 22 exist, 20
  occur here.
- **The page cache** — every web page the discovery pipeline downloaded, gzipped on disk, so that
  changing *what we read out of a page* costs minutes instead of hours.

Where things live:

| | |
|---|---|
| Berlin's 77 fields and their keywords | `airflow/dags/combined_discovery/branch_taxonomy.yaml` |
| every WZ code and its official name | `airflow/dags/combined_discovery/wz_titles.yaml` (2,008 codes) |
| the keyword matcher | `airflow/dags/combined_discovery/branch_classifier.py` |
| the WZ cluster lookup | `airflow/dags/combined_discovery/wz_cluster.py` |
| fetching better pages | `airflow/dags/combined_discovery/content_pages.py` |
| the optional tie-break | `airflow/dags/combined_discovery/branch_llm.py` |
| the commands | `scripts/run_combined_discovery.py classify …` |
| the free guard | `scripts/check_branch_classifier.py` |

## The mistake this design exists to prevent

An earlier version had **one** column and filled it from both sources. A hand-written mapping turned
a WZ code into a Technologiefeld: `56.11.1 → Gastgewerbe`, `43.22.0 → Handwerk`, `70.20.0 →
Dienstleistung`. It looked like a triumph — 3,118 companies with a field instead of 974.

It was wrong twice.

1. **It laundered one classification's answer into the other's voice.** "Fazenda Restaurantbetriebe
   GmbH" was given the Technologiefeld *Gastgewerbe* because its WZ code says restaurant. Nothing the
   company published said it. 2,632 of the 3,118 filled values were like that, and no column in the
   output said which were which.
2. **It threw the WZ classification away.** WZ has 22 sections and 87 divisions of its own, official
   and complete. Squeezing it through a 77-value Berlin list flattened all of that to gain a coverage
   number.

This is the same class of error the website side already documents: *a register number on a page
proves the page is about a company, never that it is that company's site.* Here: **an official
industry code says what a company is registered as, never which technology field it works in.**

The fix is structural rather than a rule to remember. `branch_classifier.classify()` **has no
parameter for a WZ code**, so the mapping cannot be reintroduced by accident, and
`check_branch_classifier.py` fails if that parameter ever reappears.

## Column 2 first, because it is the easy one

`wz_cluster.cluster_for(code)` and that is the whole feature:

- the **division** is `wz_code[:2]`, so `56.11.1`, `56.1` and `56` all land in `56`;
- the **section** is the letter that owns that division, looked up in `wz_titles.yaml`.

No mapping, no threshold, no human. Nothing to label, because nobody guessed anything.

| | |
|---|---|
| companies with a WZ code | **8,483** (58.7%) |
| …with none at all | 5,960 (41.3%) |
| distinct divisions present | **76** |
| distinct sections present | **20** |

The biggest divisions are a portrait of the region:

| division | companies | |
|---|---|---|
| 64 | 969 | Erbringung von Finanzdienstleistungen |
| 68 | 923 | Grundstücks- und Wohnungswesen |
| 47 | 647 | Einzelhandel |
| 94 | 626 | Interessenvertretungen, kirchliche Vereinigungen |
| 46 | 573 | Großhandel |
| 70 | 492 | Verwaltung und Führung von Unternehmen |
| 43 | 400 | Vorbereitende Baustellenarbeiten, Bauinstallation |
| 62 | 340 | IT-Dienstleistungen |
| 35 | 246 | Energieversorgung |
| 56 | 233 | Gastronomie |

Rolled up, the four biggest sections are `G Handel` (1,220), `L Finanz- und
Versicherungsdienstleistungen` (1,159), `N Wissenschaftliche und technische Dienstleistungen` (1,038)
and `M Grundstücks- und Wohnungswesen` (923).

`wz_titles.yaml` is checked in — 2,008 codes with a machine-readable `sections:` block, and the
section headings kept as comments so the file reads top to bottom. **Our codes are WZ 2025, not WZ
2008**: `70.10.4` and `62.10.9` do not exist in the 2008 edition at all, so anyone checking against a
2008 table will mismatch silently. `scripts/import_wz_titles.py <xlsx>` rebuilds it, once per WZ
edition; the .xlsx itself stays out of the repo because a 75 KB binary cannot be reviewed in a pull
request and a YAML file can.

## Column 1: the Technologiefeld, from what the company published

### Stage 1 — get the right pages

The pipeline downloads two pages per company: the homepage, and the **Impressum**, the legal contact
page every German business website must have. Neither is where a business says what it does — an
Impressum is a name, an address and a register number. Measured over the 699 companies where we have
both:

| | homepage | Impressum |
|---|---|---|
| average characters | 6,386 | 5,245 |
| at least one keyword | **474 (68%)** | 311 (44%) |

The homepage wins; the Impressum still earns its place, contributing a keyword the homepage lacks on
72 companies. But the real text is on `/leistungen`, `/ueber-uns` and `/produkte` — and the homepage
already links there, so **finding those links costs nothing**:

| | |
|---|---|
| cached homepages | 766 of 902 |
| …linking to a services or about-style page | **644 (84%)** |
| pages fetched, capped at 2 per category and 4 per company | **1,567** in 2 min 51 s |
| companies given a Technologiefeld **only** by those pages | **136** |
| …given a different one | 18 |

**Follow the links, never guess the paths**: `/leistungen` 404s on every site that calls it
`/de/unternehmen/leistungen`. And note that more text is not monotonically more answers — richer pages
spread keywords over more fields, so some companies moved from a clear winner to a visible tie. That
is probably better precision, but precision is not measured yet.

### Stage 2 — count the keywords, weighted

**A keyword matches only with a word boundary on both sides. Never inside a German compound.** German
glues nouns together, so `Batterierecycling` really does sit inside `Batterierecyclinganlage`, and
allowing it looks like free coverage. Measured: pages with at least one keyword rose 61% → 65%, but
`sicherheit` went from 90 companies to 139 — it hides in *Arbeitssicherheit* and *Sicherheitstechnik*
— and **Defence Tech became the most common industry in Osnabrück**, while companies with a clear
winner *fell* 338 → 316. More matches, worse answers.

Then two weights, both **measured from the data**:

1. **How many companies use the word at all** — `log(companies / (1 + df))`. A word on 500 of 7,739
   companies is worth almost nothing; a word on 3 is worth a lot. Counted per **company**, not per
   page, or a company with four cached pages inflates its own words fourfold.
2. **How many fields claim the word** — `schienenfahrzeuge` belongs to Bahnindustrie alone and keeps
   its full value; `sensorik` is claimed by seven and gives each a seventh.

A multi-word keyword scores higher on top of that. Same rule as `name_frequency.common_words` for
company names: genericness is measured, never hand-listed.

### The outcome, and the three reasons for NONE

| | | |
|---|---|---|
| a Berlin field | **974** (6.7%) | |
| `NONE` | 400 | `torn` — keywords fired, two fields too close to separate |
| `NONE` | 6,365 | `no_keyword` — we read the text, no keyword appeared |
| `NONE` | 6,704 | `no_text` — nothing to read |

**Never add any two of those together.** "We had no text" is not evidence that a company has no
technology field. All three print `NONE` in the column and keep their reason beside them, in
`technologiefeld_reason`.

Where the keywords came from, counting hits on the 974: `purpose` 719, `services` 527, `homepage` 289,
`imprint` 59. The business purpose from the register is the most productive single source and costs one
SQL query.

**Expect `NONE` to be the common case, and do not treat it as failure.** Berlin's list was built for
Berlin's technology economy; 277 of 526 keywords match nothing here at all, and the fields that come
out on top are Handel (242), Gastgewerbe (96), Logistik (56) and Freiberufler (56). A Lower Saxony
town of trades and restaurants is mostly not in a technology taxonomy, and the WZ column is what
describes those companies instead.

### Stage 3 — a local model, only for the ties

`branch_llm.py` asks `qwen3.5:4b` to choose, and every part of it is a restriction: **three fields
plus "keine", never 77** (asked to pick from all of them it reached for the vaguest label — a
drain-cleaning firm came back "Dienstleistung"); only where at least two keywords fired above the
score floor; `think` off and `temperature` 0 (with thinking on it spent its whole output budget
reasoning and returned an empty string 3 times out of 3); `num_ctx` set explicitly (Ollama's default
truncates a long prompt *silently*, and the page text at the end is what gets cut); an answer that was
not on the menu is discarded; and "keine" flags the row for a human.

Cost: **10.3 s a page**, 302 rows qualify, so about **52 minutes**, once — answers are cached against
the page and the taxonomy's version hash. Off by default.

### The one thing no measurement can decide

`sicherheit` appears in 1.8% of our texts, `einzelhandel` in 3.0%. `einzelhandel` names an industry;
`sicherheit` names a quality. **Document frequency cannot tell them apart**, and no other property of
this corpus can either.

Berlin's sheet listed `Sicherheit` under Defence Tech. Left in, it produced 27 of that field's 35
assignments on its own: a bed shop, a lift company, an estate agent, a transport firm, a home-help
service, a dental laboratory. It was deleted from `branch_taxonomy.yaml` with the measurement in a
comment beside it. Defence Tech keeps 14 keywords that do name the industry.

Two rules that pull opposite ways on purpose:

1. **Do not add a stop-list to the code.** Commonness is already handled by the arithmetic, measured
   rather than opined. `classify-report` prints the most-fired keywords so nobody feels the need.
2. **Do delete a keyword that is wrong for its field** — in the YAML, with a comment saying what it
   cost. That is a judgement about German, and a person has to make it.

`classify-report` prints the keywords carrying a whole assignment alone — **777 of the 974** — as a
to-do list. Most are sound: `einzelhandel` 206, `spedition` 40, `gastronomie` 40, `windenergieanlagen`
26, `steuerberater` 24.

## Running it

Step-by-step, with what each command prints:
[running-branch-classification.md](running-branch-classification.md). The short version:

```powershell
python scripts/check_branch_classifier.py                               # free, must pass first
python scripts/run_combined_discovery.py fetch-content                  # once: 1,567 pages
python scripts/run_combined_discovery.py classify --source page,purpose  # 2 min, no network
python scripts/run_combined_discovery.py classify-report
python scripts/run_combined_discovery.py csv
```

`--source wz` is **refused**: the Technologiefeld is filled only from text the company published. The
WZ cluster is attached to every row regardless, whether or not any text was read.

## What is not known yet

**Nobody has checked whether the Technologiefeld is right.** 974 companies have one and not a single
row is hand-labelled. `classify-labels` writes 40 rows in five buckets; `classify-labels-report` is
the only thing that reports precision. Start with `torn`: those are the rows the model would be asked
about, and if a person cannot separate two fields from the same text, a 4B model will not either.

The WZ column needs no labelling — it is a lookup, not a guess. The only way it can be wrong is if
`wz_titles.yaml` is older than the data, and the report prints a warning line when a division has no
title.

Nothing is written to Postgres. Making either column a real field means an observation, a Flyway
migration, and a rule for the coordinator — and for the Technologiefeld that should wait until the
labels exist.
