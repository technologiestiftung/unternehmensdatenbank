# Sources: where the data comes from

Every value in the database came from a **source**: a register, a website, a grant list, a model, or
a person in the app. This page lists them, how we get each one, and which of the 20 target columns
each fills. How each pipeline runs: [dags](dags.md). How a value is stored: [data-model](data-model.md).

## How a source enters the database

1. A source writes the record it delivered into `company_source_records` (the whole record, kept
   as evidence) and one **observation** per field into `company_field_observations`, with a
   confidence between 0 and 1.
2. It never writes the shown value itself. `pipeline_coordinator` picks one winner per field:
   **priority first** (a lower number wins), then confidence, weighed by age.
3. Today every source has priority 100, except corrections from the app (`user_feedback`, 50).

## The sources

The app shows each source on its own page (`/sources/<key>`): the description, link, terms and
schedule stored in `sources`, its values per confidence range and its last 20 runs. The text is
the `DETAILS` (`SourceDetails`) in the DAG that registers the source, written by `ensure_source` on
every run; a source's page shows it after that DAG's first run.

Record counts are from the local database on 2026-11-02.

| Source key | What it is | How we get it | How often | Records |
|---|---|---|---|---|
| `offeneregister` | OffeneRegister: a snapshot of the Handelsregister, read 2017–2019. Name, legal form, address, status, register number, officers | bulk file, imported by scripts ([data-model](data-model.md), "Loading the OffeneRegister Berlin set") | once | 296,486 |
| `registerbekanntmachungen` | register announcements on handelsregister.de (deletions, liquidations, insolvencies) | DAG `register_announcements`: 2 requests, the result list only, never a detail page | weekly | 900 |
| `gleif` | GLEIF, the LEI registry (open data, CC0): legal name, legal address, status and the day the legal entity was created (#137) of Berlin register entries that have an LEI. No officers | DAG `gleif`: GLEIF's full file once, then daily the weekly change file (up to 5 days without a good run), the monthly one (up to 28), else the full file again; only Charlottenburg entries with a Berlin address. Creates the active companies we lack (4,781 measured on 2026-11-04, most founded after 2019) | daily | 13,500 (measured on 2026-11-04, not yet loaded) |
| `openregister` | OpenRegister, the Handelsregister as an API: name, legal form, current address, business purpose, active or not. No officers requested | DAG `openregister`: **started by hand only**; at most 10 searches (100 credits) per run of the free plan's 500 per 30 days; Berlin companies incorporated since 2019, page by page across runs; then the older ones, one founding year per pass (#143): 100,000+ companies, so 10,000+ credits, about 20 months on the free plan. A backlog page also gives the founding year: the year it searched (#137). Whether storing and regular retrieval are allowed was asked of the operators and is open (developer's decision of 2026-11-04 to build it anyway, by hand) | by hand | 0 (not yet run) |
| `handelsregister_lookup` | the Handelsregister search on handelsregister.de: today's name, seat and status | DAG `handelsregister_lookup`: one search per company by register number, ≤50 per run. **Started by hand only**: the portal's terms forbid systematic retrievals (Nutzungsordnung, Ziffer 3) | by hand | 2 |
| `wirtschaftsatlas` | Berlin Partner's Wirtschaftsatlas Berlin: about 7,600 companies with website, in Berlin's industry layers (AI, FinTech, Robotik, ...) | DAG `wirtschaftsatlas`: downloads the map's company layers, matches by name and postcode, verifies each website like a Wikidata proposal. **Started by hand only**: the Impressum asks for Berlin Partner's prior consent (#105) | by hand | 2,253 (2026-11-04) |
| `register_crosslink` | discovery's own reads: a guessed site that prints another of our companies' register number | DAG `register_crosslink`: proposes the site to that company, verified like a Common Crawl proposal (#112) | weekly | 6,048 proposals (local, 2026-11-04) |
| `website_discovery` | the company's own website | DAG `website_discovery`: guesses domains from the name, reads the Impressum, keeps a site only if the page proves it is that company ([website-verification](website-verification.md)) | hourly batches | 65,587 |
| `wikidata` | official websites (property P856), employee counts, revenue and founding years (P571) Wikidata lists for Berlin organisations | DAG `wikidata`: three SPARQL queries, each website verified like a guessed domain | daily | 0 here (531 matched in a local copy, 2026-10-30) |
| `common_crawl` | Impressum pages in the Common Crawl that name a Berlin register number | DAG `common_crawl`: 300 index blocks a run, each website verified like a guessed domain | weekly | 0 here (pilot: 21 websites from 5,000 pages, #56) |
| `impressum_contacts` | email and phone from the Impressum of the proven website | DAG `website_contacts` | after discovery | 150 |
| `impressum` | the company's current address, as its own Impressum states it, when the page names its register number and no other | DAG `impressum_address`: the page's first address, only when it is in Berlin; confidence 0.8, so it wins against OffeneRegister's 2017–2019 address but not against one a register read this year (#141) | daily | not run yet |
| `website_keywords` | keywords from meta tags and services/products headings | DAG `website_keywords` | after discovery | 150 |
| `structured_data` | the founding date or year a company's own website states in its structured data (schema.org) | DAG `structured_data`: reads stored pages of the published website, no downloads | weekly | 12 (local, 2026-11-04, whole-name rule) |
| `website_founding_text` | the founding year the text of a company's own website states ("1990 gegründet", "Gründungsjahr 1990", "founded in 1990"), only with the company's name within 150 characters; "seit" never. Often dates the business, not the legal entity: 32 of 42 within a year of GLEIF (#137) | DAG `structured_data`, same stored pages | weekly | 318, 261 of them with no other year; 32 of 42 within a year of GLEIF, 18 of 19 the same as schema.org (local, 2026-11-08) |
| `branch_classifier` | Technologiefeld from the keywords on the website ([branch-classification](branch-classification.md)) | DAG `technology_field`; a spaCy text classifier may fill gaps | daily, in batches while batches come back full | 542 |
| `purpose_classifier` | the field (one of Berlin's 77) the register's business purpose names, by keywords ([purpose-classification](purpose-classification.md)); what a company was set up to do, never its Technologiefeld (#178) | DAG `purpose_field`: reads the published `business_purpose` (from `openregister`), no downloads; writes once 500 purposes over 40 characters exist | daily | 0 (local copy of play, 2026-11-08: 250 purposes, below the 500; a dry run gave 25 of the 238 readable a field, 22 of them on one keyword) |
| `llm_technology_field` | Technologiefeld suggestions from the Claude API first for companies no keyword matched, then for companies the keywords gave a field (for the WZ model, #158), with an estimated WZ division (#140); AI-made; shown in the app as a guess at 0.55, below the keywords (#183), and kept to train and check the text classifier (#87). Only website text without the Impressum; names, emails and phones replaced; it-compliance's OK confirmed 2026-11-04 | DAG `llm_labels`: **started by hand only**, one Message Batch of at most 500 companies, a monthly token budget | by hand | not run yet |
| `spacy_ner` | Geschäftsführer names a spaCy model finds on the Impressum | DAG `manager_ner_extract` | after discovery | 72 |
| `cordis` | EU research grants (CORDIS: Horizon Europe, Horizon 2020) to private companies in Berlin. Reuse under the European Commission's reuse notice, exact terms not yet checked | DAG `cordis`, into `company_fundings`, linked by name and city (no register number). Local copy, 2026-11-04, rolled back: 1,188 participations of 518 companies, 761 linked to 319 of ours, EUR 248 million | monthly | grants table |
| `jeff` | [jeff](https://github.com/technologiestiftung/jeff), TSB's self-hosted jev API (GLiFormer, 400M parameters), on the IONOS dev cluster: guesses from the website text | DAG `jeff`: Technologiefeld and WZ division at jeff's confidence × 0.5, below every other source (#194). Text sent unredacted, because TSB hosts it (decided 2026-11-11) | daily | 0 until it runs; not measured |
| `web_search` | the Brave Search API: hosts found for a company's name and "Berlin" | DAG `web_search`: proposals verified like a guessed domain (#92). **Started by hand only**: price and storage terms not checked | by hand | 0 websites from the first 20 searches (2026-11-04) |
| `ba_jobsuche` | Federal Employment Agency job search: employers with open ads in Berlin (33,514 ads on 2026-11-04) | DAG `ba_jobsuche`: reads the list view, matches employers by name and postcode, stores matches only. **Started by hand only**: the portal's terms (2a (3)) forbid automated reading for data collection; read on the developer's decision of 2026-11-04 (#106) | by hand | not run yet |
| `foerderkatalog` | grants of the federal government (Förderkatalog des Bundes) | DAG `foerderkatalog`, into `company_fundings`, linked by name and city | monthly | grants table |
| `berlin_zuwendungen` | grants of Land Berlin (Zuwendungsdatenbank) | DAG `berlin_zuwendungen`, into `company_fundings`, linked by register number; a recipient registered after the snapshot that we lack becomes a company (name, legal form, city; #103) | monthly | grants table; 1,743 companies created (local, 2026-11-04) |
| `user_feedback` | corrections people type in the app | DAG `feedback_observations`: one person 0.30 (listed, never shown), two who agree 0.60 | hourly | counts only, never who |

Grants are not observations: they sit in `company_fundings` and are linked to companies by
`refresh_company_funding_links()`.

**Two rules for everything we download.** A company's web page goes through the corpus
(`page_cache`, stored in S3), so every DAG shares one download. `email` and `phone` are stored,
never shown: they never win a field.

## Which column comes from which source

The goal is one record per Berlin company with 20 columns. Counts are from the local database on
2026-11-02 (296,897 companies: 296,486 from OffeneRegister, the rest created by
`register_announcements`). "Filled" says nothing about correctness: hand checks have only begun
(`review_sample`, #76).

| # | Column | `v_company_profile` | Filled by | Today |
|---|---|---|---|---|
| 1 | Name | `name` | OffeneRegister, `handelsregister_lookup`, `gleif` (companies with an LEI; a name differing only in case or punctuation is not written) | 296,897 |
| 2 | Mitarbeiter | `employee_count` | `wikidata` (P1128, with its date, #138): often the group's figure, unchecked, confidence 0.6 | 0 until `wikidata` runs (local, 2026-11-08: 66 matched companies have one) |
| 3 | Umsatz | `revenue` | `wikidata` (P2139, euros only, with its date, #139): often the group's figure, unchecked, confidence 0.6 | 0 until `wikidata` runs (local, 2026-11-08: 34 matched companies have one) |
| 4 | Förderungen | `funding_count`, `funding_total_eur`, `fundings` | `foerderkatalog`, `berlin_zuwendungen`, `cordis` | 5,189 companies with a grant |
| 5 | WZ-Nummer | `wz_code` | no source yet: OffeneRegister has none | 0 |
| 5a | WZ (geschätzt) | `wz_division_estimated` | `llm_technology_field` (Claude, 0.55), `wz_textcat` (`wz_estimate`), `local_classifier` and `jeff` (#194; each at most 0.50): estimates from the website text, never the official number (#158, #183). The app shows one "WZ" row: the official number where there is one, else the estimate, coloured by confidence | 0 until `local_classifier`, `jeff` or `publish_labels` runs |
| 6 | Gründungsdatum | `established_at`, `established_year` | `gleif` (the day the current legal entity was created, not when the business began; wins over the website at confidence 1.0), `structured_data` (the year goes to `founding_year`, V6; a full date also to `established_at`), `website_founding_text` (a year in the page text next to the company's name, confidence 0.55, below schema.org's 0.6), `openregister` (the year of a backlog page only, to `founding_year`, #137), `wikidata` (inception, P571, to `founding_year` at 0.58: when the organisation began, like the website; #137) | GLEIF: 9,647 of the first 10,000 Berlin LEIs have a date (API, 2026-11-04); website: year 12, date 0 (local, 2026-11-04, whole-name rule); website text: year 318 (local, 2026-11-08); Wikidata: year 424 of 552 matched companies, 274 with no other year (local, 2026-11-09) |
| 7 | Adresse | `address`, `postcode`, `city` | OffeneRegister, `gleif` (the legal address, newer), `impressum` (the address on the company's own Impressum); seat from `handelsregister_lookup` | city 296,897; street and postcode 116,527 |
| 8 | Rechtsform | `legal_form` | OffeneRegister, `gleif` (both read from the end of the name) | 257,244 |
| 9 | Cluster (n=5) | — | removed with #47; comes back with a classifier | — |
| 10 | Technologiefeld | `technology_field` | `branch_classifier` (0.60), `wirtschaftsatlas` (#148, 0.56), `llm_technology_field` (Claude, 0.55), `branch_textcat` (0.52-0.55), `local_classifier` and `jeff` (#194, each at most 0.50); any guess above 0 is shown, coloured by confidence (#183) | 194 |
| 11 | Website | `website` | `website_discovery`, `wikidata`, `common_crawl`, `wirtschaftsatlas`, `register_crosslink`, `web_search` | 6,090 |
| 12 | Geschäftsführung / Kontakt | `management` | register officers (`import_register_managers.py`), `spacy_ner`; email and phone from `impressum_contacts`, stored, never resolved; emails listed on the company page (`V6__company_emails.sql`), phones not shown | names 111,374 |
| 13 | Tätigkeitsbeschreibung | `business_purpose`, `activity_keywords`, `activity_description` | `website_keywords` (keywords published once 500 companies are read); `activity_description` ("Tätigkeit laut Website", #152): the homepage's meta or og:description, without person names or contact data, cut at 300 characters; the purpose from `openregister`; OffeneRegister has no purpose | purpose 0; description 0 until `website_keywords` re-reads (local copy of play, 2026-11-07: 300 of 482 stored homepages would get one, 46 of 50 hand-checked say what the company does) |
| 14 | Kommentar | `comments` | people in the app | — |
| 15 | DeepTech Klassifikation | `deeptech_class` | `local_classifier` (#183): non-tech, tech, hybrid or deep tech; corrections from the app | 0 until `local_classifier` runs |
| 16 | Konfidenz der Klassifikation | `deeptech_confidence` | the DeepTech observation's confidence | 0 |
| 17 | isAI | `is_ai` | `local_classifier` (#183); corrections from the app | 0 until `local_classifier` runs |
| 18 | Timestamp | `updated_at` | the coordinator | all |
| 19 | Amazon-Effekt (optional) | — | removed with #47; meaning not defined yet | — |
| 20 | Schließung / Liquidation (optional) | `register_status`, `register_announcement` | status from OffeneRegister, `handelsregister_lookup` and `gleif` (`ACTIVE` active, `INACTIVE` dissolved, dated by GLEIF's last update); `register_announcements` adds a hint next to it, never a change of it | status 296,486; announcements 854 |

Every column can also be corrected in the app (`user_feedback`).

## Sources not built

| Source | Would fill | Why not |
|---|---|---|
| Annual accounts (Bundesanzeiger, Unternehmensregister) | 2, 3, with history | no bulk access, a CAPTCHA in front of the documents, and small companies need not publish revenue (§ 326 HGB, to be checked) |
| Handelsregister documents (Gegenstand, founding date) | 6, 13 | the search page alone is used, and only by hand; documents would be systematic retrieval |
| A commercial data vendor | many | no contract; whether one comes is open |
| TED (EU public tenders) | none of the 20 | a notice lists all winners of all lots across countries; identifiers are VAT numbers, not register numbers; winners include individuals, so personal data. 11,113 notices with a Berlin winner (2026-11-04, #104) |

## Adding a source

1. Read issue #18 first.
2. Register it with `ensure_source(...)` and write observations through `insert_text_observation`
   (`source_enrichment_common.py`), never canonical values.
3. Trigger `pipeline_coordinator` when something changed.
4. A new field needs its key in the upsert key **and** the `ON CONFLICT ... WHERE` of
   `insert_text_observation` in the same commit, in feedback, in `v_field_provenance` and, if the
   app shows it, in `v_company_claims`.
