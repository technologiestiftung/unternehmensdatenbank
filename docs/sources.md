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
schedule stored in `sources` (`V4__source_details.sql`), its values per confidence range and its
last 20 runs. A new source needs those four columns filled by a migration, or its page stays
without them.

Record counts are from the local database on 2026-11-02.

| Source key | What it is | How we get it | How often | Records |
|---|---|---|---|---|
| `offeneregister` | OffeneRegister: a snapshot of the Handelsregister, read 2017–2019. Name, legal form, address, status, register number, officers | bulk file, imported by scripts ([data-model](data-model.md), "Loading the OffeneRegister Berlin set") | once | 296,486 |
| `registerbekanntmachungen` | register announcements on handelsregister.de (deletions, liquidations, insolvencies) | DAG `register_announcements`: 2 requests, the result list only, never a detail page | weekly | 900 |
| `gleif` | GLEIF, the LEI registry (open data, CC0): legal name, legal address and status of Berlin register entries that have an LEI. No officers | DAG `gleif`: GLEIF's full file once, then daily the weekly change file (up to 5 days without a good run), the monthly one (up to 28), else the full file again; only Charlottenburg entries with a Berlin address. Creates the active companies we lack (4,781 measured on 2026-11-04, most founded after 2019) | daily | 13,500 (measured on 2026-11-04, not yet loaded) |
| `handelsregister_lookup` | the Handelsregister search on handelsregister.de: today's name, seat and status | DAG `handelsregister_lookup`: one search per company by register number, ≤50 per run. **Started by hand only**: the portal's terms forbid systematic retrievals (Nutzungsordnung, Ziffer 3) | by hand | 2 |
| `wirtschaftsatlas` | Berlin Partner's Wirtschaftsatlas Berlin: about 7,600 companies with website, in Berlin's industry layers (AI, FinTech, Robotik, ...) | DAG `wirtschaftsatlas`: downloads the map's company layers, matches by name and postcode, verifies each website like a Wikidata proposal. **Started by hand only**: the Impressum asks for Berlin Partner's prior consent (#105) | by hand | 2,253 (2026-11-04) |
| `website_discovery` | the company's own website | DAG `website_discovery`: guesses domains from the name, reads the Impressum, keeps a site only if the page proves it is that company ([website-verification](website-verification.md)) | hourly batches | 65,587 |
| `wikidata` | official websites (property P856) Wikidata lists for Berlin organisations | DAG `wikidata`: one SPARQL query, each website verified like a guessed domain | daily | 0 here (531 matched in a local copy, 2026-10-30) |
| `common_crawl` | Impressum pages in the Common Crawl that name a Berlin register number | DAG `common_crawl`: 300 index blocks a run, each website verified like a guessed domain | weekly | 0 here (pilot: 21 websites from 5,000 pages, #56) |
| `impressum_contacts` | email and phone from the Impressum of the proven website | DAG `website_contacts` | after discovery | 150 |
| `website_keywords` | keywords from meta tags and services/products headings | DAG `website_keywords` | after discovery | 150 |
| `structured_data` | the founding date or year a company's own website states in its structured data (schema.org) | DAG `structured_data`: reads stored pages of the published website, no downloads | weekly | 16 (local, 2026-11-04) |
| `branch_classifier` | Technologiefeld from the keywords on the website ([branch-classification](branch-classification.md)) | DAG `technology_field`; a spaCy text classifier may fill gaps | after discovery | 542 |
| `spacy_ner` | Geschäftsführer names a spaCy model finds on the Impressum | DAG `manager_ner_extract` | after discovery | 72 |
| `foerderkatalog` | grants of the federal government (Förderkatalog des Bundes) | DAG `foerderkatalog`, into `company_fundings`, linked by name and city | monthly | grants table |
| `berlin_zuwendungen` | grants of Land Berlin (Zuwendungsdatenbank) | DAG `berlin_zuwendungen`, into `company_fundings`, linked by register number | monthly | grants table |
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
| 2 | Mitarbeiter | `employee_count` | no source yet | 0 |
| 3 | Umsatz | `revenue` | no source yet | 0 |
| 4 | Förderungen | `funding_count`, `funding_total_eur`, `fundings` | `foerderkatalog`, `berlin_zuwendungen` | 5,189 companies with a grant |
| 5 | WZ-Nummer | `wz_code` | no source yet: OffeneRegister has none | 0 |
| 6 | Gründungsdatum | `established_at`, `established_year` | `structured_data` (the year goes to `founding_year`, V6; a full date also to `established_at`) | year 16, date 1 (local, 2026-11-04) |
| 7 | Adresse | `address`, `postcode`, `city` | OffeneRegister, `gleif` (the legal address, newer); seat from `handelsregister_lookup` | city 296,897; street and postcode 116,527 |
| 8 | Rechtsform | `legal_form` | OffeneRegister, `gleif` (both read from the end of the name) | 257,244 |
| 9 | Cluster (n=5) | — | removed with #47; comes back with a classifier | — |
| 10 | Technologiefeld | `technology_field` | `branch_classifier` | 194 |
| 11 | Website | `website` | `website_discovery`, `wikidata`, `common_crawl`, `wirtschaftsatlas` | 6,090 |
| 12 | Geschäftsführung / Kontakt | `management` | register officers (`import_register_managers.py`), `spacy_ner`; email and phone from `impressum_contacts`, stored, not shown | names 111,374 |
| 13 | Tätigkeitsbeschreibung | `business_purpose`, `activity_keywords` | `website_keywords` (published once 500 companies are read); OffeneRegister has no purpose | purpose 0 |
| 14 | Kommentar | `comments` | people in the app | — |
| 15 | DeepTech Klassifikation | `deeptech_class` | no classifier yet; corrections from the app can fill it | 0 |
| 16 | Konfidenz der Klassifikation | `deeptech_confidence` | the DeepTech observation's confidence | 0 |
| 17 | isAI | `is_ai` | no classifier yet; corrections from the app can fill it | 0 |
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

## Adding a source

1. Read issue #18 first.
2. Register it with `ensure_source(...)` and write observations through `insert_text_observation`
   (`source_enrichment_common.py`), never canonical values.
3. Trigger `pipeline_coordinator` when something changed.
4. A new field needs its key in the upsert key **and** the `ON CONFLICT ... WHERE` of
   `insert_text_observation` in the same commit, in feedback, in `v_field_provenance` and, if the
   app shows it, in `v_company_claims`.
