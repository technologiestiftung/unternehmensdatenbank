# DAGs: the pipelines

Nineteen DAG packages and one library, all in `airflow/dags/`. Which source each one reads:
[sources](sources.md). Every DAG starts paused (Airflow's default, `dags_are_paused_at_creation`);
unpausing it in Airflow is the only switch.

| package | DAG id | schedule | what it does |
|---|---|---|---|
| `pipeline_coordinator/` | `pipeline_coordinator` | triggered | the ONLY thing that decides which observation wins a field |
| `website_discovery/` | `website_discovery` | `@hourly`, a full batch starts the next | guesses domains, reads the Impressum, keeps only websites the page proves are the company's (`confidence.identifies_company`). Rechecks after `WEBSITE_DISCOVERY_RECHECK_DAYS` (90). |
| `wikidata/` | `wikidata` | `@daily`, a full batch starts the next | websites Wikidata lists for Berlin organisations (one SPARQL query a day, CC0), matched by register number and name or a unique name of a company that is not dissolved, each verified like a guessed domain (`pipeline.verify_proposed`); only a page that identifies the company is shown |
| `common_crawl/` | `common_crawl` | `@weekly`, a full batch starts the next | Impressum pages of the newest Common Crawl that name a Berlin register number: 300 `.de` index blocks a run, position in `pipeline_state`; each host proposed to the company whose register identifier it names and verified like a guessed domain (`enrichment.proposals`). Each run also logs how many pages name only HRA/HRB numbers we do not have, and how many of those name one above the OffeneRegister range (#99), to decide whether such pages should create companies. The counts are per run: a page seen in two runs counts twice |
| `website_contacts/` | `website_contacts` | triggered | the company's own `info@`-style email and phone from the Impressum of its proven website (source `impressum_contacts`). Stored, never resolved, so never shown. A failed read is retried, 3 times at most |
| `website_keywords/` | `website_keywords` | triggered | keywords from meta tags and services/products headings; generic ones measured out over all companies read; publishes `activity_keywords` once 500 companies are read; retried 3 times at most |
| `structured_data/` | `structured_data` | `@weekly` | the founding date or year the company's own website states in its schema.org JSON-LD, read from the stored pages of its published website only (no downloads), only from a node that names the company. The year becomes `founding_year` (V6), a full date also `established_at`; a year alone never becomes 1 January; pages disagreeing on the year write nothing |
| `technology_field/` | `technology_field` | triggered | Berlin's Technologiefeld from the keywords on the homepage and services pages (`branch_classifier`); keywords weighed over all companies read, so every run classifies them all again; writes once 500 companies are read; a value a company loses drops to confidence 0; retried 3 times at most. Where no keyword fired, a promoted text classifier may fill the field (`decided_by = textcat`) |
| | `technology_field_train` | `@weekly` | trains the text classifier on the keywords' answers and the app's votes, from the corpus; promotes it only when it is right often enough on held-out companies people judged and the keywords left empty (none yet) |
| `gleif/` | `gleif` | `@daily` | Berlin companies with an LEI from GLEIF's open data: the full file on the first run, then the weekly change file (up to 5 days since the last good run), the monthly one (up to 28), else the full file again. Only Charlottenburg entries (`RA000217`) with a Berlin address. Linked like register announcements (`enrichment.register_match`); active companies we lack are created, inactive ones only update a company holding their register number (never matched by name). Writes name, legal form, address, postcode, city and status (`ACTIVE` active, `INACTIVE` dissolved) dated by GLEIF's last update, and the LEI as an identifier. A name differing only in case or punctuation is not written |
| `register_announcements/` | `register_announcements` | `@weekly` | Berlin's register announcements of the last 8 weeks from handelsregister.de (2 requests, the result list only, never a detail page). Linked by register number, then name and city (never to a deleted company); unknown companies are created. Publishes the latest as `register_announcement`, a hint next to `register_status`, never a change of it |
| `openregister/` | `openregister` | **by hand only, never scheduled** | Berlin companies incorporated since 2019 from OpenRegister's company search (key `OPENREGISTER_API_KEY`), at most `OPENREGISTER_MAX_SEARCHES` (10) searches of 10 credits per run, stopping when the balance runs low; the position is kept in `pipeline_state`. Written like `gleif` (`enrichment.register_match.write_entry`), with the business purpose. No retries: a retry would spend credits again. Storing and regular retrieval are not yet confirmed by the operators |
| `handelsregister_lookup/` | `handelsregister_lookup` | **by hand only, never scheduled** | name, seat and status of up to 50 companies with a website from handelsregister.de, one search each by register number, 72 s apart (the portal allows 60 an hour). The portal's Nutzungsordnung forbids systematic retrievals that keep a partial register current (Ziffer 3); built anyway on the developer's decision of 2026-10-31, so a person starts every run. Result list only, never a document |
| `wirtschaftsatlas/` | `wirtschaftsatlas` | **by hand only, never scheduled** | companies with a website from Berlin Partner's Wirtschaftsatlas (about 57 GeoJSON layers, 2 s apart), matched by name key and postcode, else a unique name of a company that is not dissolved; each website verified like a Wikidata proposal (`enrichment.proposals`), up to 500 a run. The atlas offers no licence; its Impressum asks for Berlin Partner's prior consent. Built anyway on the developer's decision of 2026-11-04 while Berlin Partner is asked (#105), so a person starts every run. The layers (AI, FinTech, ...) are kept in the record, not yet used |
| `register_crosslink/` | `register_crosslink` | `@weekly` | websites from register numbers discovery found on another company's guessed site (#112): each (host, one of our companies' register entries) is proposed to that company and verified like a Common Crawl proposal; a host naming more than 2 of our companies proposes nothing. Local, 2026-11-04: 6,048 proposals, 7 of the first 100 proven (all by the own register number), 3 of them new |
| `berlin_zuwendungen/` | `berlin_zuwendungen` | `@monthly` | Land Berlin grants to HRA/HRB recipients into `company_fundings`, linked by register number. Then `create_companies`: a recipient's register number we do not have is linked by a unique live name in the same city, or, above the OffeneRegister snapshot's range, created as a company (name, legal form, city; no register status); triggers the coordinator (#103) |
| `cordis/` | `cordis` | `@monthly` | EU research grants (Horizon Europe, Horizon 2020) of private Berlin companies from CORDIS's CSV files into `company_fundings`, one row per participation, linked by a name key unique in its city like the Förderkatalog; never creates a company (no register number) |
| `foerderkatalog/` | `foerderkatalog` | `@monthly` | every Berlin grant from the Förderkatalog des Bundes into `company_fundings` |
| `feedback_observations/` | `feedback_observations` | `@hourly` | people's corrections from the app as observations of `user_feedback` (priority 50); shown once two people agree, never who. Triggers the coordinator only when something changed |
| `manager_ner/` | `manager_ner_extract` | triggered | spaCy reads the Impressum and writes Geschäftsführer names (source `spacy_ner`) |
| | `manager_ner_train` | `@weekly` | turns the app's votes into training data, plus register officers found on the Impressum as extra labels (#58); promotes a model only if it beats the current one on a held-out fifth people voted on |
| `enrichment/` | — | — | the library: URL patterns, Impressum regex, confidence, classification. `__init__.py` is the map |

Grants are linked to companies by `refresh_company_funding_links()` after a load.

- **observation**: one claim about one field of one company, from one source, with a confidence.
  Sources write observations and nothing else.
- **canonical**: the observation that won a field, what the app shows. Only `pipeline_coordinator`
  decides, by source priority first, then confidence.

Shared code:

- `source_enrichment_common.py`: the database helpers (`PG_DSN`, `ensure_source`,
  `upsert_source_record`, `insert_text_observation`, batch queries, `DISCOVERY_ACTIVE_ONLY`).
  `insert_text_observation`'s `ON CONFLICT ... WHERE` repeats the partial index predicate of the
  upsert key verbatim: widen one, widen the other, same commit.
- `pipeline_task.py`: build every task with `pipeline_task(task_id, run)`; it runs in Airflow's
  Python locally and as a pod on the hub ([release](release.md)). A failed task runs again twice,
  10 minutes apart: on play a 503 from foerderportal.bund.de and a Wikidata timeout each failed a
  run that would otherwise wait for the next schedule (#86). `retries=0` turns that off;
  `handelsregister_lookup` uses it, because a retry would spend the portal's request limit again.
  `run` may live in any module of its package (`technology_field.train:run_train`); a unit test
  resolves every task of every DAG file the way a pod does.
- Import modules **by package name** (`from enrichment.confidence import ...`), never as a bare
  module after a `sys.path` insert: two same-named files would get each other's.

Schema changes belong in `db/migrations/`.
