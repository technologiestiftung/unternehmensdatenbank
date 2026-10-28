# DAGs

Seven DAG packages and one library. The Technologiefeld has no DAG yet; it is rebuilt on the
library next (issue #18).

| package | DAG id | schedule | flag | what it does |
|---|---|---|---|---|
| `pipeline_coordinator/` | `pipeline_coordinator` | triggered | — | the ONLY thing that decides which observation wins a field |
| `manager_ner/` | `manager_ner_extract` | triggered | `ENABLE_MANAGER_NER` | spaCy reads the Impressum of companies with a website and writes Geschäftsführer names (source `spacy_ner`) |
| | `manager_ner_train` | `@weekly` | `ENABLE_MANAGER_NER` | turns the app's votes into training examples; promotes a model only if it beats the current one on a held-out fifth |
| `website_discovery/` | `website_discovery` | triggered | `ENABLE_WEBSITE_DISCOVERY` | guesses each company's domains, reads the Impressum, and keeps only websites the page proves are the company's (`confidence.identifies_company`) |
| `website_contacts/` | `website_contacts` | triggered | `ENABLE_WEBSITE_CONTACTS` | the company's own `info@`-style email and phone from the Impressum of its proven website (source `impressum_contacts`). **Stored, not published**: nothing resolves `email`/`phone` until a privacy review. A failed read (no answer, no Impressum) is tried again, 3 times at most |
| `website_keywords/` | `website_keywords` | triggered | `ENABLE_WEBSITE_KEYWORDS` | product and service keywords from the website's meta keywords and the headings of its services/products pages; generic ones measured out over all companies read; publishes `activity_keywords` (V8) once 500 companies are read; a site that did not load is tried again, 3 times at most |
| `berlin_zuwendungen/` | `berlin_zuwendungen` | `@monthly` | `ENABLE_BERLIN_ZUWENDUNGEN` | the Land Berlin's own grants (Zuwendungsdatenbank) to HRA/HRB recipients into `company_fundings`, linked by register number (V6) |
| `foerderkatalog/` | `foerderkatalog` | `@monthly` | `ENABLE_FOERDERKATALOG` | every Berlin grant from the Förderkatalog des Bundes into `company_fundings`; linked to companies on read (`v_company_fundings`) |
| `enrichment/` | — | — | — | the library: URL patterns, OSM matching, Impressum regex, confidence, industry classification. Its `__init__.py` is the map |

Two locks stop a DAG running: its `ENABLE_*` flag must be `true` **and** it must be unpaused in
Airflow. The flag is checked when the task runs, not when the file is parsed, so a disabled DAG
still shows in the UI and fails with a clear message. `foerderkatalog` and `manager_ner_train` then
run on their schedule by themselves.

Two words you need first:

- **observation**: one claim about one field of one company, from one source, with a confidence.
  Sources write observations and nothing else; they never overwrite each other.
- **canonical**: the observation that won a field, which is what the app shows. Only
  `pipeline_coordinator` decides this, by source priority first, then confidence.

Shared code:

- `source_enrichment_common.py`: database helpers every source uses: connection (`PG_DSN`),
  `ensure_source`, `upsert_source_record`, `insert_text_observation`, the batch queries and
  `DISCOVERY_ACTIVE_ONLY`. `insert_text_observation`'s `ON CONFLICT ... WHERE` repeats the partial
  index predicate of the observation upsert key (in the baseline migration) verbatim; widen one, widen the other, in the
  same commit.
- DAG packages import their modules **by package name** (`from enrichment.confidence import ...`),
  never as a bare module after a `sys.path` insert: two packages with a same-named file would
  otherwise get each other's.

Schema changes belong in `db/migrations/`, not here.
