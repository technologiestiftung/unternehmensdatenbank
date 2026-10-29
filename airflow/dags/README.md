# DAGs

Eight DAG packages and one library. Every DAG starts paused (Airflow's default,
`dags_are_paused_at_creation`); unpausing it in Airflow is the only switch.

| package | DAG id | schedule | what it does |
|---|---|---|---|
| `pipeline_coordinator/` | `pipeline_coordinator` | triggered | the ONLY thing that decides which observation wins a field |
| `website_discovery/` | `website_discovery` | `@hourly`, a full batch starts the next | guesses domains, reads the Impressum, keeps only websites the page proves are the company's (`confidence.identifies_company`). Rechecks after `WEBSITE_DISCOVERY_RECHECK_DAYS` (90). The OpenStreetMap step is off (`WEBSITE_DISCOVERY_OSM=false`): the public Overpass server refused requests |
| `website_contacts/` | `website_contacts` | triggered | the company's own `info@`-style email and phone from the Impressum of its proven website (source `impressum_contacts`). Stored, never resolved, so never shown. A failed read is retried, 3 times at most |
| `website_keywords/` | `website_keywords` | triggered | keywords from meta tags and services/products headings; generic ones measured out over all companies read; publishes `activity_keywords` once 500 companies are read; retried 3 times at most |
| `register_announcements/` | `register_announcements` | `@weekly` | Berlin's register announcements of the last 8 weeks from handelsregister.de (2 requests, the result list only, never a detail page). Linked by register number, then name and city (never to a deleted company); unknown companies are created. Publishes the latest as `register_announcement`, a hint next to `register_status`, never a change of it |
| `berlin_zuwendungen/` | `berlin_zuwendungen` | `@monthly` | Land Berlin grants to HRA/HRB recipients into `company_fundings`, linked by register number |
| `foerderkatalog/` | `foerderkatalog` | `@monthly` | every Berlin grant from the Förderkatalog des Bundes into `company_fundings` |
| `manager_ner/` | `manager_ner_extract` | triggered | spaCy reads the Impressum and writes Geschäftsführer names (source `spacy_ner`) |
| | `manager_ner_train` | `@weekly` | turns the app's votes into training data; promotes a model only if it beats the current one on a held-out fifth |
| `enrichment/` | — | — | the library: URL patterns, OSM matching, Impressum regex, confidence, classification. `__init__.py` is the map |

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
  Python locally and as a pod on the hub ([release](../../docs/release.md)).
- Import modules **by package name** (`from enrichment.confidence import ...`), never as a bare
  module after a `sys.path` insert: two same-named files would get each other's.

Schema changes belong in `db/migrations/`.
