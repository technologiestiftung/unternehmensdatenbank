# Data Model

## ELI5

Think of the database like this:

1. one table stores the "best current company card"
2. one table stores "where did we see this company?"
3. one table stores "what exact facts did that source claim?"
4. one table stores "which source won for each field?"

If that is all you remember, the model will make sense.

## The Simple Story

Imagine three websites talk about the same company:

- source A says the website is `example.com`
- source B says the website is `example.org`
- source C has no website but has a better address

We do **not** want to lose any of that.

So we store:

- the current best company row in `public.companies`
- the raw source row in `public.company_source_records`
- each individual claim like `website_url = example.com` in `public.company_field_observations`
- the final "winner" for each field in `public.company_canonical_field_sources`

That is the whole design.

## Tables

### `public.companies`

This is the table your app reads most of the time.

It is the "current best version" of a company.

If someone asks:

- "show me the company"
- "what is the current website?"
- "what is the current address?"

this is the table they usually mean.

Important fields:

- `id`: UUID primary key
- `company_name`: the current chosen display name
- `normalized_name`: simplified name used for matching and search
- `registration_number`: strongest legal identifier when we have one
- `address`: current chosen address
- `legal_form`: current chosen legal form
- `established_at`: current chosen establishment date when we know it
- `wz_code`: current chosen business classification code
- `business_purpose`: canonical company description / purpose when we know it
- `postcode`, `city`, `district`, `locality`: location fields for search and rough deduplication
- `register_status`: whether the register still lists the company — see below, it is not a small
  detail
- `metadata`: technical/system metadata
- `canonical_updated_at`: when the winning values were last recalculated
- `data_last_updated_at`: when business data last changed

Short version:

- `companies` = "best current company profile"

#### `register_status`, and why more than half this table is dead

The Northdata export has a `Status` column. It says whether the company still exists, and over the
14,443 Osnabrück rows it says:

| value | companies | share |
|---|---|---|
| `erloschen` — dissolved, struck off | 7,673 | 53.1% |
| `aktiv` | 6,298 | 43.6% |
| `in Liquidation` — winding down | 472 | 3.3% |

So **most of this table is companies that no longer exist**, and every number this project has
published — 902 websites found, 842 of them high-confidence, 346 Technologiefelder — was measured
over the dead ones too. The dead ones contributed 36 of those 902 websites, 4.0%. Two things follow.

First, a dissolved company's domain is exactly the kind that gets sold to somebody else, so a website
"found" for an `erloschen` company deserves more suspicion than the confidence number gives it.

Second, the expensive stages can skip them. `DISCOVERY_ACTIVE_ONLY=1` restricts every discovery
query and every new model call to `register_status = 'aktiv'` — `in Liquidation` counts as inactive,
which on the earlier example data cost 7 high-confidence websites and skipped 472 companies. With
a limit, the limit applies *after* the filter, so a limited run gets a different sample, not a
smaller one. Compare a filtered run only with another filtered run.

The import fills it from the register's own status; `metadata` keeps the raw value as well.

> **The load has to map this column.** The prepared CSV goes into Postgres with a `\copy`.
> `prepare_offeneregister_import.py` emits `register_status`, but if the `\copy` column list leaves
> it out, every row lands `NULL`; the filter is `= 'aktiv'`, which excludes `NULL`, so an
> active-only run would find nothing to do at all. The command in "Loading the OffeneRegister
> Berlin set" below maps it.

### `public.sources`

This is the address book of upstream systems.

Examples:

- `handelsregister`
- `offeneregister`
- `fusionbase`
- `manual_import`

Important fields:

- `key`: stable code such as `handelsregister`
- `source_type`: registry, website, api, manual, etc.
- `priority`: lower wins during canonical resolution
- `default_sync_interval_minutes`: scheduling hint for Airflow
- `last_synced_at` and `next_sync_at`: operational sync timestamps

Short version:

- `sources` = "which systems do we import from?"

### `public.source_sync_runs`

This is the run log.

Every time a source import starts, we create one row here.

It answers questions like:

- "did yesterday's import fail?"
- "how many records did we see?"
- "when did the run finish?"

Important fields:

- `sync_type`: `full`, `incremental`, `backfill`, `manual`
- `status`: `pending`, `running`, `success`, `failed`, `partial`, `cancelled`
- counters for `records_seen`, `records_created`, `records_updated`, `records_failed`

Short version:

- `source_sync_runs` = "logbook of import jobs"

### `public.company_source_records`

Exactly one current row per `(source_id, external_id)`.

This is the raw company row from one source.

If `offeneregister` has company `F1103R_HRB12345`, that source row lives here.

It keeps:

- the source payload
- the upstream external identifier
- the source URL
- last-seen and last-synced timestamps
- soft-delete state from the upstream source

This table answers:

- "what did this source say about this company?"
- "what is the raw payload we imported?"

Short version:

- `company_source_records` = "one company as seen by one source"

### `public.company_field_observations`

This is the most important table to understand.

Each row is one claim about one field.

Examples:

- company X, field `website_url`, value `https://example.com`, source `handelsregister`
- company X, field `employee_count`, value `18`, source `manual_import`
- company X, field `address`, value `Berlin, ...`, source `offeneregister`

Each row stores:

- the company
- the source record
- the source and sync run that produced it
- the field name
- one typed value column
- confidence and timestamps
- whether the observation is currently canonical

Only one observation per `(company_id, field_name)` may have `is_canonical = true`.

Short version:

- `company_field_observations` = "all the individual facts we ever saw"

### `public.company_canonical_field_sources`

This table records the winner for each field.

This is what lets the application answer:

- where did the canonical website come from?
- which source currently owns `employee_count`?
- when was that decision made?

Example:

- company X
- field `website_url`
- winning source `handelsregister`
- winning observation row `abc-123`

Short version:

- `company_canonical_field_sources` = "which fact won, and from where?"

### `public.company_managers`

This stores people linked to the company after they are accepted into the canonical profile.

Examples:

- managing director
- owner
- board member

Short version:

- `company_managers` = "people attached to the final company profile"

## How One Import Flows Through The Tables

Let’s say one source row comes in.

1. We log the import job in `source_sync_runs`.
2. We ensure the source exists in `sources`.
3. We find or create the company in `companies`.
4. We store the raw source row in `company_source_records`.
5. We split the source row into individual facts and store them in `company_field_observations`.
6. We decide which fact wins for each field.
7. We write the winner back into `companies`.
8. We record the winning source in `company_canonical_field_sources`.

So:

- `companies` is the summary
- `company_source_records` is the raw row
- `company_field_observations` is the evidence
- `company_canonical_field_sources` is the explanation for why the summary looks the way it does

## If You Want The Mental Model In One Sentence

`companies` is the answer, `company_field_observations` is the proof.

## Loading the OffeneRegister Berlin set

**OffeneRegister** is a snapshot of the German Handelsregister, retrieved between June 2017 and
January 2019. It is closer to the target than Osnabrück (it is Berlin) but it is still example data:
6–9 years old, so companies founded since are missing and "active" means active in 2018.

It is the content of `unternehmensdatenbank` since 2026-10-28 (loaded into a database of its own on
2026-10-27, which then replaced the Osnabrück one). Loaded:

| | |
|---|---|
| companies (register court Berlin-Charlottenburg) | **296,486** |
| `aktiv` / `erloschen` | 142,571 / 153,915 |
| with an address | 116,527 |
| with a legal form (read from the end of the name) | 256,845 |
| WZ code, purpose, founding date, website | **none** — the dump does not have them |

The address in `companies` is cut after postcode and city, because some rows continue with register
text naming managers and their birth dates. That raw text, and the dump's officer table (348,904
Berlin entries with first and last names), are kept in the company's source record (below).
**They are personal data.** `company_source_records` is not exposed through Hasura, and a privacy
review is needed before any of it leaves local development (hub, app, export).
A part of the address naming who receives the post — `c/o`, `bei`, `z. Hd.`, `zu Händen` — is
dropped too, person or company alike: **10,423** of the 116,527 addresses had one. Before the
postcode, only parts with a digit (house number, building, postcode) are kept, so a name split by a
comma ("Muster, Erika") goes as well. Street, postcode and city stay.

Into an empty, migrated database (`./scripts/dev-up.sh` creates and migrates `unternehmensdatenbank`):

```bash
python scripts/prepare_offeneregister_import.py openregister.db berlin.csv
docker compose exec -T postgres psql -U app -d unternehmensdatenbank -c "\copy public.companies \
  (company_name, normalized_name, registration_number, legal_form, business_purpose, wz_code, address, \
  established_at, postcode, city, register_status, metadata) FROM STDIN WITH (FORMAT csv, HEADER true)" < berlin.csv
```

Everything else the dump knows goes into **`company_source_records`**, one row per company under
the source `offeneregister`, so nothing is lost: register court, type and number, registered
seat, status, the seven document flags, **former names (32,170 companies)**, **register moves
(25,584)**, **officers (348,904)** and the raw address. The same script fills `companies.city` from the registered seat where the address gave
none (179,959 rows), so every company has a city. Run it after the CSV load; it is safe to re-run.

```bash
python scripts/import_offeneregister_records.py openregister.db
```

## Resolution lifecycle

For each incoming company row from a source:

1. create or update a `source_sync_runs` row
2. upsert a canonical company identity in `companies`
3. upsert the upstream record in `company_source_records`
4. insert one or more `company_field_observations`
5. resolve the winning observation for each field
6. update canonical columns in `companies`
7. upsert the winning source pointer in `company_canonical_field_sources`

## Query groups

Working queries: [core-queries-current.sql](core-queries-current.sql).

The most important groups are:

- writes for canonical company rows
- writes for source records and observations
- reads for canonical company views
- resolution queries for choosing winners
- operational queries for stale data and failed syncs

## Migration strategy

- `V1__baseline.sql` is the whole schema as of 2026-10-28: 24 earlier migrations squashed while
  nothing was deployed (issue #18). `V2__identifiers_and_matching.sql` adds identifiers, matching
  and pipeline state.
- From here on: forward-only, and never edit a migration that has run anywhere shared.
- Add new schema changes under `db/migrations` as `V{N}__description.sql`.
- Run Flyway before Airflow workers interact with changed schema.

## Feedback and provenance (V3)

**Every field the app shows can be voted on** (`extraction_feedback`), under the app's column keys:
name, address, legal form, register status, WZ code, founding date, employees, revenue, each grant
(by its FKZ), cluster, Technologiefeld, website, each manager name, business purpose, DeepTech,
isAI, Amazon-Effekt. A vote on an EMPTY field has `value = ''` and must be `wrong` with the missing
value as its correction. Today only the website, Technologiefeld and manager pipelines learn from
votes; the rest are stored as labels.

**`v_field_provenance` says where each shown value came from**: source, method (`verified_by`,
`decided_by`, the model version, or `register`), confidence, evidence, a link (the Impressum it was
checked against) and dates. Register facts carry the date the register was read (2017–2019 for
OffeneRegister). It exposes nothing personal beyond the shown value.

**Every manager name keeps its provenance** (`company_managers.company_field_observation_id`,
`source_id`), and the coordinator now writes every name above the bar, not one per company: on the
Berlin sample that is 67 names for 37 companies instead of 37.

## Grant links (V4)

Which company a grant belongs to is stored in `company_funding_links`, not computed on every read.
`v_company_fundings` used to match all companies by name and city on each query (2.2 s), and the
company list asked it once per company: 71 s for a page of 50. `refresh_company_funding_links()`
recomputes the links (same rule: normalised name and city match exactly one company); the
Förderkatalog DAG and the OffeneRegister import call it after they load. A list page now takes
0.65 s.

## Identifiers and matching (V2)

How a record from any source finds its company. In this order:

1. **A shared identifier.** `company_identifiers` holds every identifier a source uses for a company
   (`register`, `register_former`, `opencorporates`, `fusionbase`, `vat`, `lei`); `(scheme, value)`
   is unique, so one identifier can never point at two companies. The OffeneRegister import writes
   the register number and the OpenCorporates number for all 296,486 Berlin companies.
2. **A fuzzy score** on name, postcode, street, domain and phone. High: link. Middle: a row in
   `match_candidates` for a person to decide. Low: a new company.
3. Every link records `match_method`, `match_confidence` and `matched_at` on its
   `company_source_records` row, so a wrong one can be found and undone.

Merging never deletes: the company that turned out to be a duplicate keeps `merged_into`, and its
source records and observations move to the survivor.

`pipeline_state` holds what a DAG has done so far, one row per (pipeline, key). On the Data Hub,
tasks run in pods with no shared disk, so a checkpoint file is gone by the next run.
