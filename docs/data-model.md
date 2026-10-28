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

The officers then give the app its management names: `import_register_managers.py` writes each
current Geschäftsführer (name and position only) as a `ceo_name_or_contact` observation, and the next
`pipeline_coordinator` run publishes them. Safe to re-run. Its docstring has the rules and the
privacy review that allows it.

```bash
python scripts/import_register_managers.py
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

- `V1__baseline.sql` is the whole schema as of 2026-10-29. Nothing was deployed, so the migrations
  before it were squashed twice: the 24 of the Osnabrück era on 2026-10-28 (issue #18), then V1-V9
  of the Berlin rebuild on 2026-10-29. Checked: a database built from it and one built from the old
  files dump to the same schema, grants and seed rows. The sections below keep the old migration
  names in brackets, so git history finds the reasoning.
- **A local database migrated before the squash** still has the old files in its Flyway history,
  and Flyway refuses to run until that is reset. The schema is already the same, so no data moves:
  ```bash
  docker compose exec -T postgres psql -U app -d unternehmensdatenbank -c "DROP TABLE flyway_schema_history"
  docker compose --profile tools run --rm flyway baseline -baselineVersion=1 -baselineDescription=baseline
  ```
  Only for a database that was at V9. One with an unmerged migration on top (a V10 from a branch)
  keeps that table and column; renumber the migration to V2 before it lands.
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
`source_id`), and the coordinator now writes every name above the bar, not one per company. A name
whose observation is gone is deleted by the coordinator: spaCy deletes what it read off a website
the company no longer has. Since V7 the management part of `v_field_provenance` carries the
observation's evidence: `["register_officer"]` when the name is one of the company's register
officers, `[]` when it is not; which officer matched is never stored. On the local Berlin data on
2026-10-28 that left 29 names (67 before), 7 of them confirmed by the register.
With the register's Geschäftsführer added (`import_register_managers.py`), 156,846 register names
and the 29 spaCy names are published, for 111,374 companies.

## Grant links (V4)

Which company a grant belongs to is stored in `company_funding_links`, not computed on every read.
`v_company_fundings` used to match all companies by name and city on each query (2.2 s), and the
company list asked it once per company: 71 s for a page of 50. `refresh_company_funding_links()`
recomputes the links (same rule: normalised name and city match exactly one company); the
Förderkatalog DAG and the OffeneRegister import call it after they load. A list page now takes
0.65 s.

## The stored company profile (V5)

`v_company_profile`, which the app and Hasura read, is now a stored copy (`company_profile_mv`) of
the live definition (`v_company_profile_live`), plus `source_keys`: which sources the company's shown
values came from (from `v_company_sources`). A selective filter used to make Postgres compute the
profile of company after company: 5 to 45 seconds to filter by source. Now every list filter takes
under 10 ms. `refresh_company_profile()` rebuilds it in about 3 seconds and refreshes the planner's
statistics; the coordinator calls it at the end of every run, the Förderkatalog DAG and the
OffeneRegister import after they load. Between refreshes the list shows the last resolved state.

## Berlin grants (V6)

`company_fundings` now holds a second source: the Land Berlin's Zuwendungsdatenbank (DAG
`berlin_zuwendungen`, monthly). Each grant has a `source` (`foerderkatalog` or `berlin_zuwendungen`).
Berlin grants also keep the recipient's register entry (`recipient_register`, e.g. `HRB 84522`) and
policy area. Only grants to an HRA/HRB entry are loaded: 22,354 of the 55,454 on 2026-10-28.
The others went to associations, public bodies and recipients without a register entry.

`refresh_company_funding_links()` links a grant that names a register entry **by that entry only**
(`match_method = 'register_number'`). A grant without one is linked by name and city, as before. A register
number that matches none of our companies stays unlinked, whatever the name says. Measured
2026-10-28: 18,427 Berlin grants linked to 3,861 companies (6.6 bn €), 3,927 unlinked. Companies
with any grant: 1,691 → 5,179. The unlinked ones are most likely companies registered after the
OffeneRegister snapshot (2017–2019); not checked.

## Register announcements (V10)

The DAG `register_announcements` reads Berlin's register announcements from handelsregister.de once a
week. The court keeps each one up for 8 weeks, so a weekly run misses none. It reads only the result
list (category, register number, name, city, date), never a detail page: the details of deletions
and mergers name liquidators and officers.

- **Every announcement is a source record** (source `registerbekanntmachungen`, `external_id` the
  portal's id), linked by register number through `company_identifiers` (`match_method =
  'identifier'`).
- **A company we do not have is created** (`match_method = 'created'`, `companies.metadata.created_by`)
  with name, legal form, city, register number and its identifier. Name key and legal form come from
  `enrichment.register_names`, the same code the OffeneRegister import uses. Measured 2026-10-28: 900
  announcements, 451 matched (all `aktiv` in the snapshot), 425 companies created. 24 Berlin grants
  then linked to created companies by register number.
- **The name comes second, before creating.** An unknown register number can be a company we have
  under an old number: a change of legal form (HRA to HRB) gets a new one. So the DAG looks for
  companies, not merged, with the same `normalized_name` and the same city (ignoring case). Exactly
  one that is not `erloschen`: it is the company (`match_method = 'fuzzy'`, `match_confidence` 0.9),
  and it gets the new register number as an identifier. Several live ones, or only deleted ones: the
  company is created, and each of them becomes an open `match_candidates` row (score 0.9, its
  `register_status` in `reasons`), so a person decides and merges. None: it is created.
  A deleted company is never linked by name. A firm name only has to be distinguishable at its place
  (§ 30 HGB, to be checked), so once a company is deleted its name can be used again by a new one.
  Why it matters, measured 2026-10-28 without the rule: 14 of the 425 created companies shared name
  key and city with a company we had (11 `aktiv`), and 66 grant links by name and city would have
  been lost, because `refresh_company_funding_links` links by name only when name and city are unique.
  The DAG refreshes those grant links after any run that created a company or linked one by name.
- **The profile shows the latest one as a hint**: `register_announcement` (its kind) and
  `register_announcement_date`. Each announcement is an observation; the DAG gives the latest per
  company confidence 1.0 and older ones 0, and the coordinator publishes the latest. It never changes
  `register_status`: 147 companies shown `aktiv` had a deletion announced on 2026-10-28.

V9 is the vote-delete migration, renumbered from a second V8 (#33).

## Website contacts and keywords (V8)

Two DAGs read more from a website `website_discovery` has already proven to be the company's.

- **`website_contacts`** (source `impressum_contacts`) writes `email` and `phone` observations from
  the Impressum. An email counts only at the website's own domain and only when its mailbox is not
  shaped like a person (`max.mustermann@`, or a manager named on the same page). On the first 37
  Berlin websites (2026-10-28): 27 emails, all role mailboxes (`info@` 16, `kontakt@` 4, ...), and
  25 phones. **Nothing resolves them**, so they reach neither `v_company_profile` nor
  `v_field_provenance`: a sole trader's phone is personal data, and the privacy review does not
  cover it yet.
- **`website_keywords`** (source `website_keywords`) stores keyword-shaped headings of the services
  and products pages, and the homepage's meta keywords, in its source record; a second task
  measures which terms are generic across all companies read and writes one `activity_keywords`
  observation per company ("Kältetechnik; Wärmepumpen"). V8 adds that column to
  `v_company_profile` and `activity_keywords` as a feedback key. Nothing is published before 500
  companies are read, because below that no heading is common enough to be measured as generic.
  A company whose published website is no longer the one that was read loses its keywords.

Both read each company once. A read that failed (the homepage did not load; for contacts, no
Impressum was found) is stored with `failed: true` and an `attempts` count in its source record and
tried again on later runs, 3 times at most. A failed read counts for nothing, including the 500.

V8 also shows how to add a profile column: `v_company_profile_live` gains it at the end, and
`company_profile_mv` and `v_company_profile` are dropped and rebuilt, since the stored copy froze its
columns when it was created.

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
