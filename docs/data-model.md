# Data Model

## In one sentence

`companies` is the answer, `company_field_observations` is the proof.

Several sources talk about the same company and may disagree. We lose none of it:

| Table | Holds |
|---|---|
| `companies` | the base record: one row per company, a random UUID as identity |
| `company_source_records` | the whole record one source delivered, one row per `(source_id, external_id)` |
| `company_field_observations` | one claim per field: company, source, field name, value, confidence |
| `company_canonical_field_sources` | which observation won each field, and when |
| `sources` | every source, with its `priority` (lower wins) |
| `source_sync_runs` | one row per run: status (`pending` … `cancelled`) and counters seen/created/updated/failed |
| `company_identifiers` | every identifier a source uses for a company |
| `company_managers` | the names shown as Geschäftsführung, each linked to its observation |
| `pipeline_state` | what a DAG has done so far, one row per (pipeline, key) |

Only one observation per `(company_id, field_name)` may be canonical. Only `pipeline_coordinator`
decides which one: sources write observations and trigger it. Enriched values (website,
Technologiefeld, …) are not columns of `companies`; they are read through `v_companies_enriched`.

### `companies` columns

`id`, `company_name`, `normalized_name` (for matching and search), `registration_number`,
`legal_form`, `business_purpose`, `wz_code`, `address`, `established_at`, `postcode`, `city`,
`district`, `locality`, `register_status`, `merged_into`, `metadata`, `data_quality_score`,
`canonical_updated_at`, `data_last_updated_at`, `created_at`, `updated_at`.

### `register_status`

Whether the register still lists the company: 142,571 `aktiv` and 153,915 `erloschen` in the Berlin
set. `DISCOVERY_ACTIVE_ONLY=true` restricts discovery to `register_status = 'aktiv'`; `in
Liquidation` counts as inactive. With a limit, the limit applies after the filter, so compare a
filtered run only with another filtered run. A website found for an `erloschen` company deserves
extra suspicion: dissolved companies' domains get sold.

> **The load has to map this column.** `prepare_offeneregister_import.py` emits it, but if the
> `\copy` column list leaves it out every row lands `NULL`, and an active-only run finds nothing.
> The command below maps it.

## Loading the OffeneRegister Berlin set

**OffeneRegister** is a snapshot of the German Handelsregister, retrieved between June 2017 and
January 2019: 6–9 years old, so newer companies are missing and "active" means active in 2018.

| | |
|---|---|
| companies (register court Berlin-Charlottenburg) | **296,486** |
| `aktiv` / `erloschen` | 142,571 / 153,915 |
| with an address | 116,527 |
| with a legal form (read from the end of the name) | 256,845 |
| WZ code, purpose, founding date, website | **none**: the dump does not have them |

The address in `companies` is cut after postcode and city, because some rows continue with register
text naming managers and their birth dates. A part naming who receives the post (`c/o`, `bei`,
`z. Hd.`, `zu Händen`) is dropped too: 10,423 of the 116,527 addresses had one. Before the postcode
only parts with a digit are kept, so a name split by a comma ("Muster, Erika") goes as well.

Into an empty, migrated database (`./scripts/dev-up.sh` creates and migrates `unternehmensdatenbank`):

```bash
python scripts/prepare_offeneregister_import.py openregister.db berlin.csv
docker compose exec -T postgres psql -U app -d unternehmensdatenbank -c "\copy public.companies \
  (company_name, normalized_name, registration_number, legal_form, business_purpose, wz_code, address, \
  established_at, postcode, city, register_status, metadata) FROM STDIN WITH (FORMAT csv, HEADER true)" < berlin.csv
```

Everything else the dump knows goes into `company_source_records`, one row per company under the
source `offeneregister`: register court, type and number, seat, status, the seven document flags,
former names (32,170 companies), register moves (25,584), officers (348,904) and the raw address.
`company_source_records` is not exposed through Hasura. The same script fills `companies.city` from
the registered seat where the address gave none (179,959 rows). Run it after the CSV load; safe to
re-run.

```bash
python scripts/import_offeneregister_records.py openregister.db
```

Then the management names: each current Geschäftsführer (name and position only) becomes a
`ceo_name_or_contact` observation, published by the next `pipeline_coordinator` run. Safe to re-run.

```bash
python scripts/import_register_managers.py
```

## Migration strategy

`V1__baseline.sql` is the whole schema as of 2026-10-28; everything before it was squashed while
nothing was deployed. `V2__source_overview.sql` came after. Forward-only from here: never edit a
migration that has run anywhere shared, add new ones as `V{N}__description.sql`, and run Flyway
before Airflow touches the changed schema. The headings below keep the pre-squash migration numbers
(V3–V10) so git history finds the reasoning.

**A local database migrated before the squash** still has the old files in its Flyway history, and
Flyway refuses to run until that is reset. Only for a database with every migration up to V10
applied (`flyway info`); one without V10 lacks the two `register_announcement` columns, so build it
fresh:

```bash
docker compose exec -T postgres psql -U app -d unternehmensdatenbank -c "DROP TABLE flyway_schema_history"
docker compose --profile tools run --rm flyway baseline -baselineVersion=1 -baselineDescription=baseline
```

Working queries: [core-queries-current.sql](core-queries-current.sql).

## Identifiers and matching (V2 before the squash)

How a record from any source finds its company, in this order:

1. **A shared identifier.** `company_identifiers` (`register`, `register_former`, `opencorporates`,
   `fusionbase`, `vat`, `lei`); `(scheme, value)` is unique, so one identifier never points at two
   companies. The OffeneRegister import writes register and OpenCorporates numbers for all 296,486.
2. **A fuzzy score** on name, postcode, street, domain and phone. High: link. Middle: a
   `match_candidates` row for a person. Low: a new company.
3. Every link records `match_method`, `match_confidence` and `matched_at` on its source record, so a
   wrong one can be found and undone.

Merging never deletes: the duplicate keeps `merged_into`, and its records and observations move to
the survivor.

## Feedback and provenance (V3)

**Every field the app shows can be voted on** (`extraction_feedback`), under the app's column keys.
A vote on an empty field has `value = ''` and must be `wrong`, with the missing value as its
correction. Only the website, Technologiefeld and manager pipelines learn from votes so far.

**`v_field_provenance` says where each shown value came from**: source, method, confidence,
evidence, a link and dates. Register facts carry the date the register was read.

**Every manager name keeps its provenance** (`company_managers.company_field_observation_id`,
`source_id`). The coordinator writes every name above the bar and deletes a name whose observation
is gone. The evidence is `["register_officer"]` when the name is one of the company's register
officers, `[]` otherwise. On 2026-10-28: 156,846 register names and 29 spaCy names published, for
111,374 companies.

## Grant links (V4, V6)

Which company a grant belongs to is stored in `company_funding_links`, refreshed by
`refresh_company_funding_links()` after grants or companies load. Linking on every read made a list
page take 71 s; now 0.65 s.

- A grant naming a register entry is linked **by that entry only** (`match_method =
  'register_number'`); one matching none of our companies stays unlinked.
- Otherwise by normalised name and city, when exactly one company matches.

`company_fundings` has two sources (`source`): `foerderkatalog` and `berlin_zuwendungen`. Berlin
grants keep `recipient_register` and the policy area; only grants to an HRA/HRB entry are loaded
(22,354 of 55,454). Measured 2026-10-28: 18,427 Berlin grants linked to 3,861 companies, 3,927
unlinked (most likely companies registered after the snapshot; not checked).

## The stored company profile (V5)

`v_company_profile`, which the app reads, is a stored copy (`company_profile_mv`) of
`v_company_profile_live`, plus `source_keys`. Filtering the live view took up to 45 s; now under
10 ms. `refresh_company_profile()` rebuilds it (about 3 s) and is called by the coordinator after
every run and by the grant DAGs and imports after they load.

**A new profile column** means dropping and rebuilding `company_profile_mv` and `v_company_profile`
after adding it at the end of `v_company_profile_live`: the stored copy froze its columns when it was
created. The activity-keywords migration (V8 in git history) shows how.

## Website contacts and keywords (V8)

Two DAGs read more from a website `website_discovery` has proven to be the company's.

- **`website_contacts`** (source `impressum_contacts`) writes `email` and `phone` observations from
  the Impressum. An email counts only at the website's own domain and only when its mailbox is not
  shaped like a person. First 37 websites: 27 emails, all role mailboxes, and 25 phones. **Nothing
  resolves them**, so they reach neither `v_company_profile` nor `v_field_provenance`.
- **`website_keywords`** (source `website_keywords`) stores keyword-shaped headings of the services
  and products pages and the meta keywords; a second task measures which terms are generic and writes
  one `activity_keywords` observation per company. Nothing is published before 500 companies are
  read. A company whose published website changed loses its keywords.

Both read each company once. A failed read is stored with `failed: true` and an `attempts` count and
retried on later runs, 3 times at most; it counts for nothing, including the 500.

## Register announcements (V10)

`register_announcements` reads Berlin's register announcements from handelsregister.de weekly. The
court keeps each up for 8 weeks, so a weekly run misses none. It reads only the result list
(category, register number, name, city, date), never a detail page.

- **Every announcement is a source record** (source `registerbekanntmachungen`), linked by register
  number through `company_identifiers` (`match_method = 'identifier'`).
- **The name comes second, before creating.** An unknown register number can be a company we have
  under an old number (HRA to HRB gets a new one). Exactly one company with the same
  `normalized_name` and city that is not `erloschen`: it is the company (`match_method = 'fuzzy'`,
  confidence 0.9) and gets the new number as an identifier. Several live ones, or only deleted ones:
  the company is created and each candidate becomes a `match_candidates` row. A deleted company is
  never linked by name, since its name may be reused. Without this rule, 14 of 425 created companies
  duplicated one we had and 66 grant links would have been lost.
- **A company we do not have is created** (`match_method = 'created'`) with name, legal form, city,
  register number and identifier, using `enrichment.register_names` like the OffeneRegister import.
  Measured 2026-10-28: 900 announcements, 451 matched, 425 companies created.
- **The profile shows the latest one as a hint** (`register_announcement`,
  `register_announcement_date`): latest per company at confidence 1.0, older ones 0. It never changes
  `register_status`: 147 companies shown `aktiv` had a deletion announced.

## Source overview (V2)

What each source has collected, for the app's `/sources` page. **Counts only, never a value**, so
`email` and `phone` can be counted (stored, but 0 shown).

- `v_source_overview`: per source, records, companies covered (records and linked grants), companies
  created, and the latest run read live from `source_sync_runs`. `runs_interrupted` counts runs left
  `running` although a newer one started (a restarted worker leaves its row behind).
- `v_source_fields`: per source and field, values stored, companies, and `companies_shown`.
  Grants count as the field `fundings`.
- The counts are stored (`source_stats_mv`, `source_field_stats_mv`) and refreshed by
  `refresh_company_profile()`. Hasura exposes both read-only (`source_overview` with `fields`).
