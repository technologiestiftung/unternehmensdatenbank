# Data Model

## In one sentence

`companies` is who a company is, `company_field_observations` is everything said about it.

Several sources talk about the same company and may disagree. We lose none of it:

| Table | Holds |
|---|---|
| `companies` | identity only: a random UUID, the register number, `merged_into` |
| `company_source_records` | the whole record one source delivered, one row per `(source_id, external_id)` |
| `company_field_observations` | one claim per field: company, source, field name, value, confidence |
| `company_canonical_field_sources` | which observation won each field, and when it was chosen |
| `sources` | every source, with its `priority` (lower wins) |
| `source_sync_runs` | one row per run: status (`pending` … `cancelled`) and counters seen/created/updated/failed |
| `company_identifiers` | every identifier a source uses for a company |
| `company_managers` | the names shown as Geschäftsführung, each linked to its observation |
| `pipeline_state` | what a DAG has done so far, one row per (pipeline, key) |

Only one observation per `(company_id, field_name)` may be canonical. Only `pipeline_coordinator`
decides which one: sources write observations and trigger it. **Every shown value is an
observation**, the register facts included, and is read through `v_companies_enriched`.

### Register fields are observations (V3)

Until V3, name, legal form, address, postcode, city, register status, founding date, purpose and WZ
code were columns of `companies`, written once by the import. Nothing could compete with them: not a
second register, not a user's correction. Now each is an observation of the source that delivered it
(`resolver.REGISTER_FIELDS`; the name's field is `name`), and the coordinator publishes the winner
like any other field. `v_companies_enriched` shows the winners under the old column names
(`company_name`, `normalized_name`, `legal_form`, `address`, `postcode`, `city`, `register_status`,
…), so readers did not change.

What it cost, measured on the Berlin set (296,897 companies) on 2026-10-29: the migration took
5 min 15 s; a first import into an empty database 30 min (publishing 1,379,357 values, each one
new); a discovery batch query 8.6 s instead of 0.3 s; `refresh_company_funding_links()`
8.3 s instead of 2.8 s; the nine register fields 22 s of each coordinator run. The app reads the
stored profile and did not get slower. Every shown value stayed the same (checksum over all
profiles, and a fresh import gave the same values as the migrated database), and grant links went from 26,236 to 26,241: names with letters like é now get the same
key as grant recipients (`normalize_company_name` in SQL, one key everywhere).

`companies` keeps: `id`, `registration_number`, `merged_into`, `metadata`, `data_last_updated_at`,
`created_at`, `updated_at`. (`data_quality_score` and `canonical_updated_at` were removed with #47.)

### `register_status`

Whether the register still lists the company: 142,571 `active` and 153,915 `dissolved` in the Berlin
set. Stored in English since V5 (#47): the register's aktiv, erloschen and in Liquidation are
`active`, `dissolved` and `in_liquidation` (`enrichment.register_names`). `DISCOVERY_ACTIVE_ONLY=true`
restricts discovery to `register_status = 'active'`; `in_liquidation` counts as inactive. With a limit, the limit applies after the filter, so compare a
filtered run only with another filtered run. A website found for a `dissolved` company deserves
extra suspicion: dissolved companies' domains get sold.

## Loading the OffeneRegister Berlin set

**OffeneRegister** is a snapshot of the German Handelsregister, retrieved between June 2017 and
January 2019: 6–9 years old, so newer companies are missing and "active" means active in 2018.

| | |
|---|---|
| companies (register court Berlin-Charlottenburg) | **296,486** |
| `active` / `dissolved` | 142,571 / 153,915 |
| with an address | 116,527 |
| with a legal form (read from the end of the name) | 256,845 |
| WZ code, purpose, founding date, website | **none**: the dump does not have them |

The address is cut after postcode and city, because some rows continue with register text naming
managers and their birth dates. A part naming who receives the post (`c/o`, `bei`, `z. Hd.`,
`zu Händen`) is dropped too: 10,423 of the 116,527 addresses had one. Before the postcode only parts
with a digit are kept, so a name split by a comma ("Muster, Erika") goes as well. Where the address
has no city, the registered seat is the city (179,959 rows).

One script, into a migrated database (`./scripts/dev-up.sh` creates and migrates
`unternehmensdatenbank`). It creates each company (identity only) or finds it by register number,
keeps everything the dump knows in `company_source_records` under the source `offeneregister`
(register court, type and number, seat, status, the seven document flags, former names (32,170
companies), register moves (25,584), officers (348,904) and the raw address; not exposed through
Hasura), writes the register fields as observations and publishes them. Safe to re-run.

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
nothing was deployed. `V2__source_overview.sql`, `V3__register_fields_as_observations.sql`,
`V4__user_feedback_source.sql` and `V5__profile_cleanup_tags_english.sql` came after. Forward-only from here: never edit a migration that has
run anywhere shared, add new ones as `V{N}__description.sql`, and run Flyway before Airflow
touches the changed schema.
The headings below keep the pre-squash migration numbers (V3–V10) so git history finds the reasoning.

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

## Age: older values weigh less (V6, #49)

Every observation knows when its **source** read the value: `source_updated_at`, else
`observed_at`. The register read Berlin between 2017-06-12 and 2019-01-31; a website is read the day
a DAG downloads it; a correction is dated by the newest vote behind it.

- **Decay only ranks.** Within one priority the coordinator compares `confidence × 0.5 ^ (age /
  half-life)`. The bar still filters on the stored confidence, so an old value with nothing newer
  stays shown, and the ⓘ (`v_field_provenance.observed_at`) says when the source read it.
- **Half-lives** (`resolver.HALF_LIVES`, decided 2026-10-29, starting values to be measured once
  Fusionbase overlaps with OffeneRegister): website 3 years; address, postcode and city 5;
  register_status 2; name and legal_form 10. Fields not listed do not decay; the founding date never.
  Manager names are a list, every name above the bar is shown, so age does not drop one.
- **Corrections give way to newer sources.** A correction from the app (priority 50) stands until
  another source has a qualifying value it read after the latest vote behind the correction; then
  that source's value is shown. An older source value never beats a newer correction.

Tested on a copy of the Berlin database on 2026-10-29: a new address read that day at 0.8 replaced
the register's 2018 address at 1.0 (worth about 0.3 after decay); a source name read a day before
two people's correction did not replace it, one read after it did.

## Tags (V5, #47)

People tag companies in the app: `tags` (free text, unique ignoring case and surrounding spaces) and
`company_tags` (which company, which tag, who added it, when). Written through Hasura only; a person
removes only tags they added, and renaming or deleting a tag waits for an admin role. The app reads
them live through the profile's `tags` relationship, not from the stored profile, so a tag shows at
once. Tags are not a source field: no vote, no observation, no coordinator.

## Feedback and provenance (V3 before the squash)

**Every field the app shows can be voted on** (`extraction_feedback`), under the app's column keys.
A vote on an empty field has `value = ''` and must be `wrong`, with the missing value as its
correction.

**Corrections are a source** (`user_feedback`, priority 50, V4; DAG `feedback_observations`). Per
company, field and corrected value, the people who typed it or voted that value `correct` count
for it; the people who voted another value `correct`, or this one `wrong` (with or without a
correction of their own), count against it. Nobody counts both ways: only a person's latest vote
on a field counts. On the list fields (managers, keywords) that is per shown value, and confirming
one value counts against no other. One person net gives confidence 0.30, below every bar (0.50,
websites 0.35), so nothing shows; two give 0.60 and three or more 0.75, which the coordinator
publishes over any other source, the register included. **A field no other source fills takes the
first correction**: one person then gives 0.60. Asked on every run, so when a source delivers a
value later, a single person's correction steps back to 0.30 and the source's value is shown.
Priority 50 is deliberate: register values carry 1.0 and verified websites up to 0.99, so at
equal priority a correction could never replace a wrong name. Nothing is deleted: a withdrawn
correction drops to 0 and the source's value is shown again. Each correction is checked first (a
URL, one of the 77 fields, a number, a date); `fundings`, `register_announcement` and
`registration_number` take none. Only counts are stored, never who.

Tested on a copy of the Berlin database on 2026-10-29: two people correcting a name replaced the
register's name; one person correcting an address or website changed nothing; two corrections
against one `correct` vote changed nothing; withdrawing one of two brought the register's name back.

Two models also learn from votes: `manager_ner_train` (names) and the Technologiefeld classifier.
Each reads the votes itself, since a vote teaches each model something different. The website
discovery does not learn from votes.

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
  name key (`normalize_company_name`) and city that is not `dissolved`: it is the company (`match_method = 'fuzzy'`,
  confidence 0.9) and gets the new number as an identifier. Several live ones, or only deleted ones:
  the company is created and each candidate becomes a `match_candidates` row. A deleted company is
  never linked by name, since its name may be reused. Without this rule, 14 of 425 created companies
  duplicated one we had and 66 grant links would have been lost.
- **A company we do not have is created** (`match_method = 'created'`): its register number and
  identifier, and name, legal form and city as observations of this source, the legal form read by
  `enrichment.register_names` like the OffeneRegister import.
  Measured 2026-10-28: 900 announcements, 451 matched, 425 companies created.
- **The profile shows the latest one as a hint** (`register_announcement`,
  `register_announcement_date`): latest per company at confidence 1.0, older ones 0. It never changes
  `register_status`: 147 companies shown `active` had a deletion announced.

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
