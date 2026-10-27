# Target columns — what each one is, where it comes from, and who fills it

The project's goal is one record per Berlin company with the 20 columns below. This page says,
for each one, which column of `public.v_company_profile` (Flyway V22) holds it and who is
expected to fill it.

Two suppliers:

- **Fusionbase** — the data vendor in the offer of 2026-10-12. It delivers **only active** companies
  from the Berlin registers (HRA, HRB, GnR, PR, VR, GsR), refreshed **once per quarter**, through an
  API. Its WZ code and its activity description are produced **by Fusionbase's own AI model**, not
  copied from a register, so they are a vendor's estimate, like ours. No import exists yet.
- **Us** — Airflow DAGs and scripts in this repo.

State on 2026-10-27. "Filled" means a code path writes it today; it says nothing about whether the
values are right — not one row of any classification column has been hand-checked.

| # | Target column | `v_company_profile` column | Fusionbase delivers it? | Our part | Filled today |
|---|---|---|---|---|---|
| 1 | Name | `name` | yes, required (register name) | — | yes (Northdata import) |
| 2 | Mitarbeiter | `employee_count` | yes, if published in the Bundesanzeiger; earlier years optional | fill gaps | the coordinator resolves it, but no source writes a number; `berlin_gewerbedaten` writes only a range (`employee_count_range`) |
| 3 | Umsatz | `revenue` | yes, if the company must publish; earlier years optional | fill gaps | the coordinator resolves it, but no source writes it |
| 4 | Förderungen | `funding_count`, `funding_total_eur`, and the `fundings` relation | **no** | `foerderkatalog` DAG (Förderkatalog des Bundes). Land Berlin's own grants (daten.berlin.de Recherchetool) are **not** in yet | yes: 19,533 grants, 3,003 recipients, measured 2026-10-27 |
| 5 | WZ-Nummer | `wz_code` | yes, required — **AI-derived** | cross-check against the register | yes (Northdata import) |
| 6 | Gründungsdatum | `established_at` | yes, required | — | yes (Northdata import) |
| 7 | Adresse | `address`, `postcode`, `city` | yes, required | — | yes |
| 8 | Rechtsform | `legal_form` | yes, required | — | yes |
| 9 | Cluster (n=5) | `cluster` | **no** | today `cluster` is free text from the search providers, **not** Berlin's five clusters. Needs a new classifier | no |
| 10 | Technologiefeld | `technologiefeld` | **no** | `branch_classifier` (docs/branch-classification.md) | yes, 403 of 14,443 Osnabrück rows |
| 11 | Website | `website` | yes, required | discovery pipelines P1–P4 verify it | yes |
| 12 | Geschäftsführung / Kontakt | `management` | contact (name, email, phone) optional | `company_managers` from the register | yes (names only) |
| 13 | Tätigkeitsbeschreibung | `business_purpose` | yes, required — **AI-written**, plus keywords | register purpose (Gegenstand) | yes (register purpose) |
| 14 | Kommentar | `comments` relation → `company_comments` | no | written by users in the app, through Hasura | table exists, empty |
| 15 | DeepTech Klassifikation | `deeptech_class` | **no** | needs a classifier | reserved, NULL |
| 16 | Konfidenz der Klassifikation | `deeptech_confidence` | **no** | the confidence of the DeepTech observation itself | reserved, NULL |
| 17 | isAI | `is_ai` | **no** | needs a classifier | reserved, NULL |
| 18 | timestamp | `updated_at` | quarterly refresh date | — | yes |
| 19 | Amazon-Effekt *(dashed)* | `amazon_effect` | no | **meaning not defined yet** — reserved so the column exists | reserved, NULL |
| 20 | Schließung / Liquidation *(dashed)* | `register_status` | **no** — Fusionbase only lists active companies | a company that drops out of a quarterly delivery is a candidate; the register decides | yes (Northdata status) |

## What follows from the table

- **Fusionbase covers 1–3, 5–8, 11–13 and 18.** Of those, 5 and 13 are AI output and need the same
  scepticism as ours.
- **Ours alone: 4, 9, 10, 14–17, 19, 20.** The classification columns (9, 10, 15–17) are where the
  remaining work is, and none of them has a precision figure.
- **Fusionbase drops dissolved companies.** So column 20 has to come from comparing two quarterly
  deliveries, or from the register — not from the vendor.

## How a new column gets a value

`deeptech_class`, `is_ai` and `amazon_effect` are read the same way as every other enriched field:
the canonical observation for that `field_name`. A DAG that wants to fill one must write an
observation through `insert_text_observation`, and before that, **widen the upsert key** in a new
migration **and** the `ON CONFLICT ... WHERE` predicate in `source_enrichment_common.py` in the same
commit (see CLAUDE.md, "Migrations").
