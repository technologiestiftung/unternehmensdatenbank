# Target columns

The goal is one record per Berlin company with the 20 columns below: which column of
`v_company_profile` holds each, and who fills it.

- **Fusionbase**: the data vendor (offer of 2026-10-12). Delivers **only active** companies from the
  Berlin registers (HRA, HRB, GnR, PR, VR, GsR), **quarterly**, by API. Its WZ code and activity
  description come from **its own AI model**, so they are estimates like ours. No import yet.
- **Us**: the DAGs and scripts in this repo.

"Today" counts are from the local database on 2026-10-29: 296,897 companies (296,486 from
OffeneRegister, the rest created by `register_announcements`). "Filled" says nothing about
correctness: no row of any classification column has been hand-checked.

| # | Column | `v_company_profile` | Fusionbase? | Our part | Today |
|---|---|---|---|---|---|
| 1 | Name | `name` | yes | — | 296,897 |
| 2 | Mitarbeiter | `employee_count` | yes, if published in the Bundesanzeiger | blocked, see below | 0: resolved, but no source writes it |
| 3 | Umsatz | `revenue` | yes, if the company must publish | blocked, see below | 0: same |
| 4 | Förderungen | `funding_count`, `funding_total_eur`, `fundings` | **no** | `foerderkatalog` (by name and city), `berlin_zuwendungen` (by register number) | 5,189 companies with a grant |
| 5 | WZ-Nummer | `wz_code` | yes, **AI-derived** | cross-check against the register | 0: OffeneRegister has none |
| 6 | Gründungsdatum | `established_at` | yes | — | 0 |
| 7 | Adresse | `address`, `postcode`, `city` | yes | — | city 296,897; street and postcode 116,527 |
| 8 | Rechtsform | `legal_form` | yes | — | 257,244 |
| 9 | Cluster (n=5) | — | **no** | column removed with #47 (2026-10-29): it held LLM free text, not Berlin's five clusters. Comes back as a new column with a classifier | — |
| 10 | Technologiefeld | `technology_field` (English names, `name_en`) | **no** | `branch_classifier` ([branch-classification](branch-classification.md)) | 27 |
| 11 | Website | `website` | yes | `website_discovery` verifies it | 4,929 |
| 12 | Geschäftsführung / Kontakt | `management` | contact optional | Geschäftsführer from the register; `website_contacts` reads email and phone from the Impressum | names 111,374. Email and phone stored, not shown (27 emails, 25 phones from 37 websites, 2026-10-28) |
| 13 | Tätigkeitsbeschreibung | `business_purpose`, `activity_keywords` | yes, **AI-written** | register purpose; `website_keywords` | purpose 0 (OffeneRegister has none); keywords publish from 500 companies read |
| 14 | Kommentar | `comments` | no | users in the app | — |
| 15 | DeepTech Klassifikation | `deeptech_class` | **no** | needs a classifier; corrections from the app (`feedback_observations`, source `user_feedback`) can already fill it | NULL |
| 16 | Konfidenz der Klassifikation | `deeptech_confidence` | **no** | the DeepTech observation's confidence | NULL |
| 17 | isAI | `is_ai` | **no** | needs a classifier; corrections from the app (`feedback_observations`, source `user_feedback`) can already fill it | NULL |
| 18 | timestamp | `updated_at` | quarterly refresh date | — | yes |
| 19 | Amazon-Effekt *(dashed)* | — | no | column removed with #47; meaning not defined yet | — |
| 20 | Schließung / Liquidation *(dashed)* | `register_status`, `register_announcement` | **no**, only active companies | `register_announcements`: the latest announcement, a hint next to the status | status 296,486 (from the snapshot); announcements 854 |

## Fusionbase's sources: which can we read ourselves

| Source | Columns | Can we? |
|---|---|---|
| The company website | 11, 12, 13 keywords | **built**: `website_discovery`, `website_contacts`, `website_keywords` |
| Fusionbase's AI on website + register purpose | 5, 13 description | from the website only (OffeneRegister has no purpose); nothing hand-checked to measure against |
| Registerbekanntmachungen (handelsregister.de) | 20, and companies registered after 2019 | **built**: `register_announcements`, weekly, 2 requests, list only. About 900 Berlin announcements in 8 weeks. New registrations and ordinary changes have not been announced since 2022 (§ 10 HGB, to be checked) |
| Handelsregister (handelsregister.de) | 13 Gegenstand, branch or head office | **not built**: no API; the terms reportedly allow 60 searches per hour, about 100 days for the 142,571 active companies |
| Annual accounts (Bundesanzeiger / Unternehmensregister) | 2–3, with history | **not built**: no bulk access, a CAPTCHA in front of the documents (not solved automatically), and small companies need not publish revenue (§ 326 HGB, to be checked). Whether accounts since 2022 sit in the Unternehmensregister (DiRUG) is to be checked |

## What follows

- **Fusionbase covers 1–3, 5–8, 11–13 and 18.** 5 and 13 are AI output.
- **Ours alone: 4, 9, 10, 14–17, 19, 20.** The classification columns (9, 10, 15–17) are the open
  work, and none has a precision figure.
- **Fusionbase drops dissolved companies**, so 20 comes from comparing quarterly deliveries or from
  the register.

## Filling a new column

`deeptech_class` and `is_ai` are read like every enriched field: the canonical
observation for that `field_name`. A DAG that fills one writes through `insert_text_observation`,
after **widening the upsert key** in a new migration **and** the `ON CONFLICT ... WHERE` in
`source_enrichment_common.py`, in the same commit.
