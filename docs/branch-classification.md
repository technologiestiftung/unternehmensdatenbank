# Two columns: Technologiefeld and WZ cluster

A company gets two industry columns. A third, `purpose_field`, comes from the register text and has
[its own document](purpose-classification.md).

> The numbers below were measured on the Osnabrück example data (14,443 companies, August–September
> 2026), which has been removed. The rules still hold. OffeneRegister has no WZ code, so for Berlin
> only the Technologiefeld can be filled today.

**They are two columns because they answer two different questions.** Answering one with the other
was the most expensive mistake made here.

| | Technologiefeld | WZ cluster |
|---|---|---|
| question | which field does the company **work in**? | what is it **registered as**? |
| decided by | Berlin's list of 77 fields | the Statistisches Bundesamt |
| filled from | words the company published about itself | its official WZ code |
| values | one of the 77, or `NONE` | 76 divisions in 20 sections |
| needs first | a website somebody downloaded | only the WZ code |
| judgement involved | yes: keyword weights, thresholds | none, it is a lookup |

Neither is a check on the other. A company registered as real estate whose Technologiefeld is `NONE`
is correctly described by both.

## Words

- **Technologiefeld**: Berlin's column *Branche / Technologie- und Anwendungsfeld*. 77 values, like
  "Bahnindustrie" or "Gastgewerbe".
- **Keyword**: a word that suggests a field. "Schienenfahrzeuge" suggests Bahnindustrie. 500 remain.
- **WZ code**: the official German industry code. `56.11.1` is "Restaurants mit Bedienung". Our codes
  are **WZ 2025**, not WZ 2008; checking against a 2008 table mismatches silently.
- **Division**: the first two digits of a WZ code (`56 Gastronomie`). **Section**: the letter above it
  (`I GASTGEWERBE`).

| what | where |
|---|---|
| the 77 fields and their keywords | `airflow/dags/enrichment/branch_taxonomy.yaml` |
| every WZ code and its name | `airflow/dags/enrichment/wz_titles.yaml` (2,008 codes; rebuilt by `scripts/import_wz_titles.py`) |
| keyword matcher | `airflow/dags/enrichment/branch_classifier.py` |
| WZ lookup | `airflow/dags/enrichment/wz_cluster.py` |
| fetching services/about pages | `airflow/dags/enrichment/content_pages.py` |
| writing both columns to Postgres | `airflow/dags/enrichment/branch_observations.py` |
| the guard | `scripts/check_branch_classifier.py` |

The DAG `technology_field` runs it (#62): it reads the homepage and services pages of every company with a website, then weighs the keywords over all companies read and classifies them all again.

## The rule: a WZ code never fills the Technologiefeld

An earlier version had one column and mapped WZ codes into it (`56.11.1 → Gastgewerbe`). Coverage went
from about 950 to 3,118, and 2,632 of those were the WZ code speaking in the Technologiefeld's voice.
Nothing the company published said so, and the WZ detail was thrown away.

The fix is structural: `branch_classifier.classify()` **has no WZ parameter**, and the guard fails if
one reappears.

## WZ cluster

`wz_cluster.cluster_for(code)`: the division is `code[:2]`, the section is the letter that owns it in
`wz_titles.yaml`. No mapping, no threshold, nothing to label.

## Technologiefeld

### 1. Read the right pages

The homepage and the Impressum rarely say what a company does. The real text is on `/leistungen`,
`/ueber-uns`, `/produkte`. **Follow the homepage's links, never guess paths**: 84% of homepages link to
such a page, and `/leistungen` 404s on a site that calls it `/de/unternehmen/leistungen`. Every page
that comes back is read; capped at 2 per category and 4 per company.

**A Technologiefeld requires a website somebody downloaded**, and the gate asks Postgres ("did anybody
fetch and check a page for this company"), not a file. Gating on unchecked canonical websites would
release fields from the register text alone, which is `purpose_field`'s job.

### 2. Count keywords, weighted

- **Never match inside a German compound.** A keyword needs a word boundary on both sides. Allowing
  compounds made `sicherheit` fire inside *Arbeitssicherheit* and made Defence Tech the most common
  industry in Osnabrück, while clear winners *fell* from 338 to 316.
- **Hyphenated compounds still match** (`Kfz-Zulassung` fires `zulassung`). Treating `-` as a boundary
  would also stop *E-Commerce-Lösungen* matching `e-commerce`. It is behind
  `BRANCH_HYPHEN_IS_BOUNDARY`, **off**, until measured both ways.
- **Weights are measured.** Rarer words count more: `log(companies / (1 + df))`, counted per company,
  not per page. A word claimed by several fields is split between them. Multi-word keywords score
  higher. Thresholds: `MIN_SCORE` 2.0, `MIN_MARGIN` 1.5.

### 3. Keywords decide; the text classifier only fills gaps

`branch_textcat.py` (spaCy) may fill a `no_keyword` row when it is at least `TEXTCAT_MIN_SCORE` sure
(`decided_by = "textcat"`). The weekly DAG `technology_field_train` trains it on the keywords' answers
and the app's votes, with the text from the company corpus. It never overrides keywords, and it is promoted only if it beats the
keywords on companies people have voted on in the app.

### `NONE` has four reasons, never added together

| reason | meaning |
|---|---|
| `no_website` | the text named a field, but no downloaded website backs it (kept in `withheld_branch`, stored as `technology_field_withheld`, never shown) |
| `torn` | keywords fired, two fields too close to separate |
| `no_keyword` | we read text, no keyword appeared |
| `no_text` | nothing to read |

"We had no text" is not evidence that a company has no field. All four print `NONE` and keep their
reason in `outcome`. Expect `NONE` to be the common case: Berlin's list describes a
technology economy, and most companies are not in it. Never add a fifth reason.

### Keywords: delete in the YAML, never a stop-list in code

Document frequency cannot tell `einzelhandel` (an industry) from `sicherheit` (a quality). `sicherheit`
alone produced 27 of Defence Tech's 35 assignments: a bed shop, a lift company, an estate agent. It was
deleted from `branch_taxonomy.yaml` with that count in a comment. 27 keywords have been deleted this
way.

1. **No stop-list in code.** Commonness is already handled by the weights.
2. **Delete a keyword that is wrong for its field**, in the YAML, with what it cost in a comment. The
   test is whether the word names an industry or an activity everybody does.

A field carried mostly by one keyword is a warning, not a verdict: `windenergieanlagen` carries 100% of
Windenergie and is right.

## Not known yet

**No row of either column is hand-checked.** The WZ column needs none, being a lookup. For the
Technologiefeld, agreement is not correctness, and there is no precision figure.
