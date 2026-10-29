# Guards and tests

**Guards** (`scripts/check_*.py`) check the **rules**: does the code still refuse a directory page,
does a keyword still fail to match inside a German compound. **Unit tests** (`tests/unit/`) check
that a function does what it says. Both are free: no API, no model, no database, no network; about
15 seconds together.

Neither checks whether the answers are *correct*: not one row of the industry columns has been
hand-labelled.

## One list, four callers

`scripts/free_checks.sh` runs ruff, the unit tests and the seven guards. **Add a check there, never
to a caller**, or the callers drift apart.

| caller | how |
|---|---|
| CI (`.github/workflows/checks.yml`) | every pull request and every push to `main` |
| no-mistakes (`.no-mistakes.yaml`) | its test step, in a `python:3.12-slim` container |
| commit hook (`run_checks_before_commit.py`) | the whole script, before Claude commits |
| edit hook (`guard_after_edit.py`) | ruff on the edited file, then the guard lines |

## The seven guards

| guard | defends |
|---|---|
| `check_branch_classifier.py` | the compound rule and hyphen knob, keyword weights, WZ lookup, services-page choice |
| `check_purpose_branch.py` | disjoint outcomes, the website gate, no WZ code or register status, the 40-character floor, `DISCOVERY_ACTIVE_ONLY` as a constant SQL fragment |
| `check_branch_observations.py` | every field the emitter writes is covered by the upsert index predicate *and* `insert_text_observation`'s `ON CONFLICT`; no `resolve_*` names a `_withheld` field |
| `check_website_discovery.py` | URL guessing, the confidence ladder, `_page_evidence`, JSON-LD, postcodes, register numbers |
| `check_own_site_filter.py` | real URLs: directories, registers and portals are never a company's own site |
| `check_imprint_owner_extraction.py` | real Impressum layouts guarding the Geschäftsführer regex |
| `check_osm_match.py` | hand-checked OpenStreetMap matches guarding `osm_snapshot` |

**Two rules for a guard**, both learned the hard way:

1. **Import the rule you check, never restate it.** A check that rebuilds the logic agrees with
   itself no matter what the real code does.
2. **Assert on the syntax tree, not the source text**, when the forbidden thing is discussed in a
   docstring. A plain `in` test once found `Handel` inside `Handelsregister`. Use `_pattern_for`,
   the project's own compound rule.

## The hooks

In `.claude/hooks/`, wired from `.claude/settings.json` (`settings.local.json` stays out of git).

- **`guard_after_edit.py`** (after Write/Edit): ruff on an edited `.py`, then the guards when the
  file is under `airflow/dags/`, `scripts/` or `tests/`. A listed guard that is missing blocks the
  edit. Timeout 60 s.
- **`run_checks_before_commit.py`** (before `git commit`): ruff on staged files, then
  `free_checks.sh`. A failure blocks the commit.
- **`ask_before_push.py`, `confirm_before_commit.py`**: every `git push` and `git commit` from Claude
  becomes a prompt showing branch, remote and staged diff. A failing check blocks before the prompt.
- **`ask_before_new_file.py`**: creating a file that does not exist yet becomes a prompt; editing an
  existing one does not.
- **`check_commit_message.py`**: a commit message body of at most 15 non-blank lines.
