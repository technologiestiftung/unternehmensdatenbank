# Guards and tests — what checks what, and what each one is defending

This project has two kinds of automated check, and they are easy to confuse.

**Guards** are scripts in `scripts/check_*.py`. They check the **rules**: does the code still refuse
a directory page, does a keyword still fail to match inside a German compound word. **Unit tests**
are pytest modules in `tests/unit/`. They check that a function does what it says.

Both are free: no API call, no language model, no database, no network. All of them together take
about 15 seconds.

Neither kind checks whether the answers are *correct*. Nothing in this repo does, because **not one
row of the industry columns has ever been hand-labelled.**

## One list, four callers

`scripts/free_checks.sh` runs ruff, the unit tests and the seven guards. It is the only list:

| caller | how |
|---|---|
| CI (`.github/workflows/checks.yml`) | on every pull request and every push to `main` |
| no-mistakes (`.no-mistakes.yaml`) | its test step, in a `python:3.12-slim` container |
| the commit hook (`.claude/hooks/run_checks_before_commit.py`) | the whole script, before Claude commits |
| the edit hook (`.claude/hooks/guard_after_edit.py`) | ruff on the edited file, then the guard lines of the script |

**Add a check to `free_checks.sh`, never to one of its callers**, or the callers drift apart.

Until 2026-10-28 two more test modules pinned the numbers of the Osnabrück example data and needed
a running Postgres. They were removed with that data.

## The seven guards

| guard | what it defends |
|---|---|
| `check_branch_classifier.py` | the compound rule and the hyphen knob, keyword weights, the WZ lookup, the services-page choice |
| `check_purpose_branch.py` | column 3: outcomes stay disjoint, the website gate, WZ code and register status cannot get in, the 40-character floor, `DISCOVERY_ACTIVE_ONLY` is a constant SQL fragment |
| `check_branch_observations.py` | the classification write path: every field the emitter writes is covered by V20's index predicate *and* `insert_text_observation`'s `ON CONFLICT`; no `resolve_*` names a `_withheld` field |
| `check_website_discovery.py` | URL guessing, the confidence ladder, what `_page_evidence` takes from a page, JSON-LD, postcodes, register numbers |
| `check_own_site_filter.py` | real URLs guarding `own_site.is_own_site_candidate` — directories, registers and portals are never a company's own site |
| `check_imprint_owner_extraction.py` | real Impressum layouts guarding the Geschäftsführer regex |
| `check_osm_match.py` | hand-checked OpenStreetMap matches guarding `osm_snapshot` |

### Two rules a guard here must follow

Both were learned by writing a guard that did not.

1. **Import the rule you are checking. Never restate it.** A check that rebuilds the logic agrees
   with itself no matter what the real code does.
2. **Assert on the syntax tree, not the source text**, when the thing you are forbidding is
   *discussed* in a docstring. `check_purpose_branch.py` proves `purpose_branch` never calls
   `classify()` by walking the parsed AST, because the module docstring explains the website gate at
   length and a substring search finds the prose. The same trap caught a check for "no Berlin field
   name in the prompt": a plain `in` test found **`Handel` inside `Handelsregister`**. Use
   `_pattern_for`, which is the project's own compound rule.

## The hooks

They live in `.claude/hooks/` and are wired from `.claude/settings.json`; `settings.local.json`
(personal permissions) stays out of git.

- **`guard_after_edit.py`** (`PostToolUse` on Write|Edit): ruff on any edited `.py`, then the guards
  when the file is under `airflow/dags/`, `scripts/` or `tests/`. A listed guard that does not exist
  blocks the edit with a message: a guard that silently stops running is the failure guards exist to
  catch. The hook timeout is 60 s, and a guard that overruns it blocks the edit.
- **`run_checks_before_commit.py`** (`PreToolUse` on `git commit`): ruff on the staged files, then
  `free_checks.sh`. A failure blocks the commit.
- **`ask_before_push.py` and `confirm_before_commit.py`**: two prompts, no checks. Every `git push`
  and `git commit` from Claude becomes a permission prompt showing the branch, the remote, and the
  staged diff. Exit 0 with no output allows silently, exit 2 blocks, the JSON `ask` decision stops and
  lets the developer decide. A block wins over an ask, so a commit that fails the checks never
  reaches the dialog.
- **`block_long_python.py`**: stops Claude starting the long, networked runs. The developer starts
  those in a terminal where the log streams live.
