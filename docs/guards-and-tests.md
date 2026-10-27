# Guards and tests — what checks what, and what each one is defending

This project has two kinds of automated check and they are easy to confuse.

**Guards** are scripts in `scripts/`. They check the **rules**: does the code still refuse a
directory page, does a keyword still fail to match inside a German compound word. They are free — no
API call, no language model, no database, no network — and finish in a second or two each.

**Tests** are pytest modules in `tests/`. Two of them check the **numbers those rules last
produced**, so that a change which quietly moves 300 companies between buckets leaves a trace.

Neither kind checks whether the answers are *correct*. Nothing in this repo does, because **not one
row of the three industry columns has ever been hand-labelled.** A failure means "these numbers
moved, find out why", not "the classification got worse".

## The eight guards

Run them after touching `search_common.py`, `imprint_probe.py`,
`url_discovery/imprint_extractor.py`, or anything under `combined_discovery/`.

| guard | what it defends |
|---|---|
| `check_own_site_filter.py` | 21 real URLs guarding `is_own_site_candidate`. Every case is a URL a search provider actually returned |
| `check_imprint_owner_extraction.py` | 19 real Impressum layouts guarding owner extraction |
| `check_combined_discovery.py` | URL guessing, the confidence ladder, the judge's gate, JSON-LD parsing |
| `check_branch_classifier.py` | the compound rule and the hyphen knob, keyword weights, the WZ lookup, the `content-index` key, label staleness, and the model's answer validation |
| `check_purpose_branch.py` | 154 checks on column 3: the seven outcomes stay disjoint, the website gate and WZ code cannot get in, the 40-character floor keeps what it takes, a failed model call never becomes `none_apply` |
| `check_osm_match.py` | `osm_snapshot`, used by combined discovery's `osm` stage |
| `check_branch_observations.py` | the classification WRITE path: that every field name the emitter can write is covered by V20's index predicate *and* `insert_text_observation`'s `ON CONFLICT`, that no confidence the policy can produce sits at or below `_FIELD_MIN_CONF`, that **no `resolve_*` names either `_withheld` field**, and that `ASSIGNED_OUTCOMES` is imported rather than restated |
| `replay_judge_gate.py` | re-runs the real judge gate over `results.jsonl`, failing if a change loses a discovery **or a demotion** |

### Two rules a guard here must follow

Both were learned by writing a guard that did not.

1. **Import the rule you are checking. Never restate it.** A check that rebuilds the logic agrees
   with itself no matter what the real code does. `check_branch_classifier.py` calls
   `branch_llm.verdict_from_answer`; `check_purpose_branch.py` calls `purpose_verdict_from_answer`.
2. **Assert on the syntax tree, not the source text**, when the thing you are forbidding is
   *discussed* in a docstring. `check_purpose_branch.py` proves `purpose_branch` never calls
   `classify()` by walking the parsed AST, because the module docstring explains the website gate at
   length and a substring search finds the prose and fails on a violation that is not there. The same
   trap caught a check for "no Berlin field name in the prompt": a plain `in` test found **`Handel`
   inside `Handelsregister`**. Use `_pattern_for`, which is the project's own compound rule.

## The hooks

Both live in `.claude/hooks/` and are wired from `.claude/settings.json`. **Tracked in git since
2026-09-24** — before that `.claude/` was gitignored, so a fresh checkout had no hooks and nothing
said so. `settings.local.json` (personal permissions) and `worktrees/` stay ignored.

### `guard_after_edit.py` — `PostToolUse` on Write|Edit

Two steps. First, **ruff** on the edited file, for any `.py` in the repo (about 0.1 s; `ruff.toml` at
the root, default rule set, E501 off — 490 lines were over 100 characters on 2026-09-24, and the gate
had to start green). A missing ruff blocks with "pip install ruff" rather than passing silently.
Second, after an edit under `combined_discovery/`, `scripts/` or `tests/` it runs **six** things —
five guards plus `tests/integration/test_ground_truth_columns.py` — and blocks the edit if one fails,
so a breakage reports in the same turn it happened rather than three edits later. About four seconds.

### `run_checks_before_commit.py` — `PreToolUse` on Bash|PowerShell

Fires only on a `git commit` (it imports `_GIT_COMMIT` from `check_commit_message.py`, the one
definition of what a commit command looks like) and only when something is staged or `-a` is given.
Then, in order, blocking on the first failure:

1. **ruff** on the `.py` files the commit would contain — staged, plus modified tracked files when
   `-a`/`--all` is present, because those are staged only when git runs.
2. **The six guards**, imported as `guard_after_edit.GUARDS` and run through its `run_guard`.
3. **`test_ground_truth_discovery.py`** — the ten Postgres tests, via `pytest -q`. It probes the
   database first through `tests.loaders.db_cursor()` with `PGCONNECT_TIMEOUT=3`. **Postgres
   unreachable blocks the commit.** That is the opposite of the edit-time rule below and is meant
   to be: an edit is provisional, a commit is history, and Tu decided on 2026-09-24 that every commit
   needs all 17 tests, a docs-only commit included. Start the stack with `./scripts/dev-up.sh`.
   **Exception since 2026-10-27:** where `airflow/data/combined_discovery/branches.jsonl` does not exist
   (folder found by `tests.paths.artifacts()`; only `classify` writes that file), the machine does not hold the reference copy of the example data, so
   both ground-truth modules are skipped, Postgres is not required, and the hook prints a notice. The
   marker is that file, not the folder (a `run` for a page cache creates the folder) and not the company count, so a bug that deletes companies still fails on the
   reference machine.

### `ask_before_push.py` and `confirm_before_commit.py` — `PreToolUse` on Bash|PowerShell

Two prompts, no checks. `ask_before_push.py` turns every `git push` from Claude into a permission
prompt naming the branch, its upstream and the remote URL, whatever the permission mode.
`confirm_before_commit.py` does the same for `git commit` and puts `git diff --cached --stat` plus the
first **120** lines of `git diff --cached` into the dialog itself (the working-tree stat too when `-a`
is given), so the change is visible without running anything. Both answer `permissionDecision:
"ask"`. The distinction that matters for all hooks here: **exit 0 with no output = allow silently;
exit 2 = block outright; the JSON `ask` decision = stop and let Tu decide.** Hooks under one matcher
run in parallel and a block wins over an ask, so a commit that fails the checks never reaches the
dialog.

Timeout 180 s. Measured 2026-09-24 with the stack up and everything green: **5 s** end to end, of
which the 17 tests take 2.2 s. With Postgres unreachable it blocks after the guards, 8 s (measured
by pointing `PG_DSN` at a closed port).

Two things about the edit-time hook that have bitten:

- **`GUARDS` entries are repo-relative paths.** They used to resolve as `REPO/"scripts"/<name>`,
  which meant a guard that moved out of `scripts/` was silently skipped by a `not exists` branch —
  exactly the failure the guards exist to catch, and it happened when the ground-truth tests moved to
  `tests/integration/`. A listed guard that does not exist now blocks the edit with a message.
- **The hook timeout is 60 s**, and a guard that overruns it blocks the edit rather than reporting a
  failure. `test_ground_truth_discovery.py` is deliberately **not** in the edit-time hook: it needs a
  running Postgres, and a guard that fails because Docker is down would block editing for a reason
  unrelated to the edit. The commit-time hook runs it, and there Docker down does block.

## The tests

```bash
ruff check .                                               # the linter, same config as the hooks
pytest -q                                                  # 17 tests
pytest -m "not integration"                                # the subset needing neither disk nor Docker
python tests/integration/test_ground_truth_columns.py      # standalone, no pytest needed
python tests/integration/test_ground_truth_discovery.py    # standalone, needs Docker
```

`tests/` splits into `unit/` (needs nothing but the code) and `integration/` (reads the artifacts on
disk, or the database). Integration modules carry `pytest.mark.integration`. `pytest.ini` sets
`testpaths = tests`, so the eight guards are never collected — they are scripts, not tests.

### `test_ground_truth_columns.py` — 7 tests, free, 1.6 s, in the hook

Pins the buckets in `branches.jsonl` (five outcomes) and `purpose_branches.jsonl` (seven), each
summing to 14,443, with the outcome set closed so a new bucket fails rather than appearing silently.
Also checks that the two columns never share a field name, that both were scored against the same
taxonomy version, and that every hand label in the worksheets is a value the reader recognises.

It is **the only thing that catches `branches.jsonl` going stale.** `run` appends to `results.jsonl`;
`classify` reads a snapshot of it. If `run` has since found more websites, every bucket count is
describing a population that no longer exists.

### `test_ground_truth_discovery.py` — 10 tests, needs Docker

Pins canonical websites per source — **2,815 / 552 / 85 / 39 / 37 / 1 = 3,529** — the seven source
priorities, four resolver invariants, V19's `company_managers` equality, and V20's six view columns
being present and empty.

**Count websites *through* `company_canonical_field_sources`, never by grouping on the observation's
own `field_name`.** The resolver aliases `website ← [candidate_url, website]`, so a company whose
winning claim is stored as `candidate_url` still has a canonical website. Grouping on the observation
name splits Brave's 85 into 82 + 3 and reads like three lost websites; the first version of this
baseline said 82 for exactly that reason.

One number in it is a **floor, not an equality**: `WEBSITE_OBSERVATION_FLOOR`. Observations are never
deleted, only un-canonicalised, so it may rise and must never fall. Exactly two migrations have been
allowed to lower it, both times because the rows removed were provably exact duplicates.

### Both run two ways, and both must keep working

`pytest tests/…` and `python tests/…` standalone. **pytest is host-only and deliberately absent from
`airflow/requirements.txt`**, which is why `import pytest` is optional and a built-in runner takes
over when it is missing: it keeps the checks dependency-free and able to run inside the Airflow image.
Do not "simplify" that away by adding the dependency.

Shared setup lives in `tests/paths.py` (a plain module importable by both modes), `tests/loaders.py`
(cached readers) and `tests/conftest.py` (one-line fixtures over those loaders).

### Changing a pinned number

If you change a weight, a threshold or the taxonomy **on purpose**, these numbers are *supposed* to
move. Re-measure with the `*-report` subcommands and edit the frozen block **in the same commit as
the change**, with the reason in the message. Never edit a pinned number to make a red test green
without first understanding why it moved.
