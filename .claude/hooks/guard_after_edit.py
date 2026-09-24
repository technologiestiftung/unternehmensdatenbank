#!/usr/bin/env python3
"""PostToolUse hook: lint the file Claude just edited, then run this repo's free guard scripts.

Why a hook and not a rule in CLAUDE.md: a rule is something Claude can forget, rationalise, or
decide does not apply this time. A hook is run by the harness. It cannot be skipped.

Reads the tool-call JSON on stdin. Two steps:

1. **ruff** on the edited file, for any `.py` inside the repo. Configured in `ruff.toml` at the repo
   root — the default rule set, no line-length check. About 0.1 s. A missing ruff is a failure, not
   a pass: a linter that silently does not run must not look like one that did.
2. **The guards**, only when the edited file is part of the branch classification code. Exit 2 makes
   the message on stderr into feedback Claude has to deal with before it can carry on, which is the
   whole point: the guards fail in the same turn the breakage happened, not three edits later.

The guards are free — no API, no model, no database, no network — so this costs about four seconds.

`run_checks_before_commit.py` imports `GUARDS`, `run_guard` and `ruff_check` from here so the two
hooks cannot drift apart. Import the rule, never restate it (docs/guards-and-tests.md).
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Sequence

REPO = Path(__file__).resolve().parents[2]

#: Only these parts of the tree are guarded. An edit to a doc or an unrelated DAG runs nothing.
WATCHED = ("combined_discovery", "scripts", "tests")

#: Free, and in the order the runbook says to run them. All five finish in about four seconds
#: together, which matters: the hook's timeout is 60s and a guard that overruns it blocks the edit
#: rather than reporting a failure. The first four check the RULES;
#: `test_ground_truth_columns` checks the NUMBERS those rules last produced, and reads three
#: artifacts off disk to do it (1.6s, measured; `check_osm_match` costs 0.3s).
#:
#: `test_ground_truth_discovery.py` is deliberately NOT here. It needs a running Postgres, and a
#: guard that fails because Docker is down would block editing for a reason that has nothing to do
#: with the edit. The pre-commit hook runs it instead, and there Docker down DOES block (decided
#: 2026-09-24: every commit needs all 17 tests).
#:
#: Repo-relative paths, not bare filenames. They used to be resolved as REPO/"scripts"/<name>,
#: which meant a guard that moved out of scripts/ was silently skipped by the `not exists` branch
#: below -- the exact failure mode these guards exist to catch. The ground-truth tests moved to
#: tests/integration/ and would have stopped running with no message at all.
GUARDS = (
    "scripts/check_branch_classifier.py",
    "scripts/check_purpose_branch.py",
    "scripts/check_combined_discovery.py",
    "scripts/check_osm_match.py",
    # The write path: which field names reach the database, at what confidence, and which of them no
    # resolver may ever name. Free and AST-based like the rest, so it belongs in the hook.
    "scripts/check_branch_observations.py",
    "tests/integration/test_ground_truth_columns.py",
)


def run_guard(guard: str) -> str | None:
    """Run one guard. None when it passes; otherwise the text Claude must read."""
    script = REPO / guard
    if not script.exists():
        # Loud on purpose. A guard that has been renamed, moved or deleted must not look like
        # a guard that passed.
        return (
            f"guard {guard} is listed in {Path(__file__).name} but does not exist. "
            f"Fix the path or drop it from GUARDS -- until then it is not running.\n"
        )
    result = subprocess.run(
        [sys.executable, str(script)], cwd=REPO, capture_output=True, text=True
    )
    if result.returncode == 0:
        return None
    failed = [
        line for line in (result.stdout or "").splitlines()
        if line.startswith("FAIL") or line.strip().startswith("- ")
    ]
    return (
        f"{guard} FAILED\n"
        + ("\n".join(failed[:12]) or (result.stderr or "").strip()[:800])
        + f"\n\nFix this before doing anything else: python {guard}\n"
    )


def ruff_check(paths: Sequence[str]) -> str | None:
    """Lint `paths` with ruff. None when clean or nothing to lint; otherwise the text to read.

    `python -m ruff` rather than a bare `ruff`, so the interpreter running the hook decides which
    install is used. ruff missing is reported as a failure, never swallowed.
    """
    if not paths:
        return None
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", *paths], cwd=REPO, capture_output=True, text=True
    )
    if result.returncode == 0:
        return None
    output = (result.stdout or "").strip() or (result.stderr or "").strip()
    if "No module named ruff" in output:
        return "ruff is not installed, so nothing was linted. Install it: pip install ruff\n"
    return (
        "ruff FAILED\n"
        + output[:1500]
        + "\n\nFix this before doing anything else: ruff check "
        + " ".join(paths)
        + "\n"
    )


def _inside_repo(path: Path) -> bool:
    try:
        path.resolve().relative_to(REPO)
        return True
    except ValueError:
        return False


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0  # a hook that cannot parse its input must not block work

    raw = str((payload.get("tool_input") or {}).get("file_path") or "")
    if not raw:
        return 0
    path = raw.replace("\\", "/")

    if path.endswith(".py") and _inside_repo(Path(raw)):
        failure = ruff_check([str(Path(raw).resolve().relative_to(REPO))])
        if failure:
            sys.stderr.write(f"after editing {path}:\n{failure}")
            return 2

    if not any(part in path for part in WATCHED):
        return 0

    for guard in GUARDS:
        failure = run_guard(guard)
        if failure:
            sys.stderr.write(f"after editing {path}: {failure}")
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
