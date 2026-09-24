#!/usr/bin/env python3
"""PreToolUse hook: before Claude runs `git commit`, lint, run the guards, run all 17 tests.

Agreed 2026-09-24. The edit-time hook (`guard_after_edit.py`) catches a breakage in the turn it
happens, but only for edits under the watched directories and never for the ten tests that need
Postgres. A commit is the moment the change becomes history, so this is where everything runs:

1. **ruff** on the `.py` files the commit would contain — staged files, plus the modified tracked
   files when the command carries `-a`/`--all`, since those are staged only when git runs.
2. **The six guards** from `guard_after_edit.GUARDS` — five rule checks plus the seven free tests in
   `test_ground_truth_columns.py`. About four seconds.
3. **`test_ground_truth_discovery.py`**, the ten tests that need the database. Postgres unreachable
   **blocks the commit**. This is deliberately the opposite of the edit-time rule ("Docker down must
   not block an edit"): an edit is provisional, a commit is not, and Tu decided on 2026-09-24 that
   every commit needs all 17 tests, a docs-only one included.

What passes straight through, in well under a second: any Bash command that is not a `git commit`
(one regex), and a `git commit` with nothing staged and no `-a` — git will refuse that itself.

Exit codes: 0 lets the command run, 2 blocks it and shows stderr to Claude as feedback. Success
prints nothing.

The commit regex and the guard runner are IMPORTED from the sibling hooks, never copied — a copy
would agree with itself no matter what the real hook does (docs/guards-and-tests.md, rule 1).
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

HOOKS = Path(__file__).resolve().parent
REPO = HOOKS.parents[1]
sys.path.insert(0, str(HOOKS))
sys.path.insert(0, str(REPO))

from check_commit_message import _GIT_COMMIT  # noqa: E402  the one definition of "a git commit"
from guard_after_edit import GUARDS, ruff_check, run_guard  # noqa: E402

#: The tests that need Postgres. Run through pytest so the 10 results are counted, not just exit 0.
DB_TESTS = "tests/integration/test_ground_truth_discovery.py"

#: libpq reads this. Docker down is "connection refused" and returns at once; a firewalled host would
#: otherwise hang for libpq's default of forever.
PG_CONNECT_TIMEOUT_S = "3"


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True)


def _commits_all(argv: list[str]) -> bool:
    """True when the command line carries `-a`/`--all`, alone or folded into `-am`."""
    for token in argv:
        if token == "--all":
            return True
        if token.startswith("-") and not token.startswith("--") and "a" in token[1:]:
            return True
    return False


def python_files_to_lint(argv: list[str]) -> list[str]:
    staged = _git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "--", "*.py")
    files = set(line.strip() for line in staged.stdout.splitlines() if line.strip())
    if _commits_all(argv):
        unstaged = _git("diff", "--name-only", "--diff-filter=ACMR", "--", "*.py")
        files.update(line.strip() for line in unstaged.stdout.splitlines() if line.strip())
    return sorted(f for f in files if (REPO / f).exists())


def anything_to_commit(argv: list[str]) -> bool:
    if _commits_all(argv):
        return True
    return _git("diff", "--cached", "--quiet").returncode == 1


def postgres_failure() -> str | None:
    """None when a `SELECT 1` succeeds through the tests' own connection helper, else the error."""
    os.environ.setdefault("PGCONNECT_TIMEOUT", PG_CONNECT_TIMEOUT_S)
    try:
        from tests.loaders import db_cursor

        with db_cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except Exception as exc:  # noqa: BLE001  any failure here means "do not run the tests"
        return str(exc).strip().splitlines()[0][:300] if str(exc).strip() else type(exc).__name__
    return None


def run_db_tests() -> str | None:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", DB_TESTS], cwd=REPO, capture_output=True, text=True
    )
    if result.returncode == 0:
        return None
    lines = [
        line for line in (result.stdout or "").splitlines()
        if line.startswith("FAILED") or line.startswith("ERROR") or "passed" in line or "failed" in line
    ]
    return (
        f"{DB_TESTS} FAILED\n"
        + ("\n".join(lines[:12]) or (result.stdout or result.stderr or "").strip()[-800:])
        + f"\n\nFix this before committing: python -m pytest -q {DB_TESTS}\n"
    )


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001  a hook that cannot parse its input must not block the session
        return 0

    command = str((payload.get("tool_input") or {}).get("command") or "")
    if not command or not _GIT_COMMIT.search(command):
        return 0

    try:
        argv = shlex.split(command, posix=True)
    except ValueError:
        return 0  # unbalanced quotes; the shell will say so

    if not anything_to_commit(argv):
        return 0

    def blocked(text: str) -> int:
        sys.stderr.write(f"BLOCKED before commit: {text}")
        return 2

    failure = ruff_check(python_files_to_lint(argv))
    if failure:
        return blocked(failure)

    for guard in GUARDS:
        failure = run_guard(guard)
        if failure:
            return blocked(failure)

    error = postgres_failure()
    if error:
        return blocked(
            f"Postgres is unreachable, so the 10 tests in {DB_TESTS} did not run.\n"
            f"  {error}\n"
            f"Every commit needs all 17 tests. Start the stack (./scripts/dev-up.sh) and commit again.\n"
        )

    failure = run_db_tests()
    if failure:
        return blocked(failure)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
