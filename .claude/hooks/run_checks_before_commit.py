#!/usr/bin/env python3
"""PreToolUse hook: before Claude runs `git commit`, lint the staged files and run the free checks.

The edit-time hook (`guard_after_edit.py`) catches a breakage in the turn it happens, but only for
edits under the watched directories. A commit is the moment the change becomes history, so this is
where everything runs:

**`scripts/free_checks.sh`**: ruff over the whole repo, the unit tests and the seven rule guards.
The same script CI and no-mistakes run (decided 2026-10-28). It lints everything, so there is no
separate pass over the staged files. No database, no network: until then
   this hook also ran tests pinned to the Osnabrück example data, which needed Postgres; they went
   with that data.

What passes straight through, in well under a second: any Bash command that is not a `git commit`
(one regex), and a `git commit` with nothing staged and no `-a` — git will refuse that itself.

Exit codes: 0 lets the command run, 2 blocks it and shows stderr to Claude as feedback. Success
prints nothing.

The commit regex and the script's path are IMPORTED from the sibling hooks, never copied — a copy
would agree with itself no matter what the real hook does (docs/guards-and-tests.md, rule 1).
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

HOOKS = Path(__file__).resolve().parent
REPO = HOOKS.parents[1]
sys.path.insert(0, str(HOOKS))

from check_commit_message import _GIT_COMMIT  # noqa: E402  the one definition of "a git commit"
from guard_after_edit import FREE_CHECKS  # noqa: E402


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


def anything_to_commit(argv: list[str]) -> bool:
    if _commits_all(argv):
        return True
    return _git("diff", "--cached", "--quiet").returncode == 1


def free_checks_failure() -> str | None:
    """Run free_checks.sh with `python` meaning this hook's interpreter.

    The script calls `python`. An interpreter's own folder may have only `python3` or `python3.12`,
    so a small `python` wrapper goes first on PATH. A wrapper, not a symlink: a virtualenv finds its
    packages through the path it was called by, and a symlink elsewhere would lose them.
    """
    with tempfile.TemporaryDirectory() as shim:
        wrapper = Path(shim) / "python"
        wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n')
        wrapper.chmod(0o755)
        env = {**os.environ, "PATH": f"{shim}{os.pathsep}{os.environ.get('PATH', '')}"}
        result = subprocess.run(["sh", str(FREE_CHECKS)], cwd=REPO, capture_output=True, text=True, env=env)
    if result.returncode == 0:
        return None
    output = ((result.stdout or "") + (result.stderr or "")).strip()
    lines = [
        line for line in output.splitlines()
        if line.startswith(("FAIL", "ERROR", "Traceback")) or line.strip().startswith("- ")
        or " failed" in line or "Found " in line
    ]
    return (
        "scripts/free_checks.sh FAILED\n"
        + ("\n".join(lines[:15]) or output[-1200:])
        + "\n\nFix this before committing: sh scripts/free_checks.sh\n"
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

    failure = free_checks_failure()
    if failure:
        return blocked(failure)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
