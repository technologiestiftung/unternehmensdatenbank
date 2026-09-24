#!/usr/bin/env python3
"""PreToolUse hook: a `git commit` from Claude becomes a permission prompt that shows the staged diff.

Agreed 2026-09-24. `run_checks_before_commit.py` decides whether a commit is *allowed* (ruff, guards,
17 tests — exit 2 blocks). This hook decides nothing: it puts what is about to be committed in front of
Tu — `git diff --cached --stat`, then the first 120 lines of `git diff --cached` — and answers
`permissionDecision: "ask"`, so the dialog itself shows the change and nothing has to be run
separately to see it.

The two hooks share a matcher and Claude Code runs matching hooks in parallel; a block from the
checks wins over an ask from here, which is the right precedence.

Exit code 2 is not used: it would block outright. Exit 0 with no output lets every other command
through unprompted.

What is shown for the two ways git picks up changes:

- plain `git commit` — the index, which is what `--cached` reads.
- `git commit -a` / `-am` — git stages the modified tracked files itself at commit time, so the
  index may be empty *now*; the working-tree diff (`git diff`) is shown as well, labelled.
"""
from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path

HOOKS = Path(__file__).resolve().parent
sys.path.insert(0, str(HOOKS))

from check_commit_message import _GIT_COMMIT  # noqa: E402  the one definition of "a git commit"
from run_checks_before_commit import _commits_all  # noqa: E402  the one definition of "-a given"

#: How much of the line-level diff goes into the dialog. The stat is always complete; the diff is
#: cut here with a marker saying how much is left. Chosen so a dialog stays one screen-ish.
MAX_DIFF_LINES = 120


def _git(*args: str) -> str:
    try:
        result = subprocess.run(["git", *args], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return (result.stdout or "").rstrip()


def _cut(text: str, limit: int) -> str:
    lines = text.splitlines()
    if len(lines) <= limit:
        return text
    return "\n".join(lines[:limit]) + f"\n... {len(lines) - limit} more lines not shown"


def staged_summary(argv: list[str]) -> str:
    branch = _git("rev-parse", "--abbrev-ref", "HEAD") or "(no branch)"
    parts = [f"COMMIT on branch {branch}"]

    stat = _git("diff", "--cached", "--stat")
    diff = _git("diff", "--cached")
    if stat:
        parts.append(f"Staged:\n{stat}")
        parts.append(_cut(diff, MAX_DIFF_LINES))
    else:
        parts.append("Staged: nothing")

    if _commits_all(argv):
        wt_stat = _git("diff", "--stat")
        if wt_stat:
            parts.append(f"Also committed by -a (modified tracked files):\n{wt_stat}")
            if not stat:
                parts.append(_cut(_git("diff"), MAX_DIFF_LINES))

    parts.append("Approve to commit; deny to change the staging first.")
    return "\n\n".join(parts)


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
        argv = command.split()

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": staged_summary(argv),
        }
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
