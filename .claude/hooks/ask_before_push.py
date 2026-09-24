#!/usr/bin/env python3
"""PreToolUse hook: a `git push` from Claude always becomes a permission prompt. Agreed 2026-09-24.

A push is the one git action here that leaves this machine. Everything before it — edits, commits,
branches — is local and reversible; a push is visible to the team the moment it lands, and a forced
one can drop somebody else's work. So the decision is Tu's, every time, whatever the permission mode.

How it asks: it prints the JSON Claude Code understands, with `permissionDecision: "ask"`. That is
the mechanism for "stop and ask" — exit code 2 would *block* the command outright with no way to say
yes, which is not what a confirmation is. Exit 0 with no output lets every other command through.

Matches `git push` in any position of a compound command line (`cd x && git push`, `git push origin
main`, `git push --force-with-lease`), including with global options before the verb
(`git -C repo push`). `git pushd`, a branch named `push` and `git push` inside a quoted string are
still matched conservatively — better one prompt too many than one push too few.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys

#: `git`, any tokens (global options and their values, e.g. `-C repo`), then `push` as a whole word
#: at the start of a token. Anchored at the start or after a command separator. Looser than
#: `_GIT_COMMIT` on purpose: `git -C . push` must prompt, and a false prompt costs one click.
_GIT_PUSH = re.compile(r"(^|[;&|]\s*)git\s+(\S+\s+)*?push\b")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001  a hook that cannot parse its input must not block the session
        return 0

    command = str((payload.get("tool_input") or {}).get("command") or "")
    match = _GIT_PUSH.search(command)
    if not match:
        return 0

    forced = bool(re.search(r"--force\b|--force-with-lease\b|\s-f\b", command))
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": (
                ("FORCE PUSH: " if forced else "PUSH: ")
                + "this leaves the machine and the team sees it.\n"
                f"  {command.strip()[:200]}\n"
                f"  {_where()}\n\n"
                "Approve to push now; deny to keep the commits local."
            ),
        }
    }))
    return 0


def _git(*args: str) -> str:
    try:
        result = subprocess.run(["git", *args], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _where() -> str:
    """`branch <name> -> <remote>/<upstream>` from the working directory the command runs in.

    The hook runs in the same cwd as the tool call, so this is the repo the push would touch. A branch
    with no upstream yet says so, because `git push` alone then fails or, with push.autoSetupRemote,
    creates the remote branch — worth seeing before saying yes.
    """
    branch = _git("rev-parse", "--abbrev-ref", "HEAD") or "(no branch)"
    upstream = _git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    remotes = _git("remote")
    if upstream:
        remote_name = upstream.split("/", 1)[0]
        url = _git("remote", "get-url", remote_name)
        return f"branch {branch} -> {upstream}  ({url})"
    if remotes:
        return f"branch {branch} has NO upstream yet; remotes: {', '.join(remotes.split())}"
    return f"branch {branch}; no remote configured"


if __name__ == "__main__":
    raise SystemExit(main())
