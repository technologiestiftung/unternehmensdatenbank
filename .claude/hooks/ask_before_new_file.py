#!/usr/bin/env python3
"""PreToolUse hook: make every NEW file in the repo a decision you make, not one Claude makes.

Over-engineering has a signature, and it is a new module. Editing an existing file is bounded by
what that file already does; creating `branch_embeddings.py` is how a 40-row labelling task turns
into a similarity engine nobody asked for.

So: writing to a file that already exists is allowed silently. Creating one that does not exist
turns into a permission prompt naming the file, and you decide. This does not block work — it makes
the scope change visible at the moment it happens, which is the only moment it is cheap to refuse.

Temporary files are exempt. Scratch work belongs in the session scratchpad and there is no reason to
be asked about it.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

#: Paths where new files need no permission: scratch space and plan files.
EXEMPT_PARTS = ("/temp/claude/", "/.claude/plans/", "/scratchpad/", "/tmp/")


def decide(path: str) -> tuple[str, str]:
    """(permissionDecision, reason). "allow" passes straight through; "ask" prompts the user."""
    if not path:
        return "allow", ""
    normalised = path.replace("\\", "/").lower()
    if any(part in normalised for part in EXEMPT_PARTS):
        return "allow", ""
    if Path(path).exists():
        return "allow", ""
    return "ask", (
        f"NEW FILE: {Path(path).name}\n"
        f"  {path}\n\n"
        "A new file is usually a widening of scope. Approve it if it was part of what you asked "
        "for; deny it and ask Claude to put the change in an existing file if it was not."
    )


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    path = str((payload.get("tool_input") or {}).get("file_path") or "")
    decision, reason = decide(path)
    if decision == "allow":
        return 0

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": decision,
            "permissionDecisionReason": reason,
        }
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
