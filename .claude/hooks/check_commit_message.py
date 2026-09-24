#!/usr/bin/env python3
"""Keep commit messages short and scannable. Agreed 2026-09-11, after one ran to 60 lines.

The habit this stops is real and it is Claude's, not Tu's: a long session produces a long message,
because everything that happened feels worth recording. It is not. A commit message is read by
somebody scanning `git log` months later, and the parts they need are what changed and why. The
reasoning belongs in the code, the docs or the vault — all three of which outlive a log entry and
none of which is reached by scrolling.

So the body is capped at **15 non-blank lines** and bullets are preferred over paragraphs. The cap
is on the BODY, not the whole message: the subject line is free, and so are trailers
(`Co-Authored-By:`, `Signed-off-by:`), because neither is prose somebody has to read through.

Blank lines are not counted, so paragraph breaks and spacing around bullet lists cost nothing —
the cap measures how much there is to read, not how it is laid out.

What it inspects, and what it deliberately does not:

- `git commit -m "..."` — the message, joined the way git joins repeated `-m` flags.
- `git commit -F <file>` / `--file <file>` — the file is read from disk.
- `git commit` with no message flag opens an editor. Nothing to inspect before the fact, so it
  passes. `--amend` with no new message re-uses the old one and passes for the same reason.

Exit codes: 0 lets the command run, 2 blocks it and shows stderr to Claude as feedback.
"""

from __future__ import annotations

import json
import re
import shlex
import sys
from pathlib import Path

#: The cap, on non-blank body lines. Chosen because it is roughly one screen of `git log` and forces
#: the bullets to carry the message rather than the prose around them.
MAX_BODY_LINES = 15

#: Trailers are mechanical and are not prose to read through, so they do not count against the cap.
#: Matched as `Key: value` on its own line at the end of the message, the git trailer convention.
_TRAILER = re.compile(r"^[A-Za-z][A-Za-z-]*:\s", re.ASCII)

#: A `git commit` and nothing else. `git commit-tree` and a branch called `commit` must not match,
#: hence the word boundary and the leading anchor after any env prefix.
_GIT_COMMIT = re.compile(r"(^|[;&|]\s*)git\s+(-\S+\s+|--\S+(=\S+)?\s+)*commit\b")


def _message_from(argv: list[str]) -> str | None:
    """The message this invocation would commit, or None when there is nothing to inspect yet.

    Repeated `-m` flags are joined with a blank line between them, which is what git itself does —
    each one becomes its own paragraph. `-F -` reads stdin, which this hook cannot see, so it is
    treated as nothing to inspect rather than guessed at.
    """
    parts: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in ("-m", "--message") and index + 1 < len(argv):
            parts.append(argv[index + 1])
            index += 2
            continue
        if token.startswith("--message="):
            parts.append(token.split("=", 1)[1])
            index += 1
            continue
        if token.startswith("-m") and len(token) > 2:
            parts.append(token[2:])
            index += 1
            continue
        if token in ("-F", "--file") and index + 1 < len(argv):
            path = argv[index + 1]
            if path == "-":
                return None
            try:
                parts.append(Path(path).read_text(encoding="utf-8"))
            except OSError:
                # The file does not exist or cannot be read. Let git produce that error itself —
                # a hook that blocks on its own inability to read is a hook nobody keeps.
                return None
            index += 2
            continue
        if token.startswith("--file="):
            try:
                parts.append(Path(token.split("=", 1)[1]).read_text(encoding="utf-8"))
            except OSError:
                return None
            index += 1
            continue
        index += 1
    return "\n\n".join(parts) if parts else None


def body_lines(message: str) -> list[str]:
    """The non-blank body lines: everything after the subject, minus blanks and trailing trailers."""
    lines = message.splitlines()
    if not lines:
        return []
    # Line 1 is the subject and is free. Everything after it is body.
    body = [line for line in lines[1:] if line.strip()]
    # Trailers sit at the end. Strip them from the back only, so a `Note: ...` line in the middle of
    # the prose still counts — it is being read, whatever it looks like.
    while body and _TRAILER.match(body[-1]):
        body.pop()
    return body


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
        # Unbalanced quotes. Not this hook's problem; the shell will say so.
        return 0

    message = _message_from(argv)
    if message is None:
        return 0

    lines = body_lines(message)
    if len(lines) <= MAX_BODY_LINES:
        return 0

    preview = "\n".join(f"  {line[:96]}" for line in lines[:4])
    print(
        f"BLOCKED: the commit message body is {len(lines)} non-blank lines, over the {MAX_BODY_LINES}"
        f" allowed.\n"
        f"\n"
        f"It starts:\n"
        f"{preview}\n"
        f"  ...\n"
        f"\n"
        f"A commit message is read by somebody scanning `git log` months later. Keep the body to\n"
        f"{MAX_BODY_LINES} lines or fewer and make them BULLETS, not paragraphs: what changed, and\n"
        f"why. One idea per bullet.\n"
        f"\n"
        f"Not counted against the cap, so use them freely: the subject line, blank lines, and\n"
        f"trailers such as `Co-Authored-By:`.\n"
        f"\n"
        f"The detail you are about to cut is not lost - it belongs where it will still be read:\n"
        f"the code, a docstring, `docs/`, or the Obsidian vault. A log entry is the one place\n"
        f"nobody scrolls back through.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
