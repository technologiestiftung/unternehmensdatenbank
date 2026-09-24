#!/usr/bin/env python3
"""Stop Claude running the expensive scripts. Tu runs those, in a terminal they can watch.

Agreed 2026-08-12 and again 2026-08-26: **Claude edits the code, Tu runs the pipeline.** The reason
is not caution about cost, it is that a long run Claude launches is unwatchable — and on 2026-08-26 it
was literally invisible, because a `run_in_background` command piped through `| tail -14` buffers all
of its output until the process exits, so the task's log file sat at 0 bytes for 20 minutes.

So this hook blocks the commands that take minutes and hours, and lets everything else through. What
counts as expensive is a **model call or a download**, not the word "python": the free guards run in a
second, `classify` takes two minutes with no network, and `classify-purpose` 13 seconds. Blocking
those would stop Claude checking its own work, which is the opposite of the point.

Exit codes: 0 lets the command run, 2 blocks it and shows stderr to Claude as feedback.
"""

from __future__ import annotations

import json
import re
import sys

#: Each entry is (pattern, what it costs). The pattern is matched against the whole command line,
#: case-insensitively. Keep the cost concrete — a number is what makes the handover credible.
EXPENSIVE = (
    # Any model pass over the register purposes. 10 s a call on qwen3.5:4b.
    (r"classify-purpose\b.*--llm", "a model call per company, ~10 s each"),
    (r"--tier\s+\S*semantic", "the 77-way ask over up to 6,481 companies, ~18 h"),
    # The website tie-break, measured at 10.3 s a page over 302 qualifying rows.
    (r"\bclassify\b.*--llm", "the tie-break model, ~52 min over the qualifying rows"),
    # Reads pages with a local model, one company at a time.
    (r"propose-keywords", "a model call per company with no keyword"),
    # Downloads. `run` probes URLs and `fetch-content` fetches services pages.
    (r"run_combined_discovery\.py\s+run\b", "network probes for every company"),
    (r"fetch-content", "downloads a services page per company"),
    # The other long pipelines in this repo.
    (r"enrich_company_fields\.py", "paid API calls per company"),
    (r"benchmark_url_owner_discovery\.py", "paid search-provider calls"),
    (r"ac_measurement\.py", "paid search-provider calls"),
    (r"run_discovery|search_discovery_runner", "paid search-provider calls"),
)

#: Let a deliberate single-call probe through. Timing one call is how the numbers above got measured,
#: and forbidding it would mean nobody could ever check whether they are still true.
PROBE = re.compile(r"--llm-limit\s+([1-9]|10)\b")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001  a hook that cannot parse its input must not block the session
        return 0

    command = str((payload.get("tool_input") or {}).get("command") or "")
    if not command:
        return 0

    # Only the FIRST LINE is examined, because that is where an invocation lives.
    #
    # Matching the whole command blocked a heredoc patch script — `python - <<'PY' … PY` — whose body
    # happened to contain the literal text "--tier semantic" inside a Python string it was writing
    # into a help message. That is an edit, not a run, and a guard that blocks edits because of what
    # they say about running is worse than no guard: the next move is to disable it.
    #
    # A real invocation is one line. The cost of this narrowing is that inline python could still
    # reach the model directly, which is accepted: a deliberate probe of a call or two is allowed
    # anyway, and the flag text is what a long run is actually spelled with.
    haystack = command.strip().splitlines()[0].lower() if command.strip() else ""
    for pattern, cost in EXPENSIVE:
        if not re.search(pattern, haystack):
            continue
        if PROBE.search(haystack):
            # A probe of ten calls or fewer is a measurement, not a run. Allowed, and worth saying so
            # in the transcript rather than letting it look like the hook missed it.
            print(f"note: allowing a small probe ({cost}) — keep it to a measurement", file=sys.stderr)
            return 0
        print(
            f"BLOCKED: this costs {cost}.\n"
            f"\n"
            f"  {command.strip()[:300]}\n"
            f"\n"
            f"Tu runs the expensive commands, in a terminal where the log streams live. Hand it over\n"
            f"instead: print the command, say what numbers you expect, and name the one output to\n"
            f"paste back. Do NOT re-run it in the background, and do NOT pipe it through `tail` — a\n"
            f"pipe buffers the whole run and the log file stays empty until it exits.\n"
            f"\n"
            f"Still allowed, and enough to check your own work: the free guards\n"
            f"(check_*.py), `classify` and `classify-purpose` without --llm, every *-report\n"
            f"subcommand, and a deliberate probe of ten calls or fewer via --llm-limit.",
            file=sys.stderr,
        )
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
