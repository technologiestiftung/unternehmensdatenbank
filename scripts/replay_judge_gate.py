"""Replay the judge gate over a finished run and report what it would have skipped.

The judge — a local language model that reads a downloaded page and says whether it belongs to a
company — is about 69% of a combined-discovery run's wall clock. `pipeline._worth_judging` decides
which candidate URLs are worth showing it. This script answers the only question that matters about
a change to that decision:

    how many model calls does it remove, and how many real discoveries does it remove with them?

It answers from a run that already happened. `results.jsonl` stores, for every candidate URL, the
evidence the hand-written rules collected, what the model then said, and which flags the model
added. That is everything the gate reads, so the gate can be re-run against it exactly — with no
model, no network, no database and no page downloads. A change to the gate is therefore free to
evaluate, unlike a change to the extraction, which needs the pages themselves.

    python scripts/replay_judge_gate.py
    python scripts/replay_judge_gate.py --results path/to/results.jsonl

Reads the same file `report` reads and writes nothing.

One trap, and it is the reason this script imports the real gate instead of restating it: the
`evidence` list stored for a judged candidate ALREADY CONTAINS the model's own flags, because the
pipeline merges them in before writing the row. Two of those flags — `register_number_match` and
`postcode_match` — are ordinary evidence the gate itself reads. Testing the gate against the stored
list would therefore let the model's answer decide whether the model gets asked, and would quietly
report a gate that cannot lose anything. `judge_added` records exactly which flags were the model's,
so subtracting it reconstructs what the rules knew BEFORE the call. That subtraction is the whole
correctness of this script.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = REPO_ROOT / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from combined_discovery import confidence as conf  # noqa: E402
from combined_discovery import pipeline  # noqa: E402
from url_discovery.scorer import name_uniqueness  # noqa: E402

DEFAULT_RESULTS = REPO_ROOT / "airflow" / "data" / "combined_discovery" / "results.jsonl"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a results file, splitting on newlines ONLY.

    Not `splitlines()`: German page text carries raw U+2028 and U+2029, which `splitlines` treats
    as line breaks and `json.dumps` has no reason to escape. One record then gets cut in two and
    reads exactly like a truncated write. Same as `run_combined_discovery.read_jsonl`, copied
    rather than imported so this script stays free of that module's database defaults.
    """
    if not path.exists():
        raise SystemExit(f"no results file at {path}")
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").split("\n"), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{path}:{number} is not valid JSON: {exc}") from exc
    return rows


def latest_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per company, the newest. The file is append-only; re-running a company adds a line."""
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        company_id = row.get("company_id")
        if company_id:
            latest[company_id] = row
    return list(latest.values())


def evidence_before_judge(answer: dict[str, Any]) -> set[str]:
    """What the hand-written rules knew about this URL before the model was asked.

    `judge_added` is what the model contributed that nothing else had — see the module docstring
    for why subtracting it is not optional.
    """
    return set(answer.get("evidence") or []) - set(answer.get("judge_added") or [])


class FakeEntry:
    """Just enough of `pipeline.UrlAnswer` for `_worth_judging` to read.

    Deliberately calling the real function rather than reimplementing its rule: a replay that
    restated the gate would keep agreeing with itself after the gate changed.
    """

    def __init__(self, evidence: set[str], has_text: bool) -> None:
        self.evidence = evidence
        # The gate's first test is "was anything downloaded". Every candidate in this file that the
        # judge actually saw had text; one it skipped may not have. `page_text` is not stored, so
        # for skipped candidates this is inferred rather than known — see `main`.
        self.page_text = "x" if has_text else ""
        self.imprint_text = ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument(
        "--show", type=int, default=10, help="how many lost results to print in full"
    )
    args = parser.parse_args()

    rows = latest_rows(read_jsonl(args.results))

    # Re-score rather than read the stored `confidence` and `band`. Those were frozen by the
    # confidence.py of the day the run happened, and the point of this script is to test today's
    # code. Scoring needs only `methods`, `evidence` and the company name, all of which are stored —
    # so the whole scoring side is replayable, not just the gate.
    #
    # `agreed` below is the honesty check: when confidence.py has not changed since the run it must
    # reproduce every stored value exactly. A mismatch count is information, not a failure — it says
    # how far today's scoring has moved from the run being replayed.
    judged: list[dict[str, Any]] = []
    agreed = total = 0
    for row in rows:
        uniqueness = name_uniqueness(row["company_name"])
        for answer in row.get("answers") or []:
            methods = list(set(answer.get("methods") or []))
            after = set(answer.get("evidence") or [])
            before = evidence_before_judge(answer)
            answer["_before"] = conf.score(
                methods=methods, evidence=before, name_uniqueness=uniqueness
            )
            answer["_after"] = conf.score(
                methods=methods, evidence=after, name_uniqueness=uniqueness
            )
            answer["_evidence_before"] = before
            total += 1
            if abs(answer["_after"].value - (answer.get("confidence") or 0.0)) < 1e-9:
                agreed += 1
            if answer.get("judged"):
                judged.append(answer)

    print(f"{args.results}")
    print(f"  companies: {len(rows)}")
    print(f"  candidate URLs judged: {len(judged)}")
    print(f"  re-scoring reproduces the stored confidence for {agreed}/{total} candidates", end="")
    print(" (confidence.py unchanged since the run)" if agreed == total else " (scoring has moved)")
    print()
    print("The gate under test, imported from pipeline.py, not restated here:")
    print(f"  skip when already proven by:        {sorted(pipeline._JUDGE_SKIP_EVIDENCE)}")
    print(f"  skip on a standing disqualifier except: {sorted(pipeline._JUDGE_DESPITE)}")
    print(f"  judge only when it includes one of: {sorted(pipeline._JUDGE_IDENTITY_EVIDENCE)}")
    print()

    kept: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for answer in judged:
        if pipeline._worth_judging(FakeEntry(answer["_evidence_before"], has_text=True)):
            kept.append(answer)
        else:
            skipped.append(answer)

    # A model call removed is a saving. A model call removed that changed the outcome is a loss, and
    # there are two ways to lose. Both are measured against today's scoring.
    #
    # 1. A discovery: the model said `own_site` and that pushed the row over the bar.
    # 2. A demotion: the row was over the bar on the rules alone and the model pushed it back under.
    #    This is the one that matters most, because it is most of what the judge does — skipping it
    #    means a wrong website returns.
    lost_finds = [
        a
        for a in skipped
        if a.get("judge_verdict") == "own_site"
        and a["_after"].promotable
        and not a["_before"].promotable
    ]
    lost_demotions = [
        a for a in skipped if a["_before"].promotable and not a["_after"].promotable
    ]

    print(f"  would still judge: {len(kept)}")
    print(f"  would skip:        {len(skipped)}", end="")
    if judged:
        print(f"   ({len(skipped) / len(judged):.0%} of the stage)")
    else:
        print()
    print()
    print("  verdicts that would no longer be collected:")
    for verdict, count in Counter(a.get("judge_verdict") for a in skipped).most_common():
        print(f"    {verdict or 'none':12s} {count}")
    print()

    print(f"  discoveries lost (the model's own_site carried the row): {len(lost_finds)}")
    for answer in lost_finds[: args.show]:
        print(f"    {answer['_after'].value:.2f} {(answer.get('url') or '')[:58]}")
        print(f"      evidence before the call: {sorted(answer['_evidence_before'])}")
    print()

    print(f"  demotions lost (a wrong website returns): {len(lost_demotions)}")
    for answer in lost_demotions[: args.show]:
        print(
            f"    {answer['_before'].value:.2f} -> {answer['_after'].value:.2f}  "
            f"{(answer.get('url') or '')[:52]}"
        )
    print()

    # What the stage still has to pay for, so the saving is quotable next to the risk it carries.
    still = Counter(a.get("judge_verdict") for a in kept)
    print(f"  of the {len(kept)} calls that remain, the model answered:")
    for verdict, count in still.most_common():
        print(f"    {verdict or 'none':12s} {count}")
    print()

    ok = not lost_finds and not lost_demotions
    print("VERDICT:", "safe on this sample" if ok else "NOT SAFE — it removes real results")
    if not ok:
        print("  A gate may only remove calls that changed nothing. Fix the scoring, not the gate:")
        print("  a row the rules should never have promoted must be refused before the model runs.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
