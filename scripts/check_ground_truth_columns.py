#!/usr/bin/env python3
"""Ground truth for the three industry columns. Free: no API, no model, no database, no network.

What this is for: `classify` and `classify-purpose` rewrite their whole output file every run, so a
change that silently moves 300 companies from `assigned` to `torn` leaves no trace. This pins the
shape of both files, and fails if any of it moves.

It asserts COUNTS, never correctness. No row of either column has ever been hand-checked, so there
is no precision number to assert; the two places the repo says "84%" are a link rate (644 of 766
homepages) and a work-reduction rate. If you deliberately change a weight, a threshold or the
taxonomy, the numbers below are SUPPOSED to move: re-measure and edit the block, in the same commit
as the change, with the reason in the message.

Nothing here restates a rule that lives in the code. `latest_rows`, `read_label_csv` and
`LABEL_VALUES` are imported from the real script: a test that re-implements which line of an
append-only file wins agrees with itself no matter what the pipeline does.

**Two ways to run it, and both must keep working.**

    python scripts/check_ground_truth_columns.py     # standalone, exit 1 on failure
    pytest scripts/check_ground_truth_columns.py     # 7 tests

The standalone path is the one the `PostToolUse` hook uses, and it does **not** require pytest: if
pytest cannot be imported, `_run_without_pytest` resolves each test's fixture names from the same
cached loaders and calls it. That keeps the promise CLAUDE.md makes about these guards — free, no
dependencies — and lets them run inside the Airflow image, which has no pytest.

`check()` accumulates rather than raising, which is deliberate. An assert per check would abort its
test at the first bad value: `test_column1_outcomes` holds twelve, and when the buckets really did
drift it reported all seven at once and they were fixed in one pass. `checks()` is what turns a
block of them into one test failure listing every mismatch.
"""
from __future__ import annotations

import importlib.util
import inspect
import json
import sys
from collections import Counter
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

try:
    import pytest
except ImportError:            # the Airflow image has no pytest; standalone mode still works
    pytest = None


def _fixture(**kwargs):
    """`pytest.fixture` when pytest is installed, a no-op decorator when it is not."""
    if pytest is None:
        return lambda fn: fn
    return pytest.fixture(**kwargs)


REPO = Path(__file__).resolve().parents[1]

for candidate in (REPO / "airflow" / "dags", Path("/opt/airflow/dags")):
    if candidate.is_dir():
        sys.path.insert(0, str(candidate))
        break

TOTAL_COMPANIES = 14_443

#: Measured 2026-09-08 from the artifacts on disk. Re-measure with:
#:   python scripts/run_combined_discovery.py classify-report
#:   python scripts/run_combined_discovery.py classify-purpose-report
#:
#: COLUMN 1 was re-run on 2026-09-08 to clear a 38-website staleness: `run` had appended to
#: `results.jsonl` since `classify` last read it. The published 346/272/563/6,558/6,704 was measured
#: over 902 websites; these numbers are the same rule over 940. The 38 extra websites bought 35
#: fields, and every bucket moved.
BRANCH_OUTCOMES = {
    "assigned": 381, "torn": 282, "no_website": 557,
    "no_keyword": 6_531, "no_text": 6_692,
}
BRANCH_WITHHELD = 557           # every one of them an outcome == no_website row
BRANCH_WITH_WEBSITE = 940       # must equal the websites in results.jsonl — see the staleness check
PURPOSE_OUTCOMES = {
    "assigned_keywords": 596, "assigned_model_77": 590, "assigned_model_3way": 14,
    "none_apply": 758, "torn": 47, "no_keyword": 5_136, "no_purpose": 7_302,
}
PURPOSE_WITHHELD_BY_FLOOR = 17
PURPOSE_FIELDS = 1_200
TAXONOMY_VERSION = "c154c74cf003"

#: `NONE` is the sentinel for "no field", not an empty string: the column is filled on every row.
NO_FIELD = "NONE"

LABEL_FILES = ("labels_todo.csv", "branch_labels_todo.csv", "purpose_labels_todo.csv")

FAILURES: list[str] = []


def check(label: str, got, want) -> None:
    """Record and print one comparison. Never raises — `checks()` does that, at the end."""
    ok = got == want
    if not ok:
        FAILURES.append(f"{label}  got={got!r} want={want!r}")
    print(f"{'ok  ' if ok else 'FAIL'} {label:<62} got={got!r:<28} want={want!r}")


def section(title: str) -> None:
    print(f"\n-- {title} --")


@contextmanager
def checks(title: str):
    """Every check in the block runs; the test then fails once, listing all of them."""
    section(title)
    first = len(FAILURES)
    yield
    new = FAILURES[first:]
    assert not new, "\n".join(["", *new])


# ---- loaders: cached, so the two run modes share one code path -------------------------------

@lru_cache(maxsize=None)
def _out_dir() -> Path:
    """Where the artifacts live. Same resolution order as the script that writes them.

    A function rather than a module constant on purpose: as a constant this raised at *import*
    time, which pytest reports as a collection error with no useful message instead of a failed
    test.
    """
    for path in (
        Path("/opt/airflow/data/combined_discovery"),
        REPO / "airflow" / "data" / "combined_discovery",
    ):
        if path.is_dir():
            return path
    raise AssertionError(
        "no combined_discovery output directory; run `classify` and `classify-purpose` first"
    )


@lru_cache(maxsize=None)
def _runner():
    """`run_combined_discovery.py` as a module, without running its CLI. Constants only."""
    path = Path(__file__).resolve().parent / "run_combined_discovery.py"
    spec = importlib.util.spec_from_file_location("_ground_truth_runner", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@lru_cache(maxsize=None)
def _rows(name: str) -> tuple[dict, ...]:
    """One dict per line. These files are one row per company, not append-only like `results`."""
    path = _out_dir() / name
    if not path.exists():
        raise AssertionError(f"no {path}; run `classify` and `classify-purpose` first")
    with path.open(encoding="utf-8") as handle:
        return tuple(json.loads(line) for line in handle if line.strip())


#: Fixture name -> loader. The only thing the two run modes have to agree on.
_LOADERS = {
    "runner": _runner,
    "branches": lambda: _rows("branches.jsonl"),
    "purposes": lambda: _rows("purpose_branches.jsonl"),
}


# ---- fixtures: one-line wrappers over the loaders above ---------------------------------------

@_fixture(scope="session")
def runner():
    """`run_combined_discovery.py`, exec'd once for the whole session — it is 4,115 lines."""
    return _LOADERS["runner"]()


@_fixture(scope="session")
def branches():
    return _LOADERS["branches"]()


@_fixture(scope="session")
def purposes():
    return _LOADERS["purposes"]()


# ---- the tests --------------------------------------------------------------------------------

def test_one_row_per_company(branches, purposes):
    with checks("one row per company, in both files"):
        check("branches.jsonl rows", len(branches), TOTAL_COMPANIES)
        check("purpose_branches.jsonl rows", len(purposes), TOTAL_COMPANIES)
        check("branches.jsonl companies are distinct",
              len({r["company_id"] for r in branches}), TOTAL_COMPANIES)
        check("purpose_branches.jsonl companies are distinct",
              len({r["company_id"] for r in purposes}), TOTAL_COMPANIES)


def test_column1_outcomes(branches):
    with checks("column 1: five outcomes, disjoint, summing to the table"):
        got = Counter(r["outcome"] for r in branches)
        for outcome, want in BRANCH_OUTCOMES.items():
            check(f"branch outcome {outcome}", got[outcome], want)
        check("no sixth branch outcome", sorted(got), sorted(BRANCH_OUTCOMES))
        check("branch outcomes sum to the table", sum(got.values()), TOTAL_COMPANIES)
        # `NONE` is the sentinel, not an empty string — the column is filled on all 14,443 rows.
        check("a Technologiefeld iff outcome == assigned",
              {(r["technologiefeld"] != NO_FIELD) == (r["outcome"] == "assigned")
               for r in branches},
              {True})
        check("withheld_branch count",
              sum(1 for r in branches if r["withheld_branch"]), BRANCH_WITHHELD)
        check("withheld only on no_website",
              {r["outcome"] for r in branches if r["withheld_branch"]}, {"no_website"})
        check("companies with a website",
              sum(1 for r in branches if r["website"]), BRANCH_WITH_WEBSITE)
        check("no website, no field",
              {r["technologiefeld"] for r in branches if not r["website"]}, {NO_FIELD})


def test_column3_outcomes(purposes):
    with checks("column 3: seven outcomes, disjoint, summing to the table"):
        got = Counter(r["outcome"] for r in purposes)
        for outcome, want in PURPOSE_OUTCOMES.items():
            check(f"purpose outcome {outcome}", got[outcome], want)
        check("no eighth purpose outcome", sorted(got), sorted(PURPOSE_OUTCOMES))
        check("purpose outcomes sum to the table", sum(got.values()), TOTAL_COMPANIES)
        check("a purpose_field iff the outcome is an assigned_* one",
              {(r["purpose_field"] != NO_FIELD) == r["outcome"].startswith("assigned")
               for r in purposes},
              {True})
        check("1,200 purpose fields in total",
              sum(1 for r in purposes if r["purpose_field"] != NO_FIELD), PURPOSE_FIELDS)
        check("withheld_by_floor count",
              sum(1 for r in purposes if r["withheld_by_floor"]), PURPOSE_WITHHELD_BY_FLOOR)
        # A model that failed is not a model that said no. Every none_apply row must carry the
        # answer that produced it; neither a reason nor a refusal means a lost call reported as a
        # verdict.
        check("a failed model call never becomes none_apply",
              [r["company_id"] for r in purposes
               if r["outcome"] == "none_apply"
               and not r.get("llm_reason") and not r.get("llm_refused")],
              [])


def test_columns_never_share_names(branches, purposes):
    with checks("the two columns never share a name (the collision this is here to stop)"):
        b_keys, p_keys = set(branches[0]), set(purposes[0])
        check("branches.jsonl has no purpose_field", "purpose_field" in b_keys, False)
        check("purpose_branches.jsonl has no technologiefeld",
              "technologiefeld" in p_keys, False)
        check("purpose_branches.jsonl has no website column", "website" in p_keys, False)
        check("purpose_branches.jsonl has no WZ code", "wz_code" in p_keys, False)
        check("`assigned` is column 1's outcome only",
              "assigned" in {r["outcome"] for r in branches}
              and "assigned" not in {r["outcome"] for r in purposes}, True)
        check("`assigned_keywords` is column 3's outcome only",
              "assigned_keywords" in {r["outcome"] for r in purposes}
              and "assigned_keywords" not in {r["outcome"] for r in branches}, True)


def test_same_taxonomy_version(branches, purposes):
    with checks("both columns were scored against the same taxonomy"):
        check("column 1 taxonomy", {r["taxonomy_version"] for r in branches}, {TAXONOMY_VERSION})
        check("column 3 taxonomy", {r["taxonomy_version"] for r in purposes}, {TAXONOMY_VERSION})


def test_column1_not_stale(runner):
    with checks("column 1 is not stale against the run that feeds it"):
        # classify reads results.jsonl through the pipeline's own selector. If run has since found
        # more websites, every branch bucket above is describing a population that no longer exists.
        live = sum(1 for r in runner.latest_rows(_out_dir() / "results.jsonl") if r.get("website"))
        check("results.jsonl websites == branches.jsonl websites", live, BRANCH_WITH_WEBSITE)


def test_labels_recognised(runner):
    with checks("every hand label is a value the reader recognises"):
        for name in LABEL_FILES:
            path = _out_dir() / name
            if not path.exists():
                print(f"     skipped {name}: not written yet")
                continue
            labelled = [(r.get("label") or "").strip().lower()
                        for r in runner.read_label_csv(path)]
            unknown = sorted({v for v in labelled if v and v not in runner.LABEL_VALUES})
            check(f"{name}: no unrecognised label", unknown, [])
            print(f"     {name}: {sum(1 for v in labelled if v in runner.LABEL_VALUES)} "
                  f"of {len(labelled)} rows labelled")


#: Definition order, which is the order both run modes report in. pytest walks the module in this
#: order anyway; the fallback runner would otherwise sort alphabetically and print a different one.
_TESTS = (
    test_one_row_per_company,
    test_column1_outcomes,
    test_column3_outcomes,
    test_columns_never_share_names,
    test_same_taxonomy_version,
    test_column1_not_stale,
    test_labels_recognised,
)


def _run_without_pytest() -> int:
    """Run every test with its fixtures resolved from `_LOADERS`. No pytest required."""
    print(f"reading {_out_dir()}")
    for test in _TESTS:
        kwargs = {name: _LOADERS[name]() for name in inspect.signature(test).parameters}
        try:
            test(**kwargs)
        except AssertionError:
            pass        # `check` has already recorded and printed every line of it
    print(f"\n{'FAILED' if FAILURES else 'all checks passed'}")
    for failure in FAILURES:
        print(f"  - {failure}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    if pytest is None:
        raise SystemExit(_run_without_pytest())
    # `-s` so the ok/FAIL table streams instead of being captured: the PostToolUse hook reads
    # stdout and keeps the lines starting with FAIL.
    raise SystemExit(pytest.main([__file__, "-q", "-s"]))
