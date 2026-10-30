#!/usr/bin/env python3
"""Guard the classification WRITE path: what reaches `company_field_observations`.

Free. No API, no model, no database, no network, and it runs in under a second — which is what lets
the `PostToolUse` hook run it after every edit under `combined_discovery/`, `scripts/` or `tests/`.

The other five guards check what the classifiers DECIDE. This one checks what gets STORED, which is
a different set of mistakes with a different blast radius: a wrong decision is one bad row in a file,
a wrong write is a wrong column in `v_companies_enriched` for thousands of companies.

Four things, each with the cost that earns it:

1. **A field name the upsert key does not cover.** The partial upsert index and
   `insert_text_observation`'s `ON CONFLICT ... WHERE` clause list the same ten names, and Postgres
   infers the partial index FROM that predicate — so a name in one and not the other cannot be
   upserted at all. It does not error; the second run just appends a duplicate, and two migrations
   before the baseline squash existed only to clean that up. This compares all three lists.

2. **A confidence policy value at or below the resolver's bar.** `model_direct` sat at exactly
   `_FIELD_MIN_CONF = 0.50` and promoted only because the filter reads `>=`. Three ordinary edits
   turn that into 4,095 fields vanishing with no error anywhere. It is now 0.52, and this fails if
   the margin is ever lost again — including by somebody raising `_FIELD_MIN_CONF` for one of the
   five OTHER logical fields that share it.

3. **A `_withheld` name reaching a resolver.** `technology_field_withheld` and
   `purpose_field_withheld` hold the 552 fields the website rule and the length floor refused to
   publish. They are unpublishable because NO `resolve_*` function names them — the `directory_url`
   trick. One entry in one `obs_field_names` array publishes all of them.

4. **The assigned-outcome list restated instead of imported.** Every unassigned row carries the
   literal string `"NONE"` as its value — 23,778 rows across the two files. A filter written against
   the value rather than the outcome writes all of them and hands the resolver a canonical industry
   field of `'NONE'`. The emitter must IMPORT `ASSIGNED_OUTCOMES`; a local copy silently stops
   tracking `purpose_branch` the day an outcome is added, and one was added on 2026-09-10.

**Asserted on the syntax tree, never on the source text.** All four field names are discussed at
length in the docstrings of every file this reads, so a substring search finds the prose and passes
on a violation that is really there — or fails on one that is not. That trap has caught this repo
twice, most memorably finding `Handel` inside `Handelsregister`.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

for candidate in (
    Path(__file__).resolve().parent.parent / "airflow" / "dags",
    Path("/opt/airflow/dags"),
):
    if candidate.is_dir():
        DAGS = Path(candidate)
        sys.path.insert(0, str(candidate))
        break
else:  # pragma: no cover - the repo layout would have to be broken
    raise SystemExit("cannot find airflow/dags")

REPO = Path(__file__).resolve().parent.parent

from enrichment import branch_observations as bo  # noqa: E402
from enrichment import purpose_branch as pb  # noqa: E402
from pipeline_coordinator.resolver import _FIELD_MIN_CONF, REGISTER_FIELDS  # noqa: E402

FAILURES: list[str] = []

#: The four names this source is allowed to write, and the two of them that must never be resolvable.
CLASSIFICATION_FIELDS = (
    "technology_field", "purpose_field", "technology_field_withheld", "purpose_field_withheld",
)
UNRESOLVABLE_FIELDS = ("technology_field_withheld", "purpose_field_withheld")


def check(label: str, got: object, want: object) -> None:
    ok = got == want
    if not ok:
        FAILURES.append(label)
    print(f"{'ok  ' if ok else 'FAIL'} {label:<62} got={got!r:<28} want={want!r}")


def section(title: str) -> None:
    print(f"\n-- {title} --")


def _string_constants(node: ast.AST) -> set[str]:
    """Every string literal anywhere under a node. The tree's answer to "which names appear here"."""
    return {
        n.value for n in ast.walk(node)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    }


def _on_conflict_names() -> set[str]:
    """The names in `insert_text_observation`'s ON CONFLICT predicate, from the syntax tree.

    Read from the tree rather than by importing: `source_enrichment_common` imports psycopg2 at
    module level, and this guard is meant to cost nothing and need nothing.
    """
    tree = ast.parse((DAGS / "source_enrichment_common.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "insert_text_observation":
            for inner in ast.walk(node):
                if isinstance(inner, ast.Constant) and isinstance(inner.value, str):
                    if "ON CONFLICT" in inner.value and "field_name IN" in inner.value:
                        after = inner.value.split("field_name IN", 1)[1]
                        block = after[: after.index(")")]
                        return {p.strip() for p in block.replace("\n", " ").split("'")
                                if p.strip() and p.strip() not in {"(", ",", "( ", ",  "}
                                and "," not in p.strip()}
    return set()


def main() -> int:
    print("== the classification write path ==")

    # -----------------------------------------------------------------------------------
    section("1. every field name the emitter can write is covered by the upsert key")
    # What the emitter is ABLE to emit, from its own tree: the string constants passed as the
    # `field_name` position of every tuple `planned_writes` appends. Reading the function rather
    # than calling it means no file and no data are needed.
    emitter_tree = ast.parse(Path(bo.__file__).read_text(encoding="utf-8"))
    planned = next(
        (n for n in ast.walk(emitter_tree)
         if isinstance(n, ast.FunctionDef) and n.name == "planned_writes"), None,
    )
    check("planned_writes exists", planned is not None, True)
    emitted = _string_constants(planned) & set(CLASSIFICATION_FIELDS) if planned else set()
    check("the emitter can write exactly the four names",
          sorted(emitted), sorted(CLASSIFICATION_FIELDS))

    # The newest migration that (re)creates the upsert index holds the predicate in force. Found by
    # content, not by file name, so a squash or a later widening cannot leave this guard reading a
    # stale file.
    index_sql = "UNIQUE INDEX uq_obs_company_field_source_value"
    defining = [m for m in sorted((REPO / "db" / "migrations").glob("V*__*.sql"),
                                  key=lambda m: int(m.name[1:].split("__")[0]))
                if index_sql in m.read_text(encoding="utf-8")]
    check("a migration creates the observation upsert index", bool(defining), True)
    sql_text = defining[-1].read_text(encoding="utf-8") if defining else index_sql + " ;"
    index_block = sql_text[sql_text.index(index_sql):]
    index_block = index_block[: index_block.index(";")]
    index_names = set(re.findall(r"'([a-z_]+)'", index_block))
    for field in CLASSIFICATION_FIELDS:
        check(f"the upsert index predicate covers {field}", field in index_names, True)
    # A register import re-delivers every value; without the key each run would append a copy.
    check("the upsert index predicate covers every register field",
          sorted(set(REGISTER_FIELDS) - index_names), [])

    conflict_names = _on_conflict_names()
    for field in CLASSIFICATION_FIELDS:
        check(f"insert_text_observation's ON CONFLICT covers {field}",
              field in conflict_names, True)
    # The two lists are the same predicate written twice, and Postgres infers the partial index from
    # the second one. A name in one and not the other cannot be upserted at all.
    check("the index predicate and the ON CONFLICT clause list the same names",
          sorted(index_names), sorted(conflict_names))

    # -----------------------------------------------------------------------------------
    section("2. every confidence the policy can produce is strictly above the resolver's bar")
    check("the lowest policy value is above _FIELD_MIN_CONF",
          min(bo._CONFIDENCE.values()) > _FIELD_MIN_CONF, True)
    check("...and the emitter asserts it at import time",
          any(isinstance(n, ast.Assert) for n in ast.walk(emitter_tree)), True)
    check("_FIELD_MIN_CONF is imported, not retyped",
          any(isinstance(n, ast.ImportFrom) and n.module and "resolver" in n.module
              and any(a.name == "_FIELD_MIN_CONF" for a in n.names)
              for n in ast.walk(emitter_tree)), True)
    # The ordering the policy encodes: most evidence to least. A keyword row is traceable to a word
    # literally in the text; a model_direct row chose from all 77 with nothing anchoring it.
    check("keywords > model_tiebreak > model_direct",
          (bo._CONFIDENCE[pb.DECIDED_BY_KEYWORDS]
           > bo._CONFIDENCE[pb.DECIDED_BY_MODEL_TIEBREAK]
           > bo._CONFIDENCE[pb.DECIDED_BY_MODEL_DIRECT]), True)

    # -----------------------------------------------------------------------------------
    section("3. no resolve_* function names a withheld field")
    resolver_tree = ast.parse(
        (DAGS / "pipeline_coordinator" / "resolver.py").read_text(encoding="utf-8")
    )
    offenders: list[str] = []
    resolvers: list[str] = []
    for node in ast.walk(resolver_tree):
        if not (isinstance(node, ast.FunctionDef) and node.name.startswith("resolve_")):
            continue
        resolvers.append(node.name)
        # Only the `obs_field_names=[...]` keyword is a publication decision. A withheld name
        # mentioned in a docstring is this guard's whole reason for walking the tree.
        for call in ast.walk(node):
            if not isinstance(call, ast.Call):
                continue
            for kw in call.keywords:
                if kw.arg != "obs_field_names":
                    continue
                for name in _string_constants(kw.value) & set(UNRESOLVABLE_FIELDS):
                    offenders.append(f"{node.name} -> {name}")
    check("no resolve_* publishes a withheld field", offenders, [])
    check("the two new resolvers exist",
          sorted(n for n in resolvers if n in
                 ("resolve_technology_field", "resolve_purpose_field")),
          ["resolve_purpose_field", "resolve_technology_field"])

    # -----------------------------------------------------------------------------------
    section("4. the assigned-outcome list is imported, not restated")
    check("the emitter imports ASSIGNED_OUTCOMES from purpose_branch",
          any(isinstance(n, ast.ImportFrom) and n.module
              and n.module.endswith("purpose_branch")
              and any(a.name == "ASSIGNED_OUTCOMES" for a in n.names)
              for n in ast.walk(emitter_tree)), True)
    # A local copy of the three names would stop tracking purpose_branch silently. One outcome was
    # added on 2026-09-10 (`unsure`), which is exactly when a stale copy would have started lying.
    restated = _string_constants(emitter_tree) & (pb.ALL_OUTCOMES - {bo.BRANCH_ASSIGNED})
    check("no column-3 outcome name is spelled out in the emitter", sorted(restated), [])
    # The non-answers must stay out. `none_apply` is a model's real answer and that answer is NONE.
    check("none_apply is not an assigned outcome", "none_apply" in pb.ASSIGNED_OUTCOMES, False)
    check("unsure is not an assigned outcome", "unsure" in pb.ASSIGNED_OUTCOMES, False)

    print(f"\n{'FAILED' if FAILURES else 'all branch-observation checks passed'}")
    for label in FAILURES:
        print(f"  - {label}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
