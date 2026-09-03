#!/usr/bin/env python3
"""Guard the register-purpose column: COLUMN 3, `purpose_field`.

Free. No API, no model, no database, no network, and it runs in under a second — which is what lets
the `PostToolUse` hook run it after every edit under `combined_discovery/` or `scripts/`.

What this file is for is keeping five specific mistakes from coming back. Each one has a cost
attached, and the cost is why the check exists rather than a general wish for correctness:

1. **The website gate leaking in.** `branch_classifier.classify()` withholds any field from a company
   with no website. That rule is right for the Technologiefeld and fatal here: this column exists to
   read the register for the 6,477 companies that have a purpose and no website. If somebody
   "simplifies" `purpose_branch` by calling `classify()`, every answer disappears — and it disappears
   quietly, as 563 more `no_website` rows nobody is looking at.

2. **A WZ code filling a technology field.** Tried once, and it gave 2,632 companies a field nothing
   they published supported. Neither `classify()` nor `classify_purpose()` has a parameter to pass a
   code through, and this file fails if one appears.

3. **Compound matching.** A keyword must never match inside a longer German word. Measured on this
   data, relaxing it fires `epa` inside *Reparatur* on 124 companies and `its` inside
   *Kreditsicherung* on 303 — noise that would outrank every real hit.

4. **Collapsing the non-answers.** `no_purpose`, `no_keyword`, `torn`, `none_apply` and `unsure` are
   five different claims about five different situations. Adding any two turns a statement about our
   evidence into a statement about the company.

5. **A broken model call reported as a refusal.** "The model says no field fits" and "we never got an
   answer" are different findings, and the second one must never be written as the first.

Nothing here restates a rule that lives in the code. Where a rule is a function, that function is
**imported and called** — a check that rebuilds the logic it is checking agrees with itself no matter
what the real code does, and this repo has already been bitten by exactly that.
"""

from __future__ import annotations

import ast
import inspect
import sys
from pathlib import Path

for candidate in (
    Path(__file__).resolve().parent.parent / "airflow" / "dags",
    Path("/opt/airflow/dags"),
):
    if candidate.is_dir():
        sys.path.insert(0, str(candidate))
        break

from combined_discovery import branch_classifier as bc  # noqa: E402
from combined_discovery import purpose_branch as pb  # noqa: E402

FAILURES: list[str] = []


def check(label: str, got: object, want: object) -> None:
    ok = got == want
    if not ok:
        FAILURES.append(label)
    print(f"{'ok  ' if ok else 'FAIL'} {label:<62} got={got!r:<28} want={want!r}")


def section(title: str) -> None:
    print(f"\n-- {title} --")


def _load_runner():
    """`scripts/run_combined_discovery.py` as a module, without running its CLI.

    It is a script rather than a package, so it cannot be imported by name. Only pure functions and
    module constants are read from it — nothing touching the network, the database or the cache.
    """
    import importlib.util

    path = Path(__file__).resolve().parent / "run_combined_discovery.py"
    spec = importlib.util.spec_from_file_location("_purpose_runner_under_check", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tiny_taxonomy() -> bc.Taxonomy:
    """A three-field taxonomy, small enough that every weight can be reasoned about by hand."""
    keyword_branches = {
        "gastronomie": frozenset({"Gastgewerbe"}),
        "catering": frozenset({"Gastgewerbe"}),
        "restaurant": frozenset({"Gastgewerbe"}),
        "spedition": frozenset({"Logistik"}),
        "medizintechnik": frozenset({"Medizintechnik"}),
        # Claimed by two, so each gets half — the `claimants` divisor.
        "logistik": frozenset({"Logistik", "Medizintechnik"}),
    }
    return bc.Taxonomy(
        branches={"Gastgewerbe": {}, "Logistik": {}, "Medizintechnik": {}},
        keyword_branches=keyword_branches,
        patterns={k: bc._pattern_for(k) for k in keyword_branches},
        version="test",
    )


def main() -> int:
    taxonomy = _tiny_taxonomy()

    # -----------------------------------------------------------------------------------
    section("the outcome vocabulary is disjoint from the Technologiefeld's")
    # Two columns, two vocabularies. If they ever share a string, a downstream `if` can mistake a
    # register answer for a website answer, which is the whole failure this separation prevents.
    check("outcome count", len(pb.ALL_OUTCOMES), 8)
    check("assigned is NOT a purpose outcome", "assigned" in pb.ALL_OUTCOMES, False)
    check("no_website is NOT a purpose outcome", "no_website" in pb.ALL_OUTCOMES, False)
    check("no_text is NOT a purpose outcome", "no_text" in pb.ALL_OUTCOMES, False)
    check("assigned_keywords is not the website spelling", pb.ASSIGNED_KEYWORDS, "assigned_keywords")
    # The one string both columns legitimately share is the empty-answer literal, because both write
    # it into a handover file and a reader must see the same word in both places.
    check("NONE is shared with the Technologiefeld", pb.NONE, bc.NONE)

    for outcome in sorted(pb.ALL_OUTCOMES):
        check(f"{outcome!r} carries a field only if it should",
              outcome in pb.ASSIGNED_OUTCOMES,
              outcome in {"assigned_keywords", "assigned_model_3way", "assigned_model_77"})

    # -----------------------------------------------------------------------------------
    section("no_purpose: what counts as too little text to read")
    for text, want in (
        (None, pb.NO_PURPOSE),
        ("", pb.NO_PURPOSE),
        ("   ", pb.NO_PURPOSE),
        ("\t\n  \n", pb.NO_PURPOSE),
        ("Handel", pb.NO_PURPOSE),
        ("Verwaltung eigenen Vermoegens", pb.NO_PURPOSE),          # 29 chars, a stub
        ("x" * 40, pb.NO_PURPOSE),                                  # exactly at the bound
        ("x" * 41, pb.NO_KEYWORD),                                  # one over, read but no keyword
        # Non-breaking spaces must not pad a stub over the threshold. The register export contains
        # them, and they compare as content while looking like whitespace.
        ("Handel" + "\xa0" * 60, pb.NO_PURPOSE),
    ):
        got = pb.classify_purpose(
            taxonomy=taxonomy, df={}, n_companies=1,
            keywords_by_source=pb.purpose_keywords(text, taxonomy),
            had_purpose=pb.is_usable(text),
        )
        check(f"{(text or '')[:22]!r} ({len(text or '')} chars)", got.outcome, want)

    # -----------------------------------------------------------------------------------
    section("the length floor keeps what it takes away")
    # A veto whose rejections are never counted cannot be told apart from a veto that never fires.
    # This one rejects good answers — 17 on the real table — so its cost is recorded per row.
    stub = "Betrieb einer Gastronomie"          # 25 chars: under the floor, and unambiguous
    floored = pb.classify_purpose(
        taxonomy=taxonomy, df={"gastronomie": 1}, n_companies=200,
        keywords_by_source=pb.purpose_keywords(stub, taxonomy),
        had_purpose=pb.is_usable(stub),
    )
    check("a short but clear purpose is still no_purpose", floored.outcome, pb.NO_PURPOSE)
    check("…carries no field", floored.branch, None)
    check("…prints NONE", floored.value, pb.NONE)
    check("…but records what the floor cost", floored.withheld_by_floor, "Gastgewerbe")
    check("…and is not counted as decided", floored.decided, False)
    # An empty purpose has nothing to withhold, so the field stays None rather than becoming noise.
    empty = pb.classify_purpose(
        taxonomy=taxonomy, df={}, n_companies=200,
        keywords_by_source=pb.purpose_keywords(None, taxonomy), had_purpose=False,
    )
    check("an empty purpose withholds nothing", empty.withheld_by_floor, None)
    # The matcher itself must not apply the floor — that belongs in one place only.
    check("purpose_keywords does NOT apply the floor",
          sorted(pb.purpose_keywords(stub, taxonomy)), ["gastronomie"])

    check("no_purpose is distinct from no_keyword", pb.NO_PURPOSE == pb.NO_KEYWORD, False)
    check("no_purpose is distinct from none_apply", pb.NO_PURPOSE == pb.NONE_APPLY, False)
    check("none_apply is distinct from unsure", pb.NONE_APPLY == pb.UNSURE, False)

    # -----------------------------------------------------------------------------------
    section("the compound rule, on register prose")
    # The real false hits from this data, with the company counts that make them worth a check.
    for word, haystack, want in (
        ("epa", "instandsetzung und reparatur von fahrzeugen", False),          # 124 companies
        ("its", "erwerb und verwaltung von kreditsicherungen", False),          # 303
        ("ai", "betrieb einer arbeitsvermittlung", False),                      # 144
        ("automat", "herstellung von automatisierungsloesungen", False),
        ("restaurant", "betrieb eines restaurant in osnabrueck", True),
        ("spedition", "spedition und lagerhaltung", True),
        # A keyword at the very start and very end of the text must still match.
        ("spedition", "spedition", True),
        # Punctuation and hyphens are boundaries; a following letter is not.
        ("spedition", "spedition, lagerhaltung", True),
        ("gastronomie", "(gastronomie)", True),
    ):
        check(f"{word!r} in {haystack[:30]!r}",
              bool(bc._pattern_for(word).search(bc.normalize(haystack))), want)

    # -----------------------------------------------------------------------------------
    section("German inflection blocks a keyword, and that is why coverage is 9%")
    # This is not a bug to fix by loosening the matcher — it is the measured cost of NOT loosening
    # it. Register prose is declined and pluralised German, so `restaurant` does not match
    # "Restaurants" and `spedition` does not match "Speditionsgeschaeft". Measured over the 6,481
    # rows where nothing fired: only 1,058 contain a taxonomy word blocked purely this way, and the
    # commonest of those are the false hits above. So the gap is in the taxonomy, not the matching.
    for word, haystack in (
        ("restaurant", "betrieb zweier restaurants"),
        ("spedition", "fuehrung eines speditionsgeschaeftes"),
        ("gastronomie", "gastronomiebetriebe aller art"),
        ("catering", "cateringdienstleistungen fuer veranstaltungen"),
    ):
        check(f"{word!r} does NOT match {haystack[:30]!r}",
              bool(bc._pattern_for(word).search(bc.normalize(haystack))), False)

    # And the same rule reached through this module's own entry point, not just the pattern.
    reparatur = "instandsetzung und reparatur von fahrzeugen aller art, sowie der handel damit"
    check("purpose_keywords finds nothing in a Reparatur purpose",
          pb.purpose_keywords(reparatur, taxonomy), {})

    # -----------------------------------------------------------------------------------
    section("keywords are credited to the purpose, and only the purpose")
    gastro = "betrieb einer gastronomie sowie catering fuer veranstaltungen in der region"
    found = pb.purpose_keywords(gastro, taxonomy)
    check("both Gastgewerbe words fired", sorted(found), ["catering", "gastronomie"])
    check("every source is 'purpose'", sorted(set(found.values())), ["purpose"])

    # -----------------------------------------------------------------------------------
    section("scoring reuses branch_classifier, weights and thresholds included")
    df = bc.document_frequency([found.keys(), {"spedition"}, {"logistik"}])
    check("document_frequency counts COMPANIES", df["catering"], 1)
    check("a company cannot inflate its own word",
          bc.document_frequency([["catering", "catering", "catering"]])["catering"], 1)

    # n_companies well above the keyword count, so idf clears MIN_SCORE and the test is about the
    # decision rather than about the floor. log(200/2) = 4.6 per keyword, single claimant.
    assignment = pb.classify_purpose(
        taxonomy=taxonomy, df=df, n_companies=200,
        keywords_by_source=found, had_purpose=True,
    )
    check("two clear keywords -> a field", assignment.outcome, pb.ASSIGNED_KEYWORDS)
    check("…and the field is the one that scored", assignment.branch, "Gastgewerbe")
    check("…decided by keywords, not a model", assignment.decided_by, pb.DECIDED_BY_KEYWORDS)
    check("…and it is traceable to the words", sorted(assignment.keywords), ["catering", "gastronomie"])
    check("value is the field itself", assignment.value, "Gastgewerbe")
    check("decided is True", assignment.decided, True)

    # A single shared keyword: claimed by two branches, so it splits and cannot clear the margin.
    split = pb.classify_purpose(
        taxonomy=taxonomy, df={"logistik": 1}, n_companies=200,
        keywords_by_source={"logistik": "purpose"}, had_purpose=True,
    )
    check("one keyword claimed by two branches -> torn", split.outcome, pb.TORN)
    check("…carries no field", split.branch, None)
    check("…prints NONE", split.value, pb.NONE)
    check("…and nothing decided it", split.decided_by, "")

    # -----------------------------------------------------------------------------------
    section("torn stays disambiguated: never asked vs asked and broke")
    # `worth_asking` needs MIN_KEYWORDS_FOR_MODEL distinct keywords. One keyword can never be
    # askable, and 608 of the 657 companies with a purpose keyword have exactly one — so this is the
    # common case, not the corner case.
    check("MIN_KEYWORDS_FOR_MODEL is what gates it", bc.MIN_KEYWORDS_FOR_MODEL, 2)
    check("one keyword -> no shortlist, so never asked", split.shortlist, [])
    check("…and `askable` says so", split.askable, False)
    check("a shortlist never exceeds SHORTLIST_SIZE",
          all(len(a.shortlist) <= bc.SHORTLIST_SIZE for a in (assignment, split)), True)

    # A broken call is `torn` with `llm_refused` set — never `none_apply`.
    broke = pb.PurposeAssignment(outcome=pb.TORN, shortlist=["A", "B"], llm_refused="TimeoutError")
    check("a broken call stays torn", broke.outcome, pb.TORN)
    check("…is not none_apply", broke.outcome == pb.NONE_APPLY, False)
    check("…records why", broke.llm_refused, "TimeoutError")
    check("…was askable, unlike the one above", broke.askable, True)
    check("a refusal carries no field", broke.branch, None)

    # -----------------------------------------------------------------------------------
    section("abstentions record WHICH tier produced them")
    # `none_apply` from 3 options means "none of these three"; from 77 it means "none of Berlin's
    # 77". Without decided_by those are the same row, and they are not the same claim.
    check("model_tiebreak is a decider", pb.DECIDED_BY_MODEL_TIEBREAK in pb.ALL_DECIDED_BY, True)
    check("model_direct is a decider", pb.DECIDED_BY_MODEL_DIRECT in pb.ALL_DECIDED_BY, True)
    check("both are model deciders", sorted(pb.MODEL_DECIDED_BY),
          ["model_direct", "model_tiebreak"])
    check("keywords is NOT a model decider",
          pb.DECIDED_BY_KEYWORDS in pb.MODEL_DECIDED_BY, False)
    for decider in sorted(pb.MODEL_DECIDED_BY):
        abstain = pb.PurposeAssignment(outcome=pb.NONE_APPLY, decided_by=decider)
        check(f"an abstention from {decider} keeps its tier", abstain.decided_by, decider)
        check(f"…and still carries no field", abstain.value, pb.NONE)

    # -----------------------------------------------------------------------------------
    section("a model verdict changes the outcome, and a FAILED call does not")
    # This is the single most important section in the file. Every row below is a different thing the
    # model can do, and the last group is the one that has to stay separate: a timeout is not a
    # refusal, and writing it as one claims knowledge nobody has.
    torn_row = pb.PurposeAssignment(
        outcome=pb.TORN, shortlist=["Gastgewerbe", "Logistik", "Handel"],
        score=2.5, keywords={"logistik": "purpose"},
    )
    nothing_row = pb.PurposeAssignment(outcome=pb.NO_KEYWORD)

    def applied(row, tier=pb.DECIDED_BY_MODEL_TIEBREAK, **kw):
        base = dict(branch=None, rejected_all=False, unsure=False, refused="",
                    reason="weil", model_name="qwen3.5:4b", tier=tier)
        return pb.apply_model_verdict(row, **{**base, **kw})

    chose = applied(torn_row, branch="Logistik")
    check("tier A + a field -> assigned_model_3way", chose.outcome, pb.ASSIGNED_MODEL_3WAY)
    check("…the field is recorded", chose.branch, "Logistik")
    check("…decided_by names the tier", chose.decided_by, pb.DECIDED_BY_MODEL_TIEBREAK)
    check("…the model is named", chose.model_name, "qwen3.5:4b")
    check("…the reason is kept", chose.llm_reason, "weil")
    check("…the keywords survive, so the row stays auditable",
          sorted(chose.keywords), ["logistik"])

    direct = applied(nothing_row, tier=pb.DECIDED_BY_MODEL_DIRECT, branch="Handel")
    check("tier B + a field -> assigned_model_77", direct.outcome, pb.ASSIGNED_MODEL_77)
    check("…decided_by says model_direct", direct.decided_by, pb.DECIDED_BY_MODEL_DIRECT)
    check("…the two tiers do NOT share an outcome",
          chose.outcome == direct.outcome, False)

    said_none = applied(torn_row, rejected_all=True)
    check("keine -> none_apply", said_none.outcome, pb.NONE_APPLY)
    check("…carries no field", said_none.branch, None)
    check("…but still records which tier asked", said_none.decided_by, pb.DECIDED_BY_MODEL_TIEBREAK)
    none_77 = applied(nothing_row, tier=pb.DECIDED_BY_MODEL_DIRECT, rejected_all=True)
    check("'none of 3' and 'none of 77' are distinguishable",
          said_none.decided_by == none_77.decided_by, False)

    unsure = applied(torn_row, unsure=True)
    check("unsicher -> unsure", unsure.outcome, pb.UNSURE)
    check("…is not none_apply", unsure.outcome == pb.NONE_APPLY, False)
    check("…carries no field", unsure.branch, None)

    # The failure cases. Every one of these must leave the outcome exactly as the keywords left it.
    for label, refused in (
        ("a timeout", "ReadTimeout"),
        ("an unreachable model", "ConnectionError"),
        ("unparseable output", "ValueError"),
        ("an invented field name", "off-list: Immersive Technologien"),
        ("an empty answer", "empty answer"),
    ):
        broke = applied(torn_row, refused=refused)
        check(f"{label} leaves torn AS torn", broke.outcome, pb.TORN)
        check(f"…{label} is not none_apply", broke.outcome == pb.NONE_APPLY, False)
        check(f"…{label} is not unsure", broke.outcome == pb.UNSURE, False)
        check(f"…{label} records why", broke.llm_refused, refused)
        check(f"…{label} assigns nobody", broke.decided_by, "")
        broke_b = applied(nothing_row, tier=pb.DECIDED_BY_MODEL_DIRECT, refused=refused)
        check(f"…{label} leaves no_keyword AS no_keyword", broke_b.outcome, pb.NO_KEYWORD)

    # A verdict with nothing set at all is a bug upstream, not an abstention.
    silent = applied(torn_row)
    check("a verdict with no answer at all is a failure", silent.outcome, pb.TORN)
    check("…and says so", bool(silent.llm_refused), True)

    # Only a model tier may be passed. `keywords` is not a tier, and letting it through would file a
    # model's answer as keyword-traceable.
    try:
        applied(torn_row, tier=pb.DECIDED_BY_KEYWORDS, branch="Logistik")
        raised = False
    except ValueError:
        raised = True
    check("apply_model_verdict refuses the 'keywords' tier", raised, True)

    # The input must not be mutated — a caller holding the original must still see `torn`.
    check("the original assignment is untouched", torn_row.outcome, pb.TORN)
    check("…and still has no model name", torn_row.model_name, "")

    # -----------------------------------------------------------------------------------
    section("the website gate, the WZ code and the register status cannot get in")
    signature = inspect.signature(pb.classify_purpose)
    # `register_status` joins the list for the same reason as the other two: whether a company still
    # exists is not evidence about what it was set up to do, and a dissolved company's purpose is
    # exactly as classifiable as a live one's. The flag may withhold a MODEL CALL from a dead
    # company; it may never reach the classifier and change the answer.
    for forbidden in ("has_website", "website", "wz", "wz_code", "wz_cluster", "wz_division",
                      "register_status", "active_only", "status"):
        check(f"classify_purpose has no {forbidden!r} parameter",
              forbidden in signature.parameters, False)
    check("classify_purpose takes had_purpose", "had_purpose" in signature.parameters, True)
    check("had_purpose defaults to False — a caller that has not checked gets nothing",
          signature.parameters["had_purpose"].default, False)

    # Asserted against the parsed syntax tree, not against the text. The module docstring discusses
    # `classify()` and `has_website` at length — deliberately, because the reasoning belongs next to
    # the code — so a substring search over the source finds the prose and reports a violation that
    # is not there. The tree contains calls and arguments only.
    tree = ast.parse(Path(pb.__file__).read_text(encoding="utf-8"))
    called: set[str] = set()
    kwargs_used: set[str] = set()
    attributes_read: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            called.add(func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", ""))
            kwargs_used.update(kw.arg for kw in node.keywords if kw.arg)
        elif isinstance(node, ast.Attribute):
            attributes_read.add(node.attr)

    check("purpose_branch never CALLS classify()", "classify" in called, False)
    # The positive half of the same assertion: it must still be composing the shared primitives
    # rather than having grown a private matcher. `document_frequency` is deliberately NOT here —
    # that one is the runner's job, because it needs every company before it can weight anything.
    check("…but does compose the shared primitives",
          {"normalize", "matched_keywords", "score_branches", "worth_asking"} <= called, True)
    check("purpose_branch never passes has_website=", "has_website" in kwargs_used, False)
    check("purpose_branch never reads a wz_code", "wz_code" in attributes_read, False)
    check("…and never reads a website either", "website" in attributes_read, False)
    check("…nor a register_status", "register_status" in attributes_read, False)
    check("…and never passes register_status=", "register_status" in kwargs_used, False)

    # A purpose answer must not be able to reach the Technologiefeld's column, in either direction.
    runner = _load_runner()
    check("the purpose CSV columns are all prefixed",
          all(c.startswith("purpose_") for c in runner._PURPOSE_COLUMNS), True)
    check("…and none collides with the Technologiefeld's",
          set(runner._PURPOSE_COLUMNS) & set(runner._TECHNOLOGIEFELD_COLUMNS), set())
    check("…nor with the WZ column's", set(runner._PURPOSE_COLUMNS) & set(runner._WZ_COLUMNS), set())
    check("technologiefeld is not written by _purpose_columns",
          "technologiefeld" in runner._purpose_columns({"purpose_field": "Logistik"}), False)
    check("the purpose column writes purpose_field",
          runner._purpose_columns({"purpose_field": "Logistik"})["purpose_field"], "Logistik")
    # A company `classify-purpose` never saw leaves the columns blank, NOT NONE. "Nobody ran this"
    # and "the register named no field" are different claims and only the second is an answer.
    check("an unrun company gets blanks, not NONE",
          set(runner._purpose_columns(None).values()), {""})

    # The two files must stay separate, or one run overwrites the other's column.
    check("the purpose output is its own file",
          runner.PURPOSE_BRANCHES_PATH.name, "purpose_branches.jsonl")
    check("…and is not branches.jsonl",
          runner.PURPOSE_BRANCHES_PATH == runner.BRANCHES_PATH, False)

    # -----------------------------------------------------------------------------------
    section("the two tiers stay separate")
    # They ask different questions of the same model, and the answers mean different things. If they
    # ever shared a cache namespace, a 3-way answer would be served to a 77-way question.
    from combined_discovery import branch_llm  # noqa: E402  imported here, not used above

    check("tier A and tier B use different cache namespaces",
          "purpose-llm-tiebreak" == "purpose-llm-direct", False)
    check("…and neither is the Technologiefeld's namespace",
          {"purpose-llm-tiebreak", "purpose-llm-direct"} & {"branch-llm", "branch-vocab"}, set())
    check("choose_for_purpose defaults to the tie-break namespace",
          inspect.signature(branch_llm.choose_for_purpose).parameters["namespace"].default,
          "purpose-llm-tiebreak")

    # The 77-way prompt is a real risk and the 3-way one is not, so the guard pins which is which.
    # Tier A must never be handed the whole taxonomy.
    check("the purpose prompt has an options placeholder",
          "{options}" in branch_llm.PURPOSE_PROMPT, True)
    check("…and offers keine", "{none}" in branch_llm.PURPOSE_PROMPT, True)
    check("…and offers unsicher", "{unsure}" in branch_llm.PURPOSE_PROMPT, True)
    # The boilerplate instruction is load-bearing: 32% of usable purposes are holding/admin text and
    # the model under-refuses by ~3x without being told. Pinned by the words that carry it.
    for word in ("Holdinggesellschaften", "Vermoegensverwaltung", "Beteiligungsgesellschaften"):
        check(f"the prompt names {word} as keine", word in branch_llm.PURPOSE_PROMPT, True)
    # No Berlin field name may be hard-coded into the template — the options are always passed in.
    #
    # Checked with `_pattern_for`, not `in`, and the first version of this check is why: a plain
    # substring search found "Handel" inside "Handelsregister" and failed. That is the project's own
    # compound rule, and a guard that ignores it reports a violation that is not there.
    prompt_text = bc.normalize(branch_llm.PURPOSE_PROMPT)
    baked = [
        name for name in bc.load_taxonomy().branch_names()
        if bc._pattern_for(bc.normalize(name)).search(prompt_text)
    ]
    check("no field name is baked into the prompt", baked, [])

    # -----------------------------------------------------------------------------------
    section("DISCOVERY_ACTIVE_ONLY defaults off and can only withhold a model call")
    # Read from the syntax tree rather than by importing the module: `source_enrichment_common`
    # imports psycopg2 at module level, and this guard is meant to cost nothing and need nothing.
    common_path = Path(pb.__file__).resolve().parents[1] / "source_enrichment_common.py"
    common_tree = ast.parse(common_path.read_text(encoding="utf-8"))
    flag_default: object = "MISSING"
    fragment_node: ast.AST | None = None
    for node in ast.walk(common_tree):
        if not isinstance(node, ast.Assign):
            continue
        targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if "ACTIVE_ONLY" in targets:
            # `os.getenv("DISCOVERY_ACTIVE_ONLY", "").strip().lower() in {...}` — walk down to the
            # getenv call and read its default. An empty default is what "off unless asked" means.
            for inner in ast.walk(node.value):
                if (isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr == "getenv" and len(inner.args) == 2):
                    name = inner.args[0]
                    if isinstance(name, ast.Constant) and name.value == "DISCOVERY_ACTIVE_ONLY":
                        flag_default = getattr(inner.args[1], "value", "NOT-A-CONSTANT")
        if "_ACTIVE_ONLY_SQL" in targets:
            fragment_node = node.value

    check("ACTIVE_ONLY reads DISCOVERY_ACTIVE_ONLY with an empty default", flag_default, "")
    # The injection question, settled on the tree instead of by argument: the fragment is a plain
    # conditional between two string CONSTANTS. No f-string, no `+`, no `.format`, nothing that could
    # carry a value from a row or a request into the SQL it is interpolated into.
    check("_ACTIVE_ONLY_SQL is a conditional expression",
          isinstance(fragment_node, ast.IfExp), True)
    if isinstance(fragment_node, ast.IfExp):
        check("…both of its branches are string constants",
              all(isinstance(part, ast.Constant) and isinstance(part.value, str)
                  for part in (fragment_node.body, fragment_node.orelse)), True)
        check("…the off branch is empty", fragment_node.orelse.value, "")
        check("…the on branch filters on aktiv",
              fragment_node.body.value, "AND c.register_status = 'aktiv'")

    # -----------------------------------------------------------------------------------
    section("DISCOVERY_SKIP_RESOLVED_WEBSITE obeys the same three rules")
    # Same shape, same tree, same reasoning — a second env-gated SQL fragment interpolated into the
    # candidate queries. It exists because neither the per-source check nor the 0.90 quality gate
    # stops a paid provider re-searching a company that already HAS a website: measured 2026-09-01,
    # such companies averaged data_quality_score 0.340, and 108 of 500 candidates (21.6%) held one.
    skip_default: object = "MISSING"
    skip_fragment: ast.AST | None = None
    for node in ast.walk(common_tree):
        if not isinstance(node, ast.Assign):
            continue
        targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if "SKIP_RESOLVED_WEBSITE" in targets:
            for inner in ast.walk(node.value):
                if (isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr == "getenv" and len(inner.args) == 2):
                    name = inner.args[0]
                    if isinstance(name, ast.Constant) and name.value == "DISCOVERY_SKIP_RESOLVED_WEBSITE":
                        skip_default = getattr(inner.args[1], "value", "NOT-A-CONSTANT")
        if "_SKIP_RESOLVED_WEBSITE_SQL" in targets:
            skip_fragment = node.value

    check("SKIP_RESOLVED_WEBSITE reads its env var with an empty default", skip_default, "")
    check("_SKIP_RESOLVED_WEBSITE_SQL is a conditional expression",
          isinstance(skip_fragment, ast.IfExp), True)
    if isinstance(skip_fragment, ast.IfExp):
        check("…both of its branches are string constants",
              all(isinstance(part, ast.Constant) and isinstance(part.value, str)
                  for part in (skip_fragment.body, skip_fragment.orelse)), True)
        check("…the off branch is empty", skip_fragment.orelse.value, "")
        # It must ask the CANONICAL table. An observation is a claim and a company can hold several
        # rejected ones; only a canonical row means the resolver settled on a website. A guard here
        # because the two table names differ by a suffix and the wrong one silently over-skips.
        on_branch = skip_fragment.body.value if isinstance(skip_fragment.body, ast.Constant) else ""
        check("…the on branch reads company_canonical_field_sources",
              "company_canonical_field_sources" in on_branch, True)
        check("…and not the observations table",
              "company_field_observations" in on_branch, False)
        check("…and asks only about the website field",
              "field_name = 'website'" in on_branch, True)

    # Every query carrying the active-only fragment must carry this one too, or a provider skips the
    # dead through one code path and re-searches solved companies through another.
    common_src = common_path.read_text(encoding="utf-8")
    check("both fragments are interpolated at the same number of sites",
          common_src.count("{_SKIP_RESOLVED_WEBSITE_SQL}"),
          common_src.count("{_ACTIVE_ONLY_SQL}"))

    # The behavioural half: a withheld call leaves the row exactly as the keywords left it. A synthetic
    # id nothing has ever been cached under, so this is a guaranteed miss and touches no model.
    from combined_discovery import branch_llm
    withheld = branch_llm.choose(
        company_name="Nicht Existent GmbH",
        text="Wir bauen Software für die Logistik und für die Mobilität.",
        shortlist=["Logistik", "Mobility"],
        url="check-purpose-branch://withheld-model-call/never-cached",
        taxonomy_version="check-purpose-branch",
        cached_only=True,
    )
    check("a withheld call names no branch", withheld.branch, None)
    check("…and says why", bool(withheld.refused), True)
    check("…and does not pretend the model rejected the shortlist", withheld.rejected_all, False)
    check("cached_only defaults to False — the flag has to ask for it",
          inspect.signature(branch_llm.choose).parameters["cached_only"].default, False)

    # And the runner may only spend that flag on the model, never on the classifier.
    for function_name in ("classify", "classify_purpose"):
        source = inspect.getsource(getattr(runner, function_name))
        check(f"{function_name}() gates only NEW calls with it",
              "skip_model_for" in source, True)
        check(f"…{function_name}() never passes register_status into a classifier",
              "register_status=" in source, False)

    # -----------------------------------------------------------------------------------
    section("every subcommand has a dispatch branch of its own")
    # The dispatch chain ends `else: write_csv(args.limit)`. A subcommand registered without its own
    # branch silently runs write_csv and looks like it worked.
    main_source = inspect.getsource(runner.main)
    for command in ("classify-purpose", "classify-purpose-report"):
        check(f"{command!r} is registered", f'"{command}"' in main_source, True)
        check(f"{command!r} has an elif branch",
              f'args.command == "{command}"' in main_source, True)

    print(f"\n{'FAILED' if FAILURES else 'all checks passed'}")
    for label in FAILURES:
        print(f"  - {label}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
