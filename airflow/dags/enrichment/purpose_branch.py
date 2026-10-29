"""What does the commercial register say this company was set up to do? A third industry column.

Germany's *Handelsregister* records, for every registered company, a **Gegenstand des Unternehmens** —
a sentence or two saying what the company is for. It arrives here as the `business_purpose` register field
and reads like this:

    Erbringung physiotherapeutischer Dienstleistungen und Anwendungen und die damit im
    Zusammenhang stehenden Taetigkeiten, ausgenommen jedoch erlaubnispflichtige Taetigkeiten.

This module turns that text into `purpose_field` — one of the same 77 Berlin technology fields the
Technologiefeld column uses, or a **recorded non-answer**. It is a third column, not more coverage in
an existing one, and the distinction is the whole reason this file exists.

## Why a third column and not a bigger second one

`branch_classifier.py` answers "what field does this company **work in**?" and, since 2026-08-25, it
requires a website to say so. The argument for that rule is in its module docstring: a register purpose
says what a company is *permitted* to do, and a great deal of it is boilerplate. A live website is
evidence the company actually trades. Requiring one cost 563 answers, and that was a considered price.

This module answers a different question — "what was it **registered to do**?" — so it may read the
register text without reopening that decision. What it must never do is write its answer into the other
column. Two columns, two questions, two provenances, compared rather than merged.

## The one number that describes this column

Of 14,443 companies, **7,141 have a usable purpose** (over 40 characters). Of those, **657 — 9.2% —
contain even one taxonomy keyword.** That is the ceiling of the keyword pass, and it is low for a
reason worth stating plainly: register prose is written by notaries to describe legal permissions, and
Berlin's keyword list is written to describe trades. They only sometimes use the same words.

The remaining ~6,484 are not failures of the matcher. Most of them genuinely contain no word for any
industry Berlin's list names, and no amount of keyword work reaches them. That gap is what Phase 2's
Tier B exists to probe, with a model that reads meaning rather than matching strings.

## The rules inherited from `branch_classifier`, and why each is load-bearing

Everything here composes that module's primitives rather than reimplementing them. Three in particular:

1. **A keyword never matches inside a longer German word.** `_pattern_for` enforces it with lookarounds.
   Relaxing it here would be exactly as wrong as relaxing it there, and this data shows why: allowing
   substring matches would fire `epa` inside *Reparatur* on 124 companies, `its` inside *Kreditsicherung*
   on 303, and `ai` on 144. Those are not near-misses; they are noise that would outrank every real hit.

2. **Document frequency counts companies, not texts** — and here it is computed over **purpose texts
   only**, with `n_companies` = the number of usable purposes. Borrowing the website corpus's frequencies
   would weight register vocabulary by how often it appears on homepages, which is a different language.

3. **`NONE` is not one answer.** `no_purpose` (there was nothing to read), `no_keyword` (we read it and
   no keyword appeared), `torn` (two fields too close), `none_apply` (a model read it and said no field
   fits) and `unsure` (a model read it and could not tell) are five different claims. **Never add any
   two of them together.** "We had nothing to read" is not evidence a company has no technology field.

## What this module does NOT do

It does not call `branch_classifier.classify()`. That function applies the website gate, which would
withhold every answer this column exists to produce — and passing `has_website=True` to get around the
gate would be lying to the function about what was checked. Composing the primitives is honest; faking
the argument is not.

It also never returns one of `branch_classifier`'s outcome strings. The vocabularies are deliberately
disjoint (`assigned_keywords`, not `assigned`) so that no downstream `if` can confuse a purpose answer
with a website answer by accident.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace

from . import branch_classifier as bc

log = logging.getLogger(__name__)

#: How much register text counts as worth reading. Below this a purpose is a stub — "Bauträger",
#: "Handel", "Verwaltung" — which names no trade specifically enough to classify and would only add
#: noise to the document frequencies. 400 companies sit in the 1-40 character band and 6,902 have
#: nothing at all; together they are the 7,302 `no_purpose` rows.
MIN_PURPOSE_CHARS = int(os.getenv("PURPOSE_MIN_CHARS", "40"))

#: The literal value the column carries when no field was established, spelled out for the same
#: reason `branch_classifier.NONE` is: an empty cell in a handover file cannot be told apart from
#: "nobody has run this yet", and those are very different claims.
NONE = bc.NONE

# ---------------------------------------------------------------------------------------
# The outcome vocabulary
# ---------------------------------------------------------------------------------------
# Named constants rather than bare strings, so a typo is an AttributeError at import instead of a
# silently unreachable branch that quietly reports zero.

#: Phase 1, free. The keyword pass produced one clear winner.
ASSIGNED_KEYWORDS = "assigned_keywords"
#: Phase 1, free. Keywords fired, but the top two are too close to separate.
TORN = "torn"
#: Phase 1, free. We read the text and not one of the ~500 keywords appeared.
NO_KEYWORD = "no_keyword"
#: Phase 1, free. Empty, whitespace, or shorter than `MIN_PURPOSE_CHARS`. Never read.
NO_PURPOSE = "no_purpose"

#: Phase 2 Tier A. Was `torn`; a model picked one of the top 3 the keywords had nominated.
ASSIGNED_MODEL_3WAY = "assigned_model_3way"
#: Phase 2 Tier B. Was `no_keyword`; a model chose from all 77. The least traceable outcome there is.
ASSIGNED_MODEL_77 = "assigned_model_77"
#: Phase 2, either tier. The model read it and said no field fits. A real answer, not a failure.
NONE_APPLY = "none_apply"
#: Phase 2, either tier. The model read it and said the text does not say enough to decide.
UNSURE = "unsure"

#: Every outcome this module can return. The guard asserts the set is exactly this, so adding one
#: without deciding where it is counted fails loudly rather than vanishing from the report.
ALL_OUTCOMES = frozenset({
    ASSIGNED_KEYWORDS, TORN, NO_KEYWORD, NO_PURPOSE,
    ASSIGNED_MODEL_3WAY, ASSIGNED_MODEL_77, NONE_APPLY, UNSURE,
})

#: The outcomes that carry an actual field. Everything else prints NONE.
ASSIGNED_OUTCOMES = frozenset({ASSIGNED_KEYWORDS, ASSIGNED_MODEL_3WAY, ASSIGNED_MODEL_77})

# ---------------------------------------------------------------------------------------
# Who decided
# ---------------------------------------------------------------------------------------
# `outcome` says whether there is a field. `decided_by` says who put it there, and that is what lets
# a consumer trust two rows differently. A `keywords` row can always be checked against a word that
# is literally in the register text. A `model_direct` row cannot be checked against anything.

DECIDED_BY_KEYWORDS = "keywords"
DECIDED_BY_MODEL_TIEBREAK = "model_tiebreak"
DECIDED_BY_MODEL_DIRECT = "model_direct"

ALL_DECIDED_BY = frozenset({
    DECIDED_BY_KEYWORDS, DECIDED_BY_MODEL_TIEBREAK, DECIDED_BY_MODEL_DIRECT,
})

#: Which tier produced an abstention. `none_apply` from a 3-way question means "none of these three
#: fit"; from a 77-way question it means "none of Berlin's 77 fit". Those are completely different
#: claims, and without this they are indistinguishable in the output — so abstentions carry
#: `decided_by` too, not only assignments.
MODEL_DECIDED_BY = frozenset({DECIDED_BY_MODEL_TIEBREAK, DECIDED_BY_MODEL_DIRECT})


@dataclass
class PurposeAssignment:
    """The register's answer for one company, plus enough of the reasoning to argue with it."""

    #: One of `ALL_OUTCOMES`. Deliberately never one of `branch_classifier.Assignment`'s values.
    outcome: str
    #: The field, when there is one. None for every non-answer, including the model's abstentions.
    branch: str | None = None
    score: float = 0.0
    #: One of `ALL_DECIDED_BY`, or "" when nothing decided. Set on abstentions too — see above.
    decided_by: str = ""
    runner_up: str | None = None
    runner_up_score: float = 0.0
    #: Every branch that scored anything, best first. Truncated by the caller when written out.
    #: **Keyword weights only.** A model's ordering goes in `model_ranked`, never here — see there.
    ranked: list[tuple[str, float]] = field(default_factory=list)
    #: The fields a model put in order, best first, and **nothing else**. Names only: a model has no
    #: calibrated score, and inventing one would put a plausible decimal next to `ranked`'s measured
    #: keyword-weight sums (7.487) with nothing downstream able to tell them apart. Kept in a second
    #: field rather than folded into `ranked` with a zero score for exactly that reason — the two
    #: kinds of ordering are different evidence and stay different data.
    model_ranked: list[str] = field(default_factory=list)
    #: The keywords that fired. Maps keyword -> where it was found, always "purpose" here, kept as a
    #: dict so the row has the same shape as the Technologiefeld's and the two can be diffed.
    keywords: dict[str, str] = field(default_factory=dict)
    #: The field this company WOULD have received had the length floor not excluded its purpose.
    #: Never the answer — `branch` stays None and `outcome` stays `no_purpose` — but keeping it is
    #: what makes the floor's cost reportable instead of invisible. Same pattern, and the same
    #: reason, as `branch_classifier.Assignment.withheld_branch` for the website gate.
    #:
    #: Measured on this data it is **17 companies**, and they are not marginal: "Gastronomie" (11
    #: chars), "Pflegedienst" (12), "Textil-Einzelhandel" (19), "Betrieb einer Spedition" (24). A
    #: short register purpose is often the least ambiguous one there is, which is an argument against
    #: the floor that only a number can make.
    withheld_by_floor: str | None = None
    #: What a model would be offered, when a torn row is worth asking about. **Empty means the row
    #: was never askable** — which is how a `torn` row that no model ever saw is told apart from one
    #: whose model call failed. See `llm_refused`.
    shortlist: list[str] = field(default_factory=list)
    #: Set when a model was asked and the ask broke: unreachable, timed out, unparseable, or an
    #: answer that was not on the list. **A failed call must never be recorded as `none_apply`** —
    #: "the model said no field fits" and "we never got an answer" are not the same finding.
    llm_refused: str = ""
    #: The model's own one-line justification, when it answered. The cheapest proxy for precision on
    #: a column where no row is hand-checked: read twenty of them and you know if it is reading.
    llm_reason: str = ""
    #: Which model produced any of the above, so two runs with different models can be told apart.
    model_name: str = ""

    @property
    def decided(self) -> bool:
        return self.outcome in ASSIGNED_OUTCOMES

    @property
    def value(self) -> str:
        """What goes in the column: a Berlin field name, or the literal NONE. Never anything else."""
        return self.branch or NONE

    @property
    def askable(self) -> bool:
        """Was there ever enough here to put to a model? Distinguishes two very different `torn` rows."""
        return bool(self.shortlist)


def is_usable(purpose: str | None) -> bool:
    """Is there enough register text to bother reading?

    Deliberately measured on the **normalised** string. A purpose padded with non-breaking spaces —
    and the register export does contain them — would otherwise clear the threshold on whitespace.
    """
    return len(bc.normalize(purpose)) > MIN_PURPOSE_CHARS


def purpose_keywords(purpose: str | None, taxonomy: bc.Taxonomy) -> dict[str, str]:
    """Which taxonomy keywords appear in this register text, as whole words.

    Returns keyword -> source, where the source is always `"purpose"`. The single-source dict looks
    redundant and is not: it keeps these rows shape-compatible with the Technologiefeld's, whose
    keywords come from four different texts, so the two columns can be compared field by field.

    **This deliberately does NOT apply the length floor.** Matching is free, and a stub that would
    have fired a keyword is exactly what `withheld_by_floor` exists to record. The floor is applied
    once, in `classify_purpose`, where it can be seen — not scattered across every helper, where it
    would silently erase its own cost.
    """
    return {keyword: "purpose" for keyword in bc.matched_keywords(bc.normalize(purpose), taxonomy)}


def classify_purpose(
    *,
    taxonomy: bc.Taxonomy,
    df: Mapping[str, int],
    n_companies: int,
    keywords_by_source: Mapping[str, str] | None = None,
    had_purpose: bool = False,
) -> PurposeAssignment:
    """The register's answer for one company. Free — no model, no network, no database.

    **There is no `has_website` parameter and no WZ parameter.** The website gate belongs to the
    Technologiefeld and would withhold every answer here; a WZ code is a different classification
    answering a different question. Both absences are asserted by `scripts/check_purpose_branch.py`.

    `had_purpose` defaults to **False**, matching the caution in `classify()`: a caller that has not
    established there was text to read must not be handed a field. Everything else was computed before
    the call — `keywords_by_source` from `purpose_keywords`, `df` from `document_frequency` over the
    purpose corpus.

    The decision order matters and mirrors `classify()`: the reason for a non-answer is settled before
    any scoring, so that each `NONE` reason counts only the rows it is actually responsible for.
    """
    keywords_by_source = dict(keywords_by_source or {})
    ranked = (
        bc.score_branches(keywords_by_source, taxonomy, df, n_companies)
        if keywords_by_source else []
    )
    from_words = [(b, round(s, 3)) for b, s in ranked]

    if not had_purpose:
        # The floor still applies — but if the excluded text would have produced a clear winner, keep
        # it. A veto whose rejections are never counted is indistinguishable from a veto that never
        # fires, and this one turns out to reject good answers.
        return PurposeAssignment(
            outcome=NO_PURPOSE,
            withheld_by_floor=(ranked[0][0] if ranked else None),
        )
    if not keywords_by_source:
        return PurposeAssignment(outcome=NO_KEYWORD)
    if not ranked:
        # Every keyword that fired is used by so many companies that its weight rounded to nothing.
        # That is "no usable keyword" rather than "no keyword", but it is not an answer either, and
        # it is counted with `no_keyword` because the consequence is identical: nothing to go on.
        return PurposeAssignment(outcome=NO_KEYWORD, keywords=keywords_by_source)

    best, best_score = ranked[0]
    second, second_score = ranked[1] if len(ranked) > 1 else (None, 0.0)

    # The same two thresholds the website pass uses. They were calibrated on homepage text, and
    # register purposes are shorter — 251 characters on average against several thousand — so scores
    # here sit lower and the mix of `assigned_keywords` to `torn` will not match. That is a thing to
    # measure from the first full run's score distribution, not to assume transfers.
    clear = best_score >= bc.MIN_SCORE and best_score >= bc.MIN_MARGIN * max(second_score, 1e-9)

    common = {
        "score": round(best_score, 3),
        "runner_up": second,
        "runner_up_score": round(second_score, 3),
        "ranked": from_words,
        "keywords": keywords_by_source,
    }
    if clear:
        return PurposeAssignment(
            outcome=ASSIGNED_KEYWORDS,
            branch=best,
            decided_by=DECIDED_BY_KEYWORDS,
            **common,
        )
    return PurposeAssignment(
        outcome=TORN,
        # Empty unless the row is genuinely worth a model call. `worth_asking` needs at least
        # MIN_KEYWORDS_FOR_MODEL distinct keywords, and 608 of the 657 companies with any purpose
        # keyword have exactly one — so that constant, not the prompt, is what governs how far Tier A
        # can ever reach. Measured: 17 of 60 torn rows are askable.
        shortlist=(
            [b for b, _ in ranked[:bc.SHORTLIST_SIZE]]
            if bc.worth_asking(keywords_by_source, best_score) else []
        ),
        **common,
    )


def apply_model_verdict(
    assignment: PurposeAssignment,
    *,
    branch: str | None,
    rejected_all: bool,
    unsure: bool,
    refused: str,
    reason: str,
    model_name: str,
    tier: str,
    model_ranked: Sequence[str] = (),
) -> PurposeAssignment:
    """Fold a model's answer into an assignment. Pure, and the only place a model may change an outcome.

    Four inputs, four different results, and **the fourth is the one that matters**:

    | the model                  | the outcome becomes                       |
    |----------------------------|-------------------------------------------|
    | named a field on the list  | `assigned_model_3way` / `assigned_model_77` |
    | answered `keine`           | `none_apply`                              |
    | answered `unsicher`        | `unsure`                                  |
    | **never answered usably**  | **unchanged** — `torn` or `no_keyword`    |

    A failed call must never become `none_apply`. "The model read this and says no Berlin field fits"
    is a finding about the company; "the model timed out" is a finding about our infrastructure. Write
    the second as the first and the report claims knowledge nobody has — so a refusal keeps the row's
    original outcome and records the reason in `llm_refused` instead.

    `tier` decides which assignment outcome a field produces, and is recorded as `decided_by` on
    **abstentions too**, not only on assignments. Without that, `none_apply` from a 3-option question
    and `none_apply` from a 77-option question are the same row — and they are not the same claim.
    """
    if tier not in MODEL_DECIDED_BY:
        raise ValueError(f"{tier!r} is not a model tier; expected one of {sorted(MODEL_DECIDED_BY)}")

    # `replace` rather than mutation, so a caller cannot accidentally keep a reference to a row that
    # has since changed outcome underneath it.
    #
    # `model_ranked` is recorded on EVERY path that got a usable answer, including the abstentions:
    # a model that ranked three fields and then said `keine` has told us both things, and the ranking
    # is what makes "was the right answer at rank 2" answerable for a row that ended up NONE.
    # A refusal is the one exception — there was no answer, so there is no ordering.
    updated = replace(
        assignment, model_name=model_name, llm_reason=reason, model_ranked=list(model_ranked),
    )

    if refused:
        # The row keeps whatever the keywords said. Nothing about the company has been learned, and
        # that includes the ordering: a failed call leaves `model_ranked` empty rather than partial.
        return replace(updated, llm_refused=refused, model_ranked=[])
    if branch:
        return replace(
            updated,
            outcome=(ASSIGNED_MODEL_3WAY if tier == DECIDED_BY_MODEL_TIEBREAK else ASSIGNED_MODEL_77),
            branch=branch,
            decided_by=tier,
        )
    if rejected_all:
        return replace(updated, outcome=NONE_APPLY, branch=None, decided_by=tier)
    if unsure:
        return replace(updated, outcome=UNSURE, branch=None, decided_by=tier)

    # No field, no abstention and no stated reason. That should be unreachable — the verdict types
    # are exhaustive — so record it as a failure rather than inventing an interpretation.
    return replace(updated, llm_refused="no answer and no refusal")


#: How a candidate's score was arrived at. Two values, and the distinction is the whole point of the
#: field: `keyword_weight` means a number was MEASURED (summed inverse-document-frequency weights
#: over the corpus, e.g. 7.487), `model_rank` means a model put the fields in an order and has no
#: number at all.
BASIS_KEYWORD = "keyword_weight"
BASIS_MODEL = "model_rank"

#: How many candidates a row may carry. Three, because the question the worksheet asks is "was the
#: right answer at rank 2 or 3" — beyond that a human is not choosing, they are reading a list.
CANDIDATE_LIMIT = 3


def purpose_candidates(
    assignment: PurposeAssignment, limit: int = CANDIDATE_LIMIT
) -> list[dict[str, object]]:
    """The top candidates for one row, best first, as `{rank, field, score, basis}`.

    Pure, and the only place this shape is built, so `scripts/check_purpose_branch.py` can call it
    rather than restate it — the same arrangement as `verdict_from_answer` and `label_staleness`.

    **A score is present only when one was measured.** `ranked` holds keyword weights, which are real
    numbers over a real corpus; `model_ranked` holds an ordering a model produced, and for those
    `score` is `None` with `basis` saying so. Folding the two into one list with a zero or an invented
    decimal for the model's entries was the alternative, and it would put `0.87` in the same column as
    `7.487` — different scales, different meanings, and nothing downstream able to tell them apart.
    That is the failure mode `confidence.py` exists to prevent, one column over.

    Which source wins when a row has both: **the model's.** A model is only ever asked about a row the
    keywords could not settle (`torn`) or could not touch (`no_keyword`), so where an ordering exists
    it is the later and better-informed judgement. The keyword weights stay in `ranked` and are not
    lost.

    A model answer from before the ranked prompt existed has no ordering, but it did name a field —
    and that field *is* rank 1. It gets a one-entry list rather than an empty one, because "the model
    chose Tourismus and we do not know its second choice" is a different fact from "nothing was
    proposed", and an empty list would make 553 answered rows indistinguishable from 5,196
    never-asked ones in the worksheet.

    Returns `[]` only when nothing was proposed at all: no keyword scored, no model ranked, and no
    model chose. That is 12,680 of the 14,443, and it is the honest answer both for a company whose
    register purpose named nothing and for one whose purpose was too short to read.
    """
    if assignment.model_ranked:
        return [
            {"rank": index, "field": field_name, "score": None, "basis": BASIS_MODEL}
            for index, field_name in enumerate(assignment.model_ranked[:limit], start=1)
        ]
    if assignment.ranked:
        return [
            {"rank": index, "field": field_name, "score": score, "basis": BASIS_KEYWORD}
            for index, (field_name, score) in enumerate(assignment.ranked[:limit], start=1)
        ]
    # A pre-ranking model answer: one field, chosen by a model, so `model_rank` and no score.
    if assignment.branch and assignment.decided_by in MODEL_DECIDED_BY:
        return [{"rank": 1, "field": assignment.branch, "score": None, "basis": BASIS_MODEL}]
    return []
