"""Which technology field does this company work in? Decided from the words it publishes. Nothing else.

A **branch** here means one thing and one thing only: a value of Berlin's column *Branche /
Technologie- und Anwendungsfeld*. There are 77 of them — "Bahnindustrie", "Quantentechnologien",
"Gastgewerbe" — and each comes with a handful of **keywords**, words that suggest a company belongs
there. "Schienenfahrzeuge" suggests Bahnindustrie. Both live in `branch_taxonomy.yaml`.

A company's answer is one of those 77 values, or **NONE**. There is no third possibility, and the
only thing that may produce a value is **the company's own text**: its services pages, its homepage,
its Impressum, or the purpose it registered.

## What must never write a branch

A **WZ code** — the official German industry classification, in `public.companies.wz_code` for 8,483
of 14,443 companies — says what a company is *registered as*. That is a different question, answered
by a different authority, and it is **not** a technology field. It gets its own column, built by
`wz_cluster.py`, and this module never sees it: `classify()` takes no WZ argument, and
`scripts/check_branch_classifier.py` fails if one ever appears.

That rule was learned the expensive way. An earlier version mapped WZ codes onto Berlin's fields by
hand — `56.11.1 → Gastgewerbe`, `43.22.0 → Handwerk` — and filled 2,632 companies' technology field
with values nothing they published supported, while flattening 22 sections and 87 divisions of good
official classification to do it. It is the same mistake this repo already documents on the website
side: a register number on a page proves the page is *about* a company, never that it *is* that
company's site.

It never fetches anything, never touches a database, and never imports `confidence`. That last one
is deliberate: `confidence.py` is the only place that decides whether a page *belongs to* a company,
and the question here — what field the company works in — must not be able to influence that.

## Why the weights are measured rather than chosen

Two keywords appearing once each are not worth the same. Measured over 860 company websites:

    sicherheit        90 companies   ->  and it belongs to "Defence Tech"
    social media      88
    nachhaltigkeit    74
    photovoltaik      23
    steuerberater     23

`photovoltaik` and `steuerberater` tell you the industry. `sicherheit` does not — it is
*Arbeitssicherheit* and *Datensicherheit* on ordinary business pages. Same raw count, opposite
usefulness, and no hand-written list of weak words can separate them. What separates them is
counting, so a keyword's weight falls with:

- **how many companies use the word at all** — the classic inverse document frequency; and
- **how many branches claim it** — `schienenfahrzeuge` belongs to Bahnindustrie alone and carries its
  full value, while `sensorik` is claimed by seven branches and carries a seventh to each.

This is the same rule `name_frequency.common_words` follows for company names and
`name_frequency.common_email_domains` follows for mail providers: genericness is *measured from the
data*, never listed by hand. The most common email domain in Osnabrück turned out to be a local
internet provider no hand-written list would have contained.

## The one rule that must not be relaxed

**A keyword matches only with a word boundary on both sides. Never inside a German compound.**

German glues nouns together, so `Batterierecycling` really does sit inside `Batterierecyclinganlage`
and allowing that looks like free coverage. Measured, it is not: pages carrying at least one keyword
rose from 61% to 65%, but `sicherheit` went from 90 companies to 139 — hiding inside
*Arbeitssicherheit* and *Sicherheitstechnik* — and **Defence Tech became the most common industry in
Osnabrück.** Companies where one branch clearly won *fell*, 338 to 316. More matches, worse answers.

That rule covers compounds German glues together. It does **not** cover the ones German writes with a
hyphen, and those are just as common:

    Arbeitssicherheit        blocked
    Instandhaltungsarbeiten  blocked
    Kfz-Zulassung            fires `zulassung`   -> Pharma & Wirkstoffe
    Foto-Galerie             fires `galerie`     -> Kunstmarkt
    Web-App-Entwicklung      fires `app`         -> Digital Health

`HYPHEN_IS_BOUNDARY` closes that hole and is **off by default**, because closing it also stops
*E-Commerce-Lösungen* matching `e-commerce` and that is a true positive lost. It is a trade, and the
glued-compound rule above was only trusted because somebody measured it. Measure this one the same
way before flipping the default.

## NONE is the answer for most companies, and there are three different reasons for it

    assigned     one branch cleared the floor and beat the runner-up. The column gets its name.
    torn         keywords fired but two branches are too close to separate
    no_keyword   there was text and not one keyword appeared in it
    no_text      there was nothing to read

The last three all print `NONE` in the column, because `NONE` is what the column is allowed to
contain. The *reason* is kept beside it and **the three are never added together**: "we had no text"
is not evidence that a company has no technology field. Same distinction
`run_combined_discovery.tier_status` draws between `no_result` and `never_tried`, for the same
reason — collapsing them turns a blank into a claim.

Expect `NONE` to be the common case. Measured over 14,443 companies: **974 (6.7%)** get a value, and
every one of them is backed by words the company published about itself. Berlin's list was built for
Berlin's technology economy; most of a Lower Saxony town is not in it, and saying `NONE` is the
correct answer rather than a failure.
"""
from __future__ import annotations

import hashlib
import logging
import math
import os
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

log = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
TAXONOMY_PATH = Path(os.getenv("BRANCH_TAXONOMY_PATH") or HERE / "branch_taxonomy.yaml")

#: How high a branch's score has to be before it is called the answer. Calibrated against homepage
#: text; richer text moves every score up, so re-check it after changing what gets read.
MIN_SCORE = float(os.getenv("BRANCH_MIN_SCORE", "2.0"))

#: How far ahead of the runner-up the winner has to be. 1.5 means "half again as much".
MIN_MARGIN = float(os.getenv("BRANCH_MIN_MARGIN", "1.5"))

#: Below this many distinct keywords there is nothing for a language model to arbitrate, so a torn
#: row stays torn rather than becoming a model call. One keyword against one keyword is a coin flip,
#: and asking a 4B model to flip it costs ten seconds and buys a confident-sounding guess.
MIN_KEYWORDS_FOR_MODEL = int(os.getenv("BRANCH_MIN_KEYWORDS_FOR_MODEL", "2"))

#: The literal value the Technologiefeld column carries when no branch was established. Spelled
#: out rather than left empty, because an empty cell in a handover file is indistinguishable from
#: "nobody has run this yet" — and those are very different claims.
NONE = "NONE"

#: How many branches a model is ever offered. Three plus "none of these" is a question a small model
#: can answer; 77 is one it guesses at.
SHORTLIST_SIZE = int(os.getenv("BRANCH_SHORTLIST_SIZE", "3"))

#: The characters that count as "inside a word" when deciding whether a keyword stands alone.
#: `à-öø-ÿ` is the lower-case accented Latin-1 range with the division sign at U+00F7 skipped, which
#: is what puts ä, ö, ü and ß in the class without hand-listing them.
_WORD_CHAR = "[0-9a-zà-öø-ÿ]"

#: The same class with the hyphen added, which German compounds also use: *IT-Sicherheit*,
#: *Kfz-Zulassung*, *Foto-Galerie*, *Web-App-Entwicklung*. The rule above stops a keyword hiding
#: inside a glued compound and does **not** stop it hiding inside a hyphenated one, so today
#: `Kfz-Zulassung` fires `zulassung` and files a car-registration office under Pharma & Wirkstoffe.
#: Measured over the 1,372 cached services pages, counting only pages where the keyword never
#: appears standing alone: `galerie` 24 of 67 pages, `app` 171 of 789, `makler` 4 of 13.
_WORD_CHAR_WITH_HYPHEN = "[-0-9a-zà-öø-ÿ]"

#: Whether a hyphen counts as being inside a word. **Off by default, which is today's behaviour.**
#: Closing the hole is not free: it also stops *E-Commerce-Lösungen* matching `e-commerce`, and that
#: is a true positive lost. So this is a knob rather than a fix, and the decision belongs to the same
#: three numbers that settled the glued-compound rule — how many companies carry a keyword at all,
#: the per-keyword company counts, and how many companies end up with a clear winner. Run `classify`
#: both ways and compare. Do not flip the default without putting that table in
#: `docs/branch-classification.md`.
HYPHEN_IS_BOUNDARY = os.getenv("BRANCH_HYPHEN_IS_BOUNDARY", "").strip().lower() in {"1", "true", "yes"}


def normalize(value: str | None) -> str:
    """One spelling for text that arrives in several: NFKC, no non-breaking spaces, lower case.

    Applied to the keywords and to the page alike, which is the only reason they can meet at all.
    The taxonomy came out of a spreadsheet carrying U+00A0 *inside* words — "Advanced\xa0Materials" —
    and that character looks exactly like a space while comparing as something else.
    """
    text = unicodedata.normalize("NFKC", value or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip().lower()


def _pattern_for(keyword: str, *, hyphen_is_boundary: bool | None = None) -> re.Pattern[str]:
    """A keyword as a regex that will not match inside a longer German word.

    See "the one rule that must not be relaxed" in the module docstring before widening this.
    `hyphen_is_boundary` defaults to `HYPHEN_IS_BOUNDARY`, and is a parameter so a measurement can
    build both patterns in one process instead of running twice.
    """
    strict = HYPHEN_IS_BOUNDARY if hyphen_is_boundary is None else hyphen_is_boundary
    edge = _WORD_CHAR_WITH_HYPHEN if strict else _WORD_CHAR
    return re.compile(f"(?<!{edge}){re.escape(keyword)}(?!{edge})")


@dataclass
class Taxonomy:
    """Berlin's branch list, plus the lookups the matcher needs."""

    #: Branch name -> whatever the YAML said about it (senweb_unit, berlin_partner, related).
    branches: dict[str, dict[str, Any]]
    #: Normalised keyword -> the branches that claim it. A keyword can belong to several.
    keyword_branches: dict[str, frozenset[str]]
    #: Normalised keyword -> compiled pattern. Built once; matching 527 patterns over 860 pages is
    #: the whole cost of this module and it is measured in seconds.
    patterns: dict[str, re.Pattern[str]]
    #: First 12 hex of a hash over the file **and** the matching rule in force. Model answers are
    #: cached against it, so editing the list — or flipping `HYPHEN_IS_BOUNDARY`, which changes what
    #: every keyword matches — invalidates exactly the answers that could have changed. It is also
    #: written into every row of `branches.jsonl`, which is what lets two runs be told apart.
    version: str

    def branch_names(self) -> list[str]:
        return sorted(self.branches)


def load_taxonomy(path: Path | None = None) -> Taxonomy:
    """Read `branch_taxonomy.yaml`. Raises rather than returning an empty taxonomy.

    An empty taxonomy classifies nothing, and "nothing was classified" reads exactly like a broken
    scorer — so a missing or malformed file has to fail where it happened.
    """
    import yaml

    target = path or TAXONOMY_PATH
    raw = target.read_text(encoding="utf-8")
    data = yaml.safe_load(raw) or {}
    branches = data.get("branches") or {}
    if not branches:
        raise ValueError(f"{target} contains no branches")

    keyword_branches: dict[str, set[str]] = {}
    clean: dict[str, dict[str, Any]] = {}
    for name, meta in branches.items():
        branch = name.strip()
        clean[branch] = dict(meta or {})
        for keyword in (meta or {}).get("keywords") or []:
            key = normalize(keyword)
            if len(key) >= 2:
                keyword_branches.setdefault(key, set()).add(branch)

    return Taxonomy(
        branches=clean,
        keyword_branches={k: frozenset(v) for k, v in keyword_branches.items()},
        patterns={k: _pattern_for(k) for k in keyword_branches},
        version=hashlib.sha256(
            f"{raw}|hyphen={int(HYPHEN_IS_BOUNDARY)}".encode("utf-8")
        ).hexdigest()[:12],
    )


# ---------------------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------------------


def matched_keywords(text: str, taxonomy: Taxonomy) -> set[str]:
    """Every keyword that appears in this text, as a whole word.

    The text must already be normalised — pass it through `normalize` once per document rather than
    once per keyword, or the same 6,000 characters get lower-cased 527 times.
    """
    if not text:
        return set()
    return {keyword for keyword, pattern in taxonomy.patterns.items() if pattern.search(text)}


def document_frequency(per_company: Iterable[Iterable[str]]) -> Counter[str]:
    """How many **companies** use each keyword.

    Companies, not pages. A company with four cached pages must not make its own words look four
    times more common than a company with one — that silently distorts every weight below it, and
    it distorts them in favour of whoever has the biggest website.
    """
    counts: Counter[str] = Counter()
    for keywords in per_company:
        counts.update(set(keywords))
    return counts


def keyword_weight(
    keyword: str, taxonomy: Taxonomy, df: Mapping[str, int], n_companies: int
) -> float:
    """What one keyword hit is worth to one branch.

    Three factors, and each answers a different question:

    - *Does anybody else use this word?* `log(n / (1 + df))` — the standard inverse document
      frequency. A word on 500 of 860 companies is worth almost nothing; a word on 3 is worth a lot.
    - *Does this word belong to this branch alone?* Divided by the number of branches claiming it.
      `sensorik` is claimed by seven and gives each a seventh.
    - *How specific is the phrase?* A multi-word keyword scores higher. "european train control
      system" appearing is far stronger evidence than "steuerung" appearing.
    """
    frequency = df.get(keyword, 0)
    idf = math.log(max(n_companies, 1) / (1 + frequency))
    if idf <= 0:
        return 0.0
    claimants = len(taxonomy.keyword_branches.get(keyword) or ()) or 1
    words = len(keyword.split())
    return idf * (1.0 + 0.5 * (words - 1)) / claimants


@dataclass
class Assignment:
    """The answer for one company, and enough of the reasoning to argue with it."""

    #: assigned | torn | no_keyword | no_text. The last three all print NONE in the column.
    outcome: str
    branch: str | None = None
    score: float = 0.0
    #: keywords | model — what actually decided it. Never a WZ code; see the module docstring.
    decided_by: str = ""
    runner_up: str | None = None
    runner_up_score: float = 0.0
    #: Every branch that scored anything, best first. Truncated by the caller when written out.
    ranked: list[tuple[str, float]] = field(default_factory=list)
    #: The keywords that fired, and which text they were found in.
    keywords: dict[str, str] = field(default_factory=dict)
    #: True when a human should look at this row. Set for a model verdict of "none of these".
    needs_review: bool = False
    #: What a language model would be asked to choose between, when one is worth asking.
    shortlist: list[str] = field(default_factory=list)

    @property
    def decided(self) -> bool:
        return self.outcome == "assigned"

    @property
    def value(self) -> str:
        """What goes in the column: a Berlin branch name, or the literal NONE. Never anything else."""
        return self.branch or NONE


def score_branches(
    keywords_by_source: Mapping[str, str],
    taxonomy: Taxonomy,
    df: Mapping[str, int],
    n_companies: int,
) -> list[tuple[str, float]]:
    """Branch -> total weight, best first. `keywords_by_source` maps keyword -> where it was found."""
    totals: Counter[str] = Counter()
    for keyword in keywords_by_source:
        weight = keyword_weight(keyword, taxonomy, df, n_companies)
        if weight <= 0:
            continue
        for branch in taxonomy.keyword_branches.get(keyword) or ():
            totals[branch] += weight
    return sorted(totals.items(), key=lambda kv: (-kv[1], kv[0]))


def classify(
    *,
    taxonomy: Taxonomy,
    df: Mapping[str, int],
    n_companies: int,
    keywords_by_source: Mapping[str, str] | None = None,
    had_text: bool = False,
) -> Assignment:
    """The answer for one company: one of Berlin's 77 fields, or NONE.

    **There is deliberately no WZ parameter.** A company's official industry code is a different
    classification answering a different question, and it belongs in its own column — see
    `wz_cluster.py` and the module docstring. If you are about to add one here to lift the coverage
    number, that is precisely the change this signature exists to prevent.

    Everything this needs was computed before the call: `keywords_by_source` maps each keyword that
    fired to the text it was found in, and `df` says how many companies use each keyword.
    """
    keywords_by_source = dict(keywords_by_source or {})
    ranked = (
        score_branches(keywords_by_source, taxonomy, df, n_companies)
        if keywords_by_source else []
    )
    from_words = [(b, round(s, 3)) for b, s in ranked]

    if not had_text:
        return Assignment(outcome="no_text")
    if not keywords_by_source:
        return Assignment(outcome="no_keyword")

    if not ranked:
        # Every keyword that fired is used by so many companies that its weight rounded to nothing.
        # That is "no usable keyword", not "no keyword" — but it is not an answer either.
        return Assignment(outcome="no_keyword", keywords=keywords_by_source)

    best, best_score = ranked[0]
    second, second_score = ranked[1] if len(ranked) > 1 else (None, 0.0)
    clear = best_score >= MIN_SCORE and best_score >= MIN_MARGIN * max(second_score, 1e-9)

    common = {
        "score": round(best_score, 3),
        "runner_up": second,
        "runner_up_score": round(second_score, 3),
        "ranked": from_words,
        "keywords": keywords_by_source,
    }
    if clear:
        return Assignment(outcome="assigned", branch=best, decided_by="keywords", **common)
    return Assignment(
        outcome="torn",
        shortlist=[b for b, _ in ranked[:SHORTLIST_SIZE]] if worth_asking(keywords_by_source, best_score) else [],
        **common,
    )


def worth_asking(keywords_by_source: Mapping[str, str], best_score: float) -> bool:
    """Is there enough here for a language model to arbitrate?

    Both conditions have to hold. Fewer than `MIN_KEYWORDS_FOR_MODEL` keywords means one word
    against one word: the model has nothing to weigh, and its answer would be a guess with a
    confident voice. A best score below the floor means even the leader is barely evidenced.

    This is the same shape as `pipeline._worth_judging`: cheap evidence may cancel a model call, and
    the saving is the point — each call is ten measured seconds on `qwen3.5:4b`.
    """
    return len(keywords_by_source) >= MIN_KEYWORDS_FOR_MODEL and best_score >= MIN_SCORE
