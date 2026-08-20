"""Which industry is this company in? Decided from its WZ code, or from the words on its website.

Two words to define first.

A **branch** (German *Branche*) is an industry plus the technology field it sits in —
"Bahnindustrie", "Gastgewerbe", "Additive Fertigung". Berlin's economic-development people keep a
list of 77, each with a handful of **keywords**: words that suggest a company belongs there.
"Schienenfahrzeuge" suggests Bahnindustrie. Both live in `branch_taxonomy.yaml`.

A **WZ code** is the official German industry code. "56.1" is restaurants, "62.10.9" software. It is
already in `public.companies` for 8,483 of 14,443 companies, and `wz_to_branch.yaml` maps it.

This module answers in that order, and the order is the whole design:

1. **The WZ code, if there is one.** Deterministic, free, and it works for the 1,943 companies that
   have no website and no describing text — nothing else in this repo can reach those at all.
2. **Otherwise the words.** Which of a branch's keywords appear in the company's text, weighted.

It never fetches anything, never touches a database, and never imports `confidence`. That last one
is deliberate: `confidence.py` is the only place that decides whether a page *belongs to* a company,
and the question here — what industry the company is in — must not be able to influence that.

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

## Not deciding is an answer, and there are four different ones

    assigned            one branch cleared the floor and beat the runner-up
    no_branch_applies   the WZ code says holding company or housing association. Berlin's list has
                        no home for those, and saying so is a result, not a gap.
    torn                keywords fired but two branches are too close to separate
    no_keyword          there was text and not one keyword appeared in it
    no_text             there was nothing to read

`no_keyword` and `no_text` must never be added together, and neither may be added to
`no_branch_applies`. That is the same distinction `run_combined_discovery.tier_status` draws between
`no_result` and `never_tried`, for the same reason: collapsing them turns a blank into a claim.
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
WZ_MAP_PATH = Path(os.getenv("WZ_BRANCH_MAP_PATH") or HERE / "wz_to_branch.yaml")

#: How high a branch's score has to be before it is called the answer. Calibrated against homepage
#: text; richer text moves every score up, so re-check it after changing what gets read.
MIN_SCORE = float(os.getenv("BRANCH_MIN_SCORE", "2.0"))

#: How far ahead of the runner-up the winner has to be. 1.5 means "half again as much".
MIN_MARGIN = float(os.getenv("BRANCH_MIN_MARGIN", "1.5"))

#: Below this many distinct keywords there is nothing for a language model to arbitrate, so a torn
#: row stays torn rather than becoming a model call. One keyword against one keyword is a coin flip,
#: and asking a 4B model to flip it costs ten seconds and buys a confident-sounding guess.
MIN_KEYWORDS_FOR_MODEL = int(os.getenv("BRANCH_MIN_KEYWORDS_FOR_MODEL", "2"))

#: How many branches a model is ever offered. Three plus "none of these" is a question a small model
#: can answer; 77 is one it guesses at.
SHORTLIST_SIZE = int(os.getenv("BRANCH_SHORTLIST_SIZE", "3"))

#: The characters that count as "inside a word" when deciding whether a keyword stands alone.
#: `à-öø-ÿ` is the lower-case accented Latin-1 range with the division sign at U+00F7 skipped, which
#: is what puts ä, ö, ü and ß in the class without hand-listing them.
_WORD_CHAR = "[0-9a-zà-öø-ÿ]"


def normalize(value: str | None) -> str:
    """One spelling for text that arrives in several: NFKC, no non-breaking spaces, lower case.

    Applied to the keywords and to the page alike, which is the only reason they can meet at all.
    The taxonomy came out of a spreadsheet carrying U+00A0 *inside* words — "Advanced\xa0Materials" —
    and that character looks exactly like a space while comparing as something else.
    """
    text = unicodedata.normalize("NFKC", value or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip().lower()


def _pattern_for(keyword: str) -> re.Pattern[str]:
    """A keyword as a regex that will not match inside a longer German word.

    See "the one rule that must not be relaxed" in the module docstring before widening this.
    """
    return re.compile(f"(?<!{_WORD_CHAR}){re.escape(keyword)}(?!{_WORD_CHAR})")


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
    #: First 12 hex of the file's SHA-256. Model answers are cached against it, so editing the list
    #: invalidates exactly the answers that could have changed.
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
        version=hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12],
    )


@dataclass
class WzMap:
    """WZ code prefix -> branch name, or None for "no branch applies"."""

    #: Prefix -> branch name or None. Present-with-None and absent are different answers.
    mappings: dict[str, str | None]

    def lookup(self, code: str | None) -> tuple[bool, str | None, str]:
        """(is the code mapped, the branch or None, the prefix that matched).

        Matched by **longest prefix**. Our codes arrive at three depths — "64.2", "64.21", "94.99"
        and "94.99.9" all occur in the same column — so a company's "94.99.9" has to fall back to
        the entry for "94.99" when there is no more specific one. Exact matching alone would leave
        most of the table unmapped while looking like the mapping was incomplete.

        The first element of the tuple is what separates "mapped to no branch" from "not mapped",
        which are the two states this whole module is careful about.
        """
        text = (code or "").strip()
        if not text:
            return False, None, ""
        best = ""
        for prefix in self.mappings:
            if text.startswith(prefix) and len(prefix) > len(best):
                best = prefix
        if not best:
            return False, None, ""
        return True, self.mappings[best], best


def load_wz_map(path: Path | None = None) -> WzMap:
    """Read `wz_to_branch.yaml`. A missing file is fine — it means Stage A has not been filled in."""
    import yaml

    target = path or WZ_MAP_PATH
    if not target.exists():
        log.info("no %s yet; WZ codes will not be used", target.name)
        return WzMap(mappings={})
    data = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    mappings = data.get("mappings")
    if mappings is None:
        raise ValueError(f"{target} has no `mappings:` block")
    return WzMap(mappings={str(k).strip(): (v.strip() if isinstance(v, str) else None)
                           for k, v in mappings.items()})


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

    #: assigned | no_branch_applies | torn | no_keyword | no_text
    outcome: str
    branch: str | None = None
    score: float = 0.0
    #: wz | keywords | model — what actually decided it.
    decided_by: str = ""
    runner_up: str | None = None
    runner_up_score: float = 0.0
    #: Every branch that scored anything, best first. Truncated by the caller when written out.
    ranked: list[tuple[str, float]] = field(default_factory=list)
    #: The keywords that fired, and which text they were found in.
    keywords: dict[str, str] = field(default_factory=dict)
    #: The WZ prefix that matched, when the WZ code answered.
    wz_prefix: str = ""
    #: True when a human should look at this row. Set for a model verdict of "none of these".
    needs_review: bool = False
    #: What a language model would be asked to choose between, when one is worth asking.
    shortlist: list[str] = field(default_factory=list)

    @property
    def decided(self) -> bool:
        return self.outcome in ("assigned", "no_branch_applies")


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
    wz_code: str | None = None,
    wz_map: WzMap | None = None,
) -> Assignment:
    """The answer for one company.

    The WZ code wins when it has something to say, because it is a statement of fact from a register
    rather than an inference from marketing copy. When it maps to nothing, the words get their turn;
    when it maps explicitly to "no branch", that IS the answer and the words are not consulted — a
    holding company whose page happens to say "Nachhaltigkeit" is still a holding company.
    """
    keywords_by_source = dict(keywords_by_source or {})

    if wz_map is not None:
        mapped, branch, prefix = wz_map.lookup(wz_code)
        if mapped and branch:
            return Assignment(
                outcome="assigned", branch=branch, decided_by="wz", wz_prefix=prefix,
                keywords=keywords_by_source,
            )
        if mapped:
            return Assignment(
                outcome="no_branch_applies", decided_by="wz", wz_prefix=prefix,
                keywords=keywords_by_source,
            )

    if not had_text:
        return Assignment(outcome="no_text")
    if not keywords_by_source:
        return Assignment(outcome="no_keyword")

    ranked = score_branches(keywords_by_source, taxonomy, df, n_companies)
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
        "ranked": [(b, round(s, 3)) for b, s in ranked],
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
