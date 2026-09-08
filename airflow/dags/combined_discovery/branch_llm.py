"""Ask a local language model to break a tie between three branches. Nothing more than that.

`branch_classifier` decides most companies from keywords alone. Some it cannot: two branches score
within a whisker of each other, and the words on the page do not separate them. This module asks
`qwen3.5:4b`, running on this machine, to choose — from **three** branches and a "none of these",
never from all 77.

That restriction is the whole reason this is safe. Measured while planning:

    asked to pick from all 77 branches   10.3 s a page (4b), 18 s (9b)
    ... and on the pages tried, it reached for the vaguest label available: a drain-cleaning
        firm came back as "Dienstleistung"

A 77-way choice from a 4B model is a guess in a confident voice, and this repo has the precedent:
a whole paragraph of prompt could not stop the same model failing the Brücklyn case in `llm_judge`.
A three-way choice, where the keywords have already done the shortlisting, is a question it can
actually answer.

Four rules, each enforced in code rather than asked for in the prompt:

1. **The answer has to be one of the three offered, or "keine".** Anything else is discarded and
   logged. A model that invents a branch name has not chosen; it has generated. Matching ignores
   case and the `- ` a model puts in front of a list item — `bahnindustrie` is option one, not a
   fourth option — and the branch is then recorded as the *taxonomy* spells it, because that string
   goes into a column and "bahnindustrie" is not a Berlin field name. The rule lives in
   `verdict_from_answer`, which the guard script calls rather than reimplements.
2. **"keine" is a real outcome and it flags the row for a human.** It means the model read the page
   and rejected all three shortlisted branches — which is different from "no keyword fired", and
   the two are never counted together.
3. **`think` is off and `temperature` is 0.** With thinking on, the model spent its entire output
   budget on reasoning and returned an empty string three times out of three.
4. **`num_ctx` is set explicitly.** Ollama's own default is small and it truncates a longer prompt
   silently — the page text, at the end of the prompt, is exactly what gets cut. That was measured:
   a ~2,400-token prompt reported 1,546 prompt tokens, and the model answered about a page it had
   never seen.

Answers are cached against the page URL **and** the taxonomy's version hash. Editing the branch list
invalidates exactly the answers that could have changed, and nothing else — so a keyword added to
Bahnindustrie does not cost 1.5 hours of re-asking.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from combined_discovery import page_cache
from ollama_search_discovery.ollama_http import (
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    OllamaUnavailableError,
    extract_json_block,
    run_chat_prompt,
)

log = logging.getLogger(__name__)

#: How much of the company's text the model sees. The same 6,000 characters `llm_judge` uses, for the
#: same reason: it is about two thousand tokens, and a small model's attention over more than that
#: buys nothing measurable.
MAX_CHARS = int(os.getenv("BRANCH_LLM_MAX_CHARS", "6000"))

#: Context length, in tokens. Big enough for the shortlist, the instructions and MAX_CHARS of text.
#: See rule 4 in the module docstring — leaving this unset silently truncates the page.
NUM_CTX = int(os.getenv("BRANCH_LLM_NUM_CTX", "8192"))

#: Output budget. The answer is a branch name and a dozen words of reason.
MAX_OUTPUT_TOKENS = int(os.getenv("BRANCH_LLM_MAX_OUTPUT_TOKENS", "120"))

#: The answer that means "none of the three fits".
NONE_ANSWER = "keine"

#: How many words the proposal call may return. Three is enough to name a business and few enough
#: that the model has to choose rather than list everything it saw.
MAX_PROPOSED_WORDS = int(os.getenv("BRANCH_VOCAB_MAX_WORDS", "3"))

#: The answer that means "the text does not say enough for me to tell". **A third answer, not a
#: shade of `keine`.** `keine` is a claim about the company — none of these fields is what it does.
#: `unsicher` is a claim about the evidence — the register text is too thin to decide. Collapsing
#: them would merge a finding with an absence of one, which is the mistake the whole outcome
#: vocabulary is built to avoid.
#:
#: It exists because of a measured asymmetry: on the 2026-08-25 tie-break run the model said "none of
#: these" 15% of the time, where roughly 50% would have been right. A model that under-refuses by
#: three times needs somewhere to put its uncertainty other than a confident field.
UNSURE_ANSWER = "unsicher"

#: The register-purpose prompt. Three differences from `PROMPT`, each earning its place:
#:
#: 1. It says the text is a REGISTER PURPOSE, not a website. That matters: a purpose describes what a
#:    company is *permitted* to do, and the model should read it as a legal permission rather than as
#:    a description of trade.
#: 2. It names the boilerplate case explicitly. About 2,294 of the 7,141 usable purposes — 32% — are
#:    holding, asset-management or administrative text, and the correct answer for all of them is
#:    `keine`. A model not told this will find a field in "Verwaltung eigenen Vermögens".
#: 3. It offers `unsicher`. See above.
PURPOSE_PROMPT = """Du ordnest ein Unternehmen einer Branche zu.

Du liest den GEGENSTAND DES UNTERNEHMENS aus dem Handelsregister. Das ist der Zweck, den das
Unternehmen bei der Eintragung angegeben hat — nicht unbedingt das, was es heute wirklich tut.

Waehle GENAU EINE der folgenden Branchen. Erfinde keine andere Branche.

Branchen:
{options}

Antworte stattdessen "{none}", wenn keine davon passt. Das ist bei Holdinggesellschaften,
Vermoegensverwaltung, Beteiligungsgesellschaften und rein verwaltenden Zwecken fast immer der
Fall — "{none}" ist dort die richtige Antwort, keine Ausrede.

Antworte "{unsure}", wenn der Text zu wenig sagt, um es zu entscheiden.

Unternehmen: {company}

Gegenstand des Unternehmens:
{text}

Antworte als JSON:
{{"branche": "<Name aus der Liste, {none} oder {unsure}>", "begruendung": "<max 15 Woerter>"}}
"""

#: The same question, asking for the best THREE in order instead of one.
#:
#: Why it exists: Tier B answers carry no score at all (`ranked` is empty and `score` is 0.0 for all
#: 553 of them), because Tier B runs precisely on the rows where no keyword fired, so there is nothing
#: to score with. A ranked answer is what makes "the classifier was wrong, but the right field was its
#: second choice" a distinguishable outcome — and that distinction is what says whether a cheap
#: candidate generator would be good enough to replace this call.
#:
#: Three differences from `PURPOSE_PROMPT`, each deliberate:
#:
#: 1. It asks for a JSON **list**, so a truncated answer loses the third entry rather than corrupting
#:    the first. `ranked_purpose_verdict_from_answer` validates every entry independently.
#: 2. `{none}` and `{unsure}` stay available and are answered INSTEAD of a list, not inside one. A
#:    model hedging with `["Handel", "keine"]` would otherwise be both an answer and an abstention.
#: 3. It says the order matters and forbids repeats. A model left to itself repeats its first answer
#:    to fill three slots, which looks like confidence and carries no information.
PURPOSE_RANK3_PROMPT = """Du ordnest ein Unternehmen einer Branche zu.

Du liest den GEGENSTAND DES UNTERNEHMENS aus dem Handelsregister. Das ist der Zweck, den das
Unternehmen bei der Eintragung angegeben hat — nicht unbedingt das, was es heute wirklich tut.

Waehle die DREI am besten passenden Branchen aus der folgenden Liste, die beste zuerst.
Erfinde keine andere Branche. Nenne keine Branche zweimal.

Branchen:
{options}

Antworte stattdessen "{none}", wenn keine davon passt. Das ist bei Holdinggesellschaften,
Vermoegensverwaltung, Beteiligungsgesellschaften und rein verwaltenden Zwecken fast immer der
Fall — "{none}" ist dort die richtige Antwort, keine Ausrede.

Antworte "{unsure}", wenn der Text zu wenig sagt, um es zu entscheiden.

Unternehmen: {company}

Gegenstand des Unternehmens:
{text}

Antworte als JSON:
{{"branchen": ["<beste>", "<zweitbeste>", "<drittbeste>"], "begruendung": "<max 15 Woerter>"}}
Oder: {{"branchen": "{none}", "begruendung": "<max 15 Woerter>"}}
"""

#: Output budget for the ranked ask. Three German field names plus a 15-word reason does not fit the
#: 120 tokens a single-answer call gets — measured names run to 30 characters ("Gesundheitsversorgung
#: & Pflege"), so three of them plus JSON punctuation is most of that budget before the reason starts.
#: A truncated third entry is handled (it parses as two ranks) but a truncated reason is a silent loss
#: of the only human-readable evidence on the row.
RANK3_OUTPUT_TOKENS = int(os.getenv("BRANCH_LLM_RANK3_OUTPUT_TOKENS", "260"))

#: The second job: read a page and say what the business IS CALLED, never which field it belongs to.
#:
#: **There is deliberately no field list in this prompt.** That absence is the whole safety property.
#: A model shown 77 field names invents a 78th that sounds like the others — measured on the tie-break
#: run, it produced "Immersive Technologien" and "Beratende Dienstleistung", neither of which exists.
#: With no list in front of it there is nothing to hallucinate against: it can only report words, and
#: a word is checkable against the page in a way a field name is not.
#:
#: It asks for the words a German business directory would use, and asks for them *from the page*,
#: because a paraphrase the company never wrote cannot help the matcher. See `run_combined_discovery`
#: `propose-keywords`, which throws away every proposal that does not actually appear in the text.
VOCAB_PROMPT = """Du liest die Webseite eines deutschen Unternehmens.

Nenne die {max_words} wichtigsten Substantive, die beschreiben, WAS dieses Unternehmen
herstellt oder anbietet. Nimm Woerter, die so oder so aehnlich auf der Seite stehen.

Keine Adjektive. Keine Werbewoerter wie "Qualitaet", "Service", "Loesungen", "Beratung".
Keine Orte, keine Firmennamen. Nur die Sache selbst.

Beispiele fuer gute Antworten: "Messebau", "Ingenieurbau", "Eiscafe", "Matratzen",
"Videoueberwachung", "Expresstransport".

Unternehmen: {company}

Text von der Webseite:
{text}

Antworte als JSON:
{{"woerter": ["...", "..."]}}
"""

PROMPT = """Du ordnest ein Unternehmen einer Branche zu.

Waehle GENAU EINE der folgenden Branchen, oder "{none}", wenn keine davon passt.
Erfinde keine andere Branche.

Branchen:
{options}

Unternehmen: {company}

Text von der Webseite:
{text}

Antworte als JSON:
{{"branche": "<Name aus der Liste oder {none}>", "begruendung": "<max 15 Woerter>"}}
"""


@dataclass
class BranchVerdict:
    """What the model said, after the answer was checked against what it was offered."""

    #: One of the shortlisted branches, or None when the model said "keine" or answered off-list.
    branch: str | None
    reason: str
    #: True when the model explicitly rejected all three. A human should look at this row.
    rejected_all: bool = False
    #: Set when the answer had to be thrown away, with what it said. Nothing is decided from this.
    refused: str = ""
    #: True when this came back from the cache rather than the model.
    cached: bool = False


@dataclass
class VocabularyProposal:
    """The words a model thinks describe one company. **Never a field, and never an answer.**

    These go into a worksheet a person curates, and only the words they accept reach
    `branch_taxonomy.yaml`. Nothing here can move the Technologiefeld column — that is the difference
    between this and `choose()`, and it is why this one is allowed to run on companies where the
    keywords found nothing at all.
    """

    #: Normalised, lower case, so a proposal and a keyword can be compared.
    words: list[str]
    #: Set when the answer had to be thrown away, with why. Nothing is proposed from this.
    refused: str = ""
    #: True when this came back from the cache rather than the model.
    cached: bool = False


def availability() -> tuple[bool, str]:
    """Can the model be reached? Checked against Ollama only — no search engine is involved here."""
    try:
        from ollama_search_discovery.ollama_http import require_ollama

        require_ollama()
    except OllamaUnavailableError as exc:
        return False, str(exc)
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"
    return True, f"Ollama model {OLLAMA_MODEL} at {OLLAMA_BASE_URL}"


def _cache_key(url: str, shortlist: list[str], taxonomy_version: str) -> str:
    return f"{taxonomy_version}|{OLLAMA_MODEL}|{url}|{'|'.join(shortlist)}"


def verdict_from_answer(answer: str, reason: str, shortlist: list[str]) -> BranchVerdict:
    """Turn what the model said into a verdict. Rules 1 and 2, and the only place they live.

    Pulled out of `choose` so that `scripts/check_branch_classifier.py` can call **this** rather
    than restate it. A check that rebuilds the rule it is checking agrees with itself whatever the
    real code does — the same reason `scripts/replay_judge_gate.py` imports the website judge's gate
    instead of copying it.

    Three answers are possible and they are not three shades of one thing:

    - a branch **from the list** decides the row;
    - `keine` means the model read the page and rejected all three, which flags the row for a human
      and is *not* the same as no keyword firing;
    - anything else decides nothing. A model that invents a branch name has not chosen, it has
      generated, and the row keeps whatever the keywords said.

    The comparison ignores case and the punctuation a model puts in front of a list item, because
    `- Bahnindustrie` and `bahnindustrie` are the model picking option one, not the model going
    off-list. Being strict there does not make the answer safer; it silently converts a correct
    choice into a refusal, and nothing in the report would say so.
    """
    cleaned = (answer or "").strip().strip("-*•\"'` \t").strip()
    if not cleaned:
        return BranchVerdict(branch=None, reason=reason, refused="empty answer")
    if cleaned.casefold() == NONE_ANSWER:
        return BranchVerdict(branch=None, reason=reason, rejected_all=True)
    for option in shortlist:
        if cleaned.casefold() == option.casefold():
            # The branch as the TAXONOMY spells it, never as the model spelled it. This value is
            # written to a column, and "bahnindustrie" is not a Berlin field name.
            return BranchVerdict(branch=option, reason=reason)
    return BranchVerdict(branch=None, reason=reason, refused=f"off-list: {cleaned[:60]}")


def choose(
    *,
    company_name: str,
    text: str,
    shortlist: list[str],
    url: str,
    taxonomy_version: str,
    cached_only: bool = False,
) -> BranchVerdict:
    """Pick one of `shortlist`, or reject them all. Never raises.

    A model that is unreachable, slow or incoherent has to behave like a model that was never asked:
    the keyword answer stands and the row stays `torn`. A tie-breaker that can fail a whole run is
    worse than no tie-breaker.

    `cached_only` is how a caller declines to spend a NEW call on this row — currently
    `DISCOVERY_ACTIVE_ONLY` for a company the register no longer lists as `aktiv`. It stops short of
    the model but still returns a verdict already in the cache, and that distinction is the point:
    skipping the lookup as well would rewrite this file as though the model had never spoken about
    those rows, dropping counts on a free re-run with nothing to say why. Same rule as the purpose
    column's `tier_on`.
    """
    if not shortlist or not text:
        return BranchVerdict(branch=None, reason="", refused="nothing to choose between")

    key = _cache_key(url, shortlist, taxonomy_version)
    cached = page_cache.get("branch-llm", key)
    if cached is not None:
        return BranchVerdict(
            branch=cached.get("branch"),
            reason=cached.get("reason") or "",
            rejected_all=bool(cached.get("rejected_all")),
            refused=cached.get("refused") or "",
            cached=True,
        )

    if cached_only:
        # Not asked, and honest about which kind of not-asked it is: `refused` is what every other
        # unanswered path sets, so the row stays exactly as the keywords left it — `torn`.
        return BranchVerdict(branch=None, reason="", refused="not asked: not aktiv")

    prompt = PROMPT.format(
        none=NONE_ANSWER,
        options="\n".join(f"- {name}" for name in shortlist),
        company=company_name or "(unbekannt)",
        text=text[:MAX_CHARS],
    )
    try:
        result = run_chat_prompt(
            prompt,
            num_predict=MAX_OUTPUT_TOKENS,
            think=False,
            temperature=0.0,
            num_ctx=NUM_CTX,
        )
        parsed = result.get("parsed") or extract_json_block(result.get("raw_text") or "")
    except Exception as exc:  # noqa: BLE001
        log.debug("branch model failed for %s: %s", url, type(exc).__name__)
        return BranchVerdict(branch=None, reason="", refused=f"{type(exc).__name__}")

    answer = str(parsed.get("branche") or "").strip()
    reason = str(parsed.get("begruendung") or "").strip()[:200]

    verdict = verdict_from_answer(answer, reason, shortlist)
    if verdict.refused:
        # Rule 1. An off-list answer decides nothing — and it is worth a log line, because a model
        # doing this often means the prompt stopped being a multiple-choice question.
        log.info("branch model answered off-list for %s: %r", url, answer[:60])

    page_cache.put(
        "branch-llm",
        key,
        {
            "branch": verdict.branch,
            "reason": verdict.reason,
            "rejected_all": verdict.rejected_all,
            "refused": verdict.refused,
        },
    )
    return verdict


def propose_vocabulary(
    *,
    company_name: str,
    text: str,
    url: str,
    taxonomy_version: str,
) -> VocabularyProposal:
    """Which words describe this company? Asked where **no keyword fired at all**. Never raises.

    The other half of the model's work, and the half that is safe to run without a shortlist. `choose`
    is closed multiple choice: the keywords have already narrowed the answer to two or three fields, so
    the model only has to compare them. Here nothing fired, so there is nothing to compare — and asking
    it to *pick a field* would be asking it to *invent one*.

    The numbers say so. On the 2026-08-25 tie-break run the model said "none of these" on 15% of rows.
    For the companies this function targets the true answer is "none" roughly half the time. A model
    that under-refuses by a factor of three, handed 77 options, produces confident wrong fields.

    So it is asked for **words**, and the words are then checked against the page by the ordinary
    matcher. A word that really appears is a candidate keyword a person can place in
    `branch_taxonomy.yaml`; a word that does not appear is a paraphrase and is discarded. The field
    itself is assigned on the next `classify` run, by keyword matching, with no model in its
    provenance.

    Cached in its own namespace so the two jobs never share an entry, and keyed on the taxonomy version
    like `choose` — editing the keyword list invalidates exactly the proposals that could have changed.
    """
    if not text:
        return VocabularyProposal(words=[], refused="nothing to read")

    key = f"{taxonomy_version}|{OLLAMA_MODEL}|{url}"
    cached = page_cache.get("branch-vocab", key)
    if cached is not None:
        return VocabularyProposal(
            words=list(cached.get("words") or []),
            refused=cached.get("refused") or "",
            cached=True,
        )

    prompt = VOCAB_PROMPT.format(
        max_words=MAX_PROPOSED_WORDS,
        company=company_name or "(unbekannt)",
        text=text[:MAX_CHARS],
    )
    try:
        result = run_chat_prompt(
            prompt,
            num_predict=MAX_OUTPUT_TOKENS,
            think=False,
            temperature=0.0,
            num_ctx=NUM_CTX,
        )
        parsed = result.get("parsed") or extract_json_block(result.get("raw_text") or "")
    except Exception as exc:  # noqa: BLE001  an unreachable model is an expected outcome
        log.debug("vocabulary model failed for %s: %s", url, type(exc).__name__)
        return VocabularyProposal(words=[], refused=f"{type(exc).__name__}")

    proposal = proposal_from_answer(parsed.get("woerter"))
    if proposal.refused:
        log.info("vocabulary model gave nothing usable for %s: %s", url, proposal.refused)

    page_cache.put(
        "branch-vocab", key, {"words": proposal.words, "refused": proposal.refused}
    )
    return proposal


def proposal_from_answer(raw: object) -> VocabularyProposal:
    """Turn whatever came back into a clean word list. Pure, so the guard can call it.

    Normalised through `branch_classifier.normalize` — the same function the keywords and the page text
    go through — because that is the only reason a proposal and a keyword can be compared at all. The
    taxonomy came out of a spreadsheet carrying non-breaking spaces inside words, and a model will
    happily hand back `"Messebau "` or `"MESSEBAU"`.

    Anything that is not a short word or phrase is dropped. A model asked for nouns sometimes returns a
    sentence, and a sentence can never match a page as a keyword would.
    """
    from combined_discovery.branch_classifier import normalize

    if not isinstance(raw, list):
        return VocabularyProposal(words=[], refused=f"expected a list, got {type(raw).__name__}")

    words: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        word = normalize(item).strip(" .,;:-\"'")
        # Two characters is the floor `load_taxonomy` uses for a keyword; five words is well past
        # anything that reads as the name of a trade.
        if len(word) >= 2 and len(word.split()) <= 5 and word not in words:
            words.append(word)

    if not words:
        return VocabularyProposal(words=[], refused="no usable word in the answer")
    return VocabularyProposal(words=words[:MAX_PROPOSED_WORDS])


#: The punctuation a model puts around a list item it has picked. Stripped before comparing, because
#: a leading dash is the model choosing option one, not the model going off-list.
#:
#: Kept as a constant used only by `purpose_verdict_from_answer`. `verdict_from_answer` still has the
#: same characters written inline, deliberately: that function's behaviour is pinned by
#: `check_branch_classifier.py:315` for the Technologiefeld column, and a shared constant is a shared
#: fate — one edit would silently change two columns.
_LIST_PUNCTUATION = "-*•\"'` \t"


def _strip_list_punctuation(answer: str) -> str:
    return (answer or "").strip().strip(_LIST_PUNCTUATION).strip()


@dataclass
class PurposeVerdict:
    """What a model made of one register purpose. Four states, and they are not four shades of one.

    `branch` set          -> it chose, and the value is the taxonomy's spelling of a real field.
    `rejected_all`        -> it read the text and said no field fits. A finding about the company.
    `unsure`              -> it read the text and said it cannot tell. A finding about the evidence.
    `refused`             -> we never got a usable answer. NOT a finding about anything.

    That last distinction is the one that keeps getting lost. A timeout is not a refusal, and writing
    it as one would report "the model says this company has no technology field" when the truth is
    "the model never answered". The caller must leave such a row `torn` or `no_keyword`.
    """

    branch: str | None
    reason: str
    #: The model answered `keine`: none of the options offered fits this company.
    rejected_all: bool = False
    #: The model answered `unsicher`: the register text does not say enough to decide.
    unsure: bool = False
    #: Set when the answer could not be used at all, with why. Never an answer.
    refused: str = ""
    #: True when this came back from the cache rather than the model.
    cached: bool = False
    #: Which model produced it, so two runs can be told apart and a cheap model's answers can be
    #: found and re-asked later without re-asking everything.
    model_name: str = ""
    #: The on-list fields the model put in order, best first. Empty unless the ranked prompt was used.
    #:
    #: Names only, never scores. A model asked for a confidence emits a plausible decimal that is not
    #: a measurement, and it would end up beside the keyword classifier's real weights. `branch` stays
    #: the rank-1 entry so every existing caller keeps working without knowing this field exists.
    ranked: list[str] = field(default_factory=list)

    @property
    def abstained(self) -> bool:
        """Did the model give a real non-answer, as opposed to failing? Excludes `refused`."""
        return self.rejected_all or self.unsure


def purpose_verdict_from_answer(
    answer: str, reason: str, options: list[str]
) -> PurposeVerdict:
    """Turn what the model said into a verdict. The only place these rules live.

    Deliberately a **separate function** from `verdict_from_answer` rather than a flag on it.
    `scripts/check_branch_classifier.py:315` pins that function's behaviour for the Technologiefeld
    tie-break, and adding a third answer to it would change a rule that column depends on. Two
    callers, two rules, two guards.

    Four answers, in the order they are tested:

    - empty -> refused. A model that returned nothing has not abstained, it has failed.
    - `unsicher` -> unsure. Tested BEFORE the options list, so a taxonomy that ever contained a field
      called "Unsicher" could not shadow it.
    - `keine` -> rejected_all.
    - an option, case-insensitively and with list punctuation stripped -> that option, spelled as the
      TAXONOMY spells it. A leading dash is the model picking an option, not going off-list; being
      strict there converts a correct choice into a refusal and nothing in the report would say so.
    - anything else -> refused. A model that invents a field name has generated, not chosen.
    """
    cleaned = _strip_list_punctuation(answer)
    if not cleaned:
        return PurposeVerdict(branch=None, reason=reason, refused="empty answer")
    folded = cleaned.casefold()
    if folded == UNSURE_ANSWER:
        return PurposeVerdict(branch=None, reason=reason, unsure=True)
    if folded == NONE_ANSWER:
        return PurposeVerdict(branch=None, reason=reason, rejected_all=True)
    for option in options:
        if folded == option.casefold():
            # The field as the TAXONOMY spells it, never as the model spelled it. This value is
            # written to a column, and "gastgewerbe" is not a Berlin field name.
            return PurposeVerdict(branch=option, reason=reason)
    return PurposeVerdict(branch=None, reason=reason, refused=f"off-list: {cleaned[:60]}")


def ranked_purpose_verdict_from_answer(
    answer: object, reason: str, options: list[str]
) -> PurposeVerdict:
    """Turn a ranked answer into a verdict. `branch` is rank 1; `ranked` is the whole ordering.

    A sibling of `purpose_verdict_from_answer` rather than a flag on it, for the reason that function
    gives for being a sibling of `verdict_from_answer`: its behaviour is pinned by a guard, and adding
    a list-shaped answer to it would change a rule two columns depend on. Three callers, three rules,
    three guards.

    Every rule of the single-answer parser holds **per entry**: case-insensitive comparison, list
    punctuation stripped, and the field returned as the TAXONOMY spells it. On top of that:

    - **Each entry is judged alone.** An off-list entry at rank 2 must not discard rank 1 — a model
      that gets one right and then drifts has still got one right, and throwing the row away would
      report "no answer" for a row that was answered.
    - **Duplicates collapse**, first occurrence kept. A model told to name three fields will repeat
      its favourite to fill the slots, and three copies of one answer is one answer.
    - `keine` / `unsicher` are answered INSTEAD of a list. Accepted as a bare string, and also as a
      one-entry list, because a model told to answer with a list sometimes wraps its abstention in
      one. Tested before the options, so a taxonomy field named "Unsicher" could not shadow it.
    - **Nothing usable at all -> refused**, never an abstention. The row keeps what the keywords said.
    """
    # A bare string is either an abstention or a single answer; either way the one-answer parser
    # already owns those rules exactly, so defer to it rather than restating them.
    if isinstance(answer, str):
        verdict = purpose_verdict_from_answer(answer, reason, options)
        if verdict.branch:
            verdict.ranked = [verdict.branch]
        return verdict

    if not isinstance(answer, (list, tuple)):
        return PurposeVerdict(branch=None, reason=reason, refused="empty answer")

    entries = [_strip_list_punctuation(str(item)) for item in answer]
    entries = [item for item in entries if item]
    if not entries:
        return PurposeVerdict(branch=None, reason=reason, refused="empty answer")

    # An abstention wrapped in a list. Checked across every entry, not just the first: a model that
    # answers ["keine"] and one that answers ["Handel", "keine"] are both telling us it wants out,
    # and treating the second as a clean pick of "Handel" would read a hedge as a decision.
    folded = [item.casefold() for item in entries]
    if UNSURE_ANSWER in folded:
        return PurposeVerdict(branch=None, reason=reason, unsure=True)
    if NONE_ANSWER in folded:
        return PurposeVerdict(branch=None, reason=reason, rejected_all=True)

    by_fold = {option.casefold(): option for option in options}
    ranked: list[str] = []
    for item in folded:
        option = by_fold.get(item)
        # Off-list entries are skipped, not fatal, and duplicates keep their first position.
        if option is not None and option not in ranked:
            ranked.append(option)

    if not ranked:
        return PurposeVerdict(
            branch=None, reason=reason, refused=f"off-list: {entries[0][:60]}",
        )
    return PurposeVerdict(branch=ranked[0], reason=reason, ranked=ranked)


def purpose_cache_key(
    *, taxonomy_version: str, model_name: str, company_id: str, options: list[str]
) -> str:
    """The cache key for one purpose answer. Shared, so two backends cannot disagree about it.

    Four parts, each earning its place:

    - `taxonomy_version` — editing the field list or the matching rule invalidates exactly the answers
      that could have changed, and nothing else.
    - `model_name` — so a cheap model's answers and an expensive one's coexist. Re-asking with a
      better model costs only the new calls, and the old answers stay readable for comparison.
    - `company_id` — a register purpose has no URL. This is the only stable handle.
    - `options` — a 3-way answer must never be served to a 77-way question. The two tiers also use
      different namespaces, so this is the second of two independent guards on that.
    """
    return f"{taxonomy_version}|{model_name}|{company_id}|{'|'.join(options)}"


def cached_purpose_verdict(
    *,
    options: list[str],
    company_id: str,
    taxonomy_version: str,
    namespace: str = "purpose-llm-tiebreak",
    model_name: str | None = None,
) -> PurposeVerdict | None:
    """An answer already paid for, or None. **Never calls the model.**

    This exists so a free run can still show what the model said last time. Without it, re-running
    `classify-purpose` with the tier switched off rewrites the output file as though no model had ever
    answered — the cache still holds every verdict, but the file a person reads loses them, and the
    counts drop with no explanation. That is a silently wrong number, which is the one kind of bug this
    codebase treats as unacceptable.

    So the caller applies whatever this returns on every run, and the `--llm` flag governs only whether
    a **new** call may be made.
    """
    model = model_name or OLLAMA_MODEL
    if not options or not company_id:
        return None
    cached = page_cache.get(namespace, purpose_cache_key(
        taxonomy_version=taxonomy_version, model_name=model,
        company_id=company_id, options=options,
    ))
    if cached is None:
        return None
    return PurposeVerdict(
        branch=cached.get("branch"),
        reason=cached.get("reason") or "",
        rejected_all=bool(cached.get("rejected_all")),
        unsure=bool(cached.get("unsure")),
        refused=cached.get("refused") or "",
        cached=True,
        model_name=cached.get("model_name") or model,
    )


def choose_for_purpose(
    *,
    company_name: str,
    purpose: str,
    options: list[str],
    company_id: str,
    taxonomy_version: str,
    namespace: str = "purpose-llm-tiebreak",
    model_name: str | None = None,
    ranked: bool = False,
) -> PurposeVerdict:
    """Read a register purpose and pick one of `options`, or abstain. Never raises.

    Used for **Tier A**, the 3-way tie-break, where `options` is the shortlist the keywords already
    nominated. The same function serves Tier B's 77-way ask with a different `namespace` — the only
    thing that differs is the length of the list, and keeping one code path means the answer
    validation, the caching and the failure handling cannot drift apart between the two tiers.

    `ranked=True` asks for the best three in order instead of one, and is the third caller through
    this same path for the same reason. It changes the prompt, the output budget and the parser, and
    nothing else. **The caller must pass a different `namespace` with it** — see the warning on
    `purpose_cache_key`: the prompt shape is not part of the cache key, so a ranked ask against the
    single-answer namespace is served the old one-answer verdicts and writes them as though they were
    ranked. `classify-purpose` passes `purpose-llm-direct-rank3`, and a guard asserts it differs.

    Keyed on **`company_id`, not a URL**: a register purpose has no URL, and the company id is the
    only stable handle. The model name is in the key, so switching models creates new entries and
    leaves the old ones — which is what makes comparing two models cheap later, and what lets a
    hosted model share this namespace with the local one without either invalidating the other.

    `model_name` exists only to label and key an answer that came from somewhere else; this function
    always asks Ollama. A hosted backend computes the same key with its own model id and writes into
    the same namespace, so `classify-purpose` reads both without knowing which produced which.

    Never raises, for the same reason `choose` does not: a model that is unreachable, slow or
    incoherent must behave like a model that was never asked. The row stays `torn` (Tier A) or
    `no_keyword` (Tier B) with `llm_refused` set, and the run continues.
    """
    if not options or not purpose:
        return PurposeVerdict(branch=None, reason="", refused="nothing to choose between")

    model = model_name or OLLAMA_MODEL
    key = purpose_cache_key(
        taxonomy_version=taxonomy_version, model_name=model,
        company_id=company_id, options=options,
    )
    cached = page_cache.get(namespace, key)
    if cached is not None:
        return PurposeVerdict(
            branch=cached.get("branch"),
            reason=cached.get("reason") or "",
            rejected_all=bool(cached.get("rejected_all")),
            unsure=bool(cached.get("unsure")),
            refused=cached.get("refused") or "",
            cached=True,
            model_name=cached.get("model_name") or model,
            # Absent from every entry written before the ranked tier existed, which is correct: those
            # answers had no ordering. `list(... or [])` so an old entry reads as "no ranking", not
            # as a crash.
            ranked=list(cached.get("ranked") or []),
        )

    template = PURPOSE_RANK3_PROMPT if ranked else PURPOSE_PROMPT
    prompt = template.format(
        none=NONE_ANSWER,
        unsure=UNSURE_ANSWER,
        options="\n".join(f"- {name}" for name in options),
        company=company_name or "(unbekannt)",
        text=purpose[:MAX_CHARS],
    )
    try:
        result = run_chat_prompt(
            prompt,
            num_predict=RANK3_OUTPUT_TOKENS if ranked else MAX_OUTPUT_TOKENS,
            # Every one of these is measured, not taste. `think=False` because with thinking on the
            # model spends the whole 120-token budget reasoning and returns an empty string, three
            # times out of three. `temperature=0.0` so the same prompt gives the same answer and "the
            # number changed" means the code changed. `num_ctx` explicit because Ollama otherwise
            # truncates silently, and what it cuts is the text at the END of the prompt — which here
            # is the register purpose itself.
            think=False,
            temperature=0.0,
            num_ctx=NUM_CTX,
        )
        parsed = result.get("parsed") or extract_json_block(result.get("raw_text") or "")
    except Exception as exc:  # noqa: BLE001  an unreachable model is an expected outcome
        log.debug("purpose model failed for %s: %s", company_id, type(exc).__name__)
        # NOT cached: a transient outage must be retried on the next run, unlike a real answer.
        return PurposeVerdict(
            branch=None, reason="", refused=f"{type(exc).__name__}", model_name=model,
        )

    reason = str(parsed.get("begruendung") or "").strip()[:200]

    if ranked:
        # `branchen` (plural) for the list, and `branche` accepted as a fallback: a model given a
        # list-shaped example still sometimes answers the singular key it saw in every other prompt,
        # and treating that as an empty answer would throw away a usable pick.
        raw = parsed.get("branchen")
        if raw is None:
            raw = str(parsed.get("branche") or "").strip()
        verdict = ranked_purpose_verdict_from_answer(raw, reason, options)
        logged = str(raw)
    else:
        answer = str(parsed.get("branche") or "").strip()
        verdict = purpose_verdict_from_answer(answer, reason, options)
        logged = answer

    verdict.model_name = model
    if verdict.refused:
        # An off-list answer decides nothing, and it is worth a log line: a model doing this often
        # means the prompt has stopped reading as a multiple-choice question.
        log.info("purpose model answered off-list for %s: %r", company_id, logged[:60])

    page_cache.put(namespace, key, {
        "branch": verdict.branch,
        "reason": verdict.reason,
        "rejected_all": verdict.rejected_all,
        "unsure": verdict.unsure,
        "refused": verdict.refused,
        "model_name": verdict.model_name,
        # Written on every entry, ranked ask or not. An empty list on a single-answer entry is the
        # truth about it, and it keeps the cached shape uniform for whoever reads this next.
        "ranked": verdict.ranked,
    })
    return verdict
