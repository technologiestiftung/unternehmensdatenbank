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
   logged. A model that invents a branch name has not chosen; it has generated.
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
from dataclasses import dataclass

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


def choose(
    *,
    company_name: str,
    text: str,
    shortlist: list[str],
    url: str,
    taxonomy_version: str,
) -> BranchVerdict:
    """Pick one of `shortlist`, or reject them all. Never raises.

    A model that is unreachable, slow or incoherent has to behave like a model that was never asked:
    the keyword answer stands and the row stays `torn`. A tie-breaker that can fail a whole run is
    worse than no tie-breaker.
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

    if answer.lower() == NONE_ANSWER:
        verdict = BranchVerdict(branch=None, reason=reason, rejected_all=True)
    elif answer in shortlist:
        verdict = BranchVerdict(branch=answer, reason=reason)
    else:
        # Rule 1. An off-list answer decides nothing — and it is worth a log line, because a model
        # doing this often means the prompt stopped being a multiple-choice question.
        log.info("branch model answered off-list for %s: %r", url, answer[:60])
        verdict = BranchVerdict(branch=None, reason=reason, refused=f"off-list: {answer[:60]}")

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
