"""Let a language model on this machine read a page we already downloaded and say whose it is.

## What this is for

`url_patterns.py` guesses a domain from a company's name and downloads whatever answers. That
leaves a question string matching is bad at: **is this page the company's own website, or a page
about the company?** A business directory prints the name, the address and the commercial-register
number of every company it lists. So does a register extract, an insolvency notice and a news
article. All of them look like a match to a rule that only asks "do these strings appear".

Measured on 521 Osnabrück companies: of the 333 we found no website for, **250 already had a page
downloaded**. The guesser did its job and the scoring would not sign off. That is the set this
module exists for.

## What it deliberately does NOT do

It never proposes a URL. Asking a model "what is this company's web address" is a question about
what it memorised during training, and for a small Osnabrück business it memorised nothing — so it
produces a plausible, well-shaped, invented domain, indistinguishable from a real answer. This
module only ever chooses among pages something else already fetched. The worst it can do is
misjudge a real page.

It also does not produce a confidence number. It emits **evidence** — the same kind of named flags
the hand-written checks emit — and `confidence.py` turns evidence into a number, as the only place
in this package that is allowed to. See the limits documented at the top of that file.

## Why anything it says is safe to use

Every number the model hands back is checked against the page before it counts:

- It transcribes a register number; `_number_is_on_page` confirms those digits really are in the
  downloaded text, and only then does `confidence.register_digits` compare them with the company's
  own. The model is given the company's register number in the prompt, so without that check it
  could simply copy it back and manufacture a match.
- A register **mismatch** is never taken from the model. A mismatch disqualifies a URL outright,
  and one misread digit would kill a correct website. Only the regex extraction, which quotes the
  page verbatim, is trusted to say that.

So the pattern throughout is: the model reads, code verifies. Anything it says that cannot be
checked is worth one weighted opinion and nothing more.

## Running it

Needs Ollama and nothing else — **no SearXNG**. Do not gate this stage on
`local_llm.availability()`, which also probes SearXNG and always fails on this network.

Measured 2026-08-12: `qwen3.5:4b` on a native Windows Ollama with `OLLAMA_IGPU_ENABLE=1` does
149 prompt tokens/s and 13.4 generated tokens/s. A 6,000-character page is about 1,700 tokens, so
one judgement costs roughly 15 seconds. Inside Docker on the same machine it was 0.36 tokens/s,
because the host had paged the virtual machine's memory to disk — if it is suddenly 40x slower
than this, that is the thing to check, not the model.

`OLLAMA_HTTP_TIMEOUT_S` defaults to 120 seconds, which is comfortable at that speed but not if a
larger model is swapped in. Raise it before blaming the judge for timing out.
"""
from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

from ollama_search_discovery.ollama_http import (
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    OllamaUnavailableError,
    require_ollama,
    run_chat_prompt,
)

from combined_discovery import confidence as conf

log = logging.getLogger(__name__)

#: How much page text the model is shown. The Impressum goes first because it is where a German
#: business identifies itself; the home page fills whatever is left. This is the main lever on how
#: long a run takes: prompt processing is measured at ~149 tokens/s, so every 400 characters costs
#: about another second per page.
MAX_CHARS = int(os.getenv("COMBINED_LLM_JUDGE_MAX_CHARS", "6000"))

#: Output tokens allowed. The answer is a four-field JSON object; 200 is generous. Kept small
#: because generation is 10x slower than reading.
MAX_OUTPUT_TOKENS = int(os.getenv("COMBINED_LLM_JUDGE_MAX_OUTPUT_TOKENS", "200"))

#: Whether to let a reasoning model think out loud first. Off: thinking tokens are generated at
#: the slow rate and this is a small comprehension task, not a puzzle.
THINK = os.getenv("COMBINED_LLM_JUDGE_THINK", "false").lower() == "true"

VERDICT_OWN_SITE = "own_site"
VERDICT_ABOUT_COMPANY = "about_company"
VERDICT_UNRELATED = "unrelated"
VERDICT_UNSURE = "unsure"

VERDICTS = (VERDICT_OWN_SITE, VERDICT_ABOUT_COMPANY, VERDICT_UNRELATED, VERDICT_UNSURE)

#: Verdict -> the evidence name it becomes. `unsure` maps to nothing, which is the point of having
#: it: a model with no opinion should leave the score exactly as the hand-written rules left it.
_VERDICT_FLAGS = {
    VERDICT_OWN_SITE: conf.LLM_OWN_SITE,
    VERDICT_ABOUT_COMPANY: conf.LLM_ABOUT_COMPANY,
    VERDICT_UNRELATED: conf.LLM_UNRELATED,
}

PROMPT = """You are checking whether one downloaded web page belongs to one specific German company.

The company we are looking for:
  Name:     {name}
  Address:  {address}
  Register: {register}

The page that was downloaded:
  URL: {url}

The text of that page (its Impressum first, then its home page):
---
{text}
---

Choose exactly one verdict:

  own_site       The company PUBLISHES this page. It is their own website.
  about_company  The page is about this company but belongs to somebody else — a business
                 directory, a commercial-register extract, an insolvency notice, a news
                 article, a job advert, a review site, a supplier listing.
  unrelated      The page belongs to a different business, or has nothing to do with this
                 company at all.
  unsure         The text does not let you tell.

The case that matters most, and the one that is easy to get wrong: a page can print this
company's name, its address and even its commercial-register number and still be
`about_company`. Directories quote all of it. Ask who PUBLISHES the page, not who it mentions.
A page written in the first person about its own products and staff is `own_site`. A page that
lists this company among others, or describes it from the outside, is not.

Two German companies sharing one word in their names are usually unrelated businesses.

But do not use a small difference in the NAME to rule a page out. German businesses trade under
names that do not match their register entry exactly — an extra word, a brand, "Studios",
"Gruppe", "Inhaber", a shortened form, or a different legal form. If the register number on the
page equals the one above, it is the SAME legal entity and the answer is `own_site` however the
name is written. Where there is no register number, an address in the same town plus a plausible
version of the name is enough — the question is whether this business publishes the page, not
whether the name is spelled identically.

Then copy out these two things, but ONLY if they literally appear in the text above. Write null
if they do not. Do not reproduce anything from the company details at the top of this message —
only what the page itself says.

  register_number  the commercial-register entry printed on the page, e.g. "HRB 213220"
  postcode         the five-digit German postcode in the page's own imprint

Answer with only this JSON object and nothing else:
{{"verdict": "unsure", "register_number": null, "postcode": null, "reason": "one short sentence"}}"""


@dataclass
class JudgeResult:
    """What the model said about one page, before any of it has been believed."""

    verdict: str = VERDICT_UNSURE
    #: Transcribed from the page by the model. Worthless until `_number_is_on_page` agrees.
    register_number: str | None = None
    postcode: str | None = None
    reason: str = ""
    elapsed_s: float = 0.0
    error: str | None = None

    @property
    def usable(self) -> bool:
        return self.error is None and self.verdict in VERDICTS


def availability() -> tuple[bool, str]:
    """Can this stage run right now? Returns (usable, one-line reason).

    Ollama only. This stage reads pages that have already been downloaded, so it needs no search
    engine — which is the entire reason it exists as its own module rather than an option on
    `local_llm.py`.
    """
    try:
        require_ollama()
    except OllamaUnavailableError as exc:
        return False, f"Ollama unusable at {OLLAMA_BASE_URL}: {exc}"
    except Exception as exc:  # noqa: BLE001
        return False, f"Ollama check failed: {exc}"
    return True, f"Ollama model {OLLAMA_MODEL} at {OLLAMA_BASE_URL}"


def build_text(page_text: str, imprint_text: str) -> str:
    """The page as the model will see it: Impressum first, home page with what is left over.

    The Impressum comes first because it is the page a German business is legally required to use
    to identify itself, so it holds the register number, the address and the owner. Truncating the
    home page loses navigation menus; truncating the Impressum loses the answer.
    """
    imprint = (imprint_text or "").strip()
    page = (page_text or "").strip()
    if not imprint:
        return page[:MAX_CHARS]
    head = imprint[:MAX_CHARS]
    remaining = MAX_CHARS - len(head)
    if remaining <= 200 or not page:
        return head
    return f"{head}\n\n--- home page ---\n{page[:remaining]}"


def _number_is_on_page(digits: str, text: str) -> bool:
    """Do these digits actually appear in the downloaded text as a number of their own?

    The model is told the company's register number, so it can hand it straight back whether the
    page carries it or not. This is the check that makes its transcription worth trusting.

    Word boundaries matter: "882" must not be found inside "172907" or a phone number, or a
    company with a short register number would match half the internet.
    """
    if not digits or not text:
        return False
    return re.search(rf"(?<!\d){re.escape(digits)}(?!\d)", text) is not None


def judge(
    company: Any,
    *,
    url: str,
    page_text: str = "",
    imprint_text: str = "",
) -> JudgeResult:
    """Ask the model about one page. One HTTP call, never raises for a bad answer.

    A model that fails, times out or answers with nonsense produces `error` and the `unsure`
    verdict, which contributes no evidence at all. That is deliberate: a broken judge must leave
    the hand-written scoring exactly as it found it, not degrade it.
    """
    started = time.monotonic()
    result = JudgeResult()

    text = build_text(page_text, imprint_text)
    if not text.strip():
        result.error = "no_page_text"
        result.elapsed_s = round(time.monotonic() - started, 2)
        return result

    address = " ".join(
        part
        for part in [company.address or "", company.postcode or "", company.city or ""]
        if part
    ).strip()

    prompt = PROMPT.format(
        name=company.company_name,
        address=address or "unknown",
        register=company.registration_number or "unknown",
        url=url,
        text=text,
    )

    try:
        # Temperature 0: the same page must give the same verdict twice, or `band_without_judge`
        # becomes noise and re-running a company looks like a code change.
        response = run_chat_prompt(
            prompt, num_predict=MAX_OUTPUT_TOKENS, think=THINK, temperature=0.0
        )
        parsed = response.get("parsed") or {}
        verdict = str(parsed.get("verdict") or "").strip().lower()
        result.verdict = verdict if verdict in VERDICTS else VERDICT_UNSURE
        if verdict and verdict not in VERDICTS:
            log.debug("model returned an unknown verdict %r for %s", verdict, url)
        register = parsed.get("register_number")
        result.register_number = str(register).strip() if isinstance(register, str) else None
        postcode = parsed.get("postcode")
        result.postcode = str(postcode).strip() if isinstance(postcode, (str, int)) else None
        result.reason = str(parsed.get("reason") or "")[:200]
    except OllamaUnavailableError:
        raise
    except Exception as exc:  # noqa: BLE001
        result.error = f"{type(exc).__name__}: {exc}"

    result.elapsed_s = round(time.monotonic() - started, 2)
    return result


def flags_for(result: JudgeResult, company: Any, *, page_text: str) -> set[str]:
    """Turn what the model said into evidence names, checking everything checkable first.

    This function is where trust is decided, which is why it is code and not prompt. Read it as
    the answer to "what is the model actually allowed to contribute":

    1. Its verdict becomes one weighted flag. Nothing more.
    2. Its register number becomes REGISTER_MATCH only when the digits are on the page AND equal
       the company's own. It can never produce REGISTER_MISMATCH — that flag disqualifies a URL,
       and a misread digit would throw away a correct website.
    3. Its postcode becomes POSTCODE_MATCH on the same terms: on the page, and equal to ours.
    """
    flags: set[str] = set()
    if not result.usable:
        return flags

    flag = _VERDICT_FLAGS.get(result.verdict)
    if flag:
        flags.add(flag)

    # A register number only counts as proof when the model also says the page BELONGS to the
    # company. When it says the page is a directory or somebody else's site, it is telling us the
    # number is quoted rather than published — and a quoted register number is exactly what a
    # directory carries for every company it lists. Measured on a hand-written FirmenWissen page:
    # without this, the directory scored 0.805 and landed in the `high` band.
    quoting = result.verdict in (VERDICT_ABOUT_COMPANY, VERDICT_UNRELATED)

    page_number = conf.register_digits(result.register_number)
    own_number = conf.register_digits(getattr(company, "registration_number", None))
    if page_number and own_number and page_number == own_number and not quoting:
        if _number_is_on_page(page_number, page_text):
            flags.add(conf.REGISTER_MATCH)
        else:
            # The model produced our own register number for a page that does not contain it.
            # Almost certainly copied from the prompt. Worth a log line: a model doing this often
            # is a model whose other answers deserve less weight.
            log.debug("model claimed register %s for %s but the page lacks it", page_number, "page")

    if (
        result.postcode
        and company.postcode
        and result.postcode == company.postcode
        and not quoting
    ):
        if _number_is_on_page(result.postcode, page_text):
            flags.add(conf.POSTCODE_MATCH)

    return flags
