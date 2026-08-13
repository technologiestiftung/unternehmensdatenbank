"""Run the three methods for one company and merge what they say into one answer.

The merge is the point of this file. Three methods each propose URLs; the same URL proposed
twice is worth more than two URLs proposed once. So results are grouped by **origin** — the
`https://host` part of a URL, with the page path thrown away — and a URL's evidence is the
union of everything all three methods learned about it.

Order of work, and why:

1. **URL patterns first.** It costs one DNS lookup per guess and a handful of page fetches.
   It is the only method that downloads the company's own page, so it is the only one that
   can produce hard proof.
2. **OSM second.** One query to a shared public service that rate-limits, so it is the slow
   one. It runs for every company anyway, not only the ones step 1 missed, because an OSM
   entry agreeing with a guessed domain is what turns a guess into a strong answer.
3. **The local model with its own search engine, only if it is switched on.** Needs SearXNG,
   which public search engines block from office networks, so it usually contributes nothing.
4. **The judge, last.** A local model reads the pages steps 1 to 3 already downloaded and says
   whether each one belongs to this company. It proposes no URLs of its own — it only decides
   about pages already in hand, which is why it cannot invent a domain. See `llm_judge.py`.

Steps 3 and 4 are different things and the names are easy to confuse. Step 3 *searches* and is
effectively dead on this network. Step 4 *reads*, needs nothing but Ollama, and is the one worth
switching on.

Every URL that any method proposed is kept, not only the winner. A rejected URL with its
reason is worth more than a blank cell when you are trying to work out why coverage is what
it is.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from imprint_probe import best_imprint
from search_common import origin_url, own_site_verdict
from url_discovery.scorer import name_uniqueness

from combined_discovery import confidence as conf
from combined_discovery import llm_judge, local_llm, name_frequency, osm_lookup, url_patterns

log = logging.getLogger(__name__)

#: Below this share of the company's distinctive words appearing in the domain, the domain holds a
#: fragment of the name rather than the name. See `url_patterns.name_coverage`. This replaced a
#: list of "rules that produce fragments": the domain itself is the evidence, and measuring it
#: also catches a fragment that arrived from OpenStreetMap rather than from a guess.
#:
#: 0.6 rather than 0.5 so that one word out of two counts as a fragment. `uniformen.de` for
#: "Uniformen Hinnersmann" holds exactly half the name, and the half it holds is the trade. The
#: true guesses are unaffected: `zweirad-liebig.de` and `bruecklyn.de` score 1.0, and
#: `muellertiefbau.de` for "Müller Hoch- und Tiefbau" scores 0.67.
MIN_NAME_COVERAGE = 0.6


@dataclass
class UrlAnswer:
    """One URL, everything known about it, and what that is worth."""

    url: str
    methods: list[str] = field(default_factory=list)
    evidence: set[str] = field(default_factory=set)
    #: Exactly which evidence names the judge contributed that nothing else had already found.
    #: Recorded so `band_without_judge` can be computed by subtraction rather than guessed at.
    judge_added: set[str] = field(default_factory=set)
    judge_verdict: str | None = None
    judge_reason: str = ""
    #: Which naming rule guessed it, when a guess is where it came from.
    pattern: str | None = None
    imprint_url: str | None = None
    register_number_seen: str | None = None
    verdict: conf.Verdict | None = None
    #: The readable text of the page and of its Impressum, kept so the judge stage can read what
    #: was already downloaded instead of fetching it a second time. Truncated on the way in — a
    #: company with four live candidates would otherwise hold four whole web pages in memory.
    page_text: str = ""
    imprint_text: str = ""
    #: The band this URL scored on the hand-written rules alone, recorded before the judge's flags
    #: are mixed in. Without it, "where did the model and the rules disagree" needs a second run.
    band_without_judge: str | None = None
    judged: bool = False


@dataclass
class CompanyResult:
    company_id: str
    company_name: str
    postcode: str | None = None
    city: str | None = None
    address: str | None = None
    registration_number: str | None = None

    website: str | None = None
    confidence_value: float = 0.0
    band: str = "reject"
    verified_by: str = ""
    why: str = ""
    methods: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    pattern: str | None = None
    imprint_url: str | None = None

    # What each stage did, for the scorecard.
    candidates_generated: int = 0
    candidates_resolved: int = 0
    candidates_probed: int = 0
    parked_rejected: int = 0
    osm_objects_seen: int = 0
    osm_id: str | None = None
    osm_match_score: float | None = None
    osm_website: str | None = None
    osm_phone: str | None = None
    #: The part of the map object's email AFTER the @, e.g. `stallkamp.de` — never the address
    #: itself. A business email domain is usually the business's website, so it is worth keeping;
    #: the local part can be a person's name, and this file gets emailed around.
    osm_email_domain: str | None = None
    llm_website: str | None = None
    llm_error: str | None = None

    # What the judge stage did. `judge_calls` counts model calls actually made, which is the
    # honest cost figure: most answers are skipped because they are already disqualified or
    # already proven, so it is always lower than the number of URLs.
    judge_verdict: str | None = None
    judge_reason: str = ""
    judge_error: str | None = None
    judge_calls: int = 0
    #: The band the winning URL had before the judge spoke. Equal to `band` when the judge changed
    #: nothing, which is the common case and should stay visible as such.
    band_without_judge: str | None = None

    answers: list[dict[str, Any]] = field(default_factory=list)
    elapsed_s: float = 0.0
    errors: list[str] = field(default_factory=list)


def _digits(text: str | None) -> str:
    """The number out of a register string. "Amtsgericht Osnabrück HRB 213220" -> "213220"."""
    import re

    match = re.search(r"\d+", text or "")
    return match.group(0) if match else ""


def _host_is_exact_name(url: str, company_name: str) -> bool:
    """True when the domain IS the name rather than merely containing it.

    `zweirad-liebig.de` for "Zweirad Liebig GmbH" is exact. `liebig-shop.de` is not.

    Checked against every spelling of the name, so `muller.de` and `xn--mller-kva.de` count as
    exactly as `mueller.de` does for "Müller GmbH". `host_stem` decodes punycode and transliterates,
    which is what puts the domain and the name in the same alphabet.
    """
    stem = url_patterns.host_stem(url)
    if not stem:
        return False
    for _spelling, words in url_patterns.name_word_spellings(company_name):
        if not words:
            continue
        core = [w for w in words if not url_patterns.is_low_signal(w)] or words
        if stem in {"".join(words), "".join(core)}:
            return True
    return False


def _page_evidence(
    url: str,
    company: Any,
    *,
    html: str | None,
    extracted: dict[str, Any] | None,
    redirected_off_host: bool = False,
    common: set[str] | frozenset[str] = frozenset(),
) -> tuple[set[str], str | None, str]:
    """Everything the page itself says about whether it belongs to this company.

    Returns (evidence names, the register number seen on the page, the page's readable text).

    The text comes back so the judge stage can read the page without downloading it again. It is
    returned even for a page that was disqualified on its host name, because whether a disqualified
    page gets shown to the model is the caller's decision, not this function's.

    `own_site_verdict` is the platform's single predicate for "is this the company's own site
    or a page about it". It is used here unchanged and on purpose: every past instance of a
    directory page becoming a company's website came from some other, looser check.
    """
    evidence: set[str] = set()
    page_text = url_patterns.visible_text(html or "")

    verdict = own_site_verdict(url, company.company_name)
    if verdict == "directory_host":
        evidence.add(conf.DIRECTORY_HOST)
        return evidence, None, page_text
    if verdict != "own_site":
        evidence.add(conf.HOST_NAME_MISMATCH)
        return evidence, None, page_text

    # An industry domain cannot identify one company. Emitted as evidence rather than decided
    # here, because `confidence.py` is the single place that turns evidence into a refusal — and
    # it is the place that knows a register number on the page overrules this one.
    generic = url_patterns.is_generic_stem(url_patterns.host_stem(url), common)
    if generic:
        evidence.add(conf.GENERIC_HOST)
    # An industry word can BE the whole of a company's name — "S & E Automobile Verwaltungs- und
    # Beteiligungsgesellschaft mbH" reduces to the single word "automobile" — and awarding the
    # exact-name bonus there rewards the very thing GENERIC_HOST exists to punish.
    if not generic and _host_is_exact_name(url, company.company_name):
        evidence.add(conf.HOST_EXACT)
    elif url_patterns.name_coverage(url, company.company_name) < MIN_NAME_COVERAGE:
        # The domain carries a fragment of the name rather than the name. Measured on the domain
        # itself, which is why this no longer depends on knowing which rule guessed it.
        evidence.add(conf.HOST_PARTIAL_NAME)
    if redirected_off_host:
        evidence.add(conf.REDIRECT_OFF_HOST)

    seen_register: str | None = None
    if extracted:
        evidence.add(conf.IMPRINT_FOUND)
        seen_register = extracted.get("register_number")
        page_reg = _digits(seen_register)
        own_reg = _digits(company.registration_number)
        if page_reg and own_reg:
            evidence.add(conf.REGISTER_MATCH if page_reg == own_reg else conf.REGISTER_MISMATCH)
        if company.postcode and extracted.get("postcode") == company.postcode:
            evidence.add(conf.POSTCODE_MATCH)
    else:
        evidence.add(conf.NO_IMPRINT)

    haystack = url_patterns.normalize(
        " ".join(
            part
            for part in [
                page_text[:20000],
                (extracted or {}).get("text_snippet") or "",
            ]
        )
    )
    if haystack:
        if company.city and url_patterns.normalize(company.city) in haystack:
            evidence.add(conf.CITY_ON_PAGE)
        if _name_is_on_page(company.company_name, haystack):
            evidence.add(conf.NAME_ON_PAGE)

    return evidence, seen_register, page_text


#: How much of the company's name has to appear on the page before it counts. Not all of it:
#: a page prints "Max Moritz" where the register says "Max Moritz Beteiligungsgesellschaft
#: mbH", and demanding the whole string found the name on almost no page at all.
NAME_ON_PAGE_MIN_SHARE = 0.6


def _name_is_on_page(company_name: str, normalized_text: str) -> bool:
    """Does the page print enough of the company's name to be that company's page?

    The test is per word, on the distinctive words only — legal form gone, shell-company words
    gone, anything under four letters gone. "Zweirad Liebig GmbH" needs "zweirad" and "liebig";
    "GmbH" proves nothing, since every third German page carries it.
    """
    words = [w for w in url_patterns.name_words(company_name) if not url_patterns.is_low_signal(w)]
    distinctive = [w for w in words if len(w) >= 4]
    if not distinctive:
        return False
    padded = f" {normalized_text} "
    found = sum(1 for w in set(distinctive) if f" {w} " in padded or w in normalized_text)
    return found / len(set(distinctive)) >= NAME_ON_PAGE_MIN_SHARE


def _fetch_and_read(url: str) -> tuple[str, dict[str, Any] | None, str | None, str]:
    """Download a page and its Impressum.

    Returns (page html, extracted fields, imprint url, imprint html).

    Used for URLs that a method proposed without downloading them — an OSM `website` tag or a
    URL the language model picked. Downloading is what lets those be verified rather than
    merely believed.
    """
    probe = url_patterns.probe(url)
    if not probe or probe.status_code == 0 or probe.status_code >= 400 or probe.parked:
        return "", None, None, ""
    got = best_imprint(probe.final_url)
    if not got:
        return probe.html, None, None, ""
    imprint_html, imprint_url, extracted = got
    return probe.html, extracted, imprint_url, imprint_html


def _remember_text(entry: UrlAnswer, page_text: str, imprint_html: str) -> None:
    """Keep enough of a downloaded page for the judge to read it later.

    Truncated to the judge's own budget: holding more would mean carrying whole web pages for
    every candidate of every company, and the judge would throw the surplus away anyway.
    """
    if page_text and not entry.page_text:
        entry.page_text = page_text[: llm_judge.MAX_CHARS]
    if imprint_html and not entry.imprint_text:
        entry.imprint_text = url_patterns.visible_text(imprint_html)[: llm_judge.MAX_CHARS]


#: Evidence that makes asking the model pointless, for two opposite reasons. `REGISTER_MATCH` means
#: the page already proved it belongs to this company, so there is nothing left to decide.
#: The three disqualifiers mean the URL is dead however the model votes — a model cannot lift a
#: disqualifier, by design.
#:
#: `GENERIC_HOST` is deliberately NOT here even though it disqualifies. It is the one disqualifier
#: a register match can overrule, and the judge can produce a register match, so an industry domain
#: is a row the model might legitimately rescue.
_JUDGE_SKIP_EVIDENCE = frozenset(
    {
        conf.REGISTER_MATCH,
        conf.REGISTER_MISMATCH,
        conf.DIRECTORY_HOST,
        conf.HOST_NAME_MISMATCH,
    }
)


def _worth_judging(entry: UrlAnswer) -> bool:
    """Would a model's opinion about this URL change anything?

    Skipping is most of what keeps this stage affordable. On the 521-company sample about half the
    candidate URLs are already disqualified on their host name alone, and each model call costs
    roughly 15 seconds.
    """
    if not (entry.page_text or entry.imprint_text):
        return False
    return not (entry.evidence & _JUDGE_SKIP_EVIDENCE)


def discover(
    company: Any,
    *,
    use_url_patterns: bool = True,
    use_osm: bool = True,
    use_local_llm: bool = False,
    use_llm_judge: bool = False,
    osm_cache: osm_lookup.OsmCache | None = None,
    osm_snapshot: Any | None = None,
    search_queries: dict[str, str] | None = None,
    common_words: set[str] | frozenset[str] = frozenset(),
    mail_hosts: set[str] | frozenset[str] = frozenset(),
) -> CompanyResult:
    """Run the enabled methods for one company and return one merged answer.

    `common_words` is the measured set of words that too many companies in this dataset share to
    identify one of them — `name_frequency.common_words` over the whole table. It decides whether
    a domain names an industry or a business. Passing nothing still works and falls back to the
    short hand-written list in `url_patterns.GENERIC_WORDS`.

    `mail_hosts` is the same idea for email addresses — `name_frequency.common_email_domains` over
    the city snapshot. It decides whether a mapped business's email domain is that business's own
    or its internet provider's. Passing nothing means no email domain is ever refused, so pass it
    whenever a snapshot exists: the top domain in Osnabrück is a local provider used by 23 mapped
    businesses, and taking it as a website would give 23 companies the same wrong answer.
    """
    common = common_words
    started = time.monotonic()
    result = CompanyResult(
        company_id=company.id,
        company_name=company.company_name,
        postcode=company.postcode,
        city=company.city,
        address=company.address,
        registration_number=company.registration_number,
    )
    answers: dict[str, UrlAnswer] = {}

    def answer_for(url: str) -> UrlAnswer:
        origin = origin_url(url)
        if origin not in answers:
            answers[origin] = UrlAnswer(url=origin)
        return answers[origin]

    # ---- 1. URL patterns -----------------------------------------------------------------
    if use_url_patterns:
        try:
            found = url_patterns.find_live_urls(company.company_name, company.city)
            result.candidates_generated = found.candidates_generated
            result.candidates_resolved = found.candidates_resolved
            result.candidates_probed = found.candidates_probed
            result.parked_rejected = len(found.rejected_parked)

            for probe in found.live:
                got = best_imprint(probe.final_url)
                extracted = got[2] if got else None
                imprint_url = got[1] if got else None
                imprint_html = got[0] if got else ""
                evidence, register_seen, page_text = _page_evidence(
                    probe.final_url,
                    company,
                    html=probe.html,
                    extracted=extracted,
                    redirected_off_host=probe.redirected_off_host,
                    common=common,
                )
                entry = answer_for(probe.final_url)
                entry.methods.append(conf.METHOD_URL_PATTERN)
                entry.evidence |= evidence
                entry.imprint_url = entry.imprint_url or imprint_url
                entry.register_number_seen = entry.register_number_seen or register_seen
                _remember_text(entry, page_text, imprint_html)
                host = (urlparse(probe.url).hostname or "").lower()
                entry.pattern = entry.pattern or found.pattern_by_host.get(host)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"url_patterns: {type(exc).__name__}: {exc}")

    # ---- 2. OpenStreetMap ----------------------------------------------------------------
    if use_osm:
        try:
            # A city-wide snapshot answers offline and is preferred whenever one was loaded. The
            # per-company Overpass query stays as the fallback so this module still works with no
            # snapshot at all.
            if osm_snapshot is not None:
                match, seen = osm_lookup.lookup_in_snapshot(company, osm_snapshot)
            else:
                match, seen = osm_lookup.lookup(company, osm_cache)
            result.osm_objects_seen = seen
            if match:
                result.osm_id = match.osm_id
                result.osm_match_score = match.match_score
                result.osm_website = match.website
                result.osm_phone = match.phone
                result.osm_email_domain = name_frequency.email_domain(match.email)
            if match and match.website:
                origin = origin_url(match.website)
                known = origin in answers
                if not known:
                    # Not seen by the guesser, so nothing has downloaded it yet.
                    html, extracted, imprint_url, imprint_html = _fetch_and_read(match.website)
                    evidence, register_seen, page_text = _page_evidence(
                        match.website, company, html=html, extracted=extracted, common=common
                    )
                    entry = answer_for(match.website)
                    entry.evidence |= evidence
                    entry.imprint_url = entry.imprint_url or imprint_url
                    entry.register_number_seen = entry.register_number_seen or register_seen
                    _remember_text(entry, page_text, imprint_html)
                else:
                    entry = answer_for(match.website)
                entry.methods.append(conf.METHOD_OSM)
                if match.strong:
                    entry.evidence.add(conf.OSM_STRONG_MATCH)

            # The mapped business's email domain, checked as a website in its own right. A business
            # that writes `info@stallkamp.de` is telling you stallkamp.de is theirs, and it says so
            # without anybody having guessed at the name — which is the whole value of it, since
            # every other route into this pipeline starts from the name.
            #
            # Only for a STRONG match. A weak one agreed on the name alone, and acting on the
            # contact details of a business we are not sure is this one is how you publish somebody
            # else's address.
            if match and match.strong and result.osm_email_domain:
                if result.osm_email_domain in mail_hosts:
                    log.debug(
                        "email domain %s is shared by several businesses — not a website",
                        result.osm_email_domain,
                    )
                else:
                    email_site = f"https://{result.osm_email_domain}"
                    if origin_url(email_site) not in answers:
                        html, extracted, imprint_url, imprint_html = _fetch_and_read(email_site)
                        evidence, register_seen, page_text = _page_evidence(
                            email_site, company, html=html, extracted=extracted, common=common
                        )
                        entry = answer_for(email_site)
                        entry.evidence |= evidence
                        entry.imprint_url = entry.imprint_url or imprint_url
                        entry.register_number_seen = entry.register_number_seen or register_seen
                        _remember_text(entry, page_text, imprint_html)
                        entry.methods.append(conf.METHOD_OSM)
                        entry.evidence.add(conf.OSM_EMAIL_DOMAIN)
                        entry.evidence.add(conf.OSM_STRONG_MATCH)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"osm: {type(exc).__name__}: {exc}")

    # ---- 3. Local search + local model ---------------------------------------------------
    if use_local_llm:
        try:
            llm = local_llm.discover(company, search_queries or {})
            result.llm_website = llm.website
            result.llm_error = llm.error
            if llm.website:
                origin = origin_url(llm.website)
                if origin not in answers:
                    html, extracted, imprint_url, imprint_html = _fetch_and_read(llm.website)
                    evidence, register_seen, page_text = _page_evidence(
                        llm.website, company, html=html, extracted=extracted, common=common
                    )
                    entry = answer_for(llm.website)
                    entry.evidence |= evidence
                    entry.imprint_url = entry.imprint_url or imprint_url
                    entry.register_number_seen = entry.register_number_seen or register_seen
                    _remember_text(entry, page_text, imprint_html)
                else:
                    entry = answer_for(llm.website)
                entry.methods.append(conf.METHOD_LOCAL_LLM)
        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"local_llm: {type(exc).__name__}: {exc}")

    # ---- 4. The judge: a local model reads the pages we already have ----------------------
    #
    # This stage proposes nothing. It reads text that stages 1-3 downloaded and says whether the
    # page belongs to this company, which is the one question string matching cannot answer: a
    # directory prints the name, the address and the register number of everybody it lists.
    #
    # Note what is NOT done here: the judge is never appended to `entry.methods`. It did not find
    # this URL independently, it read the URL the guesser found — so counting it as a second method
    # would hand every judged row the `two_methods_agree` bonus and inflate the whole run.
    if use_llm_judge:
        for entry in answers.values():
            if not _worth_judging(entry):
                continue
            try:
                got = llm_judge.judge(
                    company,
                    url=entry.url,
                    page_text=entry.page_text,
                    imprint_text=entry.imprint_text,
                )
            except Exception as exc:  # noqa: BLE001
                # Ollama being unreachable is not a per-URL problem. Stop asking for this company
                # rather than burning one timeout per candidate.
                result.judge_error = f"{type(exc).__name__}: {exc}"
                result.errors.append(f"llm_judge: {type(exc).__name__}: {exc}")
                break
            result.judge_calls += 1
            entry.judged = True
            entry.judge_verdict = got.verdict
            entry.judge_reason = got.reason
            if got.error:
                result.judge_error = result.judge_error or got.error
                continue
            flags = llm_judge.flags_for(got, company, page_text=entry.page_text)
            # Only what nothing else had already established, so subtracting these later gives the
            # honest "what would this row have scored without the model" number.
            entry.judge_added = flags - entry.evidence
            entry.evidence |= flags

    # ---- 5. Merge and score --------------------------------------------------------------
    uniqueness = name_uniqueness(company.company_name)
    for entry in answers.values():
        distinct = set(entry.methods)
        if len(distinct) >= 3:
            entry.evidence.add(conf.AGREE_THREE)
        elif len(distinct) == 2:
            entry.evidence.add(conf.AGREE_TWO)
        entry.verdict = conf.score(
            methods=list(distinct), evidence=entry.evidence, name_uniqueness=uniqueness
        )
        if entry.judge_added:
            without = conf.score(
                methods=list(distinct),
                evidence=entry.evidence - entry.judge_added,
                name_uniqueness=uniqueness,
            )
            entry.band_without_judge = without.band
        else:
            entry.band_without_judge = entry.verdict.band

    ranked = sorted(
        answers.values(),
        key=lambda a: (a.verdict.value if a.verdict else 0.0),
        reverse=True,
    )
    result.answers = [
        {
            "url": a.url,
            "confidence": a.verdict.value if a.verdict else 0.0,
            "band": a.verdict.band if a.verdict else "reject",
            "verified_by": a.verdict.verified_by if a.verdict else "",
            "methods": sorted(set(a.methods)),
            "evidence": sorted(a.evidence),
            "pattern": a.pattern,
            "imprint_url": a.imprint_url,
            "register_number_seen": a.register_number_seen,
            "why": a.verdict.why if a.verdict else "",
            # Blank when the judge did not run or skipped this URL. `judged: false` and
            # `judge_verdict: "unsure"` mean different things and must not be read as the same.
            "judged": a.judged,
            "judge_verdict": a.judge_verdict,
            "judge_reason": a.judge_reason,
            "judge_added": sorted(a.judge_added),
            "band_without_judge": a.band_without_judge,
        }
        for a in ranked
    ]

    best = next((a for a in ranked if a.verdict and a.verdict.promotable), None)
    if best and best.verdict:
        result.website = best.url
        result.confidence_value = best.verdict.value
        result.band = best.verdict.band
        result.verified_by = best.verdict.verified_by
        result.why = best.verdict.why
        result.methods = best.verdict.methods
        result.evidence = best.verdict.evidence
        result.pattern = best.pattern
        result.imprint_url = best.imprint_url
        result.judge_verdict = best.judge_verdict
        result.judge_reason = best.judge_reason
        result.band_without_judge = best.band_without_judge
    elif ranked and ranked[0].verdict:
        # Nothing cleared the bar. Report the best refusal so the row still says something.
        top = ranked[0]
        result.band = top.verdict.band
        result.verified_by = top.verdict.verified_by
        result.why = top.verdict.why
        result.confidence_value = top.verdict.value
        result.judge_verdict = top.judge_verdict
        result.judge_reason = top.judge_reason
        result.band_without_judge = top.band_without_judge

    result.elapsed_s = round(time.monotonic() - started, 2)
    return result
