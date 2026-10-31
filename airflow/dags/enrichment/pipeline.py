"""Guess a company's website and check what each guess says about the company.

URL patterns, from the current name and then the former ones, cost one DNS lookup per guess and a
handful of page fetches. Results are grouped by **origin** — the `https://host` part of a URL, with
the page path thrown away — and a URL's evidence is the union of everything learned about it.
`verify_proposed` checks a URL another source proposed (Wikidata, Common Crawl) the same way.
The OpenStreetMap step was removed on 2026-10-30 (#57): it found 3 websites in 1,000 companies.

Every URL that any method proposed is kept, not only the winner. A rejected URL with its
reason is worth more than a blank cell when you are trying to work out why coverage is what
it is.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from enrichment.imprint_probe import best_imprint
from enrichment.own_site import origin_url, own_site_verdict
from enrichment.imprint_extractor import extract_from_html, extract_register_numbers, person_key
from enrichment.url_scorer import name_uniqueness

from enrichment import confidence as conf
from enrichment import (
    name_frequency,
    page_cache,
    structured_data,
    url_patterns,
)

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
#: Former names tried per company. 32,061 of the 32,170 Berlin companies with a former name have at
#: most 3; each costs up to `url_patterns.MAX_PROBES` more page loads.
MAX_FORMER_NAMES = 3


@dataclass
class UrlAnswer:
    """One URL, everything known about it, and what that is worth."""

    url: str
    methods: list[str] = field(default_factory=list)
    evidence: set[str] = field(default_factory=set)
    #: Which naming rule guessed it, when a guess is where it came from.
    pattern: str | None = None
    imprint_url: str | None = None
    register_number_seen: str | None = None
    verdict: conf.Verdict | None = None


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
    answers: list[dict[str, Any]] = field(default_factory=list)
    elapsed_s: float = 0.0
    errors: list[str] = field(default_factory=list)


def _digits(text: str | None) -> str:
    """The number out of a register string. "Amtsgericht Osnabrück HRB 213220" -> "213220"."""
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
    host_name: str | None = None,
) -> tuple[set[str], str | None]:
    """Everything the page itself says about whether it belongs to this company.

    Returns (evidence names, the register number seen on the page). `host_name` is the name the
    domain was guessed from, a former name for a former-name guess: the domain checks compare
    against it. Identity is still proven by the page, against the company as it is today.

    `own_site_verdict` is the platform's single predicate for "is this the company's own site
    or a page about it". It is used here unchanged and on purpose: every past instance of a
    directory page becoming a company's website came from some other, looser check.
    """
    evidence: set[str] = set()
    page_text = url_patterns.visible_text(html or "")

    host_name = host_name or company.company_name
    verdict = own_site_verdict(url, host_name)
    if verdict == "directory_host":
        evidence.add(conf.DIRECTORY_HOST)
        return evidence, None
    if verdict != "own_site":
        evidence.add(conf.HOST_NAME_MISMATCH)
        # A company's website does not have to have the company's name in it. That refusal exists
        # because a *guess* that missed the name landed on somebody else — but the page's own
        # structured data is not a guess, and it does not depend on the domain string at all. It can
        # therefore lift the refusal; see `confidence._LIFTED_BY`.
        #
        # Only for `host_name_mismatch`, never for `company_listing`. A listing page's structured
        # data names the company it is listing, which is the oldest trap in this repo wearing a new
        # hat: it would prove the page is ABOUT the company, not that it IS its site.
        lifted = False
        if verdict == "host_name_mismatch" and html:
            for claimed in structured_data.names(html):
                if _name_is_on_page(company.company_name, url_patterns.normalize(claimed)):
                    evidence.add(conf.JSONLD_NAME_MATCH)
                    lifted = True
                    break
        if not lifted:
            return evidence, None
        # The refusal is lifted, so keep reading. Returning here instead would leave the row scored on
        # its base and the structured data alone, while the register number that settles it sits
        # unread further down the same page — `thebigbash.de` and `gmh-gruppe.de` both print the
        # company's own HRB number, worth 0.45 and hard proof, and both were being scored at ~0.55
        # on a guess because this function had already stopped looking.

    # An industry domain cannot identify one company. Emitted as evidence rather than decided
    # here, because `confidence.py` is the single place that turns evidence into a refusal — and
    # it is the place that knows a register number on the page overrules this one.
    generic = url_patterns.is_generic_stem(url_patterns.host_stem(url), common)
    if generic:
        evidence.add(conf.GENERIC_HOST)
    # An industry word can BE the whole of a company's name — "S & E Automobile Verwaltungs- und
    # Beteiligungsgesellschaft mbH" reduces to the single word "automobile" — and awarding the
    # exact-name bonus there rewards the very thing GENERIC_HOST exists to punish.
    if not generic and _host_is_exact_name(url, host_name):
        evidence.add(conf.HOST_EXACT)
    elif url_patterns.name_coverage(url, host_name) < MIN_NAME_COVERAGE:
        # The domain carries a fragment of the name rather than the name. Measured on the domain
        # itself, which is why this no longer depends on knowing which rule guessed it.
        evidence.add(conf.HOST_PARTIAL_NAME)
    if redirected_off_host:
        evidence.add(conf.REDIRECT_OFF_HOST)

    seen_register: str | None = None
    own_type, own_reg = conf.register_parts(company.registration_number)
    if extracted:
        evidence.add(conf.IMPRINT_FOUND)
        if extracted.get("vat_id"):
            evidence.add(conf.VAT_ID_FOUND)
        # The site's Impressum gives an email address at the site's own domain. Reused rather than
        # re-derived: `name_frequency.email_domain` is the one place that knows how to get a domain
        # out of an address, and `origin_url` normalises `www.` away.
        page_email_domain = name_frequency.email_domain(extracted.get("email"))
        if page_email_domain:
            host = (urlparse(url).hostname or "").lower().removeprefix("www.")
            if host and page_email_domain.lower().removeprefix("www.") == host:
                evidence.add(conf.IMPRINT_EMAIL_ON_DOMAIN)
        seen_register = extracted.get("register_number")
        # Against EVERY register entry on the page, not just the first. A group's Impressum lists its
        # subsidiaries, and a page often gives its own entry next to its parent's — reading only the
        # first turned two of those into a false "different company's register number", which is a
        # disqualifier and therefore fatal.
        page_registers = extracted.get("register_numbers") or (
            [seen_register] if seen_register else []
        )
        parsed = [conf.register_parts(entry) for entry in page_registers]
        if own_reg and parsed:
            # A match needs the same register as well as the same number when both name one. The five
            # German registers number independently, so `VR 213220` and `HRB 213220` are two
            # different entities that happen to share a number.
            matches = [
                (kind, number)
                for kind, number in parsed
                if number == own_reg and (not kind or not own_type or kind == own_type)
            ]
            if matches:
                evidence.add(conf.REGISTER_MATCH)
                seen_register = f"{matches[0][0]} {own_reg}".strip()
            elif any(number != own_reg and kind == own_type for kind, number in parsed):
                # Somebody else's entry in the SAME register. A page carrying only a `VR` number when
                # this company is an `HRB` is not evidence about this company either way, so it does
                # not become a mismatch — a mismatch disqualifies, and that is too strong a
                # conclusion to draw from an unrelated register.
                evidence.add(conf.REGISTER_MISMATCH)
        # Against every postal address on the page, not just the first. A company's registered
        # address and the address on its Impressum are often two different ones, and either
        # agreeing with the register is the same evidence.
        page_postcodes = extracted.get("postcodes") or (
            [extracted["postcode"]] if extracted.get("postcode") else []
        )
        if company.postcode and company.postcode in page_postcodes:
            evidence.add(conf.POSTCODE_MATCH)
        # A Geschäftsführer on the Impressum who is one of the company's register officers. The
        # officers are personal data: they arrive as comparison keys only, and only the evidence
        # NAME is stored, never which person matched.
        officers = getattr(company, "officers", None) or frozenset()
        if officers and any(person_key(n) in officers for n in extracted.get("ceo_names") or []):
            evidence.add(conf.OFFICER_MATCH)
    else:
        evidence.add(conf.NO_IMPRINT)

    # A page can print its register number and still have no Impressum this code can find — the
    # link is an image, or the number sits in the footer of every page. The number is right there
    # in the text either way, so read it here.
    #
    # Only when the Impressum produced no register entry at all. One found there is the better
    # source, and a MISMATCH found there must not be talked out of by a second number somewhere else
    # on the page — a page may quote other companies' numbers.
    #
    # The whole entry has to be on the page, register prefix included, never the bare digits. Plenty
    # of register numbers are three digits long — Bäko-Osnabrück is `GnR 205` — and searching a web
    # page for "205" finds a price, a house number or half a phone number. That mistake awarded hard
    # proof, worth 0.45 and able to overrule a disqualifier, on a coincidence.
    #
    # Safe here for one reason: this code is only reached after `own_site_verdict` said `own_site`,
    # or after structured data lifted the name refusal. A register number proves a page is ABOUT a
    # company, never that it IS that company's site, and that gap is what the host check closes. Do
    # not lift this test out of that guard.
    if own_reg and not (evidence & {conf.REGISTER_MATCH, conf.REGISTER_MISMATCH}):
        for kind, number in (
            conf.register_parts(entry) for entry in extract_register_numbers(page_text[:20000])
        ):
            if number == own_reg and (not kind or not own_type or kind == own_type):
                evidence.add(conf.REGISTER_MATCH)
                seen_register = seen_register or f"{kind} {number}".strip()
                break

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

    # What the page says about itself in machine-readable form. Read from the raw HTML, not from the
    # flattened text, because the whole claim lives inside a <script> block that `visible_text` and
    # `page_text` both strip out — which is why nothing in this repo saw it until now.
    #
    # The name is tested with the same share rule used on the page text, so "Zweirad Liebig GmbH" in
    # the structured data matches the register's "Zweirad Liebig Gesellschaft mbH". Comparing the two
    # strings exactly would fail on every legal-form abbreviation in Germany.
    if html:
        for claimed in structured_data.names(html):
            if _name_is_on_page(company.company_name, url_patterns.normalize(claimed)):
                evidence.add(conf.JSONLD_NAME_MATCH)
                break
        if company.postcode and company.postcode in structured_data.postcodes(html):
            evidence.add(conf.JSONLD_POSTCODE_MATCH)

    return evidence, seen_register


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


def _imprint_for(
    base_url: str, homepage: str | None = None
) -> tuple[str, str, dict[str, Any]] | None:
    """`imprint_probe.best_imprint`, but remembering the page it settled on.

    A miss costs up to twelve GETs: `fetch_impressum` walks its own path list and `best_imprint`
    then walks another. A hit costs one file read.

    Only the HTML and which URL it came from are cached. `extract_from_html` runs again on the way
    out, so a better postcode regex or a new field applies to pages that were downloaded weeks ago —
    which is the whole reason the cache exists. See `page_cache`.

    The cache lives here rather than inside `imprint_probe` on purpose: that module sits at the top
    of `dags/` and is shared with the three search-provider DAGs, and they have no business
    importing a combined-discovery module or inheriting its caching.
    """
    cached = page_cache.get("imprint", base_url)
    if cached is not None:
        html = cached.get("html") or ""
        if not html:
            return None
        return html, cached.get("imprint_url") or base_url, extract_from_html(html)

    got = best_imprint(base_url, homepage=homepage)
    if got is None:
        # Remembered as a miss, so a replay does not walk all twelve paths again to learn nothing.
        page_cache.put("imprint", base_url, {"html": "", "imprint_url": None})
        return None
    html, imprint_url, extracted = got
    page_cache.put("imprint", base_url, {"html": html, "imprint_url": imprint_url})
    return html, imprint_url, extracted


def _fetch_and_read(url: str) -> tuple[str, dict[str, Any] | None, str | None]:
    """Download a page and its Impressum.

    Returns (page html, extracted fields, imprint url).

    Used for URLs another source proposed without downloading them (`verify_proposed`).
    Downloading is what lets those be verified rather than merely believed.
    """
    probe = url_patterns.probe(url)
    if not probe or probe.status_code == 0 or probe.status_code >= 400 or probe.parked:
        return "", None, None
    got = _imprint_for(probe.final_url, homepage=probe.html)
    if not got:
        return probe.html, None, None
    _, imprint_url, extracted = got
    return probe.html, extracted, imprint_url




def _answer_dict(a: UrlAnswer) -> dict[str, Any]:
    """One scored URL as it is stored: the shape of `CompanyResult.answers`."""
    return {
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
    }


def verify_proposed(
    company: Any, urls: list[str], *, method: str, common: set[str] | frozenset[str] = frozenset()
) -> list[dict[str, Any]]:
    """Check URLs another source proposed for this company (Wikidata, Common Crawl).

    Download the page and its Impressum, read what the page says about the company, and score it with
    `confidence.score`: never a scorer of its own (CLAUDE.md). A proposal nothing on the page ties to
    the company stays below the bar (`confidence.UNVERIFIED_CAP`). Returns the answers, best first,
    in the shape `discover` stores.
    """
    uniqueness = name_uniqueness(company.company_name)
    answers: dict[str, UrlAnswer] = {}
    for url in urls:
        origin = origin_url(url)
        if origin in answers:
            continue
        html, extracted, imprint_url = _fetch_and_read(url)
        evidence, register_seen = _page_evidence(url, company, html=html, extracted=extracted, common=common)
        entry = UrlAnswer(url=origin, methods=[method], evidence=evidence, imprint_url=imprint_url,
                          register_number_seen=register_seen)
        entry.verdict = conf.score(methods=[method], evidence=evidence, name_uniqueness=uniqueness)
        answers[origin] = entry
    ranked = sorted(answers.values(), key=lambda a: a.verdict.value if a.verdict else 0.0, reverse=True)
    return [_answer_dict(a) for a in ranked]


def discover(
    company: Any,
    *,
    use_url_patterns: bool = True,
    common_words: set[str] | frozenset[str] = frozenset(),
) -> CompanyResult:
    """Run the enabled methods for one company and return one merged answer.

    `common_words` is the measured set of words that too many companies in this dataset share to
    identify one of them — `name_frequency.common_words` over the whole table. It decides whether
    a domain names an industry or a business. Passing nothing still works and falls back to the
    short hand-written list in `url_patterns.GENERIC_WORDS`.
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
    # The current name first, then the names the register had before: a domain often keeps the
    # old name. A former name only decides which domains are tried and how the domain is judged;
    # the page must still identify the company as it is today (`_page_evidence`).
    if use_url_patterns:
        tried: set[str] = set()
        names = [(company.company_name, "")] + [
            (name, "former_name:") for name in getattr(company, "former_names", ())[:MAX_FORMER_NAMES]]
        for name, label in names:
            try:
                found = url_patterns.find_live_urls(name, company.city, skip_hosts=tried)
                tried |= found.guessed_hosts
                result.candidates_generated += found.candidates_generated
                result.candidates_resolved += found.candidates_resolved
                result.candidates_probed += found.candidates_probed
                result.parked_rejected += len(found.rejected_parked)

                for probe in found.live:
                    # `probe.html` is already the body of `final_url` — handing it over stops
                    # `best_imprint` re-downloading the homepage it was just given.
                    got = _imprint_for(probe.final_url, homepage=probe.html)
                    extracted = got[2] if got else None
                    imprint_url = got[1] if got else None
                    evidence, register_seen = _page_evidence(
                        probe.final_url,
                        company,
                        html=probe.html,
                        extracted=extracted,
                        redirected_off_host=probe.redirected_off_host,
                        common=common,
                        host_name=name,
                    )
                    entry = answer_for(probe.final_url)
                    entry.methods.append(conf.METHOD_URL_PATTERN)
                    entry.evidence |= evidence
                    entry.imprint_url = entry.imprint_url or imprint_url
                    entry.register_number_seen = entry.register_number_seen or register_seen
                    host = (urlparse(probe.url).hostname or "").lower()
                    pattern = found.pattern_by_host.get(host)
                    entry.pattern = entry.pattern or (label + pattern if pattern else None)
            except Exception as exc:  # noqa: BLE001
                result.errors.append(f"url_patterns: {type(exc).__name__}: {exc}")

    # ---- 2. Score --------------------------------------------------------------
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
    ranked = sorted(
        answers.values(),
        key=lambda a: (a.verdict.value if a.verdict else 0.0),
        reverse=True,
    )
    result.answers = [_answer_dict(a) for a in ranked]

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
    elif ranked and ranked[0].verdict:
        # Nothing cleared the bar. Report the best refusal so the row still says something.
        top = ranked[0]
        result.band = top.verdict.band
        result.verified_by = top.verdict.verified_by
        result.why = top.verdict.why
        result.confidence_value = top.verdict.value

    result.elapsed_s = round(time.monotonic() - started, 2)
    return result
