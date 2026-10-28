"""Guess a company's web address from its name, then check whether that address exists.

This is the hand-written half of discovery. No search engine, no language model, no API key.
"Wilkening Bauunternehmung GmbH" becomes a list of guesses — `wilkening-bauunternehmung.de`,
`wilkeningbauunternehmung.de`, `wilkening.de`, `wbu.de` — and each guess is checked.

Three stages, deliberately separated so each can be improved on its own:

1. `generate_candidates()` — name in, list of URLs out. Pure text work. No network. This is
   where new naming ideas go, and it is the stage with the most room left in it.
2. `resolves()` — does the domain name exist at all? One DNS lookup. A DNS lookup takes
   milliseconds; asking a web server takes up to 10 seconds. Most guesses are wrong, so
   checking the cheap thing first is what makes 1000 companies affordable.
3. `probe()` — fetch the page. Reports where the address ended up after redirects, and
   whether what came back is a real site or a parked domain.

**Parked domain** — a domain somebody registered but never built a site on. It answers HTTP
with 200 OK and shows an advert or a "this domain is for sale" page. Without stage 3's parked
check, `bauservice.de` counts as a hit for every company with "Bauservice" in its name.

Every threshold here is an environment variable, because none of them are settled.
"""
from __future__ import annotations

import logging
import os
import re
import socket
from dataclasses import dataclass, field
from urllib.parse import urlparse


from enrichment import page_cache
from enrichment.http_client import session

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------------------
# Tunables. All of them are guesses that measurement should replace.
# ---------------------------------------------------------------------------------------

#: Top-level domains tried, in order. `.de` first: these are German companies, and in the
#: 100-company Osnabrück sample every website found was a `.de` domain. `.com` and `.eu` are
#: cheap to add because the DNS pre-check throws them away in milliseconds when they do not
#: exist. Adding a TLD multiplies the DNS lookups per company, not the page fetches.
TLDS: tuple[str, ...] = tuple(
    t.strip().lstrip(".") for t in os.getenv("COMBINED_URL_TLDS", "de,com,eu,net").split(",") if t.strip()
)

#: Hard cap on guesses per company. Guesses are ordered best-first, so the cap cuts the tail,
#: which is where the bad guesses live. 24 = roughly 6 name shapes x 4 TLDs.
#:
#: Raise it to 32 if you switch `UMLAUT_VARIANTS` on, or the extra spellings are generated and then
#: truncated away unseen.
MAX_CANDIDATES = int(os.getenv("COMBINED_URL_MAX_CANDIDATES", "24"))

#: Whether to guess the other two spellings of an umlaut — `muller.de` and `müller.de` alongside
#: `mueller.de`. **Off, because it was measured and it found nothing.**
#:
#: The hypothesis was good and the arithmetic behind it still is: 4,016 of the 14,443 names (28%)
#: carry an umlaut, and all three spellings are live registrations, so one spelling probes a third of
#: the namespace. But on 100 companies, 27 of them with umlauts, the variants produced **5 fetched
#: pages out of 144 and not one winner** — 4 disqualified outright, and `otag.de` for "ÖTAG
#: Öltanklagerverwaltungs-" short of evidence, which is the only near-miss in the set. The cost was
#: 2,426 guesses instead of 2,004 and eight more pages for the judge to read at ~15 s each.
#:
#: Kept rather than deleted because 0 of 27 is a small sample and the code is written and guarded;
#: turn it on with a bigger run to settle it. What is NOT behind this flag is the *verification*
#: half — `host_stem` decoding punycode, and `search_common.host_matches_company` accepting both
#: spellings. That is a correctness fix regardless: a search provider handing us `müller.de` for
#: "Müller GmbH" was previously refused for not carrying the company's name.
UMLAUT_VARIANTS = os.getenv("COMBINED_URL_UMLAUT_VARIANTS", "false").lower() == "true"

#: Hard cap on how many *live* domains we bother fetching pages from. Fetching is the slow part,
#: so this one number decides how long a run takes.
#:
#: It was 4, on the reasoning that "if four different guesses all resolve, the fifth is not going
#: to be the right one". Measured on 100 Osnabrück companies, that reasoning was wrong: 317 guesses
#: resolved, only 257 were fetched, the cap bit on 23 companies, and **17 of those finished with no
#: website at all** while a live domain sat unopened. A common surname resolves on several TLDs
#: before the company's own domain is reached, and `.de` is not always first past the post.
MAX_PROBES = int(os.getenv("COMBINED_URL_MAX_PROBES", "8"))

#: Seconds to wait for the page body once a server has answered.
HTTP_TIMEOUT_S = float(os.getenv("COMBINED_URL_HTTP_TIMEOUT_S", "10"))

#: Seconds to wait for the connection itself. Kept short and separate: a guessed domain that
#: nobody is serving is the common case, and this is the timeout it burns.
CONNECT_TIMEOUT_S = float(os.getenv("COMBINED_URL_CONNECT_TIMEOUT_S", "4"))

#: A domain stem shorter than this is not distinctive enough to be worth a guess. "ab.de"
#: belongs to somebody, but almost certainly not to "AB Handel GmbH".
MIN_STEM_LENGTH = int(os.getenv("COMBINED_URL_MIN_STEM_LENGTH", "4"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0; +https://ts.berlin)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------------------
# Name cleaning
# ---------------------------------------------------------------------------------------

#: The legal form at the end of a German company name — "GmbH", "AG", "e.V." and friends.
#: Nobody puts it in their domain, so it is removed before guessing. Deliberately a copy of
#: `url_discovery.candidate_generator.LEGAL_SUFFIX_RE` rather than an import: that module is
#: the *old* generator, kept working and untouched, and this one is free to diverge.
LEGAL_SUFFIX_RE = re.compile(
    r"\b(?:"
    r"GmbH\s*&\s*Co\.?\s*KGaA|GmbH\s*&\s*Co\.?\s*KG|GmbH\s*&\s*Co\.?|GmbH"
    r"|AG\s*&\s*Co\.?\s*KG|KGaA|AG|KG|OHG|GbR|PartGmbB|PartG"
    r"|UG\s*\(haftungsbeschränkt\)|UG|e\.?\s*V\.?|e\.?\s*K\.?|SE|mbH|Ltd\.?|Inc\.?"
    r")\b",
    flags=re.IGNORECASE,
)

#: Words that say what a company owns rather than what it does. A "Verwaltungs GmbH" is the
#: administrative shell of an operating business; its website, if any, is the operating
#: business's. Dropping these words produces a SECOND stem, and both are tried — the full name
#: first, because dropping words is a guess about which half is the brand.
LOW_SIGNAL_WORDS = frozenset({
    "verwaltungs", "verwaltung", "beteiligungs", "beteiligung", "holding", "grundbesitz",
    "grundstuecks", "grundstucks", "vermoegensverwaltungs", "vermogensverwaltungs",
    "besitz", "management", "gesellschaft", "unternehmensgruppe", "gruppe", "und", "der",
    "die", "das", "fuer", "for",
    # Articles carry nothing. Without "the", the first-and-last-word rule turned "The Living
    # Proof Agency GmbH" into `theagency.de`, which belongs to somebody else.
    "the", "and", "von", "vom", "zur", "zum",
})

#: Words too common to be a domain on their own. `bau.de` is not "Bau Meyer GmbH"'s site.
#: Used only to block the single-word guess, never the multi-word ones.
GENERIC_WORDS = frozenset({
    "bau", "handel", "service", "services", "beratung", "consulting", "solutions", "group",
    "invest", "immobilien", "marketing", "media", "technik", "digital", "transport",
    "logistik", "pflege", "haus", "projekt", "sport", "auto", "reisen", "energie", "garten",
    "apotheke", "hotel", "restaurant", "praxis", "studio", "shop", "center", "team", "nord",
    "sued", "west", "ost", "city", "online", "system", "systeme", "partner", "werk", "werke",
})

#: This list is deliberately NOT extended when a new bad domain turns up. Cases like
#: `anlagenbau.de`, `automobile.com` or `gartencenter.de` are not fixed by adding three more
#: words — the next sample brings three others, and the list ends up describing the last run
#: rather than the problem. Two general rules do that job instead:
#:
#: 1. `name_frequency.common_words` COUNTS how many company names contain each word. Words in 8
#:    or more of the 14,443 are trades: `immobilien` 391, `bau` 209, `automobile` 24,
#:    `anlagenbau` 12. `is_generic_stem` takes that measured set as an argument.
#: 2. `name_coverage` below asks how much of the company's name the domain accounts for, which
#:    catches the ones counting cannot — `judentum`, `african` and `kinokunst` each appear in
#:    exactly one company name, so no frequency rule will ever flag them.
#:
#: Matching on word ENDINGS was tried and dropped as well. "-bau", "-technik" and "-handel" do
#: end industry words, but German surnames glue to them just as happily: the rule threw away
#: `muellertiefbau.de` for "Müller Hoch- und Tiefbau GmbH", which is a correct guess.

#: Common German first names. A company called "Karl Widera, Gaststättenbetriebe" makes the
#: single-word rule guess `karl.de`, which belongs to somebody else entirely. Only used to
#: block that one rule; "Karl" stays in every multi-word guess, where it is harmless.
GIVEN_NAMES = frozenset({
    "karl", "hans", "peter", "klaus", "michael", "thomas", "andreas", "wolfgang", "juergen",
    "stefan", "christian", "frank", "martin", "uwe", "dieter", "heinz", "werner", "helmut",
    "gerhard", "walter", "horst", "ralf", "bernd", "manfred", "guenter", "josef", "franz",
    "anna", "maria", "ursula", "monika", "petra", "sabine", "susanne", "birgit", "claudia",
    "andrea", "gabriele", "renate", "helga", "ingrid", "brigitte", "christa", "erika",
})

#: The longest a single piece of a domain name may be. A company with a long descriptive name
#: produces a slug over this length, and the DNS resolver raises rather than saying "no".
MAX_LABEL_LENGTH = 63

_UMLAUTS = (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss"))


#: Only the entries long enough to be unambiguous as a prefix. "und" or "der" as prefixes
#: would swallow "Unternehmen" and "Dermatologie".
_LOW_SIGNAL_PREFIXES = tuple(w for w in LOW_SIGNAL_WORDS if len(w) >= 7)


def is_low_signal(word: str) -> bool:
    """True for words that say what a company owns rather than what it does.

    Prefix matching, not membership. German glues words together: a company can be a
    "Beteiligungsgesellschaft" or a "Verwaltungsgesellschaft", and a plain set lookup misses
    both while catching "Beteiligungs" and "Verwaltungs". Without this, "Max Moritz
    Beteiligungsgesellschaft mbH" kept the compound in its core name, and the check for the
    company's name on its own page then looked for a word no page would ever print.
    """
    if word in LOW_SIGNAL_WORDS:
        return True
    return any(word.startswith(prefix) for prefix in _LOW_SIGNAL_PREFIXES)

#: Text that only appears on a domain with nothing behind it. Matched against the visible page
#: text, lowercased. Kept short: every entry is a chance to throw away a real site.
PARKED_MARKERS = (
    "diese domain steht zum verkauf",
    "diese domain wurde reserviert",
    "domain is for sale",
    "this domain is parked",
    "domain parking",
    "parked free, courtesy of",
    "buy this domain",
    # Measured 2026-08-12: `goldrushproductions.com` is a DomainMarket sales listing and was scored
    # as a live website for "Goldrush Productions GmbH". It says "Domain for Sale" and "This
    # premium domain is available for purchase from DomainMarket.com" — near-misses for every
    # marker above. Both phrases below are specific to domain sales; the looser "is available for
    # purchase" was rejected because a shop selling anything writes it.
    "domain for sale",
    "domain is available for purchase",
    "website coming soon",
    "diese webseite ist noch nicht",
    "hier entsteht eine neue",
    "hier entsteht in kürze",
    "under construction",
    "sedoparking",
    "domainprofi",
    "webseite in bearbeitung",
    "platzhalterseite",
)


def normalize(text: str) -> str:
    """Lowercase, umlauts spelled out, everything else stripped to letters and digits."""
    for src, dst in _UMLAUTS:
        text = text.replace(src, dst).replace(src.upper(), dst)
    text = text.lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def name_words(company_name: str) -> list[str]:
    """The words of a company name with the legal form removed.

    "Auto-Verkehr Heiter GmbH & Co. KG" -> ["auto", "verkehr", "heiter"]. Hyphens split,
    because a domain writes "auto-verkehr" and "autoverkehr" but never treats the hyphen as
    part of a word.
    """
    stripped = LEGAL_SUFFIX_RE.sub(" ", company_name)
    words = [w for w in normalize(stripped).split() if len(w) > 1]
    return words


#: The three ways a German business writes an umlaut in its own domain name, and the label each one
#: carries into the output so its yield can be measured instead of assumed.
SPELLING_TRANSLITERATED = ""
SPELLING_DROPPED = "umlaut_dropped"
SPELLING_IDN = "idn"

#: ä -> a rather than ae. The second real spelling: `muller.de` alongside `mueller.de`.
_UMLAUTS_DROPPED = (("ä", "a"), ("ö", "o"), ("ü", "u"), ("ß", "ss"))

_HAS_UMLAUT_RE = re.compile(r"[äöüÄÖÜß]")


def _normalize_with(text: str, table: tuple[tuple[str, str], ...], keep: str = "") -> str:
    """`normalize` with a different umlaut table, and optionally umlauts left in place."""
    for src, dst in table:
        text = text.replace(src, dst).replace(src.upper(), dst)
    text = text.lower()
    return re.sub(rf"[^a-z0-9{keep}]+", " ", text).strip()


def name_word_spellings(company_name: str) -> list[tuple[str, list[str]]]:
    """Every spelling of the name a German business might have registered, best-known first.

    Returns (label, words) pairs. One pair for a name with no umlaut in it, which is 72% of this
    table — nothing changes for those.

    Why three. Measured over the 14,443 companies, **4,016 names (28%) carry an umlaut or ß**, and
    all three spellings of them are live registrations: `mueller.de`, `muller.de` and `müller.de`
    (which DNS sees as `xn--mller-kva.de`) all resolve, as do the three forms of `bäckerei`,
    `schäfer`, `grün` and `küche`. Generating only the `ae/oe/ue` transliteration therefore probed
    one third of the plausible namespace for more than a quarter of the companies.

    **ß is the exception.** A name whose only special character is ß gets one spelling, not three.
    DENIC does permit `straßen.de`, but Python's built-in `idna` codec is IDNA2003 and its nameprep
    step maps ß to ss, so `"straßen.de".encode("idna")` returns `strassen.de` — the spelling the
    transliteration already produced. Reaching the real `xn--straen-3ya.de` needs the IDNA2008
    library, for a form German businesses seldom register while `ss` is available. The variant
    therefore collapses onto the first rather than costing a lookup.
    """
    stripped = LEGAL_SUFFIX_RE.sub(" ", company_name)
    out = [(SPELLING_TRANSLITERATED, name_words(company_name))]
    if not _HAS_UMLAUT_RE.search(company_name):
        return out

    dropped = [w for w in _normalize_with(stripped, _UMLAUTS_DROPPED).split() if len(w) > 1]
    if dropped and dropped != out[0][1]:
        out.append((SPELLING_DROPPED, dropped))

    # The umlaut kept as itself. `.de` permits these, and `generate_candidates` punycode-encodes
    # the host, because that is the form DNS and the eventual URL need.
    kept = [
        w
        for w in _normalize_with(stripped, (("ß", "ss"),), keep="äöü").split()
        if len(w) > 1
    ]
    if kept and kept not in [words for _, words in out]:
        out.append((SPELLING_IDN, kept))
    return out


def to_ascii_host(host: str) -> str:
    """A hostname DNS can be asked about: `müller.de` -> `xn--mller-kva.de`.

    Returns the input unchanged when it is already ASCII or cannot be encoded — an over-long or
    empty label makes Python's `idna` codec raise rather than return a failure, and a guessed name
    produces both.
    """
    if host.isascii():
        return host
    try:
        return host.encode("idna").decode("ascii")
    except (UnicodeError, ValueError):
        return host


def from_ascii_host(host: str) -> str:
    """The reverse: `xn--mller-kva.de` -> `müller.de`. Unchanged when it is not punycode."""
    if "xn--" not in host:
        return host
    try:
        return host.encode("ascii").decode("idna")
    except (UnicodeError, ValueError):
        return host


def host_stem(url_or_host: str) -> str:
    """The distinctive piece of a web address: `https://www.zweirad-liebig.de/x` -> `zweiradliebig`.

    Punycode is decoded and then transliterated, so `xn--mller-kva.de` comes back as `mueller` —
    the same string `name_words` produces for "Müller GmbH". Without this the IDN candidates this
    module now generates would every one be refused for not carrying the company's name, which is
    the opposite of the reason they were generated.
    """
    from urllib.parse import urlparse

    host = url_or_host
    if "//" in host:
        host = urlparse(host).hostname or ""
    host = from_ascii_host(host.lower()).removeprefix("www.")
    stem = host.split(".")[0].replace("-", "")
    return normalize(stem).replace(" ", "")


def is_generic_stem(stem: str, common: set[str] | frozenset[str] = frozenset()) -> bool:
    """Is this domain an industry rather than a company?

    `anlagenbau.de` is a trade. `zweiradliebig.de` is a business. The first cannot be inferred to
    belong to "Anlagenbau & Technischer Service Alte Poststraße GmbH" merely because the words
    overlap — many companies would fit it equally well.

    `common` is the measured set from `name_frequency.common_words`: words appearing in at least
    8 of the company names. Pass it whenever it is available. The default of nothing falls back
    to `GENERIC_WORDS` alone, which is why the fallback is a short list of obvious nouns and not
    a growing catalogue of every domain that ever went wrong.
    """
    return stem in GENERIC_WORDS or stem in common


def name_coverage(url_or_host: str, company_name: str) -> float:
    """How much of the company's name the domain actually accounts for, 0.0 to 1.0.

    Counted over the distinctive words — legal form gone, shell-company words gone, anything
    under three letters gone — and asks how many of them appear in the domain.

    This is the honest version of "does the domain match the name". `theagency.de` carries one
    word out of "The Living Proof Agency GmbH" and looked like a match to every rule that only
    checked whether *a* name word was present. Coverage says 0.33.

    Scored against every spelling of the name and the best one wins, because a company that writes
    itself "Müller" may own `muller.de`. Comparing only the `ae/oe/ue` form scored that domain 0.00
    and had it refused as not carrying the company's name.
    """
    stem = host_stem(url_or_host)
    if not stem:
        return 0.0
    best = 0.0
    for _spelling, spelled in name_word_spellings(company_name):
        words = {w for w in spelled if len(w) >= 3 and not is_low_signal(w)}
        if not words:
            continue
        best = max(best, sum(1 for w in words if w in stem) / len(words))
    return best


def _stems(words: list[str]) -> list[tuple[str, str]]:
    """Name shapes to try, best first, as (slug, why) pairs.

    `why` names the rule that produced the guess. It travels all the way to the output CSV, so
    "which of my rules actually finds websites" is answerable without re-running anything.
    """
    shapes: list[tuple[str, str]] = []
    if not words:
        return shapes

    full = "-".join(words)
    shapes.append((full, "full_name"))
    if len(words) > 1:
        shapes.append(("".join(words), "full_name_joined"))

    # The same name with the shell-company words removed. "Wilkening Verwaltungs" -> "Wilkening".
    core = [w for w in words if not is_low_signal(w)]
    if core and core != words:
        shapes.append(("-".join(core), "core_words"))
        if len(core) > 1:
            shapes.append(("".join(core), "core_words_joined"))

    base = core or words

    # First two words. The most common real German company domain shape after the full name.
    if len(base) >= 2:
        shapes.append(("".join(base[:2]), "first_two_joined"))
        shapes.append(("-".join(base[:2]), "first_two"))

    # First and last word. Catches "Müller Hoch- und Tiefbau" -> muellertiefbau.de, which a
    # first-two rule cannot reach.
    if len(base) >= 3:
        shapes.append(("".join([base[0], base[-1]]), "first_last_joined"))

    # A single distinctive word. Blocked for generic words, which keeps `bau.de` out, and for
    # first names, which keeps `karl.de` out for "Karl Widera, Gaststättenbetriebe".
    if (
        base
        and base[0] not in GENERIC_WORDS
        and base[0] not in GIVEN_NAMES
        and len(base[0]) >= MIN_STEM_LENGTH
    ):
        shapes.append((base[0], "first_word"))

    # Initials. "Meyer Schmidt Weber Koch" -> mswk.de.
    #
    # In practice this only fires for FOUR-word names, despite the range below. A three-word name
    # produces a three-letter acronym, and the `MIN_STEM_LENGTH` filter at the bottom of this
    # function drops anything under four characters a few lines later. The example this comment
    # used to carry, "Norddeutsche Bau Gesellschaft -> nbg.de", produces nothing at all — twice
    # over, since "Gesellschaft" is a shell word, leaving `base` with two entries rather than three.
    #
    # Left as it is rather than "fixed": measured over 1000 companies the acronym shape fetched 64
    # pages and won nothing, so making it reach further would buy more cost and no coverage.
    if 3 <= len(base) <= 4:
        acronym = "".join(w[0] for w in base)
        if len(acronym) >= 3:
            shapes.append((acronym, "acronym"))

    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for slug, why in shapes:
        slug = re.sub(r"-+", "-", slug).strip("-")
        if len(slug.replace("-", "")) < MIN_STEM_LENGTH or slug in seen:
            continue
        # Too long to be a domain name at all. Nobody registers a 60-character domain, and
        # the resolver raises rather than returning "does not exist" when asked for one.
        if len(slug) > MAX_LABEL_LENGTH:
            continue
        # A rule that keeps only PART of the name must not hand back an industry. "S & E
        # Automobile Verwaltungs- und Beteiligungsgesellschaft" reduced to `automobile.com`,
        # which describes a trade hundreds of companies are in. The whole-name rules are exempt:
        # if a company really is called "Anlagenbau GmbH", then that is its name.
        if why not in ("full_name", "full_name_joined") and is_generic_stem(slug.replace("-", "")):
            continue
        seen.add(slug)
        out.append((slug, why))
    return out


@dataclass
class Candidate:
    """One guessed URL, and the rule that guessed it."""

    url: str
    host: str
    pattern: str
    rank: int


def _shapes_for(words: list[str], city: str | None) -> list[tuple[str, str]]:
    """The name shapes for one spelling of the name, including the city forms."""
    shapes = _stems(words)
    if city:
        city_slug = normalize(city).replace(" ", "")
        core = [w for w in words if w not in LOW_SIGNAL_WORDS] or words
        if city_slug and len(city_slug) >= 3:
            shapes.append((f"{'-'.join(core)}-{city_slug}", "name_city"))
            shapes.append((f"{core[0]}-{city_slug}", "first_word_city"))
    return shapes


def generate_candidates(company_name: str, city: str | None = None) -> list[Candidate]:
    """Guessed URLs for a company, best guess first.

    `city` adds `<name>-<city>.de`, which is how tradespeople and franchise branches name
    their sites. It is tried last because it is the least likely shape.
    """
    spellings = name_word_spellings(company_name)
    if not spellings or not spellings[0][1]:
        return []
    if not UMLAUT_VARIANTS:
        spellings = spellings[:1]

    # The top-level domain is the OUTER loop, then the spelling, then the name shape. Every `.de`
    # guess is tried before any `.com` guess, because these are German companies: in the
    # 100-company Osnabrück sample, every website found sat on a `.de` domain. The other way round
    # — shape-major — spends the probe budget on `firma-name.com`, which a domain reseller owns,
    # before reaching `firma.de`, which the company owns.
    #
    # The spelling sits inside the TLD loop rather than outside it for the same reason: a company's
    # `muller.de` is a better guess than its own `mueller.com`.
    out: list[Candidate] = []
    seen_hosts: set[str] = set()
    for tld in TLDS:
        for spelling, words in spellings:
            # An umlaut kept as itself is a `.de` registry feature. `muller.com` is reached by the
            # dropped spelling, so encoding umlauts for every TLD would only spend lookups.
            if spelling == SPELLING_IDN and tld != "de":
                continue
            for slug, why in _shapes_for(words, city):
                host = to_ascii_host(f"{slug}.{tld}")
                if host in seen_hosts:
                    continue
                seen_hosts.add(host)
                out.append(
                    Candidate(
                        url=f"https://{host}",
                        host=host,
                        # The spelling travels to the output CSV, so "did the umlaut variants ever
                        # find anything" is answerable without re-running.
                        pattern=f"{why}+{spelling}" if spelling else why,
                        rank=len(out),
                    )
                )
                if len(out) >= MAX_CANDIDATES:
                    return out
    return out


# ---------------------------------------------------------------------------------------
# Stage 2 — does the domain exist?
# ---------------------------------------------------------------------------------------


def resolves(host: str) -> bool:
    """True when the domain name has an IP address behind it.

    One DNS lookup. This is the filter that makes the whole approach affordable: of the ~24
    guesses per company, typically 2 to 4 resolve, and only those cost a page fetch.

    `UnicodeError` is caught alongside the network errors on purpose. A company called
    "Judentum begreifen - Projekt für Schule und Gemeinde" turns into a 52-character label,
    and Python raises "label empty or too long" from inside the resolver rather than returning
    a failure. Uncaught, that one company took its whole row down.
    """
    try:
        socket.getaddrinfo(host, None)
        return True
    except (OSError, UnicodeError):
        return False


# ---------------------------------------------------------------------------------------
# Stage 3 — fetch the page and judge what came back
# ---------------------------------------------------------------------------------------


@dataclass
class Probe:
    """What a live URL turned out to be."""

    url: str
    final_url: str
    status_code: int
    html: str = ""
    parked: bool = False
    #: Set when the domain redirected somewhere else entirely, e.g. to a parent company.
    redirected_off_host: bool = False
    error: str | None = None


def visible_text(html: str) -> str:
    """Page text with the tags taken out. Crude on purpose — only used for keyword checks."""
    text = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", html)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def looks_parked(html: str) -> bool:
    """True when the page is a placeholder rather than a company's site.

    Two signals. First, one of the known parking phrases. Second, a page with almost no text
    at all — under 200 visible characters — which is what a pure advert redirect looks like.
    """
    text = visible_text(html).lower()
    if any(marker in text for marker in PARKED_MARKERS):
        return True
    return len(text) < 200


def _attempts(url: str) -> list[str]:
    """The addresses to try for one guessed domain, in order.

    Measured on the first five companies of the sample: `https://eckert.de` fails with an SSL
    error and so does `https://www.eckert.de`, while `http://eckert.de` answers 200 and
    forwards to the company's real site. Small German business sites frequently have a
    certificate only on one hostname, or none at all, and a bare `https://` probe therefore
    reports "no website" for a company that plainly has one.

    Each extra address costs a request ONLY when the one before it failed in a way the next
    one could fix — see `_worth_retrying`.
    """
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return [url]
    return [f"https://{host}", f"https://www.{host}", f"http://{host}", f"http://www.{host}"]


def _worth_retrying(error_name: str) -> bool:
    """Would a different scheme or hostname have any chance after THIS failure?

    Yes for a certificate problem or a refused connection: the certificate might be valid on
    `www.`, and port 80 is often open when port 443 is not.

    No for a timeout. A host that does not answer within the timeout does not answer on the
    other port either, and retrying costs the full timeout again — four times, for every
    guessed domain, for 1000 companies. This one check is the difference between a run that
    takes an hour and a run that takes most of a day.
    """
    return error_name in {"SSLError", "ConnectionError", "TooManyRedirects"}


def _probe_from_record(url: str, record: dict) -> Probe:
    """Rebuild a Probe from a cached HTTP result.

    `parked` and `redirected_off_host` are recomputed rather than stored, so a change to
    `PARKED_MARKERS` or to the redirect rule applies to cached pages too. Only the HTTP facts —
    final url, status, body — come from the cache. See `page_cache` for why.
    """
    html = record.get("html") or ""
    status = int(record.get("status_code") or 0)
    if not status:
        return Probe(url=url, final_url=url, status_code=0, error=record.get("error"))
    final_url = record.get("final_url") or url
    guessed_host = (urlparse(url).hostname or "").removeprefix("www.")
    final_host = (urlparse(final_url).hostname or "").removeprefix("www.")
    return Probe(
        url=url,
        final_url=final_url,
        status_code=status,
        html=html,
        parked=looks_parked(html),
        redirected_off_host=bool(final_host) and final_host != guessed_host,
    )


def probe(url: str) -> Probe | None:
    """Fetch a guessed URL, trying `www.` and plain `http` when that could help.

    A GET, not a HEAD. HEAD is cheaper but returns no body, and the body is what tells a real
    site from a parked one. Since the DNS pre-check already removed the domains that do not
    exist, the number of GETs here is small.

    Served from `page_cache` when one is configured, including the failures: a host that did not
    answer is worth remembering, or every replay would pay the full timeout for it again.
    """
    cached = page_cache.get("probe", url)
    if cached is not None:
        return _probe_from_record(url, cached)

    last_error: str | None = None
    for attempt in _attempts(url):
        try:
            response = session().get(
                attempt,
                headers=HEADERS,
                # (connect, read). Connecting is where a dead host burns time, and 4 seconds
                # is already generous for a server that is going to answer at all.
                timeout=(CONNECT_TIMEOUT_S, HTTP_TIMEOUT_S),
                allow_redirects=True,
            )
        except Exception as exc:  # noqa: BLE001  an unreachable guess is the expected outcome
            last_error = type(exc).__name__
            if _worth_retrying(last_error):
                continue
            break
        if response.status_code >= 400:
            last_error = f"HTTP {response.status_code}"
            continue

        response.encoding = response.apparent_encoding or "utf-8"
        html = response.text
        page_cache.put(
            "probe",
            url,
            {
                "final_url": str(response.url),
                "status_code": response.status_code,
                "html": html,
            },
        )
        guessed_host = (urlparse(url).hostname or "").removeprefix("www.")
        final_host = (urlparse(str(response.url)).hostname or "").removeprefix("www.")
        return Probe(
            url=url,
            final_url=str(response.url),
            status_code=response.status_code,
            html=html,
            parked=looks_parked(html),
            redirected_off_host=bool(final_host) and final_host != guessed_host,
        )
    page_cache.put("probe", url, {"status_code": 0, "error": last_error})
    return Probe(url=url, final_url=url, status_code=0, error=last_error)


@dataclass
class UrlPatternResult:
    """Everything stage 1 learned about one company."""

    candidates_generated: int = 0
    candidates_resolved: int = 0
    candidates_probed: int = 0
    live: list[Probe] = field(default_factory=list)
    rejected_parked: list[str] = field(default_factory=list)
    #: host -> the rule that guessed it, so the winning URL can say which pattern found it.
    pattern_by_host: dict[str, str] = field(default_factory=dict)


def find_live_urls(company_name: str, city: str | None = None) -> UrlPatternResult:
    """Run all three stages for one company and return the live, non-parked URLs.

    Order is preserved: `live[0]` came from the best-ranked guess that survived.
    """
    result = UrlPatternResult()
    candidates = generate_candidates(company_name, city)
    result.candidates_generated = len(candidates)

    resolving = [c for c in candidates if resolves(c.host)]
    result.candidates_resolved = len(resolving)

    for candidate in resolving[:MAX_PROBES]:
        result.pattern_by_host[candidate.host] = candidate.pattern
        result.candidates_probed += 1
        got = probe(candidate.url)
        if not got or got.status_code == 0 or got.status_code >= 400:
            continue
        if got.parked:
            result.rejected_parked.append(got.final_url)
            continue
        result.live.append(got)
    return result
