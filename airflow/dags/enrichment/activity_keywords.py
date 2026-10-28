"""Keywords for what a company does, in the company's own words.

Target column 7.1 ("Stichworte zur Tätigkeit und Produkte / Dienstleistungen"). Fusionbase has an
AI model write them from the website; this module only collects what the website itself states, so
every keyword can be found on a page we can link to.

Two places on a website name the products and services, and both are cheap to read:

- **`<meta name="keywords">`** on the homepage: a comma list the site wrote for search engines.
  Often missing, and often stale, but when present it is the company describing itself.
- **The headings (`h1`–`h3`) of the services and products pages** that the homepage links to
  (`content_pages.CATEGORIES`). A German business site lists its offer there as headings:
  "Webentwicklung", "Hausverwaltung", "Kältetechnik". Two categories are left out on purpose:
  "about us" (team members) and "solutions", whose pattern also picks `Referenzen` and `Projekte`
  pages. On the first 37 Berlin websites read (2026-10-28) one such page gave ten clients' names.

**Genericness is measured, never listed.** "Unsere Leistungen", "Kontakt", "Hauptnavigation" head
many services pages. They are found the same way `name_frequency` finds trade words: a term on
more than `GENERIC_SHARE` of the companies read so far (and on more than `GENERIC_MIN_COMPANIES`)
says nothing about any one of them. On those 37 websites no heading reached 4 companies, so all of
them would have passed: below `MIN_CORPUS` companies nothing is published.

What a candidate must look like is a shape, not a word list: 1 to 4 words, 3 to 60 characters, at
least one letter, no address, not a sentence ("Rufen Sie uns an:", "Was ist ...?"), and never
anything shaped like a person's name (`imprint_extractor.looks_like_person_name`, with or without
"Dr." or "Frau" in front). That last rule also drops some real keywords ("Erneuerbare Energien"),
which is the price of never publishing a name.

Nothing here has been hand-checked; the quality of a published keyword is unmeasured.
"""
from __future__ import annotations

import os
import re
from collections.abc import Iterable

from bs4 import BeautifulSoup

from enrichment.branch_classifier import document_frequency
from enrichment.imprint_extractor import looks_like_person_name

#: The `content_pages.CATEGORIES` whose headings name an offer. Not `ueber_uns` or `loesungen`,
#: see the header.
OFFER_CATEGORIES = ("leistungen", "produkte")

#: A term on more than this share of companies is generic. 2%: on 1,000 companies, 20 of them.
GENERIC_SHARE = float(os.getenv("ACTIVITY_KEYWORDS_GENERIC_SHARE", "0.02"))
#: ... and on more than this many. Without the floor, 30 companies make every pair a "trade".
GENERIC_MIN_COMPANIES = int(os.getenv("ACTIVITY_KEYWORDS_GENERIC_MIN_COMPANIES", "3"))
#: Fewer companies read than this, and frequency means nothing; nothing is published. 500 is a
#: guess, not a measurement: at 2% it takes 11 companies to call a heading generic.
MIN_CORPUS = int(os.getenv("ACTIVITY_KEYWORDS_MIN_CORPUS", "500"))
#: Keywords per company, in the order the site lists them.
MAX_KEYWORDS = int(os.getenv("ACTIVITY_KEYWORDS_MAX", "12"))
#: Candidates kept per company before genericness is known. Bounds the stored payload.
MAX_CANDIDATES = 40

_MIN_WORDS, _MAX_WORDS = 1, 4
_MIN_CHARS, _MAX_CHARS = 3, 60
_PERSON_PREFIX = re.compile(r"^(?:dr|prof|herr|frau|mr|mrs|ms)\b\.?", re.IGNORECASE)
#: A question, an exclamation, or a heading that leads into what follows ("Rufen Sie uns an:").
_SENTENCE = re.compile(r"[?!]|[.:]\s*$")


def term_key(term: str) -> str:
    """The comparison key of a term: lowercase, single spaces, no edge punctuation."""
    return re.sub(r"\s+", " ", term).strip(" .,:;!?-–|/").lower()


def is_candidate(term: str, company_name: str = "") -> bool:
    """Does this text have the shape of a keyword? See the module header for the rule."""
    key = term_key(term)
    if not (_MIN_CHARS <= len(key) <= _MAX_CHARS):
        return False
    if not (_MIN_WORDS <= len(key.split()) <= _MAX_WORDS):
        return False
    if not re.search(r"[a-zäöüß]", key) or "@" in key or "www." in key:
        return False
    if _SENTENCE.search(term):
        return False
    bare = _PERSON_PREFIX.sub("", term.strip()).strip()
    if _PERSON_PREFIX.match(key) or looks_like_person_name(bare):
        return False
    # The company's own name as a heading is not a keyword for it.
    return not (company_name and key in company_name.lower())


def meta_keywords(html: str) -> list[str]:
    """The comma list in `<meta name="keywords">`, in order; empty when there is none."""
    soup = BeautifulSoup(html or "", "html.parser")
    tag = soup.find("meta", attrs={"name": re.compile(r"^keywords$", re.I)})
    content = tag.get("content") if tag else None
    return [part.strip() for part in re.split(r"[,;]", content or "") if part.strip()]


def headings(html: str) -> list[str]:
    """The text of every `h1`–`h3`, in page order."""
    soup = BeautifulSoup(html or "", "html.parser")
    return [" ".join(h.get_text(" ").split()) for h in soup.find_all(["h1", "h2", "h3"])]


def candidates(homepage_html: str, offer_pages_html: Iterable[str], company_name: str = "") -> list[str]:
    """Every keyword-shaped term the site states, deduplicated by `term_key`, in page order."""
    terms = meta_keywords(homepage_html)
    for html in offer_pages_html:
        terms.extend(headings(html))
    found: dict[str, str] = {}
    for term in terms:
        if is_candidate(term, company_name):
            found.setdefault(term_key(term), " ".join(term.split()))
    return list(found.values())[:MAX_CANDIDATES]


def generic_terms(per_company: Iterable[Iterable[str]]) -> set[str]:
    """The term keys too common across companies to say anything about one of them.

    Empty below `MIN_CORPUS` companies, which callers must read as "not measured yet", not as
    "nothing is generic".
    """
    keyed = [{term_key(t) for t in terms} for terms in per_company]
    if len(keyed) < MIN_CORPUS:
        return set()
    bar = max(GENERIC_MIN_COMPANIES, GENERIC_SHARE * len(keyed))
    return {key for key, n in document_frequency(keyed).items() if n > bar}


def keywords(terms: Iterable[str], generic: set[str]) -> list[str]:
    """A company's keywords: its candidates minus the generic ones, at most `MAX_KEYWORDS`."""
    return [t for t in terms if term_key(t) not in generic][:MAX_KEYWORDS]
