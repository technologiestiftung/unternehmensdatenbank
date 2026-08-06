"""Shared shape for web-search providers.

Exa, Brave and Parallel return three different JSON layouts for the same idea: a ranked
list of pages with some text attached. Normalizing to `SearchHit` means the consumers —
`scripts/benchmark_url_owner_discovery.py` and the discovery DAGs — hold one contract and
the provider becomes a swappable detail.

Deliberately NOT an LLM contract. `mistral_http.run_websearch_prompt()` and
`claude_http.run_websearch_prompt()` return a model's *answer*; these clients return raw
search results that something else still has to read. That difference is why Exa and Brave
cannot slot into the benchmark's existing provider functions.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


#: Hosts that describe a company without being its site. Their text is useful for extraction —
#: registry pages carry founding year, address and register numbers — so they are kept as
#: `candidate_url` observations, but they must never become the company's `website`.
#:
#: Needed only for search providers. main's url_discovery generates candidates from the company
#: name, so a directory can never appear there; a search engine returns them constantly.
#: Measured on 3 companies: without this, `fahrradreparatur.org` was recorded as Zweirad Liebig
#: Verwaltungs GmbH's website at confidence 0.65 and `tracxn.com` as Zeitgeist GmbH's at 0.43 —
#: both above the resolver's 0.35 website threshold, so both would have become canonical.
DIRECTORY_HOSTS = (
    "northdata.", "creditreform.", "kompany.", "implisense.", "unternehmen24.",
    "firmeneintrag.", "branchenbuch.", "meinestadt.", "dnb.com", "infobel.",
    "wer-zu-wem.", "handelsregister", "openregister.", "bundesanzeiger.", "unternehmensregister.",
    "near-place.", "onlinestreet.", "pedali.", "reparadius.", "fahrradreparatur.",
    "companyhouse.", "firmenwissen.", "moneyhouse.", "cylex.", "dasoertliche.", "11880.",
    "trustpilot.", "provenexpert.", "tracxn.", "crunchbase.", "dealroom.", "pitchbook.",
    "linkedin.com", "facebook.com", "instagram.com", "xing.com", "youtube.com", "twitter.com",
    "kununu.", "indeed.", "stepstone.", "wikipedia.org", "yelp.", "gelbeseiten.",
    "google.", "apple.com", "amazon.", "ebay.",
)


def is_directory_host(url: str) -> bool:
    """True when the URL belongs to a directory/registry/social site rather than a company."""
    from urllib.parse import urlparse

    host = (urlparse(url).hostname or "").lower()
    return bool(host) and any(marker in host for marker in DIRECTORY_HOSTS)


@dataclass
class SearchHit:
    """One result. `text` is whatever the provider gives: page text, highlights, snippets."""

    url: str
    title: str = ""
    text: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchResponse:
    hits: list[SearchHit] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)
    #: Provider-reported spend, when the API volunteers it. Only Exa does (`costDollars`),
    #: which makes it the one provider whose cost is measured rather than estimated.
    cost_usd: float | None = None
    raw: Any = None

    def as_text(self) -> str:
        """Flatten to the labelled block format the extraction prompts expect."""
        return "\n\n---\n\n".join(
            f"SOURCE: {hit.title}\nURL: {hit.url}\n{hit.text}".strip() for hit in self.hits
        )


def name_tokens(name: str) -> list[str]:
    """Distinctive lowercase tokens from a company name, legal form stripped.

    Splits on hyphens as well as spaces. Keeping "auto-verkehr" as one token would make it
    unmatchable: hit_is_about_company() flattens every non-alphanumeric run in the haystack
    to a space, so a token containing a hyphen can never be found and the filter silently
    rejects results it should keep.
    """
    from source_enrichment_common import LEGAL_SUFFIX_RE

    simplified = LEGAL_SUFFIX_RE.sub(" ", name.lower()).replace("&", " ")
    for src, dst in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        simplified = simplified.replace(src, dst)
    simplified = re.sub(r"[^a-z0-9]+", " ", simplified)
    return [t for t in simplified.split() if len(t) > 3]


def hit_is_about_company(hit: SearchHit, tokens: list[str]) -> bool:
    """Is this result about our company, or a confusably-named different one?

    The discriminating case, measured: searching "GRAWITEX GmbH" returns an infobel entry
    for *Gravitex, Berlin* and two pages from *grafitex.eu* alongside the correct northdata
    entry. Handing all of it to an LLM produced a fluent description of the wrong company.

    One distinctive token suffices for a single-word name; multi-word names need two, so
    "Heiter und Wolkig" cannot pass for "Auto-Verkehr Heiter".
    """
    if not tokens:
        return True
    from urllib.parse import unquote

    haystack = re.sub(r"[^a-z0-9]+", " ", f"{hit.title} {unquote(hit.url)}".lower())
    hits = sum(1 for t in set(tokens) if t in haystack)
    return hits >= (1 if len(set(tokens)) < 2 else 2)


def filter_hits(hits: list[SearchHit], company_name: str) -> tuple[list[SearchHit], int]:
    """Returns (kept, dropped_count)."""
    tokens = name_tokens(company_name)
    kept = [h for h in hits if hit_is_about_company(h, tokens)]
    return kept, len(hits) - len(kept)


def host_matches_company(url: str, company_name: str) -> bool:
    """Does the URL's HOST carry the company name, i.e. is this plausibly its own domain?

    Stricter than hit_is_about_company(), and deliberately so. That one accepts a match
    anywhere in title or URL, which is right for "is this text about the company" — but a
    directory listing satisfies it too: `fahrradreparatur.org/osnabrueck/zweirad-liebig-gmbh`
    carries the name in its *path* while the host belongs to someone else. It was recorded as
    Zweirad Liebig Verwaltungs GmbH's website until this check existed, and no host blocklist
    would have caught that domain.

    A company's own site almost always has its name in the domain, so require the host stem
    to match: either equal to a name-derived slug, or containing two distinct name tokens.
    """
    from urllib.parse import urlparse

    tokens = name_tokens(company_name)
    if not tokens:
        return False
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return False
    stem = host.removeprefix("www.").split(".")[0]
    flat = stem.replace("-", "")
    if flat in {"".join(tokens), *tokens} or stem in tokens:
        return True
    return sum(1 for t in set(tokens) if t in flat) >= 2
