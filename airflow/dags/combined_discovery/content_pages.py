"""Get the page that actually says what a company does.

The pipeline downloads two pages per company: the **homepage**, and the **Impressum** — the legal
contact page every German business website must have. Both are the wrong place to look for an
industry. The Impressum is boilerplate: a name, an address, a register number, a VAT id. The homepage
is a slogan and a photograph. Measured over the 699 companies where we have both, the homepage does
better than the Impressum and neither does well:

    at least one branch keyword on the homepage   474 of 699  (68%)
    at least one branch keyword on the Impressum  311 of 699  (44%)

Where a German business writes its industry vocabulary is `/leistungen`, `/ueber-uns`, `/produkte`.
And we can find those pages **without downloading anything**, because the homepage is already on disk
and it links to them. Measured over the 766 cached homepages:

    homepages linking to a services or about-style page   644  (84%)
      ... "Über uns / Unternehmen"                        463
      ... "Leistungen / Services / Angebot"               453
      ... "Produkte / Sortiment / Speisekarte"            259
      ... "Lösungen / Branchen / Referenzen"              230

So finding the links is free and only fetching costs anything: 1,813 requests for all 902 companies
under the cap below, minutes at eight threads, then cached and never fetched again.

**Follow the links, do not guess the paths.** A fixed list would ask for `/leistungen` and get a 404
from every site that calls it `/de/unternehmen/leistungen`. This repo already learned that lesson
from the other direction: `url_discovery.http_client.fetch_impressum` follows the homepage's own
links, and `imprint_probe.PROBE_PATHS` exists only as the fallback for when that fails.

**The cap is not optional.** One site with a link-heavy navigation offers forty matching links. Two
per category and four per company keeps the run finite and keeps one company from spending everyone
else's budget.

Two namespaces are used in `page_cache`, and the split matters:

    content:<page url>    the raw HTML of one services or about page
    content-index:<base>  which pages were chosen for this company, and what category each was

The index is what makes a second run free: without it, the links have to be parsed again and the
cache asked page by page. As everywhere else in `page_cache`, only raw HTML is stored — never
anything derived from it — so changing what gets read out of a page still costs nothing.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import requests

from combined_discovery import page_cache
from combined_discovery.url_patterns import CONNECT_TIMEOUT_S, HEADERS, HTTP_TIMEOUT_S

log = logging.getLogger(__name__)

#: What a link has to look like to be worth fetching, by category. Matched against the href AND the
#: anchor text, because plenty of sites label the link "Was wir tun" and point it at `/wwt`.
#:
#: These are patterns over link *labels*, not evidence about a company, so unlike the branch keywords
#: they are hand-written on purpose — there is nothing to measure them against, and a missing one
#: costs a page rather than producing a wrong answer.
CATEGORIES: dict[str, re.Pattern[str]] = {
    "leistungen": re.compile(
        r"leistung|dienstleistung|service|angebot|portfolio|kompetenz|was.wir.tun", re.I
    ),
    "produkte": re.compile(r"produkt|sortiment|katalog|speisekarte|menu|shop", re.I),
    "ueber_uns": re.compile(r"ueber.?uns|über.?uns|about|unternehmen|philosophie|wir.sind", re.I),
    "loesungen": re.compile(r"loesung|lösung|branchen|anwendung|referenz|projekte", re.I),
}

#: At most this many pages per category, and this many per company, in CATEGORIES order.
MAX_PER_CATEGORY = int(os.getenv("COMBINED_CONTENT_MAX_PER_CATEGORY", "2"))
MAX_PER_COMPANY = int(os.getenv("COMBINED_CONTENT_MAX_PER_COMPANY", "4"))

#: Links that go somewhere a browser would not fetch as a page.
_SKIP_SCHEMES = ("mailto:", "tel:", "javascript:", "data:", "#")

#: File extensions that are a download rather than a page. A 30 MB brochure PDF is not worth the
#: bandwidth and `visible_text` cannot read it anyway.
_SKIP_SUFFIXES = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".zip", ".doc", ".docx", ".xls",
                  ".xlsx", ".mp4", ".webp", ".ico")


@dataclass(frozen=True)
class ContentLink:
    """One page worth fetching, and why it was picked."""

    category: str
    url: str
    #: The anchor text that matched, kept so a surprising choice can be explained afterwards.
    label: str


def _same_host(candidate: str, base: str) -> bool:
    """Is this link on the company's own site?

    A footer links to Facebook, to a web designer and to a chamber of commerce, and all three would
    otherwise be downloaded and read as if the company had written them. `www.` is ignored: a site
    linking from `x.de` to `www.x.de` is linking to itself.
    """
    left = (urlparse(candidate).hostname or "").lower().removeprefix("www.")
    right = (urlparse(base).hostname or "").lower().removeprefix("www.")
    return bool(left) and left == right


def links_from_homepage(
    html: str,
    base_url: str,
    *,
    max_per_category: int = MAX_PER_CATEGORY,
    max_per_company: int = MAX_PER_COMPANY,
) -> list[ContentLink]:
    """The services and about pages this homepage links to, capped.

    A pure function over HTML that is already on disk, so it costs nothing and can be tested without
    a network. Deterministic: categories in `CATEGORIES` order, links in the order the page lists
    them, so two runs pick the same pages and a cached result stays valid.
    """
    if not html or not base_url:
        return []
    try:
        from bs4 import BeautifulSoup
    except ImportError:  # pragma: no cover - beautifulsoup4 is in requirements.txt
        log.warning("beautifulsoup4 is missing; no content pages will be found")
        return []

    soup = BeautifulSoup(html, "html.parser")
    found: dict[str, list[ContentLink]] = {name: [] for name in CATEGORIES}
    seen: set[str] = set()
    homepage = base_url.split("#")[0].rstrip("/")

    for anchor in soup.find_all("a", href=True):
        href = (anchor["href"] or "").strip()
        if not href or href.lower().startswith(_SKIP_SCHEMES):
            continue
        label = " ".join(anchor.get_text(" ").split())[:80]
        full = urljoin(base_url, href).split("#")[0]
        if not full.lower().startswith(("http://", "https://")):
            continue
        if full.lower().endswith(_SKIP_SUFFIXES):
            continue
        if not _same_host(full, base_url):
            continue
        # The homepage links to itself from its logo, and re-reading it as a services page would
        # double-count every keyword already found there.
        if full.rstrip("/") == homepage or full in seen:
            continue
        for category, pattern in CATEGORIES.items():
            if len(found[category]) >= max_per_category:
                continue
            if pattern.search(href) or pattern.search(label):
                found[category].append(ContentLink(category=category, url=full, label=label))
                seen.add(full)
                break

    ordered: list[ContentLink] = []
    for category in CATEGORIES:
        ordered.extend(found[category])
    return ordered[:max_per_company]


def _fetch(url: str) -> str:
    """One GET, cached, never raising. Empty string for anything that did not come back as a page.

    Failures are cached too, as an empty record: a host that refused once will refuse on every
    replay, and paying the timeout again for all of them is how a minutes-long pass becomes an
    hours-long one. Same reasoning as `url_patterns.probe`.

    No scheme or `www.` retrying here, unlike `probe`. That retrying exists because a *guessed*
    domain may only answer on one of four addresses; this URL was written by the site itself on a
    page that already loaded, so the address is known good and a second attempt only costs time.
    """
    cached = page_cache.get("content", url)
    if cached is not None:
        return cached.get("html") or ""
    html = ""
    final_url = url
    try:
        response = requests.get(
            url, headers=HEADERS, timeout=(CONNECT_TIMEOUT_S, HTTP_TIMEOUT_S), allow_redirects=True
        )
        if response.status_code < 400:
            response.encoding = response.apparent_encoding or "utf-8"
            html = response.text
            final_url = str(response.url)
    except Exception as exc:  # noqa: BLE001  an unreachable page is an expected outcome
        log.debug("content page %s failed: %s", url, type(exc).__name__)
    page_cache.put("content", url, {"html": html, "final_url": final_url})
    return html


@dataclass
class ContentPages:
    """Every services or about page we have for one company."""

    #: category -> readable text. Only pages that actually came back appear.
    pages: dict[str, str]
    #: What was chosen, whether or not it could be fetched. Distinguishes "no such link on the
    #: homepage" from "the link was there and the page would not load".
    links: list[ContentLink]
    fetched: int = 0
    failed: int = 0

    @property
    def text(self) -> str:
        """All of it, in CATEGORIES order, as one string for the matcher."""
        return " ".join(self.pages[c] for c in CATEGORIES if c in self.pages)


def content_for(
    base_url: str,
    homepage_html: str,
    *,
    fetch: bool = True,
) -> ContentPages:
    """The company's services and about pages, from the cache and — when allowed — the network.

    `fetch=False` reports what is already on disk and downloads nothing, which is what makes a
    dry run and a re-run of `classify` free.
    """
    index = page_cache.get("content-index", base_url)
    if index is not None:
        links = [ContentLink(**entry) for entry in index.get("links") or []]
    else:
        links = links_from_homepage(homepage_html, base_url)
        page_cache.put(
            "content-index",
            base_url,
            {"links": [{"category": x.category, "url": x.url, "label": x.label} for x in links]},
        )

    from combined_discovery.url_patterns import visible_text

    pages: dict[str, str] = {}
    fetched = failed = 0
    for link in links:
        if fetch:
            html = _fetch(link.url)
        else:
            cached = page_cache.get("content", link.url)
            html = (cached or {}).get("html") or ""
        if html:
            fetched += 1
            pages.setdefault(link.category, visible_text(html))
        else:
            failed += 1
    return ContentPages(pages=pages, links=links, fetched=fetched, failed=failed)
