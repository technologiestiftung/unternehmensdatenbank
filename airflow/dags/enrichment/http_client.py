from __future__ import annotations

import logging
import os

from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

URL_DISCOVERY_HTTP_TIMEOUT_S = float(os.getenv("URL_DISCOVERY_HTTP_TIMEOUT_S", "10"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0; +https://ts.berlin)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

IMPRESSUM_PATHS = [
    "/impressum",
    "/impressum.html",
    "/impressum.htm",
    "/impressum/",
    "/imprint",
    "/imprint.html",
    "/kontakt",
    "/contact",
    "/",
]

_IMPRINT_KEYWORDS = {"handelsregister", "hrb", "hra", "impressum", "umsatzsteuer", "ust-id"}


def _has_imprint_content(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in _IMPRINT_KEYWORDS)


def verify_url(url: str) -> dict | None:
    try:
        resp = requests.head(
            url, headers=HEADERS, timeout=URL_DISCOVERY_HTTP_TIMEOUT_S, allow_redirects=True
        )
        if resp.status_code < 400:
            return {"url": url, "final_url": str(resp.url), "status_code": resp.status_code}
    except Exception:
        pass

    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            timeout=URL_DISCOVERY_HTTP_TIMEOUT_S,
            allow_redirects=True,
            stream=True,
        )
        resp.close()
        if resp.status_code < 400:
            return {"url": url, "final_url": str(resp.url), "status_code": resp.status_code}
    except Exception:
        pass

    return None


def fetch_page(url: str) -> str | None:
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=URL_DISCOVERY_HTTP_TIMEOUT_S, allow_redirects=True
        )
        if resp.status_code == 200:
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
    except Exception:
        pass
    return None


def _soup(html: str) -> BeautifulSoup:
    """Parse with lxml where it exists, html.parser where it does not.

    The old code asked for lxml inside a bare `except: pass`, so on a machine without it the
    function silently returned None and every Impressum link went unseen. That is the host: lxml
    is in the container's requirements and not necessarily anywhere else, which also meant this
    could not be covered by a free guard.
    """
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:  # noqa: BLE001  FeatureNotFound, and anything else a builder raises
        return BeautifulSoup(html, "html.parser")


def _find_impressum_link(base_url: str, html: str) -> str | None:
    """The page's own Impressum link, resolved against `base_url`.

    Resolution is `urljoin`, not string concatenation, and the difference is the whole point.
    `base_url` is often a DEEP page — `search_discovery_runner` hands over whatever the search
    engine ranked, e.g. `/planen/service/kontakt`. Appending a root-relative `/service/impressum`
    to that produced `/planen/service/kontakt/service/impressum`, which 404s; `fetch_impressum`
    then fell through and settled for the contact page as the Impressum, so the register number
    was never seen and the candidate scored 20 instead of 120.

    Measured on Tourismusgesellschaft Osnabrücker Land: the register number is printed at
    /service/impressum, the row was stored with `register_number: null` and confidence 0.65.
    `combined_discovery` never hit this because it guesses origins, which is why it holds 667
    register-number matches against the search DAGs' 20.
    """
    try:
        for a in _soup(html).find_all("a", href=True):
            if "impressum" in a["href"].lower() or "imprint" in a["href"].lower():
                return urljoin(base_url, a["href"])
    except Exception:  # noqa: BLE001  malformed markup is not worth failing a candidate over
        pass
    return None


def fetch_impressum(base_url: str, homepage: str | None = None) -> tuple[str, str] | None:
    """Find an Impressum for `base_url`. Returns (html, url) or None.

    Pass `homepage` when the caller has already downloaded that exact URL's HTML. Without it this
    function starts by fetching the homepage itself, which for the combined-discovery pipeline meant
    every candidate site was downloaded twice: once by `url_patterns.probe` to decide whether it was
    a live page at all, then again here. Page fetching is about 29% of that run's wall clock.

    It must be the HTML of `base_url` after redirects, not of the URL that was originally guessed —
    the Impressum links inside it are resolved relative to `base_url`.
    """
    if homepage is None:
        homepage = fetch_page(base_url)

    if homepage:
        impressum_link = _find_impressum_link(base_url, homepage)
        if impressum_link:
            content = fetch_page(impressum_link)
            if content and _has_imprint_content(content):
                return content, impressum_link
        if _has_imprint_content(homepage):
            return homepage, base_url

    # Guessed paths hang off the SITE, not off whatever deep page was handed in. `/impressum`
    # appended to `/planen/service/kontakt` is a 404 every time — the same concatenation bug as
    # in `_find_impressum_link`, one function further down.
    for path in IMPRESSUM_PATHS[:-1]:
        url = urljoin(base_url, path)
        content = fetch_page(url)
        if content and _has_imprint_content(content):
            return content, url

    return None
