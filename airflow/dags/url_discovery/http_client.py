from __future__ import annotations

import logging
import os

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


def _find_impressum_link(base_url: str, html: str) -> str | None:
    try:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "impressum" in href or "imprint" in href:
                href_full = a["href"]
                if href_full.startswith("http"):
                    return href_full
                base = base_url.rstrip("/")
                return base + "/" + href_full.lstrip("/")
    except Exception:
        pass
    return None


def fetch_impressum(base_url: str) -> tuple[str, str] | None:
    homepage = fetch_page(base_url)

    if homepage:
        impressum_link = _find_impressum_link(base_url, homepage)
        if impressum_link:
            content = fetch_page(impressum_link)
            if content and _has_imprint_content(content):
                return content, impressum_link
        if _has_imprint_content(homepage):
            return homepage, base_url

    base = base_url.rstrip("/")
    for path in IMPRESSUM_PATHS[:-1]:
        url = f"{base}{path}"
        content = fetch_page(url)
        if content and _has_imprint_content(content):
            return content, url

    return None
