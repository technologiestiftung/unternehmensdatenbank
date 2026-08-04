from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup

HRB_RE = re.compile(r"\b(HR[AB])\s*[.:\s]?\s*(\d+)\b", re.IGNORECASE)
VAT_RE = re.compile(r"\bDE\s*(\d{9})\b")
POSTCODE_RE = re.compile(r"\b(\d{5})\b")


def extract_register_number(text: str) -> str | None:
    m = HRB_RE.search(text)
    if m:
        return f"{m.group(1).upper()} {m.group(2)}"
    return None


def extract_vat_id(text: str) -> str | None:
    m = VAT_RE.search(text)
    if m:
        return f"DE{m.group(1)}"
    return None


def extract_postcode(text: str) -> str | None:
    m = POSTCODE_RE.search(text)
    return m.group(1) if m else None


def extract_from_html(html: str) -> dict[str, Any]:
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)

    return {
        "register_number": extract_register_number(text),
        "vat_id": extract_vat_id(text),
        "postcode": extract_postcode(text),
        "text_snippet": text[:500],
    }
