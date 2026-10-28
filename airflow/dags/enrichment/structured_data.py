"""Read the machine-readable business card that many websites already publish.

Search engines want to show a business's name, address and opening hours, so a lot of sites embed
exactly that in their HTML as JSON — a `<script type="application/ld+json">` block using the
schema.org vocabulary. It is the only place on a web page where a company states its own name as
*data* rather than as decoration, and until now this repo did not look at it at all.

Why that matters here specifically: whether a domain belongs to a company is decided by
`search_common.host_matches_company`, which compares the domain string against the company name.
That works for `zweirad-liebig.de` and fails completely for a company whose domain says nothing about
its name. A page that declares

    {"@type": "LocalBusiness", "name": "Zweirad Liebig GmbH",
     "address": {"@type": "PostalAddress", "postalCode": "49074", "addressLocality": "Osnabrück"}}

has told us whose site it is without anybody having to guess a domain.

Two rules govern everything below.

1. **Never raise.** Embedded JSON-LD in the wild is frequently broken — trailing commas, raw
   newlines inside strings, PHP notices printed into the middle of the block, two JSON documents
   concatenated. A page with a malformed block has to behave exactly like a page with none.
2. **Do not decide anything.** This module reports what the page claims. Whether a claim counts as
   evidence, and what it is worth, is `confidence.py`'s job — it is the only place in the pipeline
   where evidence becomes a number.

A page can of course claim anything it likes. That is why what comes out of here is compared against
the company register rather than believed: a name that agrees with the register is evidence, and a
name that does not is nothing.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

#: `type="application/ld+json"` with the tolerance real pages need: any capitalisation, optional
#: whitespace, and a trailing `;charset=utf-8` that some content-management systems add.
_LD_JSON_TYPE = re.compile(r"^\s*application/ld\+json\s*(;.*)?$", re.IGNORECASE)

#: Fallback when there is no HTML parser available, or when the block sits somewhere BeautifulSoup
#: will not return it from. Deliberately non-greedy so two adjacent blocks stay two blocks.
_SCRIPT_RE = re.compile(
    r"<script[^>]*type\s*=\s*[\"']?application/ld\+json[\"']?[^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)

#: Keys that make a node a description of a business rather than of a product, an article or a
#: navigation menu.
#:
#: Matched structurally, on the keys a node HAS, instead of against a list of schema.org type names.
#: That is not laziness: `LocalBusiness` has several hundred subtypes — `Dentist`, `AutoRepair`,
#: `Bakery`, `Notary` — and a hand-kept list of them would be permanently one German trade short.
#: A node carrying a name and a postal address is the thing we are looking for whatever it calls
#: itself, and a node carrying neither is not, even if it calls itself an Organization.
_BUSINESS_KEYS = ("address", "telephone", "vatid", "taxid", "email")

#: Types that identify a business even with no address attached, for the pages that publish little.
#: Substring matching, so `LocalBusiness`, `OnlineBusiness` and `Organization` are all covered
#: without naming the subtypes.
_BUSINESS_TYPE_MARKERS = ("organization", "business", "corporation", "store", "ngo")


def _blocks(html: str) -> list[str]:
    """The raw text of every JSON-LD script block on the page."""
    found: list[str] = []
    try:
        soup = BeautifulSoup(html, "lxml")
        for script in soup.find_all("script"):
            if _LD_JSON_TYPE.match(script.get("type") or ""):
                text = script.string or script.get_text() or ""
                if text.strip():
                    found.append(text)
    except Exception as exc:  # noqa: BLE001
        log.debug("could not parse HTML for JSON-LD: %s", type(exc).__name__)

    if not found:
        found = [m.group(1) for m in _SCRIPT_RE.finditer(html) if m.group(1).strip()]
    return found


def _parse(block: str) -> Any:
    """One JSON-LD block, or None when it cannot be read.

    The retry is worth the four lines: a very common breakage is a block that ends with a stray
    comma or a second document appended after the first, and `raw_decode` reads the valid document
    at the front and ignores the rest.
    """
    text = block.strip()
    # Sites wrap the JSON to hide it from browsers that would try to run it, and every wrapper
    # style shows up in the wild: an HTML comment, a bare CDATA section, and CDATA behind either
    # kind of JavaScript comment — `//<![CDATA[` and `/* <![CDATA[ */`. Unwrapped in one pass each
    # end, with the comment markers optional, rather than one rule per combination.
    text = re.sub(r"^<!--+|--+>$", "", text).strip()
    text = re.sub(r"^(?://|/\*)?\s*<!\[CDATA\[\s*(?:\*/)?", "", text).strip()
    text = re.sub(r"(?://|/\*)?\s*\]\]>\s*(?:\*/)?$", "", text).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        try:
            return json.JSONDecoder().raw_decode(text)[0]
        except Exception:  # noqa: BLE001
            return None


def _walk(node: Any, out: list[dict[str, Any]]) -> None:
    """Collect every dict anywhere in the structure.

    Recursive because the shape varies more than the specification suggests: a bare object, a list of
    objects, an `@graph` holding the real content, or a business nested under a `publisher` or
    `parentOrganization` key. Walking everything and filtering afterwards is shorter than trying to
    know all the shapes, and it cannot miss one.
    """
    if isinstance(node, dict):
        out.append(node)
        for value in node.values():
            _walk(value, out)
    elif isinstance(node, list):
        for value in node:
            _walk(value, out)


def _text(value: Any) -> str | None:
    """A string out of a field that is allowed to be a string, a list, or an object with a name."""
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        for item in value:
            got = _text(item)
            if got:
                return got
    if isinstance(value, dict):
        for key in ("name", "@value", "legalName"):
            got = _text(value.get(key))
            if got:
                return got
    return None


def _types(node: dict[str, Any]) -> str:
    """The node's `@type`, flattened to one lowercase string for substring tests."""
    value = node.get("@type") or node.get("type") or ""
    if isinstance(value, list):
        value = " ".join(str(v) for v in value)
    return str(value).lower()


def _looks_like_a_business(node: dict[str, Any]) -> bool:
    lowered = {str(k).lower() for k in node}
    if not _text(node.get("name")) and not _text(node.get("legalName")):
        return False
    if any(key in lowered for key in _BUSINESS_KEYS):
        return True
    return any(marker in _types(node) for marker in _BUSINESS_TYPE_MARKERS)


def _addresses(node: dict[str, Any]) -> list[dict[str, Any]]:
    value = node.get("address")
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    return []


def organizations(html: str) -> list[dict[str, Any]]:
    """Every business the page describes in its structured data.

    Each entry holds only plain strings, so a caller never has to defend against the shapes JSON-LD
    allows:

        name, legal_name, postcodes (list), localities (list), street, telephone, email, vat_id

    An empty list means the page published nothing usable — which is the common case, and not an
    error.
    """
    nodes: list[dict[str, Any]] = []
    for block in _blocks(html):
        parsed = _parse(block)
        if parsed is not None:
            _walk(parsed, nodes)

    found: list[dict[str, Any]] = []
    for node in nodes:
        if not _looks_like_a_business(node):
            continue
        postcodes: list[str] = []
        localities: list[str] = []
        street = None
        for address in _addresses(node):
            code = _text(address.get("postalCode"))
            if code and code not in postcodes:
                postcodes.append(code)
            locality = _text(address.get("addressLocality"))
            if locality and locality not in localities:
                localities.append(locality)
            street = street or _text(address.get("streetAddress"))
        # An address given as a single string — "Musterstr. 1, 49074 Osnabrück" — carries the same
        # facts without the structure, so keep it as the street and let the postcode regex have it.
        plain_address = _text(node.get("address")) if not _addresses(node) else None

        found.append(
            {
                "name": _text(node.get("name")),
                "legal_name": _text(node.get("legalName")),
                "postcodes": postcodes,
                "localities": localities,
                "street": street or plain_address,
                "telephone": _text(node.get("telephone")),
                "email": _text(node.get("email")),
                "vat_id": _text(node.get("vatID")) or _text(node.get("taxID")),
            }
        )
    return found


def names(html: str) -> list[str]:
    """Every name and legal name the page's structured data claims, deduplicated."""
    out: list[str] = []
    for org in organizations(html):
        for value in (org.get("name"), org.get("legal_name")):
            if value and value not in out:
                out.append(value)
    return out


def postcodes(html: str) -> list[str]:
    """Every postcode the page's structured data claims, deduplicated."""
    out: list[str] = []
    for org in organizations(html):
        for value in org.get("postcodes") or []:
            if value and value not in out:
                out.append(value)
    return out
