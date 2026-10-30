"""Wikidata's Berlin organisations and the websites it lists for them (issue #26).

Wikidata is free (CC0) and needs no key. One SPARQL query returns every item whose headquarters
(P159) lies in Berlin (Q64, districts included through P131*) and which has an official website
(P856), with its German label and, where known, its German register number (P12012).

Measured on 2026-10-29: 4,174 items, 380 with a register number. The register number is mostly the
bare digits (`165662`), sometimes an association register entry (`VR 36562 B`) we do not have.

A listed website is a **proposal**. Wikidata itself warns that a URL may be dead or hijacked, so
every one goes through the same verification as a guessed domain (`pipeline.verify_proposed`).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

ENDPOINT = "https://query.wikidata.org/sparql"
#: Wikimedia asks every client to name itself and a contact.
USER_AGENT = ("unternehmensdatenbank/1.0 (Technologiestiftung Berlin; "
              "https://github.com/technologiestiftung/unternehmensdatenbank)")
QUERY = """
SELECT ?item ?label ?website ?register ?article ?description WHERE {
  ?item wdt:P159 ?hq . ?hq wdt:P131* wd:Q64 .
  ?item wdt:P856 ?website .
  OPTIONAL { ?item wdt:P12012 ?register }
  OPTIONAL { ?item rdfs:label ?label FILTER(lang(?label) = "de") }
  OPTIONAL { ?article schema:about ?item ; schema:isPartOf <https://de.wikipedia.org/> }
  OPTIONAL { ?item schema:description ?description FILTER(lang(?description) = "de") }
}
"""
_DIGITS = re.compile(r"\d+")


@dataclass(frozen=True)
class Item:
    qid: str
    label: str | None
    websites: tuple[str, ...]
    #: Only the digits of a commercial register number (HRA/HRB or bare). An association (`VR`) or
    #: cooperative register entry is None: our register data has no such numbers to match.
    register_digits: str | None
    #: The German Wikipedia article, and Wikidata's German one-line description: texts for the
    #: company corpus (#61). Wikipedia is CC BY-SA, Wikidata CC0.
    wikipedia_url: str | None = None
    description: str | None = None


def register_digits(value: str | None) -> str | None:
    """`'165662'` and `'HRB 165662 B'` -> `'165662'`; `'VR 36562 B'` -> None."""
    if not value or re.match(r"\s*(VR|GnR|PR)\b", value, re.IGNORECASE):
        return None
    found = _DIGITS.search(value)
    return found.group(0) if found else None


def parse(bindings: Iterable[dict[str, Any]]) -> list[Item]:
    """SPARQL result rows -> one item per Wikidata id. An item with two websites is two rows."""
    grouped: dict[str, dict[str, Any]] = {}
    for row in bindings:
        qid = row["item"]["value"].rsplit("/", 1)[-1]
        item = grouped.setdefault(qid, {"label": None, "websites": set(), "register": None,
                                         "article": None, "description": None})
        item["websites"].add(row["website"]["value"])
        item["article"] = item["article"] or (row.get("article") or {}).get("value")
        item["description"] = item["description"] or (row.get("description") or {}).get("value")
        item["label"] = item["label"] or (row.get("label") or {}).get("value")
        item["register"] = item["register"] or register_digits((row.get("register") or {}).get("value"))
    return [Item(qid, g["label"], tuple(sorted(g["websites"])), g["register"], g["article"], g["description"])
            for qid, g in sorted(grouped.items())]


def fetch() -> list[Item]:
    """One query to Wikidata's public endpoint. Raises on an HTTP error."""
    from enrichment.http_client import session

    response = session().get(ENDPOINT, params={"query": QUERY}, timeout=120,
                             headers={"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT})
    response.raise_for_status()
    return parse(response.json()["results"]["bindings"])


def wikipedia_text(article_url: str) -> str | None:
    """The plain text of a German Wikipedia article (CC BY-SA), or None. One API call, no HTML."""
    from urllib.parse import unquote, urlparse

    from enrichment.http_client import session

    title = unquote(urlparse(article_url).path.rsplit("/", 1)[-1])
    response = session().get(
        "https://de.wikipedia.org/w/api.php",
        params={"action": "query", "prop": "extracts", "explaintext": 1, "format": "json",
                "formatversion": 2, "redirects": 1, "titles": title},
        headers={"User-Agent": USER_AGENT}, timeout=60,
    )
    response.raise_for_status()
    pages = response.json().get("query", {}).get("pages", [])
    return (pages[0].get("extract") or None) if pages else None
