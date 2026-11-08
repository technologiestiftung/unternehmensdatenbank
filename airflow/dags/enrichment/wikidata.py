"""Wikidata's Berlin organisations and the websites it lists for them (issue #26).

Wikidata is free (CC0) and needs no key. One SPARQL query returns every item whose headquarters
(P159) lies in Berlin (Q64, districts included through P131*) and which has an official website
(P856), with its German label and, where known, its German register number (P12012).

Measured on 2026-10-29: 4,174 items, 380 with a register number.

A second query (`FIGURES_QUERY`, #138/#139) reads the same items' number of employees and revenue.
Measured on 2026-11-08: 353 items have an employee count, 327 a revenue (all but 3 in euros); 820 of
899 and 1,041 of 1,048 statements carry the date they are for. Of the items the local Berlin database
matches, 66 have an employee count and 34 a revenue. Read by hand: mostly the group's figure, not the
registered entity's alone (Zalando SE, HELIOS Kliniken GmbH), and at least one plainly wrong value
(a small Kreuzberg UG with 98 million euros revenue). So they get an unmeasured, middling confidence. The register number is mostly the
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


# --- employees and revenue (#138, #139) -------------------------------------------------------

#: The same Berlin items, their number of employees (P1128) and total revenue (P2139), with the date
#: each figure is for (P585); best-ranked statements only (preferred, else normal; never deprecated).
#: A separate query: in the main one each figure would multiply the rows.
FIGURES_QUERY = """
SELECT ?item ?prop ?amount ?unit ?date WHERE {
  ?item wdt:P159 ?hq . ?hq wdt:P131* wd:Q64 . ?item wdt:P856 ?website .
  { ?item p:P1128 ?st . ?st a wikibase:BestRank ; psv:P1128 ?v . BIND("P1128" AS ?prop) }
  UNION
  { ?item p:P2139 ?st . ?st a wikibase:BestRank ; psv:P2139 ?v . BIND("P2139" AS ?prop) }
  ?v wikibase:quantityAmount ?amount .
  OPTIONAL { ?v wikibase:quantityUnit ?unit }
  OPTIONAL { ?st pq:P585 ?date }
}
"""
#: Wikidata property -> our observation field.
FIGURE_FIELDS = {"P1128": "employee_count", "P2139": "revenue"}
#: Revenue is kept in euros only: converting another currency would need a rate for the figure's year.
EURO = "http://www.wikidata.org/entity/Q4916"


@dataclass(frozen=True)
class Figure:
    field: str
    amount: float
    #: The date the figure is for ("2023-12-31"), None when Wikidata gives none.
    as_of: str | None


def parse_figures(bindings: Iterable[dict[str, Any]]) -> dict[str, dict[str, Figure]]:
    """SPARQL rows -> per Wikidata id, the newest figure per field. A dated figure beats an undated
    one, then the larger (deterministic); revenue not in euros and amounts that are not positive are left out."""
    newest: dict[str, dict[str, Figure]] = {}
    for row in bindings:
        field = FIGURE_FIELDS.get(row["prop"]["value"])
        amount = float(row["amount"]["value"])
        if field is None or amount <= 0:
            continue
        if field == "revenue" and (row.get("unit") or {}).get("value") != EURO:
            continue
        date = (row.get("date") or {}).get("value", "")[:10]
        figure = Figure(field, amount, date if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date) else None)
        qid = row["item"]["value"].rsplit("/", 1)[-1]
        old = newest.setdefault(qid, {}).get(field)
        if old is None or (figure.as_of or "", figure.amount) > (old.as_of or "", old.amount):
            newest[qid][field] = figure
    return newest


def fetch_figures() -> dict[str, dict[str, Figure]]:
    """One query to Wikidata's public endpoint. Raises on an HTTP error."""
    from enrichment.http_client import session

    response = session().get(ENDPOINT, params={"query": FIGURES_QUERY}, timeout=120,
                             headers={"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT})
    response.raise_for_status()
    return parse_figures(response.json()["results"]["bindings"])
