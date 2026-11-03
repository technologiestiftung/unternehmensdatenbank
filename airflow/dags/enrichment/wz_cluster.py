"""A company's official industry cluster, straight from its WZ code. No judgement anywhere.

**WZ** is the *Klassifikation der Wirtschaftszweige*, the official German industry classification.
The `wz_code` register field held one for 8,483 of 14,443 companies, courtesy of the Northdata export.
A code names an industry: `56.11.1` is "Restaurants mit Bedienung", `64.2` is
"Beteiligungsgesellschaften".

The classification is a tree, and this module reads the top two levels of it:

    Abschnitt (section)    a letter    I    GASTGEWERBE    22 of them
    Abteilung (division)   two digits  56   Gastronomie    87

A company's **cluster** is those two. Both are derived, not decided:

- the division is the **first two characters** of whatever code the company carries, so `56.11.1`,
  `56.1` and `56` all land in `56`;
- the section is the letter that owns that division, looked up in `wz_divisions` (Postgres, #84).

That is the whole module. There is no mapping to write, no threshold to tune and nothing to label,
because nobody is being asked for an opinion.

## This is NOT the Technologiefeld, and the two must never be mixed

`technology_fields` holds Berlin's 77 **Technologie- und Anwendungsfelder** — what field a company
*works in*. A WZ code says what it is *registered as*. Neither can be derived from the other.

An earlier version of this code mapped WZ codes onto Berlin's fields by hand: `56.11.1 → Gastgewerbe`,
`43.22.0 → Handwerk`. It filled 2,632 companies' Technologiefeld with values that nothing the company
published supported, and it threw away 22 sections and 87 divisions of perfectly good official
classification to do it. Both were losses.

So: `branch_classifier.classify()` takes **no WZ argument at all**, and this module is never imported
by it. The two answers live in two columns, and `scripts/check_branch_classifier.py` fails if that
stops being true. It is the same rule the website side already lives by — a register number on a page
proves the page is *about* a company, never that it *is* the company's site.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

#: How many leading characters of a code make its division. Two, always: WZ divisions are
#: two-digit, and every deeper level is written as an extension of them.
DIVISION_LENGTH = 2


@dataclass(frozen=True)
class WzCluster:
    """Where one company sits in the official classification."""

    #: The code the company actually carries, e.g. "56.11.1".
    code: str
    #: Two digits, e.g. "56".
    division: str
    #: The official name of the division, e.g. "Gastronomie". Empty when `wz_divisions` does not
    #: know the division — see `cluster_for`.
    division_title: str
    #: A letter, e.g. "I". Empty when the division is unknown.
    section: str
    #: e.g. "GASTGEWERBE". Empty when the division is unknown.
    section_title: str


@dataclass
class Clusters:
    """The two lookups this module needs, from `wz_divisions`."""

    #: "56" -> "Gastronomie"
    division_titles: dict[str, str]
    #: "56" -> ("I", "GASTGEWERBE")
    division_sections: dict[str, tuple[str, str]]


def clusters_from_rows(rows: Iterable[tuple[str, str, str, str]]) -> Clusters:
    """(division, title, section, section_title) rows, as `wz_divisions` holds them."""
    rows = list(rows)
    return Clusters(
        division_titles={division: title for division, title, _, _ in rows},
        division_sections={division: (section, section_title) for division, _, section, section_title in rows},
    )


@lru_cache(maxsize=1)
def load_clusters() -> Clusters:
    """Read `wz_divisions`. Cached, because it is read once per company otherwise."""
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute("SELECT division, title, section, section_title FROM public.wz_divisions")
            return clusters_from_rows(cur.fetchall())
    finally:
        connection.close()


def cluster_for(wz_code: str | None, *, clusters: Clusters | None = None) -> WzCluster | None:
    """The company's cluster, or None when it has no usable code.

    None means "this company has no WZ code", which is a different statement from "its code is not in
    our copy of the classification". The second case comes back as a `WzCluster` with the division
    filled in and the titles empty — we know where it sits, we just cannot name the place. Collapsing
    the two would hide a stale `wz_divisions` behind what looks like missing source data.
    """
    code = (wz_code or "").strip()
    if len(code) < DIVISION_LENGTH:
        return None
    division = code[:DIVISION_LENGTH]
    if not division.isdigit():
        return None

    clusters = clusters or load_clusters()
    section, section_title = clusters.division_sections.get(division, ("", ""))
    return WzCluster(
        code=code,
        division=division,
        division_title=clusters.division_titles.get(division, ""),
        section=section,
        section_title=section_title,
    )
