#!/usr/bin/env python3
"""Regression check for `search_common.is_own_site_candidate`.

Every case below is a real URL a search provider returned for a real company in this database,
with the expected verdict read off the site by hand. They exist because each false positive here
reached `company_field_observations` as a website candidate at high confidence and would have
become the canonical website — a directory host blocklist alone did not stop them.

Run against the host checkout:

    python scripts/check_own_site_filter.py

or inside a container, where the DAGs live at /opt/airflow/dags:

    docker compose exec airflow-scheduler python /opt/airflow/dags/../scripts/check_own_site_filter.py

Exit code is non-zero on any mismatch, so it works as a pre-commit gate.
"""
from __future__ import annotations

import sys
from pathlib import Path

for candidate in (
    Path(__file__).resolve().parent.parent / "airflow" / "dags",
    Path("/opt/airflow/dags"),
):
    if candidate.is_dir():
        sys.path.insert(0, str(candidate))
        break

from search_common import is_own_site_candidate  # noqa: E402

AUTO_ACCEPT_SCORE = 100

#: (url, company_name, score, expected_is_own_site)
CASES: list[tuple[str, str, int, bool]] = [
    # --- third-party pages that a register match made look verified -----------------------
    # A registry, credit-bureau or insolvency page quotes the real HRB/HRA, so
    # scorer.score_candidate gives it +100 → confidence 1.0. That proves the page is ABOUT the
    # company, never that it IS the company's site.
    ("https://www.creditsafe.com/business-index/de-de/company/wohnplan-x-de30878844",
     "Wohnplan Wohnbesitz- und Vermietungs GmbH & Co. KG", 100, False),
    ("https://www.creditsafe.com/business-index/nl-nl/company/zender-germany-gmbh-de31254417",
     "Zender Germany GmbH", 100, False),
    ("https://insolvenz-radar.de/firmeninsolvenz/hrb+217094/osnabrueck/p3313/",
     "Zender Medical GmbH", 100, False),
    ("https://www.insolvenzindex.de/insolvenz/zender-medical-gmbh-osnabruck-hrb-217094",
     "Zender Medical GmbH", 100, False),
    ("https://online-handelsregister.de/handelsregisterauszug/ni/Osnabrueck/HRB/217094",
     "yt energie GmbH", 100, False),
    # --- wrong company entirely, matched on a postcode ------------------------------------
    # "Ö. H. O. GmbH" yields no name token longer than 3 characters, so name-based matching
    # cannot help; only the postcode matched, on a heating-oil price page listing every city.
    ("https://www.heizoel24.de/heizoelpreise/niedersachsen/osnabrueck-49086",
     "Oe. H. O. GmbH", 20, False),
    ("https://www.thecrag.com/de/klettern/germany/teutoburger-wald/area/1606417827",
     "Zenit Klettern GmbH", 0, False),
    # --- directory listings whose PATH carries the name, host does not --------------------
    ("https://www.wlw.de/de/firma/zender-germany-gmbh-1727703", "Zender Germany GmbH", 20, False),
    ("https://fahrradreparatur.org/osnabrueck/zweirad-liebig-gmbh",
     "Zweirad Liebig Verwaltungs GmbH", 20, False),
    # Hungarian company-information site. Neither the host blocklist nor the listing-path
    # markers caught this one — "/cr3452210419" looks like nothing in particular — which is what
    # settled the question of whether a register match may substitute for a host match.
    ("https://www.ceginformacio.hu/cr3452210419",
     "WSM - Wach- und Sicherheitsdienst Muensterland GmbH", 120, False),
    # --- genuine company sites that must keep passing -------------------------------------
    ("https://zaunkontor.eu/impressum/", "Zaunkontor GmbH", 120, True),
    ("https://www.zweirad-liebig.de/about", "Zweirad Liebig GmbH & Co. KG", 120, True),
    ("https://www.zecharchitekten.de/impressum", "ZECH ARCHITEKTEN GmbH", 120, True),
    ("https://www.zenit-klettern.de/", "Zenit Klettern GmbH", 20, True),
    ("https://www.xl-media-capital.de/impressum/", "xl-media-capital GmbH", 120, True),
    # Single distinctive token plus a suffix in the domain: "Bar" is too short to be a token, so
    # only substring containment can accept this.
    ("https://zebrabar.de", "Zebra Bar UG", 120, True),
    ("https://www.zeitgeist-shop.com", "Zeitgeist GmbH", 20, True),
    # Own-site paths that resemble directory paths. Exa returned all of these for Zaunkontor
    # GmbH; an earlier version tested the path before the host and would have refused them.
    ("https://zaunkontor.eu/unternehmen/", "Zaunkontor GmbH", 120, True),
    ("https://zaunkontor.eu/kontakt/", "Zaunkontor GmbH", 20, True),
    ("https://zaunkontor.eu/firmen/", "Zaunkontor GmbH", 20, True),
    # --- accepted known miss ---------------------------------------------------------------
    # zffcn.com IS ZF Foxconn Chassis Modules GmbH's site, but the host shares no token with the
    # name, so it is refused. Recording no website beats recording a registry page: the escape
    # hatch that would have admitted it returned 6 false positives and 0 true positives.
    ("https://zffcn.com", "ZF Foxconn Chassis Modules GmbH", 120, False),
]


def main() -> int:
    failures = 0
    for url, name, score, want in CASES:
        got = is_own_site_candidate(url, name, score, AUTO_ACCEPT_SCORE)
        if got != want:
            failures += 1
        marker = "ok  " if got == want else "FAIL"
        print(f"{marker} own_site={str(got):5} want={str(want):5} {name[:38]:38} {url[:58]}")

    print(f"\n{len(CASES) - failures}/{len(CASES)} as intended")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
