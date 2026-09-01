#!/usr/bin/env python3
"""Regression check for `url_discovery.imprint_extractor.extract_ceo_names`.

An Impressum is the legal contact page every German business website must have. It names the
managing director. Reading that name off the page is the only way this pipeline gets an owner it
can actually stand behind — everything else is a language model's reading of a search snippet,
which nothing verified.

It used to fail on two thirds of the pages it was given. Measured over the Impressum pages of 31
companies whose website had already been verified: 26 gave up a phone number or an email
address, but only 7 gave up a name. The page was fetched and parsed either way, so the search
engine was never the problem.

Two causes, one case below for each:

  1. `page_text` joined every HTML element with a space, so no line breaks survived. `CEO_RE`
     stops its capture at a line break, so with none left it swallowed 70 characters of whatever
     followed the label, and the name filter then rejected the whole run for having too many
     words. It also swallowed the NEXT label, so re.finditer never offered it.
  2. The filter required the entire captured run to be a name. Real Impressum lines continue
     past the name ("Erika Mustermann Diplom-Kauffrau eingetragen beim Amtsgericht"), so a name
     that was plainly there got thrown away.

Every layout below was taken from a real page in the measured sample. The names are invented —
a real managing director's name is personal data and does not belong in a git repository. The
layouts are what matters, and they are reproduced exactly.

After the fix the same 31 pages yield a name on 19. The remaining 12: four have no manager label
anywhere, and the rest name no person at all.

Run:

    python scripts/check_imprint_owner_extraction.py

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

from url_discovery.imprint_extractor import extract_from_html  # noqa: E402

#: (name of the case, Impressum HTML, names that must come out, in order)
CASES: list[tuple[str, str, list[str]]] = [
    # --- the two bugs this check exists for ------------------------------------------------
    (
        "label and name in separate elements",
        "<p>Geschäftsführer:</p><p>Anna Beispiel</p>",
        ["Anna Beispiel"],
    ),
    (
        "a second label inside the first label's old 70-character window",
        "<p>Vertreten durch</p><p>Geschäftsführer: Anna Beispiel</p>",
        ["Anna Beispiel"],
    ),
    (
        "line continues past the name",
        "<p>Geschäftsführerin: Erika Beispiel Diplom-Kauffrau eingetragen beim "
        "Amtsgericht Osnabrück HRB 12345</p>",
        ["Erika Beispiel"],
    ),
    (
        "a clause between the label and the name",
        "<p>Vertreten durch den Geschäftsführer</p><p>Herr Bernd Beispiel</p>",
        ["Bernd Beispiel"],
    ),
    # --- shapes that must keep working -----------------------------------------------------
    ("two managers joined by und", "<p>Geschäftsführer: Anna Beispiel und Bernd Muster</p>",
     ["Anna Beispiel", "Bernd Muster"]),
    ("two managers separated by a comma", "<p>Vertreten durch: Anna Beispiel, Bernd Muster</p>",
     ["Anna Beispiel", "Bernd Muster"]),
    ("honorific dropped", "<p>Inhaber: Herr Bernd Beispiel</p>", ["Bernd Beispiel"]),
    ("particle inside the name", "<p>Geschäftsführer: Maximilian von Beispiel</p>",
     ["Maximilian von Beispiel"]),
    ("surname written in capitals", "<p>Geschäftsführer: Anna BEISPIEL</p>", ["Anna BEISPIEL"]),
    ("academic title kept", "<p>Vorstand: Dr. Anna Beispiel</p>", ["Dr. Anna Beispiel"]),
    ("english label", "<p>Managing Director: Anna Beispiel</p>", ["Anna Beispiel"]),
    ("br tags instead of block elements", "Geschäftsführer:<br>Anna Beispiel<br>Musterweg 1",
     ["Anna Beispiel"]),

    # --- must NOT produce a name -----------------------------------------------------------
    # A company standing in for a person. Caught on the hyphen part, not the whole token:
    # "Beteiligungs-GmbH" is not equal to "GmbH".
    ("company as the representative",
     "<p>Vertreten durch: Musterbau Beteiligungs-GmbH</p>", []),
    # Leading all-caps is an acronym or a brand, never a first name.
    ("leading acronym", "<p>Geschäftsführer: ABC Musterverwaltungsgesellschaft</p>", []),
    # The address block sits directly under the label on many pages.
    ("address line under the label",
     "<p>Vertreten durch:</p><p>Musterweg 12</p><p>49080 Osnabrück</p>", []),
    ("label with nothing after it", "<p>Geschäftsführer:</p><p>49080 Osnabrück</p>", []),
    # One word cannot be told apart from a job title.
    ("single word after the label", "<p>Inhaber: Beispiel</p>", []),
    ("no label at all", "<p>Musterfirma GmbH</p><p>Musterweg 12</p>", []),
    # The address must not be read as a second manager once a name was already found.
    ("name found, address ignored",
     "<p>Geschäftsführer: Anna Beispiel</p><p>Musterweg 12, 49080 Osnabrück</p>",
     ["Anna Beispiel"]),
]


#: Finding the Impressum page at all, which has to happen before any of the layouts above matter.
#:
#: `base_url` is often a DEEP page: `search_discovery_runner` hands over whatever the search engine
#: ranked. Resolving a root-relative link by concatenation then produced
#: `/planen/service/kontakt/service/impressum`, a 404, and `fetch_impressum` settled for the
#: contact page instead. The register number was never read, so the candidate scored 20 rather than
#: 120 and the website was stored at 0.65 instead of 1.0. Found on Tourismusgesellschaft
#: Osnabrücker Land, 2026-09-01, whose register number sits at /service/impressum.
#:
#: (label, base_url, href in the page, expected resolved URL)
LINK_CASES = [
    (
        "root-relative link from a deep page resolves against the SITE",
        "https://www.osnabruecker-land.de/planen/service/kontakt",
        "/service/impressum",
        "https://www.osnabruecker-land.de/service/impressum",
    ),
    (
        "root-relative link from the origin still works",
        "https://www.osnabruecker-land.de",
        "/service/impressum",
        "https://www.osnabruecker-land.de/service/impressum",
    ),
    (
        "document-relative link resolves against the deep page's DIRECTORY",
        "https://example.de/ueber/kontakt.html",
        "impressum.html",
        "https://example.de/ueber/impressum.html",
    ),
    (
        "an absolute link is left alone",
        "https://example.de/a/b",
        "https://cdn.example.de/impressum",
        "https://cdn.example.de/impressum",
    ),
    (
        "a protocol-relative link keeps the page's scheme",
        "https://example.de/a/b",
        "//example.de/impressum",
        "https://example.de/impressum",
    ),
]


def _check_link_resolution() -> int:
    """Free: no network. The parser falls back to html.parser where lxml is not installed."""
    from url_discovery.http_client import _find_impressum_link

    failures = 0
    for label, base_url, href, expected in LINK_CASES:
        html = f'<html><body><a href="{href}">Impressum</a></body></html>'
        actual = _find_impressum_link(base_url, html)
        if actual != expected:
            failures += 1
            print(f"FAIL  {label}")
            print(f"      base     {base_url}")
            print(f"      expected {expected}")
            print(f"      got      {actual}")
    return failures


def main() -> int:
    failures = 0
    for label, html, expected in CASES:
        actual = extract_from_html(html)["ceo_names"]
        if actual != expected:
            failures += 1
            print(f"FAIL  {label}")
            print(f"      expected {expected}")
            print(f"      got      {actual}")
    failures += _check_link_resolution()

    total = len(CASES) + len(LINK_CASES)
    print(f"\n{total - failures}/{total} cases pass")
    if failures:
        print(
            "\nA failure here is not automatically a bug in the code — a new layout may simply "
            "need a new case. But check first that the change has not started inventing names: "
            "a wrong owner is worse than no owner, because nothing downstream can tell."
        )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
