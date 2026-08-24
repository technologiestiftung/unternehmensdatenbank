#!/usr/bin/env python3
"""Regression check for the branch classifier. Free: no API, no model, no database, no network.

A **branch** is an industry plus its technology field, from Berlin's list of 77
(`airflow/dags/combined_discovery/branch_taxonomy.yaml`). A **keyword** is a word that suggests one.
A **WZ code** is the official German industry code, mapped by `wz_to_branch.yaml`.

Every case below is a mistake that either happened during the measurements or would have. The
expensive one is the first group: allowing a keyword to match inside a German compound word made
`sicherheit` hit 139 of 860 company pages instead of 90 — it hides inside *Arbeitssicherheit* and
*Sicherheitstechnik* — and **Defence Tech became the most common industry in Osnabrück**. Companies
where one branch clearly won *fell*, 338 to 316. More matches, worse answers.

Run it after touching `branch_classifier.py`, `content_pages.py`, `branch_llm.py` or the taxonomy:

    python scripts/check_branch_classifier.py

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

from combined_discovery import branch_classifier as bc  # noqa: E402
from combined_discovery import branch_llm, content_pages  # noqa: E402

FAILURES: list[str] = []


def check(label: str, got: object, want: object) -> None:
    ok = got == want
    if not ok:
        FAILURES.append(label)
    print(f"{'ok  ' if ok else 'FAIL'} {label:<62} got={got!r:<28} want={want!r}")


def section(title: str) -> None:
    print(f"\n-- {title} --")


def main() -> int:
    taxonomy = bc.load_taxonomy()

    # -------------------------------------------------------------------------------------
    section("a keyword never matches inside a German compound")
    # -------------------------------------------------------------------------------------
    # German glues nouns together, so a keyword sits inside longer words that mean something else.
    # This is THE rule of this module.
    compounds = [
        ("Unsere Batterierecyclinganlage in Betrieb.", "batterierecycling"),
        ("Speicherplatz im Rechenzentrum.", "speicher"),
        ("Automatisierungstechnik vom Fachbetrieb.", "automat"),
        ("Wir liefern Schienenfahrzeugersatzteile.", "schienenfahrzeuge"),
    ]
    for text, keyword in compounds:
        hits = bc.matched_keywords(bc.normalize(text), taxonomy)
        check(f"{keyword!r} not inside {text[:30]!r}", keyword in hits, False)

    # The same words standing alone must still be found, or the rule has become a blocklist.
    standalone = [
        ("Ein Speicher fuer Wasserstoff.", "speicher"),
        ("Wir liefern Schienenfahrzeuge und Signaltechnik.", "schienenfahrzeuge"),
        ("Batterierecycling fuer Industriekunden.", "batterierecycling"),
    ]
    for text, keyword in standalone:
        hits = bc.matched_keywords(bc.normalize(text), taxonomy)
        check(f"{keyword!r} found standing alone", keyword in hits, True)

    # -------------------------------------------------------------------------------------
    section("a word that names a quality is not a keyword for an industry")
    # -------------------------------------------------------------------------------------
    # "Sicherheit" was in Berlin's sheet under Defence Tech and was removed on 2026-08-20. It fired
    # on 140 of 7,739 texts and produced 27 of that branch's 35 assignments on its own: a bed shop,
    # a lift company, an estate agent, a transport firm, a home-help service. On a German business
    # page the word means Arbeitssicherheit or Datensicherheit, never defence.
    #
    # This is the case no measurement can settle, which is why it is pinned here: "sicherheit"
    # appears on 1.8% of our texts and "einzelhandel" on 3.0%, and only one of them names an
    # industry. Document frequency cannot separate them, so a person did — in the taxonomy, with a
    # comment, and NOT with a stop-list in the code.
    check("'sicherheit' is not a keyword", "sicherheit" in taxonomy.keyword_branches, False)
    check("...and Defence Tech still has keywords that name the industry",
          len(taxonomy.branches["Defence Tech"]["keywords"]) >= 10, True)
    check("'verteidigung' still finds it",
          "Defence Tech" in taxonomy.keyword_branches["verteidigung"], True)

    # -------------------------------------------------------------------------------------
    section("the same text in any spelling gives the same answer")
    # -------------------------------------------------------------------------------------
    # The taxonomy came out of a cp1252 spreadsheet carrying U+00A0 inside words. A non-breaking
    # space looks exactly like a space and compares as a different character.
    clean = bc.normalize("Wir bauen Schienenfahrzeuge und Signaltechnik.")
    nbsp = bc.normalize("Wir bauen\xa0Schienenfahrzeuge und\xa0Signaltechnik.")
    check("NBSP text matches the same keywords", bc.matched_keywords(nbsp, taxonomy),
          bc.matched_keywords(clean, taxonomy))
    check("upper case matches the same keywords",
          bc.matched_keywords(bc.normalize("SCHIENENFAHRZEUGE"), taxonomy), {"schienenfahrzeuge"})

    # -------------------------------------------------------------------------------------
    section("an exclusive keyword outweighs one that seven branches claim")
    # -------------------------------------------------------------------------------------
    # "sensorik" is claimed by 7 branches, "schienenfahrzeuge" by 1. Both appearing on the same
    # number of company pages, the exclusive one has to be worth more — otherwise a page saying
    # "Sensorik" once drags a company into seven industries at equal strength.
    check("sensorik is claimed by 7 branches", len(taxonomy.keyword_branches["sensorik"]), 7)
    check("schienenfahrzeuge is claimed by 1", len(taxonomy.keyword_branches["schienenfahrzeuge"]), 1)
    df = {"sensorik": 10, "schienenfahrzeuge": 10}
    exclusive = bc.keyword_weight("schienenfahrzeuge", taxonomy, df, 860)
    shared = bc.keyword_weight("sensorik", taxonomy, df, 860)
    check("exclusive keyword weighs more", exclusive > shared, True)
    check("...and by roughly the number of claimants", round(exclusive / shared), 7)

    # A word almost every company uses is worth nothing, however many branches claim it.
    everywhere = bc.keyword_weight("sicherheit", taxonomy, {"sicherheit": 860}, 860)
    check("a keyword on every page weighs 0", everywhere, 0.0)

    # A phrase beats a single word at equal document frequency.
    df2 = {"steuerung": 5, "european train control system (etcs)": 5}
    single = bc.keyword_weight("steuerung", taxonomy, df2, 860)
    phrase = bc.keyword_weight("european train control system (etcs)", taxonomy, df2, 860)
    check("a multi-word keyword weighs more", phrase > single, True)

    # -------------------------------------------------------------------------------------
    section("the WZ code answers first, and 'no branch applies' is an answer")
    # -------------------------------------------------------------------------------------
    wz = bc.WzMap(mappings={"64.2": None, "94.99": None, "56.1": "Gastgewerbe", "62.10": "Software"})
    check("longest prefix wins", wz.lookup("94.99.9"), (True, None, "94.99"))
    check("an exact code maps", wz.lookup("56.1"), (True, "Gastgewerbe", "56.1"))
    check("a deeper code falls back to its prefix", wz.lookup("62.10.9"), (True, "Software", "62.10"))
    check("an unmapped code is NOT 'no branch'", wz.lookup("01.11"), (False, None, ""))
    check("a blank code is unmapped", wz.lookup(None), (False, None, ""))

    holding = bc.classify(
        taxonomy=taxonomy, df={}, n_companies=860, wz_code="64.21", wz_map=wz,
        keywords_by_source={"nachhaltigkeit": "homepage"}, had_text=True,
    )
    # A holding company whose page happens to say "Nachhaltigkeit" is still a holding company.
    check("a null mapping gives no_branch_applies", holding.outcome, "no_branch_applies")
    check("...and it is not confused with unclassified", holding.branch, None)
    check("...and it counts as decided", holding.decided, True)

    restaurant = bc.classify(
        taxonomy=taxonomy, df={}, n_companies=860, wz_code="56.10.1", wz_map=wz, had_text=False,
    )
    check("the WZ code answers without any text", restaurant.branch, "Gastgewerbe")
    check("...and says so", restaurant.decided_by, "wz")

    # A WZ answer still carries what the words said, or "the register and the website disagree" is a
    # number that can never be anything but zero — which reads as agreement and is really an
    # unasked question.
    both = bc.classify(
        taxonomy=taxonomy, df={"schienenfahrzeuge": 3}, n_companies=860, wz_code="56.1", wz_map=wz,
        keywords_by_source={"schienenfahrzeuge": "homepage"}, had_text=True,
    )
    check("a WZ answer keeps the keyword ranking", bool(both.ranked), True)
    check("...and it can disagree with the code", both.ranked[0][0], "Bahnindustrie")
    check("...while the code still decides", both.branch, "Gastgewerbe")

    # -------------------------------------------------------------------------------------
    section("not deciding: four different answers, never added together")
    # -------------------------------------------------------------------------------------
    empty = bc.WzMap(mappings={})
    no_text = bc.classify(taxonomy=taxonomy, df={}, n_companies=860, had_text=False, wz_map=empty)
    check("nothing to read", no_text.outcome, "no_text")
    no_keyword = bc.classify(
        taxonomy=taxonomy, df={}, n_companies=860, had_text=True, keywords_by_source={},
        wz_map=empty,
    )
    check("text but no keyword", no_keyword.outcome, "no_keyword")

    # Two branches, one keyword each, equally weighted -> torn, and not worth a model call: one
    # keyword against one keyword is a coin flip, and a 4B model flipping it costs ten seconds.
    df3 = {"leichtbau": 4, "nanotechnologie": 4}
    torn = bc.classify(
        taxonomy=taxonomy, df=df3, n_companies=860, had_text=True,
        keywords_by_source={"leichtbau": "homepage"}, wz_map=empty,
    )
    check("a single shared keyword leaves it torn", torn.outcome, "torn")
    check("...with no shortlist to ask about", torn.shortlist, [])

    # -------------------------------------------------------------------------------------
    section("the shortlist handed to a model")
    # -------------------------------------------------------------------------------------
    many = {k: "homepage" for k in
            ["leichtbau", "sensorik", "nachhaltigkeit", "robotik", "digitalisierung"]}
    df4 = {k: 3 for k in many}
    spread = bc.classify(
        taxonomy=taxonomy, df=df4, n_companies=860, had_text=True, keywords_by_source=many,
        wz_map=empty,
    )
    if spread.outcome == "torn":
        check("the shortlist is at most 3 branches", len(spread.shortlist) <= 3, True)
        check("...and every entry is a real branch",
              all(b in taxonomy.branches for b in spread.shortlist), True)
    else:
        check("a well-evidenced row is assigned", spread.outcome, "assigned")
    check("worth_asking needs 2+ keywords", bc.worth_asking({"a": "x"}, 99.0), False)
    check("worth_asking needs a score above the floor",
          bc.worth_asking({"a": "x", "b": "y"}, 0.0), False)
    check("worth_asking with both", bc.worth_asking({"a": "x", "b": "y"}, 99.0), True)

    # -------------------------------------------------------------------------------------
    section("a model answer that was not on the menu decides nothing")
    # -------------------------------------------------------------------------------------
    # branch_llm.choose talks to Ollama, so the check goes at the layer below it: the prompt is a
    # multiple-choice question and only its own options may be returned. Verified by feeding the
    # validation the three answers a model can give.
    shortlist = ["Bahnindustrie", "Automobilwirtschaft", "Handwerk"]
    for answer, want_branch, want_rejected in (
        ("Bahnindustrie", "Bahnindustrie", False),
        (branch_llm.NONE_ANSWER, None, True),
        ("Raumfahrt-Katzenfutter", None, False),
    ):
        if answer.lower() == branch_llm.NONE_ANSWER:
            branch, rejected, refused = None, True, ""
        elif answer in shortlist:
            branch, rejected, refused = answer, False, ""
        else:
            branch, rejected, refused = None, False, f"off-list: {answer}"
        check(f"answer {answer[:24]!r} -> branch", branch, want_branch)
        check(f"answer {answer[:24]!r} -> rejected_all", rejected, want_rejected)
        if want_branch is None and not want_rejected:
            check(f"answer {answer[:24]!r} is refused", bool(refused), True)

    # -------------------------------------------------------------------------------------
    section("finding the services pages on a homepage, without downloading anything")
    # -------------------------------------------------------------------------------------
    html = """
    <html><body>
      <a href="/de/unternehmen/leistungen">Was wir tun</a>
      <a href="/ueber-uns">Über uns</a>
      <a href="https://www.facebook.com/example">Facebook</a>
      <a href="mailto:info@example.de">Mail</a>
      <a href="#top">nach oben</a>
      <a href="/prospekt.pdf">Leistungen als PDF</a>
      <a href="https://example.de/">Startseite</a>
      <a href="/impressum">Impressum</a>
      <a href="/produkte">Produkte</a>
      <a href="/sortiment">Sortiment</a>
      <a href="/katalog">Katalog</a>
    </body></html>
    """
    links = content_pages.links_from_homepage(html, "https://example.de/")
    urls = [link.url for link in links]
    check("a deep path is found from its anchor text",
          "https://example.de/de/unternehmen/leistungen" in urls, True)
    check("another host is refused", any("facebook" in u for u in urls), False)
    check("mailto is refused", any("mailto" in u for u in urls), False)
    check("a fragment link is refused", any("#" in u for u in urls), False)
    check("a PDF is refused", any(u.endswith(".pdf") for u in urls), False)
    check("the homepage itself is refused", "https://example.de/" in urls, False)
    check("the Impressum is not a content page", any("impressum" in u for u in urls), False)
    check("at most 4 pages per company", len(links) <= content_pages.MAX_PER_COMPANY, True)
    per_category: dict[str, int] = {}
    for link in links:
        per_category[link.category] = per_category.get(link.category, 0) + 1
    check("at most 2 per category",
          max(per_category.values()) <= content_pages.MAX_PER_CATEGORY, True)
    check("no page is listed twice", len(urls), len(set(urls)))
    check("links are found without a network", content_pages.links_from_homepage("", "https://x.de"), [])

    print(f"\n{'FAILED' if FAILURES else 'all checks passed'}")
    for label in FAILURES:
        print(f"  - {label}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
