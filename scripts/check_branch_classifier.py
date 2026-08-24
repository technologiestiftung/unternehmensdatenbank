#!/usr/bin/env python3
"""Regression check for the branch classifier. Free: no API, no model, no database, no network.

Two classifications live side by side and must never be mixed:

**Technologiefeld** — one of Berlin's 77 *Technologie- und Anwendungsfelder*
(`branch_taxonomy.yaml`), or NONE. Only text the company published may fill it. In this file the
word "branch" always means one of those 77 values.

**WZ cluster** — the official German industry classification the company is registered under
(`wz_titles.yaml`, read by `wz_cluster.py`). A different question by a different authority. It is
derived, never decided, so nothing about it needs labelling.

Every case below is a mistake that either happened during the measurements or would have. The
expensive one is the first group: allowing a keyword to match inside a German compound word made
`sicherheit` hit 139 of 860 company pages instead of 90 — it hides inside *Arbeitssicherheit* and
*Sicherheitstechnik* — and **Defence Tech became the most common industry in Osnabrück**. Companies
where one branch clearly won *fell*, 338 to 316. More matches, worse answers.

Where a rule lives in the code, this file **imports and calls it** rather than restating it. A check
that rebuilds the rule it is checking agrees with itself no matter what the real code does — which is
exactly what the model-answer section here used to do, and why `scripts/replay_judge_gate.py` imports
the website judge's gate instead of copying it.

Run it after touching `branch_classifier.py`, `content_pages.py`, `branch_llm.py` or the taxonomy:

    python scripts/check_branch_classifier.py

Exit code is non-zero on any mismatch, so it works as a pre-commit gate.
"""
from __future__ import annotations

import inspect
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
from combined_discovery import branch_llm, content_pages, wz_cluster  # noqa: E402

FAILURES: list[str] = []


def _load_runner():
    """`scripts/run_combined_discovery.py` as a module, without running its CLI.

    It is a script rather than a package, so it cannot simply be imported by name. Only pure
    functions are called from here — nothing that touches the network, the database or the page
    cache — so this stays as free as the rest of the file.
    """
    import importlib.util

    path = Path(__file__).resolve().parent / "run_combined_discovery.py"
    spec = importlib.util.spec_from_file_location("_runner_under_check", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    # Each pair is (a compound the keyword hides inside, the keyword standing on its own). Both
    # halves are needed: the first says the rule works, the second says it has not become a
    # blocklist. Every keyword used here is asserted to still BE a keyword first — without that,
    # deleting one from the taxonomy turns its compound test into a check that passes because
    # nothing can match at all. That happened: "speicher" was a Wasserstoff keyword until it was
    # deleted on 2026-08-24, and its compound case would have gone on printing `ok` for ever.
    pairs = [
        ("Unsere Batterierecyclinganlage in Betrieb.",
         "Batterierecycling fuer Industriekunden.", "batterierecycling"),
        ("Wir liefern Schienenfahrzeugersatzteile.",
         "Wir liefern Schienenfahrzeuge und Signaltechnik.", "schienenfahrzeuge"),
        ("Industrieanlagenbau seit 1960.",
         "Wir planen Industrieanlagen.", "industrieanlagen"),
        ("Holzbauweise mit kurzen Wegen.",
         "Holzbau vom Fachbetrieb.", "holzbau"),
    ]
    for inside, alone, keyword in pairs:
        check(f"{keyword!r} is still a keyword at all", keyword in taxonomy.keyword_branches, True)
        check(f"{keyword!r} not inside {inside[:26]!r}",
              keyword in bc.matched_keywords(bc.normalize(inside), taxonomy), False)
        check(f"{keyword!r} found standing alone",
              keyword in bc.matched_keywords(bc.normalize(alone), taxonomy), True)

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
    section("the WZ cluster is its own column and can never reach the Technologiefeld")
    # -------------------------------------------------------------------------------------
    # This is the invariant the whole split exists to protect. An earlier version mapped WZ codes
    # onto Berlin's fields by hand and filled 2,632 companies' Technologiefeld with values nothing
    # they published supported. The signature is now the guard: there is no parameter to pass a code
    # through, so the mistake cannot be made by accident.
    params = set(inspect.signature(bc.classify).parameters)
    check("classify() takes no WZ argument", params & {"wz_code", "wz_map", "wz_cluster"}, set())
    check("...and no WZ mapping loader survives", hasattr(bc, "load_wz_map"), False)
    check("...nor a WzMap type", hasattr(bc, "WzMap"), False)
    check("classify() knows nothing about wz_cluster", "wz_cluster" in dir(bc), False)

    # A division is the first two characters of whatever code the company carries. No prefix search,
    # no mapping, no judgement — which is why this column needs no labelling.
    for code, division in (("56", "56"), ("56.1", "56"), ("56.11", "56"), ("56.11.1", "56"),
                           ("94.99.9", "94"), ("70.10.4", "70")):
        got = wz_cluster.cluster_for(code)
        check(f"{code!r} sits in division {division}", got.division if got else None, division)

    gastro = wz_cluster.cluster_for("56.11.1")
    check("...and the division has its official name", gastro.division_title, "Gastronomie")
    check("...and rolls up to a section letter", gastro.section, "I")
    check("...with the section's name", gastro.section_title, "GASTGEWERBE")

    # A company with no code has no cluster. That is not an error and not an empty cluster: nothing
    # was classified because nothing was registered.
    for empty in (None, "", "   ", "X"):
        check(f"no cluster for {empty!r}", wz_cluster.cluster_for(empty), None)

    # A division this edition of wz_titles.yaml does not know still gets its division back, with the
    # titles empty. "We know where it sits, we cannot name the place" must not look like missing
    # source data — that is how a stale titles file hides.
    check("the titles file is loadable at all", bool(wz_cluster.load_clusters().division_titles), True)
    check("87 divisions are known", len(wz_cluster.load_clusters().division_titles), 87)
    check("...in 22 sections",
          len({s for s, _ in wz_cluster.load_clusters().division_sections.values()}), 22)

    # -------------------------------------------------------------------------------------
    section("not deciding: four different answers, never added together")
    # -------------------------------------------------------------------------------------
    no_text = bc.classify(taxonomy=taxonomy, df={}, n_companies=860, had_text=False)
    check("nothing to read", no_text.outcome, "no_text")
    no_keyword = bc.classify(
        taxonomy=taxonomy, df={}, n_companies=860, had_text=True, keywords_by_source={},
    )
    check("text but no keyword", no_keyword.outcome, "no_keyword")
    # Whatever the reason, the column carries the literal NONE — never a blank, never a null. A
    # blank cell in a handover file is indistinguishable from "nobody ran this yet".
    check("no_text prints NONE in the column", no_text.value, "NONE")
    check("no_keyword prints NONE in the column", no_keyword.value, "NONE")

    # Two branches, one keyword each, equally weighted -> torn, and not worth a model call: one
    # keyword against one keyword is a coin flip, and a 4B model flipping it costs ten seconds.
    df3 = {"leichtbau": 4, "nanotechnologie": 4}
    torn = bc.classify(
        taxonomy=taxonomy, df=df3, n_companies=860, had_text=True,
        keywords_by_source={"leichtbau": "homepage"},
    )
    check("a single shared keyword leaves it torn", torn.outcome, "torn")
    check("...with no shortlist to ask about", torn.shortlist, [])
    check("...and NONE in the column", torn.value, "NONE")

    # -------------------------------------------------------------------------------------
    section("the shortlist handed to a model")
    # -------------------------------------------------------------------------------------
    many = {k: "homepage" for k in
            ["leichtbau", "sensorik", "nachhaltigkeit", "robotik", "digitalisierung"]}
    df4 = {k: 3 for k in many}
    spread = bc.classify(
        taxonomy=taxonomy, df=df4, n_companies=860, had_text=True, keywords_by_source=many,
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
    # `branch_llm.choose` talks to Ollama, so the check goes at the layer below it: the prompt is a
    # multiple-choice question and only its own options may be returned. It calls the REAL
    # validation, `branch_llm.verdict_from_answer` — the one `choose` uses. An earlier version of
    # this section rebuilt that if/elif inline, which meant it compared a copy against itself and
    # would have kept printing `ok` after the real rule broke. Same reason `replay_judge_gate.py`
    # imports the website judge's gate instead of restating it.
    shortlist = ["Bahnindustrie", "Automobilwirtschaft", "Handwerk"]
    for answer, want_branch, want_rejected in (
        ("Bahnindustrie", "Bahnindustrie", False),
        (branch_llm.NONE_ANSWER, None, True),
        ("Raumfahrt-Katzenfutter", None, False),
        # A model picking option one, in the shapes it actually writes them. None of these is a
        # fourth option, and refusing them silently turns a correct choice into a lost row.
        ("bahnindustrie", "Bahnindustrie", False),
        ("- Bahnindustrie", "Bahnindustrie", False),
        ('"Handwerk"', "Handwerk", False),
        ("KEINE", None, True),
        # Nothing at all is not a rejection of the three. It is a broken answer.
        ("", None, False),
    ):
        verdict = branch_llm.verdict_from_answer(answer, "weil", shortlist)
        check(f"answer {answer[:24]!r} -> branch", verdict.branch, want_branch)
        check(f"answer {answer[:24]!r} -> rejected_all", verdict.rejected_all, want_rejected)
        if want_branch is None and not want_rejected:
            check(f"answer {answer[:24]!r} is refused", bool(verdict.refused), True)
    # The value written to the column is the TAXONOMY's spelling, never the model's. "bahnindustrie"
    # is not a Berlin field name and must not reach a column that may only hold one of the 77.
    for offered in ("bahnindustrie", "- BAHNINDUSTRIE"):
        got = branch_llm.verdict_from_answer(offered, "", shortlist).branch
        check(f"{offered[:20]!r} is normalised to the list's spelling", got in taxonomy.branches, True)

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

    # -------------------------------------------------------------------------------------
    section("every page that came back gets read")
    # -------------------------------------------------------------------------------------
    # The cap fetches TWO pages per category, and an earlier version kept only the first one in each
    # — so 443 of the 1,372 pages that loaded were downloaded, parsed and thrown away, 32% of the
    # fetch. The fallback that arrangement existed for, a second page rescuing a first that would
    # not load, had fired twice.
    both = content_pages.ContentPages(
        pages={"leistungen": ["wir bauen schienenfahrzeuge", "und signaltechnik"]},
        links=[],
    )
    check("the second page in a category is still read",
          bc.matched_keywords(bc.normalize(both.text), taxonomy),
          {"schienenfahrzeuge", "signaltechnik"})
    # Order is CATEGORIES order, not dict order, so the same pages always build the same text and a
    # cached answer stays valid.
    mixed = content_pages.ContentPages(
        pages={"ueber_uns": ["zweitens"], "leistungen": ["erstens"]}, links=[],
    )
    check("text is assembled in CATEGORIES order", mixed.text, "erstens zweitens")
    check("a category with no page contributes nothing",
          content_pages.ContentPages(pages={}, links=[]).text, "")

    # -------------------------------------------------------------------------------------
    section("the cached page CHOICE carries the rules that made it")
    # -------------------------------------------------------------------------------------
    # `content-index` is the one entry in page_cache that is not raw HTML — it stores which pages
    # were chosen. That cache exists so changing what we derive from a page costs nothing, so the
    # key has to move when the deriving rules move, or 692 companies quietly serve the old choice
    # and the change looks like it did nothing.
    before = content_pages.selection_version()
    check("the version is stable when nothing changes", content_pages.selection_version(), before)
    original = content_pages.MAX_PER_CATEGORY
    try:
        content_pages.MAX_PER_CATEGORY = original + 1
        check("...and moves when a cap moves", content_pages.selection_version() != before, True)
    finally:
        content_pages.MAX_PER_CATEGORY = original
    original_pattern = content_pages.CATEGORIES["leistungen"]
    try:
        content_pages.CATEGORIES["leistungen"] = __import__("re").compile("nothing-like-it")
        check("...and when a category pattern changes",
              content_pages.selection_version() != before, True)
    finally:
        content_pages.CATEGORIES["leistungen"] = original_pattern
    check("...and comes back to the same value", content_pages.selection_version(), before)

    # -------------------------------------------------------------------------------------
    section("the hyphen knob, off by default")
    # -------------------------------------------------------------------------------------
    # The compound rule blocks words German glues together and NOT the ones it hyphenates, so
    # `Kfz-Zulassung` fires `zulassung` today. Closing that also stops `E-Commerce-Lösungen` matching
    # `e-commerce`, which is a true positive lost — so it is a knob, and the default is today's
    # behaviour until somebody measures the trade.
    check("the default is today's behaviour", bc.HYPHEN_IS_BOUNDARY, False)
    loose = bc._pattern_for("zulassung", hyphen_is_boundary=False)
    strict = bc._pattern_for("zulassung", hyphen_is_boundary=True)
    check("'kfz-zulassung' matches with the knob off", bool(loose.search("kfz-zulassung")), True)
    check("...and does not with it on", bool(strict.search("kfz-zulassung")), False)
    check("a standalone word matches either way",
          (bool(loose.search("die zulassung")), bool(strict.search("die zulassung"))), (True, True))
    # The cost of turning it on, in one line: a hyphenated compound ABOUT the keyword stops matching.
    ecom_strict = bc._pattern_for("e-commerce", hyphen_is_boundary=True)
    check("the knob's price: 'e-commerce-lösungen' stops matching",
          bool(ecom_strict.search("e-commerce-lösungen")), False)
    # Flipping it has to change the version stamped into every row, or two runs look identical.
    check("the rule is part of the taxonomy version",
          bc.load_taxonomy().version == bc.hashlib.sha256(
              bc.TAXONOMY_PATH.read_text(encoding="utf-8").encode("utf-8")).hexdigest()[:12],
          False)

    # -------------------------------------------------------------------------------------
    section("a hand label is never silently thrown away or silently reused")
    # -------------------------------------------------------------------------------------
    # 40 hand labels are the only thing that can say whether the Technologiefeld is right, which
    # makes them the most expensive data in this project. Both halves of this rule were wrong once.
    label_staleness = _load_runner().label_staleness
    unlabelled = {"bucket": "torn", "technologiefeld": "NONE", "label": "", "stale": ""}
    check("an empty row is not stale", label_staleness(unlabelled, "torn", "NONE"), (False, False))

    same = {"bucket": "keyword_decided", "technologiefeld": "Handel", "label": "yes", "stale": ""}
    check("an unchanged row keeps its label",
          label_staleness(same, "keyword_decided", "Handel"), (False, False))

    # The field changed under the label. "yes, Handel is right" is not an answer about "Logistik".
    check("a changed Technologiefeld is stale",
          label_staleness(same, "keyword_decided", "Logistik"), (True, False))
    # The bucket changed, so the QUESTION changed: "is this field right" vs "does any field fit".
    # This used to erase the label outright, because the merge key included the bucket.
    check("a changed bucket is stale", label_staleness(same, "no_text", "NONE"), (True, False))

    # Stale is STICKY. Writing the row back makes `was` match the current state, so recomputing from
    # scratch said "unchanged" on the very next run and the label rejoined the precision count with
    # nobody having re-read it.
    flagged = dict(same, stale="yes")
    check("a flagged row stays flagged next run",
          label_staleness(flagged, "keyword_decided", "Handel"), (False, True))
    check("...and blanking the cell is what clears it",
          label_staleness(dict(flagged, stale=""), "keyword_decided", "Handel"), (False, False))

    # -------------------------------------------------------------------------------------
    section("a label can be made without opening the company's website")
    # -------------------------------------------------------------------------------------
    # 40 hand labels are the gate on this whole column, so the worksheet has to carry the company's
    # own sentence. Without it a person opens 40 websites and the labelling does not get done.
    runner = _load_runner()
    texts = {"services": "Familienbetrieb.  Unser Einzelhandel fuehrt Wein und Feinkost seit 1953."}
    found = runner.keywords_with_source(texts, taxonomy)
    snippets, fallback = runner.evidence_snippets(texts, found, taxonomy)
    check("the deciding keyword is found", "einzelhandel" in found, True)
    check("...and its sentence comes back with it",
          "wein und feinkost" in (snippets.get("einzelhandel") or ""), True)
    # A `no_keyword` row is the one that most needs this: "we read this and none of Berlin's words
    # appeared" is a claim a person can check in seconds, where a blank cell is not.
    holding = {"purpose": "Verwaltung eigenen Vermoegens und Beteiligung an anderen Unternehmen"}
    empty_snippets, holding_fallback = runner.evidence_snippets(holding, {}, taxonomy)
    check("nothing fired -> no snippets", empty_snippets, {})
    check("...but the text we read is still shown",
          "verwaltung eigenen vermoegens" in holding_fallback, True)
    check("...labelled with which text it was", holding_fallback.startswith("[purpose]"), True)
    check("no text at all -> nothing claimed", runner.evidence_snippets({}, {}, taxonomy), ({}, ""))
    check("the worksheet has a column for it", "evidence" in runner.BRANCH_LABEL_COLUMNS, True)

    print(f"\n{'FAILED' if FAILURES else 'all checks passed'}")
    for label in FAILURES:
        print(f"  - {label}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    raise SystemExit(main())
