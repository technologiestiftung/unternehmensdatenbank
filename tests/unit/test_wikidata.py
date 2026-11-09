"""`enrichment.wikidata`: SPARQL rows become one item per Wikidata id; only commercial register digits.
`enrichment.proposals`: which companies lose what an item proved for them, and which URLs drop to 0."""
from __future__ import annotations

from enrichment.confidence import METHOD_WIKIDATA, UNVERIFIED_CAP, WEBSITE_MIN_CONFIDENCE, score
from enrichment.wikidata import parse, register_digits
from enrichment.proposals import moved_from, unproven


def row(qid: str, website: str, label: str | None = None, register: str | None = None) -> dict:
    out = {"item": {"value": f"http://www.wikidata.org/entity/{qid}"}, "website": {"value": website}}
    if label:
        out["label"] = {"value": label}
    if register:
        out["register"] = {"value": register}
    return out


def test_two_websites_of_one_item_are_one_item() -> None:
    items = parse([row("Q1", "https://b.de", "Beispiel GmbH", "165662"), row("Q1", "https://a.de")])
    assert len(items) == 1
    assert items[0].websites == ("https://a.de", "https://b.de")
    assert (items[0].label, items[0].register_digits) == ("Beispiel GmbH", "165662")


def test_only_commercial_register_digits_are_kept() -> None:
    assert register_digits("165662") == "165662"
    assert register_digits("HRB 165662 B") == "165662"
    assert register_digits("VR 36562 B") is None  # association register: we have no such numbers
    assert register_digits(None) is None


def test_a_wikidata_website_nothing_on_the_page_confirms_stays_below_the_bar() -> None:
    # Wikidata only proposes: its base alone must never publish a website.
    verdict = score(methods=[METHOD_WIKIDATA], evidence=set(), name_uniqueness=1.0)
    assert verdict.value <= UNVERIFIED_CAP < WEBSITE_MIN_CONFIDENCE


def test_a_wikidata_website_with_the_register_number_on_the_page_is_published() -> None:
    verdict = score(methods=[METHOD_WIKIDATA], evidence={"register_number_match"}, name_uniqueness=0.5)
    assert verdict.value >= WEBSITE_MIN_CONFIDENCE
    assert verdict.verified_by == "register_number_match"


def test_only_an_item_that_now_matches_another_company_takes_its_websites_away() -> None:
    previous = {"Q1": "a", "Q2": "b", "Q3": "c"}
    # Q1 moved to company d; Q2 matches b again; Q3 stopped matching and keeps what its page proved.
    assert moved_from(previous, {"Q1": "d", "Q2": "b", "Q4": "e"}) == {"a"}


def test_a_url_the_page_no_longer_proves_drops_unless_another_record_proves_it() -> None:
    observations = [
        ("old-website", "c", "https://a.de"),   # proven 90 days ago, now only a candidate
        ("new-candidate", "c", "https://a.de"),  # just written below the bar: keeps its confidence
        ("down", "c", "https://down.de"),        # unreachable now, nobody proves it
        ("other", "c", "https://b.de"),          # another item of company c still proves it
    ]
    proven = {("c", "https://b.de"), ("d", "https://a.de")}
    assert unproven(observations, proven, frozenset({"new-candidate"})) == ["old-website", "down"]


def figure_row(qid: str, prop: str, amount: str, date: str | None = None, unit: str | None = None) -> dict:
    out = {"item": {"value": f"http://www.wikidata.org/entity/{qid}"}, "prop": {"value": prop},
           "amount": {"value": amount}}
    if date:
        out["date"] = {"value": f"{date}T00:00:00Z"}
    if unit:
        out["unit"] = {"value": unit}
    return out


def test_the_newest_dated_figure_per_field_is_kept() -> None:
    from enrichment.wikidata import EURO, Figure, parse_figures

    figures = parse_figures([
        figure_row("Q1", "P1128", "+900"),
        figure_row("Q1", "P1128", "+1200", "2023-12-31"),
        figure_row("Q1", "P1128", "+1000", "2021-12-31"),
        figure_row("Q1", "P2139", "+5000000", "2023-01-01", EURO),
    ])
    assert figures == {"Q1": {"employee_count": Figure("employee_count", 1200.0, "2023-12-31"),
                              "revenue": Figure("revenue", 5000000.0, "2023-01-01")}}


def test_revenue_not_in_euros_and_non_positive_amounts_are_left_out() -> None:
    from enrichment.wikidata import parse_figures

    assert parse_figures([
        figure_row("Q1", "P2139", "+5000000", "2023-01-01", "http://www.wikidata.org/entity/Q4917"),
        figure_row("Q2", "P1128", "0", "2023-01-01"),
    ]) == {}


def test_figures_are_inserted_updated_and_dropped() -> None:
    from wikidata.main import FIGURE_CONFIDENCE, planned_figures

    current = {("c1", "revenue"): ("r1", 5.0, "2023-01-01"), ("c2", "employee_count"): ("r2", 10.0, None)}
    stored = {
        ("c2", "employee_count"): ("o2", 10.0, FIGURE_CONFIDENCE, None),  # unchanged
        ("c3", "revenue"): ("o3", 7.0, FIGURE_CONFIDENCE, "2020-01-01"),  # item no longer matches
        ("c4", "revenue"): ("o4", 7.0, 0.0, "2020-01-01"),  # already dropped
    }
    assert planned_figures(current, stored) == [
        ("insert", ("c1", "revenue"), ("r1", 5.0, "2023-01-01")),
        ("drop", ("c3", "revenue"), None),
    ]
    stored[("c1", "revenue")] = ("o1", 4.0, FIGURE_CONFIDENCE, "2022-01-01")
    assert ("update", ("c1", "revenue"), ("r1", 5.0, "2023-01-01")) in planned_figures(current, stored)


class FakeCursor:
    """Answers write_figures' two SELECTs in order and records every statement."""

    def __init__(self, *results: list[tuple]) -> None:
        self.results, self.statements = list(results), []

    def execute(self, sql: str, params: tuple) -> None:
        self.statements.append((sql, params))

    def fetchall(self) -> list[tuple]:
        return self.results.pop(0)


def test_two_items_on_one_company_write_the_newest_figure() -> None:
    from enrichment.wikidata import Figure
    from wikidata.main import write_figures

    # Records come sorted by external_id as text, so Q10 (2024) before Q9 (2016): the 2024 figure must win.
    records = [("r10", "c1", "Q10"), ("r9", "c1", "Q9")]
    figures = {"Q10": {"revenue": Figure("revenue", 9.0, "2024-12-31")},
               "Q9": {"revenue": Figure("revenue", 5.0, "2016-12-31")}}
    cur = FakeCursor(records, [])
    assert write_figures(cur, figures, source_id="s", sync_run_id="run") == 1
    insert = [params for sql, params in cur.statements if "INSERT" in sql]
    assert [(p[0], p[1], p[4], p[5], p[6]) for p in insert] == [("c1", "r10", "revenue", 9.0, "2024-12-31")]


def founding_row(qid: str, date: str, precision: int = 9) -> dict:
    return {"item": {"value": f"http://www.wikidata.org/entity/{qid}"},
            "date": {"value": f"{date}T00:00:00Z"}, "precision": {"value": str(precision)}}


def test_the_earliest_year_at_least_to_the_year_is_the_founding_year() -> None:
    from enrichment.wikidata import parse_founding

    assert parse_founding([
        founding_row("Q1", "+1998-01-01"), founding_row("Q1", "+1990-05-01", 10),
        founding_row("Q2", "+1900-01-01", 7),  # a century names no year
        founding_row("Q3", "-0500-01-01"),  # not a company's founding
        founding_row("Q4", "0950-01-01"),  # before 1000
    ]) == {"Q1": 1990}


def test_founding_years_are_written_kept_and_dropped() -> None:
    from wikidata.main import FOUNDING_CONFIDENCE, planned_founding_years

    current = {"c1": ("r1", 1990), "c2": ("r2", 2005), "c5": ("r5", 2012)}
    stored = {
        ("c2", "2005"): FOUNDING_CONFIDENCE,  # unchanged
        ("c3", "1999"): FOUNDING_CONFIDENCE,  # item no longer matches
        ("c4", "1980"): 0.0,  # already dropped
        ("c5", "2011"): FOUNDING_CONFIDENCE,  # Wikidata corrected the year
    }
    assert planned_founding_years(current, stored) == [
        ("write", "c1", "1990", "r1"), ("write", "c5", "2012", "r5"),
        ("drop", "c3", "1999", None), ("drop", "c5", "2011", None),
    ]


def test_two_items_on_one_company_write_the_earliest_year(monkeypatch) -> None:
    from wikidata import main

    written = []
    monkeypatch.setattr(main, "insert_text_observation", lambda cur, **k: written.append(k))
    cur = FakeCursor([("r10", "c1", "Q10"), ("r9", "c1", "Q9")], [])
    assert main.write_founding_years(cur, {"Q10": 1995, "Q9": 2001}, source_id="s", sync_run_id="run") == 1
    assert [(k["company_id"], k["source_record_id"], k["value_text"], k["confidence"]) for k in written] == [
        ("c1", "r10", "1995", main.FOUNDING_CONFIDENCE)]
    assert main.write_founding_years(cur, None, source_id="s", sync_run_id="run") == 0
