"""`openregister.main`: search results as register entries, and where the next search starts (#88), and the walk back by founding year (#143)."""
from __future__ import annotations

from datetime import date

from openregister import main
from openregister.main import (
    backlog_bounds,
    entry_for,
    first_backlog,
    next_backlog,
    next_search,
    next_state,
    search_body,
)


def item(**values: object) -> dict[str, object]:
    base = {"company_id": "DE-HRB-F1103R-123456", "name": "Beispiel Software GmbH", "register_number": "123456 B",
            "register_type": "HRB", "register_court": "Berlin (Charlottenburg)", "active": True, "legal_form": "gmbh",
            "address": {"street": "Musterstraße 1", "postal_code": "10115", "city": "Berlin", "country": "DE"},
            "purpose": "Die Entwicklung von Software."}
    return base | values


def test_a_berlin_result_becomes_our_identifier_and_register_fields() -> None:
    entry = entry_for(item(), "2026-11-04")
    assert entry.registers == ["Berlin (Charlottenburg) HRB 123456", "Berlin (Charlottenburg) HRB 123456 B"]
    assert entry.fields == {
        "name": "Beispiel Software GmbH", "legal_form": "GmbH", "address": "Musterstraße 1, 10115 Berlin",
        "postcode": "10115", "city": "Berlin", "register_status": "active",
        "business_purpose": "Die Entwicklung von Software.",
    }
    assert entry.updated == "2026-11-04" and entry.active is True


def test_inactive_is_dissolved_and_missing_values_are_left_out() -> None:
    entry = entry_for(item(active=False, address=None, purpose=None), "2026-11-04")
    assert entry.fields["register_status"] == "dissolved"
    assert "address" not in entry.fields and "business_purpose" not in entry.fields


def test_other_courts_and_unreadable_entries_are_left_out() -> None:
    assert entry_for(item(register_court="Hamburg"), "2026-11-04") is None
    assert entry_for(item(register_type="GsR"), "2026-11-04") is None
    assert entry_for(item(name=" "), "2026-11-04") is None


def test_the_search_asks_for_berlin_since_a_day_one_page_at_a_time() -> None:
    body = search_body("01-01-2019", 3)
    assert {"field": "register_court", "value": "Berlin (Charlottenburg)"} in body["filters"]
    assert {"field": "incorporated_at", "min": "01-01-2019"} in body["filters"]
    assert body["pagination"]["page"] == 3


def test_the_next_run_continues_the_pass_then_starts_a_new_one() -> None:
    first = next_state(None, 5, date(2026, 3, 2))
    assert first == {"incorporated_from": "01-01-2019", "page": 2, "pass_started": "2026-03-02"}
    assert next_state({**first, "page": 4}, 5, date(2026, 9, 1))["pass_started"] == "2026-03-02"
    # The pass began on 2 March; 31 days before is 30 January, not 31 days before the day it ended.
    assert next_state({**first, "page": 5}, 5, date(2026, 11, 4)) == {
        "incorporated_from": "30-01-2026", "page": 1}


def test_a_one_page_pass_starts_the_next_from_its_own_day() -> None:
    assert next_state({"incorporated_from": "30-01-2026", "page": 1}, 1, date(2026, 11, 4)) == {
        "incorporated_from": "04-10-2026", "page": 1}


def test_the_backlog_reads_one_founding_year_and_ends_the_day_before_the_first_pass() -> None:
    assert first_backlog() == {"year": 2018, "page": 1}
    assert backlog_bounds(2018) == ("01-01-2018", "31-12-2018")
    low, high = backlog_bounds(2017)
    body = search_body(low, 2, high)
    assert {"field": "incorporated_at", "min": "01-01-2017", "max": "31-12-2017"} in body["filters"]


def test_a_first_pass_from_mid_year_leaves_no_gap(monkeypatch) -> None:
    monkeypatch.setattr(main, "INCORPORATED_FROM", "15-06-2019")
    assert main.first_backlog() == {"year": 2019, "page": 1}
    assert main.backlog_bounds(2019) == ("01-01-2019", "14-06-2019")


def test_the_backlog_walks_back_year_by_year_and_ends_at_the_oldest() -> None:
    assert next_backlog({"year": 2018, "page": 1}, 3) == {"year": 2018, "page": 2}
    assert next_backlog({"year": 2018, "page": 3}, 3) == {"year": 2017, "page": 1}
    assert next_backlog({"year": 2005, "page": 1}, 0) == {"year": 2004, "page": 1}  # a year with nobody
    assert next_backlog({"year": main.OLDEST_YEAR, "page": 1}, 1) is None


def test_new_companies_come_first_then_the_backlog() -> None:
    state = {"incorporated_from": "30-01-2026", "page": 2}
    new = next_search(state, {"year": 2018, "page": 4}, new_pass_done=False)
    assert {"field": "incorporated_at", "min": "30-01-2026"} in new["filters"] and new["pagination"]["page"] == 2
    old = next_search(state, {"year": 2018, "page": 4}, new_pass_done=True)
    assert {"field": "incorporated_at", "min": "01-01-2018", "max": "31-12-2018"} in old["filters"]
    assert old["pagination"]["page"] == 4
    assert next_search(state, None, new_pass_done=True) is None
    # Until this run has read the new pass to its end, a finished backlog does not stop it.
    assert next_search(state, None, new_pass_done=False) is not None
