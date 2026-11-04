"""`openregister.main`: search results as register entries, and where the next search starts (#88)."""
from __future__ import annotations

from datetime import date

from openregister.main import entry_for, next_state, search_body


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
    today = date(2026, 11, 4)
    assert next_state(None, 5, today) == {"incorporated_from": "01-01-2019", "page": 2}
    assert next_state({"incorporated_from": "01-01-2019", "page": 4}, 5, today)["page"] == 5
    assert next_state({"incorporated_from": "01-01-2019", "page": 5}, 5, today) == {
        "incorporated_from": "04-10-2026", "page": 1}
