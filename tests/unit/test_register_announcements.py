"""`register_announcements.main`: Berlin rows read off the portal's result list, other courts left out."""
from __future__ import annotations

from datetime import date

import pytest

from enrichment.register_names import legal_form, normalize_company_name
from register_announcements.main import name_match, parse_announcements, parse_java_date


def row(n: int, when: str, portal_id: str, category: str, court: str, name_city: str) -> str:
    """One result row, shaped as handelsregister.de writes it (2026-10-28)."""
    return (
        f'<a id="bekanntMachungenForm:datalistId:0:j_idt175:{n}:j_idt176" href="#" class="ui-commandlink ui-widget" '
        f"onclick=\"fireBekanntmachung{n}('{when}', '{portal_id}');;PrimeFaces.ab({{}});return false;\">"
        f'<label class="ui-outputlabel ui-widget">             {category}  <br>           {court}         <br> '
        f"{name_city}</label></a><br /><br />"
    )


PAGE = "<form id='bekanntMachungenForm'>" + "".join([
    row(0, "Wed Oct 28 00:00:00 CET 2026", "182538", "Löschungsankündigung",
        "Berlin Amtsgericht Berlin (Charlottenburg) HRB 211322", "Beststrom24 UG (haftungsbeschränkt) – Berlin"),
    row(1, "Fri Oct 02 00:00:00 CEST 2026", "181001", "Registerbekanntmachung nach dem Umwandlungsgesetz",
        "Berlin Amtsgericht Berlin (Charlottenburg) HRA 59880", "DEVG Deutsche Energie Versorgung GmbH & Co. KG – Berlin"),
    row(2, "Wed Oct 28 00:00:00 CET 2026", "182529", "Löschungsankündigung",
        "Hamburg Amtsgericht Hamburg HRB 192874", "BRND Digital GmbH – Hamburg"),
    row(3, "Tue Oct 27 00:00:00 CET 2026", "182400", "Sonstige Registerbekanntmachung",
        "Berlin Amtsgericht Berlin (Charlottenburg) HRB 300001", "Ohne Ort GmbH"),
    '<a class="ui-commandlink" onclick="somethingElse()"><label>Filter löschen</label></a>',
]) + "</form>"


def test_berlin_rows_are_read_and_other_courts_are_not() -> None:
    found = parse_announcements(PAGE)
    assert [a.portal_id for a in found] == ["182538", "181001", "182400"]
    first = found[0]
    assert first.published_on == date(2026, 10, 28)
    assert first.category == "Löschungsankündigung"
    assert first.register == "Berlin (Charlottenburg) HRB 211322"
    assert first.registration_number == "Amtsgericht Berlin (Charlottenburg) HRB 211322"
    assert (first.name, first.city) == ("Beststrom24 UG (haftungsbeschränkt)", "Berlin")


def test_the_register_value_has_the_identifier_format() -> None:
    # `company_identifiers.value` for scheme 'register', as the OffeneRegister import writes it.
    assert parse_announcements(PAGE)[1].register == "Berlin (Charlottenburg) HRA 59880"


def test_created_companies_get_the_import_name_key_and_legal_form() -> None:
    name = parse_announcements(PAGE)[1].name
    assert legal_form(name) == "GmbH & Co. KG"
    # The import's key, quirks included ("& Co." stays as "und co"): same key, whichever source created it.
    assert normalize_company_name(name) == "devg deutsche energie versorgung und co"


def test_java_dates_in_summer_and_winter_time() -> None:
    assert parse_java_date("Wed Oct 28 00:00:00 CET 2026") == date(2026, 10, 28)
    assert parse_java_date("Fri Oct 02 00:00:00 CEST 2026") == date(2026, 10, 2)
    with pytest.raises(ValueError):
        parse_java_date("28.10.2026")


def test_an_empty_page_gives_nothing() -> None:
    assert parse_announcements("<html></html>") == []


def test_a_row_without_a_city_keeps_its_whole_line_as_the_name() -> None:
    no_city = parse_announcements(PAGE)[2]
    assert (no_city.name, no_city.city) == ("Ohne Ort GmbH", None)


def test_no_company_with_the_same_name_and_city_creates_one() -> None:
    assert name_match([]) == (None, [])


def test_exactly_one_company_with_the_same_name_and_city_is_the_company() -> None:
    assert name_match(["a"]) == ("a", [])


def test_several_companies_with_the_same_name_and_city_go_to_a_person() -> None:
    assert name_match(["a", "b"]) == (None, ["a", "b"])
