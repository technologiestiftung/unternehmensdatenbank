"""`prepare_offeneregister_import`: legal form from the name, postcode from the address, status words."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from import_offeneregister_records import FLAGS, OFFICER_FIELDS, REGISTRATION_FIELDS, payload  # noqa: E402
from prepare_offeneregister_import import legal_form, parse_address, to_row  # noqa: E402


def test_longest_legal_form_wins() -> None:
    assert legal_form("Beispiel Verwaltungs GmbH & Co. KG") == "GmbH & Co. KG"
    assert legal_form("Beispiel UG (haftungsbeschränkt)") == "UG"
    assert legal_form("Beispiel gGmbH") == "gGmbH"
    assert legal_form("Beispiel GmbH") == "GmbH"
    assert legal_form("Sportverein Beispiel e.V.") == "e. V."
    assert legal_form("Beispiel e.K.") == "e. K."
    assert legal_form("Beispiel KG") == "KG"


def test_legal_form_only_at_the_end_and_not_inside_words() -> None:
    assert legal_form("AG Beispiel Holding") is None
    assert legal_form("Bauhaus Tag") is None


def test_variants_and_trailing_words() -> None:
    assert legal_form("Beispiel Gesellschaft mbH") == "GmbH"
    assert legal_form("Beispiel G.m.b.H.") == "GmbH"
    assert legal_form("Beispiel GmbH Berlin") == "GmbH"
    assert legal_form("Beispiel GmbH Steuerberatungsgesellschaft") == "GmbH"
    assert legal_form("Beispiel Rechtsanwälte PartG mbB") == "PartG mbB"
    assert legal_form("Beispiel Limited") == "Ltd."
    assert legal_form("Beispiel K.G.") == "KG"
    assert legal_form("Beispiel GmbH & Co. KG in Liquidation") == "GmbH & Co. KG"


def test_address_stops_after_postcode_and_city() -> None:
    assert parse_address("Kleine Straße 9, 10119 Berlin") == ("Kleine Straße 9, 10119 Berlin", "10119", "Berlin")
    assert parse_address("Gebäude 53, 13403 Berlin.") == ("Gebäude 53, 13403 Berlin", "13403", "Berlin")
    # Register text after the city, sometimes with people's names, is cut off.
    assert parse_address("Weg 1, 10115 Berlin Geschäftsführer: Erika Muster, *01.01.1970") == (
        "Weg 1, 10115 Berlin", "10115", "Berlin")
    assert parse_address("Berlin. Gegenstand: Handel") == (None, None, None)


def test_address_drops_care_of_segments() -> None:
    assert parse_address("c/o Erika Muster, Weg 1, 10115 Berlin") == ("Weg 1, 10115 Berlin", "10115", "Berlin")
    assert parse_address("Weg 1, bei Max Beispiel, 10115 Berlin") == ("Weg 1, 10115 Berlin", "10115", "Berlin")
    assert parse_address("Weg 1, z.Hd. Erika Muster, 10115 Berlin") == ("Weg 1, 10115 Berlin", "10115", "Berlin")
    assert parse_address("C.O. Beispiel GmbH, Weg 1, 10115 Berlin") == ("Weg 1, 10115 Berlin", "10115", "Berlin")
    assert parse_address("Weg 1, zu Händen Erika Muster, 10115 Berlin") == ("Weg 1, 10115 Berlin", "10115", "Berlin")
    # A name split by a comma, and a marker in the postcode's own segment.
    assert parse_address("c/o Muster, Erika, Weg 1, 10115 Berlin") == ("Weg 1, 10115 Berlin", "10115", "Berlin")
    assert parse_address("Weg 1, c/o Muster 10115 Berlin") == ("Weg 1, 10115 Berlin", "10115", "Berlin")
    assert parse_address("Weg 1, Aufgang 2, 10115 Berlin") == ("Weg 1, Aufgang 2, 10115 Berlin", "10115", "Berlin")
    # "bei" only as a word, not inside one.
    assert parse_address("Beiersdorfweg 1, 10115 Berlin") == ("Beiersdorfweg 1, 10115 Berlin", "10115", "Berlin")


def test_row_uses_northdatas_register_format_and_german_status() -> None:
    row = to_row({
        "company_number": "F1103R_HRB1", "native_company_number": "Berlin (Charlottenburg) HRB 1",
        "name": "Beispiel GmbH", "current_status": "removed", "registered_address": "",
        "retrieved_at": "2018-11-01T00:00:00Z",
    })
    assert row["registration_number"] == "Amtsgericht Berlin (Charlottenburg) HRB 1"
    assert row["register_status"] == "erloschen"
    assert row["address"] is None and row["postcode"] is None
    assert row["legal_form"] == "GmbH" and row["normalized_name"] == "beispiel"


def test_city_falls_back_to_the_registered_seat() -> None:
    row = to_row({
        "company_number": "F1103R_HRB2", "native_company_number": "Berlin (Charlottenburg) HRB 2",
        "name": "Beispiel GmbH", "current_status": "currently registered", "registered_address": "",
        "registered_office": "Potsdam", "retrieved_at": "2018-11-01T00:00:00Z",
    })
    assert row["city"] == "Potsdam" and row["address"] is None


def test_payload_keeps_register_history_officers_and_raw_address() -> None:
    company = {
        "company_number": "F1103R_HRB3", "native_company_number": "Berlin (Charlottenburg) HRB 3",
        "name": "Neu GmbH", "current_status": "currently registered", "jurisdiction_code": "de",
        "registered_address": "Weg 1, 10115 Berlin Geschäftsführer: Erika Muster", "registered_office": "Berlin",
        "registrar": "Berlin (Charlottenburg)", "register_art": "HRB", "register_nummer": "3",
        "_registerNummerSuffix": "", "former_registrar": "", "federal_state": "Berlin",
        "retrieved_at": "2018-11-01T00:00:00Z",
        "register_flag_AD": 1, "register_flag_CD": 0, "register_flag_DK": 1, "register_flag_HD": 0,
        "register_flag_SI": 1, "register_flag_UT": 0, "register_flag_VOE": 1,
    }
    names = [{"company_name": "Alt GmbH"}, {"company_name": "neu  gmbh"}]
    # A real sqlite row has every column; empty ones must not reach the payload.
    moves = [dict.fromkeys(REGISTRATION_FIELDS) | {
        "data_type": "subsequent-registration", "previous_company_number": "F1103R_HRB1", "source_url": ""}]
    officers = [{**dict.fromkeys(OFFICER_FIELDS), "firstname": "Erika", "lastname": "Muster",
                 "position": "Geschäftsführer", "city": ""}]
    p = payload(company, names, moves, officers)
    assert p["former_names"] == ["Alt GmbH"]
    assert p["address"]["address"] == "Weg 1, 10115 Berlin"
    assert p["address"]["raw"] == company["registered_address"]
    assert p["officers"] == [{"firstname": "Erika", "lastname": "Muster", "position": "Geschäftsführer"}]
    assert p["register"]["document_flags"] == {"AD": True, "CD": False, "DK": True, "HD": False,
                                               "SI": True, "UT": False, "VÖ": True}
    assert p["registrations"] == [{"data_type": "subsequent-registration", "previous_company_number": "F1103R_HRB1"}]


def test_payload_seat_of_only_spaces_is_none() -> None:
    company = dict.fromkeys(["company_number", "native_company_number", "current_status", "jurisdiction_code",
                             "registrar", "register_art", "register_nummer", "_registerNummerSuffix",
                             "former_registrar", "federal_state", "retrieved_at", *FLAGS.values()])
    company |= {"name": "Leer GmbH", "registered_address": "", "registered_office": "  "}
    assert payload(company, [], [], [])["register"]["registered_office"] is None
