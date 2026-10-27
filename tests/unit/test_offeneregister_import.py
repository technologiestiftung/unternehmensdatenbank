"""`prepare_offeneregister_import`: legal form from the name, postcode from the address, status words."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

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
