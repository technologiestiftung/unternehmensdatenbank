"""`impressum_address.main`: the address an Impressum gives, only when it proves the company (#141)."""
from __future__ import annotations

from impressum_address.main import address_on, claims_from_impressum, names_only_own_register

OWN = ["Berlin (Charlottenburg) HRB 126307 B"]
PAGE = """Impressum
The INOMICS service is owned and operated by
INOMICS GmbH
Bleibtreustr. 4
10623 Berlin, Germany
Registration number: HRB 126307 B"""


def test_the_address_of_an_impressum_naming_our_register_number() -> None:
    assert claims_from_impressum(PAGE, OWN) == {
        "address": "Bleibtreustr. 4, 10623 Berlin", "postcode": "10623", "city": "Berlin"}


def test_another_register_number_on_the_page_proves_nothing() -> None:
    assert claims_from_impressum(PAGE + "\nMuttergesellschaft: HRB 99999 B", OWN) == {}
    assert claims_from_impressum(PAGE.replace("126307", "126308"), OWN) == {}
    assert not names_only_own_register("VR 126307 B", OWN)  # another register, the same digits


def test_a_number_printed_with_a_space_is_still_ours() -> None:
    # Read as "HRB 794" and "HRB 79428"; the shorter reading is not another company.
    assert names_only_own_register("Registernummer: HRB 794 28", ["Berlin (Charlottenburg) HRB 79428"])


def test_a_parent_whose_number_starts_like_ours_is_another_entry() -> None:
    # 1263 is a prefix of our 126307, but the page does not print ours split there.
    assert claims_from_impressum(PAGE + "\nMuttergesellschaft: HRB 1263 B", OWN) == {}


def test_only_the_first_address_counts() -> None:
    page = "Beispiel GmbH\nMusterweg 1\n20539 Hamburg\nAufsicht: Bezirksamt, Karl-Marx-Allee 31, 10178 Berlin"
    assert address_on(page) is None  # moved to Hamburg; the Berlin address is the office's
    page = "Beispiel GmbH, Pestalozzistraße 103, 10625 Berlin\nHosting: STRATO AG, Otto-Ostrowski-Str. 7, 10249 Berlin"
    assert address_on(page)["postcode"] == "10625"


def test_a_berlin_district_after_the_town_is_still_berlin() -> None:
    assert address_on("Bleibtreustr. 4\n10623 Berlin-Charlottenburg") == {
        "address": "Bleibtreustr. 4, 10623 Berlin", "postcode": "10623", "city": "Berlin"}


def test_a_room_or_a_mailbox_is_not_a_street() -> None:
    assert address_on("Haus der Demokratie\nRaum 128\n10243 Berlin") is None
    assert address_on("Postfach 12 34\n10115 Berlin") is None
    assert address_on("D-10969 Berlin") is None  # no street at all


def test_a_company_or_a_person_ahead_of_the_street_is_not_the_address() -> None:
    assert address_on("INOMICS GmbH Bleibtreustr. 4\n10623 Berlin") is None
    assert address_on("INOMICS GmbH Bleibtreustr. 4 10623 Berlin") is None
    assert address_on("Max Mustermann Musterstr. 1\n10115 Berlin") is None
    assert address_on("Copyright Muster GmbH 2024\n10115 Berlin") is None


def test_streets_of_several_words_are_still_streets() -> None:
    for street in ["Unter den Linden 16", "Straße des 17. Juni 135", "Am Kupfergraben 6a", "Alt-Moabit 59-61",
                   "Rheinsberger Str. 76/77", "Karl-Marx-Allee 31"]:
        assert address_on(f"{street}\n10117 Berlin")["address"] == f"{street}, 10117 Berlin"
