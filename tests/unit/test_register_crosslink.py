"""`register_crosslink`: a register number seen on another company's guessed site proposes that site."""
from __future__ import annotations

from register_crosslink.main import proposals_from


def test_one_proposal_per_host_and_company() -> None:
    rows = [("https://www.sobera-capital.com/impressum", "HRB 124440", "b"),
            ("https://sobera-capital.com/", "HRB 124440", "b")]
    assert proposals_from(rows) == {"sobera-capital.com HRB 124440": ("b", "sobera-capital.com", "HRB 124440")}


def test_a_host_naming_many_of_our_companies_proposes_nothing() -> None:
    rows = [("https://group.de", f"HRB {n}", f"c{n}") for n in range(3)] + [("https://pair.de", "HRB 7", "x"),
                                                                            ("https://pair.de", "HRA 8", "y")]
    assert sorted(proposals_from(rows)) == ["pair.de HRA 8", "pair.de HRB 7"]


def test_a_host_naming_three_companies_is_crowded_and_its_earlier_records_retire() -> None:
    # #125: found for two companies once, for a third later: the host is a group page.
    from register_crosslink.main import crowded_hosts

    rows = [("https://group.de/impressum", "HRB 1", "a"), ("https://www.group.de/x", "HRB 2", "b"),
            ("https://group.de/y", "HRB 3", "c"), ("https://own.de/impressum", "HRB 4", "d")]
    assert crowded_hosts(rows) == {"group.de"}
    assert crowded_hosts(rows[:2]) == set()
