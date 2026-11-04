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
