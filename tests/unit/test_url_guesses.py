"""The guesses #89 added to `url_patterns`, and the affixes `name_frequency` measures for them.

Every example is a website discovery missed on play and Wikidata, Common Crawl or a person found.
"""
from __future__ import annotations

from enrichment import name_frequency, url_patterns
from enrichment.imprint_extractor import extract_register_numbers

COMMON = frozenset({"berlin", "foerderverein", "verlag", "medien"})
AFFIXES = ("{}-berlin", "{}-{legal}", "{}berlin")


def _hosts(name: str, **settings) -> list[str]:
    return [c.host for c in url_patterns.generate_candidates(name, "Berlin", **settings)]


def test_legal_forms_and_the_owner_are_not_name_words() -> None:
    assert url_patterns.name_words("Aktion gegen den Hunger gGmbH") == ["aktion", "gegen", "den", "hunger"]
    assert url_patterns.name_words("Zentralkonsum eG") == ["zentralkonsum"]
    assert url_patterns.name_words("Wester Styling UG (haftungsbeschränkt)") == ["wester", "styling"]
    assert url_patterns.legal_form_slug("Wester Styling UG (haftungsbeschränkt)") == "ug"
    assert url_patterns.name_words("Inhalt Media GmbH") == ["inhalt", "media"]


def test_the_owner_is_left_out_of_guesses_but_not_out_of_verification() -> None:
    name = "Alt Marzahner Apotheke Inhaber Martin Pöhlmann e.K."
    hosts = _hosts(name)
    assert "alt-marzahner-apotheke.de" in hosts
    assert not any("poehlmann" in h or "martin" in h for h in hosts)
    assert url_patterns.name_words(name) [-2:] == ["martin", "poehlmann"]


def test_single_letters_and_digits_are_guessed_too() -> None:
    assert "8greenbottles.de" in _hosts("8 Green Bottles GmbH")
    assert "a-zelte.de" in _hosts("A-Zelte UG (haftungsbeschränkt)")


def test_a_run_of_words_from_the_middle_of_the_name() -> None:
    hosts = _hosts("Förderverein Technologiestiftung Berlin e.V.", common=COMMON)
    assert "technologiestiftung-berlin.de" in hosts
    assert "orbitalsystems.de" in _hosts("German Orbital Systems GmbH")


def test_a_run_of_common_words_alone_is_never_guessed() -> None:
    extra = [c.host for c in url_patterns.generate_candidates("Alpha Morgenpost Verlag Berlin GmbH", "Berlin", common=COMMON)
             if c.pattern in url_patterns.EXTRA_PATTERNS]
    assert "verlag-berlin.de" not in extra and "berlin.de" not in extra
    assert "morgenpost-verlag.de" in extra


def test_the_new_guesses_come_after_every_older_one_and_on_de_only() -> None:
    before = _hosts("Navitas Bildung gGmbH")
    after = url_patterns.generate_candidates("Navitas Bildung gGmbH", "Berlin", common=COMMON, affixes=AFFIXES)
    assert [c.host for c in after][:len(before)] == before
    extra = [c for c in after if c.pattern in url_patterns.EXTRA_PATTERNS]
    assert extra and all(c.host.endswith(".de") for c in extra)
    assert "navitas-bildung-ggmbh.de" in [c.host for c in extra]
    assert "navitasbildungberlin.de" in [c.host for c in extra]
    assert "wester-styling-ug.de" in _hosts("Wester Styling UG (haftungsbeschränkt)", affixes=AFFIXES)
    assert [c.rank for c in after] == list(range(len(after)))


def test_an_affix_is_what_a_proven_domain_adds_to_the_name() -> None:
    assert name_frequency.affix_of("Navitas gGmbH", "navitas-ggmbh.de") == "{}-{legal}"
    assert name_frequency.affix_of("AKANTHUS GmbH", "akanthusberlin.de") == "{}berlin"
    assert name_frequency.affix_of("Foo Bar GmbH", "my-foo.de") == "my-{}"
    assert name_frequency.affix_of("Meier GmbH", "meier.de") is None
    # Longer than an affix: a second name, not a pattern to repeat.
    assert name_frequency.affix_of("bebra Verlag GmbH", "bebra-wissenschaft.de") is None


def test_only_affixes_enough_companies_share_are_guessed() -> None:
    proven = [(f"Firma{i} GmbH", f"firma{i}-berlin.de") for i in range(9)] + [("Solo GmbH", "solo24.de")]
    assert name_frequency.common_affixes(proven, min_companies=8) == ("{}-berlin",)


def test_a_berlin_register_number_with_its_b_glued_on() -> None:
    assert extract_register_numbers("Amtgericht Berlin Charlottenburg HRB 132889B Steuernummer") == ["HRB 132889"]
    assert extract_register_numbers("HRB 12345BX") == []
