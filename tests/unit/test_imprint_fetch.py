"""`enrichment.http_client.fetch_impressum`: which page counts as a company's Impressum."""
from __future__ import annotations

import pytest

from enrichment import http_client
from enrichment.http_client import _has_imprint_content, fetch_impressum

# The shape of rustlerschriever.com: no Impressum link in the HTML, news posts naming a board,
# and `hra` only inside an image attribute.
NEWS_HOMEPAGE = (
    '<html><body><img data-heightwidthratio="0.6033"><p>Dem Vorstand gehören Erika Muster '
    "(Vorsitz) und Max Beispiel an.</p></body></html>"
)
IMPRINT = "<html><body><h1>Impressum</h1><p>Amtsgericht Charlottenburg, PR 1118 B</p></body></html>"


def test_register_words_count_only_as_whole_words_in_visible_text() -> None:
    assert not _has_imprint_content(NEWS_HOMEPAGE)
    assert not _has_imprint_content('<a href="/impressum">Kontakt</a>', imprint_url=False)
    assert _has_imprint_content("<p>Amtsgericht Berlin HRB 12345 B</p>", imprint_url=False)
    assert _has_imprint_content("<p>HRB12345</p>", imprint_url=False)
    assert _has_imprint_content("<p>USt-IdNr.: DE123456789</p>", imprint_url=False)


def test_the_footer_word_impressum_does_not_make_a_homepage_the_impressum() -> None:
    assert not _has_imprint_content("<footer>Impressum · Datenschutz</footer>", imprint_url=False)
    assert _has_imprint_content(IMPRINT)


def _site(pages: dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(http_client, "fetch_page", lambda url: pages.get(url))


def test_a_guessed_impressum_path_beats_a_homepage_of_news(monkeypatch: pytest.MonkeyPatch) -> None:
    _site({"https://a.de": NEWS_HOMEPAGE, "https://a.de/impressum.html": IMPRINT}, monkeypatch)
    assert fetch_impressum("https://a.de") == (IMPRINT, "https://a.de/impressum.html")


def test_a_homepage_with_a_register_is_the_last_resort(monkeypatch: pytest.MonkeyPatch) -> None:
    one_pager = "<p>Impressum</p><p>Amtsgericht Berlin HRB 12345 B</p>"
    _site({"https://a.de": one_pager}, monkeypatch)
    assert fetch_impressum("https://a.de") == (one_pager, "https://a.de")


def test_a_homepage_with_a_register_beats_a_contact_page(monkeypatch: pytest.MonkeyPatch) -> None:
    one_pager = "<p>Amtsgericht Charlottenburg HRB 12345 B</p>"
    contact = "<p>USt-IdNr.: DE123456789</p>"
    _site({"https://a.de": one_pager, "https://a.de/kontakt": contact}, monkeypatch)
    assert fetch_impressum("https://a.de") == (one_pager, "https://a.de")


def test_no_impressum_anywhere_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _site({"https://a.de": NEWS_HOMEPAGE}, monkeypatch)
    assert fetch_impressum("https://a.de") is None
