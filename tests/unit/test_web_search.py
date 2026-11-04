"""`web_search`: which result hosts become proposals."""
from __future__ import annotations

from web_search.main import hosts, search_query


def test_directories_and_repeats_are_skipped_and_at_most_three_hosts_kept() -> None:
    results = [{"url": "https://www.northdata.de/Muster+GmbH,+Berlin"}, {"url": "https://muster.de/impressum"},
               {"url": "https://muster.de/"}, {"url": "https://www.linkedin.com/company/muster"},
               {"url": "https://a.de"}, {"url": "https://b.de"}, {"url": "https://c.de"}]
    assert hosts(results) == ["https://muster.de", "https://a.de", "https://b.de"]


def test_no_results_no_proposals() -> None:
    assert hosts([]) == [] and hosts([{"url": ""}]) == []


def test_www_and_bare_domain_are_one_host() -> None:
    results = [{"url": "https://www.muster.de/"}, {"url": "https://muster.de/kontakt"}, {"url": "https://a.de"}]
    assert hosts(results) == ["https://www.muster.de", "https://a.de"]


def test_a_quote_in_the_name_does_not_end_the_phrase() -> None:
    assert search_query('Café "Zur Linde" GmbH') == '"Café Zur Linde GmbH" Berlin'
    assert search_query("Muster GmbH") == '"Muster GmbH" Berlin'
