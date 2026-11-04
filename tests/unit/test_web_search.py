"""`web_search`: which result hosts become proposals."""
from __future__ import annotations

from web_search.main import hosts, search_query

NAME = "Muster Bau GmbH"


def test_only_hosts_with_the_name_are_kept_and_at_most_three() -> None:
    results = [{"url": "https://www.northdata.de/Muster+GmbH,+Berlin"}, {"url": "https://www.webvalid.de/muster-bau"},
               {"url": "https://muster-bau.de/impressum"}, {"url": "https://muster-bau.de/"},
               {"url": "https://www.linkedin.com/company/muster"}, {"url": "https://musterbau-berlin.de"},
               {"url": "https://www.musterbau.com"}, {"url": "https://muster-bau.org"}]
    assert hosts(results, NAME) == ["https://muster-bau.de", "https://musterbau-berlin.de", "https://www.musterbau.com"]


def test_no_results_no_proposals() -> None:
    assert hosts([], NAME) == [] and hosts([{"url": ""}], NAME) == []
    assert hosts([{"url": "https://www.yellowmap.de/muster-bau"}], NAME) == []


def test_www_and_bare_domain_are_one_host() -> None:
    results = [{"url": "https://www.musterbau.de/"}, {"url": "https://musterbau.de/kontakt"}]
    assert hosts(results, NAME) == ["https://www.musterbau.de"]


def test_a_quote_in_the_name_does_not_end_the_phrase() -> None:
    assert search_query('Café "Zur Linde" GmbH') == '"Café Zur Linde GmbH" Berlin'
    assert search_query("Muster GmbH") == '"Muster GmbH" Berlin'
