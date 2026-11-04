"""`web_search`: which result hosts become proposals."""
from __future__ import annotations

from web_search.main import hosts


def test_directories_and_repeats_are_skipped_and_at_most_three_hosts_kept() -> None:
    results = [{"url": "https://www.northdata.de/Muster+GmbH,+Berlin"}, {"url": "https://muster.de/impressum"},
               {"url": "https://muster.de/"}, {"url": "https://www.linkedin.com/company/muster"},
               {"url": "https://a.de"}, {"url": "https://b.de"}, {"url": "https://c.de"}]
    assert hosts(results) == ["https://muster.de", "https://a.de", "https://b.de"]


def test_no_results_no_proposals() -> None:
    assert hosts([]) == [] and hosts([{"url": ""}]) == []
