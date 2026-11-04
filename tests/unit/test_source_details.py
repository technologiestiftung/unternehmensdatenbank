"""A source's text for the app comes from the code that registers it (`SourceDetails`, `ensure_source`)."""
from __future__ import annotations

import pytest

from source_enrichment_common import SourceDetails, ensure_source, schedule_label


class _Cursor:
    def __init__(self) -> None:
        self.params: tuple = ()

    def execute(self, _sql: str, params: tuple) -> None:
        self.params = params

    def fetchone(self) -> tuple:
        return ("source-id",)


def test_every_airflow_schedule_we_use_has_a_german_word() -> None:
    assert schedule_label("@weekly") == "wöchentlich"
    assert schedule_label(None) == "nur von Hand"


def test_an_unknown_schedule_fails_instead_of_showing_a_guess() -> None:
    with pytest.raises(KeyError):
        schedule_label("0 3 * * *")


def test_ensure_source_writes_the_text_with_the_row() -> None:
    cur = _Cursor()
    details = SourceDetails(description="Was die Quelle ist.", schedule="täglich", base_url="https://example.org",
                            terms="CC0", terms_url="https://example.org/terms")
    assert ensure_source(cur, key="k", name="K", source_type="register", metadata={}, details=details) == "source-id"
    assert cur.params[4] == 100  # DEFAULT_PRIORITY
    assert cur.params[5:] == ("https://example.org", "Was die Quelle ist.", "CC0", "https://example.org/terms",
                              "täglich")


def test_every_dag_module_can_be_imported_with_its_details() -> None:
    # Building DETAILS runs schedule_label on each DAG's SCHEDULE: an unmapped schedule fails here.
    import importlib

    for module in ("ba_jobsuche", "berlin_zuwendungen", "common_crawl", "cordis", "foerderkatalog", "gleif",
                   "handelsregister_lookup", "openregister", "register_announcements", "register_crosslink",
                   "structured_data", "feedback_observations", "website_discovery", "wikidata", "wirtschaftsatlas",
                   "technology_field", "website_contacts", "manager_ner", "website_keywords"):
        details = importlib.import_module(f"{module}.main").DETAILS
        assert isinstance(details, SourceDetails) and details.description and details.schedule, module
