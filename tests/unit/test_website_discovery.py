"""`website_discovery.main`: the re-check rule for stored websites, and officers as comparison keys."""
from __future__ import annotations

from enrichment import confidence as conf
from enrichment.officers import officer_keys
from website_discovery.main import demotion


def test_a_website_the_page_never_identified_drops_below_the_bar() -> None:
    assert demotion({"evidence": ["company_name_on_page", "imprint_found"]}, 0.63) == conf.UNVERIFIED_CAP
    assert conf.UNVERIFIED_CAP < conf.WEBSITE_MIN_CONFIDENCE


def test_a_proven_website_stays() -> None:
    assert demotion({"evidence": ["register_number_match"]}, 0.9) is None
    assert demotion({"evidence": ["officer_name_on_imprint"]}, 0.8) is None
    assert demotion({"evidence": ["company_name_on_page", "postcode_match"]}, 0.6) is None


def test_already_below_the_bar_or_unscored_is_left_alone() -> None:
    assert demotion({"evidence": []}, conf.UNVERIFIED_CAP) is None
    assert demotion({}, None) is None


def test_officers_become_comparison_keys_people_only() -> None:
    keys = officer_keys([
        {"firstname": "Erika", "lastname": "Muster", "type": "person"},
        {"name": "Dr. Max Beispiel"},
        {"name": "Beispiel Holding GmbH", "type": "company"},
        {"name": "  "},
    ])
    assert keys == frozenset({"erika muster", "max beispiel"})
    assert officer_keys(None) == frozenset()


def test_one_failing_company_does_not_discard_the_batch(monkeypatch) -> None:
    """A page that crashes discovery used to end the batch and throw away every other result."""
    from contextlib import nullcontext
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from source_enrichment_common import CompanyCandidate
    from website_discovery import main

    companies = [CompanyCandidate(id=str(i), company_name=f"Firma {i}", address=None, postcode=None,
                                  city=None, registration_number=None) for i in range(5)]

    def discover(company, **_):
        if company.id == "2":
            raise ValueError("a page bs4 cannot read")
        return SimpleNamespace(website=f"https://firma{company.id}.de", errors=[])

    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value = nullcontext(MagicMock())
    written, finished = [], {}
    monkeypatch.setattr(main, "fetch_companies_without_source", lambda *a, **k: companies)
    monkeypatch.setattr(main, "get_connection", lambda: connection)
    monkeypatch.setattr(main, "ensure_source", lambda *a, **k: "source")
    monkeypatch.setattr(main, "start_sync_run", lambda *a, **k: "run")
    monkeypatch.setattr(main, "load_officers", lambda *a, **k: {})
    monkeypatch.setattr(main, "_common_words", lambda cur: set())
    monkeypatch.setattr(main.pipeline, "discover", discover)
    monkeypatch.setattr(main.dataclasses, "asdict", vars)
    monkeypatch.setattr(main, "write_company_result", lambda cur, *, company, **k: written.append(company.id))
    monkeypatch.setattr(main, "finish_sync_run", lambda cur, **k: finished.update(k))

    assert main.run_discover() == 5

    assert sorted(written) == ["0", "1", "3", "4"]
    assert finished["records_created"] == 4
    assert finished["records_failed"] == 1


def test_only_a_full_batch_starts_the_next_run(monkeypatch) -> None:
    from website_discovery import main

    monkeypatch.setattr(main, "BATCH_SIZE", 500)
    assert main.is_full_batch(500)
    assert not main.is_full_batch(499)
    assert not main.is_full_batch(0)
    assert not main.is_full_batch(None)


def test_only_a_clean_recheck_retires_websites_it_did_not_find_again() -> None:
    from enrichment.observations import retires_unconfirmed

    assert retires_unconfirmed({"website": None, "errors": []})
    assert retires_unconfirmed({"website": None})
    assert not retires_unconfirmed({"website": None, "errors": ["url_patterns: ConnectionError: ..."]})
