"""`review_round.draw`: which feedback field a verdict is copied to, and what a round refuses (#153)."""
from __future__ import annotations

import pytest

from review_round.draw import accepted_fields, draw, feedback_field

# As pg_get_constraintdef prints extraction_feedback_field_name_check (shortened).
CONSTRAINT = ("CHECK ((field_name = ANY (ARRAY['name'::text, 'technology_field'::text, 'website'::text, "
              "'deeptech'::text, 'is_ai'::text])))")


def test_the_accepted_fields_are_read_from_the_constraint() -> None:
    assert accepted_fields(CONSTRAINT) == {"name", "technology_field", "website", "deeptech", "is_ai"}


def test_a_website_candidate_counts_as_a_website_and_email_is_counted_only() -> None:
    accepted = accepted_fields(CONSTRAINT)
    assert feedback_field("technology_field", accepted) == "technology_field"
    assert feedback_field("candidate_url", accepted) == "website"
    assert feedback_field("deeptech_class", accepted) == "deeptech"
    assert feedback_field("email", accepted) is None
    assert feedback_field("phone", accepted) is None


def test_a_round_without_what_it_needs_is_refused_before_touching_the_database() -> None:
    with pytest.raises(ValueError, match="needs --source"):
        draw(None, field="technology_field", kind="shown")
    with pytest.raises(ValueError, match="no gap rule"):
        draw(None, field="email", kind="gap")


def test_email_and_phone_are_never_refused_because_they_are_never_resolved() -> None:
    # CLAUDE.md: email and phone are stored, never resolved, so the app shows every stored value.
    for field in ("email", "phone"):
        with pytest.raises(ValueError, match="never resolved"):
            draw(None, source="impressum_contacts", field=field, kind="refused")


class _UnnestCursor:
    """Answers the two queries of a technology_field gap draw: the drawn ids come back as given."""

    def __init__(self) -> None:
        self.rows: list[tuple] = []

    def execute(self, sql: str, params: tuple) -> None:
        self.rows = [(cid,) for cid in params[0]] if "unnest" in sql else []

    def fetchall(self) -> list[tuple]:
        return self.rows


def test_the_technology_field_gap_round_draws_only_companies_the_keywords_left_empty(monkeypatch) -> None:
    # The intent: the gap round draws exactly the `no_keyword` companies; an `assigned` one has a value.
    import llm_labels.main
    from enrichment import branch_classifier
    from review_round.draw import _technology_field_gaps

    monkeypatch.setattr(branch_classifier, "load_taxonomy", lambda: None)
    monkeypatch.setattr(llm_labels.main, "_gaps", lambda cur, taxonomy: {
        "assigned-co": ("A GmbH", "https://a.example", "assigned"),
        "empty-co": ("B GmbH", "https://b.example", "no_keyword"),
    })
    tasks = _technology_field_gaps(_UnnestCursor(), size=10, salt="round")
    assert [task["company_id"] for task in tasks] == ["empty-co"]
