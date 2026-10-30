"""`technology_field.train`: promoted only on enough people's judgements, and only when it is better."""
from __future__ import annotations

from enrichment.branch_textcat import MIN_ACCURACY, MIN_HUMAN_HOLDOUT
from technology_field.train import should_promote


def test_promoted_only_with_enough_judged_companies_and_a_good_enough_accuracy() -> None:
    assert should_promote((MIN_ACCURACY, MIN_HUMAN_HOLDOUT), None)
    assert not should_promote((1.0, MIN_HUMAN_HOLDOUT - 1), None)
    assert not should_promote((MIN_ACCURACY - 0.01, MIN_HUMAN_HOLDOUT), None)


def test_promoted_only_when_it_beats_the_model_in_use() -> None:
    assert should_promote((0.9, MIN_HUMAN_HOLDOUT), (0.8, MIN_HUMAN_HOLDOUT))
    assert not should_promote((0.8, MIN_HUMAN_HOLDOUT), (0.8, MIN_HUMAN_HOLDOUT))
