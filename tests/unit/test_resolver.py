"""`pipeline_coordinator.resolver`: the age rules (#49) name fields the coordinator really publishes."""
from __future__ import annotations

from pipeline_coordinator import resolver


def _published_fields(monkeypatch) -> set[str]:  # noqa: ANN001
    """Every field the resolve_* functions hand to `_apply_canonical`, recorded by calling them."""
    published: set[str] = set()

    def record(cur, *, logical_field, **_kwargs):  # noqa: ANN001, ANN003, ANN202
        published.add(logical_field)
        return 0

    monkeypatch.setattr(resolver, "_apply_canonical", record)
    for name in dir(resolver):
        if name.startswith("resolve_") and name != "resolve_ceo_contact":  # writes company_managers
            getattr(resolver, name)(None)
    return published


def test_every_half_life_names_a_published_field(monkeypatch) -> None:  # noqa: ANN001
    # A misspelt key would silently switch decay off for that field.
    assert set(resolver.HALF_LIVES) - _published_fields(monkeypatch) == set()


def test_half_lives_are_positive_and_the_founding_date_never_decays() -> None:
    assert all(years > 0 for years in resolver.HALF_LIVES.values())
    assert "established_at" not in resolver.HALF_LIVES
