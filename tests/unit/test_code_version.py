"""Every observation says which code wrote it, so review rounds can compare rule versions (#153)."""
from __future__ import annotations

from source_enrichment_common import with_code_version


def test_the_code_version_is_added_when_the_image_sets_it(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("CODE_VERSION", "abc123")
    assert with_code_version({"evidence": ["register_match"]}) == {"evidence": ["register_match"], "code_version": "abc123"}
    assert with_code_version(None) == {"code_version": "abc123"}


def test_nothing_is_added_when_it_is_not_set(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.delenv("CODE_VERSION", raising=False)
    metadata = {"evidence": []}
    assert with_code_version(metadata) == {"evidence": []}
    assert with_code_version(metadata) is not metadata
