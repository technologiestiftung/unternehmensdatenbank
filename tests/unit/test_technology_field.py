"""`technology_field.main`: which text a keyword fired in, and weighing over every company read."""
from __future__ import annotations

from pathlib import Path

from enrichment import branch_classifier as bc
from technology_field.main import classify_all, keywords_by_source

YAML = """
branches:
  Bahnindustrie:
    name_en: Rail industry
    keywords: [Schienenfahrzeuge, Stellwerk]
  Photonik:
    name_en: Photonics
    keywords: [Lasertechnik]
"""


def _taxonomy(tmp_path: Path) -> bc.Taxonomy:
    path = tmp_path / "taxonomy.yaml"
    path.write_text(YAML, encoding="utf-8")
    return bc.load_taxonomy(path)


def test_a_keyword_counts_for_the_first_text_it_fired_in(tmp_path: Path) -> None:
    taxonomy = _taxonomy(tmp_path)
    texts = {"services": "Wir bauen Schienenfahrzeuge.", "homepage": "Schienenfahrzeuge und Lasertechnik"}
    assert keywords_by_source(texts, taxonomy) == {"schienenfahrzeuge": "services", "lasertechnik": "homepage"}


def test_never_inside_a_compound(tmp_path: Path) -> None:
    assert keywords_by_source({"homepage": "Stellwerkstechnik"}, _taxonomy(tmp_path)) == {}


def test_weighed_over_all_companies_and_deleted_keywords_dropped(tmp_path: Path) -> None:
    taxonomy = _taxonomy(tmp_path)
    reads = {f"other-{i}": ({}, True) for i in range(50)}
    reads["rail"] = ({"schienenfahrzeuge": "services", "stellwerk": "services", "removed": "homepage"}, True)
    reads["silent"] = ({}, True)
    reads["empty"] = ({}, False)
    answers = classify_all(reads, taxonomy)
    assert answers["rail"].branch == "Bahnindustrie"
    assert "removed" not in answers["rail"].keywords
    assert answers["silent"].outcome == "no_keyword"
    assert answers["empty"].outcome == "no_text"
