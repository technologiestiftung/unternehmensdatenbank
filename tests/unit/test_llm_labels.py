"""`enrichment.llm` and `llm_labels`: what is sent to the Claude API and how answers are read (#87)."""
from __future__ import annotations

import json

from enrichment import branch_classifier as bc
from enrichment import llm
from llm_labels.main import pick, tokens_this_month

TAXONOMY = bc.taxonomy_from_rows(
    [{"name": "Software", "name_en": "Software"}, {"name": "Photonik", "name_en": "Photonics"}],
    [("Software", "App-Entwicklung"), ("Photonik", "Laser")],
)


def test_names_emails_and_phones_never_leave() -> None:
    text = "Kontakt: Erika Muster, erika@beispiel.de, Tel. 030 1234 5678. Wir bauen Laser."

    def people(t: str) -> list[tuple[int, int]]:  # stands in for spaCy's PER entities
        start = t.find("Erika Muster")
        return [(start, start + len("Erika Muster"))] if start >= 0 else []

    assert llm.redact(text, people) == "Kontakt: [NAME], [EMAIL], Tel. [PHONE]. Wir bauen Laser."


def test_text_is_cut_before_sending() -> None:
    assert len(llm.redact("a" * (llm.MAX_CHARS + 100), lambda t: [])) == llm.MAX_CHARS


def test_the_answer_is_one_of_the_fields_or_none() -> None:
    choices = llm.answers(TAXONOMY)
    assert choices == ["Photonics", "Software", "NONE"]
    params = llm.request("c1", "Wir bauen Laser.", model="m", system="s", choices=choices)["params"]
    assert params["output_config"]["format"]["schema"]["properties"]["field"]["enum"] == choices
    assert params["system"][0]["cache_control"] == {"type": "ephemeral"}


def test_system_prompt_lists_every_field_and_its_version_follows_it() -> None:
    system = llm.system_prompt(TAXONOMY)
    assert "- Photonics (German: Photonik): Laser" in system and "- Software (German: Software): App-Entwicklung" in system
    assert llm.prompt_version(system) != llm.prompt_version(system + " ")


def test_answers_are_read_or_marked() -> None:
    choices = llm.answers(TAXONOMY)
    assert llm.label_of("end_turn", [json.dumps({"field": "Photonics"})], choices) == ("Photonics", "labelled")
    assert llm.label_of("end_turn", [json.dumps({"field": "NONE"})], choices) == ("NONE", "labelled")
    assert llm.label_of("end_turn", [json.dumps({"field": "Cooking"})], choices) == (None, "unparsable")
    assert llm.label_of("end_turn", [], choices) == (None, "unparsable")
    assert llm.label_of("refusal", [], choices) == (None, "refused")
    assert llm.label_of("max_tokens", ["{"], choices) == (None, "truncated")


def test_pick_skips_labelled_companies_in_a_fixed_order() -> None:
    gaps = {cid: ("Name", "https://example.org") for cid in ("a", "b", "c", "d")}
    first = pick(gaps, set(), 4)
    assert pick(gaps, set(), 2) == first[:2]
    assert pick(gaps, {first[0]}, 4) == first[1:]


def test_the_budget_starts_again_each_month() -> None:
    assert tokens_this_month({"month": "2026-11", "tokens": 50}, "2026-11") == 50
    assert tokens_this_month({"month": "2026-10", "tokens": 50}, "2026-11") == 0
    assert tokens_this_month(None, "2026-11") == 0


def test_score_counts_only_judged_rows() -> None:
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "scripts" / "llm_labels_sample.py"
    spec = importlib.util.spec_from_file_location("llm_labels_sample", path)
    sample = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sample)
    rows = [{"label": "Photonics", "verdict": "correct"}, {"label": "Software", "verdict": "wrong"},
            {"label": "NONE", "verdict": "Correct"}, {"label": "Software", "verdict": ""}]
    assert sample.score(rows) == {"field": (1, 2), "NONE": (1, 1), "all": (2, 3)}
