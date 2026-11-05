"""`enrichment.llm` and `llm_labels`: what is sent to the Claude API and how answers are read (#87)."""
from __future__ import annotations

import json

from enrichment import branch_classifier as bc
from enrichment import llm
from llm_labels.main import pick, tokens_this_month

WZ_TITLES = {"62": "Erbringung von Dienstleistungen der Informationstechnologie", "56": "Gastronomie"}

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
    wz_choices = llm.wz_answers(WZ_TITLES)
    assert wz_choices == ["56", "62", "NONE"]
    params = llm.request("c1", "Wir bauen Laser.", model="m", system="s", choices=choices,
                         wz_choices=wz_choices)["params"]
    schema = params["output_config"]["format"]["schema"]
    assert schema["properties"]["field"]["enum"] == choices
    assert schema["properties"]["wz_division"]["enum"] == wz_choices
    assert schema["required"] == ["field", "wz_division"]
    assert params["system"][0]["cache_control"] == {"type": "ephemeral"}


def test_system_prompt_lists_every_field_and_its_version_follows_it() -> None:
    system = llm.system_prompt(TAXONOMY, WZ_TITLES)
    assert "- Photonics (German: Photonik): Laser" in system and "- Software (German: Software): App-Entwicklung" in system
    assert "- 56: Gastronomie" in system
    assert llm.prompt_version(system) != llm.prompt_version(system + " ")


def test_answers_are_read_or_marked() -> None:
    choices = llm.answers(TAXONOMY)
    assert llm.label_of("end_turn", [json.dumps({"field": "Photonics"})], choices) == ("Photonics", "labelled")
    assert llm.label_of("end_turn", [json.dumps({"field": "NONE"})], choices) == ("NONE", "labelled")
    assert llm.label_of("end_turn", [json.dumps({"field": "Cooking"})], choices) == (None, "unparsable")
    assert llm.label_of("end_turn", [], choices) == (None, "unparsable")
    assert llm.label_of("refusal", [], choices) == (None, "refused")
    assert llm.label_of("max_tokens", ["{"], choices) == (None, "truncated")


def test_the_wz_division_is_read_or_left_empty() -> None:
    wz_choices = llm.wz_answers(WZ_TITLES)
    assert llm.wz_division_of([json.dumps({"field": "NONE", "wz_division": "56"})], wz_choices) == "56"
    assert llm.wz_division_of([json.dumps({"field": "NONE", "wz_division": "NONE"})], wz_choices) == "NONE"
    assert llm.wz_division_of([json.dumps({"field": "NONE", "wz_division": "99"})], wz_choices) is None
    assert llm.wz_division_of([json.dumps({"field": "NONE"})], wz_choices) is None
    assert llm.wz_division_of([], wz_choices) is None


def test_pick_skips_labelled_companies_in_a_fixed_order() -> None:
    gaps = {cid: ("Name", f"https://{cid}.example") for cid in ("a", "b", "c", "d")}
    first = list(pick(gaps, set(), 4, lambda website: "text"))
    assert list(pick(gaps, set(), 2, lambda website: "text")) == first[:2]
    assert list(pick(gaps, {first[0]}, 4, lambda website: "text")) == first[1:]


def test_pick_fills_the_limit_past_companies_without_text() -> None:
    gaps = {cid: ("Name", f"https://{cid}.example") for cid in ("a", "b", "c", "d")}
    order = list(pick(gaps, set(), 4, lambda website: "text"))
    textless = {f"https://{cid}.example" for cid in order[:2]}
    chosen = pick(gaps, set(), 2, lambda website: "" if website in textless else website)
    assert chosen == {cid: f"https://{cid}.example" for cid in order[2:]}


def test_an_email_or_phone_cut_at_the_limit_is_still_redacted() -> None:
    for secret in ("erika.muster@beispiel.de", "030 1234 5678"):
        text = "a" * (llm.MAX_CHARS - 10) + " " + secret + " mehr Text"
        sent = llm.redact(text, lambda t: [])
        assert len(sent) == llm.MAX_CHARS
        assert not any(secret[:n] in sent for n in range(6, len(secret) + 1))


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
            {"label": "NONE", "verdict": "Correct", "wz_verdict": "wrong"},
            {"label": "Software", "verdict": "", "wz_verdict": "correct"}]
    assert sample.score(rows) == {"field": (1, 2), "NONE": (1, 1), "all": (2, 3), "wz": (1, 2)}


def test_the_client_lives_until_every_result_is_read(monkeypatch) -> None:  # noqa: ANN001
    from types import SimpleNamespace

    state = {"closed": False}

    def stream():  # stands in for the JSONL stream, which needs the client's open connection
        for n in range(3):
            assert not state["closed"], "connection closed while results were still read"
            text = SimpleNamespace(type="text", text=json.dumps({"field": "NONE", "wz_division": "56"}))
            message = SimpleNamespace(content=[text], stop_reason="end_turn", usage=None)
            yield SimpleNamespace(custom_id=f"c{n}", result=SimpleNamespace(type="succeeded", message=message))

    class Client:
        def __init__(self) -> None:
            self.messages = SimpleNamespace(batches=SimpleNamespace(results=lambda batch_id: stream()))

        def __del__(self) -> None:  # the real client closes its connection here
            state["closed"] = True

    monkeypatch.setattr(llm, "_client", Client)
    got = list(llm.results("b1", llm.answers(TAXONOMY), llm.wz_answers(WZ_TITLES)))
    assert [row[0] for row in got] == ["c0", "c1", "c2"]
