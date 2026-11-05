"""`scripts/wirtschaftsatlas_layers_sample.py`: which companies the layer sample draws (#148)."""
from __future__ import annotations

import importlib.util
from pathlib import Path


def _script():  # noqa: ANN202
    path = Path(__file__).resolve().parents[2] / "scripts" / "wirtschaftsatlas_layers_sample.py"
    spec = importlib.util.spec_from_file_location("wirtschaftsatlas_layers_sample", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_only_one_field_is_drawn_and_agreement_counts_shown_fields_only() -> None:
    rows = [("a", "A GmbH", "https://a.de", ["FinTech"], "FinTech"),
            ("b", "B GmbH", "https://b.de", ["Photonics"], "Drones"),
            ("c", "C GmbH", "https://c.de", ["Games", "Film Industry"], None),  # several fields: left out
            ("d", "D GmbH", "https://d.de", ["Robotics"], "NONE"),  # NONE is no field to agree with
            ("e", "E GmbH", "https://e.de", ["Games"], None)]
    single, several, agreement = _script().split(rows)
    assert [row[0] for row in single] == ["a", "b", "d", "e"]
    assert several == 1
    assert agreement == (1, 2)
