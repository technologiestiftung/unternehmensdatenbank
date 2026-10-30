"""`enrichment.model_store`: a model trained in one pod is there for the next."""
from __future__ import annotations

import json
import shutil

from enrichment import model_store


def _train(folder, version: str) -> dict:  # noqa: ANN001
    """What run_train leaves on disk for a promoted version."""
    (folder / version).mkdir(parents=True)
    (folder / version / "meta.json").write_text('{"name": "test"}')
    (folder / version / "weights.bin").write_bytes(b"\x00\x01")
    metrics = {"version": version, "promoted": True}
    (folder / version / "metrics.json").write_text(json.dumps(metrics))
    return metrics


def test_a_promoted_model_survives_losing_the_local_folder(s3, tmp_path) -> None:  # noqa: ANN001
    pod_a = tmp_path / "pod_a"
    model_store.publish("ner", "v1", pod_a, _train(pod_a, "v1"), promoted=True)
    shutil.rmtree(pod_a)  # the pod is gone

    pod_b = tmp_path / "pod_b"
    assert model_store.current_version("ner", pod_b) == "v1"
    folder = model_store.local_copy("ner", "v1", pod_b)
    assert (folder / "weights.bin").read_bytes() == b"\x00\x01"
    assert model_store.local_copy("ner", "v1", pod_b) == folder  # not downloaded twice


def test_a_run_that_was_not_promoted_keeps_its_metrics_but_not_the_pointer(s3, tmp_path) -> None:  # noqa: ANN001
    model_store.publish("ner", "v1", tmp_path, _train(tmp_path, "v1"), promoted=True)
    model_store.publish("ner", "v2", tmp_path, {"version": "v2", "promoted": False}, promoted=False)
    assert model_store.current_version("ner", tmp_path / "elsewhere") == "v1"
    assert "models/ner/v2/metrics.json" in s3.objects
    assert "models/ner/v2.tar.gz" not in s3.objects


def test_without_a_store_everything_stays_local(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.delenv("CORPUS_S3_ENDPOINT", raising=False)
    (tmp_path / "current.json").write_text('{"version": "v9"}')
    assert model_store.current_version("ner", tmp_path) == "v9"
    assert model_store.local_copy("ner", "v9", tmp_path) is None  # nothing local, nothing to download
