"""`enrichment.model_store`: a model trained in one pod is there for the next."""
from __future__ import annotations

import json
import shutil

import pytest

from enrichment import corpus, model_store
from tests.unit.conftest import S3Error


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
    with pytest.raises(FileNotFoundError):  # nothing local, nothing to download
        model_store.local_copy("ner", "v9", tmp_path)


def test_a_named_version_missing_from_the_store_fails_instead_of_falling_back(s3, tmp_path) -> None:  # noqa: ANN001
    model_store.publish("ner", "v1", tmp_path, _train(tmp_path, "v1"), promoted=True)
    del s3.objects["models/ner/v1.tar.gz"]  # the pointer survived, the model files did not
    assert model_store.current_version("ner", tmp_path / "pod") == "v1"
    with pytest.raises(FileNotFoundError):
        model_store.local_copy("ner", "v1", tmp_path / "pod")


def test_a_store_without_current_json_has_no_version(s3, tmp_path) -> None:  # noqa: ANN001
    (tmp_path / "current.json").write_text('{"version": "v9"}')  # a stale working copy is not asked
    assert model_store.current_version("ner", tmp_path) is None


def test_a_store_that_fails_raises_instead_of_meaning_no_version(s3, tmp_path, monkeypatch) -> None:  # noqa: ANN001
    model_store.publish("ner", "v1", tmp_path, _train(tmp_path, "v1"), promoted=True)

    def broken(*_args):  # noqa: ANN002, ANN202
        raise S3Error("InternalError")

    monkeypatch.setattr(s3, "get_object", broken)
    with pytest.raises(S3Error):
        model_store.current_version("ner", tmp_path / "pod")
    monkeypatch.setattr(corpus, "_unreachable", True)  # an earlier page read lost the store
    with pytest.raises(ConnectionError):
        model_store.current_version("ner", tmp_path / "pod")
