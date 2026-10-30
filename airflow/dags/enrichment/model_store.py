"""Trained models in S3, so a model survives the pod that trained it.

On the Data Hub a DAG task runs in a pod that shares no disk with any other (CLAUDE.md), so a model
written to a local folder is gone when the task ends: the manager-name model was trained into
`airflow/data/models/` and would have been lost after every training run. S3 (the bucket of the
company corpus, `enrichment.corpus`) is the durable home; the local folder is a working copy.

Per model name, under `models/<name>/`:

    <version>/metrics.json    every training run, promoted or not
    <version>.tar.gz          the model files, only for a promoted version
    current.json              {"version": ...}, the version in use

Without S3 everything stays in the local folder, as before. A version already in the local folder is
not downloaded again.
"""
from __future__ import annotations

import io
import json
import logging
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Any

from enrichment import corpus

log = logging.getLogger(__name__)


def _key(name: str, path: str) -> str:
    return f"models/{name}/{path}"


def current_version(name: str, local_dir: Path) -> str | None:
    """The version in use: S3's `current.json` when a store is configured, else the local one.
    None when there is none; raises when the store cannot be read."""
    if corpus.configured():
        raw = corpus.get_bytes(_key(name, "current.json"))
        if raw is None:
            return None
    else:
        try:
            raw = (local_dir / "current.json").read_bytes()
        except FileNotFoundError:
            return None
    try:
        return json.loads(raw)["version"]
    except (KeyError, ValueError):
        return None


def local_copy(name: str, version: str, local_dir: Path) -> Path:
    """The local folder of a version, downloaded from S3 if it is not there yet.

    Raises FileNotFoundError if neither has it: a named version that cannot be loaded fails the task
    (Airflow retries it), never a silent fallback to another model.
    """
    folder = local_dir / version
    if (folder / "meta.json").exists():
        return folder
    data = corpus.get_bytes(_key(name, f"{version}.tar.gz"))
    if data is None:
        raise FileNotFoundError(f"Model {name} {version} is neither in {local_dir} nor in the store.")
    # Unpacked next to its place and renamed into it, so a folder only exists once it is complete.
    local_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=local_dir) as tmp:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            archive.extractall(tmp, filter="data")
        shutil.rmtree(folder, ignore_errors=True)  # a metrics-only folder of the same version
        Path(tmp).rename(folder)
        Path(tmp).mkdir()  # TemporaryDirectory cleans up the name it created
    log.info("Model %s %s downloaded from the store.", name, version)
    return folder


def publish(name: str, version: str, local_dir: Path, metrics: dict[str, Any], *, promoted: bool) -> None:
    """Keep a training run in S3: always its metrics; the model files and the pointer when promoted.
    The caller has written the same into `local_dir` already. Raises when the store cannot be written;
    the pointer is written only after the model files."""
    if not corpus.configured():
        return
    corpus.put_bytes(_key(name, f"{version}/metrics.json"), json.dumps(metrics, indent=2).encode(),
                     "application/json")
    if not promoted:
        return
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        archive.add(local_dir / version, arcname=".")
    corpus.put_bytes(_key(name, f"{version}.tar.gz"), buffer.getvalue(), "application/gzip")
    corpus.put_bytes(_key(name, "current.json"), json.dumps({"version": version}).encode(), "application/json")
    log.info("Model %s %s published to the store.", name, version)
