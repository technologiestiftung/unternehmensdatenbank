"""Run one DAG task outside Airflow: `python -m run_task website_discovery.main:run_discover`.

The entry point of the pipelines image. On the Data Hub a task is a pod started from that image
(`pipeline_task.py`), so Airflow never imports our packages; this runs the same function a local
`PythonOperator` calls, and its exit code is the task's result.

Only `run_*` functions in our own DAG packages can be named, so a pod command cannot be turned into
an arbitrary call.
"""
from __future__ import annotations

import importlib
import json
import logging
import sys
from pathlib import Path

#: Where a KubernetesPodOperator with `do_xcom_push` reads a pod's result. It mounts the directory
#: only then, so its absence means nobody is asking.
XCOM_FILE = Path("/airflow/xcom/return.json")


def resolve(target: str):  # noqa: ANN201
    """`"website_discovery.main:run_discover"` -> the function. Raises ValueError for anything else."""
    module_name, _, function_name = target.partition(":")
    if not module_name.endswith(".main") or not function_name.startswith("run_"):
        raise ValueError(f"not a task: {target!r} (expected '<package>.main:run_<name>')")
    function = getattr(importlib.import_module(module_name), function_name, None)
    if not callable(function):
        raise ValueError(f"no task function {target!r}")
    return function


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__)
        return 2
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = resolve(argv[0])()
    if XCOM_FILE.parent.is_dir():
        XCOM_FILE.write_text(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
