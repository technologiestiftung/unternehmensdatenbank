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
import os
import sys
from pathlib import Path

#: Where a KubernetesPodOperator with `do_xcom_push` reads a pod's result. It mounts the directory
#: only then, so its absence means nobody is asking.
XCOM_FILE = Path("/airflow/xcom/return.json")

log = logging.getLogger("run_task")


#: The DAG packages live next to this file, in the image as locally.
DAGS = Path(__file__).resolve().parent


def resolve(target: str):  # noqa: ANN201
    """`"website_discovery.main:run_discover"` -> the function. Raises ValueError for anything else.

    Any module of one of our DAG packages, not only `main`: `technology_field.train` is a DAG file of
    its own, and refusing it failed every run on play (#86).
    """
    module_name, _, function_name = target.partition(":")
    package, _, module = module_name.partition(".")
    if not (package.isidentifier() and (DAGS / package / f"{module}.py").is_file() and (DAGS / package / "__init__.py").is_file()
            and module.isidentifier() and function_name.startswith("run_")):
        raise ValueError(f"not a task: {target!r} (expected '<package>.<module>:run_<name>')")
    function = getattr(importlib.import_module(module_name), function_name, None)
    if not callable(function):
        raise ValueError(f"no task function {target!r}")
    return function


def failure_line(target: str, error: BaseException, last_try: str) -> str:
    """The one line a failed task logs, for an alert to match (#86).

    Only the error's type and the first 200 characters of its first line: never a page's text.
    """
    message = (str(error).splitlines() or [""])[0][:200]
    return f"pipeline_task_failed target={target} last_try={last_try} error={type(error).__name__}: {message}"


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__)
        return 2
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        result = resolve(argv[0])()
    except Exception as error:
        log.error(failure_line(argv[0], error, os.environ.get("PIPELINE_LAST_TRY", "unknown")))
        raise
    if XCOM_FILE.parent.is_dir():
        XCOM_FILE.write_text(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
