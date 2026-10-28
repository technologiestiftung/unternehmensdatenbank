"""One DAG task, run where it belongs: in Airflow's own Python locally, in a pod on the Data Hub.

The hub offers no custom Airflow image, so our packages (spaCy, psycopg2, bs4 ...) cannot be installed
next to its Airflow. Decided in issue #18 and allowed by the hub team: every task runs as a
KubernetesPodOperator pod from our own image `unternehmensdatenbank-pipelines`, which holds these DAG
packages and their requirements and runs `python -m run_task <package>.main:<run_function>`.

Chosen per Airflow installation by Airflow variables, so the DAG files are the same everywhere:

  pipeline_runner      "python" (default, local) or "kubernetes" (the hub)
  pipeline_image       the image with its tag: `:main` on staging, a version on production
  pipeline_namespace   the Kubernetes namespace the hub team gives us
  pipeline_secret      a Kubernetes secret whose keys become the pod's environment: PG_DSN for the
                       writer login and the ENABLE_* flags

A pod shares no disk with anything, so a task's state belongs in Postgres (see CLAUDE.md).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

DAGS = Path(__file__).resolve().parent


def _setting(key: str, default: str = "") -> str:
    from airflow.sdk import Variable

    return Variable.get(key, default=default)


def task_target(run: Callable[[], None]) -> str:
    """`run_task`'s argument for a function: `"website_discovery.main:run_discover"`.

    From the function's FILE, not `run.__module__`: Airflow imports a DAG file under a made-up module
    name (`unusual_prefix_…`), which would mean nothing inside the pod.
    """
    module = Path(run.__code__.co_filename).resolve().relative_to(DAGS).with_suffix("")
    return f"{'.'.join(module.parts)}:{run.__name__}"


def pipeline_task(task_id: str, run: Callable[[], Any], *, returns: bool = False) -> Any:
    """A `PythonOperator` calling `run`, or a pod running the same function from the pipelines image.

    `returns=True` makes `run`'s return value the task's XCom on the hub too: the pod writes it to
    `/airflow/xcom/return.json` (`run_task`), and the operator reads it from there. A PythonOperator
    pushes its return value anyway.
    """
    if _setting("pipeline_runner", "python") != "kubernetes":
        from airflow.providers.standard.operators.python import PythonOperator

        return PythonOperator(task_id=task_id, python_callable=run)

    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from kubernetes.client import models as k8s

    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        # Templated, so they are read when the task runs, not on every parse of the DAG file.
        image="{{ var.value.pipeline_image }}",
        namespace="{{ var.value.pipeline_namespace }}",
        arguments=[task_target(run)],
        env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=_setting("pipeline_secret")))],
        get_logs=True,
        do_xcom_push=returns,
    )
