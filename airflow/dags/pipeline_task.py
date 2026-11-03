"""One DAG task, run where it belongs: in Airflow's own Python locally, in a pod on the Data Hub.

The hub offers no custom Airflow image, so our packages (spaCy, psycopg2, bs4 ...) cannot be installed
next to its Airflow. Decided in issue #18 and allowed by the hub team: every task runs as a
KubernetesPodOperator pod from our own image `unternehmensdatenbank-pipelines`, which holds these DAG
packages and their requirements and runs `python -m run_task <package>.<module>:<run_function>`.

Chosen per Airflow installation by Airflow variables, so the DAG files are the same everywhere:

  pipeline_runner      "python" (default, local), "kubernetes" (the hub) or "docker" (the play
                       server, which emulates the hub with docker compose)
  pipeline_image       the image with its tag: `:main` on staging, a version on production
  pipeline_namespace   the Kubernetes namespace the hub team gives us
  pipeline_secret      a Kubernetes secret whose keys become the pod's environment: PG_DSN for the
                       writer login and the DAG settings
  pipeline_network     docker only: the network the container joins to reach Postgres

"docker" is the pod as a container: same image, same `run_task` argument. Its environment is the
Airflow worker's own PG_DSN and DAG settings (`pipeline_environment`), handed over as
`private_environment`, which Airflow neither templates nor shows. Docker is reached through
DOCKER_HOST, on the play server a socket proxy.

A pod shares no disk with anything, so a task's state belongs in Postgres (see CLAUDE.md).
"""
from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Mapping

DAGS = Path(__file__).resolve().parent

#: What a "docker" task container gets from the worker: the DAG settings of docker-compose.yml.
#: Never PATH or PYTHONPATH (the image sets its own), never AIRFLOW_* (Airflow's secrets).
PIPELINE_ENV_PREFIXES = ("PG_DSN", "DISCOVERY_", "WEBSITE_", "SOURCE_ENRICHMENT_",
                         "MANAGER_NER_", "URL_DISCOVERY_", "IMPRINT_", "WIKIDATA_",
                         "CORPUS_")


#: A task failed by an outside service (a 503, a timeout) runs again twice, 10 minutes apart (#86).
RETRIES = 2
RETRY_DELAY = timedelta(minutes=10)

#: Tells `run_task` whether a failure is final, so only a failure no retry follows raises an alert.
#: Templated: rendered when the task runs. `max_tries`, not `task.retries`: a clear in the UI raises
#: it to the try number plus the retries, while the try number keeps counting.
LAST_TRY_ENV = {"PIPELINE_LAST_TRY": "{{ 'true' if ti.try_number > ti.max_tries else 'false' }}"}


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


def pipeline_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """The part of `environ` a "docker" task container needs, nothing else."""
    return {k: v for k, v in environ.items() if k.startswith(PIPELINE_ENV_PREFIXES)}


def pipeline_task(task_id: str, run: Callable[[], Any], *, returns: bool = False, retries: int = RETRIES) -> Any:
    """A `PythonOperator` calling `run`, or a pod running the same function from the pipelines image.

    `returns=True` makes `run`'s return value the task's XCom on the hub too: the pod writes it to
    `/airflow/xcom/return.json` (`run_task`), and the operator reads it from there. A PythonOperator
    pushes its return value anyway.

    `retries=0` for a task that must not run again by itself, such as one under a request limit.
    """
    retry = {"retries": retries, "retry_delay": RETRY_DELAY}
    runner = _setting("pipeline_runner", "python")
    if runner == "docker":
        return _docker_task(task_id, run, returns, retry)
    if runner != "kubernetes":
        from airflow.providers.standard.operators.python import PythonOperator

        return PythonOperator(task_id=task_id, python_callable=run, **retry)

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
        env_vars=LAST_TRY_ENV,
        get_logs=True,
        do_xcom_push=returns,
        **retry,
    )


def _docker_task(task_id: str, run: Callable[[], Any], returns: bool, retry: dict[str, Any]) -> Any:
    from airflow.providers.docker.operators.docker import DockerOperator

    class PipelineDockerOperator(DockerOperator):
        # run_task writes the return value as JSON; the operator unpickles unless told otherwise.
        pickling_library = json

        def execute(self, context: Any) -> Any:
            # Read on the worker when the task runs, so the secrets never enter the parsed DAG.
            self._private_environment = pipeline_environment(os.environ)
            return super().execute(context)

    return PipelineDockerOperator(
        task_id=task_id,
        image="{{ var.value.pipeline_image }}",
        # `:main` is re-tagged on every merge; without a pull the host keeps running the old image.
        force_pull=True,
        command=[task_target(run)],
        environment=LAST_TRY_ENV,
        network_mode=_setting("pipeline_network") or None,
        # The worker's /tmp is not the Docker host's when Docker is reached over a proxy.
        mount_tmp_dir=False,
        auto_remove="success",
        retrieve_output=returns,
        retrieve_output_path="/airflow/xcom/return.json" if returns else None,
        **retry,
    )
