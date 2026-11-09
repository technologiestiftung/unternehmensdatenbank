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

**API keys are Airflow variables**, not environment variables: set one in Airflow's UI (Admin →
Variables, the name the DAG asks for, e.g. `OPENREGISTER_API_KEY`) and the next run uses it, with no
compose change and no restart. Airflow stores it encrypted with its Fernet key and shows a value whose
name contains `api_key`, `secret`, `token` or `password` as `***` in the UI and in task logs. A DAG
names the keys it needs (`pipeline_task(..., secrets=(...))`); they are read when the task starts and
handed to the task as environment variables, so the task code reads them with `os.getenv`. Never in a
DAG file: the DAGs come from the public git repository. An environment variable of the same name still
works where no variable is set. On the hub (kubernetes) the keys come from the pod's secret, as before.
"""
from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
import functools
from typing import Any, Callable, Mapping, Optional

DAGS = Path(__file__).resolve().parent

#: What a "docker" task container gets from the worker: the DAG settings of docker-compose.yml.
#: Never PATH or PYTHONPATH (the image sets its own), never AIRFLOW_* (Airflow's secrets).
PIPELINE_ENV_PREFIXES = ("PG_DSN", "DISCOVERY_", "WEBSITE_", "SOURCE_ENRICHMENT_",
                         "MANAGER_NER_", "URL_DISCOVERY_", "IMPRINT_", "WIKIDATA_",
                         "CORPUS_", "OPENREGISTER_", "BA_JOBSUCHE_", "LLM_")


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


def secret_environment(names: tuple[str, ...], variable: Callable[[str], Optional[str]],
                       environ: Mapping[str, str] = os.environ) -> dict[str, str]:
    """The API keys `names` as environment variables: the Airflow variable, else the environment.

    A key set nowhere is left out, so the task fails with its own clear message, not with an empty key.
    """
    found = {name: variable(name) or environ.get(name, "") for name in names}
    return {name: value for name, value in found.items() if value}


def _variable(name: str) -> Optional[str]:
    from airflow.sdk import Variable

    return Variable.get(name, default=None)


def _with_secrets(run: Callable[[], Any], names: tuple[str, ...]) -> Callable[[], Any]:
    """`run`, with the keys set in this worker's environment first (the local "python" runner)."""
    if not names:
        return run

    @functools.wraps(run)
    def call() -> Any:
        os.environ.update(secret_environment(names, _variable))
        return run()

    return call


def _with_env(run: Callable[[], Any]) -> Callable[..., Any]:
    """`run`, with the rendered `env` set in this worker's environment first (the "python" runner)."""
    def call(templates_dict: Optional[Mapping[str, str]] = None, **_: Any) -> Any:
        os.environ.update(templates_dict or {})
        return run()

    return call


def pipeline_task(task_id: str, run: Callable[[], Any], *, returns: bool = False, retries: int = RETRIES,
                  secrets: tuple[str, ...] = (), env: Optional[Mapping[str, str]] = None,
                  memory: Optional[str] = None) -> Any:
    """A `PythonOperator` calling `run`, or a pod running the same function from the pipelines image.

    `returns=True` makes `run`'s return value the task's XCom on the hub too: the pod writes it to
    `/airflow/xcom/return.json` (`run_task`), and the operator reads it from there. A PythonOperator
    pushes its return value anyway.

    `retries=0` for a task that must not run again by itself, such as one under a request limit.

    `secrets`: the API keys the task needs, by Airflow variable name (see the module docstring).

    `env`: environment variables for the task, Jinja-templated, rendered when the task runs: how a DAG
    started by hand passes its form's inputs (`{"REVIEW_FIELD": "{{ params.field }}"}`). The task reads
    them with `os.getenv`, on every runner. Never a secret: a rendered value shows in the UI.

    `memory`: what the pod or container is given, as Kubernetes writes it ("3Gi"), requested and the limit at
    once, for a task the namespace default would kill (`local_classifier`'s model needs about 2.1 GB). The
    Python runner ignores it: it runs inside the worker.
    """
    env = dict(env or {})
    retry = {"retries": retries, "retry_delay": RETRY_DELAY}
    runner = _setting("pipeline_runner", "python")
    if runner == "docker":
        return _docker_task(task_id, run, returns, retry, secrets, env, memory)
    if runner != "kubernetes":
        from airflow.providers.standard.operators.python import PythonOperator

        if env:
            return PythonOperator(task_id=task_id, python_callable=_with_env(_with_secrets(run, secrets)),
                                  templates_dict=env, **retry)
        return PythonOperator(task_id=task_id, python_callable=_with_secrets(run, secrets), **retry)

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
        env_vars={**LAST_TRY_ENV, **env},
        get_logs=True,
        do_xcom_push=returns,
        container_resources=k8s.V1ResourceRequirements(requests={"memory": memory}, limits={"memory": memory})
        if memory else None,
        **retry,
    )


def _docker_task(task_id: str, run: Callable[[], Any], returns: bool, retry: dict[str, Any],
                 secrets: tuple[str, ...], env: dict[str, str], memory: Optional[str] = None) -> Any:
    from airflow.providers.docker.operators.docker import DockerOperator

    class PipelineDockerOperator(DockerOperator):
        # run_task writes the return value as JSON; the operator unpickles unless told otherwise.
        pickling_library = json

        def execute(self, context: Any) -> Any:
            # Read on the worker when the task runs, so the secrets never enter the parsed DAG.
            self._private_environment = pipeline_environment(os.environ) | secret_environment(secrets, _variable)
            return super().execute(context)

    return PipelineDockerOperator(
        task_id=task_id,
        image="{{ var.value.pipeline_image }}",
        # `:main` is re-tagged on every merge; without a pull the host keeps running the old image.
        force_pull=True,
        command=[task_target(run)],
        environment={**LAST_TRY_ENV, **env},
        network_mode=_setting("pipeline_network") or None,
        # The worker's /tmp is not the Docker host's when Docker is reached over a proxy.
        mount_tmp_dir=False,
        auto_remove="success",
        retrieve_output=returns,
        retrieve_output_path="/airflow/xcom/return.json" if returns else None,
        # Docker writes sizes without the binary "i": "3Gi" -> "3g".
        mem_limit=memory.lower().replace("gi", "g").replace("mi", "m") if memory else None,
        **retry,
    )
