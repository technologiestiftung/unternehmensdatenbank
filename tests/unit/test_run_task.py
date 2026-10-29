"""`run_task` and `pipeline_task.task_target`: what a hub pod runs, and that it can run nothing else."""
from __future__ import annotations

import pytest

from pipeline_coordinator.main import run_pipeline_coordinator
from pipeline_task import task_target
from run_task import resolve
from website_discovery.main import run_discover


def test_a_pod_command_names_the_package_and_function() -> None:
    assert task_target(run_discover) == "website_discovery.main:run_discover"
    assert task_target(run_pipeline_coordinator) == "pipeline_coordinator.main:run_pipeline_coordinator"


def test_the_pod_command_resolves_back_to_the_same_function() -> None:
    assert resolve(task_target(run_discover)) is run_discover


@pytest.mark.parametrize("target", [
    "os:system",                          # not one of our DAG packages
    "website_discovery.main:demotion",    # ours, but not a task
    "website_discovery.main:run_nothing", # no such task
    "website_discovery.main",             # no function at all
])
def test_anything_but_a_task_is_refused(target: str) -> None:
    with pytest.raises(ValueError):
        resolve(target)


def test_a_pod_hands_the_return_value_to_airflow_only_when_asked(monkeypatch, tmp_path) -> None:
    import json

    import run_task

    monkeypatch.setattr(run_task, "resolve", lambda target: lambda: 500)
    monkeypatch.setattr(run_task, "XCOM_FILE", tmp_path / "missing" / "return.json")
    assert run_task.main(["website_discovery.main:run_discover"]) == 0
    assert not (tmp_path / "missing").exists()

    monkeypatch.setattr(run_task, "XCOM_FILE", tmp_path / "return.json")
    assert run_task.main(["website_discovery.main:run_discover"]) == 0
    assert json.loads((tmp_path / "return.json").read_text()) == 500


def test_a_docker_task_gets_the_dag_settings_and_nothing_else() -> None:
    from pipeline_task import pipeline_environment

    worker = {"PG_DSN": "host=db", "WEBSITE_KEYWORDS_BATCH_SIZE": "50", "WEBSITE_DISCOVERY_OSM": "false",
              "PATH": "/usr/bin", "PYTHONPATH": "/opt/airflow", "AIRFLOW__CORE__FERNET_KEY": "x"}
    assert pipeline_environment(worker) == {
        "PG_DSN": "host=db", "WEBSITE_KEYWORDS_BATCH_SIZE": "50", "WEBSITE_DISCOVERY_OSM": "false"}
