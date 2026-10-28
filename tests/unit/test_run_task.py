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
