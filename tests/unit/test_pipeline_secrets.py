"""API keys reach a task from Airflow variables, the environment as a fallback (`pipeline_task`)."""
from __future__ import annotations

from pipeline_task import secret_environment


def test_the_airflow_variable_wins_over_the_environment() -> None:
    got = secret_environment(("OPENREGISTER_API_KEY",), lambda _name: "from-variable",
                             {"OPENREGISTER_API_KEY": "from-env"})
    assert got == {"OPENREGISTER_API_KEY": "from-variable"}


def test_without_a_variable_the_environment_still_works() -> None:
    got = secret_environment(("OPENREGISTER_API_KEY",), lambda _name: None, {"OPENREGISTER_API_KEY": "from-env"})
    assert got == {"OPENREGISTER_API_KEY": "from-env"}


def test_a_key_set_nowhere_is_left_out_so_the_task_fails_with_its_own_message() -> None:
    assert secret_environment(("OPENREGISTER_API_KEY",), lambda _name: None, {}) == {}


def test_only_the_named_keys_are_handed_over() -> None:
    got = secret_environment(("A_KEY",), lambda name: f"v-{name}", {"OTHER_KEY": "x"})
    assert got == {"A_KEY": "v-A_KEY"}
