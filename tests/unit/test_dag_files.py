"""A DAG file must not import another DAG file at the top.

Importing a module builds the DAGs it defines, so Airflow would find them in the importing file too
and refuse one copy (`AirflowDagDuplicatedIdException`): `technology_field/train.py` once imported
`technology_field/main.py` this way. Import inside the task function instead.
"""
from __future__ import annotations

import ast
from pathlib import Path

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"


def _module(path: Path) -> str:
    return ".".join(path.relative_to(DAGS).with_suffix("").parts)


def _defines_a_dag(tree: ast.Module) -> bool:
    """A module-level `dag = ...` or `dags = ...`, the way every DAG file here ends."""
    return any(
        isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in {"dag", "dags"} for t in node.targets)
        for node in tree.body
    )


def test_no_dag_file_imports_another_dag_file_at_the_top() -> None:
    trees = {_module(p): ast.parse(p.read_text(encoding="utf-8")) for p in DAGS.glob("*/*.py")}
    dag_modules = {name for name, tree in trees.items() if _defines_a_dag(tree)}
    assert "technology_field.train" in dag_modules  # the test sees what it is meant to see
    offending = [
        (name, node.module)
        for name in sorted(dag_modules)
        for node in trees[name].body
        if isinstance(node, ast.ImportFrom) and node.module in dag_modules - {name}
    ]
    assert offending == []
