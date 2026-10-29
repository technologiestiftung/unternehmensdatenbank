"""The DAG files must load in the official Airflow image, which lacks rapidfuzz and spaCy.

The hub runs that image, and so does the play server. A top-level `from rapidfuzz import` once kept
`website_discovery` out of it entirely. Blocking the two packages reproduces the image.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"
NOT_IN_OFFICIAL_IMAGE = ("rapidfuzz", "spacy")


def test_every_dag_file_imports_without_our_extra_packages() -> None:
    dag_files = sorted(p.relative_to(DAGS) for p in DAGS.glob("*/main.py"))
    code = (
        "import sys, runpy\n"
        f"for m in {NOT_IN_OFFICIAL_IMAGE!r}: sys.modules[m] = None\n"
        f"sys.path.insert(0, {str(DAGS)!r})\n"
        f"for f in {[str(f) for f in dag_files]!r}: runpy.run_path({str(DAGS)!r} + '/' + f)\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-2000:]
