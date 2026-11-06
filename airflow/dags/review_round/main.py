"""Draw a review round from Airflow (#153): started by hand, never scheduled.

Trigger the DAG `review_round` with its form ("Trigger DAG w/ config"): field, kind, source, size, mode,
version, title. One run draws one round (`review_round.draw.run_draw`); the person then judges it on
the app's "Prüfen" page. The same as `python -m review_round.draw ...`, without a shell on the server.
"""
from __future__ import annotations

from datetime import datetime, timezone

from review_round.draw import DEFAULT_SIZE, KINDS, MAX_SIZE, MODES, run_draw

DAG_ID = "review_round"


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.sdk import Param
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="By hand only: draw a review round (Prüfen) for one source and field",
        schedule=None, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["review"],
        params={
            "field": Param("technology_field", type="string", minLength=1,
                           description="Observation field, e.g. technology_field, candidate_url, website, wz_division_estimated"),
            "kind": Param("shown", enum=list(KINDS),
                          description="shown: values the app shows; refused: values it does not; gap: companies without a value"),
            "source": Param("", type="string",
                            description="Source key (sources.key), e.g. branch_classifier; required for shown and refused"),
            "size": Param(DEFAULT_SIZE, type="integer", minimum=1, maximum=MAX_SIZE),
            "mode": Param("measure", enum=list(MODES),
                          description="measure: random, gives a precision; teach: closest to the bar, never a precision"),
            "version": Param("", type="string", description="Code or model version the values came from (optional)"),
            "title": Param("", type="string", description="Title shown in the app (optional)"),
        },
    ) as dag:
        pipeline_task(
            "draw", run_draw, retries=0,  # a retry would draw a second round
            env={f"REVIEW_{name.upper()}": f"{{{{ params.{name} }}}}"
                 for name in ("field", "kind", "source", "size", "mode", "version", "title")},
        )
    return dag


dag = _make_dag()
