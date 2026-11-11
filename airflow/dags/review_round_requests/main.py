"""Draw the review rounds someone asked for in the app (#190).

The app's "Prüfen" page writes a row into `review_round_requests` (any logged-in person, Hasura role `user`; V11). Every
`SCHEDULE` this DAG draws each pending request with `review_round.draw.draw`, oldest first, each in its own
transaction, and writes back `drawn` with the round, or `failed` with the reason (e.g. "nothing to draw
for ..."), which the app shows. The app never talks to Airflow.

A request and its round are committed together and the request is locked while drawn (`FOR UPDATE SKIP
LOCKED`), so a retry or a second run never draws the same request twice.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from review_round.draw import draw

log = logging.getLogger(__name__)

DAG_ID = "review_round_requests"
SCHEDULE = "*/5 * * * *"

_NEXT = """
    SELECT id, field_name, kind, source_key, size, mode, title FROM public.review_round_requests
    WHERE status = 'pending' ORDER BY requested_at LIMIT 1 FOR UPDATE SKIP LOCKED
"""


def draw_request(cur: Any, request: tuple, draw_round: Callable[..., tuple[str, int]] = draw) -> str:
    """Draw one locked request and write back its outcome. Returns the new status."""
    request_id, field, kind, source, size, mode, title = request
    cur.execute("SAVEPOINT draw_request")
    try:
        round_id, _ = draw_round(cur, field=field, kind=kind, source=source or None, size=size, mode=mode,
                                 title=title or None)
    except ValueError as error:  # the draw's own refusals; anything else fails the run and leaves it pending
        cur.execute("ROLLBACK TO SAVEPOINT draw_request")
        cur.execute("UPDATE public.review_round_requests SET status = 'failed', error = %s, done_at = now() "
                    "WHERE id = %s", (str(error), request_id))
        return "failed"
    cur.execute("UPDATE public.review_round_requests SET status = 'drawn', round_id = %s, done_at = now() "
                "WHERE id = %s", (round_id, request_id))
    return "drawn"


def run_requests() -> int:
    """Draw every pending request. Returns how many were handled."""
    from source_enrichment_common import get_connection

    handled = 0
    connection = get_connection()
    try:
        while True:
            with connection, connection.cursor() as cur:
                cur.execute(_NEXT)
                request = cur.fetchone()
                if request is None:
                    break
                status = draw_request(cur, request)
            handled += 1
            log.info("Request %s: %s.", request[0], status)
    finally:
        connection.close()
    return handled


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Draws the review rounds someone asked for on the app's Prüfen page",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 11, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["review"],
    ) as dag:
        pipeline_task("draw_requests", run_requests)
    return dag


dag = _make_dag()
