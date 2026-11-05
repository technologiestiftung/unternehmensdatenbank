"""Draw a review round (#153): ~200 values of one source and field for a person to judge in the app.

    python -m review_round.draw --source branch_classifier --field technology_field --kind shown
    python -m review_round.draw --field technology_field --kind gap --size 150
    python -m review_round.draw --source website_discovery --field candidate_url --kind refused

Locally with `PYTHONPATH=airflow/dags` and `PG_DSN` (the writer); on play in the pipelines image
(`docker run --entrypoint python ... -m review_round.draw ...`), where this package is.

Three kinds (V4__review_queue.sql):

- `shown`: values of `--source` and `--field` that the app shows (the coordinator picked them).
- `refused`: values of `--source` and `--field` it does not show, e.g. `candidate_url` below the bar.
- `gap`: companies without a value; the person enters it. `technology_field` draws the companies the
  keywords left empty on a website that was read (the ones `technology_field_train` is judged on) and
  shows the LLM's label (#87) as a suggestion where there is one. Other fields: the company profile's
  column is empty.

The order is random but fixed per round (md5 of the observation or company and the round id). At most
one task per company: `feedback_observations` keeps one vote per person, company and field. Only
`measure` rounds so far: `teach` (the cases a model is least sure of) comes with a model to ask.

A verdict reaches `extraction_feedback` under the round's feedback field (the trigger in V4), which
must be one that table accepts: read from its check constraint, never restated here. Email and phone
have none, so their verdicts are counted, never turned into a correction. They are stored, never
resolved (`NEVER_RESOLVED`), so a `shown` round draws every stored value and there is no `refused`.
"""
from __future__ import annotations

import argparse
import logging
import re
import uuid
from typing import Any

from psycopg2.extras import Json

log = logging.getLogger(__name__)

DEFAULT_SIZE = 200
#: Fields the coordinator stores but never resolves (CLAUDE.md): the app shows every stored value.
NEVER_RESOLVED = frozenset({"email", "phone"})
#: Observation fields whose verdict counts for another field: a website candidate is a website.
FEEDBACK_ALIASES = {"candidate_url": "website", "directory_url": "website", "deeptech_class": "deeptech"}
#: Gap rounds other than technology_field: the profile column that is empty, and who is asked about.
PROFILE_GAPS = {
    "website": ("website", "p.register_status = 'active'"),
    "wz_code": ("wz_code", "p.register_status = 'active'"),
    "deeptech": ("deeptech_class", "p.website IS NOT NULL"),
    "is_ai": ("is_ai", "p.website IS NOT NULL"),
}


def accepted_fields(constraint: str) -> set[str]:
    """The field names in `extraction_feedback_field_name_check`, as `pg_get_constraintdef` prints it."""
    return set(re.findall(r"'([a-z_]+)'::text", constraint))


def feedback_field(field: str, accepted: set[str]) -> str | None:
    """The `extraction_feedback` field a verdict on `field` is copied to, or None (counted only)."""
    target = FEEDBACK_ALIASES.get(field, field)
    return target if target in accepted else None


def _accepted(cur: Any) -> set[str]:
    cur.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'extraction_feedback_field_name_check'")
    row = cur.fetchone()
    return accepted_fields(row[0]) if row else set()


def _observations(cur: Any, *, source: str, field: str, shown: bool, size: int, salt: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT id, company_id, value_text, confidence, evidence, method
        FROM (
            SELECT DISTINCT ON (o.company_id)
                   o.id::text AS id, o.company_id::text AS company_id, o.value_text, o.confidence,
                   CASE WHEN jsonb_typeof(o.metadata -> 'evidence') = 'array' THEN o.metadata -> 'evidence'
                        ELSE '[]' END AS evidence,
                   COALESCE(o.metadata ->> 'method', o.metadata -> 'methods' ->> 0) AS method,
                   md5(o.id::text || %(salt)s) AS draw_key
            FROM public.company_field_observations o
            JOIN public.sources s ON s.id = o.source_id AND s.key = %(source)s
            JOIN public.companies c ON c.id = o.company_id AND c.merged_into IS NULL
            WHERE o.field_name = %(field)s AND COALESCE(o.value_text, '') <> ''
              AND (%(stored_only)s OR EXISTS (SELECT 1 FROM public.company_canonical_field_sources k
                                              WHERE k.company_field_observation_id = o.id) = %(shown)s)
            ORDER BY o.company_id, draw_key
        ) one_per_company
        ORDER BY draw_key
        LIMIT %(size)s
        """,
        {"source": source, "field": field, "shown": shown, "stored_only": field in NEVER_RESOLVED,
         "size": size, "salt": salt},
    )
    return [{"observation_id": oid, "company_id": cid, "value": value[:500], "confidence": conf,
             "evidence": evidence, "method": method} for oid, cid, value, conf, evidence, method in cur.fetchall()]


def _technology_field_gaps(cur: Any, *, size: int, salt: str) -> list[dict[str, Any]]:
    # Imported here: a DAG file must not import another at the top (tests/unit/test_dag_files.py).
    from enrichment import branch_classifier as bc
    from llm_labels.main import SOURCE_KEY as LLM_SOURCE, _gaps

    gaps = sorted(_gaps(cur, bc.load_taxonomy()))
    cur.execute(
        """
        SELECT id FROM unnest(%s::text[]) AS id ORDER BY md5(id || %s) LIMIT %s
        """,
        (gaps, salt, size),
    )
    chosen = [row[0] for row in cur.fetchall()]
    cur.execute(
        """
        SELECT DISTINCT ON (r.company_id) r.company_id::text, r.source_payload ->> 'label'
        FROM public.company_source_records r
        JOIN public.sources s ON s.id = r.source_id AND s.key = %s
        WHERE r.company_id = ANY(%s::uuid[]) AND r.source_payload ->> 'outcome' = 'labelled'
        ORDER BY r.company_id, r.last_synced_at DESC
        """,
        (LLM_SOURCE, chosen),
    )
    suggestions = dict(cur.fetchall())
    return [{"company_id": cid, "suggestion": suggestions.get(cid)} for cid in chosen]


def _profile_gaps(cur: Any, *, field: str, size: int, salt: str) -> list[dict[str, Any]]:
    column, who = PROFILE_GAPS[field]
    cur.execute(
        f"""
        SELECT p.id::text FROM public.company_profile_mv p
        WHERE p.{column} IS NULL AND {who}
        ORDER BY md5(p.id::text || %s)
        LIMIT %s
        """,  # noqa: S608  column and condition come from PROFILE_GAPS, never from input
        (salt, size),
    )
    return [{"company_id": cid} for (cid,) in cur.fetchall()]


def draw(cur: Any, *, field: str, kind: str, source: str | None = None, size: int = DEFAULT_SIZE,
         title: str | None = None, version: str | None = None) -> tuple[str, int]:
    """Create one round and its tasks. Returns (round id, tasks drawn)."""
    if kind in ("shown", "refused") and not source:
        raise ValueError(f"a {kind} round needs --source")
    if kind == "refused" and field in NEVER_RESOLVED:
        raise ValueError(f"{field} is never resolved, so nothing is refused: draw it as shown")
    if kind == "gap" and field != "technology_field" and field not in PROFILE_GAPS:
        raise ValueError(f"no gap rule for {field!r}; known: technology_field, {', '.join(sorted(PROFILE_GAPS))}")
    round_id = str(uuid.uuid4())
    if kind == "gap":
        tasks = (_technology_field_gaps(cur, size=size, salt=round_id) if field == "technology_field"
                 else _profile_gaps(cur, field=field, size=size, salt=round_id))
    else:
        tasks = _observations(cur, source=source, field=field, shown=kind == "shown", size=size, salt=round_id)
    if not tasks:
        raise ValueError(f"nothing to draw for {field} · {kind} · {source}")
    target = feedback_field(field, _accepted(cur))
    cur.execute(
        """
        INSERT INTO public.review_rounds (id, source_key, field_name, feedback_field, kind, version, title, size)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (round_id, source, field, target, kind, version,
         title or " · ".join(part for part in (field, kind, source) if part), len(tasks)),
    )
    for position, task in enumerate(tasks, start=1):
        cur.execute(
            """
            INSERT INTO public.review_tasks
              (round_id, position, company_id, observation_id, value, confidence, evidence, method, suggestion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (round_id, company_id, value) DO NOTHING
            """,
            (round_id, position, task["company_id"], task.get("observation_id"), task.get("value", ""),
             task.get("confidence"), Json(task.get("evidence") or []), task.get("method"), task.get("suggestion")),
        )
    return round_id, len(tasks)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--field", required=True)
    parser.add_argument("--kind", required=True, choices=("shown", "refused", "gap"))
    parser.add_argument("--source")
    parser.add_argument("--size", type=int, default=DEFAULT_SIZE)
    parser.add_argument("--title")
    parser.add_argument("--version")
    args = parser.parse_args(argv)

    from source_enrichment_common import get_connection

    logging.basicConfig(level=logging.INFO)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            round_id, n = draw(cur, field=args.field, kind=args.kind, source=args.source, size=args.size,
                               title=args.title, version=args.version)
    finally:
        connection.close()
    print(f"round {round_id}: {n} tasks")


if __name__ == "__main__":
    main()
