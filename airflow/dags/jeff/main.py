"""jeff: Technologiefeld and WZ guesses from TSB's self-hosted jev API, next to `local_classifier` (#194).

[jeff](https://github.com/technologiestiftung/jeff) (`enrichment.jeff`) reads the same text `local_classifier`
reads, the website text the `technology_field` DAG read off the company's own published site, and answers two
questions:

| field | labels |
|---|---|
| `technology_field` | Berlin's 77 Technologiefelder (stored in English, `name_en`; asked in German) |
| `wz_division_estimated` | the 87 WZ 2025 divisions (stored as two digits, "62"); never the official `wz_code` |

**Its own source, ranked like the local model** (decided 2026-11-11): jeff's confidence times
`decide.CONFIDENCE_SCALE`, so at most 0.50. The keywords (0.60), the Wirtschaftsatlas layers (0.56), Claude
(0.55) and spaCy (0.52-0.55) win wherever they have an answer, and people's corrections (priority 50) always
do. Next to `local_classifier`, never instead of it, so a Prüfen round can tell which of the two is right
more often. Nothing here has been measured.

**Text leaves the pod, to TSB's own server** (`enrichment.jeff`), whole and unredacted (decided 2026-11-11).
Needs `JEFF_API_KEY`, an Airflow variable (Admin → Variables; `pipeline_task`, "API keys").

**Cost.** Six requests per company: each field's labels are more than jeff takes in one question, so they
are asked in two parts and a final round (`jeff.choose`). Answers are cached in each company's source record
with the model and the website, as in `local_classifier`; at most `MAX_COMPANIES` per run, never asked first.
"""
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Mapping

from enrichment import jeff
from local_classifier import main as local
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    schedule_label,
    start_sync_run,
)

log = logging.getLogger(__name__)

DAG_ID = "jeff"
SOURCE_KEY = "jeff"
SCHEDULE = "@daily"
DETAILS = SourceDetails(
    description=(
        "Vermutungen von jeff, dem selbst betriebenen Klassifikationsdienst der Technologiestiftung "
        "(GLiFormer), aus dem Text der Firmen-Website: Technologiefeld und WZ-Abteilung. Immer unter "
        "Stichwörtern, Claude und Korrekturen."
    ),
    schedule=schedule_label(SCHEDULE),
    base_url="https://github.com/technologiestiftung/jeff",
)
MAX_COMPANIES = int(os.getenv("JEFF_MAX_COMPANIES", "300"))
MAX_CHARS = int(os.getenv("JEFF_MAX_CHARS", "2500"))
WORKERS = 4
FIELDS = ("technology_field", "wz_division_estimated")
INSTRUCTIONS = {
    "technology_field": "In welchem Technologiefeld arbeitet diese Firma?",
    "wz_division_estimated": "Zu welcher Abteilung der Wirtschaftszweige (WZ 2025) gehört diese Firma?",
}
#: Part of each cached answer's key, after the model jeff serves. Change a question's wording, raise the number.
QUESTIONS = "questions-1"


def _source_id(cur: Any, model_version: str) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="jeff (GLiFormer, self-hosted)", source_type="classifier",
                         metadata={"dag_id": DAG_ID, "model": model_version, "url": jeff.BASE_URL}, details=DETAILS)


def answer_one(http: Any, text: str, asked: Mapping[str, Mapping[str, str]]) -> dict[str, tuple[str, float]]:
    """field -> (stored value, jeff's own confidence); a field jeff gave no known label for is left out."""
    answers = {}
    for field, labels in asked.items():
        label, confidence = jeff.choose(http, text, INSTRUCTIONS[field], list(labels))
        if label in labels:
            answers[field] = (labels[label], confidence)
    return answers


def run_classify() -> int:
    """Ask jeff about up to `MAX_COMPANIES` companies, write its guesses and drop those of companies whose
    website is no longer the one they were read off. Returns companies answered plus guesses dropped."""
    from enrichment import page_cache
    from technology_field.train import require_store, stored_text

    if not page_cache.enabled():
        log.info("No page store configured: no website text to read, nothing asked or dropped.")
        return 0
    http = jeff.session()
    model_version = f"jeff/{jeff.model(http)}"
    asked_with = f"{model_version}/{QUESTIONS}"
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur, model_version)
            asked = {field: labels for field, labels in local.questions(cur).items() if field in FIELDS}
            websites = local._websites(cur)
            todo = local.to_ask(websites, local._cached(cur, source_id), MAX_COMPANIES, asked_with=asked_with)
            sync_run_id = start_sync_run(cur, source_id=source_id)
    finally:
        connection.close()
    written = without_text = dropped = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for start in range(0, len(todo), local.CHUNK):
            chunk = todo[start:start + local.CHUNK]
            texts = {cid: text[:MAX_CHARS] for cid in chunk if (text := stored_text(websites[cid]))}
            require_store()
            answers = dict(zip(texts, pool.map(lambda cid: answer_one(http, texts[cid], asked), texts)))
            # A company without stored text is recorded too, with no answers, so it is not picked again every run.
            dropped += local._write(source_id, sync_run_id, asked, websites, {cid: {} for cid in chunk} | answers,
                                    model=model_version, asked_with=asked_with)
            written += len(texts)
            without_text += len(chunk) - len(texts)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            dropped += local._drop_stale(cur, source_id, list(FIELDS), websites)
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(todo), records_created=written,
                            records_updated=dropped, records_failed=without_text)
    finally:
        connection.close()
    log.info("Asked jeff about %d companies (%d without stored text); %d old guesses dropped.",
             written, without_text, dropped)
    return written + dropped


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Technologiefeld and WZ guesses from jeff (TSB's self-hosted jev API), shown with their confidence",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 11, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["classifier", "sources", "websites"],
    ) as dag:
        classify = pipeline_task("classify", run_classify, returns=True, secrets=("JEFF_API_KEY",))
        any_written = ShortCircuitOperator(
            task_id="any_written", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="classify")),
        )
        coordinate = TriggerDagRunOperator(task_id="trigger_pipeline_coordinator",
                                           trigger_dag_id="pipeline_coordinator", wait_for_completion=False)
        classify >> any_written >> coordinate
    return dag


dag = _make_dag()
