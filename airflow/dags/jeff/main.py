"""jeff: Technologiefeld and WZ guesses from TSB's self-hosted jev API, next to `local_classifier` (#194).

[jeff](https://github.com/technologiestiftung/jeff) (`enrichment.jeff`) reads the same text `local_classifier`
reads, the website text the `technology_field` DAG read off the company's own published site, and answers two
questions:

| field | labels |
|---|---|
| `technology_field` | Berlin's 77 Technologiefelder (stored in English, `name_en`; asked in German) |
| `wz_division_estimated` | the 87 WZ 2025 divisions (stored as two digits, "62"); never the official `wz_code` |

**The WZ division in two steps** (#198): first the 22 sections ("J Information und Kommunikation"), then only
that section's divisions. Two small questions instead of 87 labels in parts and a final round; the stored
confidence is the section's times the division's, because both have to be right. A section with one division
needs no second question.

**Measuring** (form field `companies`): `next` (the schedule) asks the next companies never asked; `claude`
asks only companies Claude labelled (`llm_labels`), so jeff's agreement with Claude, logged at the end of every
run (`agreement`), rests on more than the few companies both happen to share. Agreement, not correctness: the
precision is a Prüfen round.

**Its own source, ranked like the local model** (decided 2026-11-11): jeff's confidence times
`decide.CONFIDENCE_SCALE`, so at most 0.50. The keywords (0.60), the Wirtschaftsatlas layers (0.56), Claude
(0.55) and spaCy (0.52-0.55) win wherever they have an answer, and people's corrections (priority 50) always
do. Next to `local_classifier`, never instead of it, so a Prüfen round can tell which of the two is right
more often. Nothing here has been measured.

**Text leaves the pod, to TSB's own server** (`enrichment.jeff`), whole and unredacted (decided 2026-11-11).
Needs `JEFF_API_KEY`, an Airflow variable (Admin → Variables; `pipeline_task`, "API keys").

**Cost.** Three requests per company: the 77 Technologiefelder in one question (jeff takes up to
`JEFF_MAX_LABELS`, 128 on TSB's server since 2026-11-19; with jeff's default 64 it is two parts and a final
round, `jeff.choose`), the WZ in two. On play, 2026-11-19, six requests took about 25 s per company. The 87 WZ
divisions would now fit one question too; the two steps stay until a measurement shows which agrees better. Answers are cached in each company's source record
with the model, the questions, the label limit (a new `JEFF_MAX_LABELS` asks everyone again) and the website, as
in `local_classifier`; at most `MAX_COMPANIES` per run, never asked first.
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
WZ_SECTION_INSTRUCTIONS = "Zu welchem Abschnitt der Wirtschaftszweige (WZ 2025) gehört diese Firma?"
#: Part of each cached answer's key, after the model jeff serves. Change a question's wording, raise the number.
QUESTIONS = "questions-2"
#: Which companies a run asks (form field `companies`, `JEFF_COMPANIES`).
COMPANIES = ("next", "claude")


def _source_id(cur: Any, model_version: str) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="jeff (GLiFormer, self-hosted)", source_type="classifier",
                         metadata={"dag_id": DAG_ID, "model": model_version, "url": jeff.BASE_URL}, details=DETAILS)


WzTree = tuple[dict[str, str], dict[str, dict[str, str]]]


def wz_tree(cur: Any) -> WzTree:
    """(section label -> section letter, section letter -> {division label -> division}); the division labels
    are `local_classifier.questions`' own, so an answer maps to the same stored value."""
    cur.execute("SELECT division, title, section, section_title FROM public.wz_divisions ORDER BY section, division")
    sections: dict[str, str] = {}
    divisions: dict[str, dict[str, str]] = {}
    for division, title, section, section_title in cur.fetchall():
        sections[f"{section} {section_title}"] = section
        divisions.setdefault(section, {})[f"{division} {title}"] = division
    return sections, divisions


def choose_wz(http: Any, text: str, tree: WzTree) -> tuple[str | None, float]:
    """(division label, confidence): the section first, then its divisions; the confidence is both multiplied."""
    sections, divisions = tree
    section_label, section_confidence = jeff.choose(http, text, WZ_SECTION_INSTRUCTIONS, list(sections))
    if section_label not in sections:
        return None, 0.0
    labels = list(divisions[sections[section_label]])
    if len(labels) == 1:
        return labels[0], section_confidence
    label, confidence = jeff.choose(http, text, INSTRUCTIONS["wz_division_estimated"], labels)
    return label, section_confidence * confidence


def answer_one(http: Any, text: str, asked: Mapping[str, Mapping[str, str]],
               wz: WzTree) -> dict[str, tuple[str, float]]:
    """field -> (stored value, jeff's own confidence); a field jeff gave no known label for is left out. The WZ
    division is asked in two steps (`choose_wz`)."""
    answers = {}
    for field, labels in asked.items():
        if field == "wz_division_estimated":
            label, confidence = choose_wz(http, text, wz)
        else:
            label, confidence = jeff.choose(http, text, INSTRUCTIONS[field], list(labels))
        if label in labels:
            answers[field] = (labels[label], confidence)
    return answers


def claude_labelled(cur: Any) -> set[str]:
    """Companies Claude has a live label for."""
    from llm_labels.main import SOURCE_KEY as CLAUDE

    cur.execute(
        """
        SELECT DISTINCT o.company_id::text FROM public.company_field_observations o
        JOIN public.sources s ON s.id = o.source_id
        WHERE s.key = %s AND o.field_name = ANY(%s) AND o.confidence > 0
        """,
        (CLAUDE, list(FIELDS)),
    )
    return {row[0] for row in cur.fetchall()}


def agreement(cur: Any, source_id: str) -> dict[str, tuple[int, int]]:
    """field -> (companies jeff and Claude both answered, of them the same answer)."""
    from llm_labels.main import SOURCE_KEY as CLAUDE

    cur.execute(
        """
        SELECT j.field_name, count(*), count(*) FILTER (WHERE j.value_text = c.value_text)
        FROM public.company_field_observations j
        JOIN public.company_field_observations c ON c.company_id = j.company_id AND c.field_name = j.field_name
        JOIN public.sources cs ON cs.id = c.source_id
        WHERE j.source_id = %s AND cs.key = %s AND j.field_name = ANY(%s) AND j.confidence > 0 AND c.confidence > 0
        GROUP BY j.field_name
        """,
        (source_id, CLAUDE, list(FIELDS)),
    )
    return {field: (both, same) for field, both, same in cur.fetchall()}


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
    asked_with = f"{model_version}/{QUESTIONS}/labels-{jeff.MAX_LABELS}"
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur, model_version)
            asked = {field: labels for field, labels in local.questions(cur).items() if field in FIELDS}
            tree = wz_tree(cur)
            websites = local._websites(cur)
            # Only who is asked narrows; `websites` stays whole, or `_drop_stale` would zero everybody else's guesses.
            candidates = websites
            if companies() == "claude":
                labelled = claude_labelled(cur)
                candidates = {cid: website for cid, website in websites.items() if cid in labelled}
            todo = local.to_ask(candidates, local._cached(cur, source_id), MAX_COMPANIES, asked_with=asked_with)
            sync_run_id = start_sync_run(cur, source_id=source_id)
    finally:
        connection.close()
    written = without_text = dropped = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for start in range(0, len(todo), local.CHUNK):
            chunk = todo[start:start + local.CHUNK]
            texts = {cid: text[:MAX_CHARS] for cid in chunk if (text := stored_text(websites[cid]))}
            require_store()
            answers = dict(zip(texts, pool.map(lambda cid: answer_one(http, texts[cid], asked, tree), texts)))
            # A company without stored text is recorded too, with no answers, so it is not picked again every run.
            dropped += local._write(source_id, sync_run_id, asked, websites, {cid: {} for cid in chunk} | answers,
                                    model=model_version, asked_with=asked_with)
            written += len(texts)
            without_text += len(chunk) - len(texts)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            dropped += local._drop_stale(cur, source_id, list(FIELDS), websites)
            for field, (both, same) in sorted(agreement(cur, source_id).items()):
                log.info("Agreement with Claude, %s: %d of %d companies (%.0f%%).", field, same, both,
                         100 * same / both if both else 0)
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(todo), records_created=written,
                            records_updated=dropped, records_failed=without_text)
    finally:
        connection.close()
    log.info("Asked jeff about %d companies (%d without stored text); %d old guesses dropped.",
             written, without_text, dropped)
    return written + dropped


def companies() -> str:
    """The form field `companies`, `next` when unset or unknown (a scheduled run)."""
    value = (os.getenv("JEFF_COMPANIES") or "").strip()
    return value if value in COMPANIES else "next"


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import ShortCircuitOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from airflow.sdk import Param
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Technologiefeld and WZ guesses from jeff (TSB's self-hosted jev API), shown with their confidence",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 11, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["classifier", "sources", "websites"],
        params={"companies": Param("next", enum=list(COMPANIES),
                                   description="next: never asked first; claude: only companies Claude labelled")},
    ) as dag:
        classify = pipeline_task("classify", run_classify, returns=True, secrets=("JEFF_API_KEY",),
                                 env={"JEFF_COMPANIES": "{{ params.companies }}"})
        any_written = ShortCircuitOperator(
            task_id="any_written", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="classify")),
        )
        coordinate = TriggerDagRunOperator(task_id="trigger_pipeline_coordinator",
                                           trigger_dag_id="pipeline_coordinator", wait_for_completion=False)
        classify >> any_written >> coordinate
    return dag


dag = _make_dag()
