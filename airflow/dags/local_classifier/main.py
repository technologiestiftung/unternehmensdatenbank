"""Technologiefeld, WZ, DeepTech and KI for every company with a website, from a local model (#183).

GLiNER2.5-multi-Decide (`enrichment.decide`) reads the text the `technology_field` DAG read off the company's
own published website (services pages, then the homepage; corpus, S3) and answers four questions:

| field | labels |
|---|---|
| `technology_field` | Berlin's 77 Technologiefelder (stored in English, `name_en`; asked in German) |
| `wz_division_estimated` | the 87 WZ 2025 divisions (stored as two digits, "62"); never the official `wz_code` |
| `deeptech_class` | `non-tech`, `tech`, `hybrid`, `deep tech` |
| `is_ai` | `true` / `false` |

**A guess, shown as one** (decided 2026-11-09): every answer is written with the model's confidence times
`decide.CONFIDENCE_SCALE` (at most 0.50), so the keywords (0.60), the Wirtschaftsatlas layers (0.56) and
Claude (0.55) win wherever they have an answer, and people's corrections (priority 50) always do. The
coordinator shows any guess above 0 for these fields; the app colours it by confidence. There is no "none"
label for the Technologiefeld and the WZ: a guess is better than an empty field.

**Cost.** About 2.4 s per question on a laptop CPU (#182). Each company's answers are cached in its source
record with the model version and the website, so a company is asked again only for a new model or a new
website; at most `MAX_COMPANIES` per run, never asked ones first.

**Local only:** `enrichment.decide` loads baked-in weights offline; no text leaves the pod.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Mapping

from enrichment import decide
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    replace_guess,
    schedule_label,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "local_classifier"
SOURCE_KEY = "local_classifier"
SCHEDULE = "@daily"
DETAILS = SourceDetails(
    description=(
        "Vermutungen eines lokalen KI-Modells (GLiNER2.5-multi-Decide) aus dem Text der Firmen-Website: "
        "Technologiefeld, WZ-Abteilung, DeepTech-Klasse und ob es ein KI-Unternehmen ist. Läuft auf unseren "
        "Rechnern, kein Text verlässt sie. Immer unter Stichwörtern, Claude und Korrekturen."
    ),
    schedule=schedule_label(SCHEDULE),
    base_url="https://huggingface.co/fastino/GLiNER2.5-multi-Decide",
    terms="Apache-2.0; lokal ausgeführt, ohne Fastinos Cloud-Dienst.",
)
MAX_COMPANIES = int(os.getenv("LOCAL_CLASSIFIER_MAX_COMPANIES", "300"))
#: The start of the services pages and homepage, where a company says what it does. Memory and time grow
#: steeply with length (`enrichment.decide.BATCH_SIZE`): 2,500 characters is about 2 s per question.
MAX_CHARS = int(os.getenv("LOCAL_CLASSIFIER_MAX_CHARS", "2500"))

#: Label shown to the model -> value stored. The plain class names: longer German descriptions made the model
#: follow the wording, not the text (2026-11-09, 183 companies: with eight research fields named, 27 of the first
#: 30 came out "deep tech"; plain names gave 57 of the 83 companies Claude saw no Technologiefeld in "non-tech",
#: none "deep tech").
DEEPTECH = {label: label for label in ("non-tech", "tech", "hybrid", "deep tech")}
IS_AI = {
    "KI-Unternehmen: entwickelt künstliche Intelligenz oder bietet KI-Produkte an": "true",
    "kein KI-Unternehmen": "false",
}


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Local classifier (GLiNER2.5-multi-Decide)",
                         source_type="classifier", metadata={"dag_id": DAG_ID, "model": decide.MODEL_VERSION},
                         details=DETAILS)


def questions(cur: Any) -> dict[str, dict[str, str]]:
    """Per field: label shown to the model -> value stored."""
    cur.execute("SELECT name, name_en FROM public.technology_fields ORDER BY name")
    fields = dict(cur.fetchall())
    cur.execute("SELECT division, title FROM public.wz_divisions ORDER BY division")
    divisions = {f"{division} {title}": division for division, title in cur.fetchall()}
    return {"technology_field": fields, "wz_division_estimated": divisions, "deeptech_class": DEEPTECH, "is_ai": IS_AI}


def to_ask(websites: Mapping[str, str], cached: Mapping[str, Mapping[str, Any]], limit: int,
           asked_with: str | None = None) -> list[str]:
    """Companies whose answers are missing or stale (another model, another website): never asked first,
    then the rest, each in id order; at most `limit`. `asked_with` defaults to this model's `ASKED_WITH`."""
    asked_with = asked_with or ASKED_WITH
    stale = [cid for cid, website in websites.items()
             if cached.get(cid, {}).get("model") != asked_with or cached[cid].get("website") != website]
    return sorted(stale, key=lambda cid: (cid in cached, cid))[:limit]


def answer(texts: Mapping[str, str], asked: Mapping[str, Mapping[str, str]]) -> dict[str, dict[str, tuple[str, float]]]:
    """company_id -> field -> (stored value, the model's own confidence)."""
    ids = sorted(texts)
    answers: dict[str, dict[str, tuple[str, float]]] = {cid: {} for cid in ids}
    for field, labels in asked.items():
        for cid, (label, confidence) in zip(ids, decide.classify([texts[cid] for cid in ids], labels, field)):
            if label in labels:
                answers[cid][field] = (labels[label], confidence)
    return answers


def _websites(cur: Any) -> dict[str, str]:
    """company_id -> website, for every company the `technology_field` DAG read text from on its published site."""
    from technology_field.main import _source_id as keyword_source, read_records

    return {cid: website for cid, _, _, _, had_text, still_published, website in read_records(cur, keyword_source(cur))
            if had_text and still_published}


def _cached(cur: Any, source_id: str) -> dict[str, dict[str, Any]]:
    cur.execute("SELECT company_id::text, source_payload FROM public.company_source_records WHERE source_id = %s",
                (source_id,))
    return dict(cur.fetchall())


#: What a cached answer was made with: the model and the questions. Change a label's wording, raise the number.
ASKED_WITH = f"{decide.MODEL_VERSION}/questions-2"
#: Companies asked and written together: a pod that dies keeps what it wrote, and no connection waits idle
#: through minutes of model work (one did, and was closed under memory pressure, 2026-11-09).
CHUNK = 10


def _write(source_id: str, sync_run_id: str, asked: Mapping[str, Mapping[str, str]], websites: Mapping[str, str],
           answers: Mapping[str, Mapping[str, tuple[str, float]]], *, model: str = decide.MODEL_VERSION,
           asked_with: str = ASKED_WITH) -> int:
    """One chunk's answers, on a fresh connection. Returns how many earlier guesses were dropped. `jeff`
    writes through here too, with its own `model` and `asked_with`."""
    dropped = 0
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            for cid, fields in answers.items():
                record_id, _ = upsert_source_record(
                    cur, company_id=cid, source_id=source_id, external_id=cid, external_url=websites[cid],
                    source_company_name=None,
                    source_payload={"model": asked_with, "website": websites[cid],
                                    "answers": {field: [value, round(conf, 4)] for field, (value, conf) in fields.items()}},
                )
                for field in asked:
                    value, conf = fields.get(field, (None, 0.0))
                    dropped += replace_guess(
                        cur, company_id=cid, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=field, value_text=value,
                        confidence=decide.stored_confidence(conf),
                        metadata={"model": model, "model_confidence": round(conf, 4),
                                  "ai_generated": True})
    finally:
        connection.close()
    return dropped


def run_classify() -> int:
    """Ask the model about up to `MAX_COMPANIES` companies, write its guesses and drop those of companies
    whose website is no longer the one they were read off. Returns companies answered plus guesses dropped
    (0: nothing changed)."""
    from enrichment import page_cache
    from technology_field.train import require_store, stored_text

    if not page_cache.enabled():
        log.info("No page store configured: no website text to read, nothing asked or dropped.")
        return 0
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            asked = questions(cur)
            websites = _websites(cur)
            todo = to_ask(websites, _cached(cur, source_id), MAX_COMPANIES)
            sync_run_id = start_sync_run(cur, source_id=source_id)
    finally:
        connection.close()
    written = without_text = dropped = 0
    for start in range(0, len(todo), CHUNK):
        chunk = todo[start:start + CHUNK]
        texts = {cid: text[:MAX_CHARS] for cid in chunk if (text := stored_text(websites[cid]))}
        require_store()
        # A company without stored text is recorded too, with no answers, so it is not picked again every run.
        dropped += _write(source_id, sync_run_id, asked, websites, {cid: {} for cid in chunk} | answer(texts, asked))
        written += len(texts)
        without_text += len(chunk) - len(texts)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            dropped += _drop_stale(cur, source_id, list(asked), websites)
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(todo), records_created=written,
                            records_updated=dropped, records_failed=without_text)
    finally:
        connection.close()
    log.info("Asked the local classifier about %d companies (%d without stored text); %d old guesses dropped.",
             written, without_text, dropped)
    return written + dropped


#: This source's records whose website is not the company's current one (published, with text).
_STALE = """
    SELECT r.company_id FROM public.company_source_records r
    LEFT JOIN unnest(%s::uuid[], %s::text[]) AS c(company_id, website)
      ON c.company_id = r.company_id AND c.website = r.external_url
    WHERE r.source_id = %s AND c.company_id IS NULL
"""


def _drop_stale(cur: Any, source_id: str, fields: list[str], websites: Mapping[str, str]) -> int:
    """Guesses at 0 for every company whose website is gone or another than the one it was read off, and its
    cached answers marked stale, so it is asked again. Returns how many guesses were dropped."""
    ids = sorted(websites)
    current = (ids, [websites[cid] for cid in ids], source_id)
    cur.execute(
        f"""
        UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
        WHERE source_id = %s AND field_name = ANY(%s) AND confidence > 0 AND company_id IN ({_STALE})
        """,
        (source_id, fields, *current),
    )
    dropped = cur.rowcount
    cur.execute(
        f"""
        UPDATE public.company_source_records SET source_payload = source_payload - 'model'
        WHERE source_id = %s AND source_payload ? 'model' AND company_id IN ({_STALE})
        """,
        (source_id, *current),
    )
    return dropped


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
        description="Technologiefeld, WZ, DeepTech and KI guesses from a local model, shown with their confidence",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 9, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["classifier", "sources", "websites"],
    ) as dag:
        # The model needs about 2.1 GB (`enrichment.decide.BATCH_SIZE`); a namespace default would kill it.
        classify = pipeline_task("classify", run_classify, returns=True, memory="3Gi")
        any_written = ShortCircuitOperator(
            task_id="any_written", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="classify")),
        )
        coordinate = TriggerDagRunOperator(task_id="trigger_pipeline_coordinator",
                                           trigger_dag_id="pipeline_coordinator", wait_for_completion=False)
        classify >> any_written >> coordinate
    return dag


dag = _make_dag()
