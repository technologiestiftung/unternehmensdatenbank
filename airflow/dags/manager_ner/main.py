"""Two DAGs that close the loop between the app's votes and the spaCy model for manager names.

manager_ner_extract  The current model reads the Impressum of companies that have a website and
                     have not been read yet, and writes each name as a `ceo_name_or_contact`
                     observation of the source `spacy_ner`. Then the coordinator runs.
manager_ner_train    Votes and corrections from the app (`extraction_feedback`) become
                     training examples. The current model is trained further, scored on a held-out
                     fifth of the companies against itself and the regex extractor, and promoted
                     only if it beats the current model.

Models live under MANAGER_NER_MODEL_DIR: one folder per trained version with its `metrics.json`,
the model files only for a promoted version, and `current.json` naming the version in use. Without
one, the base model (de_core_news_md) is used.
On the Data Hub that directory needs a persistent volume or S3; Airflow there has no shared disk.

Each company is read once, like every other source DAG here: `ceo_name_or_contact` has no upsert
key, so reading it again would append a second copy. Re-reading everyone with a better model needs
that key first (a migration plus `insert_text_observation`, same commit).
"""
from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from manager_ner.ner import (
    Labels,
    Score,
    entity_tags,
    extract_names,
    is_holdout,
    labels_from_votes,
    train,
    window_text,
)
from source_enrichment_common import (
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

SOURCE_KEY = "spacy_ner"
ENABLED_FLAG = "ENABLE_MANAGER_NER"
MODEL_DIR = Path(os.getenv("MANAGER_NER_MODEL_DIR", "/opt/airflow/data/models/manager_ner"))
BASE_MODEL = os.getenv("MANAGER_NER_BASE_MODEL", "de_core_news_md")
BATCH_SIZE = int(os.getenv("MANAGER_NER_BATCH_SIZE", "50"))
MIN_COMPANIES = int(os.getenv("MANAGER_NER_MIN_COMPANIES", "30"))
EPOCHS = int(os.getenv("MANAGER_NER_EPOCHS", "10"))
#: spaCy gives no calibrated score per entity. 0.6 clears the resolver's 0.5 floor and stays below
#: names with register or Impressum evidence from other sources.
CONFIDENCE = float(os.getenv("MANAGER_NER_CONFIDENCE", "0.6"))


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


def current_version() -> str | None:
    try:
        return json.loads((MODEL_DIR / "current.json").read_text())["version"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return None


def load_model(version: str | None) -> Any:
    import spacy

    return spacy.load(MODEL_DIR / version if version else BASE_MODEL)


def read_impressum(website: str) -> str:
    """The company's Impressum as plain text, or "" when none is found."""
    from enrichment.http_client import fetch_impressum
    from enrichment.imprint_extractor import page_text

    found = fetch_impressum(website)
    return page_text(found[0]) if found else ""


# --- extract --------------------------------------------------------------------------------


def _companies_to_read(cur: Any) -> list[tuple[str, str, str]]:
    cur.execute(
        """
        SELECT e.id, e.company_name, e.website_url
        FROM public.v_companies_enriched e
        WHERE e.website_url IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.company_source_records r
            JOIN public.sources s ON s.id = r.source_id
            WHERE r.company_id = e.id AND s.key = %s
          )
        ORDER BY e.id
        LIMIT %s
        """,
        (SOURCE_KEY, BATCH_SIZE),
    )
    return cur.fetchall()


def run_extract() -> None:
    _require_enabled()
    version = current_version()
    nlp = load_model(version)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur, key=SOURCE_KEY, name="spaCy manager names", source_type="ner_model",
                metadata={"dag_id": "manager_ner_extract"},
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = _companies_to_read(cur)

        seen = names_written = failed = 0
        for company_id, company_name, website in companies:
            seen += 1
            try:
                names = extract_names(nlp, window_text(read_impressum(website)))
            except Exception as exc:  # noqa: BLE001  one unreachable site must not stop the batch
                log.warning("Impressum not readable for %s: %s", company_id, type(exc).__name__)
                names, failed = [], failed + 1
            with connection, connection.cursor() as cur:
                # Written even with no names, so the company counts as read and is not fetched again.
                record_id, _ = upsert_source_record(
                    cur, company_id=company_id, source_id=source_id, external_id=f"{SOURCE_KEY}:{company_id}",
                    external_url=website, source_company_name=company_name,
                    source_payload={"model_version": version or BASE_MODEL, "names_found": len(names)},
                )
                for name in names:
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name="ceo_name_or_contact", value_text=name,
                        confidence=CONFIDENCE, metadata={"model_version": version or BASE_MODEL},
                    )
                    names_written += 1

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=seen, records_created=names_written,
                            records_updated=0, records_failed=failed)
        log.info("Read %d companies with model %s: %d names, %d unreadable.",
                 seen, version or BASE_MODEL, names_written, failed)
    finally:
        connection.close()


# --- train ----------------------------------------------------------------------------------


def _votes(cur: Any) -> dict[tuple[str, str], list[dict[str, Any]]]:
    cur.execute(
        """
        SELECT f.company_id::text, e.website_url, f.value, f.verdict, f.correction
        FROM public.extraction_feedback f
        JOIN public.v_companies_enriched e ON e.id = f.company_id
        WHERE f.field_name = 'management' AND e.website_url IS NOT NULL
        """
    )
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for company_id, website, value, verdict, correction in cur.fetchall():
        grouped[(company_id, website)].append({"value": value, "verdict": verdict, "correction": correction})
    return grouped


def _score(nlp: Any, held_out: list[tuple[str, Labels]]) -> Score:
    score = Score()
    for page, labels in held_out:
        score.add(extract_names(nlp, window_text(page)), labels)
    return score


def run_train() -> None:
    _require_enabled()
    from spacy.training import Example
    from enrichment.imprint_extractor import extract_ceo_names

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            votes = _votes(cur)
    finally:
        connection.close()

    version = current_version()
    nlp = load_model(version)
    training: list[Any] = []
    held_out: list[tuple[str, Labels]] = []
    for (company_id, website), rows in votes.items():
        labels = labels_from_votes(rows)
        if not labels:
            continue
        try:
            page = read_impressum(website)
        except Exception as exc:  # noqa: BLE001
            log.warning("Impressum not readable for %s: %s", company_id, type(exc).__name__)
            continue
        doc = nlp.make_doc(window_text(page))
        tags = entity_tags(doc, labels)
        if tags is None:
            continue  # the voted names are not on the page any more
        if is_holdout(company_id):
            held_out.append((page, labels))
        else:
            training.append(Example.from_dict(doc, {"entities": tags}))

    usable = len(training) + len(held_out)
    if usable < MIN_COMPANIES or not held_out:
        log.info("Only %d companies with usable votes (%d held out); %d needed. Not training.",
                 usable, len(held_out), MIN_COMPANIES)
        return

    regex = Score()
    for page, labels in held_out:
        regex.add(extract_ceo_names(page), labels)
    before = _score(nlp, held_out)
    losses = train(nlp, training, epochs=EPOCHS)
    after = _score(nlp, held_out)

    new_version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = MODEL_DIR / new_version
    out.mkdir(parents=True)  # spaCy's to_disk creates only the last folder, not MODEL_DIR
    promoted = after.f1 > before.f1
    if promoted:
        nlp.to_disk(out)
    metrics = {
        "version": new_version, "trained_from": version or BASE_MODEL,
        "companies_train": len(training), "companies_held_out": len(held_out), "losses": losses,
        "held_out": {"regex": regex.as_dict(), "current_model": before.as_dict(), "new_model": after.as_dict()},
        "promoted": promoted,
    }
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2))
    if promoted:
        (MODEL_DIR / "current.json").write_text(json.dumps({"version": new_version}, indent=2))
    log.info("Model %s: F1 %.3f -> %.3f (regex %.3f). %s", new_version, before.f1, after.f1, regex.f1,
             "Promoted." if promoted else "Kept the current model.")


# --- DAGs -----------------------------------------------------------------------------------


def _make_dags():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    except ImportError:
        return None, None

    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    with DAG(
        dag_id="manager_ner_extract",
        description="Manager names from Impressum pages with the current spaCy model",
        schedule=None, start_date=start, catchup=False, max_active_runs=1,
        tags=["sources", "spacy", "managers"],
    ) as extract:
        read = PythonOperator(task_id="extract_manager_names", python_callable=run_extract)
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator", wait_for_completion=False,
        )
        read >> coordinate

    with DAG(
        dag_id="manager_ner_train",
        description="Train the spaCy manager-name model on the app's votes and corrections",
        schedule="@weekly", start_date=start, catchup=False, max_active_runs=1,
        tags=["spacy", "training", "managers"],
    ) as training:
        PythonOperator(task_id="train_from_feedback", python_callable=run_train)

    return extract, training


extract_dag, train_dag = _make_dags()
