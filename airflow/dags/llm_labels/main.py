"""Technologiefeld teacher labels from the Claude API for companies the keywords left open (#87).

**Started by hand only**, never scheduled, until a hand-checked sample shows the labels are right
often enough (`scripts/llm_labels_sample.py`). What is sent and how: `enrichment.llm`.

A run:

1. **picks** up to `MAX_COMPANIES` companies whose published website the `technology_field` DAG read
   and where no keyword fired (`no_keyword`), that have stored text and no label from this model and
   prompt version yet. The order is fixed (a hash of the company id), so two models asked in turn get
   the same companies first.
2. **sends** them as one Message Batch and waits for it (most end within an hour, all within 24).
   The batch id is kept in `pipeline_state` (llm_labels / batch), so a run that fails while waiting
   picks up the same batch instead of paying for a new one.
3. **writes** one source record per company and model (`external_id` = model:company id): the label,
   the outcome, model, prompt version and tokens. No observation: a label never reaches the app.

**Budget.** At most `MAX_COMPANIES` per run, and no new batch once this month's tokens
(`pipeline_state` llm_labels / usage) reach `MONTHLY_TOKENS`. Set a spend limit on the key in the
Anthropic Console as well.

Needs `ANTHROPIC_API_KEY` (an Airflow variable, `pipeline_task`).
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

from psycopg2.extras import Json

from enrichment import branch_classifier as bc
from enrichment import llm
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    schedule_label,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "llm_labels"
SOURCE_KEY = "llm_technology_field"
SCHEDULE = None
DETAILS = SourceDetails(
    description=(
        "KI-Vorschläge (Claude API) für das Technologiefeld von Firmen, bei denen kein Stichwort passte. "
        "Nur zum Trainieren und Prüfen gespeichert, nie in der App angezeigt."
    ),
    schedule=schedule_label(SCHEDULE),
    base_url="https://www.anthropic.com/api",
    terms=(
        "Gesendet wird nur Text der Firmen-Website ohne Impressum; Namen, E-Mail-Adressen und "
        "Telefonnummern werden vorher ersetzt. Freigabe der IT-Compliance liegt vor (2026-11-04)."
    ),
)
MODEL = os.getenv("LLM_MODEL", "claude-opus-5-5")
MAX_COMPANIES = int(os.getenv("LLM_MAX_COMPANIES_PER_RUN", "500"))
MONTHLY_TOKENS = int(os.getenv("LLM_MONTHLY_TOKENS", "10000000"))
POLL_SECONDS = 60
MAX_WAIT_HOURS = 24
_BATCH = ("llm_labels", "batch")
_USAGE = ("llm_labels", "usage")


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="Technologiefeld-Vorschläge (Claude API)",
                         source_type="classifier", metadata={"dag_id": DAG_ID}, details=DETAILS)


def _state(cur: Any, key: tuple[str, str]) -> Any:
    cur.execute("SELECT value FROM public.pipeline_state WHERE pipeline = %s AND key = %s", key)
    row = cur.fetchone()
    return row[0] if row else None


def _save_state(cur: Any, key: tuple[str, str], value: Any) -> None:
    cur.execute(
        """
        INSERT INTO public.pipeline_state (pipeline, key, value) VALUES (%s, %s, %s)
        ON CONFLICT (pipeline, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
        """,
        (*key, Json(value)),
    )


def tokens_this_month(usage: dict[str, Any] | None, month: str) -> int:
    return int(usage["tokens"]) if usage and usage.get("month") == month else 0


def pick(gaps: dict[str, tuple[str, str]], done: set[str], limit: int,
         text_of: Callable[[str], str]) -> dict[str, str]:
    """company_id -> text for up to `limit` gap companies not labelled yet that have text, in a fixed
    order (a hash of the id). `text_of` gets the website; a company without text is skipped, not counted."""
    todo = sorted((cid for cid in gaps if cid not in done), key=lambda cid: hashlib.sha256(cid.encode()).hexdigest())
    chosen: dict[str, str] = {}
    for cid in todo:
        if len(chosen) == limit:
            break
        if text := text_of(gaps[cid][1]):
            chosen[cid] = text
    return chosen


def _gaps(cur: Any, taxonomy: bc.Taxonomy) -> dict[str, tuple[str, str]]:
    """company_id -> (name, website) for every company the keywords left open on its published site."""
    # Imported here: importing `technology_field.main` builds that DAG (see `technology_field.train`).
    from technology_field.main import _source_id as keyword_source, keyword_answers, read_records

    records = read_records(cur, keyword_source(cur))
    answers = keyword_answers(records, taxonomy)
    websites = {cid: website for cid, *_, still_published, website in records if still_published}
    gap_ids = [cid for cid, a in answers.items() if a.outcome == "no_keyword" and cid in websites]
    cur.execute("SELECT id::text, name FROM public.company_profile_mv WHERE id = ANY(%s::uuid[])", (gap_ids,))
    return {cid: (name, websites[cid]) for cid, name in cur.fetchall()}


def _labelled(cur: Any, source_id: str, version: str) -> set[str]:
    cur.execute(
        """
        SELECT company_id::text FROM public.company_source_records
        WHERE source_id = %s AND source_payload ->> 'model' = %s AND source_payload ->> 'prompt_version' = %s
        """,
        (source_id, MODEL, version),
    )
    return {row[0] for row in cur.fetchall()}


def _texts(gaps: dict[str, tuple[str, str]], done: set[str]) -> dict[str, str]:
    """company_id -> redacted text, for up to `MAX_COMPANIES` unlabelled gap companies with stored text."""
    import spacy

    from technology_field.train import require_store, stored_text

    nlp = spacy.load("de_core_news_md", disable=["parser", "lemmatizer"])

    def people(text: str) -> list[tuple[int, int]]:
        return [(e.start_char, e.end_char) for e in nlp(text).ents if e.label_ == "PER"]

    chosen = pick(gaps, done, MAX_COMPANIES, stored_text)
    require_store()
    texts = {cid: llm.redact(text, people) for cid, text in chosen.items()}
    return texts


def _wait(batch_id: str) -> None:
    deadline = time.monotonic() + MAX_WAIT_HOURS * 3600
    while not llm.ended(batch_id):
        if time.monotonic() > deadline:
            raise TimeoutError(f"Batch {batch_id} has not ended after {MAX_WAIT_HOURS} h; run again to collect it.")
        time.sleep(POLL_SECONDS)


def run_label() -> int:
    """Label up to `MAX_COMPANIES` gap companies. Returns how many got a label (NONE included)."""
    taxonomy = bc.load_taxonomy()
    system = llm.system_prompt(taxonomy)
    version, choices = llm.prompt_version(system), llm.answers(taxonomy)
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            pending = _state(cur, _BATCH)
            used = tokens_this_month(_state(cur, _USAGE), month)
            gaps = _gaps(cur, taxonomy)
            done = _labelled(cur, source_id, version)

        if pending:
            log.info("Collecting batch %s from an earlier run.", pending["batch_id"])
        elif used >= MONTHLY_TOKENS:
            log.info("%d tokens used this month, budget %d; no new batch.", used, MONTHLY_TOKENS)
            return 0
        else:
            texts = _texts(gaps, done)
            if not texts:
                log.info("%d gap companies, %d labelled; nothing left with stored text.", len(gaps), len(done))
                return 0
            requests = [llm.request(cid, text, model=MODEL, system=system, choices=choices)
                        for cid, text in sorted(texts.items())]
            pending = {"batch_id": llm.submit(requests), "model": MODEL, "prompt_version": version,
                       "taxonomy_version": taxonomy.version, "companies": len(requests)}
            with connection, connection.cursor() as cur:
                _save_state(cur, _BATCH, pending)
            log.info("Sent batch %s: %d companies to %s.", pending["batch_id"], len(requests), MODEL)

        _wait(pending["batch_id"])
        outcomes: dict[str, int] = {}
        tokens = 0
        with connection, connection.cursor() as cur:
            sync_run_id = start_sync_run(cur, source_id=source_id)
            for cid, label, outcome, used_tokens in llm.results(pending["batch_id"], choices):
                outcomes[outcome] = outcomes.get(outcome, 0) + 1
                tokens += used_tokens
                if cid not in gaps or outcome in ("errored", "expired"):
                    continue  # sent again by the next run
                name, website = gaps[cid]
                upsert_source_record(
                    cur, company_id=cid, source_id=source_id, external_id=f"{pending['model']}:{cid}",
                    external_url=website, source_company_name=name,
                    source_payload={"label": label, "outcome": outcome, "provider": llm.PROVIDER,
                                    "model": pending["model"], "prompt_version": pending["prompt_version"],
                                    "taxonomy_version": pending["taxonomy_version"],
                                    "batch_id": pending["batch_id"], "tokens": used_tokens, "ai_generated": True},
                )
            _save_state(cur, _USAGE, {"month": month, "tokens": used + tokens})
            cur.execute("DELETE FROM public.pipeline_state WHERE pipeline = %s AND key = %s", _BATCH)
            labelled = outcomes.get("labelled", 0)
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=sum(outcomes.values()),
                            records_created=labelled, records_updated=0,
                            records_failed=sum(outcomes.values()) - labelled)
        log.info("Batch %s: %s; %d tokens (%d this month).", pending["batch_id"], outcomes, tokens, used + tokens)
        return labelled
    finally:
        connection.close()


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="By hand only: Technologiefeld teacher labels from the Claude API, never shown in the app",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["classifier", "llm"],
    ) as dag:
        pipeline_task("label", run_label, returns=True, secrets=("ANTHROPIC_API_KEY",))
    return dag


dag = _make_dag()
