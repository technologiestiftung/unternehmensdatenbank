"""WZ (geschätzt): a WZ division per company, from our own text classifier (#158, second step of #140).

**An estimate, never the official `wz_code`.** The field is `wz_division_estimated` (two digits,
"62"), its source `wz_textcat`; the app labels it "WZ (geschätzt)". The official code, where a source
ever gives one, stays in `wz_code`. Not the Technologiefeld either: that one comes from keywords and
answers a different question (CLAUDE.md, three columns).

Weekly, two steps, then `pipeline_coordinator` publishes:

1. **train.** A spaCy text classifier over the WZ divisions (`branch_textcat.train`, the same code
   as the Technologiefeld's) on the website text the `technology_field` DAG read (corpus, S3).
   - **Labels:** Claude's estimate (`llm_labels`, `source_payload.wz_division`), overridden per
     company by people's votes on `wz_division_estimated` in the app (`branch_textcat.human_label`:
     a correction replaces the label, a majority "wrong" removes it).
   - **Judged on a held-out fifth** (`manager_ner.ner.is_holdout`), never trained on. Who judges
     (`judge`): **people** once at least `MIN_HOLDOUT` held-out companies have a person's label;
     before that **Claude's labels** (decided 2026-11-05, #158). A model judged by Claude measured
     agreement with Claude, not correctness (CLAUDE.md, rule 7): `metrics.json` says which
     (`judged_by`).
   - **Promoted** with at least `MIN_HOLDOUT` judged companies, accuracy at least `MIN_ACCURACY`,
     and better than the model in use, judged the same way.
2. **classify.** Every company whose published website the `technology_field` DAG read gets the
   promoted model's division, confidence = the model's probability (the coordinator shows it from
   0.50). The prediction is cached in this source's record with the model version, so a company is
   read again only for a new model. A company whose prediction changed keeps its old row at
   confidence 0: never deleted. No promoted model, nothing written.

Not hand-checked: no precision is known (#158). Labels come only from companies Claude was asked
about, so far those the keywords left open (`llm_labels`).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from enrichment import branch_textcat as textcat
from enrichment import model_store, wz_cluster
from manager_ner.ner import is_holdout
from source_enrichment_common import (
    SourceDetails,
    ensure_source,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    schedule_label,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "wz_estimate"
SOURCE_KEY = "wz_textcat"
FIELD = "wz_division_estimated"
MODEL_NAME = "wz_textcat"
SCHEDULE = "@weekly"
DETAILS = SourceDetails(
    description=(
        "Geschätzte WZ-Abteilung aus dem Text der Firmen-Website, von einem eigenen Textmodell, das an "
        "KI-Vorschlägen (Claude) gelernt hat und durch Korrekturen in der App besser wird. Nie die "
        "amtliche WZ-Nummer."
    ),
    schedule=schedule_label(SCHEDULE),
)
LLM_SOURCE_KEY = "llm_technology_field"
MODEL_DIR = Path(os.getenv("WZ_TEXTCAT_MODEL_DIR", "/opt/airflow/data/models/wz_textcat"))
EPOCHS = int(os.getenv("WZ_TEXTCAT_EPOCHS", "10"))
#: Below this many labelled companies there is nothing to learn 87 divisions from.
MIN_EXAMPLES = int(os.getenv("WZ_TEXTCAT_MIN_EXAMPLES", "100"))
#: Promotion bars. A policy, not a measurement: no model has been judged yet.
MIN_HOLDOUT = int(os.getenv("WZ_TEXTCAT_MIN_HOLDOUT", "30"))
MIN_ACCURACY = float(os.getenv("WZ_TEXTCAT_MIN_ACCURACY", "0.7"))


def _source_id(cur: Any) -> str:
    return ensure_source(cur, key=SOURCE_KEY, name="WZ-Schätzung (Textmodell)", source_type="classifier",
                         metadata={"dag_id": DAG_ID}, details=DETAILS)


# --- rules (no I/O) -----------------------------------------------------------------------------


def labels(llm: Mapping[str, str | None], votes: Mapping[str, list[Mapping[str, Any]]],
           divisions: set[str]) -> tuple[dict[str, str], set[str]]:
    """(company_id -> division, the companies whose label came from a person).

    A person's label replaces Claude's; Claude's is dropped when people called exactly it wrong."""
    result: dict[str, str] = {}
    human: set[str] = set()
    for cid in sorted(set(llm) | set(votes)):
        person, rejected = textcat.human_label(votes.get(cid, []), divisions)
        if person:
            result[cid] = person
            human.add(cid)
        elif (claude := llm.get(cid)) in divisions and claude not in rejected:
            result[cid] = claude
    return result, human


def judge(predict: Callable[[str], str | None], held_out: Iterable[str], label_of: Mapping[str, str],
          votes: Mapping[str, list[Mapping[str, Any]]], human: set[str], divisions: set[str]) -> dict[str, Any]:
    """{accuracy, judged, judged_by} over the held-out companies. People judge once `MIN_HOLDOUT` of
    them have a person's label; until then Claude's labels do. Decided by the data, not the model, so
    a new and the current model are always judged the same way."""
    held_out = sorted(held_out)
    if len([c for c in held_out if c in human]) >= MIN_HOLDOUT:
        accuracy, judged = textcat.gap_accuracy(((predict(c), votes.get(c, [])) for c in held_out if c in votes),
                                                divisions)
        return {"accuracy": accuracy, "judged": judged, "judged_by": "people"}
    accuracy, judged = textcat.accuracy((predict(c), label_of[c]) for c in held_out if c in label_of)
    return {"accuracy": accuracy, "judged": judged, "judged_by": "llm"}


def should_promote(new: Mapping[str, Any], current: Mapping[str, Any] | None) -> bool:
    return new["judged"] >= MIN_HOLDOUT and new["accuracy"] >= MIN_ACCURACY and (
        current is None or new["accuracy"] > current["accuracy"])


# --- train ----------------------------------------------------------------------------------------


def load_current() -> tuple[str | None, Any]:
    """(version, model) in use, or (None, None) while nothing was promoted."""
    version = model_store.current_version(MODEL_NAME, MODEL_DIR)
    if not version:
        return None, None
    import spacy

    return version, spacy.load(model_store.local_copy(MODEL_NAME, version, MODEL_DIR))


def _published_websites(cur: Any) -> dict[str, tuple[str, Any]]:
    """company_id -> (website, read at) for every company whose published website was read."""
    # Imported here: importing `technology_field.main` builds that DAG (tests/unit/test_dag_files.py).
    from technology_field.main import _source_id as keyword_source, read_records

    return {cid: (website, read_at) for cid, _, read_at, *_, still_published, website
            in read_records(cur, keyword_source(cur)) if still_published}


def _llm_divisions(cur: Any) -> dict[str, str | None]:
    """company_id -> Claude's newest WZ division, from `llm_labels`' source records."""
    cur.execute(
        """
        SELECT DISTINCT ON (r.company_id) r.company_id::text, r.source_payload ->> 'wz_division'
        FROM public.company_source_records r JOIN public.sources s ON s.id = r.source_id
        WHERE s.key = %s
        ORDER BY r.company_id, r.last_synced_at DESC
        """,
        (LLM_SOURCE_KEY,),
    )
    return dict(cur.fetchall())


def _votes(cur: Any) -> dict[str, list[dict[str, Any]]]:
    cur.execute("SELECT company_id::text, value, verdict, correction FROM public.extraction_feedback "
                "WHERE field_name = %s", (FIELD,))
    grouped: dict[str, list[dict[str, Any]]] = {}
    for company_id, value, verdict, correction in cur.fetchall():
        grouped.setdefault(company_id, []).append({"value": value, "verdict": verdict, "correction": correction})
    return grouped


def run_train() -> None:
    from technology_field.train import require_store, stored_text

    divisions = set(wz_cluster.load_clusters().division_titles)
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            websites = _published_websites(cur)
            llm, votes = _llm_divisions(cur), _votes(cur)
    finally:
        connection.close()

    label_of, human = labels({c: d for c, d in llm.items() if c in websites},
                             {c: v for c, v in votes.items() if c in websites}, divisions)
    texts = {cid: text for cid in sorted(label_of.keys() | votes.keys() & websites.keys())
             if (text := stored_text(websites[cid][0]))}
    require_store()
    label_of = {cid: label for cid, label in label_of.items() if cid in texts}
    held_out = {cid for cid in texts if is_holdout(cid)}
    examples = [(texts[cid], label) for cid, label in sorted(label_of.items()) if cid not in held_out]
    if len(examples) < MIN_EXAMPLES:
        log.info("Only %d labelled companies to train on; %d needed. Not training.", len(examples), MIN_EXAMPLES)
        return

    nlp, losses = textcat.train(examples, sorted(divisions), epochs=EPOCHS)
    in_use, current_model = load_current()

    def judged(model: Any) -> dict[str, Any]:
        return judge(lambda c: textcat.predict(model, texts[c])[0], held_out, label_of, votes, human, divisions)

    new = judged(nlp)
    current = judged(current_model) if current_model is not None else None
    promoted = should_promote(new, current)

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = MODEL_DIR / version
    out.mkdir(parents=True)
    if promoted:
        nlp.to_disk(out)
    metrics = {
        "version": version, "companies_train": len(examples), "labels_from_people": len(human),
        "losses": losses, "held_out": {"new_model": new, "current_model": {"version": in_use, **current}
                                       if current else None},
        "bars": {"min_judged": MIN_HOLDOUT, "min_accuracy": MIN_ACCURACY}, "promoted": promoted,
    }
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2))
    if promoted:
        (MODEL_DIR / "current.json").write_text(json.dumps({"version": version}, indent=2))
    model_store.publish(MODEL_NAME, version, MODEL_DIR, metrics, promoted=promoted)
    log.info("Model %s: %d companies trained; judged by %s on %d held out, accuracy %.3f (bars %d, %.2f). %s",
             version, len(examples), new["judged_by"], new["judged"], new["accuracy"], MIN_HOLDOUT,
             MIN_ACCURACY, "Promoted." if promoted else "Kept the current model.")


# --- classify -------------------------------------------------------------------------------------


def predictions(websites: Mapping[str, str], cached: Mapping[str, Mapping[str, Any]], version: str,
                predict: Callable[[str], tuple[str | None, float]],
                text_of: Callable[[str], str]) -> tuple[dict[str, Mapping[str, Any]], set[str]]:
    """(prediction per company, the ids predicted anew) for {company_id: website}.

    A cached prediction of `version` from the same website is reused. Without a stored text the
    cached one stays, even an older model's, rather than one made on nothing; one from another
    website never does."""
    result: dict[str, Mapping[str, Any]] = {}
    new: set[str] = set()
    for cid, website in websites.items():
        old = cached.get(cid)
        if old and old.get("website") != website:
            old = None
        if old and old.get("model_version") == version:
            result[cid] = old
        elif text := text_of(website):
            division, probability = predict(text)
            if division:
                result[cid] = {"model_version": version, "website": website, "division": division,
                               "probability": round(probability, 3)}
                new.add(cid)
        elif old:
            result[cid] = old
    return result, new


def run_classify() -> None:
    from technology_field.train import require_store, stored_text

    version, model = load_current()
    if model is None:
        log.info("No promoted WZ model yet; nothing written.")
        return
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = _source_id(cur)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            websites = _published_websites(cur)
            cur.execute("SELECT company_id::text, source_payload FROM public.company_source_records "
                        "WHERE source_id = %s", (source_id,))
            cached = dict(cur.fetchall())
            found, new = predictions({c: w for c, (w, _) in websites.items()}, cached, version,
                                     lambda text: textcat.predict(model, text), stored_text)
            require_store()

            written = dropped = 0
            for cid, prediction in sorted(found.items()):
                record_id, _ = upsert_source_record(
                    cur, company_id=cid, source_id=source_id, external_id=cid, external_url=websites[cid][0],
                    source_company_name="", source_payload=dict(prediction))
                insert_text_observation(
                    cur, company_id=cid, source_record_id=record_id, source_id=source_id, sync_run_id=sync_run_id,
                    field_name=FIELD, value_text=prediction["division"], confidence=prediction["probability"],
                    metadata={"decided_by": "textcat", "model_name": f"{MODEL_NAME}:{prediction['model_version']}"},
                    # The value is as old as the page it came from, not this run (#49).
                    source_updated_at=websites[cid][1],
                )
                cur.execute(
                    """
                    UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
                    WHERE company_id = %s AND source_id = %s AND field_name = %s AND value_text <> %s
                      AND confidence > 0
                    """,
                    (cid, source_id, FIELD, prediction["division"]),
                )
                written, dropped = written + 1, dropped + cur.rowcount
            # A company no longer predicted (its website changed or was removed) keeps no shown value.
            cur.execute(
                """
                UPDATE public.company_field_observations SET confidence = 0, updated_at = now()
                WHERE source_id = %s AND field_name = %s AND confidence > 0 AND NOT (company_id = ANY(%s::uuid[]))
                """,
                (source_id, FIELD, sorted(found)),
            )
            dropped += cur.rowcount
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(websites),
                            records_created=len(new), records_updated=dropped, records_failed=0)
        log.info("Model %s: %d companies with a division (%d predicted anew), %d old values dropped.",
                 version, written, len(new), dropped)
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="WZ (geschätzt): train a text classifier on Claude's labels and the app's votes, then classify",
        schedule=SCHEDULE, start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["models", "classification"],
    ) as dag:
        coordinate = TriggerDagRunOperator(task_id="trigger_pipeline_coordinator",
                                           trigger_dag_id="pipeline_coordinator", wait_for_completion=False)
        pipeline_task("train", run_train) >> pipeline_task("classify", run_classify) >> coordinate
    return dag


dag = _make_dag()
