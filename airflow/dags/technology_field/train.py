"""Weekly: train the Technologiefeld text classifier, promote it only when people say it is better (#62).

The rules live in `enrichment.branch_textcat`; this file does the I/O.

- **Labels:** what the keywords assigned (the seed), overridden by the app's votes on
  `technology_field`, translated back from English to Berlin's keys (`german_votes`).
- **Text:** the homepage and services pages the `technology_field` DAG read, from the company corpus
  (S3). Nothing is downloaded; a company whose pages are no longer stored is left out.
- **Judged only against people.** A fixed fifth of the companies people voted on is held out
  (`manager_ner.ner.is_holdout`, the same stable split) and never trained on. Of those, only the
  companies the keywords left empty count: there the model would decide.
- **Promoted** when at least `MIN_HUMAN_HOLDOUT` such companies were judged, the accuracy is at least
  `MIN_ACCURACY`, and it beats the model in use. Until people have judged companies the keywords left
  empty, a run trains, stores its metrics and promotes nothing. A hand-checked set entered in the app
  as corrections counts.

The model is kept in the model store (`enrichment.model_store`, S3) under `branch_textcat`;
`BRANCH_TEXTCAT_MODEL_DIR` is only a working copy. The `technology_field` DAG uses the promoted
version to fill companies where no keyword fired.
"""
from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from enrichment import branch_classifier as bc
from enrichment import branch_textcat as textcat
from enrichment import model_store
from manager_ner.ner import is_holdout
from source_enrichment_common import get_connection
from technology_field.main import _source_id, keyword_answers, read_records

log = logging.getLogger(__name__)

DAG_ID = "technology_field_train"
MODEL_DIR = Path(os.getenv("BRANCH_TEXTCAT_MODEL_DIR", "/opt/airflow/data/models/branch_textcat"))
EPOCHS = int(os.getenv("BRANCH_TEXTCAT_EPOCHS", "10"))
#: Below this many training companies there is nothing to learn 77 fields from.
MIN_EXAMPLES = int(os.getenv("BRANCH_TEXTCAT_MIN_EXAMPLES", "100"))


def load_current() -> tuple[str | None, Any]:
    """(version, model) in use, or (None, None) while nothing was promoted."""
    version = model_store.current_version(textcat.MODEL_NAME, MODEL_DIR)
    if not version:
        return None, None
    import spacy

    return version, spacy.load(model_store.local_copy(textcat.MODEL_NAME, version, MODEL_DIR))


def stored_text(website: str) -> str:
    """The text the keywords read for this website, from the store only: services pages, then homepage."""
    from enrichment import page_cache
    from enrichment.content_pages import content_for
    from enrichment.url_patterns import visible_text

    homepage = (page_cache.get("page", website) or {}).get("html") or ""
    if not homepage:
        return ""
    return textcat.classifier_text({"services": content_for(website, homepage, fetch=False).text,
                                    "homepage": visible_text(homepage)})


def should_promote(new: tuple[float, int], current: tuple[float, int] | None) -> bool:
    """`new` and `current` are (accuracy, companies judged) on the held-out gap companies."""
    accuracy, judged = new
    return judged >= textcat.MIN_HUMAN_HOLDOUT and accuracy >= textcat.MIN_ACCURACY and (
        current is None or accuracy > current[0])


def _votes(cur: Any) -> dict[str, list[dict[str, Any]]]:
    cur.execute(
        """
        SELECT company_id::text, value, verdict, correction
        FROM public.extraction_feedback
        WHERE field_name = 'technology_field'
        """
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for company_id, value, verdict, correction in cur.fetchall():
        grouped[company_id].append({"value": value, "verdict": verdict, "correction": correction})
    return grouped


def run_train() -> None:
    taxonomy = bc.load_taxonomy()
    fields = set(taxonomy.branch_names())
    english_to_key = {taxonomy.english(branch): branch for branch in fields}

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            records = read_records(cur, _source_id(cur))
            votes = {cid: textcat.german_votes(rows, english_to_key) for cid, rows in _votes(cur).items()}
    finally:
        connection.close()

    rows = [{"company_id": cid, "outcome": a.outcome, "decided_by": a.decided_by, "branch": a.branch}
            for cid, a in keyword_answers(records, taxonomy).items()]
    texts = {cid: text for cid, *_, still_published, website in records
             if still_published and (text := stored_text(website))}
    labelled = textcat.training_set(rows, texts, votes, fields)
    held_out = {cid for cid in votes if is_holdout(cid)}
    examples = [pair for cid, pair in sorted(labelled.labelled.items()) if cid not in held_out]
    if len(examples) < MIN_EXAMPLES:
        log.info("Only %d companies to train on (%d with text); %d needed. Not training.",
                 len(examples), len(texts), MIN_EXAMPLES)
        return

    nlp, losses = textcat.train(examples, sorted(fields), epochs=EPOCHS)
    gap_held_out = sorted(held_out & textcat.gap_companies(rows) & texts.keys())

    def judge(model: Any) -> tuple[float, int]:
        return textcat.gap_accuracy(((textcat.predict(model, texts[c])[0], votes[c]) for c in gap_held_out), fields)

    in_use, current_model = load_current()
    new, current = judge(nlp), judge(current_model) if current_model is not None else None
    # For information only: on companies the keywords answered, the model mostly copies its seed.
    other = textcat.accuracy((textcat.predict(nlp, texts[c])[0], labelled.labelled[c][1])
                             for c in sorted(held_out & labelled.human - set(gap_held_out)))
    promoted = should_promote(new, current)

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = MODEL_DIR / version
    out.mkdir(parents=True)  # spaCy's to_disk creates only the last folder, not MODEL_DIR
    if promoted:
        nlp.to_disk(out)
    metrics = {
        "version": version, "taxonomy_version": taxonomy.version, "companies_train": len(examples),
        "label_counts": dict(labelled.counts), "losses": losses,
        "gap_held_out": {"new_model": {"accuracy": new[0], "judged": new[1]},
                         "current_model": {"version": in_use, "accuracy": current[0], "judged": current[1]}
                         if current else None},
        "other_held_out": {"accuracy": other[0], "judged": other[1]},
        "bars": {"min_judged": textcat.MIN_HUMAN_HOLDOUT, "min_accuracy": textcat.MIN_ACCURACY},
        "promoted": promoted,
    }
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2))
    if promoted:
        (MODEL_DIR / "current.json").write_text(json.dumps({"version": version}, indent=2))
    # On the hub this pod's disk is gone after the task: the store (S3) keeps the run and the model.
    model_store.publish(textcat.MODEL_NAME, version, MODEL_DIR, metrics, promoted=promoted)
    log.info("Model %s: %d companies trained; %d of %d needed held-out gap companies judged, accuracy "
             "%.3f (bar %.2f). %s", version, len(examples), new[1], textcat.MIN_HUMAN_HOLDOUT, new[0],
             textcat.MIN_ACCURACY, "Promoted." if promoted else "Kept the current model.")


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    with DAG(
        dag_id=DAG_ID,
        description="Train the Technologiefeld text classifier from the keywords and the app's votes",
        schedule="@weekly", start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["models", "classification"],
    ) as dag:
        pipeline_task("train", run_train)
    return dag


dag = _make_dag()
