#!/usr/bin/env python3
"""Train the Technologiefeld text classifier from the keyword seed and the app's votes.

    python scripts/train_branch_textcat.py              # train, measure, promote if it beats the keywords
    python scripts/train_branch_textcat.py --promote    # promote even without enough human labels

Reads `branches.jsonl` (run `classify` first), the cached website texts, and the `technologiefeld`
votes in `extraction_feedback`. Writes a model folder under `TEXTCAT_DIR` with `metrics.json`, and
`current.json` when the model is promoted. `classify --textcat` then uses it for the gaps.

Free: no API and no download, but it needs the database for the votes and the page cache for the
texts, and training takes minutes on a laptop. The reasoning is in
airflow/dags/combined_discovery/branch_textcat.py.

PROMOTION

Automatic only when at least MIN_HUMAN_HOLDOUT held-out companies carry a human label and the
classifier is at least as accurate on them as the keywords are. Without those labels nobody knows
whether it is better, so `--promote` is a human decision, logged as such in metrics.json.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "airflow" / "dags"))
# Same host-shaped default as the other host scripts; the DAG default only works inside Docker.
os.environ.setdefault("PG_DSN", "host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app")

from combined_discovery import branch_textcat  # noqa: E402
from manager_ner.ner import is_holdout  # noqa: E402

log = logging.getLogger("train_branch_textcat")

MIN_EXAMPLES = int(os.getenv("BRANCH_TEXTCAT_MIN_EXAMPLES", "50"))
MIN_HUMAN_HOLDOUT = int(os.getenv("BRANCH_TEXTCAT_MIN_HUMAN_HOLDOUT", "20"))
EPOCHS = int(os.getenv("BRANCH_TEXTCAT_EPOCHS", "10"))


def load_runner():
    spec = importlib.util.spec_from_file_location("runner", REPO / "scripts" / "run_combined_discovery.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_votes() -> dict[str, list[dict]]:
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(
                "SELECT company_id::text, value, verdict, correction FROM public.extraction_feedback "
                "WHERE field_name = 'technologiefeld'"
            )
            votes: dict[str, list[dict]] = defaultdict(list)
            for company_id, value, verdict, correction in cur.fetchall():
                votes[company_id].append({"value": value, "verdict": verdict, "correction": correction})
            return votes
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--promote", action="store_true", help="promote without enough human labels")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    runner = load_runner()
    from combined_discovery import branch_classifier as bc

    fields = bc.load_taxonomy().branch_names()
    branch_rows = runner.read_jsonl(runner.BRANCHES_PATH)
    if not branch_rows:
        raise SystemExit(f"nothing in {runner.BRANCHES_PATH}; run `classify` first")
    sample = {r["company_id"]: r for r in runner.sample_rows(None) if r.get("company_id")}
    facts = runner.load_company_facts()
    texts = {}
    for row in branch_rows:
        cid = row.get("company_id")
        if cid in facts:
            page = runner.company_texts(sample.get(cid, {}), facts[cid], use_pages=True, use_purpose=False)
            texts[cid] = branch_textcat.classifier_text(page)
    votes = load_votes()

    data = branch_textcat.training_set(branch_rows, texts, votes, set(fields))
    log.info("Labels: %s (%d companies, %d with a human label)", dict(data.counts), len(data.labelled), len(data.human))
    holdout = {cid for cid in data.human if is_holdout(cid)}
    train_examples = [data.labelled[cid] for cid in data.labelled if cid not in holdout]
    if len(train_examples) < MIN_EXAMPLES:
        log.info("Only %d training examples; %d needed. Not training.", len(train_examples), MIN_EXAMPLES)
        return 0

    nlp, losses = branch_textcat.train(train_examples, fields, epochs=EPOCHS)

    seeds = {r["company_id"]: r.get("branch") for r in branch_rows if r.get("decided_by") == "keywords"}
    clf_acc, n = branch_textcat.accuracy(
        (branch_textcat.predict(nlp, data.labelled[cid][0])[0], data.labelled[cid][1]) for cid in holdout
    )
    kw_acc, n_kw = branch_textcat.accuracy((seeds.get(cid), data.labelled[cid][1]) for cid in holdout)
    measured = n >= MIN_HUMAN_HOLDOUT
    promoted = (measured and clf_acc >= kw_acc) or args.promote

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = runner.TEXTCAT_DIR / version
    nlp.to_disk(out)
    metrics = {
        "version": version, "fields": len(fields), "examples_train": len(train_examples),
        "labels": dict(data.counts), "losses": losses,
        "human_holdout": {"companies": n, "classifier_accuracy": round(clf_acc, 3),
                          "keyword_accuracy": round(kw_acc, 3), "keyword_compared": n_kw},
        "promoted": promoted,
        "promoted_by": "measurement" if promoted and not args.promote else ("person (--promote)" if promoted else None),
    }
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False))
    if promoted:
        (runner.TEXTCAT_DIR / "current.json").write_text(json.dumps({"version": version}, indent=2))
    log.info(
        "Model %s: classifier %.3f vs keywords %.3f on %d human-labelled held-out companies. %s",
        version, clf_acc, kw_acc, n,
        "Promoted." if promoted else (
            "Not promoted: too few human labels to know whether it is better (use --promote to decide yourself)."
            if not measured else "Not promoted: the keywords are more accurate."
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
