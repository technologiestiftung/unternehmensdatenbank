#!/usr/bin/env python3
"""Train the Technologiefeld text classifier from the keyword seed and the app's votes.

    python scripts/train_branch_textcat.py              # train, measure, promote if accurate on the gaps
    python scripts/train_branch_textcat.py --promote    # promote even without enough human labels

Reads `branches.jsonl` (run `classify` first), the cached website texts, and the `technologiefeld`
votes in `extraction_feedback`. Writes a version folder under `TEXTCAT_DIR` with `metrics.json`; the
model files and `current.json` only when the model is promoted. `classify --textcat` then uses it for the gaps.

Free: no API and no download, but it needs the database for the votes and the page cache for the
texts, and training takes minutes on a laptop. The reasoning is in
airflow/dags/combined_discovery/branch_textcat.py.

PROMOTION

Automatic only when at least MIN_HUMAN_HOLDOUT held-out gap companies (no keyword decided them) carry
a human judgement and the classifier gets at least MIN_ACCURACY of them right; a plain "wrong" on
exactly its prediction counts as a miss. Accuracy on the other held-out companies is logged, never
used. Without those labels nobody knows whether it helps, so
`--promote` is a human decision, logged as such in metrics.json. Where gap labels come from:
branch_textcat.py, "HOW IT IS JUDGED".
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
MIN_ACCURACY = float(os.getenv("BRANCH_TEXTCAT_MIN_ACCURACY", "0.7"))
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

    gaps = branch_textcat.gap_companies(branch_rows)

    gap_acc, n_gap = branch_textcat.gap_accuracy(
        ((branch_textcat.predict(nlp, texts[cid])[0], votes[cid])
         for cid in gaps if cid in votes and texts.get(cid) and is_holdout(cid)),
        set(fields),
    )
    # Every held-out company that is not a gap (keywords, torn, no_website, model tie-break): information only.
    other_acc, n_other = branch_textcat.accuracy(
        (branch_textcat.predict(nlp, data.labelled[cid][0])[0], data.labelled[cid][1]) for cid in holdout - gaps
    )
    measured = n_gap >= MIN_HUMAN_HOLDOUT
    promoted = (measured and gap_acc >= MIN_ACCURACY) or args.promote

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = runner.TEXTCAT_DIR / version
    out.mkdir(parents=True)  # spaCy's to_disk creates only the last folder, not TEXTCAT_DIR
    if promoted:
        nlp.to_disk(out)
    metrics = {
        "version": version, "fields": len(fields), "examples_train": len(train_examples),
        "labels": dict(data.counts), "losses": losses,
        "human_holdout": {"gap_companies": n_gap, "gap_accuracy": round(gap_acc, 3),
                          "min_accuracy": MIN_ACCURACY,
                          "other_companies": n_other, "other_company_accuracy": round(other_acc, 3)},
        "promoted": promoted,
        "promoted_by": "measurement" if promoted and not args.promote else ("person (--promote)" if promoted else None),
    }
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2, ensure_ascii=False))
    if promoted:
        (runner.TEXTCAT_DIR / "current.json").write_text(json.dumps({"version": version}, indent=2))
    log.info(
        "Model %s: %.3f on %d human-judged held-out gap companies (%.3f on %d other companies, information only). %s",
        version, gap_acc, n_gap, other_acc, n_other,
        "Promoted." if promoted else (
            "Not promoted, model files not kept: too few human labels on gap companies to know whether it "
            "helps (use --promote to decide yourself)."
            if not measured else f"Not promoted, model files not kept: below {MIN_ACCURACY} on the gaps."
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
