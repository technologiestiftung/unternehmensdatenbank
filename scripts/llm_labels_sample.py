"""Hand-check the Claude API's Technologiefeld labels (#87): draw a sample, then score it.

    PG_DSN="host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app" \\
      python scripts/llm_labels_sample.py draw claude-opus-5-5 > sample.csv

prints `SAMPLE_SIZE` labels of one model as CSV, in a fixed order (md5 of the id), so the same
database draws the same sample: company name, website, the model's label, and two empty columns for
the person: `verdict` (`correct` or `wrong`) and, if wrong, `correction` (one of the 77 English
names, or NONE). The same for the estimated WZ division (#140): `wz_division`, then `wz_verdict`
and `wz_correction` (a two-digit division, or NONE). Keep the file out of git: it names companies, and the verdicts are a person's work.

    python scripts/llm_labels_sample.py score sample.csv

prints the precision: of the labels a person judged, how many were correct, split into field labels
and NONE, and the same for the WZ division (`wz`). That number, with the cost per label from the DAG's log, goes into
docs/branch-classification.md. Needs the database for `draw` only.
"""
from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))

from llm_labels.main import SOURCE_KEY  # noqa: E402

SAMPLE_SIZE = 100
COLUMNS = ["company_id", "name", "website", "label", "model", "verdict", "correction",
           "wz_division", "wz_verdict", "wz_correction"]


def draw(model: str) -> None:
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(
                """
                SELECT r.company_id::text, r.source_company_name, r.external_url, r.source_payload ->> 'label',
                       r.source_payload ->> 'wz_division'
                FROM public.company_source_records r
                JOIN public.sources s ON s.id = r.source_id AND s.key = %s
                WHERE r.source_payload ->> 'model' = %s AND r.source_payload ->> 'outcome' = 'labelled'
                ORDER BY md5(r.company_id::text)
                LIMIT %s
                """,
                (SOURCE_KEY, model, SAMPLE_SIZE),
            )
            rows = cur.fetchall()
    finally:
        connection.close()
    writer = csv.writer(sys.stdout)
    writer.writerow(COLUMNS)
    for company_id, name, website, label, wz_division in rows:
        writer.writerow([company_id, name, website, label, model, "", "", wz_division or "", "", ""])


def score(rows: list[dict[str, str]]) -> dict[str, tuple[int, int]]:
    """kind ('field', 'NONE', 'all', 'wz') -> (correct, judged)."""
    tally: dict[str, Counter] = {kind: Counter() for kind in ("field", "NONE", "all", "wz")}
    for row in rows:
        wz_verdict = (row.get("wz_verdict") or "").strip().lower()
        if wz_verdict in ("correct", "wrong"):
            tally["wz"]["judged"] += 1
            tally["wz"]["correct"] += wz_verdict == "correct"
        verdict = (row.get("verdict") or "").strip().lower()
        if verdict not in ("correct", "wrong"):
            continue
        for kind in ("NONE" if row["label"] == "NONE" else "field", "all"):
            tally[kind]["judged"] += 1
            tally[kind]["correct"] += verdict == "correct"
    return {kind: (t["correct"], t["judged"]) for kind, t in tally.items()}


def main(argv: list[str]) -> None:
    if len(argv) == 3 and argv[1] == "draw":
        draw(argv[2])
    elif len(argv) == 3 and argv[1] == "score":
        with open(argv[2], newline="", encoding="utf-8") as handle:
            result = score(list(csv.DictReader(handle)))
        for kind, (correct, judged) in result.items():
            print(f"{kind}: {correct} of {judged} correct" + (f" ({correct / judged:.0%})" if judged else ""))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
