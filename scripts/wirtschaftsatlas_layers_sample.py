"""Hand-check the Wirtschaftsatlas layers as a Technologiefeld signal (#148): draw a sample, then score it.

    PG_DSN="host=localhost port=5433 dbname=unternehmensdatenbank user=app password=app" \\
      python scripts/wirtschaftsatlas_layers_sample.py draw > sample.csv

prints `SAMPLE_SIZE` companies whose Wirtschaftsatlas layers name exactly one field
(`wirtschaftsatlas_layer_fields`, V2), as CSV in a fixed order (md5 of the id): name, website, the
layer's field (`label`), the keyword Technologiefeld we show today (`keyword_field`, for comparison
only), and two empty columns for the person: `verdict` (`correct` or `wrong`) and, if wrong,
`correction`. The counts go to stderr: companies with a mapped layer, those whose layers name several
fields (left out), and how often the layer agrees with the keyword field where both exist. Keep the
file out of git: it names companies, and the verdicts are a person's work.

    python scripts/wirtschaftsatlas_layers_sample.py score sample.csv

prints the precision, as for the LLM labels (`llm_labels_sample.score`). Only with that number does a
layer write anything: the precision goes into #148 and docs/branch-classification.md.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "dags"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from llm_labels_sample import score  # noqa: E402
from wirtschaftsatlas.main import SOURCE_KEY  # noqa: E402

SAMPLE_SIZE = 100
COLUMNS = ["company_id", "name", "website", "label", "keyword_field", "verdict", "correction"]

#: Per company: the distinct fields its mapped layers name, and what we show today.
QUERY = """
    SELECT r.company_id::text, p.name, p.website, array_agg(DISTINCT tf.name_en ORDER BY tf.name_en),
           p.technology_field
    FROM public.company_source_records r
    JOIN public.sources s ON s.id = r.source_id AND s.key = %s AND r.is_active
    CROSS JOIN LATERAL jsonb_array_elements_text(r.source_payload -> 'layers') AS layer(path)
    JOIN public.wirtschaftsatlas_layer_fields m ON m.layer = layer.path
    JOIN public.technology_fields tf ON tf.name = m.technology_field
    JOIN public.company_profile_mv p ON p.id = r.company_id
    GROUP BY r.company_id, p.name, p.website, p.technology_field
    ORDER BY md5(r.company_id::text)
"""


def split(rows: list[tuple]) -> tuple[list[tuple], int, tuple[int, int]]:
    """(companies with exactly one field, how many had several, (agree, both) with the keyword field)."""
    single = [row for row in rows if len(row[3]) == 1]
    both = [row for row in single if row[4] and row[4] != "NONE"]
    return single, len(rows) - len(single), (sum(row[3][0] == row[4] for row in both), len(both))


def draw() -> None:
    from source_enrichment_common import get_connection

    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            cur.execute(QUERY, (SOURCE_KEY,))
            rows = cur.fetchall()
    finally:
        connection.close()
    single, several, (agree, both) = split(rows)
    print(f"{len(rows)} companies with a mapped layer; {several} name several fields and are left out; "
          f"the layer agrees with the keyword field in {agree} of {both}", file=sys.stderr)
    writer = csv.writer(sys.stdout)
    writer.writerow(COLUMNS)
    for company_id, name, website, fields, keyword_field in single[:SAMPLE_SIZE]:
        writer.writerow([company_id, name, website, fields[0], keyword_field or "", "", ""])


def main(argv: list[str]) -> None:
    if argv[1:] == ["draw"]:
        draw()
    elif len(argv) == 3 and argv[1] == "score":
        with open(argv[2], newline="", encoding="utf-8") as handle:
            result = score(list(csv.DictReader(handle)))
        correct, judged = result["field"]
        print(f"{correct} of {judged} correct" + (f" ({correct / judged:.0%})" if judged else ""))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
