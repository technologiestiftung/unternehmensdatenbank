"""Turn one company's two classification rows into rows in the database.

Everything in this file is a write. `branch_classifier.py`, `purpose_branch.py` and the `classify`
subcommands deliberately write nothing but files, so the same code can be argued with and re-run
without touching any state the application reads; this is the one place that decides what a
classification looks like once stored. Same split, and the same reason, as
`observations.py` has for combined discovery.

Three vocabulary notes, because these words are invented by this codebase:

- An **observation** is one claim by one source about one field of one company. Sources never
  overwrite each other; they each add observations, and `pipeline_coordinator` later decides which
  one wins.
- **Confidence** is a number between 0 and 1 on that claim. For these two columns it is a POLICY,
  not a measurement -- see `_CONFIDENCE` below.
- A **source record** is the one row per company saying "this source looked at this company", with
  the raw detail attached as JSON.

## The four field names, and why two of them can never be published

| field_name | holds | resolver |
|---|---|---|
| `technology_field` | one of Berlin's 77 fields, from words the company published | `resolve_technology_field` |
| `purpose_field` | one of the same 77, from the register's Gegenstand | `resolve_purpose_field` |
| `technology_field_withheld` | the field the website rule refused to publish | **none, ever** |
| `purpose_field_withheld` | the field the 40-character floor cost | **none, ever** |

The `_withheld` pair reuses the `directory_url` trick: a field name no `resolve_*` function reads
cannot be promoted by any threshold change. A low confidence in the real field would have been one
edit away from release. **Never add either name to an `obs_field_names` array.**

## The bug this file exists to avoid

**Every unassigned row carries the literal string `"NONE"` as its value** -- 14,040 rows in
`branches.jsonl` and 9,738 in `purpose_branches.jsonl`, 23,778 together, measured 2026-09-11. A
filter written on the truthiness of the value rather than on the OUTCOME would write all of them,
and `pipeline_coordinator` would then promote `'NONE'` to the canonical industry field for thousands
of companies.

So every write below is gated on `outcome`, and the three outcomes that count as assigned are
**imported** from `purpose_branch` rather than spelled out again. `none_apply` (2,225 rows) is the
case that makes this matter most: a model that read the register text and said none of the 77 fields
fit has given a real answer, and that answer is NONE. It is not a field.

## What this does NOT write, and why that differs from observations.py

No `discovery_tier` ledger. `observations.py` writes one because for
combined discovery "OpenStreetMap had nothing for this company" and "OpenStreetMap was never asked"
are otherwise the same absence in the database, and the second reads as evidence that a company has
no website.

That problem does not exist here: `classify` and `classify-purpose` each write **one row per company
for all 14,443**, whatever the outcome, and `tests/integration/test_ground_truth_columns.py`
asserts exactly that. The file is already the ledger. Writing markers would add ~28,886 rows saying
nothing the files do not already say.
"""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

from source_enrichment_common import (
    insert_text_observation,
    now_iso,
    upsert_source_record,
)

#: The three outcome names that mean "a field was established". Imported, never restated: a filter
#: that rebuilds this list agrees with itself no matter what the classifier actually does, and the
#: whole `NONE` trap above is a filter written against the wrong thing. `none_apply` and `unsure` are
#: absent from it on purpose -- both are a model's real answer, and that answer is not a field.
from enrichment.purpose_branch import (
    ASSIGNED_OUTCOMES,
    DECIDED_BY_KEYWORDS,
    DECIDED_BY_MODEL_DIRECT,
    DECIDED_BY_MODEL_TIEBREAK,
)

#: Column 1's text classifier. It only fills gaps the keywords left (see branch_textcat.py).
from enrichment.branch_textcat import DECIDED_BY_TEXTCAT

#: The resolver's own bar, imported and never re-typed. Two copies of a threshold drift apart, and
#: this one has zero margin: see the assertion below.
from pipeline_coordinator.resolver import _FIELD_MIN_CONF

log = logging.getLogger(__name__)

SOURCE_KEY = "branch_classifier"

#: Column 1's outcome name for "a field was established". Its own vocabulary, deliberately unlike
#: column 3's `assigned_keywords` -- the two columns answer different questions and a row from one
#: must never be mistakable for a row from the other.
BRANCH_ASSIGNED = "assigned"

#: Confidence by who decided. **A POLICY, NOT A MEASUREMENT**, and the `branch_classifier` row's `sources.metadata` says so
#: because that row is where a reader will look. These columns have no calibrated confidence: the
#: score is an unbounded keyword-weight sum and 4,095 of column 3's 4,705 assigned rows have no score
#: at all, while `confidence` is `NUMERIC(5,4) CHECK (0..1)`, so a number had to be chosen. The three
#: are ordered by how much evidence stands behind them, and the real score travels in
#: `metadata.score` where a later calibration can find it.
#:
#: `model_direct` is **0.52 and was 0.50**. At 0.50 it sat exactly on `_FIELD_MIN_CONF` and promoted
#: only because the resolver's filter reads `>=`. That is zero margin under three ordinary edits:
#: tightening `>=` to `>`, raising `_FIELD_MIN_CONF` for one of the five OTHER logical fields that
#: share it, or choosing a future policy value just under the bar. 0.52 keeps the ordering the policy
#: encodes -- most evidence to least -- and buys two notches.
_CONFIDENCE = {
    DECIDED_BY_KEYWORDS: 0.60,
    DECIDED_BY_MODEL_TIEBREAK: 0.55,
    DECIDED_BY_MODEL_DIRECT: 0.52,
    # A classifier trained on keyword seeds and app votes, used only where no keyword fired. Below
    # the keywords it learned from and the tie-break model, above model_direct and the 0.50 bar.
    DECIDED_BY_TEXTCAT: 0.53,
}

#: The withheld pair, and column 1's assigned rows, carry a flat 0.60. Column 1 has no model in the
#: current file -- all 403 assigned rows are `decided_by == "keywords"` -- and the 535 withheld rows
#: carry `decided_by == ""`, so neither can be looked up in the policy above.
_KEYWORD_CONFIDENCE = _CONFIDENCE[DECIDED_BY_KEYWORDS]

#: 0.52 > 0.50 today. This fails loudly at import if a future edit makes the lowest policy value
#: unpublishable -- which would otherwise drop 4,095 fields with no error anywhere, because the
#: resolver simply would not select them.
assert min(_CONFIDENCE.values()) > _FIELD_MIN_CONF, (
    f"the lowest confidence policy value {min(_CONFIDENCE.values())} is not above the resolver's "
    f"_FIELD_MIN_CONF {_FIELD_MIN_CONF}; every observation written at it would be silently dropped"
)


def confidence_for(decided_by: str | None) -> float:
    """The policy value for a row, or the keyword value when nobody is named.

    A withheld row has no `decided_by` -- the website rule and the length floor are not decisions a
    keyword or a model made -- and column 1 names only `keywords`. Falling back rather than raising
    keeps a future column-1 model row writable at its own policy value the day one appears.
    """
    return _CONFIDENCE.get(decided_by or "", _KEYWORD_CONFIDENCE)


def score_for(row: dict[str, Any]) -> float | None:
    """The keyword-weight sum behind a row, or None where no score was ever measured.

    **`None`, not the 0.0 the file stores.** Measured 2026-09-11: every one of the 4,095
    `model_direct` rows carries `score == 0.0`, and every `keywords` (596) and `model_tiebreak` (14)
    row carries a real sum. Tier B asks a model to choose from all 77 fields with no keyword to
    anchor it, so there is no weight to sum -- 0.0 means *never measured*, not *measured as zero*.

    Storing the 0.0 would put a plausible decimal in the same column as 7.487 with nothing able to
    tell them apart, which is the exact reasoning `purpose_branch.purpose_candidates` already applies
    to the `candidates` array via `BASIS_KEYWORD` / `BASIS_MODEL`. Same rule, same place it matters.
    """
    if row.get("decided_by") == DECIDED_BY_MODEL_DIRECT:
        return None
    score = row.get("score")
    return float(score) if isinstance(score, (int, float)) else None


def _strip_nul(value: Any) -> Any:
    """Remove NUL bytes from every string, at any depth. Postgres JSONB cannot store `\\u0000`.

    Found the first time this module wrote the whole table: one company's `evidence` snippet carried
    a NUL, and `psycopg2` failed the entire transaction with
    `unsupported Unicode escape sequence ... \\u0000 cannot be converted to text`. One bad byte in
    one scraped page, and 5,660 observations do not land.

    It belongs here rather than in the importer because `evidence` is page text somebody else
    fetched, so **any** caller of this module hits it — and because a write path that can be defeated
    by one byte of upstream noise is not a write path. Stripping rather than escaping: the byte
    carries no meaning, it is an artefact of the page, and the surrounding sentence is what the
    column is for.
    """
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {_strip_nul(k): _strip_nul(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_nul(v) for v in value]
    return value


def _metadata(row: dict[str, Any], *, column: str, withheld: bool) -> dict[str, Any]:
    """The reasoning behind one claim, as stored JSONB.

    `keywords` is a dict in column 1 (keyword -> which text it came from) and a list in column 3.
    Both are sorted so a re-run produces byte-identical JSON and the upsert is a genuine no-op --
    `insert_text_observation` compares on `(company_id, field_name, source_id, value_text)` and then
    overwrites `metadata`, so an unstable ordering would rewrite every row on every run.
    """
    keywords = row.get("keywords")
    if isinstance(keywords, dict):
        keywords = {k: keywords[k] for k in sorted(keywords)}
    elif isinstance(keywords, (list, tuple, set)):
        keywords = sorted(keywords)

    meta: dict[str, Any] = {
        "provider": SOURCE_KEY,
        "column": column,
        "outcome": row.get("outcome"),
        "decided_by": row.get("decided_by") or None,
        "score": score_for(row),
        "keywords": keywords,
        "taxonomy_version": row.get("taxonomy_version"),
        "evidence": row.get("evidence"),
        "runner_up": row.get("runner_up"),
        "runner_up_score": row.get("runner_up_score"),
        # What the corpus was. A keyword's weight depends on how many companies use it, so two files
        # scored over different scopes give the same company different answers and neither is wrong.
        "scope_companies": row.get("scope_companies") or row.get("n_companies"),
        "df_companies": row.get("df_companies"),
    }
    if row.get("llm_reason"):
        meta["llm_reason"] = row["llm_reason"]
    if row.get("model_name"):
        meta["model_name"] = row["model_name"]
    if withheld:
        # Why it was withheld, so the two rules can be argued with from the database alone rather
        # than by re-deriving which gate bit.
        meta["withheld_by"] = (
            "website_gate" if column == "technology_field" else "purpose_length_floor"
        )
    # Last, and over the whole structure: `evidence` is page text and one company's carries a NUL.
    return _strip_nul(meta)


def build_payload(
    company: Any, branch_row: dict[str, Any] | None, purpose_row: dict[str, Any] | None
) -> dict[str, Any]:
    """The raw JSON kept on the source record.

    `status` is `"fetched"`, never `"planned"`. The query that picks the next batch of companies
    skips a company only when its source record's status is something other than `"planned"`, so a
    source that leaves it at `"planned"` re-processes the same companies forever. Two DAGs in this
    repo have exactly that bug; this one does not.
    """
    return {
        "status": "fetched",
        "source_key": SOURCE_KEY,
        "company_id": company.id,
        "company_name": company.company_name,
        "generated_at": now_iso(),
        "technology_field": {
            "outcome": (branch_row or {}).get("outcome"),
            "value": (branch_row or {}).get("technology_field"),
            "withheld": (branch_row or {}).get("withheld_branch"),
            "decided_by": (branch_row or {}).get("decided_by") or None,
        },
        "purpose_field": {
            "outcome": (purpose_row or {}).get("outcome"),
            "value": (purpose_row or {}).get("purpose_field"),
            "withheld": (purpose_row or {}).get("withheld_by_floor"),
            "decided_by": (purpose_row or {}).get("decided_by") or None,
        },
        "taxonomy_version": (
            (branch_row or {}).get("taxonomy_version")
            or (purpose_row or {}).get("taxonomy_version")
        ),
    }


@lru_cache(maxsize=1)
def _taxonomy() -> Any:
    from enrichment.branch_classifier import load_taxonomy

    return load_taxonomy()


def english(branch: str) -> str:
    """The name the database stores: the classifiers speak Berlin's German key, the app shows `name_en`."""
    return _taxonomy().english(branch)


def planned_writes(
    branch_row: dict[str, Any] | None, purpose_row: dict[str, Any] | None
) -> list[tuple[str, str, float, dict[str, Any]]]:
    """`(field_name, value, confidence, metadata)` for one company. Empty when there is nothing.

    **The gate is the OUTCOME, never the truthiness of the value.** See the module docstring: the
    value is the literal string `"NONE"` on 23,778 rows, and writing those would hand the resolver a
    canonical industry field of `'NONE'` for thousands of companies.

    Separated from the write so the importer can forecast a run without a database, and so the guard
    `scripts/check_branch_observations.py` can assert on what this function is able to emit.
    """
    writes: list[tuple[str, str, float, dict[str, Any]]] = []

    if branch_row:
        if branch_row.get("outcome") == BRANCH_ASSIGNED and branch_row.get("technology_field"):
            writes.append((
                "technology_field",
                english(branch_row["technology_field"]),
                confidence_for(branch_row.get("decided_by")),
                _metadata(branch_row, column="technology_field", withheld=False),
            ))
        if branch_row.get("withheld_branch"):
            writes.append((
                "technology_field_withheld",
                english(branch_row["withheld_branch"]),
                _KEYWORD_CONFIDENCE,
                _metadata(branch_row, column="technology_field", withheld=True),
            ))

    if purpose_row:
        if purpose_row.get("outcome") in ASSIGNED_OUTCOMES and purpose_row.get("purpose_field"):
            writes.append((
                "purpose_field",
                english(purpose_row["purpose_field"]),
                confidence_for(purpose_row.get("decided_by")),
                _metadata(purpose_row, column="purpose_field", withheld=False),
            ))
        if purpose_row.get("withheld_by_floor"):
            writes.append((
                "purpose_field_withheld",
                english(purpose_row["withheld_by_floor"]),
                _KEYWORD_CONFIDENCE,
                _metadata(purpose_row, column="purpose_field", withheld=True),
            ))

    return writes


def write_company_classification(
    cur: Any,
    *,
    company: Any,
    branch_row: dict[str, Any] | None,
    purpose_row: dict[str, Any] | None,
    source_id: str,
    sync_run_id: str,
) -> tuple[bool, int]:
    """Store one company's classification. Returns `(source record was created, rows written)`.

    Writes nothing at all -- not even a source record -- for a company with no field in either
    column. 9,568 of the 14,443 are in that state, and a source record for each would be 9,568 rows
    asserting only that a free, offline script ran over a file that already says so.
    """
    writes = planned_writes(branch_row, purpose_row)
    if not writes:
        return False, 0

    source_record_id, was_created = upsert_source_record(
        cur,
        company_id=company.id,
        source_id=source_id,
        external_id=f"{SOURCE_KEY}:{company.id}",
        # No URL to stand behind: this source classifies, it does not discover.
        external_url=None,
        source_company_name=company.company_name,
        source_payload=build_payload(company, branch_row, purpose_row),
    )

    for field_name, value, confidence, metadata in writes:
        insert_text_observation(
            cur,
            company_id=company.id,
            source_record_id=source_record_id,
            source_id=source_id,
            sync_run_id=sync_run_id,
            field_name=field_name,
            value_text=value,
            metadata=metadata,
            confidence=confidence,
        )

    return was_created, len(writes)
