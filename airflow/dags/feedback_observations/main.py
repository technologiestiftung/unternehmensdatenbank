"""People's corrections from the app, as a source like any other (issue #45).

In the app, anyone can mark a shown value wrong and type the right one (`extraction_feedback`, one
row per person, company, field and shown value). This DAG turns those corrections into observations
of the source `user_feedback`, and `pipeline_coordinator` decides what is shown, as for every source.

**Agreement decides, not one person.** Per company, field and corrected value, it counts the
distinct people who typed it or voted that value `correct` (`agree`), and the distinct people who
voted another value of the field `correct` or this value `wrong`, with or without a correction of
their own (`disagree`; on `LIST_FIELDS` only this value `wrong`). Nobody counts both ways on one
value: their latest vote on it wins. The difference sets the confidence (`confidence`):

    net agreement   confidence   shown?
    0 or less       0            no
    1               0.30         no: below the coordinator's bars (0.50, websites 0.35)
    1, field empty  0.60         yes: no other source has a value (`filled_fields`, asked each run)
    2               0.60         yes
    3 or more       0.75         yes

The source has priority 50, ahead of every other source (V4), so a shown correction replaces the
register's or a website's value; that value stays stored and wins again when people withdraw their
votes. Nothing is ever deleted: a correction nobody stands behind any more drops to confidence 0.

**Checked before it is written** (`clean`): a website must be a URL, a Technologiefeld one of
Berlin's 77, a number a number, a date a date. A correction that fails is counted and skipped.
Three fields take no corrections: `fundings` (a grant link, not a value), `register_announcement`
(what the court published) and `registration_number` (identity, not a value).

**No personal data leaves `extraction_feedback`.** An observation stores how many people agreed,
never who. A corrected `management` name is a person's name; showing it was cleared on 2026-10-29
because the app is behind Keycloak.

The trainers read the votes themselves (`manager_ner_train`, the Technologiefeld classifier):
what a vote teaches a model differs per model, so they share the table, not a function.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

from psycopg2.extras import Json

from enrichment.url_patterns import to_ascii_host
from enrichment.register_names import REGISTER_STATUSES
from source_enrichment_common import (
    ensure_source,
    finish_sync_run,
    get_connection,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "feedback_observations"
SOURCE_KEY = "user_feedback"

#: App field key -> (observation field name, kind of value). The app votes under its column keys;
#: the observation names are the coordinator's.
FIELDS: dict[str, tuple[str, str]] = {
    "name": ("name", "text"),
    "address": ("address", "text"),
    "legal_form": ("legal_form", "text"),
    "register_status": ("register_status", "register_status"),
    "wz_code": ("wz_code", "wz_code"),
    "established_at": ("established_at", "date"),
    "business_purpose": ("business_purpose", "text"),
    "employee_count": ("employee_count", "integer"),
    "revenue": ("revenue", "number"),
    "technology_field": ("technology_field", "technology_field"),
    "website": ("website", "url"),
    "management": ("ceo_name_or_contact", "text"),
    "deeptech": ("deeptech_class", "text"),
    "is_ai": ("is_ai", "boolean"),
    "activity_keywords": ("activity_keywords", "text"),
}
#: Voted on in the app, never corrected into a value. See the module docstring.
NOT_CORRECTABLE = {"fundings", "register_announcement", "registration_number"}
#: Shown as several values at once: a vote is about one of them, and confirming one denies no other.
LIST_FIELDS = {"management", "activity_keywords"}

_WZ_CODE = re.compile(r"^\d{2}(\.\d{1,2}(\.\d{1,2})?)?$")
_THOUSANDS = re.compile(r"^\d{1,3}(\.\d{3})+$")
_HOST = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$")
_TRUE, _FALSE = {"true", "ja", "yes", "1"}, {"false", "nein", "no", "0"}


def confidence(agree: int, disagree: int, *, filled: bool = True) -> float:
    """The confidence for a correction `agree` people typed and `disagree` people voted against.

    `filled`: another source has a value for the field. Where none has, one person is enough: there
    is nothing to protect, and a gap filled is better than a gap.
    """
    net = agree - disagree
    if net <= 0:
        return 0.0
    if net == 1 and not filled:
        return 0.60
    return {1: 0.30, 2: 0.60}.get(net, 0.75)


def _technology_fields() -> frozenset[str]:
    from enrichment.branch_classifier import load_taxonomy

    return frozenset(load_taxonomy().english_names())


def clean(kind: str, raw: str, technology_fields: frozenset[str] = frozenset()) -> str | float | None:
    """A correction as the observation stores it, or None when it is not a valid value of `kind`."""
    text = " ".join(raw.split())
    if not text:
        return None
    if kind == "text":
        return text
    if kind == "url":
        parts = urlparse(text if "://" in text else f"https://{text}")
        host = to_ascii_host(parts.netloc.lower())
        if parts.scheme not in {"http", "https"} or "." not in host or not _HOST.match(host):
            return None
        return f"{parts.scheme}://{host}"
    if kind == "technology_field":
        return text if text in technology_fields else None
    if kind == "register_status":
        return text if text in REGISTER_STATUSES else None
    if kind == "wz_code":
        return text if _WZ_CODE.match(text) else None
    if kind == "boolean":
        word = text.casefold()
        return "true" if word in _TRUE else "false" if word in _FALSE else None
    if kind == "date":
        for parse in (date.fromisoformat, lambda s: datetime.strptime(s, "%d.%m.%Y").date()):
            try:
                return parse(text).isoformat()
            except ValueError:
                pass
        return None
    if kind in {"integer", "number"}:
        # German notation, as the app is German: "1.200" is 1200, "1.200,5" is 1200.5.
        german = "," in text or _THOUSANDS.match(text)
        number = text.replace(".", "").replace(",", ".") if german else text
        try:
            value = float(number)
        except ValueError:
            return None
        if value < 0 or (kind == "integer" and not value.is_integer()):
            return None
        return value
    raise ValueError(f"unknown kind {kind!r}")


def planned_observations(
    votes: Iterable[Mapping[str, Any]], technology_fields: frozenset[str] = frozenset(),
    filled: set[tuple[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Every correction people stand behind, with its confidence; and what was skipped, counted.

    `votes` are `extraction_feedback` rows: company_id, field_name, value (what the person saw),
    verdict, correction, author_id, in the order they were last changed. Only a person's latest vote
    per company and field counts (per shown value on `LIST_FIELDS`), so a changed mind replaces the
    old vote. Corrections are grouped
    per company, field and cleaned value, ignoring case, so "Muster GmbH" and "muster gmbh" are one
    claim (the spelling seen first wins; the stored observation keeps its own).
    `filled` holds the (company, observation field) pairs another source has a value for
    (`filled_fields`); None treats every field as filled.
    """
    latest: dict[tuple[str, str, str, str | None], Mapping[str, Any]] = {}
    for vote in votes:
        shown_item = vote.get("value") if vote["field_name"] in LIST_FIELDS else None
        person = (str(vote["company_id"]), vote["field_name"], vote["author_id"], shown_item)
        latest.pop(person, None)
        latest[person] = vote

    agree: dict[tuple[str, str, Any], set[str]] = defaultdict(set)
    spelling: dict[tuple[str, str, Any], str | float] = {}
    confirmed: dict[tuple[str, str], dict[Any, set[str]]] = defaultdict(lambda: defaultdict(set))
    rejected: dict[tuple[str, str, Any], set[str]] = defaultdict(set)
    skipped: dict[str, int] = defaultdict(int)
    for (company, key, _, _), vote in latest.items():
        if key in NOT_CORRECTABLE or key not in FIELDS:
            if vote.get("correction"):
                skipped[f"not correctable: {key}"] += 1
            continue
        author = vote["author_id"]
        shown = clean(FIELDS[key][1], vote.get("value") or "", technology_fields)
        if shown is not None:
            seen = (company, key, claim_key(shown))
            if vote["verdict"] == "correct":
                confirmed[(company, key)][seen[2]].add(author)
                rejected[seen].discard(author)
            else:
                rejected[seen].add(author)
                agree.get(seen, set()).discard(author)
                confirmed.get((company, key), {}).get(seen[2], set()).discard(author)
        if not vote.get("correction"):
            continue
        value = clean(FIELDS[key][1], vote["correction"], technology_fields)
        if value is None:
            skipped[f"invalid {key}"] += 1
            continue
        claim = (company, key, claim_key(value))
        agree[claim].add(author)
        rejected[claim].discard(author)
        spelling.setdefault(claim, value)

    planned = []
    for claim, typed in sorted(agree.items(), key=lambda item: str(item[0])):
        company, key, value = claim
        field_confirmed = confirmed.get((company, key), {})
        people = typed | field_confirmed.get(value, set())
        against = rejected[claim] if key in LIST_FIELDS else rejected[claim].union(
            *(authors for other, authors in field_confirmed.items() if other != value))
        planned.append({
            "company_id": company, "field_name": FIELDS[key][0], "value": spelling[claim],
            "confidence": confidence(len(people), len(against),
                                     filled=filled is None or (company, FIELDS[key][0]) in filled),
            "agree": len(people), "disagree": len(against),
        })
    return planned, dict(skipped)


def claim_key(value: str | float) -> str | float:
    """What makes two corrections the same claim: text ignoring case, numbers as numbers."""
    return value.casefold() if isinstance(value, str) else value


def filled_fields(cur: Any, votes: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """(company, observation field) pairs another source has a value for, above the coordinator's bar.

    Asked on every run, not when the vote was cast: when a source delivers a value later, a single
    person's correction steps back to 0.30 and the source's value is shown.
    """
    from pipeline_coordinator.resolver import _FIELD_MIN_CONF, _WEBSITE_MIN_CONF

    companies = sorted({str(v["company_id"]) for v in votes if v.get("correction")})
    if not companies:
        return set()
    cur.execute(
        """
        SELECT DISTINCT o.company_id::text,
               CASE WHEN o.field_name = 'candidate_url' THEN 'website' ELSE o.field_name END
        FROM public.company_field_observations o
        JOIN public.sources s ON s.id = o.source_id
        WHERE s.key <> %s
          AND o.company_id = ANY(%s::uuid[])
          AND o.field_name = ANY(%s)
          AND COALESCE(o.confidence, 0) >= CASE WHEN o.field_name IN ('website', 'candidate_url')
                                                THEN %s ELSE %s END
        """,
        (SOURCE_KEY, companies, [f for f, _ in FIELDS.values()] + ["candidate_url"],
         _WEBSITE_MIN_CONF, _FIELD_MIN_CONF),
    )
    return {(row[0], row[1]) for row in cur.fetchall()}


def run_publish() -> int:
    """Write every correction as an observation and demote the ones nobody stands behind any more.

    Returns how many observations changed, so the coordinator is only triggered when it matters.
    """
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(cur, key=SOURCE_KEY, name="Corrections from the app",
                                      source_type="user_feedback", metadata={"dag_id": DAG_ID})
            sync_run_id = start_sync_run(cur, source_id=source_id)
            cur.execute("SELECT company_id, field_name, value, verdict, correction, author_id"
                        " FROM public.extraction_feedback ORDER BY updated_at, created_at, id")
            columns = [c.name for c in cur.description]
            votes = [dict(zip(columns, row)) for row in cur.fetchall()]
            planned, skipped = planned_observations(votes, _technology_fields(), filled_fields(cur, votes))

            cur.execute(
                """
                SELECT company_id::text, field_name, value_text, value_number, id, confidence
                FROM public.company_field_observations WHERE source_id = %s
                """,
                (source_id,),
            )
            # Numbers compared as numbers: their text differs between Python and Postgres ("12" / "12.0").
            existing = {(r[0], r[1], claim_key(r[2] if r[2] is not None else float(r[3]))): (r[4], float(r[5] or 0))
                        for r in cur.fetchall()}

            records: dict[str, str] = {}
            written: set[str] = set()
            created = changed = 0
            for row in planned:
                company = row["company_id"]
                if company not in records:
                    records[company], _ = upsert_source_record(
                        cur, company_id=company, source_id=source_id,
                        external_id=f"{SOURCE_KEY}:{company}", external_url=None,
                        source_company_name="", source_payload={},
                    )
                value = row["value"]
                is_number = isinstance(value, float)
                metadata = {"users_agreeing": row["agree"], "users_disagreeing": row["disagree"],
                            "decided_by": f"corrected by {row['agree']} people in the app"}
                stored = existing.get((company, row["field_name"], claim_key(value)))
                if stored is None:
                    cur.execute(
                        """
                        INSERT INTO public.company_field_observations
                          (company_id, company_source_record_id, source_id, source_sync_run_id,
                           field_name, value_text, value_number, confidence, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (company, records[company], source_id, sync_run_id, row["field_name"],
                         None if is_number else value, value if is_number else None,
                         row["confidence"], Json(metadata)),
                    )
                    written.add(str(cur.fetchone()[0]))
                    created += 1
                    continue
                obs_id = stored[0]
                written.add(str(obs_id))
                cur.execute(
                    "UPDATE public.company_field_observations SET confidence = %s, metadata = %s, updated_at = now()"
                    " WHERE id = %s AND (confidence IS DISTINCT FROM %s OR metadata IS DISTINCT FROM %s)",
                    (row["confidence"], Json(metadata), obs_id, row["confidence"], Json(metadata)),
                )
                changed += cur.rowcount

            # A correction nobody stands behind any more: votes withdrawn or changed.
            stale = [obs_id for obs_id, conf in existing.values() if str(obs_id) not in written and conf > 0]
            if stale:
                cur.execute(
                    "UPDATE public.company_field_observations SET confidence = 0, updated_at = now() WHERE id = ANY(%s::uuid[])",
                    ([str(s) for s in stale],),
                )
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(votes), records_created=created,
                            records_updated=changed + len(stale), records_failed=sum(skipped.values()))
        log.info("%d votes: %d corrections planned, %d new, %d changed, %d withdrawn; skipped %s.",
                 len(votes), len(planned), created, changed, len(stale), skipped or "none")
        return created + changed + len(stale)
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------


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
        description="People's corrections from the app as observations of the source user_feedback",
        schedule="@hourly", start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "feedback"],
    ) as dag:
        # Returns how many observations changed, as XCom (see website_discovery): 0 skips the coordinator.
        publish = pipeline_task("publish_corrections", run_publish, returns=True)
        anything_changed = ShortCircuitOperator(
            task_id="anything_changed", python_callable=lambda ti: bool(ti.xcom_pull(task_ids="publish_corrections")),
        )
        coordinate = TriggerDagRunOperator(
            task_id="trigger_pipeline_coordinator", trigger_dag_id="pipeline_coordinator",
            wait_for_completion=False,
        )
        publish >> anything_changed >> coordinate
    return dag


dag = _make_dag()
