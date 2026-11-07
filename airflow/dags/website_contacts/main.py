"""Website contacts: the company's own email address and phone number, read off its Impressum.

Target column 18 ("Kontaktdaten Unternehmen"). Every German business site must name an address and a way to reach it (§ 5 DDG), so the
Impressum of a website we have already proven to be the company's is the place to look.

Each run reads one batch of companies that have a published website and have not been read by this
source yet, and writes what it finds as `email` and `phone` observations of the source
`impressum_contacts`.

**Every address on the Impressum is kept (research phase, decided 2026-11-07), each with a kind and
the confidence of that kind** (`EMAIL_CONFIDENCE`, in the observation's `method` and `evidence`):

- `role`: at the website's domain (or one a subdomain of the other) and not shaped like a person:
  `info@`, `kontakt@`, `office@`. The company's own address, as far as we can tell;
- `person`: at the website's domain but shaped like a person: `max.mustermann@`, `m.mustermann@`
  (two words joined by a dot, hyphen or underscore), or a word of a manager's name the same
  Impressum lists. A shape, not a list of words: `vertrieb-berlin@` lands here too;
- `other_domain`: any other domain. The web agency, a host, a data-protection officer or a parent
  company as often as the company.

The confidences are guesses until people have judged them: a Prüfen round of `email` (`review_round`)
breaks its precision down by confidence and by evidence, that is by kind.

**Not published.** A phone number, and a role address of a sole trader, are still personal data.
So `email` and `phone` are stored, never resolved: nothing in `pipeline_coordinator` resolves
them, and they reach neither the history nor `v_field_provenance`. Emails alone are listed on the
app's company page through the view `v_company_emails` (`V6__company_emails.sql`, research phase,
needs a privacy review before production); phones are never shown.

Each company is read once per `READ_KEY`: `email` and `phone` have no upsert key, so a read first
deletes what this source stored for the company before, and writes it again. A company read under an
older rule (its record lacks `READ_KEY`) is read again. A read that failed (the site did not answer, or no Impressum was
found) does not count: it is marked `failed` and tried again on a later run, at most
`MAX_READ_ATTEMPTS` times. A company whose website changes keeps the contacts read from the
old one until that key exists (a migration plus `insert_text_observation`, same commit).
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from enrichment import corpus
from source_enrichment_common import (
    SourceDetails,
    companies_with_website_to_read,
    ensure_source,
    failed_read_payload,
    finish_sync_run,
    get_connection,
    insert_text_observation,
    start_sync_run,
    upsert_source_record,
)

log = logging.getLogger(__name__)

DAG_ID = "website_contacts"
SOURCE_KEY = "impressum_contacts"
#: What the app's source page says about this source (German).
DETAILS = SourceDetails(
    description=(
        "Alle E-Mail-Adressen und die Telefonnummer aus dem Impressum der belegten Website. "
        "Jede Adresse mit Art und Sicherheit: Firmenadresse, Personenadresse oder andere Domain. "
        "E-Mails stehen auf der Firmenseite, Telefonnummern nicht."
    ),
    schedule="nach der Website-Suche",
)
BATCH_SIZE = int(os.getenv("WEBSITE_CONTACTS_BATCH_SIZE", "50"))
#: Nothing has measured how often these are right; Prüfen rounds will. `role` keeps the 0.6
#: `manager_ner` gives an unmeasured Impressum reading; the other kinds are guessed lower.
EMAIL_CONFIDENCE = {"role": 0.6, "person": 0.4, "other_domain": 0.2}
PHONE_CONFIDENCE = 0.6
RULE = "all-emails-2026-11-07"
#: The payload key of a read under this rule: a record without it is read again.
READ_KEY = "emails"

#: The separators that join a first and a last name in a mailbox: `max.mustermann`, `m_mustermann`.
_LOCAL_PART_SPLIT = re.compile(r"[._\-]")


def site_domain(website: str) -> str:
    """The website's host, lowercased, without `www.`."""
    return (urlparse(website).hostname or "").lower().removeprefix("www.")


def same_domain(email_domain: str, domain: str) -> bool:
    """Equal, or one a subdomain of the other: `info@firma.de` on `shop.firma.de` and back."""
    if not email_domain or not domain:
        return False
    return (
        email_domain == domain
        or email_domain.endswith("." + domain)
        or domain.endswith("." + email_domain)
    )


def looks_personal(local_part: str, names_on_page: list[str]) -> bool:
    """Is this mailbox a person's? Two words joined by `.`, `_` or `-`, or a manager's name in it."""
    words = [w for w in _LOCAL_PART_SPLIT.split(local_part) if w]
    if len(words) >= 2 and all(w.isalpha() for w in words):
        return True
    for name in names_on_page:
        for word in re.split(r"[\s\-]+", name.lower()):
            word = word.strip(".,")
            if len(word) >= 3 and word in local_part:
                return True
    return False


def email_kind(address: str, website: str, names_on_page: list[str]) -> str:
    """`role`, `person` or `other_domain` (see the module docstring)."""
    local_part, _, email_domain = address.partition("@")
    if not same_domain(email_domain, site_domain(website)):
        return "other_domain"
    return "person" if looks_personal(local_part, names_on_page) else "role"


def contacts_from_impressum(text: str, website: str) -> list[tuple[str, str, str, float]]:
    """Every email and the phone on one Impressum's plain text: (field, value, kind, confidence)."""
    from enrichment.imprint_extractor import extract_ceo_names, extract_emails, extract_phone

    names = extract_ceo_names(text)
    found = []
    for address in extract_emails(text):
        kind = email_kind(address, website, names)
        found.append(("email", address, kind, EMAIL_CONFIDENCE[kind]))
    phone = extract_phone(text)
    if phone:
        found.append(("phone", phone, "impressum", PHONE_CONFIDENCE))
    return found


def read_impressum(website: str) -> tuple[str, str | None]:
    """The company's Impressum as plain text and the URL it was read from; ("", None) when none is found."""
    from enrichment.http_client import fetch_impressum
    from enrichment.imprint_extractor import page_text

    found = fetch_impressum(website)
    return (page_text(found[0]), found[1]) if found else ("", None)


def run_read() -> None:
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur, key=SOURCE_KEY, name="Impressum contacts", source_type="website",
                metadata={"dag_id": DAG_ID, "rule": RULE}, details=DETAILS)
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = companies_with_website_to_read(cur, source_id=source_id, limit=BATCH_SIZE,
                                                       read_key=READ_KEY)

        written = failed = 0
        for company_id, company_name, website, attempts in companies:
            text, imprint_url = "", None
            try:
                text, imprint_url = read_impressum(website)
                found = contacts_from_impressum(text, website) if text else []
            except Exception as exc:  # noqa: BLE001  one unreachable site must not stop the batch
                log.warning("Impressum not readable for %s: %s", company_id, type(exc).__name__)
                text, found = "", []
            # Counts only: the addresses themselves stay in the observations.
            payload = {"rule": RULE, "imprint_url": imprint_url,
                       READ_KEY: sum(1 for f in found if f[0] == "email"),
                       "phone": any(f[0] == "phone" for f in found)}
            if not text:
                payload, failed = failed_read_payload(payload, attempts), failed + 1
            with connection, connection.cursor() as cur:
                # Written even when nothing was found, so the company counts as read (unless `failed`).
                record_id, _ = upsert_source_record(
                    cur, company_id=company_id, source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{company_id}", external_url=website,
                    source_company_name=company_name, source_payload=payload,
                )
                # The Impressum is in the company corpus (S3, via fetch_page); this says it is this company's.
                if imprint_url:
                    corpus.record_document(cur, company_id=company_id, source_id=source_id, kind="webpage",
                                           url=imprint_url, namespace="page")
                if text:
                    # A new read replaces the last one: these fields have no upsert key.
                    cur.execute(
                        "DELETE FROM public.company_field_observations"
                        " WHERE company_id = %s AND source_id = %s AND field_name IN ('email', 'phone')",
                        (company_id, source_id),
                    )
                for field_name, value, kind, confidence in found:
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=field_name, value_text=value,
                        confidence=confidence,
                        metadata={"rule": RULE, "imprint_url": imprint_url, "method": kind, "evidence": [kind]},
                    )
                    written += 1

        with connection, connection.cursor() as cur:
            finish_sync_run(cur, sync_run_id=sync_run_id, records_seen=len(companies),
                            records_created=written, records_updated=0, records_failed=failed)
        log.info("Read %d Impressums: %d contacts written, %d unreadable.", len(companies), written, failed)
    finally:
        connection.close()


# --- DAG ------------------------------------------------------------------------------------


def _make_dag():  # noqa: ANN201
    try:
        from airflow import DAG
        from pipeline_task import pipeline_task
    except ImportError:
        return None

    # No coordinator trigger: nothing resolves `email` or `phone`.
    with DAG(
        dag_id=DAG_ID,
        description="Company email and phone from the Impressum of its proven website",
        schedule=None, start_date=datetime(2026, 10, 1, tzinfo=timezone.utc), catchup=False,
        max_active_runs=1, tags=["sources", "websites", "contacts"],
    ) as dag:
        pipeline_task("read_contacts", run_read)
    return dag


dag = _make_dag()
