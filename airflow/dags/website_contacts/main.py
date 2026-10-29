"""Website contacts: the company's own email address and phone number, read off its Impressum.

Target column 18 ("Kontaktdaten Unternehmen"), the field Fusionbase also reads from company
websites. Every German business site must name an address and a way to reach it (§ 5 DDG), so the
Impressum of a website we have already proven to be the company's is the place to look.

Each run reads one batch of companies that have a published website and have not been read by this
source yet, and writes what it finds as `email` and `phone` observations of the source
`impressum_contacts`.

**An email address counts only when it is the company's, not a person's:**

- its domain is the website's domain (or one is a subdomain of the other). An address at another
  domain belongs to the web agency, a host or a parent company as often as to the company;
- its local part is not shaped like a person: `max.mustermann@`, `m.mustermann@` (two words with a
  dot, hyphen or underscore between them) are left out, and so is any address whose local part
  contains a word of a name the same Impressum lists as a manager. `info@`, `kontakt@`, `office@`
  pass. This is a shape, not a list of words, and it errs towards leaving an address out:
  `vertrieb-berlin@` is dropped too.

**Not published.** A phone number, and a role address of a sole trader, are still personal data.
So `email` and `phone` are stored, never resolved: nothing in `pipeline_coordinator` resolves
them, and they reach neither the app nor `v_field_provenance`.

Each company is read once, like `manager_ner`: `email` and `phone` have no upsert key, so reading
again would append a second copy. A read that failed (the site did not answer, or no Impressum was
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

from source_enrichment_common import (
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
ENABLED_FLAG = "ENABLE_WEBSITE_CONTACTS"
BATCH_SIZE = int(os.getenv("WEBSITE_CONTACTS_BATCH_SIZE", "50"))
#: Nothing has measured how often these are right. 0.6 is what `manager_ner` gives an unmeasured
#: Impressum reading: above the resolver's 0.5 floor, below anything with register evidence.
CONFIDENCE = 0.6
RULE = "own-domain-no-person-2026-10-28"

#: The separators that join a first and a last name in a mailbox: `max.mustermann`, `m_mustermann`.
_LOCAL_PART_SPLIT = re.compile(r"[._\-]")


def _require_enabled() -> None:
    if os.getenv(ENABLED_FLAG, "false").lower() != "true":
        raise RuntimeError(f"{ENABLED_FLAG} is not set to true.")


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


def company_email(emails: list[str], website: str, names_on_page: list[str]) -> str | None:
    """The first address on the page that is at the website's domain and not a person's."""
    domain = site_domain(website)
    for address in emails:
        local_part, _, email_domain = address.partition("@")
        if same_domain(email_domain, domain) and not looks_personal(local_part, names_on_page):
            return address
    return None


def contacts_from_impressum(text: str, website: str) -> dict[str, str]:
    """The company email and phone on one Impressum's plain text; a key is absent when not found."""
    from enrichment.imprint_extractor import extract_ceo_names, extract_emails, extract_phone

    found: dict[str, str] = {}
    email = company_email(extract_emails(text), website, extract_ceo_names(text))
    if email:
        found["email"] = email
    phone = extract_phone(text)
    if phone:
        found["phone"] = phone
    return found


def read_impressum(website: str) -> tuple[str, str | None]:
    """The company's Impressum as plain text and the URL it was read from; ("", None) when none is found."""
    from enrichment.http_client import fetch_impressum
    from enrichment.imprint_extractor import page_text

    found = fetch_impressum(website)
    return (page_text(found[0]), found[1]) if found else ("", None)


def run_read() -> None:
    _require_enabled()
    connection = get_connection()
    try:
        with connection, connection.cursor() as cur:
            source_id = ensure_source(
                cur, key=SOURCE_KEY, name="Impressum contacts", source_type="website",
                metadata={"dag_id": DAG_ID, "rule": RULE},
            )
            sync_run_id = start_sync_run(cur, source_id=source_id)
            companies = companies_with_website_to_read(cur, source_id=source_id, limit=BATCH_SIZE)

        written = failed = 0
        for company_id, company_name, website, attempts in companies:
            text, imprint_url = "", None
            try:
                text, imprint_url = read_impressum(website)
                found = contacts_from_impressum(text, website) if text else {}
            except Exception as exc:  # noqa: BLE001  one unreachable site must not stop the batch
                log.warning("Impressum not readable for %s: %s", company_id, type(exc).__name__)
                text, found = "", {}
            payload = {"rule": RULE, "imprint_url": imprint_url, "found": sorted(found)}
            if not text:
                payload, failed = failed_read_payload(payload, attempts), failed + 1
            with connection, connection.cursor() as cur:
                # Written even when nothing was found, so the company counts as read (unless `failed`).
                record_id, _ = upsert_source_record(
                    cur, company_id=company_id, source_id=source_id,
                    external_id=f"{SOURCE_KEY}:{company_id}", external_url=website,
                    source_company_name=company_name, source_payload=payload,
                )
                for field_name, value in found.items():
                    insert_text_observation(
                        cur, company_id=company_id, source_record_id=record_id, source_id=source_id,
                        sync_run_id=sync_run_id, field_name=field_name, value_text=value,
                        confidence=CONFIDENCE, metadata={"rule": RULE, "imprint_url": imprint_url},
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
