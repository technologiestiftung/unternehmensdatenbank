-- The Northdata export, kept whole, in a table of its own.
--
-- Until now the export has been treated as an import formality: `prepare_company_import.py` takes
-- a handful of columns into `public.companies` and drops everything else into `metadata` as loose
-- JSON, where nothing reads it. That has cost real work twice.
--
--   * `register_status` sat in `metadata ->> 'status'` from day one. More than half the table was
--     dissolved companies, and every URL guess, page download and model call paid for them until
--     V13 promoted the column on 2026-08-27.
--
--   * `website` is doing it right now. `metadata ->> 'website'` holds 3,283 URLs, 3,027 of them on
--     active companies. Discovery has produced 1,109 canonical websites after weeks of work and
--     roughly 1,400 paid searches. 2,394 of the imported ones are for companies discovery has
--     never found a site for. Where both exist they agree 76.7% of the time (682 of 889).
--
-- So the export lands whole, and the parts nobody wants today are still here next year. The
-- scalar columns below are for querying; `raw` holds the untouched record, so the nested halves —
-- financials, history, sheets, relatedPersons, relatedCompanies, 230 leaf paths in all — stay
-- reachable without being modelled by somebody guessing at what matters.
--
-- This table feeds NOTHING. It writes no observations and changes no canonical field. Whether an
-- unverified third-party URL should outrank a page carrying the company's own register number is a
-- separate decision, and the 207 companies where the two disagree are the evidence for it.

CREATE TABLE IF NOT EXISTS public.northdata_export (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Provenance. Keyed on (source_file, internal_id) rather than internal_id alone, because two
  -- exports a week apart already exist and keeping both is what makes "what did Northdata change"
  -- answerable. Same shape as company_source_records' (source_id, external_id). Re-loading one
  -- file upserts its own rows and leaves the other snapshot untouched.
  source_file         TEXT NOT NULL,
  imported_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  internal_id         TEXT NOT NULL,

  -- Matched from `companies.metadata ->> 'internal_id'`, which is unique across all 14,443 rows —
  -- unlike `unique_key` and `registration_number`, which each carry 158 duplicates.
  --
  -- Nullable and deliberately NOT a foreign key. A Berlin export may land before its companies are
  -- imported; with an FK that load fails outright, and losing the arrival of data is worse than
  -- holding it unmatched. NULL means "no company matched yet", never "the load went wrong".
  company_id          UUID,

  name                TEXT,
  legal_form          TEXT,

  -- CAREFUL. This column speaks ENGLISH: active / terminated / liquidation.
  -- `companies.register_status` speaks GERMAN for the same fact: aktiv / erloschen / in Liquidation.
  -- A query joining the two and comparing the strings matches nothing at all, and reports a clean
  -- zero rather than an error. Compare deliberately or not at all.
  --
  -- No CHECK constraint, for V13's reason: Berlin may carry a fourth value, a failing import is
  -- worse than an unexpected string, and an unknown value shows up immediately in a GROUP BY.
  status              TEXT,

  register_id         TEXT,
  register_city       TEXT,
  register_unique_key TEXT,
  eu_id               TEXT,

  street              TEXT,
  postal_code         TEXT,
  city                TEXT,
  state               TEXT,
  country             TEXT,
  address_formatted   TEXT,
  lat                 DOUBLE PRECISION,
  lng                 DOUBLE PRECISION,

  -- The contact block. NOT top-level fields in the source: every one of these lives at
  -- `extras[].items[]`, identified by that item's `id`. Anyone looking for a `Website` key in the
  -- JSONL finds none and concludes the export carries no websites. It carries 3,283.
  website             TEXT,
  email               TEXT,
  phone               TEXT,
  fax                 TEXT,
  vat_id              TEXT,

  -- Arrays rather than joined strings: a company can hold several codes and `= ANY(...)` is the
  -- query everybody wants. `wz` is the one that lines up with `companies.wz_code`.
  nace                TEXT[],
  nace2025            TEXT[],
  wz                  TEXT[],
  wz2025              TEXT[],

  -- The register purpose, full length. Not truncated here: the 40-character floor and the 2,000
  -- character Excel cap are presentation concerns and belong to whatever presents it.
  subject             TEXT,

  north_data_url      TEXT,
  last_update         TIMESTAMPTZ,

  raw                 JSONB NOT NULL,

  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS northdata_export_file_internal_id_key
  ON public.northdata_export (source_file, internal_id);

CREATE INDEX IF NOT EXISTS northdata_export_internal_id_idx
  ON public.northdata_export (internal_id);

CREATE INDEX IF NOT EXISTS northdata_export_company_id_idx
  ON public.northdata_export (company_id);

CREATE INDEX IF NOT EXISTS northdata_export_status_idx
  ON public.northdata_export (status);

-- Partial, because "which companies came with a website" is the first query anybody writes and it
-- touches 3,283 rows out of 14,456.
CREATE INDEX IF NOT EXISTS northdata_export_website_idx
  ON public.northdata_export (company_id)
  WHERE website IS NOT NULL;

DROP TRIGGER IF EXISTS northdata_export_touch_updated_at ON public.northdata_export;
CREATE TRIGGER northdata_export_touch_updated_at
  BEFORE UPDATE ON public.northdata_export
  FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

COMMENT ON TABLE public.northdata_export IS
  'One row per company per Northdata export file, kept verbatim. A LANDING table: it feeds nothing '
  'and no canonical field reads it. Keyed (source_file, internal_id) so several snapshots coexist. '
  'Osnabrück 2026-04-01: 14,456 rows, 3,283 with a website, 3,677 with an email.';

COMMENT ON COLUMN public.northdata_export.status IS
  'English: active / terminated / liquidation. companies.register_status is German for the same '
  'fact: aktiv / erloschen / in Liquidation. Comparing them as strings matches nothing.';

COMMENT ON COLUMN public.northdata_export.website IS
  'From extras[].items[] where id = ''url'' — there is no Website field at the top level of the '
  'source. 3,283 of 14,456 rows carry one; 3,027 of those are on active companies.';

COMMENT ON COLUMN public.northdata_export.company_id IS
  'Matched on companies.metadata ->> ''internal_id''. Nullable with no FK on purpose: an export may '
  'arrive before its companies do, and NULL means unmatched, not failed.';

COMMENT ON COLUMN public.northdata_export.raw IS
  'The untouched record. The scalar columns above cover the flat half; financials, history, sheets, '
  'relatedPersons and relatedCompanies are only in here.';
