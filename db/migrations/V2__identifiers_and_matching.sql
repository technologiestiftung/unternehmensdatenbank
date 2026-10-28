-- Identifiers and matching: how a record from any source finds its company (issue #18).
--
-- A company's id is a random UUID and never changes. A source record is linked to a company by
-- matching, in three steps:
--
--   1. a shared IDENTIFIER (register number, Fusionbase id, VAT id, ...)  -> link, confidence 1.0
--   2. a fuzzy score on name + postcode + street + domain + phone, high    -> link, confidence = score
--      middle band                                                         -> match_candidates, a person decides
--   3. low                                                                 -> a new company
--
-- Every link records how it was made, so a wrong one can be found and undone. Merging two companies
-- never deletes: the loser keeps `merged_into`, its source records and observations move over.

-- ------------------------------------------------------------------------------------------------
-- Identifiers: every way a source names a company. Several per company, each unique per scheme.
-- ------------------------------------------------------------------------------------------------
CREATE TABLE public.company_identifiers (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    company_id uuid NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    -- What kind of identifier. Kept to a known list so a typo cannot create a second scheme.
    scheme text NOT NULL CHECK (scheme IN (
        'register',          -- court + type + number, as the register writes it: "Berlin (Charlottenburg) HRB 94315"
        'register_former',   -- a number the company had before moving court or being re-registered
        'opencorporates',    -- OffeneRegister / OpenCorporates company number: "F1103R_HRB94315"
        'fusionbase',        -- Fusionbase's own id, once it delivers
        'vat',               -- USt-IdNr.
        'lei'                -- Legal Entity Identifier
    )),
    value text NOT NULL,
    -- Which source said so; NULL for identifiers that came with the company's first load.
    source_id uuid REFERENCES public.sources(id) ON DELETE SET NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT uq_company_identifiers_scheme_value UNIQUE (scheme, value)
);

CREATE INDEX company_identifiers_company_id_idx ON public.company_identifiers (company_id);

COMMENT ON TABLE public.company_identifiers IS
  'Every identifier a source uses for a company. Matching tries these first; (scheme, value) is unique, so one identifier can never point at two companies.';

-- ------------------------------------------------------------------------------------------------
-- How a source record was linked to its company.
-- ------------------------------------------------------------------------------------------------
ALTER TABLE public.company_source_records
    ADD COLUMN match_method text CHECK (match_method IN ('identifier', 'fuzzy', 'manual', 'created')),
    ADD COLUMN match_confidence numeric(4,3) CHECK (match_confidence BETWEEN 0 AND 1),
    ADD COLUMN matched_at timestamp with time zone;

COMMENT ON COLUMN public.company_source_records.match_method IS
  'identifier: a shared identifier; fuzzy: name/address score; manual: a person decided; created: no match, the record created the company. NULL for records linked before matching existed.';

-- ------------------------------------------------------------------------------------------------
-- Merging: the company that lost keeps a pointer to the one that survived.
-- ------------------------------------------------------------------------------------------------
ALTER TABLE public.companies
    ADD COLUMN merged_into uuid REFERENCES public.companies(id) ON DELETE SET NULL,
    ADD CONSTRAINT companies_not_merged_into_self CHECK (merged_into IS DISTINCT FROM id);

CREATE INDEX companies_merged_into_idx ON public.companies (merged_into) WHERE merged_into IS NOT NULL;

COMMENT ON COLUMN public.companies.merged_into IS
  'Set when this company turned out to be the same as another. Its source records and observations moved there; the row stays so old links keep resolving.';

-- ------------------------------------------------------------------------------------------------
-- The review queue: fuzzy matches too uncertain to link and too likely to ignore.
-- ------------------------------------------------------------------------------------------------
CREATE TABLE public.match_candidates (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    source_record_id uuid NOT NULL REFERENCES public.company_source_records(id) ON DELETE CASCADE,
    company_id uuid NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    score numeric(4,3) NOT NULL CHECK (score BETWEEN 0 AND 1),
    -- What agreed and what did not, e.g. {"name": 0.91, "postcode": true, "street": false}.
    reasons jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'open' NOT NULL CHECK (status IN ('open', 'accepted', 'rejected')),
    -- The Keycloak user id of whoever decided; set together with decided_at.
    decided_by text,
    decided_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT uq_match_candidates_record_company UNIQUE (source_record_id, company_id),
    CONSTRAINT match_candidates_decision_complete CHECK ((status = 'open') = (decided_at IS NULL))
);

CREATE INDEX match_candidates_open_idx ON public.match_candidates (score DESC) WHERE status = 'open';

-- ------------------------------------------------------------------------------------------------
-- Pipeline state: what a DAG has done so far, in the database instead of a file.
-- ------------------------------------------------------------------------------------------------
-- On the hub, tasks run in pods with no shared disk, so a checkpoint file written by one run is gone
-- for the next. One row per (pipeline, key), e.g. ('website_discovery', 'last_company_id').
CREATE TABLE public.pipeline_state (
    pipeline text NOT NULL,
    key text NOT NULL,
    value jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    PRIMARY KEY (pipeline, key)
);

CREATE TRIGGER pipeline_state_touch_updated_at BEFORE UPDATE ON public.pipeline_state
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();
