-- The company corpus index (issue #61): which stored texts belong to which company.
--
-- The texts themselves are in S3 (`enrichment.corpus`), one object per document, latest version
-- only. This table is the index a trainer reads: "every text about company X", by kind and source.
--
-- PERSONAL DATA: a web page's text can name people (an Impressum). Stored in full, decided by the
-- developer on 2026-10-30. Not exposed through Hasura (no grant to the reader role); CLAUDE.md:
-- nothing of it leaves local development without a privacy review.

CREATE TABLE public.company_documents (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    company_id uuid NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    source_id uuid NOT NULL REFERENCES public.sources(id) ON DELETE CASCADE,
    kind text NOT NULL CHECK (kind IN ('webpage', 'wikipedia', 'wikidata_description')),
    url text NOT NULL,
    language text,
    -- When the source's text was read; a re-read overwrites the S3 object and this row.
    read_at timestamptz NOT NULL DEFAULT now(),
    s3_key text NOT NULL,
    sha256 text NOT NULL,
    bytes integer NOT NULL,
    UNIQUE (company_id, source_id, kind, url)
);
CREATE INDEX company_documents_company_idx ON public.company_documents (company_id, kind);
COMMENT ON TABLE public.company_documents IS
  'Index of the company corpus: every text read about a company (web pages, Wikipedia, Wikidata descriptions), stored in S3 under s3_key, latest version only (#61). Contains references to personal data; pipelines only, not in Hasura.';

GRANT SELECT, INSERT, UPDATE, DELETE ON public.company_documents TO ${writer_role};
