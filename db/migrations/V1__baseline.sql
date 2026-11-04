-- Baseline: the whole schema as of 2026-11-04, in one file.
--
-- Squashed five times: the 24 migrations of the Osnabrück era, then V1-V9 and V10 of the Berlin
-- rebuild (2026-10-28), then V1-V10 (2026-10-31, #68), then V1-V8 (2026-11-04). They live on in git
-- history; their reasoning is in the comments of the objects they created. In short:
--
--   identifiers and matching   company_identifiers, match columns on source records, merged_into,
--                              match_candidates, pipeline_state (issue #18)
--   feedback and provenance    a vote on every shown field, v_field_provenance for the ⓘ
--   grant links                company_funding_links: the list went from 71 s to 0.65 s a page
--   stored company profile     company_profile_mv + source_keys: every list filter under 10 ms
--   Berlin grants              company_fundings.source / recipient_register / policy_area
--   manager evidence           whether the register confirms a manager name
--   activity keywords          products and services from the company's own website
--   register announcements     the latest court announcement per company, a hint next to its status
--   source overview            stored counts per source for the app's sources page, never a value
--   register fields            name, address, status ... are observations; `companies` keeps
--                              identity only, and the register number is only an identifier
--   corrections                the app's votes are the source `user_feedback`, priority 50
--   profile                    English values, user tags, the register number in the profile
--   source dates               `source_updated_at`: when the source read a value; older weighs less
--   company history            append-only history of every shown value, every claim on demand (#54)
--   company documents          the index of the S3 corpus of every text read about a company (#61)
--   review sample              a fixed sample people check by hand, precision per field (#71)
--   Technologiefelder as tables technology_fields, technology_field_keywords, wz_divisions (#84)
--   source details             description, terms, schedule; confidence ranges and runs per source (#102)
--   founding year              established_year in the profile, from the register and schema.org
--
-- Generated from the play server's database (migrated to V6; V7 and V8 changed rows only) with
-- `pg_restore --schema-only --no-owner`, privileges included, so every grant is exactly what the
-- migrations left. Role names are Flyway placeholders (`${writer_role}`, `${reader_role}`) so the hub
-- can use its own, and default privileges apply to whoever runs this file. The materialized views are
-- created WITH DATA: refresh_company_profile() refreshes them CONCURRENTLY, which an unpopulated view
-- refuses. PostGIS and pgvector are created by the database setup, not here. The sources' text for the
-- app is in R__source_details.sql, which Flyway runs after this file.

-- pg_dump writes functions before the tables they read; their bodies are checked when called.
SET check_function_bodies = false;


--
-- Name: draw_review_sample(integer, integer, integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.draw_review_sample(websites integer DEFAULT 40, technology_fields integer DEFAULT 25, managers integer DEFAULT 20, registers integer DEFAULT 15) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  added integer;
BEGIN
  WITH candidates AS (
    SELECT p.id,
           CASE WHEN p.technology_field IS NOT NULL THEN 'technology_field'
                WHEN p.website IS NOT NULL THEN 'website'
                WHEN p.management IS NOT NULL THEN 'management'
                ELSE 'register' END AS stratum
    FROM public.company_profile_mv p
    WHERE p.register_status = 'active'
      AND NOT EXISTS (SELECT 1 FROM public.review_sample s WHERE s.company_id = p.id)
  ),
  ranked AS (
    SELECT id, stratum,
           row_number() OVER (PARTITION BY stratum ORDER BY md5(id::text || 'review-sample')) AS rn
    FROM candidates
  ),
  missing AS (
    SELECT t.stratum, t.target - COALESCE(have.n, 0) AS n
    FROM (VALUES ('website', websites), ('technology_field', technology_fields),
                 ('management', managers), ('register', registers)) t(stratum, target)
    LEFT JOIN (SELECT stratum, count(*) AS n FROM public.review_sample GROUP BY stratum) have
      ON have.stratum = t.stratum
  )
  INSERT INTO public.review_sample (company_id, stratum)
  SELECT r.id, r.stratum
  FROM ranked r JOIN missing m ON m.stratum = r.stratum
  WHERE r.rn <= m.n;
  GET DIAGNOSTICS added = ROW_COUNT;
  RETURN added;
END
$$;


--
-- Name: normalize_company_name(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.normalize_company_name(input_name text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
  WITH step_1 AS (
    SELECT lower(input_name) AS value
  ),
  step_2 AS (
    SELECT regexp_replace(value, '&', ' und ', 'g') AS value
    FROM step_1
  ),
  step_3 AS (
    SELECT regexp_replace(value, '[^[:alnum:]äöüß]+', ' ', 'g') AS value
    FROM step_2
  ),
  step_4 AS (
    SELECT regexp_replace(
      value,
      '\m(gmbh(\s+co\s+kg)?|ug|haftungsbeschraenkt|haftungsbeschränkt|ag|eg|kg|ohg|gbr|mbh|se|kgaa|ev)\M',
      ' ',
      'g'
    ) AS value
    FROM step_3
  )
  SELECT NULLIF(trim(regexp_replace(value, '\s+', ' ', 'g')), '')
  FROM step_4;
$$;


--
-- Name: FUNCTION normalize_company_name(input_name text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.normalize_company_name(input_name text) IS 'Normalizes company names for stable matching across legal forms and punctuation.';


--
-- Name: record_company_history(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.record_company_history() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  written integer;
  managers integer;
BEGIN
  -- Overlapping coordinator runs would both compare with the same latest row and write it twice.
  PERFORM pg_advisory_xact_lock(hashtext('record_company_history'));
  -- A change is what the app shows: value, source or confidence. A new observation id alone is not
  -- (scripts/import_register_managers.py re-inserts the same names), so nothing is recorded for it.
  -- One value per field: the canonical observation.
  WITH current AS (
    SELECT cfs.company_id, cfs.field_name,
           COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text) AS value,
           cfs.source_id, o.id AS observation_id, o.confidence
    FROM public.company_canonical_field_sources cfs
    JOIN public.company_field_observations o ON o.id = cfs.company_field_observation_id
  ),
  last AS (
    SELECT DISTINCT ON (h.company_id, h.field_name)
           h.company_id, h.field_name, h.event, h.value, h.source_id, h.observation_id, h.confidence
    FROM public.company_field_history h
    WHERE h.field_name <> 'management'
    ORDER BY h.company_id, h.field_name, h.changed_at DESC, h.id DESC
  )
  INSERT INTO public.company_field_history
    (company_id, field_name, event, value, previous_value, source_id, observation_id, confidence,
     previous_confidence)
  SELECT COALESCE(c.company_id, l.company_id), COALESCE(c.field_name, l.field_name),
         CASE WHEN c.company_id IS NULL THEN 'removed'
              WHEN l.company_id IS NULL OR l.event = 'removed' THEN 'shown'
              WHEN c.value IS DISTINCT FROM l.value THEN 'replaced'
              ELSE 'confidence_changed' END,
         c.value,
         CASE WHEN l.event = 'removed' THEN NULL ELSE l.value END,
         COALESCE(c.source_id, l.source_id), COALESCE(c.observation_id, l.observation_id),
         c.confidence,
         CASE WHEN l.event = 'removed' THEN NULL ELSE l.confidence END
  FROM current c
  FULL JOIN last l ON l.company_id = c.company_id AND l.field_name = c.field_name
  WHERE (c.company_id IS NULL AND l.event <> 'removed')
     OR (c.company_id IS NOT NULL AND (
           l.company_id IS NULL OR l.event = 'removed'
           OR c.value IS DISTINCT FROM l.value
           OR c.source_id IS DISTINCT FROM l.source_id
           OR c.confidence IS DISTINCT FROM l.confidence));
  GET DIAGNOSTICS written = ROW_COUNT;

  -- Several values per field: every current manager name, one history line per name.
  WITH current AS (
    SELECT m.company_id, m.full_name AS value, m.source_id, o.id AS observation_id, o.confidence
    FROM public.company_managers m
    LEFT JOIN public.company_field_observations o ON o.id = m.company_field_observation_id
    WHERE m.is_current
  ),
  last AS (
    SELECT DISTINCT ON (h.company_id, h.value)
           h.company_id, h.value, h.event, h.source_id, h.observation_id, h.confidence
    FROM public.company_field_history h
    WHERE h.field_name = 'management'
    ORDER BY h.company_id, h.value, h.changed_at DESC, h.id DESC
  )
  INSERT INTO public.company_field_history
    (company_id, field_name, event, value, previous_value, source_id, observation_id, confidence,
     previous_confidence)
  SELECT COALESCE(c.company_id, l.company_id), 'management',
         CASE WHEN c.company_id IS NULL THEN 'removed'
              WHEN l.company_id IS NULL OR l.event = 'removed' THEN 'shown'
              ELSE 'confidence_changed' END,
         COALESCE(c.value, l.value),
         NULL,
         COALESCE(c.source_id, l.source_id), COALESCE(c.observation_id, l.observation_id),
         c.confidence,
         CASE WHEN l.event = 'removed' THEN NULL ELSE l.confidence END
  FROM current c
  FULL JOIN last l ON l.company_id = c.company_id AND l.value = c.value
  WHERE (c.company_id IS NULL AND l.event <> 'removed')
     OR (c.company_id IS NOT NULL AND (
           l.company_id IS NULL OR l.event = 'removed'
           OR c.source_id IS DISTINCT FROM l.source_id
           OR c.confidence IS DISTINCT FROM l.confidence));
  GET DIAGNOSTICS managers = ROW_COUNT;
  RETURN written + managers;
END
$$;


--
-- Name: refresh_company_funding_links(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_company_funding_links() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  linked integer;
BEGIN
  DELETE FROM public.company_funding_links;
  INSERT INTO public.company_funding_links (fkz, company_id, match_method)
  SELECT f.fkz, i.company_id, 'register_number'
  FROM public.company_fundings f
  JOIN public.company_identifiers i
    ON i.scheme = 'register' AND i.value = 'Berlin (Charlottenburg) ' || f.recipient_register
  WHERE f.recipient_register IS NOT NULL;

  INSERT INTO public.company_funding_links (fkz, company_id)
  WITH unique_names AS (
    SELECT c.normalized_name, lower(c.city) AS city, (array_agg(c.id))[1] AS company_id
    FROM public.v_companies_enriched c
    WHERE c.city IS NOT NULL
    GROUP BY c.normalized_name, lower(c.city)
    HAVING count(*) = 1
  )
  SELECT f.fkz, u.company_id
  FROM public.company_fundings f
  JOIN unique_names u ON u.normalized_name = f.recipient_normalized_name AND u.city = lower(f.city)
  WHERE f.recipient_register IS NULL;

  SELECT count(*) INTO linked FROM public.company_funding_links;
  RETURN linked;
END
$$;


--
-- Name: refresh_company_profile(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_company_profile() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public', 'pg_temp'
    AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.company_profile_mv;
  ANALYZE public.company_profile_mv;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.source_stats_mv;
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.source_field_stats_mv;
END
$$;


--
-- Name: touch_updated_at(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.touch_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;


--
-- Name: FUNCTION touch_updated_at(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.touch_updated_at() IS 'Generic trigger function that refreshes updated_at on row updates.';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: companies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.companies (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    data_last_updated_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    merged_into uuid,
    CONSTRAINT companies_not_merged_into_self CHECK ((merged_into IS DISTINCT FROM id))
);


--
-- Name: TABLE companies; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.companies IS 'Company identity: a random UUID and merged_into. The register number is in company_identifiers (scheme register). Every shown value (name, address, status, ...) is an observation; read the published one through v_companies_enriched.';


--
-- Name: COLUMN companies.merged_into; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.companies.merged_into IS 'Set when this company turned out to be the same as another. Its source records and observations moved there; the row stays so old links keep resolving.';


--
-- Name: company_canonical_field_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_canonical_field_sources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    field_name text NOT NULL,
    source_id uuid NOT NULL,
    company_field_observation_id uuid NOT NULL,
    selected_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE company_canonical_field_sources; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_canonical_field_sources IS 'Selected winning source and observation for each canonical company field.';


--
-- Name: company_comments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_comments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    author_id text NOT NULL,
    body text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT company_comments_body_check CHECK (((length(TRIM(BOTH FROM body)) >= 1) AND (length(TRIM(BOTH FROM body)) <= 5000)))
);


--
-- Name: COLUMN company_comments.author_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.company_comments.author_id IS 'Keycloak `sub` of the writer, set by Hasura from the JWT, never by the client.';


--
-- Name: company_documents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_documents (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    source_id uuid NOT NULL,
    kind text NOT NULL,
    url text NOT NULL,
    language text,
    read_at timestamp with time zone DEFAULT now() NOT NULL,
    s3_key text NOT NULL,
    sha256 text NOT NULL,
    bytes integer NOT NULL,
    CONSTRAINT company_documents_kind_check CHECK ((kind = ANY (ARRAY['webpage'::text, 'wikipedia'::text, 'wikidata_description'::text])))
);


--
-- Name: TABLE company_documents; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_documents IS 'Index of the company corpus: every text read about a company (web pages, Wikipedia, Wikidata descriptions), stored in S3 under s3_key, latest version only (#61). Contains references to personal data; pipelines only, not in Hasura.';


--
-- Name: company_field_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_field_history (
    id bigint NOT NULL,
    company_id uuid NOT NULL,
    field_name text NOT NULL,
    event text NOT NULL,
    value text,
    previous_value text,
    source_id uuid,
    observation_id uuid,
    confidence numeric(5,4),
    previous_confidence numeric(5,4),
    changed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT company_field_history_event_check CHECK ((event = ANY (ARRAY['shown'::text, 'replaced'::text, 'confidence_changed'::text, 'removed'::text])))
);


--
-- Name: TABLE company_field_history; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_field_history IS 'Append-only history of every shown value per company: shown, replaced, confidence_changed, removed. Written by record_company_history() at the end of each pipeline_coordinator run. Never updated or deleted.';


--
-- Name: company_field_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.company_field_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: company_field_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.company_field_history_id_seq OWNED BY public.company_field_history.id;


--
-- Name: company_field_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_field_observations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    company_source_record_id uuid NOT NULL,
    source_id uuid NOT NULL,
    source_sync_run_id uuid,
    field_name text NOT NULL,
    value_text text,
    value_number numeric,
    value_date date,
    value_jsonb jsonb,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    source_updated_at timestamp with time zone,
    confidence numeric(5,4),
    is_canonical boolean DEFAULT false NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT company_field_observations_confidence_check CHECK (((confidence IS NULL) OR ((confidence >= (0)::numeric) AND (confidence <= (1)::numeric)))),
    CONSTRAINT company_field_observations_has_value_check CHECK ((num_nonnulls(value_text, value_number, value_date, value_jsonb) >= 1))
);


--
-- Name: TABLE company_field_observations; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_field_observations IS 'Raw field-level evidence collected from upstream source records.';


--
-- Name: company_funding_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_funding_links (
    fkz text NOT NULL,
    company_id uuid NOT NULL,
    match_method text DEFAULT 'name_and_city'::text NOT NULL,
    matched_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE company_funding_links; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_funding_links IS 'Which company each grant belongs to. Written by refresh_company_funding_links() after grants or companies are loaded; read by v_company_fundings.';


--
-- Name: company_fundings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_fundings (
    fkz text NOT NULL,
    recipient_name text NOT NULL,
    recipient_normalized_name text GENERATED ALWAYS AS (public.normalize_company_name(recipient_name)) STORED,
    city text,
    amount_eur numeric(14,2),
    period_from date,
    period_to date,
    topic text,
    ministry text,
    funding_type text,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    source text DEFAULT 'foerderkatalog'::text NOT NULL,
    recipient_register text,
    policy_area text
);


--
-- Name: TABLE company_fundings; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_fundings IS 'Grants, one row per grant: the Förderkatalog des Bundes (fkz = Förderkennzeichen) and the Berlin Zuwendungsdatenbank (fkz = ''berlin-'' || its id). company_funding_links links them to companies.';


--
-- Name: COLUMN company_fundings.recipient_register; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.company_fundings.recipient_register IS 'The recipient''s register entry as in company_identifiers, without the court (''HRB 84522''). Set only by sources that name it; such a grant is linked by it and never by name.';


--
-- Name: company_identifiers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_identifiers (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    scheme text NOT NULL,
    value text NOT NULL,
    source_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT company_identifiers_scheme_check CHECK ((scheme = ANY (ARRAY['register'::text, 'register_former'::text, 'opencorporates'::text, 'vat'::text, 'lei'::text])))
);


--
-- Name: TABLE company_identifiers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_identifiers IS 'Every identifier a source uses for a company. Matching tries these first; (scheme, value) is unique, so one identifier can never point at two companies.';


--
-- Name: company_managers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_managers (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    full_name text NOT NULL,
    role text NOT NULL,
    email text,
    phone text,
    is_current boolean DEFAULT true NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    company_field_observation_id uuid,
    source_id uuid
);


--
-- Name: TABLE company_managers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_managers IS 'Canonical manager contacts attached to a company.';


--
-- Name: company_source_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_source_records (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    source_id uuid NOT NULL,
    external_id text NOT NULL,
    external_url text,
    source_company_name text,
    source_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_synced_at timestamp with time zone DEFAULT now() NOT NULL,
    is_deleted_in_source boolean DEFAULT false NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    match_method text,
    match_confidence numeric(4,3),
    matched_at timestamp with time zone,
    CONSTRAINT company_source_records_match_confidence_check CHECK (((match_confidence >= (0)::numeric) AND (match_confidence <= (1)::numeric))),
    CONSTRAINT company_source_records_match_method_check CHECK ((match_method = ANY (ARRAY['identifier'::text, 'fuzzy'::text, 'manual'::text, 'created'::text])))
);


--
-- Name: TABLE company_source_records; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_source_records IS 'One raw source record per external company identity and source.';


--
-- Name: COLUMN company_source_records.match_method; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.company_source_records.match_method IS 'identifier: a shared identifier; fuzzy: name/address score; manual: a person decided; created: no match, the record created the company. NULL for records linked before matching existed.';


--
-- Name: sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    key text NOT NULL,
    name text NOT NULL,
    source_type text NOT NULL,
    base_url text,
    priority integer DEFAULT 100 NOT NULL,
    default_sync_interval_minutes integer DEFAULT 1440 NOT NULL,
    last_synced_at timestamp with time zone,
    next_sync_at timestamp with time zone,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    description text,
    terms text,
    terms_url text,
    schedule text,
    CONSTRAINT sources_default_sync_interval_minutes_check CHECK ((default_sync_interval_minutes > 0)),
    CONSTRAINT sources_priority_check CHECK ((priority >= 0))
);


--
-- Name: TABLE sources; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.sources IS 'Registry of upstream sources with sync scheduling metadata.';


--
-- Name: COLUMN sources.description; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sources.description IS 'One or two sentences for the app (German): what the source is and how we get it.';


--
-- Name: COLUMN sources.terms; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sources.terms IS 'What we know about its terms of use (German); NULL when nobody has checked them.';


--
-- Name: COLUMN sources.terms_url; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sources.terms_url IS 'Where those terms are published, when there is such a page.';


--
-- Name: COLUMN sources.schedule; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sources.schedule IS 'How often it runs (German), as the DAG is set up.';


--
-- Name: v_companies_enriched; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_companies_enriched AS
 SELECT c.id,
    reg.company_name,
    public.normalize_company_name(reg.company_name) AS normalized_name,
    reg.legal_form,
    reg.business_purpose,
    reg.address,
    reg.established_at,
    ( SELECT ('Amtsgericht '::text || i.value)
           FROM public.company_identifiers i
          WHERE ((i.company_id = c.id) AND (i.scheme = 'register'::text))
          ORDER BY i.created_at DESC, ("substring"(i.value, '(\d+)\D*$'::text))::bigint DESC NULLS LAST, i.id DESC
         LIMIT 1) AS registration_number,
    reg.wz_code,
    c.data_last_updated_at,
    c.metadata,
    c.created_at,
    c.updated_at,
    obs_website.value_text AS website_url,
    obs_revenue.value_number AS revenue,
    (obs_emp.value_number)::integer AS employee_count,
    obs_website.confidence AS website_confidence,
    obs_revenue.confidence AS revenue_confidence,
    obs_emp.confidence AS employee_count_confidence,
    src_website.key AS website_source,
    src_revenue.key AS revenue_source,
    src_emp.key AS employee_count_source,
    reg.register_status,
    obs_tfield.value_text AS technology_field,
    obs_tfield.confidence AS technology_field_confidence,
    src_tfield.key AS technology_field_source,
    obs_purpose.value_text AS purpose_field,
    obs_purpose.confidence AS purpose_field_confidence,
    src_purpose.key AS purpose_field_source,
    reg.postcode,
    reg.city,
    c.merged_into
   FROM ((((((((((((((((public.companies c
     LEFT JOIN ( SELECT cfs.company_id,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'name'::text)) AS company_name,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'legal_form'::text)) AS legal_form,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'business_purpose'::text)) AS business_purpose,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'address'::text)) AS address,
            (max(o.value_text) FILTER (WHERE (cfs.field_name = 'established_at'::text)))::date AS established_at,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'wz_code'::text)) AS wz_code,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'register_status'::text)) AS register_status,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'postcode'::text)) AS postcode,
            max(o.value_text) FILTER (WHERE (cfs.field_name = 'city'::text)) AS city
           FROM (public.company_canonical_field_sources cfs
             JOIN public.company_field_observations o ON ((o.id = cfs.company_field_observation_id)))
          WHERE (cfs.field_name = ANY (ARRAY['name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text]))
          GROUP BY cfs.company_id) reg ON ((reg.company_id = c.id)))
     LEFT JOIN public.company_canonical_field_sources cfs_website ON (((cfs_website.company_id = c.id) AND (cfs_website.field_name = 'website'::text))))
     LEFT JOIN public.company_field_observations obs_website ON ((obs_website.id = cfs_website.company_field_observation_id)))
     LEFT JOIN public.sources src_website ON ((src_website.id = cfs_website.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_revenue ON (((cfs_revenue.company_id = c.id) AND (cfs_revenue.field_name = 'revenue'::text))))
     LEFT JOIN public.company_field_observations obs_revenue ON ((obs_revenue.id = cfs_revenue.company_field_observation_id)))
     LEFT JOIN public.sources src_revenue ON ((src_revenue.id = cfs_revenue.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_emp ON (((cfs_emp.company_id = c.id) AND (cfs_emp.field_name = 'employee_count'::text))))
     LEFT JOIN public.company_field_observations obs_emp ON ((obs_emp.id = cfs_emp.company_field_observation_id)))
     LEFT JOIN public.sources src_emp ON ((src_emp.id = cfs_emp.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_tfield ON (((cfs_tfield.company_id = c.id) AND (cfs_tfield.field_name = 'technology_field'::text))))
     LEFT JOIN public.company_field_observations obs_tfield ON ((obs_tfield.id = cfs_tfield.company_field_observation_id)))
     LEFT JOIN public.sources src_tfield ON ((src_tfield.id = cfs_tfield.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_purpose ON (((cfs_purpose.company_id = c.id) AND (cfs_purpose.field_name = 'purpose_field'::text))))
     LEFT JOIN public.company_field_observations obs_purpose ON ((obs_purpose.id = cfs_purpose.company_field_observation_id)))
     LEFT JOIN public.sources src_purpose ON ((src_purpose.id = cfs_purpose.source_id)));


--
-- Name: VIEW v_companies_enriched; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_companies_enriched IS 'The company as applications should read it: the published value of every field, register facts included (V3). technology_field and purpose_field are two different questions (one of Berlin''s 77 fields in English, from published words vs. from the register purpose) and must never be merged; a WZ code may never fill technology_field.';


--
-- Name: v_company_fundings; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_fundings AS
 SELECT l.company_id,
    f.fkz,
    f.recipient_name,
    f.recipient_normalized_name,
    f.city,
    f.amount_eur,
    f.period_from,
    f.period_to,
    f.topic,
    f.ministry,
    f.funding_type,
    f.first_seen_at,
    f.last_seen_at
   FROM (public.company_fundings f
     JOIN public.company_funding_links l ON ((l.fkz = f.fkz)));


--
-- Name: v_company_profile_live; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_profile_live AS
 SELECT e.id,
    e.company_name AS name,
    e.registration_number,
    e.employee_count,
    e.revenue,
    COALESCE(fund.funding_count, (0)::bigint) AS funding_count,
    fund.funding_total_eur,
    e.wz_code,
    e.established_at,
    e.address,
    e.postcode,
    e.city,
    e.legal_form,
    e.technology_field,
    e.website_url AS website,
    mgr.managers AS management,
    e.business_purpose,
    obs_deeptech.value_text AS deeptech_class,
    obs_deeptech.confidence AS deeptech_confidence,
    (obs_ai.value_text)::boolean AS is_ai,
    e.register_status,
    GREATEST(e.data_last_updated_at, chosen.selected_at) AS updated_at,
    obs_keywords.value_text AS activity_keywords,
    obs_announce.value_text AS register_announcement,
    ((obs_announce.metadata ->> 'published_on'::text))::date AS register_announcement_date,
    COALESCE(
        CASE
            WHEN (obs_year.value_text ~ '^\d{4}$'::text) THEN (obs_year.value_text)::integer
            ELSE NULL::integer
        END, (EXTRACT(year FROM e.established_at))::integer) AS established_year
   FROM (((((((((((((public.v_companies_enriched e
     LEFT JOIN LATERAL ( SELECT count(*) AS funding_count,
            sum(f.amount_eur) AS funding_total_eur
           FROM public.v_company_fundings f
          WHERE (f.company_id = e.id)) fund ON (true))
     LEFT JOIN LATERAL ( SELECT string_agg(DISTINCT cm.full_name, '; '::text ORDER BY cm.full_name) AS managers
           FROM public.company_managers cm
          WHERE ((cm.company_id = e.id) AND cm.is_current)) mgr ON (true))
     LEFT JOIN LATERAL ( SELECT max(cfs.selected_at) AS selected_at
           FROM public.company_canonical_field_sources cfs
          WHERE (cfs.company_id = e.id)) chosen ON (true))
     LEFT JOIN public.company_canonical_field_sources cfs_deeptech ON (((cfs_deeptech.company_id = e.id) AND (cfs_deeptech.field_name = 'deeptech_class'::text))))
     LEFT JOIN public.company_field_observations obs_deeptech ON ((obs_deeptech.id = cfs_deeptech.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_ai ON (((cfs_ai.company_id = e.id) AND (cfs_ai.field_name = 'is_ai'::text))))
     LEFT JOIN public.company_field_observations obs_ai ON ((obs_ai.id = cfs_ai.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_keywords ON (((cfs_keywords.company_id = e.id) AND (cfs_keywords.field_name = 'activity_keywords'::text))))
     LEFT JOIN public.company_field_observations obs_keywords ON ((obs_keywords.id = cfs_keywords.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_announce ON (((cfs_announce.company_id = e.id) AND (cfs_announce.field_name = 'register_announcement'::text))))
     LEFT JOIN public.company_field_observations obs_announce ON ((obs_announce.id = cfs_announce.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_year ON (((cfs_year.company_id = e.id) AND (cfs_year.field_name = 'founding_year'::text))))
     LEFT JOIN public.company_field_observations obs_year ON ((obs_year.id = cfs_year.company_field_observation_id)));


--
-- Name: VIEW v_company_profile_live; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_profile_live IS 'The target record per company, read by the app through Hasura (via company_profile_mv). Tags and comments are separate tables. deeptech_class and is_ai are filled only by corrections from the app so far.';


--
-- Name: v_company_sources; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_sources AS
 SELECT cfs.company_id,
    cfs.field_name,
    s.key AS source_key
   FROM (public.company_canonical_field_sources cfs
     JOIN public.sources s ON ((s.id = cfs.source_id)))
UNION ALL
 SELECT m.company_id,
    'management'::text AS field_name,
    s.key AS source_key
   FROM (public.company_managers m
     JOIN public.sources s ON ((s.id = m.source_id)))
  WHERE m.is_current
UNION ALL
 SELECT r.company_id,
    'register'::text AS field_name,
    s.key AS source_key
   FROM (public.company_source_records r
     JOIN public.sources s ON (((s.id = r.source_id) AND (s.source_type = 'register'::text))))
UNION ALL
 SELECT l.company_id,
    'fundings'::text AS field_name,
    f.source AS source_key
   FROM (public.company_funding_links l
     JOIN public.company_fundings f ON ((f.fkz = l.fkz)));


--
-- Name: VIEW v_company_sources; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_sources IS 'Which source each shown value of a company came from (company_id, field_name, source_key).';


--
-- Name: company_profile_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.company_profile_mv AS
 SELECT p.id,
    p.name,
    p.registration_number,
    p.employee_count,
    p.revenue,
    p.funding_count,
    p.funding_total_eur,
    p.wz_code,
    p.established_at,
    p.address,
    p.postcode,
    p.city,
    p.legal_form,
    p.technology_field,
    p.website,
    p.management,
    p.business_purpose,
    p.deeptech_class,
    p.deeptech_confidence,
    p.is_ai,
    p.register_status,
    p.updated_at,
    p.activity_keywords,
    p.register_announcement,
    p.register_announcement_date,
    COALESCE(src.source_keys, '{}'::text[]) AS source_keys,
    p.established_year
   FROM (public.v_company_profile_live p
     LEFT JOIN ( SELECT v_company_sources.company_id,
            array_agg(DISTINCT v_company_sources.source_key ORDER BY v_company_sources.source_key) AS source_keys
           FROM public.v_company_sources
          GROUP BY v_company_sources.company_id) src ON ((src.company_id = p.id)))
  WITH DATA;


--
-- Name: company_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_tags (
    company_id uuid NOT NULL,
    tag_id uuid NOT NULL,
    added_by text NOT NULL,
    added_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE company_tags; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_tags IS 'Which company has which tag, who added it and when. A person removes only tags they added (Hasura); an admin role for the rest comes later.';


--
-- Name: extraction_feedback; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.extraction_feedback (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_id uuid NOT NULL,
    field_name text NOT NULL,
    value text NOT NULL,
    verdict text NOT NULL,
    author_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    correction text,
    CONSTRAINT extraction_feedback_correction_check CHECK (((correction IS NULL) OR ((length(correction) >= 1) AND (length(correction) <= 500)))),
    CONSTRAINT extraction_feedback_correction_only_when_wrong CHECK (((correction IS NULL) OR (verdict = 'wrong'::text))),
    CONSTRAINT extraction_feedback_empty_needs_correction CHECK (((value <> ''::text) OR ((verdict = 'wrong'::text) AND (correction IS NOT NULL)))),
    CONSTRAINT extraction_feedback_field_name_check CHECK ((field_name = ANY (ARRAY['name'::text, 'address'::text, 'legal_form'::text, 'register_status'::text, 'wz_code'::text, 'established_at'::text, 'established_year'::text, 'employee_count'::text, 'revenue'::text, 'fundings'::text, 'technology_field'::text, 'website'::text, 'management'::text, 'business_purpose'::text, 'deeptech'::text, 'is_ai'::text, 'activity_keywords'::text, 'register_announcement'::text, 'registration_number'::text]))),
    CONSTRAINT extraction_feedback_value_check CHECK ((length(value) <= 500)),
    CONSTRAINT extraction_feedback_verdict_check CHECK ((verdict = ANY (ARRAY['correct'::text, 'wrong'::text])))
);


--
-- Name: TABLE extraction_feedback; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.extraction_feedback IS 'Users'' votes on extracted values (correct/wrong), one per user per company, field and value. The labels for measuring and training extraction. Contains personal data for field management. See V23.';


--
-- Name: COLUMN extraction_feedback.value; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.extraction_feedback.value IS 'The value shown when the vote was cast; '''' when the field showed nothing. For `fundings` it is the grant''s Förderkennzeichen (FKZ), one vote per grant.';


--
-- Name: match_candidates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.match_candidates (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_record_id uuid NOT NULL,
    company_id uuid NOT NULL,
    score numeric(4,3) NOT NULL,
    reasons jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    decided_by text,
    decided_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT match_candidates_decision_complete CHECK (((status = 'open'::text) = (decided_at IS NULL))),
    CONSTRAINT match_candidates_score_check CHECK (((score >= (0)::numeric) AND (score <= (1)::numeric))),
    CONSTRAINT match_candidates_status_check CHECK ((status = ANY (ARRAY['open'::text, 'accepted'::text, 'rejected'::text])))
);


--
-- Name: pipeline_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pipeline_state (
    pipeline text NOT NULL,
    key text NOT NULL,
    value jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: review_sample; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.review_sample (
    company_id uuid NOT NULL,
    stratum text NOT NULL,
    drawn_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT review_sample_stratum_check CHECK ((stratum = ANY (ARRAY['website'::text, 'technology_field'::text, 'management'::text, 'register'::text])))
);


--
-- Name: TABLE review_sample; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.review_sample IS 'Companies people check by hand, drawn by draw_review_sample() (stratified, stable random order). Never redrawn, only topped up, so measurements stay comparable.';


--
-- Name: source_field_stats_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.source_field_stats_mv AS
 WITH canonical AS (
         SELECT company_field_observations.company_id,
            company_field_observations.field_name,
            company_field_observations.source_id,
            company_field_observations.value_text,
            company_field_observations.value_number,
            company_field_observations.value_date,
            company_field_observations.value_jsonb
           FROM public.company_field_observations
          WHERE company_field_observations.is_canonical
        )
 SELECT s.key AS source_key,
    o.field_name,
    count(*) AS observations,
    count(DISTINCT o.company_id) AS companies,
    count(DISTINCT o.company_id) FILTER (WHERE (o.is_canonical OR (m.id IS NOT NULL))) AS companies_shown,
    count(DISTINCT o.company_id) FILTER (WHERE ((o.confidence > (0)::numeric) AND (NOT o.is_canonical) AND (m.id IS NULL) AND (c.source_id IS NOT NULL) AND (c.source_id <> o.source_id) AND ((((c.value_text IS DISTINCT FROM o.value_text) OR (c.value_number IS DISTINCT FROM o.value_number)) OR (c.value_date IS DISTINCT FROM o.value_date)) OR (c.value_jsonb IS DISTINCT FROM o.value_jsonb)))) AS companies_beaten,
    round(avg(o.confidence), 3) AS avg_confidence,
    count(*) FILTER (WHERE (o.confidence = (0)::numeric)) AS confidence_zero,
    count(*) FILTER (WHERE ((o.confidence > (0)::numeric) AND (o.confidence < 0.35))) AS confidence_below_035,
    count(*) FILTER (WHERE ((o.confidence >= 0.35) AND (o.confidence < 0.6))) AS confidence_035_060,
    count(*) FILTER (WHERE ((o.confidence >= 0.6) AND (o.confidence < 0.8))) AS confidence_060_080,
    count(*) FILTER (WHERE (o.confidence >= 0.8)) AS confidence_080_up,
    max(o.observed_at) AS last_observed_at
   FROM (((public.company_field_observations o
     JOIN public.sources s ON ((s.id = o.source_id)))
     LEFT JOIN public.company_managers m ON (((m.company_field_observation_id = o.id) AND m.is_current)))
     LEFT JOIN canonical c ON (((c.company_id = o.company_id) AND (c.field_name = o.field_name))))
  GROUP BY s.key, o.field_name
UNION ALL
 SELECT f.source AS source_key,
    'fundings'::text AS field_name,
    count(DISTINCT f.fkz) AS observations,
    count(DISTINCT l.company_id) AS companies,
    count(DISTINCT l.company_id) AS companies_shown,
    (0)::bigint AS companies_beaten,
    NULL::numeric AS avg_confidence,
    (0)::bigint AS confidence_zero,
    (0)::bigint AS confidence_below_035,
    (0)::bigint AS confidence_035_060,
    (0)::bigint AS confidence_060_080,
    (0)::bigint AS confidence_080_up,
    max(f.last_seen_at) AS last_observed_at
   FROM (public.company_fundings f
     LEFT JOIN public.company_funding_links l ON ((l.fkz = f.fkz)))
  GROUP BY f.source
  WITH DATA;


--
-- Name: source_stats_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.source_stats_mv AS
 WITH covered AS (
         SELECT r.source_id,
            r.company_id,
            true AS is_record,
            r.match_method,
            r.match_confidence
           FROM public.company_source_records r
        UNION ALL
         SELECT s_1.id,
            l.company_id,
            false,
            NULL::text AS text,
            NULL::numeric AS "numeric"
           FROM ((public.company_funding_links l
             JOIN public.company_fundings f ON ((f.fkz = l.fkz)))
             JOIN public.sources s_1 ON ((s_1.key = f.source)))
        ), per_source AS (
         SELECT covered.source_id,
            count(*) FILTER (WHERE covered.is_record) AS records,
            count(*) FILTER (WHERE (covered.match_method = 'created'::text)) AS companies_created,
            count(*) FILTER (WHERE (covered.match_method = 'identifier'::text)) AS records_by_identifier,
            count(*) FILTER (WHERE (covered.match_method = 'fuzzy'::text)) AS records_by_name,
            count(*) FILTER (WHERE (covered.match_method = 'manual'::text)) AS records_by_hand,
            round(avg(covered.match_confidence) FILTER (WHERE covered.is_record), 3) AS avg_match_confidence,
            count(DISTINCT covered.company_id) AS companies
           FROM covered
          GROUP BY covered.source_id
        )
 SELECT s.key AS source_key,
    COALESCE(p.records, (0)::bigint) AS records,
    COALESCE(p.companies_created, (0)::bigint) AS companies_created,
    COALESCE(p.companies, (0)::bigint) AS companies,
    COALESCE(p.records_by_identifier, (0)::bigint) AS records_by_identifier,
    COALESCE(p.records_by_name, (0)::bigint) AS records_by_name,
    COALESCE(p.records_by_hand, (0)::bigint) AS records_by_hand,
    p.avg_match_confidence,
    now() AS computed_at
   FROM (public.sources s
     LEFT JOIN per_source p ON ((p.source_id = s.id)))
  WITH DATA;


--
-- Name: source_sync_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_sync_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_id uuid NOT NULL,
    sync_type text NOT NULL,
    status text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    records_seen integer DEFAULT 0 NOT NULL,
    records_created integer DEFAULT 0 NOT NULL,
    records_updated integer DEFAULT 0 NOT NULL,
    records_failed integer DEFAULT 0 NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT source_sync_runs_records_created_check CHECK ((records_created >= 0)),
    CONSTRAINT source_sync_runs_records_failed_check CHECK ((records_failed >= 0)),
    CONSTRAINT source_sync_runs_records_seen_check CHECK ((records_seen >= 0)),
    CONSTRAINT source_sync_runs_records_updated_check CHECK ((records_updated >= 0)),
    CONSTRAINT source_sync_runs_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'success'::text, 'failed'::text, 'partial'::text, 'cancelled'::text]))),
    CONSTRAINT source_sync_runs_sync_type_check CHECK ((sync_type = ANY (ARRAY['full'::text, 'incremental'::text, 'backfill'::text, 'manual'::text])))
);


--
-- Name: TABLE source_sync_runs; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.source_sync_runs IS 'Execution log for source synchronization runs.';


--
-- Name: tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tags (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL,
    created_by text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tags_name_check CHECK (((btrim(name) <> ''::text) AND (length(name) <= 60)))
);


--
-- Name: TABLE tags; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.tags IS 'Tags people give companies in the app. Free text, unique ignoring case and surrounding spaces. created_by is a Keycloak user id.';


--
-- Name: technology_field_keywords; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.technology_field_keywords (
    field text NOT NULL,
    keyword text NOT NULL,
    removed_at date,
    removed_reason text,
    CONSTRAINT technology_field_keywords_removed_check CHECK (((removed_at IS NULL) = (removed_reason IS NULL)))
);


--
-- Name: TABLE technology_field_keywords; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.technology_field_keywords IS 'Words on a company''s pages that suggest a Technologiefeld; one row per (field, keyword). Matched case-insensitively, as whole words only.';


--
-- Name: COLUMN technology_field_keywords.keyword; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_field_keywords.keyword IS 'As Berlin wrote it; the classifier normalises it (lower case, NFKC).';


--
-- Name: COLUMN technology_field_keywords.removed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_field_keywords.removed_at IS 'Set when the keyword was found wrong for its field. The classifier skips such rows; they are never deleted.';


--
-- Name: COLUMN technology_field_keywords.removed_reason; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_field_keywords.removed_reason IS 'Why it was removed, with the measurement (texts it fired on, companies it decided).';


--
-- Name: technology_fields; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.technology_fields (
    name text NOT NULL,
    name_en text NOT NULL,
    senweb_unit text,
    berlin_partner text,
    related text[] DEFAULT '{}'::text[] NOT NULL
);


--
-- Name: TABLE technology_fields; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.technology_fields IS 'Berlin''s 77 Technologie- und Anwendungsfelder (#84). The classifier answers with `name`; the database stores and the app shows `name_en`.';


--
-- Name: COLUMN technology_fields.name; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_fields.name IS 'Berlin''s official German name; the key the keywords and the classifier use.';


--
-- Name: COLUMN technology_fields.name_en; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_fields.name_en IS 'Our English translation (Berlin publishes none). What technology_field and purpose_field store.';


--
-- Name: COLUMN technology_fields.senweb_unit; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_fields.senweb_unit IS 'The Senate department unit that owns the field, as in Berlin''s sheet.';


--
-- Name: COLUMN technology_fields.berlin_partner; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_fields.berlin_partner IS 'The field''s contact person(s) as named in Berlin''s sheet, comma-separated (67 of 77 fields); NULL where the sheet said "nicht bekannt" or "?" or gave nothing. A person''s name: not exposed through Hasura.';


--
-- Name: COLUMN technology_fields.related; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.technology_fields.related IS 'Related fields or WZ codes, as in Berlin''s sheet. Never used to classify.';


--
-- Name: v_company_claims; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_claims AS
 SELECT o.id,
    o.company_id,
        CASE o.field_name
            WHEN 'ceo_name_or_contact'::text THEN 'management'::text
            WHEN 'candidate_url'::text THEN 'website'::text
            WHEN 'deeptech_class'::text THEN 'deeptech'::text
            WHEN 'founding_year'::text THEN 'established_year'::text
            ELSE o.field_name
        END AS field_key,
    COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text) AS value,
    o.confidence,
    (o.is_canonical OR (EXISTS ( SELECT 1
           FROM public.company_managers m
          WHERE ((m.company_field_observation_id = o.id) AND m.is_current)))) AS is_shown,
    COALESCE((o.metadata ->> 'verified_by'::text), (o.metadata ->> 'decided_by'::text)) AS method,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE(o.source_updated_at, o.observed_at) AS read_at
   FROM (public.company_field_observations o
     JOIN public.sources s ON ((s.id = o.source_id)))
  WHERE (o.field_name = ANY (ARRAY['name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text, 'website'::text, 'candidate_url'::text, 'technology_field'::text, 'purpose_field'::text, 'activity_keywords'::text, 'register_announcement'::text, 'ceo_name_or_contact'::text, 'employee_count'::text, 'revenue'::text, 'deeptech_class'::text, 'is_ai'::text, 'founding_year'::text]));


--
-- Name: VIEW v_company_claims; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_claims IS 'Every claim any source made about a company, shown or not, for the app''s "all claims" toggle (#54). Excludes email, phone, _withheld fields and bookkeeping by a field allow-list. Deliberately includes manager names never shown and corrections below the bar (one person''s, or withdrawn), visible to every logged-in user: flagged for the privacy review (#54).';


--
-- Name: v_company_history; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_history AS
 SELECT h.id,
    h.company_id,
        CASE h.field_name
            WHEN 'deeptech_class'::text THEN 'deeptech'::text
            WHEN 'founding_year'::text THEN 'established_year'::text
            ELSE h.field_name
        END AS field_key,
    h.event,
    h.value,
    h.previous_value,
    h.confidence,
    h.previous_confidence,
    s.key AS source_key,
    s.name AS source_name,
    h.changed_at
   FROM (public.company_field_history h
     LEFT JOIN public.sources s ON ((s.id = h.source_id)));


--
-- Name: VIEW v_company_history; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_history IS 'The shown-value history per company, for the app''s company page (#54). Newest first by changed_at.';


--
-- Name: v_company_profile; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_profile AS
 SELECT company_profile_mv.id,
    company_profile_mv.name,
    company_profile_mv.registration_number,
    company_profile_mv.employee_count,
    company_profile_mv.revenue,
    company_profile_mv.funding_count,
    company_profile_mv.funding_total_eur,
    company_profile_mv.wz_code,
    company_profile_mv.established_at,
    company_profile_mv.address,
    company_profile_mv.postcode,
    company_profile_mv.city,
    company_profile_mv.legal_form,
    company_profile_mv.technology_field,
    company_profile_mv.website,
    company_profile_mv.management,
    company_profile_mv.business_purpose,
    company_profile_mv.deeptech_class,
    company_profile_mv.deeptech_confidence,
    company_profile_mv.is_ai,
    company_profile_mv.register_status,
    company_profile_mv.updated_at,
    company_profile_mv.activity_keywords,
    company_profile_mv.register_announcement,
    company_profile_mv.register_announcement_date,
    company_profile_mv.source_keys,
    company_profile_mv.established_year
   FROM public.company_profile_mv;


--
-- Name: VIEW v_company_profile; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_profile IS 'The target record per company, as of the last refresh_company_profile(). The live definition is v_company_profile_live. established_year is the canonical founding_year, else the year of established_at (V6).';


--
-- Name: v_field_provenance; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_field_provenance AS
 SELECT cfs.company_id,
        CASE cfs.field_name
            WHEN 'founding_year'::text THEN 'established_year'::text
            ELSE cfs.field_name
        END AS field_name,
    COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text) AS value,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE((o.metadata ->> 'verified_by'::text), (o.metadata ->> 'decided_by'::text), s.source_type) AS method,
    o.confidence,
    COALESCE((o.metadata -> 'evidence'::text), (o.metadata -> 'keywords'::text)) AS evidence,
    COALESCE((o.metadata ->> 'imprint_url'::text), (o.metadata ->> 'matched_url'::text)) AS link,
    COALESCE(o.source_updated_at, o.observed_at) AS observed_at,
    cfs.selected_at
   FROM ((public.company_canonical_field_sources cfs
     JOIN public.company_field_observations o ON ((o.id = cfs.company_field_observation_id)))
     JOIN public.sources s ON ((s.id = cfs.source_id)))
UNION ALL
 SELECT m.company_id,
    'management'::text AS field_name,
    m.full_name AS value,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE((o.metadata ->> 'model_version'::text), s.source_type) AS method,
    o.confidence,
    (o.metadata -> 'evidence'::text) AS evidence,
    (o.metadata ->> 'imprint_url'::text) AS link,
    COALESCE(o.source_updated_at, o.observed_at) AS observed_at,
    m.updated_at AS selected_at
   FROM ((public.company_managers m
     LEFT JOIN public.company_field_observations o ON ((o.id = m.company_field_observation_id)))
     LEFT JOIN public.sources s ON ((s.id = m.source_id)))
  WHERE m.is_current
UNION ALL
 SELECT l.company_id,
    'fundings'::text AS field_name,
    f.fkz AS value,
    f.source AS source_key,
    COALESCE(s.name, f.source) AS source_name,
        CASE l.match_method
            WHEN 'register_number'::text THEN 'linked by register number'::text
            ELSE 'linked by recipient name and city'::text
        END AS method,
    NULL::numeric AS confidence,
    NULL::jsonb AS evidence,
    NULL::text AS link,
    f.first_seen_at AS observed_at,
    f.last_seen_at AS selected_at
   FROM ((public.company_funding_links l
     JOIN public.company_fundings f ON ((f.fkz = l.fkz)))
     LEFT JOIN public.sources s ON ((s.key = f.source)))
UNION ALL
 SELECT r.company_id,
    'registration_number'::text AS field_name,
    ('Amtsgericht '::text || r.value) AS value,
    s.key AS source_key,
    s.name AS source_name,
    'register'::text AS method,
    NULL::numeric AS confidence,
    NULL::jsonb AS evidence,
    NULL::text AS link,
    r.created_at AS observed_at,
    r.created_at AS selected_at
   FROM (( SELECT DISTINCT ON (i.company_id) i.company_id,
            i.value,
            i.source_id,
            i.created_at
           FROM public.company_identifiers i
          WHERE (i.scheme = 'register'::text)
          ORDER BY i.company_id, i.created_at DESC, ("substring"(i.value, '(\d+)\D*$'::text))::bigint DESC NULLS LAST, i.id DESC) r
     LEFT JOIN public.sources s ON ((s.id = r.source_id)));


--
-- Name: VIEW v_field_provenance; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_field_provenance IS 'Where every value the app shows came from: source, method, confidence, evidence, link and dates, one row per company, field and value. No personal data beyond the shown value.';


--
-- Name: v_review_results; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_review_results AS
 WITH verdicts AS (
         SELECT s.stratum,
            f.company_id,
            f.field_name,
            f.value,
                CASE sign(((count(*) FILTER (WHERE (f.verdict = 'correct'::text)) - count(*) FILTER (WHERE (f.verdict = 'wrong'::text))))::double precision)
                    WHEN 1 THEN 'correct'::text
                    WHEN '-1'::integer THEN 'wrong'::text
                    ELSE NULL::text
                END AS verdict
           FROM ((public.review_sample s
             JOIN public.extraction_feedback f ON (((f.company_id = s.company_id) AND (f.value <> ''::text))))
             JOIN public.company_profile_mv p ON ((p.id = f.company_id)))
          WHERE
                CASE f.field_name
                    WHEN 'name'::text THEN (f.value = p.name)
                    WHEN 'address'::text THEN (f.value = p.address)
                    WHEN 'legal_form'::text THEN (f.value = p.legal_form)
                    WHEN 'register_status'::text THEN (f.value = p.register_status)
                    WHEN 'wz_code'::text THEN (f.value = p.wz_code)
                    WHEN 'established_at'::text THEN (f.value = (p.established_at)::text)
                    WHEN 'established_year'::text THEN (f.value = (p.established_year)::text)
                    WHEN 'employee_count'::text THEN
                    CASE
                        WHEN (f.value ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((f.value)::numeric = (p.employee_count)::numeric)
                        ELSE NULL::boolean
                    END
                    WHEN 'revenue'::text THEN
                    CASE
                        WHEN (f.value ~ '^-?[0-9]+(\.[0-9]+)?$'::text) THEN ((f.value)::numeric = p.revenue)
                        ELSE NULL::boolean
                    END
                    WHEN 'fundings'::text THEN (EXISTS ( SELECT 1
                       FROM public.company_funding_links l
                      WHERE ((l.company_id = f.company_id) AND (l.fkz = f.value))))
                    WHEN 'technology_field'::text THEN (f.value = p.technology_field)
                    WHEN 'website'::text THEN (f.value = p.website)
                    WHEN 'management'::text THEN (f.value = ANY (string_to_array(p.management, '; '::text)))
                    WHEN 'business_purpose'::text THEN (f.value = p.business_purpose)
                    WHEN 'deeptech'::text THEN (f.value = p.deeptech_class)
                    WHEN 'is_ai'::text THEN (f.value = (p.is_ai)::text)
                    WHEN 'activity_keywords'::text THEN (f.value = ANY (string_to_array(p.activity_keywords, '; '::text)))
                    WHEN 'register_announcement'::text THEN (f.value = p.register_announcement)
                    WHEN 'registration_number'::text THEN (f.value = p.registration_number)
                    ELSE NULL::boolean
                END
          GROUP BY s.stratum, f.company_id, f.field_name, f.value
        )
 SELECT verdicts.stratum,
    verdicts.field_name,
    count(DISTINCT verdicts.company_id) AS companies_judged,
    count(*) FILTER (WHERE (verdicts.verdict = 'correct'::text)) AS correct,
    count(*) FILTER (WHERE (verdicts.verdict = 'wrong'::text)) AS wrong,
    round(((count(*) FILTER (WHERE (verdicts.verdict = 'correct'::text)))::numeric / (NULLIF(count(*) FILTER (WHERE (verdicts.verdict IS NOT NULL)), 0))::numeric), 3) AS "precision"
   FROM verdicts
  GROUP BY verdicts.stratum, verdicts.field_name;


--
-- Name: VIEW v_review_results; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_review_results IS 'Hand-checked precision per field and stratum of the values shown now, from the app''s votes on the review sample: a vote counts only while its value is what company_profile_mv shows; one verdict per value by majority (ties left out). Missing values (a vote on an empty field) are not counted here.';


--
-- Name: v_source_fields; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_source_fields AS
 SELECT source_field_stats_mv.source_key,
    source_field_stats_mv.field_name,
    source_field_stats_mv.observations,
    source_field_stats_mv.companies,
    source_field_stats_mv.companies_shown,
    source_field_stats_mv.companies_beaten,
    source_field_stats_mv.avg_confidence,
    source_field_stats_mv.confidence_zero,
    source_field_stats_mv.confidence_below_035,
    source_field_stats_mv.confidence_035_060,
    source_field_stats_mv.confidence_060_080,
    source_field_stats_mv.confidence_080_up,
    source_field_stats_mv.last_observed_at
   FROM public.source_field_stats_mv;


--
-- Name: VIEW v_source_fields; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_source_fields IS 'Per source and field: values stored, companies, companies whose shown value came from this source, companies where another source''s different value is shown, and values per confidence range. Counts only; no value is exposed.';


--
-- Name: v_source_overview; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_source_overview AS
 SELECT s.key AS source_key,
    s.name,
    s.source_type,
    s.base_url,
    s.description,
    s.terms,
    s.terms_url,
    s.schedule,
    s.priority,
    COALESCE(st.records, (0)::bigint) AS records,
    COALESCE(st.companies, (0)::bigint) AS companies,
    COALESCE(st.companies_created, (0)::bigint) AS companies_created,
    COALESCE(st.records_by_identifier, (0)::bigint) AS records_by_identifier,
    COALESCE(st.records_by_name, (0)::bigint) AS records_by_name,
    COALESCE(st.records_by_hand, (0)::bigint) AS records_by_hand,
    st.avg_match_confidence,
    run.started_at AS last_run_started_at,
    run.finished_at AS last_run_finished_at,
    run.status AS last_run_status,
    run.records_seen AS last_run_seen,
    run.records_created AS last_run_created,
    run.records_updated AS last_run_updated,
    run.records_failed AS last_run_failed,
    ( SELECT count(*) AS count
           FROM public.source_sync_runs r
          WHERE (r.source_id = s.id)) AS runs,
    ( SELECT count(*) AS count
           FROM public.source_sync_runs r
          WHERE ((r.source_id = s.id) AND (r.status = 'running'::text) AND (EXISTS ( SELECT 1
                   FROM public.source_sync_runs n
                  WHERE ((n.source_id = r.source_id) AND (n.started_at > r.started_at)))))) AS runs_interrupted,
    st.computed_at AS counts_as_of
   FROM ((public.sources s
     LEFT JOIN public.source_stats_mv st ON ((st.source_key = s.key)))
     LEFT JOIN LATERAL ( SELECT r.started_at,
            r.finished_at,
            r.records_seen,
            r.records_created,
            r.records_updated,
            r.records_failed,
            r.status
           FROM public.source_sync_runs r
          WHERE (r.source_id = s.id)
          ORDER BY r.started_at DESC
         LIMIT 1) run ON (true));


--
-- Name: VIEW v_source_overview; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_source_overview IS 'One row per source for the app''s sources pages: what it is, its terms, records and how they were matched, and its latest run. Counts as of counts_as_of (refresh_company_profile()); the run is live.';


--
-- Name: v_source_runs; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_source_runs AS
 SELECT s.key AS source_key,
    r.started_at,
    r.finished_at,
    r.status,
    r.sync_type,
    r.records_seen,
    r.records_created,
    r.records_updated,
    r.records_failed,
    ((r.status = 'running'::text) AND (EXISTS ( SELECT 1
           FROM public.source_sync_runs n
          WHERE ((n.source_id = r.source_id) AND (n.started_at > r.started_at))))) AS interrupted
   FROM (public.sources s
     CROSS JOIN LATERAL ( SELECT r_1.id,
            r_1.source_id,
            r_1.sync_type,
            r_1.status,
            r_1.started_at,
            r_1.finished_at,
            r_1.records_seen,
            r_1.records_created,
            r_1.records_updated,
            r_1.records_failed,
            r_1.metadata,
            r_1.error_message,
            r_1.created_at,
            r_1.updated_at
           FROM public.source_sync_runs r_1
          WHERE (r_1.source_id = s.id)
          ORDER BY r_1.started_at DESC
         LIMIT 20) r);


--
-- Name: VIEW v_source_runs; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_source_runs IS 'The last 20 runs of every source: when, status and counts. Never the error message.';


--
-- Name: wz_divisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wz_divisions (
    division text NOT NULL,
    title text NOT NULL,
    section text NOT NULL,
    section_title text NOT NULL,
    CONSTRAINT wz_divisions_division_check CHECK ((division ~ '^[0-9]{2}$'::text)),
    CONSTRAINT wz_divisions_section_check CHECK ((section ~ '^[A-Z]$'::text))
);


--
-- Name: TABLE wz_divisions; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.wz_divisions IS 'The 87 divisions (Abteilungen) of WZ 2025 in their 22 sections, Statistisches Bundesamt. A company''s WZ cluster is its code''s first two digits looked up here. Never a Technologiefeld.';


--
-- Name: company_field_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_history ALTER COLUMN id SET DEFAULT nextval('public.company_field_history_id_seq'::regclass);


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (id);


--
-- Name: company_canonical_field_sources company_canonical_field_sources_company_field_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_canonical_field_sources
    ADD CONSTRAINT company_canonical_field_sources_company_field_key UNIQUE (company_id, field_name);


--
-- Name: company_canonical_field_sources company_canonical_field_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_canonical_field_sources
    ADD CONSTRAINT company_canonical_field_sources_pkey PRIMARY KEY (id);


--
-- Name: company_comments company_comments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_comments
    ADD CONSTRAINT company_comments_pkey PRIMARY KEY (id);


--
-- Name: company_documents company_documents_company_id_source_id_kind_url_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_documents
    ADD CONSTRAINT company_documents_company_id_source_id_kind_url_key UNIQUE (company_id, source_id, kind, url);


--
-- Name: company_documents company_documents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_documents
    ADD CONSTRAINT company_documents_pkey PRIMARY KEY (id);


--
-- Name: company_field_history company_field_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_history
    ADD CONSTRAINT company_field_history_pkey PRIMARY KEY (id);


--
-- Name: company_field_observations company_field_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_observations
    ADD CONSTRAINT company_field_observations_pkey PRIMARY KEY (id);


--
-- Name: company_funding_links company_funding_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_funding_links
    ADD CONSTRAINT company_funding_links_pkey PRIMARY KEY (fkz);


--
-- Name: company_fundings company_fundings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_fundings
    ADD CONSTRAINT company_fundings_pkey PRIMARY KEY (fkz);


--
-- Name: company_identifiers company_identifiers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_identifiers
    ADD CONSTRAINT company_identifiers_pkey PRIMARY KEY (id);


--
-- Name: company_managers company_managers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_managers
    ADD CONSTRAINT company_managers_pkey PRIMARY KEY (id);


--
-- Name: company_source_records company_source_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_source_records
    ADD CONSTRAINT company_source_records_pkey PRIMARY KEY (id);


--
-- Name: company_source_records company_source_records_source_external_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_source_records
    ADD CONSTRAINT company_source_records_source_external_key UNIQUE (source_id, external_id);


--
-- Name: company_tags company_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_tags
    ADD CONSTRAINT company_tags_pkey PRIMARY KEY (company_id, tag_id);


--
-- Name: extraction_feedback extraction_feedback_one_vote; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.extraction_feedback
    ADD CONSTRAINT extraction_feedback_one_vote UNIQUE (company_id, field_name, value, author_id);


--
-- Name: extraction_feedback extraction_feedback_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.extraction_feedback
    ADD CONSTRAINT extraction_feedback_pkey PRIMARY KEY (id);


--
-- Name: match_candidates match_candidates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_candidates
    ADD CONSTRAINT match_candidates_pkey PRIMARY KEY (id);


--
-- Name: pipeline_state pipeline_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_state
    ADD CONSTRAINT pipeline_state_pkey PRIMARY KEY (pipeline, key);


--
-- Name: review_sample review_sample_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.review_sample
    ADD CONSTRAINT review_sample_pkey PRIMARY KEY (company_id);


--
-- Name: source_sync_runs source_sync_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_sync_runs
    ADD CONSTRAINT source_sync_runs_pkey PRIMARY KEY (id);


--
-- Name: sources sources_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources
    ADD CONSTRAINT sources_key_key UNIQUE (key);


--
-- Name: sources sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (id);


--
-- Name: tags tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (id);


--
-- Name: technology_field_keywords technology_field_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technology_field_keywords
    ADD CONSTRAINT technology_field_keywords_pkey PRIMARY KEY (field, keyword);


--
-- Name: technology_fields technology_fields_name_en_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technology_fields
    ADD CONSTRAINT technology_fields_name_en_key UNIQUE (name_en);


--
-- Name: technology_fields technology_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technology_fields
    ADD CONSTRAINT technology_fields_pkey PRIMARY KEY (name);


--
-- Name: company_identifiers uq_company_identifiers_scheme_value; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_identifiers
    ADD CONSTRAINT uq_company_identifiers_scheme_value UNIQUE (scheme, value);


--
-- Name: company_managers uq_company_managers_company_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_managers
    ADD CONSTRAINT uq_company_managers_company_name UNIQUE (company_id, full_name);


--
-- Name: CONSTRAINT uq_company_managers_company_name ON company_managers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON CONSTRAINT uq_company_managers_company_name ON public.company_managers IS 'What makes resolve_ceo_contact idempotent. Before it existed, ON CONFLICT DO NOTHING had no target to conflict on and every coordinator run appended ~1,014 duplicate rows: 9,195 rows for 1,047 distinct names, worst case 21 copies. Do not drop this without also changing pipeline_coordinator/resolver.py, which names it explicitly.';


--
-- Name: match_candidates uq_match_candidates_record_company; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_candidates
    ADD CONSTRAINT uq_match_candidates_record_company UNIQUE (source_record_id, company_id);


--
-- Name: wz_divisions wz_divisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wz_divisions
    ADD CONSTRAINT wz_divisions_pkey PRIMARY KEY (division);


--
-- Name: companies_data_last_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_data_last_updated_at_idx ON public.companies USING btree (data_last_updated_at);


--
-- Name: companies_merged_into_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_merged_into_idx ON public.companies USING btree (merged_into) WHERE (merged_into IS NOT NULL);


--
-- Name: companies_metadata_gin_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_metadata_gin_idx ON public.companies USING gin (metadata);


--
-- Name: company_canonical_field_sources_company_field_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_canonical_field_sources_company_field_idx ON public.company_canonical_field_sources USING btree (company_id, field_name);


--
-- Name: company_canonical_field_sources_observation_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_canonical_field_sources_observation_idx ON public.company_canonical_field_sources USING btree (company_field_observation_id);


--
-- Name: company_canonical_field_sources_source_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_canonical_field_sources_source_idx ON public.company_canonical_field_sources USING btree (source_id);


--
-- Name: company_comments_company_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_comments_company_idx ON public.company_comments USING btree (company_id, created_at DESC);


--
-- Name: company_documents_company_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_documents_company_idx ON public.company_documents USING btree (company_id, kind);


--
-- Name: company_field_history_latest_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_history_latest_idx ON public.company_field_history USING btree (company_id, field_name, changed_at DESC, id DESC);


--
-- Name: company_field_history_management_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_history_management_idx ON public.company_field_history USING btree (company_id, value, changed_at DESC, id DESC) WHERE (field_name = 'management'::text);


--
-- Name: company_field_observations_company_field_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_observations_company_field_idx ON public.company_field_observations USING btree (company_id, field_name, observed_at DESC);


--
-- Name: company_field_observations_name_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_observations_name_key_idx ON public.company_field_observations USING btree (public.normalize_company_name(value_text)) WHERE ((field_name = 'name'::text) AND is_canonical);


--
-- Name: company_field_observations_resolution_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_observations_resolution_idx ON public.company_field_observations USING btree (company_id, field_name, source_id, confidence DESC, source_updated_at DESC, observed_at DESC);


--
-- Name: company_field_observations_single_canonical_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX company_field_observations_single_canonical_idx ON public.company_field_observations USING btree (company_id, field_name) WHERE is_canonical;


--
-- Name: company_field_observations_source_record_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_observations_source_record_idx ON public.company_field_observations USING btree (company_source_record_id);


--
-- Name: company_field_observations_sync_run_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_observations_sync_run_idx ON public.company_field_observations USING btree (source_sync_run_id);


--
-- Name: company_funding_links_company_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_funding_links_company_id_idx ON public.company_funding_links USING btree (company_id);


--
-- Name: company_fundings_recipient_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_fundings_recipient_idx ON public.company_fundings USING btree (recipient_normalized_name, lower(city));


--
-- Name: company_identifiers_company_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_identifiers_company_id_idx ON public.company_identifiers USING btree (company_id);


--
-- Name: company_managers_company_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_managers_company_id_idx ON public.company_managers USING btree (company_id);


--
-- Name: company_managers_observation_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_managers_observation_idx ON public.company_managers USING btree (company_field_observation_id);


--
-- Name: company_profile_mv_established_year; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_established_year ON public.company_profile_mv USING btree (established_year);


--
-- Name: company_profile_mv_fundings; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_fundings ON public.company_profile_mv USING btree (name) WHERE (funding_count > 0);


--
-- Name: company_profile_mv_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX company_profile_mv_id ON public.company_profile_mv USING btree (id);


--
-- Name: company_profile_mv_legal_form; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_legal_form ON public.company_profile_mv USING btree (legal_form);


--
-- Name: company_profile_mv_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_name ON public.company_profile_mv USING btree (name);


--
-- Name: company_profile_mv_register_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_register_status ON public.company_profile_mv USING btree (register_status);


--
-- Name: company_profile_mv_source_keys; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_source_keys ON public.company_profile_mv USING gin (source_keys);


--
-- Name: company_profile_mv_technology_field; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_technology_field ON public.company_profile_mv USING btree (technology_field);


--
-- Name: company_profile_mv_website; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_website ON public.company_profile_mv USING btree (name) WHERE (website IS NOT NULL);


--
-- Name: company_profile_mv_wz_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_wz_code ON public.company_profile_mv USING btree (wz_code text_pattern_ops);


--
-- Name: company_source_records_active_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_source_records_active_idx ON public.company_source_records USING btree (source_id, is_active, is_deleted_in_source);


--
-- Name: company_source_records_company_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_source_records_company_id_idx ON public.company_source_records USING btree (company_id);


--
-- Name: company_source_records_last_synced_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_source_records_last_synced_at_idx ON public.company_source_records USING btree (last_synced_at);


--
-- Name: company_tags_tag_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_tags_tag_idx ON public.company_tags USING btree (tag_id);


--
-- Name: extraction_feedback_field_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX extraction_feedback_field_idx ON public.extraction_feedback USING btree (field_name, verdict);


--
-- Name: match_candidates_open_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX match_candidates_open_idx ON public.match_candidates USING btree (score DESC) WHERE (status = 'open'::text);


--
-- Name: source_field_stats_mv_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX source_field_stats_mv_key ON public.source_field_stats_mv USING btree (source_key, field_name);


--
-- Name: source_stats_mv_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX source_stats_mv_key ON public.source_stats_mv USING btree (source_key);


--
-- Name: source_sync_runs_source_started_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX source_sync_runs_source_started_at_idx ON public.source_sync_runs USING btree (source_id, started_at DESC);


--
-- Name: source_sync_runs_status_started_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX source_sync_runs_status_started_at_idx ON public.source_sync_runs USING btree (status, started_at DESC);


--
-- Name: sources_next_sync_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX sources_next_sync_at_idx ON public.sources USING btree (next_sync_at);


--
-- Name: tags_name_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX tags_name_key ON public.tags USING btree (lower(btrim(name)));


--
-- Name: uq_obs_company_field_source_value; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_obs_company_field_source_value ON public.company_field_observations USING btree (company_id, field_name, source_id, value_text) WHERE (field_name = ANY (ARRAY['website'::text, 'candidate_url'::text, 'directory_url'::text, 'discovery_tier'::text, 'osm_id'::text, 'technology_field'::text, 'purpose_field'::text, 'technology_field_withheld'::text, 'purpose_field_withheld'::text, 'name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text, 'founding_year'::text]));


--
-- Name: companies companies_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER companies_touch_updated_at BEFORE UPDATE ON public.companies FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: company_canonical_field_sources company_canonical_field_sources_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER company_canonical_field_sources_touch_updated_at BEFORE UPDATE ON public.company_canonical_field_sources FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: company_field_observations company_field_observations_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER company_field_observations_touch_updated_at BEFORE UPDATE ON public.company_field_observations FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: company_managers company_managers_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER company_managers_touch_updated_at BEFORE UPDATE ON public.company_managers FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: company_source_records company_source_records_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER company_source_records_touch_updated_at BEFORE UPDATE ON public.company_source_records FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: extraction_feedback extraction_feedback_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER extraction_feedback_touch_updated_at BEFORE UPDATE ON public.extraction_feedback FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: pipeline_state pipeline_state_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER pipeline_state_touch_updated_at BEFORE UPDATE ON public.pipeline_state FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: source_sync_runs source_sync_runs_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER source_sync_runs_touch_updated_at BEFORE UPDATE ON public.source_sync_runs FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: sources sources_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER sources_touch_updated_at BEFORE UPDATE ON public.sources FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();


--
-- Name: companies companies_merged_into_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_merged_into_fkey FOREIGN KEY (merged_into) REFERENCES public.companies(id) ON DELETE SET NULL;


--
-- Name: company_canonical_field_sources company_canonical_field_sourc_company_field_observation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_canonical_field_sources
    ADD CONSTRAINT company_canonical_field_sourc_company_field_observation_id_fkey FOREIGN KEY (company_field_observation_id) REFERENCES public.company_field_observations(id) ON DELETE CASCADE;


--
-- Name: company_canonical_field_sources company_canonical_field_sources_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_canonical_field_sources
    ADD CONSTRAINT company_canonical_field_sources_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_canonical_field_sources company_canonical_field_sources_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_canonical_field_sources
    ADD CONSTRAINT company_canonical_field_sources_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- Name: company_comments company_comments_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_comments
    ADD CONSTRAINT company_comments_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_documents company_documents_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_documents
    ADD CONSTRAINT company_documents_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_documents company_documents_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_documents
    ADD CONSTRAINT company_documents_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- Name: company_field_history company_field_history_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_history
    ADD CONSTRAINT company_field_history_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_field_history company_field_history_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_history
    ADD CONSTRAINT company_field_history_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE SET NULL;


--
-- Name: company_field_observations company_field_observations_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_observations
    ADD CONSTRAINT company_field_observations_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_field_observations company_field_observations_company_source_record_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_observations
    ADD CONSTRAINT company_field_observations_company_source_record_id_fkey FOREIGN KEY (company_source_record_id) REFERENCES public.company_source_records(id) ON DELETE CASCADE;


--
-- Name: company_field_observations company_field_observations_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_observations
    ADD CONSTRAINT company_field_observations_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- Name: company_field_observations company_field_observations_source_sync_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_field_observations
    ADD CONSTRAINT company_field_observations_source_sync_run_id_fkey FOREIGN KEY (source_sync_run_id) REFERENCES public.source_sync_runs(id) ON DELETE SET NULL;


--
-- Name: company_funding_links company_funding_links_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_funding_links
    ADD CONSTRAINT company_funding_links_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_funding_links company_funding_links_fkz_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_funding_links
    ADD CONSTRAINT company_funding_links_fkz_fkey FOREIGN KEY (fkz) REFERENCES public.company_fundings(fkz) ON DELETE CASCADE;


--
-- Name: company_identifiers company_identifiers_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_identifiers
    ADD CONSTRAINT company_identifiers_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_identifiers company_identifiers_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_identifiers
    ADD CONSTRAINT company_identifiers_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE SET NULL;


--
-- Name: company_managers company_managers_company_field_observation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_managers
    ADD CONSTRAINT company_managers_company_field_observation_id_fkey FOREIGN KEY (company_field_observation_id) REFERENCES public.company_field_observations(id) ON DELETE SET NULL;


--
-- Name: company_managers company_managers_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_managers
    ADD CONSTRAINT company_managers_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_managers company_managers_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_managers
    ADD CONSTRAINT company_managers_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE SET NULL;


--
-- Name: company_source_records company_source_records_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_source_records
    ADD CONSTRAINT company_source_records_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_source_records company_source_records_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_source_records
    ADD CONSTRAINT company_source_records_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- Name: company_tags company_tags_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_tags
    ADD CONSTRAINT company_tags_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: company_tags company_tags_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_tags
    ADD CONSTRAINT company_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.tags(id) ON DELETE CASCADE;


--
-- Name: extraction_feedback extraction_feedback_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.extraction_feedback
    ADD CONSTRAINT extraction_feedback_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: match_candidates match_candidates_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_candidates
    ADD CONSTRAINT match_candidates_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: match_candidates match_candidates_source_record_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.match_candidates
    ADD CONSTRAINT match_candidates_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES public.company_source_records(id) ON DELETE CASCADE;


--
-- Name: review_sample review_sample_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.review_sample
    ADD CONSTRAINT review_sample_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: source_sync_runs source_sync_runs_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_sync_runs
    ADD CONSTRAINT source_sync_runs_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;


--
-- Name: technology_field_keywords technology_field_keywords_field_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.technology_field_keywords
    ADD CONSTRAINT technology_field_keywords_field_fkey FOREIGN KEY (field) REFERENCES public.technology_fields(name) ON UPDATE CASCADE;


-- ================================================================================================
-- Seed rows: the sources the DAGs and the app expect to exist.
-- ================================================================================================

INSERT INTO public.sources (key, name, source_type, priority, metadata)
VALUES (
  'branch_classifier',
  'Branch Classifier (Berlin 77-field taxonomy)',
  'classifier',
  100,
  '{"writes": ["technology_field", "purpose_field", "technology_field_withheld", "purpose_field_withheld"],
    "confidence_basis": "fixed_by_decided_by",
    "confidence_by_decided_by": {"keywords": 0.60, "model_direct": 0.50, "model_tiebreak": 0.55}}'
);

INSERT INTO public.sources (key, name, source_type, base_url) VALUES
    ('foerderkatalog', 'Förderkatalog des Bundes', 'fundings', 'https://foerderportal.bund.de/foekat/'),
    ('berlin_zuwendungen', 'Zuwendungsdatenbank des Landes Berlin', 'fundings',
     'https://www.berlin.de/sen/finanzen/service/zuwendungsdatenbank/')
ON CONFLICT (key) DO NOTHING;

-- People's corrections from the app: a source at priority 50, ahead of every other (100).
INSERT INTO public.sources (key, name, source_type, priority, metadata)
VALUES (
  'user_feedback',
  'Corrections from the app',
  'user_feedback',
  50,
  '{"dag_id": "feedback_observations",
    "confidence_by_net_agreement": {"1": 0.30, "1 on an empty field": 0.60, "2": 0.60, "3+": 0.75}}'
)
ON CONFLICT (key) DO UPDATE SET priority = EXCLUDED.priority;

-- Berlin's 77 Technologiefelder with their keywords, and the WZ divisions (#84). A change is a new
-- migration: a keyword is removed by setting `removed_at` and `removed_reason`, never deleted.

INSERT INTO public.technology_fields (name, name_en, senweb_unit, berlin_partner, related) VALUES
    ('5G- bzw. 6G Anwendungen', '5G/6G Applications', 'IIIB4', 'Juliane Haupt', ARRAY['62.10.3']::text[]),
    ('Additive Fertigung', 'Additive Manufacturing', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Advanced Materials', 'Advanced Materials', 'III B 3', 'David Hampel', '{}'::text[]),
    ('Architekturmarkt', 'Architecture Market', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Automobilwirtschaft', 'Automotive Industry', 'IIIB3, IIIC1', 'Sascha Tiede, David Hampel', '{}'::text[]),
    ('Autonomes, vernetzes Fahren', 'Autonomous and Connected Driving', 'IIIB3', 'Sascha Tiede', '{}'::text[]),
    ('Bahnindustrie', 'Rail Industry', 'IIIB3', 'Sascha Tiede', '{}'::text[]),
    ('Batterietechnologien', 'Battery Technologies', NULL, NULL, ARRAY['Elektromobilität', 'Ladeinfrastruktur']::text[]),
    ('Bauindustrie', 'Construction Industry', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Bioproduktion', 'Bioproduction', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Biotechnologie', 'Biotechnology', 'IIIB2', 'Carolin Clement', ARRAY['Pharma & Wirkstoffe', 'Künstliche Intelligenz']::text[]),
    ('Buchmarkt', 'Book Market', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Carbon Management', 'Carbon Management', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Chemische Industrie', 'Chemical Industry', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Cybersicherheit', 'Cybersecurity', 'IIIB1', 'Juliane Haupt', ARRAY['Künstliche Intelligenz']::text[]),
    ('Defence Tech', 'Defence Tech', NULL, NULL, ARRAY['Drohnen', 'Raumfahrt']::text[]),
    ('Designwirtschaft', 'Design Industry', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Dienstleistung', 'Services', 'IIB2', NULL, '{}'::text[]),
    ('Digital Health', 'Digital Health', 'IIIB2', 'Petra Schmauß', ARRAY['Künstliche Intelligenz', 'Softwaretechnologien']::text[]),
    ('Drohnen', 'Drones', 'IIIB3', 'Sascha Tiede', ARRAY['Dual Use']::text[]),
    ('Elektroindustrie', 'Electrical Industry', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Elektromobilität', 'Electromobility', 'IIIB3', 'Sascha Tiede', ARRAY['Ladeinfrastruktur', 'Batterietechnologien']::text[]),
    ('Energiespeicher', 'Energy Storage', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Ernährungsindustrie', 'Food Industry', 'III C 11', 'David Hampel, Jens Woelki, Dorota Thiel-Jankievicz', '{}'::text[]),
    ('Fernwärme', 'District Heating', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Filmwirtschaft', 'Film Industry', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('FinTech', 'FinTech', 'IIA4', 'Juliane Haupt', ARRAY['Web3', 'Blockchain', 'DLT']::text[]),
    ('Foodtech', 'Foodtech', 'III C 11', 'Jens Woelki', '{}'::text[]),
    ('Freiberufler', 'Freelancers', 'IIB2', NULL, '{}'::text[]),
    ('Games', 'Games', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Gastgewerbe', 'Hospitality', 'IIB3', NULL, '{}'::text[]),
    ('Geothermie', 'Geothermal Energy', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Gesundheitsversorgung & Pflege', 'Healthcare & Nursing', 'IIIB2', 'Petra Schmauß', ARRAY['Digital Health', 'Medizintechnik', 'Künstliche Intelligenz']::text[]),
    ('GovTech', 'GovTech', 'IIA4', 'Juliane Haupt', '{}'::text[]),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Rubber and Plastics Processing Industry', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Halbleitertechnologien', 'Semiconductor Technologies', 'IIIB1', 'Gerrit Rössler', ARRAY['Mikroelektronik']::text[]),
    ('Handel', 'Trade', 'IIB2', NULL, '{}'::text[]),
    ('Handwerk', 'Skilled Crafts', 'IIB2', NULL, '{}'::text[]),
    ('Immersive Technologien (VR, AR, XR)', 'Immersive Technologies (VR, AR, XR)', 'IIIB1', 'Juliane Haupt', '{}'::text[]),
    ('Kreislaufwirtschaft', 'Circular Economy', NULL, NULL, '{}'::text[]),
    ('Kunstmarkt', 'Art Market', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Künstliche Intelligenz', 'Artificial Intelligence', 'IIIB1', 'Juliane Haupt', '{}'::text[]),
    ('Ladeinfrastruktur', 'Charging Infrastructure', 'IIIB3', 'Sascha Tiede', ARRAY['Elektromobilität', 'Batterietechnologien']::text[]),
    ('Logistik', 'Logistics', 'IIIB3', 'Sascha Tiede', '{}'::text[]),
    ('Markt für darstellende Künste', 'Performing Arts Market', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Maschinen- und Anlagenbau', 'Mechanical and Plant Engineering', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Medizintechnik', 'Medical Technology', 'IIIB2', 'Petra Schmauß', ARRAY['Digital Health', 'Optik & Photonik', 'Künstliche Intelligenz']::text[]),
    ('Messe- und Kongresswirtschaft', 'Trade Fair and Congress Industry', 'IIB3', NULL, '{}'::text[]),
    ('Metallindustrie', 'Metal Industry', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Mikroelektronik', 'Microelectronics', 'IIIB1', 'Gerrit Rössler', ARRAY['Halbleitertechnologien']::text[]),
    ('Mobilitätssoftware', 'Mobility Software', 'IIIB3', 'Sascha Tiede', '{}'::text[]),
    ('Modewirtschaft', 'Fashion Industry', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Musikwirtschaft', 'Music Industry', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Nachhaltige Wirtschaft', 'Sustainable Economy', 'IIA6', 'Diana Woelki, Robin Bruck', '{}'::text[]),
    ('Neue Mobilitätsangebote', 'New Mobility Services', 'IIIB3', 'Sascha Tiede', '{}'::text[]),
    ('Papier- und Druckindustrie', 'Paper and Printing Industry', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Pharma & Wirkstoffe', 'Pharma & Active Ingredients', 'IIIB2', 'Carolin Clement', ARRAY['Biotechnologie', 'Medizintechnik']::text[]),
    ('Pharmaindustrie', 'Pharmaceutical Industry', 'IIIC1', 'Carolin Clement', '{}'::text[]),
    ('Photonik', 'Photonics', 'IIIB1', 'Gerrit Rössler', ARRAY['Quantentechnologien']::text[]),
    ('Pressemarkt', 'Press Market', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Quantentechnologien', 'Quantum Technologies', 'IIIB1', 'Gerrit Rössler', ARRAY['Photonik']::text[]),
    ('Raumfahrt', 'Space', 'IIIB3', 'Sascha Tiede', ARRAY['DefenceTech']::text[]),
    ('Rechenzentrenbetreiber', 'Data Centre Operators', 'IIIB4', 'Juliane Haupt', ARRAY['63.10.1', '63.10.2', '63.10.9']::text[]),
    ('Recycling', 'Recycling', 'IIIC1', 'Hannes Lebert', '{}'::text[]),
    ('Robotik', 'Robotics', 'IIIC1', 'David Hampel', '{}'::text[]),
    ('Rundfunkwirtschaft', 'Broadcasting Industry', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Sicherheits- und Verteidigungsindustrie', 'Security and Defence Industry', 'III C 1', 'David Hampel', '{}'::text[]),
    ('Solarenergie', 'Solar Energy', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Soziale Unternehmen', 'Social Enterprises', 'IIA6', 'Diana Woelki, Anne Le Rolland, Janis Gummersbach', '{}'::text[]),
    ('Stromnetz', 'Power Grid', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Telekommunikationsanbieter', 'Telecommunications Providers', 'IIIB4', 'Juliane Haupt', ARRAY['61.10.0', '61.10.1', '61.10.2']::text[]),
    ('Tourismus', 'Tourism', 'IIB3', NULL, '{}'::text[]),
    ('Wasserstoff', 'Hydrogen', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Wassertechnologien', 'Water Technologies', 'IIIA4', 'Wolfgang Korek', '{}'::text[]),
    ('Web3, Blockchain, DLT', 'Web3, Blockchain, DLT', 'IIIB1', 'Juliane Haupt', ARRAY['FinTech']::text[]),
    ('Werbemarkt', 'Advertising Market', 'IIB4', 'Justina Siegmund-Born', '{}'::text[]),
    ('Windenergie', 'Wind Energy', 'IIIA4', 'Wolfgang Korek', '{}'::text[]);

INSERT INTO public.technology_field_keywords (field, keyword) VALUES
    ('5G- bzw. 6G Anwendungen', 'Campusnetze'),
    ('5G- bzw. 6G Anwendungen', 'IoT'),
    ('5G- bzw. 6G Anwendungen', 'Edge Computing'),
    ('5G- bzw. 6G Anwendungen', 'Network Slicing'),
    ('Additive Fertigung', '3D-Druck'),
    ('Additive Fertigung', 'Rapid Prototyping'),
    ('Additive Fertigung', 'Serienfertigung'),
    ('Additive Fertigung', 'Leichtbau'),
    ('Additive Fertigung', 'Funktionsbauteile'),
    ('Advanced Materials', 'Hochleistungswerkstoffe'),
    ('Advanced Materials', 'Verbundwerkstoffe'),
    ('Advanced Materials', 'Leichtbau'),
    ('Advanced Materials', 'Nanotechnologie'),
    ('Advanced Materials', 'Funktionswerkstoffe'),
    ('Architekturmarkt', 'BIM'),
    ('Architekturmarkt', 'Nachhaltiges Bauen'),
    ('Architekturmarkt', 'Generalplanung'),
    ('Architekturmarkt', 'Objektplanung'),
    ('Architekturmarkt', 'Revitalisierung'),
    ('Automobilwirtschaft', 'Elektromobilität'),
    ('Automobilwirtschaft', 'Autonomes Fahren'),
    ('Automobilwirtschaft', 'Konnektivität'),
    ('Automobilwirtschaft', 'Nachhaltigkeit'),
    ('Automobilwirtschaft', 'Automotive'),
    ('Automobilwirtschaft', 'Fahrzeugtechnik'),
    ('Automobilwirtschaft', 'Smart Mobility'),
    ('Automobilwirtschaft', 'Future Mobility'),
    ('Automobilwirtschaft', 'Fahrzeugentwicklung'),
    ('Automobilwirtschaft', 'Fahrzeugproduktion'),
    ('Automobilwirtschaft', 'Automotive Engineering'),
    ('Automobilwirtschaft', 'Smart Factory'),
    ('Automobilwirtschaft', 'Industrie 4.0'),
    ('Automobilwirtschaft', 'Robotik'),
    ('Automobilwirtschaft', 'Predictive Maintenance'),
    ('Automobilwirtschaft', 'Supply Chain'),
    ('Automobilwirtschaft', 'Leichtbau'),
    ('Automobilwirtschaft', 'Circular Economy'),
    ('Autonomes, vernetzes Fahren', 'V2X-Kommunikation'),
    ('Autonomes, vernetzes Fahren', 'Sensorfusion'),
    ('Autonomes, vernetzes Fahren', 'Fahrerassistenzsysteme'),
    ('Autonomes, vernetzes Fahren', 'Software-defined Vehicle'),
    ('Autonomes, vernetzes Fahren', 'Telematik'),
    ('Autonomes, vernetzes Fahren', 'Teleoperiertes Fahren'),
    ('Autonomes, vernetzes Fahren', 'Connected Car'),
    ('Autonomes, vernetzes Fahren', 'V2X'),
    ('Autonomes, vernetzes Fahren', 'V2I (Vehicle-to-Infrastructure)'),
    ('Autonomes, vernetzes Fahren', 'V2V (Vehicle-to-Vehicle)'),
    ('Autonomes, vernetzes Fahren', 'Cloud Connectivity'),
    ('Autonomes, vernetzes Fahren', 'Autonomous Driving'),
    ('Autonomes, vernetzes Fahren', 'Automated Driving'),
    ('Autonomes, vernetzes Fahren', 'ADAS'),
    ('Autonomes, vernetzes Fahren', 'LiDAR'),
    ('Autonomes, vernetzes Fahren', 'Radar'),
    ('Autonomes, vernetzes Fahren', 'Machine Learning'),
    ('Autonomes, vernetzes Fahren', 'Deep Learning'),
    ('Autonomes, vernetzes Fahren', 'Digital Twin'),
    ('Autonomes, vernetzes Fahren', 'AI'),
    ('Autonomes, vernetzes Fahren', 'Simulation'),
    ('Autonomes, vernetzes Fahren', 'Selbstfahrende Autos'),
    ('Autonomes, vernetzes Fahren', 'Robotaxis'),
    ('Autonomes, vernetzes Fahren', 'SAE/Level 3-5'),
    ('Autonomes, vernetzes Fahren', 'Operational Design Domain'),
    ('Autonomes, vernetzes Fahren', 'Motion Planing'),
    ('Autonomes, vernetzes Fahren', 'Localization'),
    ('Autonomes, vernetzes Fahren', 'Autonomous Shuttle'),
    ('Autonomes, vernetzes Fahren', 'Shuttle-Bus'),
    ('Autonomes, vernetzes Fahren', 'Connected Vehicle'),
    ('Autonomes, vernetzes Fahren', 'Vehicle Operating System'),
    ('Autonomes, vernetzes Fahren', 'AI for Mobility'),
    ('Autonomes, vernetzes Fahren', '3D-Umgebungserfassung'),
    ('Autonomes, vernetzes Fahren', 'Vollautonome Fahrzeuge'),
    ('Bahnindustrie', 'Bahntechnik'),
    ('Bahnindustrie', 'Schienenfahrzeuge'),
    ('Bahnindustrie', 'Signaltechnik'),
    ('Bahnindustrie', 'Schieneninfrastruktur'),
    ('Bahnindustrie', 'Railway Technology'),
    ('Bahnindustrie', 'Rail Industry'),
    ('Bahnindustrie', 'Railway Engineering'),
    ('Bahnindustrie', 'Rail Systems'),
    ('Bahnindustrie', 'Rail Infrastructure'),
    ('Bahnindustrie', 'Schienenverkehr'),
    ('Bahnindustrie', 'Eisenbahntechnik'),
    ('Bahnindustrie', 'Bahninfrastruktur'),
    ('Bahnindustrie', 'Bahnbetrieb'),
    ('Bahnindustrie', 'Digitale Schiene'),
    ('Bahnindustrie', 'Digitaler Zwilling'),
    ('Bahnindustrie', 'Automatisierter Bahnverkehr'),
    ('Bahnindustrie', 'hochautomatisiertes Fahren'),
    ('Bahnindustrie', 'Zugsteuerung'),
    ('Bahnindustrie', 'Sensorik'),
    ('Bahnindustrie', 'Bahnsicherheit'),
    ('Bahnindustrie', 'Autonomous Rail'),
    ('Bahnindustrie', 'Smart Infrastructure'),
    ('Bahnindustrie', 'Green Rail'),
    ('Bahnindustrie', 'Sustainable Mobility'),
    ('Bahnindustrie', 'Rail Telematics'),
    ('Bahnindustrie', 'AI for Rail'),
    ('Bahnindustrie', 'European Train Control System (ETCS)'),
    ('Bahnindustrie', 'European Rail Traffic Management System (ERTMS)'),
    ('Batterietechnologien', 'Lithium-Ionen-Technologie'),
    ('Batterietechnologien', 'Lithium-Schwefel-Batterien'),
    ('Batterietechnologien', 'Feststoffbatterie'),
    ('Batterietechnologien', 'Energiespeichersysteme'),
    ('Batterietechnologien', 'Batteriemanagementsystem'),
    ('Batterietechnologien', 'Batterierecycling'),
    ('Batterietechnologien', 'Akkus'),
    ('Bauindustrie', 'Nachhaltiges Bauen'),
    ('Bauindustrie', 'Kreislaufwirtschaft'),
    ('Bauindustrie', 'Baustoffe'),
    ('Bauindustrie', 'Holzbau'),
    ('Bauindustrie', 'Dekarbonisierung'),
    ('Bioproduktion', 'Fermentation'),
    ('Bioproduktion', 'Bioreaktoren'),
    ('Bioproduktion', 'Bioprozesstechnik'),
    ('Bioproduktion', 'Zellkultur'),
    ('Biotechnologie', 'Biotechnologie'),
    ('Biotechnologie', 'Wirkstoffentwicklung'),
    ('Biotechnologie', 'Antikörper'),
    ('Biotechnologie', 'Bioprozess'),
    ('Biotechnologie', 'GMP'),
    ('Biotechnologie', 'Genomik'),
    ('Biotechnologie', 'ATMP'),
    ('Biotechnologie', 'Zell- und Gentherapie'),
    ('Buchmarkt', 'Verlagswesen'),
    ('Buchmarkt', 'Digital Publishing'),
    ('Buchmarkt', 'Buchvertrieb'),
    ('Buchmarkt', 'Print-on-Demand'),
    ('Buchmarkt', 'Self-Publishing'),
    ('Buchmarkt', 'Buchhandel'),
    ('Carbon Management', 'direct air capture'),
    ('Carbon Management', 'ccs'),
    ('Carbon Management', 'ccu'),
    ('Chemische Industrie', 'Spezialchemie'),
    ('Chemische Industrie', 'Feinchemikalien'),
    ('Chemische Industrie', 'Polymere'),
    ('Chemische Industrie', 'Kreislaufwirtschaft'),
    ('Chemische Industrie', 'Grüne Chemie'),
    ('Cybersicherheit', 'IT-Sicherheit'),
    ('Cybersicherheit', 'Verschlüsselung'),
    ('Cybersicherheit', 'Penetration Testing'),
    ('Cybersicherheit', 'Cloud-Security'),
    ('Cybersicherheit', 'Digitale Identität'),
    ('Defence Tech', 'Dual Use'),
    ('Defence Tech', 'Verteidigung'),
    ('Defence Tech', 'Defense Technology'),
    ('Defence Tech', 'Verteidigungstechnik'),
    ('Defence Tech', 'Cyber-Defense'),
    ('Defence Tech', 'Schutzsysteme'),
    ('Defence Tech', 'Sensorik'),
    ('Defence Tech', 'Aufklärungssysteme'),
    ('Defence Tech', 'Drohnenabwehr'),
    ('Defence Tech', 'KI basierte Lagebilder'),
    ('Defence Tech', 'Erdbedobachtung'),
    ('Defence Tech', 'Satellitendaten'),
    ('Defence Tech', 'Raumfahrttechnologien'),
    ('Defence Tech', 'Software Defined Defence'),
    ('Designwirtschaft', 'Mediendesign'),
    ('Designwirtschaft', 'Markenidentität'),
    ('Designwirtschaft', 'Produktdesign'),
    ('Designwirtschaft', 'Kreativagentur'),
    ('Designwirtschaft', 'Corporate Design'),
    ('Dienstleistung', 'Schausteller'),
    ('Dienstleistung', 'Sicherheitskräfte'),
    ('Dienstleistung', 'Taxigewerbe'),
    ('Dienstleistung', 'Lieferdienste'),
    ('Dienstleistung', 'Makler'),
    ('Dienstleistung', 'Fitnessstudio'),
    ('Digital Health', 'Digital Health'),
    ('Digital Health', 'Telemedizin'),
    ('Digital Health', 'DiGA'),
    ('Digital Health', 'ePA'),
    ('Digital Health', 'Interoperabilität'),
    ('Digital Health', 'KI-Diagnostik'),
    ('Drohnen', 'UAV'),
    ('Drohnen', 'UAM'),
    ('Drohnen', 'UIC'),
    ('Drohnen', 'unbemannte Luftfahrt'),
    ('Drohnen', 'Drohnentechnologie'),
    ('Drohnen', 'Luftbildvermessung'),
    ('Drohnen', 'Sensorik'),
    ('Drohnen', 'Vermessung'),
    ('Drohnen', 'Drohnensicherheit'),
    ('Drohnen', 'Bildkomposition'),
    ('Drohnen', 'Lufträume'),
    ('Drohnen', 'U-Space'),
    ('Drohnen', 'unbemannten Fluggeräte'),
    ('Drohnen', 'Drohnendetektion'),
    ('Drohnen', 'Drohneninfrastruktur'),
    ('Elektroindustrie', 'Automatisierungstechnik'),
    ('Elektroindustrie', 'Elektrifizierung'),
    ('Elektroindustrie', 'Elektrotechnik'),
    ('Elektroindustrie', 'Halbleiter'),
    ('Elektroindustrie', 'Industrie 4.0'),
    ('Elektroindustrie', 'Sensorik'),
    ('Elektroindustrie', 'Elektromobilität'),
    ('Elektromobilität', 'Ladeinfrastruktur'),
    ('Elektromobilität', 'Batterietechnologie'),
    ('Elektromobilität', 'Ladestationen'),
    ('Elektromobilität', 'E-Mobilität'),
    ('Elektromobilität', 'E-Autos'),
    ('Elektromobilität', 'Flottenmanagement'),
    ('Elektromobilität', 'Elektrifizierung'),
    ('Elektromobilität', 'Elektrotechnik'),
    ('Energiespeicher', 'Wasserstoffspeicher'),
    ('Energiespeicher', 'Wärmespeicher'),
    ('Energiespeicher', 'Stromspeicher'),
    ('Ernährungsindustrie', 'Lebensmittelsicherheit'),
    ('Ernährungsindustrie', 'Rückverfolgbarkeit'),
    ('Ernährungsindustrie', 'Nachhaltigkeit'),
    ('Fernwärme', 'Wärmequellen'),
    ('Fernwärme', 'Steuerung'),
    ('Fernwärme', 'Smart Grid'),
    ('Filmwirtschaft', 'Filmproduktion'),
    ('Filmwirtschaft', 'Postproduktion'),
    ('Filmwirtschaft', 'Content Creation'),
    ('Filmwirtschaft', 'Audiovisuelle Medien'),
    ('Filmwirtschaft', 'VFX'),
    ('FinTech', 'Payment-Lösungen'),
    ('FinTech', 'Open Banking'),
    ('FinTech', 'Digital Banking'),
    ('FinTech', 'Blockchain'),
    ('FinTech', 'Robo-Advisory'),
    ('Foodtech', 'Pflanzenbasierte Proteine'),
    ('Foodtech', 'Biotechnologie'),
    ('Foodtech', 'Feermentation'),
    ('Foodtech', 'Aquaponik'),
    ('Freiberufler', 'Ingenieure'),
    ('Freiberufler', 'Architekten'),
    ('Freiberufler', 'Steuerberater'),
    ('Freiberufler', 'Unternehmensberater'),
    ('Freiberufler', 'Wirtschaftsprüfer'),
    ('Games', 'Spieleentwicklung'),
    ('Games', 'Game Design'),
    ('Games', 'Gamification'),
    ('Games', 'Esports'),
    ('Games', 'Virtual Reality'),
    ('Gastgewerbe', 'Hotellerie'),
    ('Gastgewerbe', 'Gastronomie'),
    ('Gastgewerbe', 'Beherbergung'),
    ('Gastgewerbe', 'Bewirtung'),
    ('Gastgewerbe', 'Restaurant'),
    ('Gastgewerbe', 'Café'),
    ('Geothermie', 'Umweltwärme'),
    ('Geothermie', 'Tiefengeothermie'),
    ('Geothermie', 'Erdwärme'),
    ('Gesundheitsversorgung & Pflege', 'Krankenhaus'),
    ('Gesundheitsversorgung & Pflege', 'ambulante Versorgung'),
    ('Gesundheitsversorgung & Pflege', 'MVZ'),
    ('Gesundheitsversorgung & Pflege', 'Pflegedienst'),
    ('Gesundheitsversorgung & Pflege', 'Rehabilitation'),
    ('Gesundheitsversorgung & Pflege', 'Pflege-Robotik'),
    ('GovTech', 'Verwaltungsdigitalisierung'),
    ('GovTech', 'E-Government'),
    ('GovTech', 'Digitale Souveränität'),
    ('GovTech', 'Bürgerservice'),
    ('GovTech', 'Smart Government'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Spritzguss'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Extrusion'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Elastomertechnik'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Thermoplaste'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Compoundierung'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Formenbau'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Verpackungen'),
    ('Gummi- und Kunststoffverarbeitende Industrie', 'Kunststoffrecycling'),
    ('Halbleitertechnologien', 'Mikroelektronik'),
    ('Halbleitertechnologien', 'Halbleiterfertigung'),
    ('Halbleitertechnologien', 'Leistungselektronik'),
    ('Halbleitertechnologien', 'Chipdesign'),
    ('Halbleitertechnologien', 'Wafertechnologie'),
    ('Halbleitertechnologien', 'Semiconductor'),
    ('Handel', 'E-Commerce'),
    ('Handel', 'Einzelhandel'),
    ('Handel', 'Lebensmittel'),
    ('Handel', 'Spätverkaufsstellen'),
    ('Handel', 'Shoppingcenter/Einkaufszentren'),
    ('Immersive Technologien (VR, AR, XR)', 'Virtual Reality'),
    ('Immersive Technologien (VR, AR, XR)', 'Augmented Reality'),
    ('Immersive Technologien (VR, AR, XR)', 'Mixed Reality'),
    ('Immersive Technologien (VR, AR, XR)', 'Spatial Computing'),
    ('Immersive Technologien (VR, AR, XR)', 'Immersive Experience'),
    ('Kreislaufwirtschaft', 'Cradle-to-Cradle'),
    ('Kreislaufwirtschaft', 'Ressourceneffizienz'),
    ('Kreislaufwirtschaft', 'Wertstoffkreislauf'),
    ('Kreislaufwirtschaft', 'Kreislauffähigkeit'),
    ('Kreislaufwirtschaft', 'Zero Waste'),
    ('Kreislaufwirtschaft', 'Lebenszyklusverlängerung'),
    ('Kreislaufwirtschaft', 'R-Strategien'),
    ('Kreislaufwirtschaft', 'zirkulär'),
    ('Kunstmarkt', 'Kunsthandel'),
    ('Kunstmarkt', 'Kunstbewertung'),
    ('Kunstmarkt', 'Sammlungsmanagement'),
    ('Künstliche Intelligenz', 'Maschinelles Lernen'),
    ('Künstliche Intelligenz', 'Neuronale Netze'),
    ('Künstliche Intelligenz', 'Deep Learning'),
    ('Künstliche Intelligenz', 'Generative KI'),
    ('Künstliche Intelligenz', 'Natural Language Processing'),
    ('Ladeinfrastruktur', 'Ladetechnologien'),
    ('Ladeinfrastruktur', 'Schnellladen'),
    ('Ladeinfrastruktur', 'Smart Charging'),
    ('Ladeinfrastruktur', 'Wallbox'),
    ('Ladeinfrastruktur', 'bidirektionales Laden'),
    ('Ladeinfrastruktur', 'HPC-Laden'),
    ('Ladeinfrastruktur', 'Megawatt-Charging'),
    ('Ladeinfrastruktur', 'MCS'),
    ('Logistik', 'Lieferkettenmanagement'),
    ('Logistik', 'Kontraktlogistik'),
    ('Logistik', 'Lagerlogistik'),
    ('Logistik', 'Transportlogistik'),
    ('Logistik', 'Green Logistics'),
    ('Logistik', 'Verkehrslogistik'),
    ('Logistik', 'Güterverkehr'),
    ('Logistik', 'Warenverkehr'),
    ('Logistik', 'Letzte Meile'),
    ('Logistik', 'Spedition'),
    ('Logistik', 'Logistikdienstleister'),
    ('Logistik', 'Wertschöpfungskette'),
    ('Logistik', 'Transportation Management System'),
    ('Logistik', 'Supply Chain Resilience'),
    ('Logistik', 'Digitale Logistik'),
    ('Logistik', 'Internet of Things'),
    ('Logistik', 'Smart Logistics'),
    ('Logistik', 'Cloud Logistics'),
    ('Logistik', 'Automatisiertes Lager'),
    ('Logistik', 'Robotik'),
    ('Logistik', 'Autonome Logistik'),
    ('Logistik', 'Fahrerlose Transportsysteme'),
    ('Logistik', 'Urbane Logistik'),
    ('Logistik', 'Last Mile Delivery'),
    ('Logistik', 'City Logistics'),
    ('Logistik', 'Depot'),
    ('Logistik', 'Mikrodepot'),
    ('Logistik', 'Paketlogistik'),
    ('Logistik', 'ITS'),
    ('Logistik', 'Cargo Bikes'),
    ('Logistik', 'Sustainable Logistics'),
    ('Logistik', 'Nachhaltige Lieferketten'),
    ('Logistik', 'Flurförderfahrzeuge'),
    ('Logistik', 'Kurier-Express Dienstleistungen'),
    ('Logistik', 'Drohnenlieferservice'),
    ('Markt für darstellende Künste', 'Bühnenproduktion'),
    ('Markt für darstellende Künste', 'Kulturmanagement'),
    ('Markt für darstellende Künste', 'Live-Entertainment'),
    ('Markt für darstellende Künste', 'Inszenierung'),
    ('Markt für darstellende Künste', 'Spielstätte'),
    ('Maschinen- und Anlagenbau', 'Automatisierungstechnik'),
    ('Maschinen- und Anlagenbau', 'Antriebssysteme'),
    ('Maschinen- und Anlagenbau', 'Prozessanlagen'),
    ('Maschinen- und Anlagenbau', 'Industrieanlagen'),
    ('Maschinen- und Anlagenbau', 'Werkzeugmaschinen'),
    ('Maschinen- und Anlagenbau', 'Fördertechnik'),
    ('Maschinen- und Anlagenbau', 'Sondermaschinenbau'),
    ('Maschinen- und Anlagenbau', 'Industrie 4.0'),
    ('Maschinen- und Anlagenbau', 'Steuerungstechnik'),
    ('Medizintechnik', 'Medizinprodukt'),
    ('Medizintechnik', 'MDR'),
    ('Medizintechnik', 'Implantat'),
    ('Medizintechnik', 'Diagnostik'),
    ('Medizintechnik', 'Bildgebung'),
    ('Medizintechnik', 'Automat'),
    ('Messe- und Kongresswirtschaft', 'Veranstaltungsort'),
    ('Messe- und Kongresswirtschaft', 'Networking'),
    ('Messe- und Kongresswirtschaft', 'Standbau'),
    ('Messe- und Kongresswirtschaft', 'Kongressorganisation'),
    ('Metallindustrie', 'Metallverarbeitung'),
    ('Metallindustrie', 'Blechbearbeitung'),
    ('Metallindustrie', 'CNC-Zerspanung'),
    ('Metallindustrie', 'Präzisionstechnik'),
    ('Metallindustrie', 'Oberflächentechnik'),
    ('Metallindustrie', 'Umformtechnik'),
    ('Mikroelektronik', 'Halbleiter'),
    ('Mikroelektronik', 'Chipdesign'),
    ('Mikroelektronik', 'Sensorik'),
    ('Mikroelektronik', 'Leistungselektronik'),
    ('Mikroelektronik', 'Mikrosystemtechnik'),
    ('Mikroelektronik', 'Advanced Packaging'),
    ('Mikroelektronik', 'Heterointergration'),
    ('Mobilitätssoftware', 'Mobility-as-a-Service'),
    ('Mobilitätssoftware', 'Flottenmanagement'),
    ('Mobilitätssoftware', 'Shared Mobility'),
    ('Mobilitätssoftware', 'Routenoptimierung'),
    ('Mobilitätssoftware', 'Smart Mobility'),
    ('Mobilitätssoftware', 'Digitale Mobilität'),
    ('Mobilitätssoftware', 'Mobilitätsplattform'),
    ('Mobilitätssoftware', 'Mobilitätsdienstleistungen'),
    ('Mobilitätssoftware', 'Verkehrstechnologie'),
    ('Mobilitätssoftware', 'Verkehrssysteme'),
    ('Mobilitätssoftware', 'Intelligente Mobilität'),
    ('Mobilitätssoftware', 'Zukunftsmobilität'),
    ('Mobilitätssoftware', 'Sustainable Mobility'),
    ('Mobilitätssoftware', 'Multimodale Mobilität'),
    ('Mobilitätssoftware', 'Mobilitäts-Apps'),
    ('Mobilitätssoftware', 'Mobility Data'),
    ('Mobilitätssoftware', 'Open Data'),
    ('Mobilitätssoftware', 'Digitaler Zwilling'),
    ('Mobilitätssoftware', 'Big Data'),
    ('Mobilitätssoftware', 'Open Mobility Data'),
    ('Mobilitätssoftware', 'Sharing'),
    ('Mobilitätssoftware', 'Mobilitätssoftware'),
    ('Mobilitätssoftware', 'Verkehrsoptimierung'),
    ('Mobilitätssoftware', 'Verkehrsmanagement'),
    ('Mobilitätssoftware', 'Smart City Plattform'),
    ('Mobilitätssoftware', 'Digitale Infrastruktur'),
    ('Mobilitätssoftware', 'ITS'),
    ('Mobilitätssoftware', 'Intelligente Verkehrsysteme'),
    ('Mobilitätssoftware', 'ÖPNV-Software'),
    ('Mobilitätssoftware', 'Vehicle Data Platform'),
    ('Mobilitätssoftware', 'Vehicle Connectivity'),
    ('Mobilitätssoftware', 'Telematik'),
    ('Modewirtschaft', 'Modedesign'),
    ('Modewirtschaft', 'Kollektion'),
    ('Musikwirtschaft', 'Musikproduktion'),
    ('Musikwirtschaft', 'Musikvertrieb'),
    ('Musikwirtschaft', 'Künstlermanagement'),
    ('Musikwirtschaft', 'Musikverlag'),
    ('Musikwirtschaft', 'Konzertveranstalter'),
    ('Musikwirtschaft', 'Clubs'),
    ('Nachhaltige Wirtschaft', 'Kreislaufwirtschaft'),
    ('Nachhaltige Wirtschaft', 'Erneuerbare Energien'),
    ('Nachhaltige Wirtschaft', 'Ressourceneffizienz'),
    ('Nachhaltige Wirtschaft', 'CO2-Neutralität'),
    ('Nachhaltige Wirtschaft', 'Corporate Social Responsibility'),
    ('Nachhaltige Wirtschaft', 'ESG'),
    ('Neue Mobilitätsangebote', 'Mobility-as-a-Service'),
    ('Neue Mobilitätsangebote', 'Carsharing'),
    ('Neue Mobilitätsangebote', 'Mikromobilität'),
    ('Neue Mobilitätsangebote', 'Ridepooling'),
    ('Neue Mobilitätsangebote', 'Intermodale Mobilität'),
    ('Neue Mobilitätsangebote', 'Shared Mobility'),
    ('Neue Mobilitätsangebote', 'Bikesharing'),
    ('Neue Mobilitätsangebote', 'Ridehailing'),
    ('Neue Mobilitätsangebote', 'On-Demand Mobility'),
    ('Neue Mobilitätsangebote', 'Elektrokleinstfahrzeuge'),
    ('Neue Mobilitätsangebote', 'E-Roller'),
    ('Neue Mobilitätsangebote', 'E-Scooter'),
    ('Papier- und Druckindustrie', 'Digitaldruck'),
    ('Papier- und Druckindustrie', 'Offsetdruck'),
    ('Papier- und Druckindustrie', 'Druckveredelung'),
    ('Papier- und Druckindustrie', 'Verpackungsdruck'),
    ('Papier- und Druckindustrie', 'Printmedien'),
    ('Pharma & Wirkstoffe', 'Arzneimittel'),
    ('Pharma & Wirkstoffe', 'Wirkstoff'),
    ('Pharma & Wirkstoffe', 'klinische Studien'),
    ('Pharma & Wirkstoffe', 'Galenik'),
    ('Pharma & Wirkstoffe', 'Generika'),
    ('Pharma & Wirkstoffe', 'Orphan Drugs'),
    ('Pharma & Wirkstoffe', 'Pharmazeutika'),
    ('Pharmaindustrie', 'Wirkstoffentwicklung'),
    ('Pharmaindustrie', 'Klinische Studien'),
    ('Pharmaindustrie', 'Biotechnologie'),
    ('Pharmaindustrie', 'Arzneimittelsicherheit'),
    ('Pharmaindustrie', 'Zulassungsverfahren'),
    ('Photonik', 'Lasertechnik'),
    ('Photonik', 'Optoelektronik'),
    ('Photonik', 'Optische Systeme'),
    ('Photonik', 'Sensorik'),
    ('Photonik', 'Machine Vision'),
    ('Pressemarkt', 'Digital-Publishing'),
    ('Pressemarkt', 'Qualitätsjournalismus'),
    ('Pressemarkt', 'Paid Content'),
    ('Pressemarkt', 'Medienvertrieb'),
    ('Pressemarkt', 'Printmedien'),
    ('Quantentechnologien', 'Quantencomputing'),
    ('Quantentechnologien', 'Quantensensorik'),
    ('Quantentechnologien', 'Quantenkryptographie'),
    ('Quantentechnologien', 'Quantenkommunikation'),
    ('Quantentechnologien', 'Quantum Key Distribution'),
    ('Raumfahrt', 'Satelliten'),
    ('Raumfahrt', 'New Space'),
    ('Raumfahrt', 'Luft- und Raumfahrt'),
    ('Raumfahrt', 'Trägerraketen'),
    ('Raumfahrt', 'Erdbeobachtung'),
    ('Raumfahrt', 'Weltraumschrott'),
    ('Raumfahrt', 'Mondmission'),
    ('Raumfahrt', 'Kleinsatelliten'),
    ('Raumfahrt', 'Datenauswertung'),
    ('Raumfahrt', 'Moon Rover'),
    ('Raumfahrt', 'Satellitenkonstellation'),
    ('Rechenzentrenbetreiber', 'Compute'),
    ('Rechenzentrenbetreiber', 'Rechenleistung'),
    ('Rechenzentrenbetreiber', 'Cloud-Infrastruktur'),
    ('Recycling', 'Kreislaufwirtschaft'),
    ('Recycling', 'Wertstoffrückgewinnung'),
    ('Recycling', 'Ressourceneffizienz'),
    ('Recycling', 'Nachhaltigkeit'),
    ('Recycling', 'Abfallmanagement'),
    ('Robotik', 'Industrieroboter'),
    ('Robotik', 'Cobots'),
    ('Robotik', 'Sensorik'),
    ('Rundfunkwirtschaft', 'Broadcasting'),
    ('Rundfunkwirtschaft', 'Medienproduktion'),
    ('Rundfunkwirtschaft', 'Content-Distribution'),
    ('Rundfunkwirtschaft', 'Streaming-Technologie'),
    ('Rundfunkwirtschaft', 'Medientechnologie'),
    ('Sicherheits- und Verteidigungsindustrie', 'Rüstungsgüter'),
    ('Solarenergie', 'Photovoltaik'),
    ('Solarenergie', 'Solarthermie'),
    ('Solarenergie', 'PVT'),
    ('Soziale Unternehmen', 'Gemeinwohl'),
    ('Soziale Unternehmen', 'Soziale Innovation'),
    ('Soziale Unternehmen', 'Impact'),
    ('Soziale Unternehmen', 'Inklusion'),
    ('Soziale Unternehmen', 'Verantwortungseigentum'),
    ('Soziale Unternehmen', 'Kollektiv'),
    ('Stromnetz', 'Lastsenke'),
    ('Stromnetz', 'Steuerung'),
    ('Stromnetz', 'Smart-Grid'),
    ('Stromnetz', 'Resilienz'),
    ('Telekommunikationsanbieter', 'Glasfaser'),
    ('Telekommunikationsanbieter', '5G'),
    ('Telekommunikationsanbieter', '6G'),
    ('Telekommunikationsanbieter', 'Campusnetze'),
    ('Telekommunikationsanbieter', 'Richtfunk'),
    ('Telekommunikationsanbieter', 'LoRaWAN'),
    ('Telekommunikationsanbieter', 'Mobilfunk'),
    ('Telekommunikationsanbieter', 'Telekommunikation'),
    ('Telekommunikationsanbieter', 'Open RAN'),
    ('Tourismus', 'Destination'),
    ('Tourismus', 'Pauschalreise'),
    ('Tourismus', 'Fremdenverkehr'),
    ('Tourismus', 'Reiseveranstalter'),
    ('Tourismus', 'Urlaubserlebnis'),
    ('Tourismus', 'Incomingagentur'),
    ('Tourismus', 'Stadtführung'),
    ('Tourismus', 'Sightseeing'),
    ('Wasserstoff', 'Elektrolyseur'),
    ('Wassertechnologien', 'Abwasserbehandlung'),
    ('Wassertechnologien', 'Trinkwasseraufbereitung'),
    ('Wassertechnologien', 'Membrantechnologie'),
    ('Wassertechnologien', 'Prozesswasser'),
    ('Wassertechnologien', 'Wasserwiederverwendung'),
    ('Web3, Blockchain, DLT', 'Tokenisierung'),
    ('Web3, Blockchain, DLT', 'Dezentralisierung'),
    ('Web3, Blockchain, DLT', 'Smart Contracts'),
    ('Web3, Blockchain, DLT', 'Digitale Assets'),
    ('Web3, Blockchain, DLT', 'Hash'),
    ('Web3, Blockchain, DLT', 'Onchain Asset Management'),
    ('Werbemarkt', 'Programmatic Advertising'),
    ('Werbemarkt', 'Performance Marketing'),
    ('Werbemarkt', 'Content-Kreation'),
    ('Werbemarkt', 'Branding'),
    ('Windenergie', 'Windenergieanlagen'),
    ('Windenergie', 'Steuerung'),
    ('Windenergie', 'Turbinen'),
    ('Windenergie', 'Rotorblätter End-of-life');

-- Deleted by hand from the YAML, each with the measurement that justified it. Never add one back
-- without a new measurement. Handwerk's fifth keyword, a sentence about apprenticeships, was not kept
-- in the YAML's comment and so is not here either.
INSERT INTO public.technology_field_keywords (field, keyword, removed_at, removed_reason) VALUES
    ('Automobilwirtschaft', 'Transformation', DATE '2026-08-24', '"Transformation" removed 2026-08-24. It fired on 53 texts and decided 6 companies for this branch, every one a consultancy with the word in its OWN NAME — zeitgeist Transformations, TransformIT Europe, Transferagentur Niedersachsen, "Transformation in Healthcare Services". None sells cars. Worth knowing generally: the page text we read includes the company''s own name in its header, so a generic word in a company name assigns that company to itself.'),
    ('Bahnindustrie', 'Instandhaltung', DATE '2026-08-24', '"Instandhaltung" removed 2026-08-24. Measured: it fired on 60 of 7,739 texts and produced 26 of this branch''s 29 assignments on its own. Maintenance is what every workshop, facility manager, machine shop and building service does; it is a chore, not an industry, and none of the 26 was a railway company. Closing the hyphen hole does not help here — the word stands alone on those pages, so the count was 26 with the rule on as well. The branch keeps 28 keywords that do name the railway industry, down to "European Train Control System".'),
    ('Bioproduktion', 'Skalierung', DATE '2026-08-24', '"Skalierung" removed 2026-08-24. Scaling up is what every start-up says it is doing. It decided 5 of this branch''s 8 assignments: a talent-recruitment firm (twice), a solar UG and a "Global Solutions" consultancy. Only one, MycelCore, was plausibly biological. The branch keeps Fermentation, Bioreaktoren, Bioprozesstechnik and Zellkultur — words with no second meaning in German.'),
    ('Defence Tech', 'Sicherheit', DATE '2026-08-20', '"Sicherheit" removed 2026-08-20. Measured: it fired on 140 of 7,739 texts and produced 27 of this branch''s 35 assignments on its own — a bed shop, a lift company, an estate agent, a transport firm and a home-help service among them. On a German business page the word means Arbeitssicherheit or Datensicherheit, never defence. This is the one thing no measurement can decide: "sicherheit" appears on 1.8% of our texts and "einzelhandel" on 3.0%, and only one of those names an industry. The branch keeps 14 keywords that do name one.'),
    ('Digital Health', 'App', DATE '2026-08-24', '"App" removed 2026-08-24. Measured: it fired on 90 of 7,739 texts — the 6th most common keyword we have — and produced 11 of this branch''s 14 assignments on its own. An app is how a business reaches its customers, not the field it works in, and every trade has one. It is also the keyword the hyphen rule hurt most: 171 of its 789 page hits came only from "Web-App" or "App-Entwicklung". The branch keeps DiGA, ePA, Telemedizin and KI-Diagnostik, which no company outside digital health writes.'),
    ('Drohnen', 'Inspektion', DATE '2026-08-24', '"Inspektion" removed 2026-08-24. In Germany an Inspektion is the service interval on your car. It decided 5 companies for this branch: two car dealerships, a Pkw/Lkw repair shop, a testing service and a fittings maker. Drones do inspect things, but "Luftbildvermessung", "Drohnentechnologie" and "unbemannte Luftfahrt" say so.'),
    ('Ernährungsindustrie', 'Qualitätsmanagement', DATE '2026-08-24', '"Qualitätsmanagement" (35 texts) and "Produktentwicklung" removed 2026-08-24. Every manufacturer of anything does both. Together they decided 7 companies for this branch: a shoe company, a skin-testing laboratory, a concept agency, a components firm. "Lebensmittelsicherheit" and "Rückverfolgbarkeit" stay — those are food words.'),
    ('Ernährungsindustrie', 'Produktentwicklung', DATE '2026-08-24', '"Qualitätsmanagement" (35 texts) and "Produktentwicklung" removed 2026-08-24. Every manufacturer of anything does both. Together they decided 7 companies for this branch: a shoe company, a skin-testing laboratory, a concept agency, a components firm. "Lebensmittelsicherheit" and "Rückverfolgbarkeit" stay — those are food words.'),
    ('Gastgewerbe', 'Bar', DATE '2026-08-24', '"Bar" removed 2026-08-24. In German "bar" is how you pay — "bar bezahlen", "bar oder mit Karte" — and it fired on 41 texts, deciding 9 companies for this branch alone: an insurance broker, a literature society, a media agency, a foundation. Three letters cannot carry a branch. "Hotellerie", "Gastronomie", "Beherbergung", "Bewirtung", "Restaurant" and "Café" all stay, and a real bar''s website says at least one of them.'),
    ('Handwerk', 'Digitalisierung', DATE '2026-08-24', 'EMPTIED 2026-08-24, and this one is not a bad keyword — it is a branch with no usable keyword at all. Berlin''s sheet gave Handwerk five: "Digitalisierung", "Nachhaltigkeit", "Energieeffizienz", "Fachkräfte" and a sentence about apprenticeships. Not one of them names a craft. They are the topics a chamber of crafts talks ABOUT, and every company in Germany writes them.  Measured: all 26 assignments came from those words — digitalisierung 12, fachkräfte 9, energieeffizienz 8 — and the companies were an energy co-operative, a dental laboratory (twice), a document-management firm, a foundation, a metering company and an e-learning start-up. Zero craft businesses out of 26.  So the branch is left with nothing rather than filled with noise. A roofer, a joiner or an electrician is now correctly NONE: we would need words like Dachdecker, Tischlerei, Elektroinstallation, Sanitär, Malerbetrieb, and Berlin''s list does not have them. That is a gap in the taxonomy for whoever owns it — the same kind as the missing general-software field — and it is not something the classifier can fix.  "Nachhaltigkeit" survives elsewhere: Automobilwirtschaft, Ernährungsindustrie and Recycling also claim it, and removing Handwerk''s claim makes it worth more to each of them.'),
    ('Handwerk', 'Nachhaltigkeit', DATE '2026-08-24', 'EMPTIED 2026-08-24, and this one is not a bad keyword — it is a branch with no usable keyword at all. Berlin''s sheet gave Handwerk five: "Digitalisierung", "Nachhaltigkeit", "Energieeffizienz", "Fachkräfte" and a sentence about apprenticeships. Not one of them names a craft. They are the topics a chamber of crafts talks ABOUT, and every company in Germany writes them.  Measured: all 26 assignments came from those words — digitalisierung 12, fachkräfte 9, energieeffizienz 8 — and the companies were an energy co-operative, a dental laboratory (twice), a document-management firm, a foundation, a metering company and an e-learning start-up. Zero craft businesses out of 26.  So the branch is left with nothing rather than filled with noise. A roofer, a joiner or an electrician is now correctly NONE: we would need words like Dachdecker, Tischlerei, Elektroinstallation, Sanitär, Malerbetrieb, and Berlin''s list does not have them. That is a gap in the taxonomy for whoever owns it — the same kind as the missing general-software field — and it is not something the classifier can fix.  "Nachhaltigkeit" survives elsewhere: Automobilwirtschaft, Ernährungsindustrie and Recycling also claim it, and removing Handwerk''s claim makes it worth more to each of them.'),
    ('Handwerk', 'Energieeffizienz', DATE '2026-08-24', 'EMPTIED 2026-08-24, and this one is not a bad keyword — it is a branch with no usable keyword at all. Berlin''s sheet gave Handwerk five: "Digitalisierung", "Nachhaltigkeit", "Energieeffizienz", "Fachkräfte" and a sentence about apprenticeships. Not one of them names a craft. They are the topics a chamber of crafts talks ABOUT, and every company in Germany writes them.  Measured: all 26 assignments came from those words — digitalisierung 12, fachkräfte 9, energieeffizienz 8 — and the companies were an energy co-operative, a dental laboratory (twice), a document-management firm, a foundation, a metering company and an e-learning start-up. Zero craft businesses out of 26.  So the branch is left with nothing rather than filled with noise. A roofer, a joiner or an electrician is now correctly NONE: we would need words like Dachdecker, Tischlerei, Elektroinstallation, Sanitär, Malerbetrieb, and Berlin''s list does not have them. That is a gap in the taxonomy for whoever owns it — the same kind as the missing general-software field — and it is not something the classifier can fix.  "Nachhaltigkeit" survives elsewhere: Automobilwirtschaft, Ernährungsindustrie and Recycling also claim it, and removing Handwerk''s claim makes it worth more to each of them.'),
    ('Handwerk', 'Fachkräfte', DATE '2026-08-24', 'EMPTIED 2026-08-24, and this one is not a bad keyword — it is a branch with no usable keyword at all. Berlin''s sheet gave Handwerk five: "Digitalisierung", "Nachhaltigkeit", "Energieeffizienz", "Fachkräfte" and a sentence about apprenticeships. Not one of them names a craft. They are the topics a chamber of crafts talks ABOUT, and every company in Germany writes them.  Measured: all 26 assignments came from those words — digitalisierung 12, fachkräfte 9, energieeffizienz 8 — and the companies were an energy co-operative, a dental laboratory (twice), a document-management firm, a foundation, a metering company and an e-learning start-up. Zero craft businesses out of 26.  So the branch is left with nothing rather than filled with noise. A roofer, a joiner or an electrician is now correctly NONE: we would need words like Dachdecker, Tischlerei, Elektroinstallation, Sanitär, Malerbetrieb, and Berlin''s list does not have them. That is a gap in the taxonomy for whoever owns it — the same kind as the missing general-software field — and it is not something the classifier can fix.  "Nachhaltigkeit" survives elsewhere: Automobilwirtschaft, Ernährungsindustrie and Recycling also claim it, and removing Handwerk''s claim makes it worth more to each of them.'),
    ('Kunstmarkt', 'Galerie', DATE '2026-08-24', '"Galerie" removed 2026-08-24. Measured: 9 of this branch''s 11 assignments rested on it alone. On a German business site "Galerie" is the photo-gallery item in the navigation — a roofer''s finished jobs, a restaurant''s dishes — and 24 of the 67 pages carrying the word only had it as part of "Foto-Galerie" or "Bild-Galerie". The branch keeps Kunsthandel, Kunstbewertung and Sammlungsmanagement, all three of which name the art trade itself.'),
    ('Maschinen- und Anlagenbau', 'Prozessoptimierung', DATE '2026-08-24', '"Prozessoptimierung" removed 2026-08-24. It fired on 38 texts and decided 6 companies here, all management consultancies — ECOVIS Unternehmensberatung, DO! Business Consulting, Praxis Erfolgskonzept. Optimising a process is advice, not machinery. "Industrieanlagen" is the counter-example and stays: its 8 companies are Weil Industrieanlagen, IAB Industrie-Anlagen-Bau, Balsmann Anlagenbau. It names the thing.'),
    ('Messe- und Kongresswirtschaft', 'Eventmanagement', DATE '2026-08-24', '"Eventmanagement" removed 2026-08-24. It decided 4 of this branch''s 5 assignments: an IT company, a childcare service, a data company and a sausage stall. Every business that has ever run a summer party writes it. "Standbau" and "Kongressorganisation" stay — those are the trade itself.'),
    ('Modewirtschaft', 'Fashion', DATE '2026-08-24', '"Fashion" removed 2026-08-24. An English word German businesses use as decoration — "Hair Fashion", "Fashion Store" — it decided 4 of this branch''s 6 assignments: a photo studio, a workwear and uniforms maker, a holding company. "Modedesign" and "Kollektion" stay, and a fashion label writes at least one of them.'),
    ('Musikwirtschaft', 'Lizenzierung', DATE '2026-08-24', '"Lizenzierung" and "Labels" removed 2026-08-24 — 12 of this branch''s 20 assignments. Software is licensed too, so `lizenzierung` decided a travel app, a law firm and two IT companies. `labels` in German business text is what you stick on a product: it decided a distributor and a retail group three times over. Exactly one of the twelve was a music business. "Musikproduktion", "Musikvertrieb", "Musikverlag" and "Konzertveranstalter" stay, and they cannot mean anything else.'),
    ('Musikwirtschaft', 'Labels', DATE '2026-08-24', '"Lizenzierung" and "Labels" removed 2026-08-24 — 12 of this branch''s 20 assignments. Software is licensed too, so `lizenzierung` decided a travel app, a law firm and two IT companies. `labels` in German business text is what you stick on a product: it decided a distributor and a retail group three times over. Exactly one of the twelve was a music business. "Musikproduktion", "Musikvertrieb", "Musikverlag" and "Konzertveranstalter" stay, and they cannot mean anything else.'),
    ('Pharma & Wirkstoffe', 'Zulassung', DATE '2026-08-24', '"Zulassung" removed 2026-08-24. In pharma it means getting a drug approved; in ordinary German it is registering a car, admitting a student or certifying a machine. Measured with the hyphen rule on, it carried 6 of this branch''s 10 assignments alone, and the pages saying it write "Kfz-Zulassung". The branch keeps Arzneimittel, Wirkstoff, klinische Studien, Galenik, Generika, Orphan Drugs and Pharmazeutika — seven words nobody else uses.'),
    ('Robotik', 'Automatisierung', DATE '2026-08-24', '"Automatisierung" (50 texts) and "Systemintegration" removed 2026-08-24 — all 7 of this branch''s assignments came from them, and not one company had a robot: four software firms, an IT consultancy and a Catholic youth charity. Automating a process and integrating a system are what every software company sells. "Industrieroboter" and "Cobots" stay, and a robotics company writes one of them.'),
    ('Robotik', 'Systemintegration', DATE '2026-08-24', '"Automatisierung" (50 texts) and "Systemintegration" removed 2026-08-24 — all 7 of this branch''s assignments came from them, and not one company had a robot: four software firms, an IT consultancy and a Catholic youth charity. Automating a process and integrating a system are what every software company sells. "Industrieroboter" and "Cobots" stay, and a robotics company writes one of them.'),
    ('Soziale Unternehmen', 'Wirkung', DATE '2026-08-24', '"Wirkung" removed 2026-08-24. It fired on 90 of 7,739 texts, level with "app". Berlin''s sheet plainly meant it as the German for "impact", but on a business page it is the ordinary word for "effect" — the effect of a cream, a fertiliser, an advert. Same class as the "Sicherheit" deletion under Defence Tech: it names a quality, not an industry.'),
    ('Soziale Unternehmen', 'Fair', DATE '2026-08-24', '"Fair" removed 2026-08-24. It fired on 44 texts, and in German it is an adjective every business applies to itself — "faire Preise", "ein fairer Partner". In English it is also a trade fair, which is a different branch entirely. "Impact" and "Gemeinwohl" stay: those are terms of art, and a company writing them has chosen to.'),
    ('Wasserstoff', 'Netz', DATE '2026-08-24', '"Netz" and "Speicher" removed 2026-08-24, and they were 19 of this branch''s 22 assignments. "Im Netz finden Sie uns" is how a German company says "we are online", so `netz` decided: Terre des Hommes (a children''s charity), a car-parts dealer, a vehicle-valuation firm, an office-supplies wholesaler, a theatre-education workshop. `speicher` is any storage — data, water, a warehouse — and decided a crypto UG, a body-care UG and the same theatre workshop. Hydrogen storage is a real thing, and these words are not how a company says it. "Elektrolyseur" stays: nobody writes it by accident, and it is the one word here that only this industry uses.'),
    ('Wasserstoff', 'Speicher', DATE '2026-08-24', '"Netz" and "Speicher" removed 2026-08-24, and they were 19 of this branch''s 22 assignments. "Im Netz finden Sie uns" is how a German company says "we are online", so `netz` decided: Terre des Hommes (a children''s charity), a car-parts dealer, a vehicle-valuation firm, an office-supplies wholesaler, a theatre-education workshop. `speicher` is any storage — data, water, a warehouse — and decided a crypto UG, a body-care UG and the same theatre workshop. Hydrogen storage is a real thing, and these words are not how a company says it. "Elektrolyseur" stays: nobody writes it by accident, and it is the one word here that only this industry uses.'),
    ('Werbemarkt', 'Social Media', DATE '2026-08-24', '"Social Media" removed 2026-08-24. It fired on 132 of 7,739 texts — the second most common keyword we have — and decided 24 companies for this branch on its own. Read those 24: a sports club, a firm of solicitors, a Caritas care service, a circus association, a landscape gardener, a furniture shop. Exactly one was an advertising business. Every German company links its Instagram in the footer, so the words say nothing about what the company sells. This costs the genuine agencies too — it also supported 29 assignments in other branches — and it is still worth it at 1 right in 12. The branch keeps four words only the trade uses.');

INSERT INTO public.wz_divisions (division, title, section, section_title) VALUES
    ('01', 'Landwirtschaft, Jagd und damit verbundene Tätigkeiten', 'A', 'LAND- UND FORSTWIRTSCHAFT, FISCHEREI'),
    ('02', 'Forstwirtschaft und Holzeinschlag', 'A', 'LAND- UND FORSTWIRTSCHAFT, FISCHEREI'),
    ('03', 'Fischerei und Aquakultur', 'A', 'LAND- UND FORSTWIRTSCHAFT, FISCHEREI'),
    ('05', 'Kohlenbergbau', 'B', 'BERGBAU UND GEWINNUNG VON STEINEN UND ERDEN'),
    ('06', 'Gewinnung von Erdöl und Erdgas', 'B', 'BERGBAU UND GEWINNUNG VON STEINEN UND ERDEN'),
    ('07', 'Erzbergbau', 'B', 'BERGBAU UND GEWINNUNG VON STEINEN UND ERDEN'),
    ('08', 'Gewinnung von Steinen und Erden, sonstiger Bergbau', 'B', 'BERGBAU UND GEWINNUNG VON STEINEN UND ERDEN'),
    ('09', 'Erbringung von Dienstleistungen für den Bergbau und für die Gewinnung von Steinen und Erden', 'B', 'BERGBAU UND GEWINNUNG VON STEINEN UND ERDEN'),
    ('10', 'Herstellung von Nahrungs- und Futtermitteln', 'C', 'VERARBEITENDES GEWERBE'),
    ('11', 'Getränkeherstellung', 'C', 'VERARBEITENDES GEWERBE'),
    ('12', 'Tabakverarbeitung', 'C', 'VERARBEITENDES GEWERBE'),
    ('13', 'Herstellung von Textilien', 'C', 'VERARBEITENDES GEWERBE'),
    ('14', 'Herstellung von Bekleidung', 'C', 'VERARBEITENDES GEWERBE'),
    ('15', 'Herstellung von Leder, Lederwaren und ähnlichen Produkten aus anderen Materialen', 'C', 'VERARBEITENDES GEWERBE'),
    ('16', 'Herstellung von Holz-, Flecht-, Korb- und Korkwaren, ohne Möbel', 'C', 'VERARBEITENDES GEWERBE'),
    ('17', 'Herstellung von Papier, Pappe und Waren daraus', 'C', 'VERARBEITENDES GEWERBE'),
    ('18', 'Herstellung von Druckerzeugnissen; Vervielfältigung von bespielten Ton-, Bild- und Datenträgern', 'C', 'VERARBEITENDES GEWERBE'),
    ('19', 'Kokerei und Mineralölverarbeitung', 'C', 'VERARBEITENDES GEWERBE'),
    ('20', 'Herstellung von chemischen Erzeugnissen', 'C', 'VERARBEITENDES GEWERBE'),
    ('21', 'Herstellung von pharmazeutischen Erzeugnissen', 'C', 'VERARBEITENDES GEWERBE'),
    ('22', 'Herstellung von Gummi- und Kunststoffwaren', 'C', 'VERARBEITENDES GEWERBE'),
    ('23', 'Herstellung von Glas und Glaswaren, Keramik, Verarbeitung von Steinen und Erden', 'C', 'VERARBEITENDES GEWERBE'),
    ('24', 'Metallerzeugung und -bearbeitung', 'C', 'VERARBEITENDES GEWERBE'),
    ('25', 'Herstellung von Metallerzeugnissen', 'C', 'VERARBEITENDES GEWERBE'),
    ('26', 'Herstellung von Datenverarbeitungsgeräten, elektronischen und optischen Erzeugnissen', 'C', 'VERARBEITENDES GEWERBE'),
    ('27', 'Herstellung von elektrischen Ausrüstungen', 'C', 'VERARBEITENDES GEWERBE'),
    ('28', 'Maschinenbau', 'C', 'VERARBEITENDES GEWERBE'),
    ('29', 'Herstellung von Kraftwagen und Kraftwagenteilen', 'C', 'VERARBEITENDES GEWERBE'),
    ('30', 'Sonstiger Fahrzeugbau', 'C', 'VERARBEITENDES GEWERBE'),
    ('31', 'Herstellung von Möbeln', 'C', 'VERARBEITENDES GEWERBE'),
    ('32', 'Herstellung von sonstigen Waren', 'C', 'VERARBEITENDES GEWERBE'),
    ('33', 'Reparatur, Instandhaltung und Installation von Maschinen und Ausrüstungen', 'C', 'VERARBEITENDES GEWERBE'),
    ('35', 'Energieversorgung', 'D', 'ENERGIEVERSORGUNG'),
    ('36', 'Wasserversorgung', 'E', 'WASSERVERSORGUNG; ABWASSER- UND ABFALLENTSORGUNG UND BESEITIGUNG VON UMWELTVERSCHMUTZUNGEN'),
    ('37', 'Abwasserentsorgung', 'E', 'WASSERVERSORGUNG; ABWASSER- UND ABFALLENTSORGUNG UND BESEITIGUNG VON UMWELTVERSCHMUTZUNGEN'),
    ('38', 'Sammlung, Verwertung und Beseitigung von Abfällen', 'E', 'WASSERVERSORGUNG; ABWASSER- UND ABFALLENTSORGUNG UND BESEITIGUNG VON UMWELTVERSCHMUTZUNGEN'),
    ('39', 'Beseitigung von Umweltverschmutzungen und sonstige Tätigkeiten in der Abfallbewirtschaftung', 'E', 'WASSERVERSORGUNG; ABWASSER- UND ABFALLENTSORGUNG UND BESEITIGUNG VON UMWELTVERSCHMUTZUNGEN'),
    ('41', 'Hochbau', 'F', 'BAUGEWERBE'),
    ('42', 'Tiefbau', 'F', 'BAUGEWERBE'),
    ('43', 'Vorbereitende Baustellenarbeiten, Bauinstallation und sonstiges Ausbaugewerbe', 'F', 'BAUGEWERBE'),
    ('46', 'Großhandel', 'G', 'HANDEL'),
    ('47', 'Einzelhandel', 'G', 'HANDEL'),
    ('49', 'Landverkehr und Transport in Rohrfernleitungen', 'H', 'VERKEHR UND LAGEREI'),
    ('50', 'Schifffahrt', 'H', 'VERKEHR UND LAGEREI'),
    ('51', 'Luftfahrt', 'H', 'VERKEHR UND LAGEREI'),
    ('52', 'Lagerei sowie Erbringung von sonstigen Dienstleistungen für den Verkehr', 'H', 'VERKEHR UND LAGEREI'),
    ('53', 'Post-, Kurier- und Expressdienste', 'H', 'VERKEHR UND LAGEREI'),
    ('55', 'Beherbergung', 'I', 'GASTGEWERBE'),
    ('56', 'Gastronomie', 'I', 'GASTGEWERBE'),
    ('58', 'Verlagswesen', 'J', 'VERLAGSWESEN, RUNDFUNK SOWIE ERSTELLUNG UND VERBREITUNG VON MEDIENINHALTEN'),
    ('59', 'Herstellung, Verleih und Vertrieb von Filmen und Fernsehprogrammen; Kinos; Tonstudios und Verlegen von Musik', 'J', 'VERLAGSWESEN, RUNDFUNK SOWIE ERSTELLUNG UND VERBREITUNG VON MEDIENINHALTEN'),
    ('60', 'Rundfunkveranstalter, Nachrichtenagenturen und sonstige Verbreitung von Medieninhalten', 'J', 'VERLAGSWESEN, RUNDFUNK SOWIE ERSTELLUNG UND VERBREITUNG VON MEDIENINHALTEN'),
    ('61', 'Telekommunikation', 'K', 'TELEKOMMUNIKATION, SOFTWAREENTWICKLUNG, IT-BERATUNG UND ERBRINGUNG SONSTIGER DIENSTLEISTUNGEN DER INFORMATIONSTECHNOLOGIE UND DER COMPUTERINFRASTRUKTUR'),
    ('62', 'Erbringung von Dienstleistungen der Informationstechnologie', 'K', 'TELEKOMMUNIKATION, SOFTWAREENTWICKLUNG, IT-BERATUNG UND ERBRINGUNG SONSTIGER DIENSTLEISTUNGEN DER INFORMATIONSTECHNOLOGIE UND DER COMPUTERINFRASTRUKTUR'),
    ('63', 'Datenverarbeitung, Hosting und Erbringung sonstiger Informationsdienstleistungen', 'K', 'TELEKOMMUNIKATION, SOFTWAREENTWICKLUNG, IT-BERATUNG UND ERBRINGUNG SONSTIGER DIENSTLEISTUNGEN DER INFORMATIONSTECHNOLOGIE UND DER COMPUTERINFRASTRUKTUR'),
    ('64', 'Erbringung von Finanzdienstleistungen', 'L', 'ERBRINGUNG VON FINANZ- UND VERSICHERUNGSDIENSTLEISTUNGEN'),
    ('65', 'Versicherungen, Rückversicherungen und Pensionskassen, ohne Sozialversicherung', 'L', 'ERBRINGUNG VON FINANZ- UND VERSICHERUNGSDIENSTLEISTUNGEN'),
    ('66', 'Mit Finanz- und Versicherungsdienstleistungen verbundene Tätigkeiten', 'L', 'ERBRINGUNG VON FINANZ- UND VERSICHERUNGSDIENSTLEISTUNGEN'),
    ('68', 'Grundstücks- und Wohnungswesen', 'M', 'GRUNDSTÜCKS- UND WOHNUNGSWESEN'),
    ('69', 'Rechts- und Steuerberatung, Wirtschaftsprüfung', 'N', 'ERBRINGUNG VON WISSENSCHAFTLICHEN UND TECHNISCHEN DIENSTLEISTUNGEN'),
    ('70', 'Verwaltung und Führung von Unternehmen und Betrieben; Unternehmensberatung', 'N', 'ERBRINGUNG VON WISSENSCHAFTLICHEN UND TECHNISCHEN DIENSTLEISTUNGEN'),
    ('71', 'Tätigkeiten von Architektur- und Ingenieurbüros; technische, physikalische und chemische Untersuchung', 'N', 'ERBRINGUNG VON WISSENSCHAFTLICHEN UND TECHNISCHEN DIENSTLEISTUNGEN'),
    ('72', 'Forschung und Entwicklung', 'N', 'ERBRINGUNG VON WISSENSCHAFTLICHEN UND TECHNISCHEN DIENSTLEISTUNGEN'),
    ('73', 'Werbung und Marktforschung sowie Public-Relations-Beratung', 'N', 'ERBRINGUNG VON WISSENSCHAFTLICHEN UND TECHNISCHEN DIENSTLEISTUNGEN'),
    ('74', 'Sonstige wissenschaftliche und technische Tätigkeiten', 'N', 'ERBRINGUNG VON WISSENSCHAFTLICHEN UND TECHNISCHEN DIENSTLEISTUNGEN'),
    ('75', 'Veterinärwesen', 'N', 'ERBRINGUNG VON WISSENSCHAFTLICHEN UND TECHNISCHEN DIENSTLEISTUNGEN'),
    ('77', 'Vermietung und Leasing', 'O', 'ERBRINGUNG VON SONSTIGEN WIRTSCHAFTLICHEN DIENSTLEISTUNGEN'),
    ('78', 'Vermittlung und Überlassung von Arbeitskräften', 'O', 'ERBRINGUNG VON SONSTIGEN WIRTSCHAFTLICHEN DIENSTLEISTUNGEN'),
    ('79', 'Reisebüros, Reiseveranstalter und Erbringung sonstiger Reservierungsdienstleistungen', 'O', 'ERBRINGUNG VON SONSTIGEN WIRTSCHAFTLICHEN DIENSTLEISTUNGEN'),
    ('80', 'Wach- und Sicherheitsdienste sowie Detekteien', 'O', 'ERBRINGUNG VON SONSTIGEN WIRTSCHAFTLICHEN DIENSTLEISTUNGEN'),
    ('81', 'Gebäudebetreuung; Garten- und Landschaftsbau', 'O', 'ERBRINGUNG VON SONSTIGEN WIRTSCHAFTLICHEN DIENSTLEISTUNGEN'),
    ('82', 'Erbringung von wirtschaftlichen Dienstleistungen für Unternehmen und Privatpersonen', 'O', 'ERBRINGUNG VON SONSTIGEN WIRTSCHAFTLICHEN DIENSTLEISTUNGEN'),
    ('84', 'Öffentliche Verwaltung, Verteidigung; Sozialversicherung', 'P', 'ÖFFENTLICHE VERWALTUNG, VERTEIDIGUNG; SOZIALVERSICHERUNG'),
    ('85', 'Erziehung und Unterricht', 'Q', 'ERZIEHUNG UND UNTERRICHT'),
    ('86', 'Gesundheitswesen', 'R', 'GESUNDHEITS- UND SOZIALWESEN'),
    ('87', 'Heime, ohne Erholungs- und Ferienheime', 'R', 'GESUNDHEITS- UND SOZIALWESEN'),
    ('88', 'Sozialwesen, ohne Heime', 'R', 'GESUNDHEITS- UND SOZIALWESEN'),
    ('90', 'Kunstschaffende Tätigkeiten und Tätigkeiten in der darstellenden Kunst', 'S', 'KUNST, SPORT UND ERHOLUNG'),
    ('91', 'Bibliotheken, Archive, Museen, botanische und zoologische Gärten', 'S', 'KUNST, SPORT UND ERHOLUNG'),
    ('92', 'Spiel-, Wett- und Lotteriewesen', 'S', 'KUNST, SPORT UND ERHOLUNG'),
    ('93', 'Erbringung von Dienstleistungen des Sports, der Unterhaltung und der Erholung', 'S', 'KUNST, SPORT UND ERHOLUNG'),
    ('94', 'Interessenvertretungen sowie kirchliche und sonstige religiöse Vereinigungen, ohne Sozialwesen und Sport', 'T', 'ERBRINGUNG VON SONSTIGEN DIENSTLEISTUNGEN'),
    ('95', 'Reparatur und Instandhaltung von Datenverarbeitungsgeräten und Gebrauchsgütern sowie von Kraftwagen und Krafträdern', 'T', 'ERBRINGUNG VON SONSTIGEN DIENSTLEISTUNGEN'),
    ('96', 'Erbringung von überwiegend persönlichen Dienstleistungen', 'T', 'ERBRINGUNG VON SONSTIGEN DIENSTLEISTUNGEN'),
    ('97', 'Private Haushalte mit Hauspersonal', 'U', 'PRIVATE HAUSHALTE MIT HAUSPERSONAL SOWIE HERSTELLUNG VON WAREN UND ERBRINGUNG VON DIENSTLEISTUNGEN DURCH PRIVATE HAUSHALTE FÜR DEN EIGENBEDARF OHNE AUSGEPRÄGTEN SCHWERPUNKT'),
    ('98', 'Herstellung von Waren und Erbringung von Dienstleistungen durch private Haushalte für den Eigenbedarf ohne ausgeprägten Schwerpunkt', 'U', 'PRIVATE HAUSHALTE MIT HAUSPERSONAL SOWIE HERSTELLUNG VON WAREN UND ERBRINGUNG VON DIENSTLEISTUNGEN DURCH PRIVATE HAUSHALTE FÜR DEN EIGENBEDARF OHNE AUSGEPRÄGTEN SCHWERPUNKT'),
    ('99', 'Exterritoriale Organisationen und Körperschaften', 'V', 'EXTERRITORIALE ORGANISATIONEN UND KÖRPERSCHAFTEN');


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA public TO ${writer_role};
GRANT USAGE ON SCHEMA public TO ${reader_role};


--
-- Name: FUNCTION normalize_company_name(input_name text); Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON FUNCTION public.normalize_company_name(input_name text) TO ${writer_role};
GRANT ALL ON FUNCTION public.normalize_company_name(input_name text) TO ${reader_role};


--
-- Name: FUNCTION refresh_company_funding_links(); Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON FUNCTION public.refresh_company_funding_links() TO ${writer_role};


--
-- Name: FUNCTION refresh_company_profile(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.refresh_company_profile() FROM PUBLIC;
GRANT ALL ON FUNCTION public.refresh_company_profile() TO ${writer_role};


--
-- Name: FUNCTION touch_updated_at(); Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON FUNCTION public.touch_updated_at() TO ${writer_role};
GRANT ALL ON FUNCTION public.touch_updated_at() TO ${reader_role};


--
-- Name: TABLE companies; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.companies TO ${writer_role};
GRANT SELECT ON TABLE public.companies TO ${reader_role};


--
-- Name: TABLE company_canonical_field_sources; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_canonical_field_sources TO ${writer_role};
GRANT SELECT ON TABLE public.company_canonical_field_sources TO ${reader_role};


--
-- Name: TABLE company_comments; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_comments TO ${writer_role};
GRANT SELECT,INSERT ON TABLE public.company_comments TO ${reader_role};


--
-- Name: TABLE company_documents; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_documents TO ${writer_role};
GRANT SELECT ON TABLE public.company_documents TO ${reader_role};


--
-- Name: TABLE company_field_history; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_field_history TO ${writer_role};
GRANT SELECT ON TABLE public.company_field_history TO ${reader_role};


--
-- Name: SEQUENCE company_field_history_id_seq; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,USAGE ON SEQUENCE public.company_field_history_id_seq TO ${writer_role};


--
-- Name: TABLE company_field_observations; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_field_observations TO ${writer_role};
GRANT SELECT ON TABLE public.company_field_observations TO ${reader_role};


--
-- Name: TABLE company_funding_links; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_funding_links TO ${writer_role};
GRANT SELECT ON TABLE public.company_funding_links TO ${reader_role};


--
-- Name: TABLE company_fundings; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_fundings TO ${writer_role};
GRANT SELECT ON TABLE public.company_fundings TO ${reader_role};


--
-- Name: TABLE company_identifiers; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_identifiers TO ${writer_role};
GRANT SELECT ON TABLE public.company_identifiers TO ${reader_role};


--
-- Name: TABLE company_managers; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_managers TO ${writer_role};
GRANT SELECT ON TABLE public.company_managers TO ${reader_role};


--
-- Name: TABLE company_source_records; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_source_records TO ${writer_role};
GRANT SELECT ON TABLE public.company_source_records TO ${reader_role};


--
-- Name: TABLE sources; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.sources TO ${writer_role};
GRANT SELECT ON TABLE public.sources TO ${reader_role};


--
-- Name: TABLE v_companies_enriched; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_companies_enriched TO ${writer_role};
GRANT SELECT ON TABLE public.v_companies_enriched TO ${reader_role};


--
-- Name: TABLE v_company_fundings; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_company_fundings TO ${writer_role};
GRANT SELECT ON TABLE public.v_company_fundings TO ${reader_role};


--
-- Name: TABLE v_company_profile_live; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_company_profile_live TO ${writer_role};
GRANT SELECT ON TABLE public.v_company_profile_live TO ${reader_role};


--
-- Name: TABLE v_company_sources; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_company_sources TO ${writer_role};
GRANT SELECT ON TABLE public.v_company_sources TO ${reader_role};


--
-- Name: TABLE company_profile_mv; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_profile_mv TO ${writer_role};
GRANT SELECT ON TABLE public.company_profile_mv TO ${reader_role};


--
-- Name: TABLE company_tags; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.company_tags TO ${writer_role};
GRANT SELECT,INSERT,DELETE ON TABLE public.company_tags TO ${reader_role};


--
-- Name: TABLE extraction_feedback; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.extraction_feedback TO ${writer_role};
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.extraction_feedback TO ${reader_role};


--
-- Name: TABLE match_candidates; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.match_candidates TO ${writer_role};
GRANT SELECT ON TABLE public.match_candidates TO ${reader_role};


--
-- Name: TABLE pipeline_state; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.pipeline_state TO ${writer_role};
GRANT SELECT ON TABLE public.pipeline_state TO ${reader_role};


--
-- Name: TABLE review_sample; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.review_sample TO ${writer_role};
GRANT SELECT ON TABLE public.review_sample TO ${reader_role};


--
-- Name: TABLE source_field_stats_mv; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.source_field_stats_mv TO ${writer_role};
GRANT SELECT ON TABLE public.source_field_stats_mv TO ${reader_role};


--
-- Name: TABLE source_stats_mv; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.source_stats_mv TO ${writer_role};
GRANT SELECT ON TABLE public.source_stats_mv TO ${reader_role};


--
-- Name: TABLE source_sync_runs; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.source_sync_runs TO ${writer_role};
GRANT SELECT ON TABLE public.source_sync_runs TO ${reader_role};


--
-- Name: TABLE tags; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.tags TO ${writer_role};
GRANT SELECT,INSERT ON TABLE public.tags TO ${reader_role};


--
-- Name: TABLE technology_field_keywords; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT ON TABLE public.technology_field_keywords TO ${writer_role};
GRANT SELECT ON TABLE public.technology_field_keywords TO ${reader_role};


--
-- Name: TABLE technology_fields; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT ON TABLE public.technology_fields TO ${writer_role};
GRANT SELECT ON TABLE public.technology_fields TO ${reader_role};


--
-- Name: TABLE v_company_claims; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_company_claims TO ${writer_role};
GRANT SELECT ON TABLE public.v_company_claims TO ${reader_role};


--
-- Name: TABLE v_company_history; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_company_history TO ${writer_role};
GRANT SELECT ON TABLE public.v_company_history TO ${reader_role};


--
-- Name: TABLE v_company_profile; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_company_profile TO ${writer_role};
GRANT SELECT ON TABLE public.v_company_profile TO ${reader_role};


--
-- Name: TABLE v_field_provenance; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_field_provenance TO ${writer_role};
GRANT SELECT ON TABLE public.v_field_provenance TO ${reader_role};


--
-- Name: TABLE v_review_results; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_review_results TO ${writer_role};
GRANT SELECT ON TABLE public.v_review_results TO ${reader_role};


--
-- Name: TABLE v_source_fields; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_source_fields TO ${writer_role};
GRANT SELECT ON TABLE public.v_source_fields TO ${reader_role};


--
-- Name: TABLE v_source_overview; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_source_overview TO ${writer_role};
GRANT SELECT ON TABLE public.v_source_overview TO ${reader_role};


--
-- Name: TABLE v_source_runs; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.v_source_runs TO ${writer_role};
GRANT SELECT ON TABLE public.v_source_runs TO ${reader_role};


--
-- Name: TABLE wz_divisions; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT ON TABLE public.wz_divisions TO ${writer_role};
GRANT SELECT ON TABLE public.wz_divisions TO ${reader_role};


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: -
--

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,USAGE ON SEQUENCES  TO ${writer_role};


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: -
--

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES  TO ${writer_role};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES  TO ${reader_role};


--
-- Name: company_profile_mv; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: -
--

REFRESH MATERIALIZED VIEW public.company_profile_mv;


--
-- Name: source_field_stats_mv; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: -
--

REFRESH MATERIALIZED VIEW public.source_field_stats_mv;


--
-- Name: source_stats_mv; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: -
--

REFRESH MATERIALIZED VIEW public.source_stats_mv;


--
-- PostgreSQL database dump complete
--


-- The DAGs' bulk updates use TEMP tables; a database-level grant, so pg_dump does not carry it.
DO $$
BEGIN
  EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO ${writer_role}', current_database());
END
$$;
