-- Baseline: the whole schema as of 2026-10-31, in one file.
--
-- Squashed four times: the 24 migrations of the Osnabrück era, then V1-V9 and V10 of the Berlin
-- rebuild (2026-10-28), then V1-V10 (2026-10-31, #68). They live on in git history; their reasoning
-- is in the comments of the objects they created. In short:
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
--
-- Generated from a database migrated to V10 with `pg_dump --schema-only --no-owner`, privileges
-- included, so every grant is exactly what V1-V10 left. Role names are Flyway placeholders
-- (`${writer_role}`, `${reader_role}`) so the hub can use its own, and default privileges apply to
-- whoever runs this file. The materialized views are created WITH DATA: refresh_company_profile()
-- refreshes them CONCURRENTLY, which an unpopulated view refuses. PostGIS and pgvector are created
-- by the database setup, not here. Checked: a database built from this file and one built from
-- V1-V10 dump to the same schema, grants and seed rows.

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
    CONSTRAINT company_identifiers_scheme_check CHECK ((scheme = ANY (ARRAY['register'::text, 'register_former'::text, 'opencorporates'::text, 'fusionbase'::text, 'vat'::text, 'lei'::text])))
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
    CONSTRAINT sources_default_sync_interval_minutes_check CHECK ((default_sync_interval_minutes > 0)),
    CONSTRAINT sources_priority_check CHECK ((priority >= 0))
);


--
-- Name: TABLE sources; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.sources IS 'Registry of upstream sources with sync scheduling metadata.';


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
    ((obs_announce.metadata ->> 'published_on'::text))::date AS register_announcement_date
   FROM (((((((((((public.v_companies_enriched e
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
     LEFT JOIN public.company_field_observations obs_announce ON ((obs_announce.id = cfs_announce.company_field_observation_id)));


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
    COALESCE(src.source_keys, '{}'::text[]) AS source_keys
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
    CONSTRAINT extraction_feedback_field_name_check CHECK ((field_name = ANY (ARRAY['name'::text, 'address'::text, 'legal_form'::text, 'register_status'::text, 'wz_code'::text, 'established_at'::text, 'employee_count'::text, 'revenue'::text, 'fundings'::text, 'technology_field'::text, 'website'::text, 'management'::text, 'business_purpose'::text, 'deeptech'::text, 'is_ai'::text, 'activity_keywords'::text, 'register_announcement'::text, 'registration_number'::text]))),
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
 SELECT s.key AS source_key,
    o.field_name,
    count(*) AS observations,
    count(DISTINCT o.company_id) AS companies,
    count(DISTINCT o.company_id) FILTER (WHERE (o.is_canonical OR (m.id IS NOT NULL))) AS companies_shown,
    round(avg(o.confidence), 3) AS avg_confidence,
    max(o.observed_at) AS last_observed_at
   FROM ((public.company_field_observations o
     JOIN public.sources s ON ((s.id = o.source_id)))
     LEFT JOIN public.company_managers m ON (((m.company_field_observation_id = o.id) AND m.is_current)))
  GROUP BY s.key, o.field_name
UNION ALL
 SELECT f.source AS source_key,
    'fundings'::text AS field_name,
    count(DISTINCT f.fkz) AS observations,
    count(DISTINCT l.company_id) AS companies,
    count(DISTINCT l.company_id) AS companies_shown,
    NULL::numeric AS avg_confidence,
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
            (r.match_method = 'created'::text) AS created
           FROM public.company_source_records r
        UNION ALL
         SELECT s_1.id,
            l.company_id,
            false,
            false
           FROM ((public.company_funding_links l
             JOIN public.company_fundings f ON ((f.fkz = l.fkz)))
             JOIN public.sources s_1 ON ((s_1.key = f.source)))
        ), per_source AS (
         SELECT covered.source_id,
            count(*) FILTER (WHERE covered.is_record) AS records,
            count(*) FILTER (WHERE covered.created) AS companies_created,
            count(DISTINCT covered.company_id) AS companies
           FROM covered
          GROUP BY covered.source_id
        )
 SELECT s.key AS source_key,
    COALESCE(p.records, (0)::bigint) AS records,
    COALESCE(p.companies_created, (0)::bigint) AS companies_created,
    COALESCE(p.companies, (0)::bigint) AS companies,
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
-- Name: v_company_claims; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_claims AS
 SELECT o.id,
    o.company_id,
        CASE o.field_name
            WHEN 'ceo_name_or_contact'::text THEN 'management'::text
            WHEN 'candidate_url'::text THEN 'website'::text
            WHEN 'deeptech_class'::text THEN 'deeptech'::text
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
  WHERE (o.field_name = ANY (ARRAY['name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text, 'website'::text, 'candidate_url'::text, 'technology_field'::text, 'purpose_field'::text, 'activity_keywords'::text, 'register_announcement'::text, 'ceo_name_or_contact'::text, 'employee_count'::text, 'revenue'::text, 'deeptech_class'::text, 'is_ai'::text]));


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
    company_profile_mv.source_keys
   FROM public.company_profile_mv;


--
-- Name: VIEW v_company_profile; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_profile IS 'The target record per company, as of the last refresh_company_profile(). The live definition is v_company_profile_live.';


--
-- Name: v_field_provenance; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_field_provenance AS
 SELECT cfs.company_id,
    cfs.field_name,
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
    source_field_stats_mv.avg_confidence,
    source_field_stats_mv.last_observed_at
   FROM public.source_field_stats_mv;


--
-- Name: VIEW v_source_fields; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_source_fields IS 'Per source and field: values stored, companies, and companies whose shown value came from this source. Counts only; no value is exposed.';


--
-- Name: v_source_overview; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_source_overview AS
 SELECT s.key AS source_key,
    s.name,
    s.source_type,
    s.base_url,
    COALESCE(st.records, (0)::bigint) AS records,
    COALESCE(st.companies, (0)::bigint) AS companies,
    COALESCE(st.companies_created, (0)::bigint) AS companies_created,
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

COMMENT ON VIEW public.v_source_overview IS 'One row per source for the app''s sources page: records, companies, companies created, and its latest run. Counts as of counts_as_of (refresh_company_profile()); the run is live.';


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

CREATE UNIQUE INDEX uq_obs_company_field_source_value ON public.company_field_observations USING btree (company_id, field_name, source_id, value_text) WHERE (field_name = ANY (ARRAY['website'::text, 'candidate_url'::text, 'directory_url'::text, 'discovery_tier'::text, 'osm_id'::text, 'technology_field'::text, 'purpose_field'::text, 'technology_field_withheld'::text, 'purpose_field_withheld'::text, 'name'::text, 'legal_form'::text, 'address'::text, 'postcode'::text, 'city'::text, 'register_status'::text, 'established_at'::text, 'business_purpose'::text, 'wz_code'::text]));


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
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: -
--

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,USAGE ON SEQUENCES  TO ${writer_role};


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: -
--

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES  TO ${writer_role};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES  TO ${reader_role};


--
--

-- The DAGs' bulk updates use TEMP tables; a database-level grant, so pg_dump does not carry it.
DO $$
BEGIN
  EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO ${writer_role}', current_database());
END
$$;
