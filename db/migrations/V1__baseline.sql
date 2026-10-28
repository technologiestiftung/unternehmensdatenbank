-- Baseline: the whole schema as of 2026-10-28, in one file.
--
-- Nothing has been deployed, so the migrations before it were squashed into this one three times: the
-- 24 of the Osnabrück era, then V1-V9 of the Berlin rebuild, then V10, all on 2026-10-28. They live on
-- in git history; their reasoning is in the comments of the objects they created. In short, V2-V10
-- added:
--
--   identifiers and matching   company_identifiers, match columns on source records, merged_into,
--                              match_candidates, pipeline_state (issue #18)
--   feedback and provenance    a vote on every shown field, v_field_provenance for the ⓘ
--   grant links                company_funding_links: the list went from 71 s to 0.65 s a page
--   stored company profile     company_profile_mv + source_keys: every list filter under 10 ms
--   Berlin grants              company_fundings.source / recipient_register / policy_area, linked
--                              by register number
--   manager evidence           whether the register confirms a manager name
--   activity keywords          products and services from the company's own website
--   own votes                  the app's reader may delete a user's own vote
--   register announcements     the latest court announcement per company, a hint next to its status
--
-- Generated from a database migrated to V10 with `pg_dump --schema-only --no-owner`, privileges
-- included, so every grant is exactly what V1-V10 left. Role names are Flyway placeholders
-- (`${writer_role}`, `${reader_role}`) so the hub can use its own, and default privileges apply to
-- whoever runs this file. PostGIS and pgvector are created by the database setup, not here.
-- Checked: a database built from this file and one built from V1 + V10 dump to the same schema,
-- grants and seed rows.

-- pg_dump writes functions before the tables they read; their bodies are checked when called.
SET check_function_bodies = false;

--
--


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
    FROM public.companies c
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


--
-- Name: companies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.companies (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    company_name text NOT NULL,
    normalized_name text NOT NULL,
    legal_form text,
    business_purpose text,
    address text,
    established_at date,
    registration_number text,
    wz_code text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    canonical_updated_at timestamp with time zone,
    data_last_updated_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    postcode text,
    city text,
    district text,
    locality text,
    data_quality_score numeric(4,3),
    register_status text,
    merged_into uuid,
    CONSTRAINT companies_data_quality_score_check CHECK (((data_quality_score IS NULL) OR ((data_quality_score >= (0)::numeric) AND (data_quality_score <= (1)::numeric)))),
    CONSTRAINT companies_not_merged_into_self CHECK ((merged_into IS DISTINCT FROM id))
);


--
-- Name: TABLE companies; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.companies IS 'Canonical company records consumed by the application.';


--
-- Name: COLUMN companies.register_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.companies.register_status IS 'Register status as the source states it. Osnabrück: aktiv 6298, erloschen 7673, in Liquidation 472. NULL means the source did not say — note that the discovery filter is = ''aktiv'', which EXCLUDES NULL, so an import that fails to map this column yields no work at all rather than too much. Backfilled from metadata->>''status'', which is kept as the raw record.';


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
    c.company_name,
    c.normalized_name,
    c.legal_form,
    c.business_purpose,
    c.address,
    c.established_at,
    c.registration_number,
    c.wz_code,
    c.data_quality_score,
    c.canonical_updated_at,
    c.data_last_updated_at,
    c.metadata,
    c.created_at,
    c.updated_at,
    obs_website.value_text AS website_url,
    obs_cluster.value_text AS cluster,
    obs_tech.value_text AS tech_field,
    obs_revenue.value_number AS revenue,
    (obs_emp.value_number)::integer AS employee_count,
    obs_website.confidence AS website_confidence,
    obs_cluster.confidence AS cluster_confidence,
    obs_tech.confidence AS tech_field_confidence,
    obs_revenue.confidence AS revenue_confidence,
    obs_emp.confidence AS employee_count_confidence,
    src_website.key AS website_source,
    src_cluster.key AS cluster_source,
    src_tech.key AS tech_field_source,
    src_revenue.key AS revenue_source,
    src_emp.key AS employee_count_source,
    c.register_status,
    obs_tfeld.value_text AS technologiefeld,
    obs_tfeld.confidence AS technologiefeld_confidence,
    src_tfeld.key AS technologiefeld_source,
    obs_purpose.value_text AS purpose_field,
    obs_purpose.confidence AS purpose_field_confidence,
    src_purpose.key AS purpose_field_source
   FROM (((((((((((((((((((((public.companies c
     LEFT JOIN public.company_canonical_field_sources cfs_website ON (((cfs_website.company_id = c.id) AND (cfs_website.field_name = 'website'::text))))
     LEFT JOIN public.company_field_observations obs_website ON ((obs_website.id = cfs_website.company_field_observation_id)))
     LEFT JOIN public.sources src_website ON ((src_website.id = cfs_website.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_cluster ON (((cfs_cluster.company_id = c.id) AND (cfs_cluster.field_name = 'cluster'::text))))
     LEFT JOIN public.company_field_observations obs_cluster ON ((obs_cluster.id = cfs_cluster.company_field_observation_id)))
     LEFT JOIN public.sources src_cluster ON ((src_cluster.id = cfs_cluster.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_tech ON (((cfs_tech.company_id = c.id) AND (cfs_tech.field_name = 'tech_field'::text))))
     LEFT JOIN public.company_field_observations obs_tech ON ((obs_tech.id = cfs_tech.company_field_observation_id)))
     LEFT JOIN public.sources src_tech ON ((src_tech.id = cfs_tech.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_revenue ON (((cfs_revenue.company_id = c.id) AND (cfs_revenue.field_name = 'revenue'::text))))
     LEFT JOIN public.company_field_observations obs_revenue ON ((obs_revenue.id = cfs_revenue.company_field_observation_id)))
     LEFT JOIN public.sources src_revenue ON ((src_revenue.id = cfs_revenue.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_emp ON (((cfs_emp.company_id = c.id) AND (cfs_emp.field_name = 'employee_count'::text))))
     LEFT JOIN public.company_field_observations obs_emp ON ((obs_emp.id = cfs_emp.company_field_observation_id)))
     LEFT JOIN public.sources src_emp ON ((src_emp.id = cfs_emp.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_tfeld ON (((cfs_tfeld.company_id = c.id) AND (cfs_tfeld.field_name = 'technologiefeld'::text))))
     LEFT JOIN public.company_field_observations obs_tfeld ON ((obs_tfeld.id = cfs_tfeld.company_field_observation_id)))
     LEFT JOIN public.sources src_tfeld ON ((src_tfeld.id = cfs_tfeld.source_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_purpose ON (((cfs_purpose.company_id = c.id) AND (cfs_purpose.field_name = 'purpose_field'::text))))
     LEFT JOIN public.company_field_observations obs_purpose ON ((obs_purpose.id = cfs_purpose.company_field_observation_id)))
     LEFT JOIN public.sources src_purpose ON ((src_purpose.id = cfs_purpose.source_id)));


--
-- Name: VIEW v_companies_enriched; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_companies_enriched IS 'The view applications should query. THREE industry-ish columns live here and they are three different questions -- getting that wrong was the expensive mistake in this project. tech_field: free-text LLM output from the search providers, 508 rows, no controlled vocabulary, whatever the model felt like typing. technologiefeld: one of Berlin''s 77 taxonomy fields, requires a website somebody actually fetched, written by branch_classifier. cluster: the same kind of free text as tech_field. They must never be merged or coalesced, and a WZ code may never fill technologiefeld. purpose_field is a fourth question again: what the register says the company was set up to do, with no website requirement at all. See docs/branch-classification.md and docs/purpose-classification.md.';


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
    e.employee_count,
    e.revenue,
    COALESCE(fund.funding_count, (0)::bigint) AS funding_count,
    fund.funding_total_eur,
    e.wz_code,
    e.established_at,
    e.address,
    c.postcode,
    c.city,
    e.legal_form,
    e.cluster,
    e.technologiefeld,
    e.website_url AS website,
    mgr.managers AS management,
    e.business_purpose,
    obs_deeptech.value_text AS deeptech_class,
    obs_deeptech.confidence AS deeptech_confidence,
    (obs_ai.value_text)::boolean AS is_ai,
    obs_amazon.value_text AS amazon_effect,
    e.register_status,
    GREATEST(e.data_last_updated_at, e.canonical_updated_at) AS updated_at,
    obs_keywords.value_text AS activity_keywords,
    obs_announce.value_text AS register_announcement,
    ((obs_announce.metadata ->> 'published_on'::text))::date AS register_announcement_date
   FROM (((((((((((((public.v_companies_enriched e
     JOIN public.companies c ON ((c.id = e.id)))
     LEFT JOIN LATERAL ( SELECT count(*) AS funding_count,
            sum(f.amount_eur) AS funding_total_eur
           FROM public.v_company_fundings f
          WHERE (f.company_id = e.id)) fund ON (true))
     LEFT JOIN LATERAL ( SELECT string_agg(DISTINCT cm.full_name, '; '::text ORDER BY cm.full_name) AS managers
           FROM public.company_managers cm
          WHERE ((cm.company_id = e.id) AND cm.is_current)) mgr ON (true))
     LEFT JOIN public.company_canonical_field_sources cfs_deeptech ON (((cfs_deeptech.company_id = e.id) AND (cfs_deeptech.field_name = 'deeptech_class'::text))))
     LEFT JOIN public.company_field_observations obs_deeptech ON ((obs_deeptech.id = cfs_deeptech.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_ai ON (((cfs_ai.company_id = e.id) AND (cfs_ai.field_name = 'is_ai'::text))))
     LEFT JOIN public.company_field_observations obs_ai ON ((obs_ai.id = cfs_ai.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_amazon ON (((cfs_amazon.company_id = e.id) AND (cfs_amazon.field_name = 'amazon_effect'::text))))
     LEFT JOIN public.company_field_observations obs_amazon ON ((obs_amazon.id = cfs_amazon.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_keywords ON (((cfs_keywords.company_id = e.id) AND (cfs_keywords.field_name = 'activity_keywords'::text))))
     LEFT JOIN public.company_field_observations obs_keywords ON ((obs_keywords.id = cfs_keywords.company_field_observation_id)))
     LEFT JOIN public.company_canonical_field_sources cfs_announce ON (((cfs_announce.company_id = e.id) AND (cfs_announce.field_name = 'register_announcement'::text))))
     LEFT JOIN public.company_field_observations obs_announce ON ((obs_announce.id = cfs_announce.company_field_observation_id)));


--
-- Name: VIEW v_company_profile_live; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_profile_live IS 'The target record per company, read by the app through Hasura. Comments are a separate table (company_comments). deeptech_class, is_ai and amazon_effect are NULL until something writes them.';


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
    p.cluster,
    p.technologiefeld,
    p.website,
    p.management,
    p.business_purpose,
    p.deeptech_class,
    p.deeptech_confidence,
    p.is_ai,
    p.amazon_effect,
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
  WITH NO DATA;


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
    CONSTRAINT extraction_feedback_field_name_check CHECK ((field_name = ANY (ARRAY['name'::text, 'address'::text, 'legal_form'::text, 'register_status'::text, 'wz_code'::text, 'established_at'::text, 'employee_count'::text, 'revenue'::text, 'fundings'::text, 'cluster'::text, 'technologiefeld'::text, 'website'::text, 'management'::text, 'business_purpose'::text, 'deeptech'::text, 'is_ai'::text, 'amazon_effect'::text, 'activity_keywords'::text, 'register_announcement'::text]))),
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
-- Name: v_company_profile; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_profile AS
 SELECT company_profile_mv.id,
    company_profile_mv.name,
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
    company_profile_mv.cluster,
    company_profile_mv.technologiefeld,
    company_profile_mv.website,
    company_profile_mv.management,
    company_profile_mv.business_purpose,
    company_profile_mv.deeptech_class,
    company_profile_mv.deeptech_confidence,
    company_profile_mv.is_ai,
    company_profile_mv.amazon_effect,
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
    o.observed_at,
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
    o.observed_at,
    m.updated_at AS selected_at
   FROM ((public.company_managers m
     LEFT JOIN public.company_field_observations o ON ((o.id = m.company_field_observation_id)))
     LEFT JOIN public.sources s ON ((s.id = m.source_id)))
  WHERE m.is_current
UNION ALL
 SELECT c.id AS company_id,
    f.field_name,
    f.value,
    s.key AS source_key,
    s.name AS source_name,
    f.method,
    NULL::numeric AS confidence,
    NULL::jsonb AS evidence,
    NULL::text AS link,
    ((r.source_payload #>> '{register,retrieved_at}'::text[]))::timestamp with time zone AS observed_at,
    r.last_synced_at AS selected_at
   FROM (((public.companies c
     JOIN public.company_source_records r ON ((r.company_id = c.id)))
     JOIN public.sources s ON (((s.id = r.source_id) AND (s.source_type = 'register'::text))))
     CROSS JOIN LATERAL ( VALUES ('name'::text,c.company_name,'register'::text), ('address'::text,c.address,'register'::text), ('legal_form'::text,c.legal_form,'read from the end of the registered name'::text), ('register_status'::text,c.register_status,'register'::text), ('wz_code'::text,c.wz_code,'register'::text), ('business_purpose'::text,c.business_purpose,'register'::text), ('established_at'::text,(c.established_at)::text,'register'::text)) f(field_name, value, method))
  WHERE (f.value IS NOT NULL)
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
     LEFT JOIN public.sources s ON ((s.key = f.source)));


--
-- Name: VIEW v_field_provenance; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_field_provenance IS 'Where every value the app shows came from: source, method, confidence, evidence, link and dates, one row per company, field and value. No personal data beyond the shown value.';


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (id);


--
-- Name: companies companies_registration_number_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_registration_number_key UNIQUE (registration_number);


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
-- Name: companies_data_quality_score_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_data_quality_score_idx ON public.companies USING btree (data_quality_score NULLS FIRST);


--
-- Name: companies_merged_into_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_merged_into_idx ON public.companies USING btree (merged_into) WHERE (merged_into IS NOT NULL);


--
-- Name: companies_metadata_gin_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_metadata_gin_idx ON public.companies USING gin (metadata);


--
-- Name: companies_normalized_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_normalized_name_idx ON public.companies USING btree (normalized_name);


--
-- Name: companies_normalized_name_postcode_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_normalized_name_postcode_idx ON public.companies USING btree (normalized_name, postcode);


--
-- Name: companies_postcode_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_postcode_idx ON public.companies USING btree (postcode);


--
-- Name: companies_register_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_register_status_idx ON public.companies USING btree (register_status);


--
-- Name: company_canonical_field_sources_company_field_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_canonical_field_sources_company_field_idx ON public.company_canonical_field_sources USING btree (company_id, field_name);


--
-- Name: company_canonical_field_sources_source_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_canonical_field_sources_source_idx ON public.company_canonical_field_sources USING btree (source_id);


--
-- Name: company_comments_company_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_comments_company_idx ON public.company_comments USING btree (company_id, created_at DESC);


--
-- Name: company_field_observations_company_field_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_field_observations_company_field_idx ON public.company_field_observations USING btree (company_id, field_name, observed_at DESC);


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
-- Name: company_profile_mv_technologiefeld; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_profile_mv_technologiefeld ON public.company_profile_mv USING btree (technologiefeld);


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
-- Name: extraction_feedback_field_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX extraction_feedback_field_idx ON public.extraction_feedback USING btree (field_name, verdict);


--
-- Name: match_candidates_open_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX match_candidates_open_idx ON public.match_candidates USING btree (score DESC) WHERE (status = 'open'::text);


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
-- Name: uq_obs_company_field_source_value; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_obs_company_field_source_value ON public.company_field_observations USING btree (company_id, field_name, source_id, value_text) WHERE (field_name = ANY (ARRAY['website'::text, 'candidate_url'::text, 'directory_url'::text, 'discovery_tier'::text, 'search_query'::text, 'osm_id'::text, 'technologiefeld'::text, 'purpose_field'::text, 'technologiefeld_withheld'::text, 'purpose_field_withheld'::text]));


--
-- Name: INDEX uq_obs_company_field_source_value; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON INDEX public.uq_obs_company_field_source_value IS 'Upsert key for the ten field names combined_discovery/observations.py and the branch classifier write, so a re-run updates instead of appending. Partial: 71 duplicate groups remain in ceo_name_or_contact, email, phone, website_search_query and cluster, and each needs its own review first. insert_text_observation repeats this predicate and must match it exactly.';


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
  '{"writes": ["technologiefeld", "purpose_field", "technologiefeld_withheld", "purpose_field_withheld"],
    "confidence_basis": "fixed_by_decided_by",
    "confidence_by_decided_by": {"keywords": 0.60, "model_direct": 0.50, "model_tiebreak": 0.55}}'
);

INSERT INTO public.sources (key, name, source_type, base_url) VALUES
    ('foerderkatalog', 'Förderkatalog des Bundes', 'fundings', 'https://foerderportal.bund.de/foekat/'),
    ('berlin_zuwendungen', 'Zuwendungsdatenbank des Landes Berlin', 'fundings',
     'https://www.berlin.de/sen/finanzen/service/zuwendungsdatenbank/')
ON CONFLICT (key) DO NOTHING;

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
-- Name: TABLE source_sync_runs; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.source_sync_runs TO ${writer_role};
GRANT SELECT ON TABLE public.source_sync_runs TO ${reader_role};


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
