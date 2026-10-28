-- Baseline: the whole schema as of 2026-10-28, in one file.
--
-- Nothing had been deployed when this was written, so the 24 migrations before it (V1-V24, up to
-- the extraction feedback of V23/V24) were squashed into this one (issue #18). They live on in git
-- history; their reasoning moved into the comments of the objects they created.
--
-- Left out on purpose, because nothing reads them since the Osnabrück example data and the search
-- experiments were removed: the table `northdata_export` and the views `v_company_full`,
-- `v_website_provenance`, `v_discovery_results` and `v_discovery_scorecard`. The two data-fix
-- migrations V17 and V19 repaired rows that no longer exist and leave only their indexes here.
--
-- Generated from a database migrated to V24 with `pg_dump --schema-only --no-owner
-- --no-privileges`; the grants at the end are written by hand from V21-V23, so the role names stay
-- Flyway placeholders (`${writer_role}`, `${reader_role}`) and the hub can use its own.
-- PostGIS and pgvector are created by the database setup, not here.

--
--

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
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
    CONSTRAINT companies_data_quality_score_check CHECK (((data_quality_score IS NULL) OR ((data_quality_score >= (0)::numeric) AND (data_quality_score <= (1)::numeric))))
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
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL
);

--
-- Name: TABLE company_fundings; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_fundings IS 'Grants from the Förderkatalog des Bundes, one row per Förderkennzeichen. Not linked to companies here; v_company_fundings does the matching on read. See V22.';

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
    updated_at timestamp with time zone DEFAULT now() NOT NULL
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
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

--
-- Name: TABLE company_source_records; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.company_source_records IS 'One raw source record per external company identity and source.';

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
    CONSTRAINT extraction_feedback_field_name_check CHECK ((field_name = ANY (ARRAY['management'::text, 'website'::text, 'technologiefeld'::text]))),
    CONSTRAINT extraction_feedback_value_check CHECK (((length(value) >= 1) AND (length(value) <= 500))),
    CONSTRAINT extraction_feedback_verdict_check CHECK ((verdict = ANY (ARRAY['correct'::text, 'wrong'::text])))
);

--
-- Name: TABLE extraction_feedback; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.extraction_feedback IS 'Users'' votes on extracted values (correct/wrong), one per user per company, field and value. The labels for measuring and training extraction. Contains personal data for field management. See V23.';

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
 WITH unique_names AS (
         SELECT companies.normalized_name,
            lower(companies.city) AS city,
            (array_agg(companies.id))[1] AS company_id
           FROM public.companies
          WHERE (companies.city IS NOT NULL)
          GROUP BY companies.normalized_name, (lower(companies.city))
         HAVING (count(*) = 1)
        )
 SELECT u.company_id,
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
     JOIN unique_names u ON (((u.normalized_name = f.recipient_normalized_name) AND (u.city = lower(f.city)))));

--
-- Name: v_company_profile; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_company_profile AS
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
    GREATEST(e.data_last_updated_at, e.canonical_updated_at) AS updated_at
   FROM (((((((((public.v_companies_enriched e
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
     LEFT JOIN public.company_field_observations obs_amazon ON ((obs_amazon.id = cfs_amazon.company_field_observation_id)));

--
-- Name: VIEW v_company_profile; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.v_company_profile IS 'The target record per company, read by the app through Hasura. Comments are a separate table (company_comments). deeptech_class, is_ai and amazon_effect are NULL until something writes them.';

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
-- Name: company_fundings company_fundings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_fundings
    ADD CONSTRAINT company_fundings_pkey PRIMARY KEY (fkz);

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
-- Name: company_managers uq_company_managers_company_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_managers
    ADD CONSTRAINT uq_company_managers_company_name UNIQUE (company_id, full_name);

--
-- Name: CONSTRAINT uq_company_managers_company_name ON company_managers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON CONSTRAINT uq_company_managers_company_name ON public.company_managers IS 'What makes resolve_ceo_contact idempotent. Before it existed, ON CONFLICT DO NOTHING had no target to conflict on and every coordinator run appended ~1,014 duplicate rows: 9,195 rows for 1,047 distinct names, worst case 21 copies. Do not drop this without also changing pipeline_coordinator/resolver.py, which names it explicitly.';

--
-- Name: companies_data_last_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_data_last_updated_at_idx ON public.companies USING btree (data_last_updated_at);

--
-- Name: companies_data_quality_score_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX companies_data_quality_score_idx ON public.companies USING btree (data_quality_score NULLS FIRST);

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
-- Name: company_fundings_recipient_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_fundings_recipient_idx ON public.company_fundings USING btree (recipient_normalized_name, lower(city));

--
-- Name: company_managers_company_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX company_managers_company_id_idx ON public.company_managers USING btree (company_id);

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
-- Name: source_sync_runs source_sync_runs_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER source_sync_runs_touch_updated_at BEFORE UPDATE ON public.source_sync_runs FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

--
-- Name: sources sources_touch_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER sources_touch_updated_at BEFORE UPDATE ON public.sources FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

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
-- Name: company_managers company_managers_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_managers
    ADD CONSTRAINT company_managers_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;

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
-- Name: source_sync_runs source_sync_runs_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_sync_runs
    ADD CONSTRAINT source_sync_runs_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.sources(id) ON DELETE CASCADE;

--
--

-- ================================================================================================
-- Seed data: what an empty database needs to work.
-- ================================================================================================

-- The industry classifier's source row. Every other source creates its own row when it first runs
-- (`source_enrichment_common.ensure_source`). Confidence is policy, not measurement: the classifier
-- scores by unbounded keyword weight, and the real score is in each observation's metadata.score.
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

-- ================================================================================================
-- Grants for the hub's three logins: the owner runs this file; DAGs write as the writer; Hasura
-- reads as the reader. The writer may change rows but never the schema.
-- ================================================================================================

GRANT USAGE ON SCHEMA public TO ${writer_role}, ${reader_role};

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${writer_role};
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${writer_role};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO ${writer_role}, ${reader_role};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${reader_role};

-- Every table a later migration creates gets the same grants, because migrations run as the owner.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${writer_role};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO ${writer_role};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO ${reader_role};

-- TEMP tables for the DAGs' bulk updates.
DO $$
BEGIN
  EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO ${writer_role}', current_database());
END
$$;

-- What the app may write through Hasura: comments, and votes and corrections on extracted values.
GRANT INSERT ON public.company_comments TO ${reader_role};
GRANT INSERT, UPDATE ON public.extraction_feedback TO ${reader_role};
