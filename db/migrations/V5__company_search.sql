-- Fuzzy company search and the website top-level domains, for the app's list filters (#164).
--
-- 1. `pg_trgm`: trigram similarity. A trusted extension, so the schema owner creates it (no superuser).
-- 2. `company_search_text`: what the search box searches, lower-cased: name, register number, website and
--    address. Not the management: the list is no way to look up a person. IMMUTABLE, so it can be indexed.
-- 3. A trigram GIN index on that text in `company_profile_mv`; `refresh_company_profile()` keeps it current.
-- 4. `search_companies(search)`: the companies whose text contains the search, the closest names first
--    ("zalando": Zalando AG before Zalando Payments GmbH). Only when nothing contains it, the ones with a
--    word like it (word similarity >= 0.5, so "berghian" finds "Berghain"). It returns `v_company_profile`
--    rows, so Hasura offers it with the same `where`, relationships and permissions as `companies`. The
--    threshold is a SET on the function, so Postgres runs it as a function scan: locally 2-20 ms for a
--    name, about 1 s for "gmbh" (half of the 300,000 companies).
-- 5. `v_website_tlds`: each top-level domain of a company website with its count, the options of the
--    app's website filter. The app filters with the same pattern on `website`.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE FUNCTION public.company_search_text(name text, registration_number text, website text, address text)
    RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
  SELECT lower(coalesce(name, '') || ' ' || coalesce(registration_number, '') || ' ' || coalesce(website, '') || ' ' || coalesce(address, ''))
$$;

COMMENT ON FUNCTION public.company_search_text(text, text, text, text) IS 'What the app''s search box searches, lower-cased: name, register number, website, address. Never the management. Indexed on company_profile_mv (V5).';

CREATE INDEX company_profile_mv_search_trgm ON public.company_profile_mv
    USING gin (public.company_search_text(name, registration_number, website, address) public.gin_trgm_ops);

CREATE FUNCTION public.search_companies(search text)
    RETURNS SETOF public.v_company_profile
    LANGUAGE sql STABLE
    SET pg_trgm.word_similarity_threshold = 0.5
    AS $$
  WITH q AS (
    SELECT lower(trim(search)) AS term,
           '%' || replace(replace(replace(lower(trim(search)), '\', '\\'), '%', '\%'), '_', '\_') || '%' AS pattern
  ),
  hits AS (
    SELECT p.*, public.similarity(q.term, lower(p.name)) AS score
    FROM public.v_company_profile p, q
    WHERE public.company_search_text(p.name, p.registration_number, p.website, p.address) LIKE q.pattern
  ),
  near AS (
    SELECT p.*, public.word_similarity(q.term, lower(p.name)) AS score
    FROM public.v_company_profile p, q
    WHERE NOT EXISTS (SELECT 1 FROM hits)
      AND q.term OPERATOR(public.<%) public.company_search_text(p.name, p.registration_number, p.website, p.address)
  )
  SELECT id, name, registration_number, employee_count, revenue, funding_count, funding_total_eur, wz_code,
         established_at, address, postcode, city, legal_form, technology_field, website, management,
         business_purpose, deeptech_class, deeptech_confidence, is_ai, register_status, updated_at,
         activity_keywords, register_announcement, register_announcement_date, source_keys, established_year,
         wz_division_estimated
  FROM (SELECT * FROM hits UNION ALL SELECT * FROM near) r
  ORDER BY score DESC, name, id
$$;

COMMENT ON FUNCTION public.search_companies(text) IS 'The app''s search: companies whose name, register number, website or address contains the search, closest names first; only when none does, those with a word like it (word similarity >= 0.5) (V5).';

CREATE VIEW public.v_website_tlds AS
 SELECT lower(substring(website FROM '^(?:[a-zA-Z]+://)?[^/:?#]*\.([A-Za-z]{2,24})(?:[:/?#]|$)')) AS tld,
    count(*) AS company_count
   FROM public.company_profile_mv
  WHERE website IS NOT NULL
  GROUP BY 1
 HAVING lower(substring(website FROM '^(?:[a-zA-Z]+://)?[^/:?#]*\.([A-Za-z]{2,24})(?:[:/?#]|$)')) IS NOT NULL;

COMMENT ON VIEW public.v_website_tlds IS 'Each top-level domain of a company website ("de", "berlin") with its number of companies: the options of the app''s website filter (V5).';

GRANT EXECUTE ON FUNCTION public.company_search_text(text, text, text, text) TO ${reader_role};
GRANT EXECUTE ON FUNCTION public.search_companies(text) TO ${reader_role};
GRANT SELECT ON TABLE public.v_website_tlds TO ${reader_role};
