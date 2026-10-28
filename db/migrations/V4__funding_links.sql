-- The grant ↔ company link as a table, so the list loads in milliseconds instead of minutes.
--
-- `v_company_fundings` linked every grant to its company on read: group all companies by normalised
-- name and city, keep the unique ones, join the grants. Over the 296,486 Berlin companies that is
-- 2.2 seconds per query, and `v_company_profile` asks it once per company for the grant count, so a
-- list page of 50 companies took 71 seconds. The link changes only when grants or companies are
-- loaded, so it is computed then, once, by `refresh_company_funding_links()`, and read from here.
--
-- The rule is unchanged: a grant belongs to a company when the recipient's normalised name and city
-- match exactly one company. `match_method` records it, so a better matcher can be told apart later.

CREATE TABLE public.company_funding_links (
    fkz text PRIMARY KEY REFERENCES public.company_fundings(fkz) ON DELETE CASCADE,
    company_id uuid NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    match_method text DEFAULT 'name_and_city' NOT NULL,
    matched_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE INDEX company_funding_links_company_id_idx ON public.company_funding_links (company_id);

COMMENT ON TABLE public.company_funding_links IS
  'Which company each grant belongs to. Written by refresh_company_funding_links() after grants or companies are loaded; read by v_company_fundings.';

-- Recompute every link. Called by the foerderkatalog DAG after each load and by the company imports.
-- Returns the number of linked grants.
CREATE FUNCTION public.refresh_company_funding_links() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  linked integer;
BEGIN
  DELETE FROM public.company_funding_links;
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
  JOIN unique_names u ON u.normalized_name = f.recipient_normalized_name AND u.city = lower(f.city);
  GET DIAGNOSTICS linked = ROW_COUNT;
  RETURN linked;
END
$$;

-- Same columns, same order: the views and the app built on it do not change.
CREATE OR REPLACE VIEW public.v_company_fundings AS
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
FROM public.company_fundings f
JOIN public.company_funding_links l ON l.fkz = f.fkz;

SELECT public.refresh_company_funding_links();

GRANT EXECUTE ON FUNCTION public.refresh_company_funding_links() TO ${writer_role};
