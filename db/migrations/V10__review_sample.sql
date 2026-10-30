-- A fixed sample of companies people check by hand, so precision is measured, not assumed.
--
-- Until now no shown value had been checked by a person (CLAUDE.md, rule 7): every confidence,
-- threshold and half-life is a starting value. `review_sample` is a FIXED set, so a measurement is
-- comparable over time; people vote on its companies with the app's existing ✓/✗ (list filter
-- "Stichprobe"), and `v_review_results` turns those votes into precision per field and stratum.
--
-- Stratified, so every kind of value gets checked, not only the common register facts. Drawn by a
-- stable random order (a hash of the company id), not by low confidence: the doubtful cases alone
-- would make every source look worse than it is.

CREATE TABLE public.review_sample (
    company_id uuid PRIMARY KEY REFERENCES public.companies(id) ON DELETE CASCADE,
    -- Why it is in the sample: the kind of value it was drawn for.
    stratum text NOT NULL CHECK (stratum IN ('website', 'technology_field', 'management', 'register')),
    drawn_at timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE public.review_sample IS
  'Companies people check by hand, drawn by draw_review_sample() (stratified, stable random order). Never redrawn, only topped up, so measurements stay comparable.';

-- Tops every stratum up to its target with active companies not yet in the sample. Calling it again
-- never replaces anyone; a stratum with too few candidates (no Technologiefeld yet) fills on a later
-- call. A company is drawn for the first stratum that fits: website, technology_field, management,
-- register. Returns how many companies were added.
CREATE FUNCTION public.draw_review_sample(
    websites integer DEFAULT 40, technology_fields integer DEFAULT 25,
    managers integer DEFAULT 20, registers integer DEFAULT 15
) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  added integer;
BEGIN
  WITH candidates AS (
    SELECT p.id,
           CASE WHEN p.website IS NOT NULL THEN 'website'
                WHEN p.technology_field IS NOT NULL THEN 'technology_field'
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

-- Precision per field and stratum: of the shown values people judged in the sample, how many they
-- called correct. A value people corrected counts as wrong. Only votes on companies in the sample.
CREATE VIEW public.v_review_results AS
 SELECT s.stratum,
    f.field_name,
    count(DISTINCT s.company_id) FILTER (WHERE f.id IS NOT NULL) AS companies_judged,
    count(*) FILTER (WHERE f.verdict = 'correct') AS correct,
    count(*) FILTER (WHERE f.verdict = 'wrong') AS wrong,
    round(count(*) FILTER (WHERE f.verdict = 'correct')::numeric
          / NULLIF(count(*) FILTER (WHERE f.verdict IN ('correct', 'wrong')), 0), 3) AS precision
   FROM public.review_sample s
     JOIN public.extraction_feedback f ON f.company_id = s.company_id AND f.value <> ''
  GROUP BY s.stratum, f.field_name;
COMMENT ON VIEW public.v_review_results IS
  'Hand-checked precision per field and stratum, from the app''s votes on the review sample. Missing values (a vote on an empty field) are not counted here.';

GRANT SELECT, INSERT, DELETE, UPDATE ON public.review_sample TO ${writer_role};
GRANT SELECT ON public.review_sample, public.v_review_results TO ${reader_role};
GRANT SELECT ON public.v_review_results TO ${writer_role};
