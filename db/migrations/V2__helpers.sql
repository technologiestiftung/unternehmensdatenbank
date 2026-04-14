CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION public.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.normalize_company_name(input_name TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
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

COMMENT ON FUNCTION public.touch_updated_at() IS 'Generic trigger function that refreshes updated_at on row updates.';
COMMENT ON FUNCTION public.normalize_company_name(TEXT) IS 'Normalizes company names for stable matching across legal forms and punctuation.';
