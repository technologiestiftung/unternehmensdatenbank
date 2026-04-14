DO $$
BEGIN
  IF to_regclass('app.sources') IS NOT NULL THEN
    INSERT INTO public.sources (
      key,
      name,
      source_type,
      priority,
      created_at,
      updated_at
    )
    SELECT
      slug,
      display_name,
      'legacy',
      100,
      created_at,
      updated_at
    FROM app.sources
    ON CONFLICT (key) DO NOTHING;

    DROP TABLE app.sources;
  END IF;
END;
$$;

DROP SCHEMA IF EXISTS app;
