-- The register_crosslink source (#112) gets its text for the app's sources page, like every source in V4.
INSERT INTO public.sources (key, name, source_type, description, terms, terms_url, schedule) VALUES
  ('register_crosslink', 'Register numbers on other companies'' guessed sites', 'derived',
   'Seiten, die die Website-Suche für eine andere Firma gelesen hat und die die Registernummer dieser Firma nennen. Die Domain wird ihr vorgeschlagen und wie eine geratene Domain geprüft.',
   NULL, NULL, 'wöchentlich')
ON CONFLICT (key) DO UPDATE SET
  description = EXCLUDED.description,
  terms = EXCLUDED.terms,
  terms_url = EXCLUDED.terms_url,
  schedule = EXCLUDED.schedule;
