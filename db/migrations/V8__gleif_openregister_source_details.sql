-- GLEIF (#88) and OpenRegister (#88) came after V4, so the app's source page showed no origin, text or
-- terms for them (ensure_source registers a source without these columns). Filled here like V4 did.
INSERT INTO public.sources (key, name, source_type, base_url, description, terms, terms_url, schedule) VALUES
  ('gleif', 'GLEIF (LEI records, Berlin register)', 'register', 'https://www.gleif.org',
   'Die offenen Daten der Global Legal Entity Identifier Foundation: jede Firma mit einer LEI-Kennung am Amtsgericht Charlottenburg, mit Name, Rechtsform, Adresse und Status. Über die Registernummer zugeordnet; fehlt die Firma bei uns, wird sie angelegt.',
   'GLEIF veröffentlicht die LEI-Daten unter CC0.', NULL, 'täglich'),
  ('openregister', 'OpenRegister (Handelsregister API)', 'register_api', 'https://openregister.de',
   'Das Handelsregister als API: Berliner Firmen, die seit 2019 eingetragen wurden, mit Adresse und Unternehmensgegenstand. Über die Registernummer zugeordnet; fehlt die Firma bei uns, wird sie angelegt.',
   'Ob OpenRegister das Speichern und regelmäßige Abrufen erlaubt, ist angefragt und noch nicht beantwortet. Deshalb startet jeden Lauf ein Mensch.',
   NULL, 'nur von Hand')
ON CONFLICT (key) DO UPDATE SET
  base_url = EXCLUDED.base_url,
  description = EXCLUDED.description,
  terms = EXCLUDED.terms,
  terms_url = EXCLUDED.terms_url,
  schedule = EXCLUDED.schedule;
