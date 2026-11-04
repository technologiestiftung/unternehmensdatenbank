-- The ba_jobsuche source (#106): Berlin employers with open job ads, with its text for the app's sources
-- page, like every source in V4. Read against the portal's terms, on the developer's decision of 2026-11-04.
INSERT INTO public.sources (key, name, source_type, description, terms, terms_url, schedule) VALUES
  ('ba_jobsuche', 'Bundesagentur für Arbeit, Jobsuche (Berlin employers)', 'job_board',
   'Arbeitgeber mit offenen Stellen in Berlin aus der Jobsuche der Bundesagentur für Arbeit, über Name und Postleitzahl einer Firma zugeordnet. Gespeichert werden nur zugeordnete Arbeitgeber und die Zahl ihrer Anzeigen, nie Ansprechpersonen.',
   'Die Nutzungsbedingungen (2a Abs. 3) untersagen das automatisierte Auslesen zur Datensammlung; gelesen auf Entscheidung des Entwicklers vom 04.11.2026, nur von Hand gestartet.',
   'https://www.arbeitsagentur.de/nutzungsbedingungen', 'von Hand')
ON CONFLICT (key) DO UPDATE SET
  name = EXCLUDED.name,
  source_type = EXCLUDED.source_type,
  description = EXCLUDED.description,
  terms = EXCLUDED.terms,
  terms_url = EXCLUDED.terms_url,
  schedule = EXCLUDED.schedule;
