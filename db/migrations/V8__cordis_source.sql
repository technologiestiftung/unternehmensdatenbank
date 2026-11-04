-- The cordis source (#104): EU research grants of Berlin companies, with its text for the app's sources
-- page, like every source in V4.
INSERT INTO public.sources (key, name, source_type, description, terms, terms_url, schedule) VALUES
  ('cordis', 'CORDIS (EU research projects)', 'fundings',
   'Förderungen der EU-Forschungsprogramme Horizon Europe und Horizon 2020 an Firmen in Berlin, über Name und Ort einer Firma zugeordnet.',
   'Weiterverwendung nach dem Reuse Notice der Europäischen Kommission; die genauen Bedingungen sind noch nicht geprüft.', 'https://data.europa.eu/data/datasets/cordis-eu-research-projects-under-horizon-europe-2021-2027',
   'monatlich')
ON CONFLICT (key) DO UPDATE SET
  name = EXCLUDED.name,
  source_type = EXCLUDED.source_type,
  description = EXCLUDED.description,
  terms = EXCLUDED.terms,
  terms_url = EXCLUDED.terms_url,
  schedule = EXCLUDED.schedule;
