-- A page per source in the app (#102): what the source is, where it comes from, under which terms,
-- and how good its values look: how confident, how often shown, how often another source won.
--
-- 1. `sources` gets four descriptive columns, filled here for every source we have. A source a DAG
--    registers later (ensure_source) gets a row without them until a migration fills them.
-- 2. `source_field_stats_mv` also counts values per confidence range and the companies where another
--    source's value is shown. The ranges are fixed numbers, the same for every field; each field has
--    its own bar in the resolver, so "below 0.35" is not "never shown" for every field.
-- 3. `source_stats_mv` also counts how records found their company (identifier, name, created).
-- 4. `v_source_runs`: the last 20 runs per source, counts only (never the error message).
-- Counts only, never a value: the views stay as safe to expose as before.

ALTER TABLE public.sources
    ADD COLUMN description text,
    ADD COLUMN terms text,
    ADD COLUMN terms_url text,
    ADD COLUMN schedule text;
COMMENT ON COLUMN public.sources.description IS 'One or two sentences for the app (German): what the source is and how we get it.';
COMMENT ON COLUMN public.sources.terms IS 'What we know about its terms of use (German); NULL when nobody has checked them.';
COMMENT ON COLUMN public.sources.terms_url IS 'Where those terms are published, when there is such a page.';
COMMENT ON COLUMN public.sources.schedule IS 'How often it runs (German), as the DAG is set up.';

INSERT INTO public.sources (key, name, source_type, base_url, description, terms, terms_url, schedule) VALUES
  ('offeneregister', 'OffeneRegister (Handelsregister snapshot 2017–2019)', 'register', 'https://offeneregister.de',
   'Ein Abzug des Handelsregisters, gelesen von Juni 2017 bis Januar 2019: Name, Rechtsform, Adresse, Status und Registernummer jeder Firma am Amtsgericht Charlottenburg. Die Grundlage aller anderen Quellen.',
   NULL, NULL, 'einmal importiert'),
  ('registerbekanntmachungen', 'Registerbekanntmachungen (handelsregister.de)', 'register_announcements',
   'https://www.handelsregister.de/rp_web/bekanntmachungen/welcome.xhtml',
   'Bekanntmachungen des Registerportals für Berlin (Umwandlungen, Löschungsankündigungen, neue Dokumente) der letzten acht Wochen. Nur die Ergebnisliste, nie eine Detailseite. Ein Hinweis neben dem Registerstatus, nie seine Änderung.',
   NULL, NULL, 'wöchentlich'),
  ('handelsregister_lookup', 'Registerportal lookups (handelsregister.de)', 'register',
   'https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml',
   'Die Suche des Registerportals, eine Abfrage je Firma nach Registernummer: heutiger Name, Sitz und Status, höchstens 50 Firmen je Lauf.',
   'Die Nutzungsordnung des Portals erlaubt einzelne Abrufe und höchstens 60 je Stunde; systematische Abrufe sind unzulässig (Ziffer 3). Deshalb startet jeden Lauf ein Mensch.',
   NULL, 'nur von Hand'),
  ('website_discovery', 'Website discovery (URL patterns, checked against the Impressum)', 'discovery', NULL,
   'Rät Domains aus dem Firmennamen und liest das Impressum. Eine Website zählt nur, wenn die Seite die Firma belegt: eigene Registernummer, ein Geschäftsführer aus dem Register oder Name und Postleitzahl.',
   NULL, NULL, 'stündlich in Paketen'),
  ('wikidata', 'Wikidata (official websites, P856)', 'knowledge_base', 'https://www.wikidata.org',
   'Offizielle Websites, die Wikidata für Berliner Organisationen nennt, über Registernummer oder eindeutigen Namen zugeordnet und wie eine geratene Domain geprüft.',
   'Wikidata steht unter CC0.', 'https://www.wikidata.org/wiki/Wikidata:Licensing', 'täglich'),
  ('common_crawl', 'Common Crawl (Impressum pages with a register number)', 'web_archive', 'https://commoncrawl.org',
   'Impressumsseiten im Common Crawl, die eine Berliner Registernummer nennen. Die Domain wird der Firma mit dieser Nummer vorgeschlagen und wie eine geratene Domain geprüft.',
   NULL, 'https://commoncrawl.org/terms-of-use', 'wöchentlich'),
  ('wirtschaftsatlas', 'Wirtschaftsatlas Berlin (Berlin Partner)', 'directory',
   'https://www.businesslocationcenter.de/wirtschaftsatlas-berlin/',
   'Unternehmen mit Website aus den Branchenebenen des Wirtschaftsatlas von Berlin Partner, über Name und Postleitzahl zugeordnet. Jede Website wird wie eine geratene Domain geprüft.',
   'Laut Impressum braucht jede Verwendung die vorherige Zustimmung von Berlin Partner; die Zustimmung ist angefragt (#105). Deshalb startet jeden Lauf ein Mensch.',
   'https://www.businesslocationcenter.de/impressum', 'nur von Hand'),
  ('impressum_contacts', 'Impressum contacts', 'website', NULL,
   'E-Mail-Adresse und Telefonnummer der Firma aus dem Impressum ihrer belegten Website. Gespeichert, nie angezeigt.',
   NULL, NULL, 'nach der Website-Suche'),
  ('website_keywords', 'Website keywords', 'website', NULL,
   'Stichworte aus Meta-Tags und Überschriften der Leistungs- und Produktseiten der belegten Website. Zu häufige Wörter werden über alle Firmen herausgerechnet.',
   NULL, NULL, 'nach der Website-Suche'),
  ('branch_classifier', 'Branch Classifier (Berlin 77-field taxonomy)', 'classifier', NULL,
   'Das Technologiefeld aus den Stichworten auf der Website (Berlins 77 Felder). Ein Textmodell füllt nur Lücken, in denen kein Stichwort vorkam.',
   NULL, NULL, 'nach der Website-Suche'),
  ('spacy_ner', 'spaCy manager names', 'ner_model', NULL,
   'Namen der Geschäftsführung, die ein spaCy-Modell im Impressum findet. Das Modell wird mit den Bewertungen aus der App weiter trainiert.',
   NULL, NULL, 'nach der Website-Suche'),
  ('foerderkatalog', 'Förderkatalog des Bundes', 'fundings', 'https://foerderportal.bund.de/foekat/',
   'Jede Förderung des Bundes an Empfänger in Berlin, über Name und Ort einer Firma zugeordnet.',
   NULL, NULL, 'monatlich'),
  ('berlin_zuwendungen', 'Zuwendungsdatenbank des Landes Berlin', 'fundings',
   'https://www.berlin.de/sen/finanzen/service/zuwendungsdatenbank/',
   'Zuwendungen des Landes Berlin an Empfänger mit HRA- oder HRB-Nummer, über die Registernummer zugeordnet.',
   NULL, NULL, 'monatlich'),
  ('user_feedback', 'Corrections from the app', 'user_feedback', NULL,
   'Korrekturen, die Menschen in der App eingeben. Angezeigt erst, wenn zwei Personen übereinstimmen; nie, wer es war.',
   NULL, NULL, 'stündlich')
ON CONFLICT (key) DO UPDATE SET
  base_url = COALESCE(EXCLUDED.base_url, public.sources.base_url),
  description = EXCLUDED.description,
  terms = EXCLUDED.terms,
  terms_url = EXCLUDED.terms_url,
  schedule = EXCLUDED.schedule;

-- The views on top go first; they are recreated below with the new columns.
DROP VIEW public.v_source_overview;
DROP VIEW public.v_source_fields;
DROP MATERIALIZED VIEW public.source_field_stats_mv;
DROP MATERIALIZED VIEW public.source_stats_mv;

CREATE MATERIALIZED VIEW public.source_field_stats_mv AS
 WITH canonical AS (
   SELECT company_id, field_name, source_id FROM public.company_field_observations WHERE is_canonical
 )
 SELECT s.key AS source_key,
    o.field_name,
    count(*) AS observations,
    count(DISTINCT o.company_id) AS companies,
    count(DISTINCT o.company_id) FILTER (WHERE o.is_canonical OR m.id IS NOT NULL) AS companies_shown,
    -- Shown for this company and field, but another source's value: this one lost.
    count(DISTINCT o.company_id) FILTER (WHERE o.confidence > 0 AND NOT o.is_canonical AND m.id IS NULL
                                         AND c.source_id IS NOT NULL AND c.source_id <> o.source_id) AS companies_beaten,
    round(avg(o.confidence), 3) AS avg_confidence,
    count(*) FILTER (WHERE o.confidence = 0) AS confidence_zero,
    count(*) FILTER (WHERE o.confidence > 0 AND o.confidence < 0.35) AS confidence_below_035,
    count(*) FILTER (WHERE o.confidence >= 0.35 AND o.confidence < 0.6) AS confidence_035_060,
    count(*) FILTER (WHERE o.confidence >= 0.6 AND o.confidence < 0.8) AS confidence_060_080,
    count(*) FILTER (WHERE o.confidence >= 0.8) AS confidence_080_up,
    max(o.observed_at) AS last_observed_at
   FROM public.company_field_observations o
     JOIN public.sources s ON s.id = o.source_id
     LEFT JOIN public.company_managers m ON m.company_field_observation_id = o.id AND m.is_current
     LEFT JOIN canonical c ON c.company_id = o.company_id AND c.field_name = o.field_name
  GROUP BY s.key, o.field_name
UNION ALL
 SELECT f.source AS source_key,
    'fundings'::text AS field_name,
    count(DISTINCT f.fkz) AS observations,
    count(DISTINCT l.company_id) AS companies,
    count(DISTINCT l.company_id) AS companies_shown,
    0::bigint, NULL::numeric, 0::bigint, 0::bigint, 0::bigint, 0::bigint, 0::bigint,
    max(f.last_seen_at) AS last_observed_at
   FROM public.company_fundings f
     LEFT JOIN public.company_funding_links l ON l.fkz = f.fkz
  GROUP BY f.source
  WITH DATA;
CREATE UNIQUE INDEX source_field_stats_mv_key ON public.source_field_stats_mv USING btree (source_key, field_name);

CREATE MATERIALIZED VIEW public.source_stats_mv AS
 WITH covered AS (
   SELECT r.source_id, r.company_id, true AS is_record, r.match_method, r.match_confidence
     FROM public.company_source_records r
   UNION ALL
   SELECT s_1.id, l.company_id, false, NULL::text, NULL::numeric
     FROM public.company_funding_links l
       JOIN public.company_fundings f ON f.fkz = l.fkz
       JOIN public.sources s_1 ON s_1.key = f.source
 ), per_source AS (
   SELECT source_id,
          count(*) FILTER (WHERE is_record) AS records,
          count(*) FILTER (WHERE match_method = 'created') AS companies_created,
          count(*) FILTER (WHERE match_method = 'identifier') AS records_by_identifier,
          count(*) FILTER (WHERE match_method = 'fuzzy') AS records_by_name,
          count(*) FILTER (WHERE match_method = 'manual') AS records_by_hand,
          round(avg(match_confidence) FILTER (WHERE is_record), 3) AS avg_match_confidence,
          count(DISTINCT company_id) AS companies
     FROM covered
    GROUP BY source_id
 )
 SELECT s.key AS source_key,
    COALESCE(p.records, 0::bigint) AS records,
    COALESCE(p.companies_created, 0::bigint) AS companies_created,
    COALESCE(p.companies, 0::bigint) AS companies,
    COALESCE(p.records_by_identifier, 0::bigint) AS records_by_identifier,
    COALESCE(p.records_by_name, 0::bigint) AS records_by_name,
    COALESCE(p.records_by_hand, 0::bigint) AS records_by_hand,
    p.avg_match_confidence,
    now() AS computed_at
   FROM public.sources s
     LEFT JOIN per_source p ON p.source_id = s.id
  WITH DATA;
CREATE UNIQUE INDEX source_stats_mv_key ON public.source_stats_mv USING btree (source_key);

CREATE VIEW public.v_source_fields AS
 SELECT source_key, field_name, observations, companies, companies_shown, companies_beaten, avg_confidence,
        confidence_zero, confidence_below_035, confidence_035_060, confidence_060_080, confidence_080_up,
        last_observed_at
   FROM public.source_field_stats_mv;
COMMENT ON VIEW public.v_source_fields IS
  'Per source and field: values stored, companies, companies whose shown value came from this source, companies where another source''s value is shown, and values per confidence range. Counts only; no value is exposed.';

CREATE VIEW public.v_source_overview AS
 SELECT s.key AS source_key,
    s.name,
    s.source_type,
    s.base_url,
    s.description,
    s.terms,
    s.terms_url,
    s.schedule,
    s.priority,
    COALESCE(st.records, 0::bigint) AS records,
    COALESCE(st.companies, 0::bigint) AS companies,
    COALESCE(st.companies_created, 0::bigint) AS companies_created,
    COALESCE(st.records_by_identifier, 0::bigint) AS records_by_identifier,
    COALESCE(st.records_by_name, 0::bigint) AS records_by_name,
    COALESCE(st.records_by_hand, 0::bigint) AS records_by_hand,
    st.avg_match_confidence,
    run.started_at AS last_run_started_at,
    run.finished_at AS last_run_finished_at,
    run.status AS last_run_status,
    run.records_seen AS last_run_seen,
    run.records_created AS last_run_created,
    run.records_updated AS last_run_updated,
    run.records_failed AS last_run_failed,
    (SELECT count(*) FROM public.source_sync_runs r WHERE r.source_id = s.id) AS runs,
    (SELECT count(*) FROM public.source_sync_runs r
      WHERE r.source_id = s.id AND r.status = 'running'
        AND EXISTS (SELECT 1 FROM public.source_sync_runs n
                     WHERE n.source_id = r.source_id AND n.started_at > r.started_at)) AS runs_interrupted,
    st.computed_at AS counts_as_of
   FROM public.sources s
     LEFT JOIN public.source_stats_mv st ON st.source_key = s.key
     LEFT JOIN LATERAL (SELECT r.started_at, r.finished_at, r.records_seen, r.records_created,
                               r.records_updated, r.records_failed, r.status
                          FROM public.source_sync_runs r
                         WHERE r.source_id = s.id
                         ORDER BY r.started_at DESC
                         LIMIT 1) run ON true;
COMMENT ON VIEW public.v_source_overview IS
  'One row per source for the app''s sources pages: what it is, its terms, records and how they were matched, and its latest run. Counts as of counts_as_of (refresh_company_profile()); the run is live.';

CREATE VIEW public.v_source_runs AS
 SELECT s.key AS source_key, r.started_at, r.finished_at, r.status, r.sync_type,
        r.records_seen, r.records_created, r.records_updated, r.records_failed,
        -- A run still 'running' while a later one started was interrupted (as in v_source_overview).
        (r.status = 'running' AND EXISTS (SELECT 1 FROM public.source_sync_runs n
                                           WHERE n.source_id = r.source_id AND n.started_at > r.started_at)) AS interrupted
   FROM public.sources s
     CROSS JOIN LATERAL (SELECT * FROM public.source_sync_runs r
                          WHERE r.source_id = s.id ORDER BY r.started_at DESC LIMIT 20) r;
COMMENT ON VIEW public.v_source_runs IS
  'The last 20 runs of every source: when, status and counts. Never the error message.';

GRANT SELECT, INSERT, DELETE, UPDATE ON public.source_field_stats_mv, public.source_stats_mv TO ${writer_role};
GRANT SELECT ON public.source_field_stats_mv, public.source_stats_mv TO ${reader_role};
GRANT SELECT ON public.v_source_fields, public.v_source_overview, public.v_source_runs TO ${writer_role}, ${reader_role};
