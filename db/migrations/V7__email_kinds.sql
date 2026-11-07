-- Every email with its kind and confidence (research phase, decided 2026-11-07).
--
-- `website_contacts` now keeps every address on the Impressum, not only the company's role mailbox,
-- and labels each: `role` (own domain, not shaped like a person), `person` (own domain, shaped like a
-- person) or `other_domain`. The label is the observation's `metadata.method`, and each kind has its
-- own confidence (`website_contacts.main.EMAIL_CONFIDENCE`), guessed until Prüfen rounds measure it.
-- `v_company_emails` (V6) gains both, so the app can show them. A view cannot gain columns in the
-- middle, so it is dropped and created again; Hasura tracks it by name and picks the columns up on restart.

DROP VIEW public.v_company_emails;

CREATE VIEW public.v_company_emails AS
SELECT DISTINCT ON (o.company_id, lower(o.value_text))
       o.company_id,
       o.value_text AS email,
       s.name AS source,
       o.metadata ->> 'method' AS kind,
       o.confidence,
       o.observed_at
  FROM public.company_field_observations o
  JOIN public.sources s ON s.id = o.source_id
 WHERE o.field_name = 'email'
   AND o.value_text IS NOT NULL
 ORDER BY o.company_id, lower(o.value_text), o.confidence DESC NULLS LAST, o.observed_at DESC, s.name;

COMMENT ON VIEW public.v_company_emails IS 'Every stored email per company, the most confident read per address, with its kind (role, person, other_domain; NULL before V7) and confidence. Shown on the app''s company page for research (2026-11-07); email is still never resolved. Remove: drop this view and its Hasura entry.';

GRANT SELECT ON TABLE public.v_company_emails TO ${reader_role};
