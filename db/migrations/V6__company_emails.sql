-- Every stored email of a company, for the app's company page (research phase, decided 2026-11-07).
--
-- `email` stays stored, never resolved: no coordinator, no `company_field_history`, no
-- `v_field_provenance`, no claims list. This view is the one door, so closing it is one DROP VIEW plus
-- its Hasura entry. All emails are listed, also when a mailbox might belong to a person (a sole
-- trader's `info@`). Before production or public use this needs a privacy review.

CREATE VIEW public.v_company_emails AS
SELECT DISTINCT ON (o.company_id, lower(o.value_text))
       o.company_id,
       o.value_text AS email,
       s.name AS source,
       o.observed_at
  FROM public.company_field_observations o
  JOIN public.sources s ON s.id = o.source_id
 WHERE o.field_name = 'email'
   AND o.value_text IS NOT NULL
 ORDER BY o.company_id, lower(o.value_text), o.observed_at DESC;

COMMENT ON VIEW public.v_company_emails IS 'Every stored email per company, newest read per address. Shown on the app''s company page for research (2026-11-07); email is still never resolved. Remove: drop this view and its Hasura entry.';

GRANT SELECT ON TABLE public.v_company_emails TO ${reader_role};
