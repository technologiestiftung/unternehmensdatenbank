-- Fusionbase is not a source: whether we get it is open. Its identifier scheme leaves the allowed list;
-- no row used it. A vendor that comes later adds its own scheme in its own migration.
ALTER TABLE public.company_identifiers DROP CONSTRAINT company_identifiers_scheme_check;
ALTER TABLE public.company_identifiers ADD CONSTRAINT company_identifiers_scheme_check
    CHECK (scheme = ANY (ARRAY['register', 'register_former', 'opencorporates', 'vat', 'lei']));
