-- V15: one view answering "where did this company's website come from, and is it evidence-backed?"
--
-- WHY THIS EXISTS
--
-- `v_companies_enriched` already serves the website, its source key, its confidence and the
-- register status. What it cannot serve is the METHOD -- whether a URL was proved by a register
-- number on the page, guessed from the company name, or simply copied out of a third-party
-- register export. That distinction became load-bearing on 2026-09-03, when importing the
-- Northdata export took website coverage from 1,136 companies to 3,511 and 2,816 of those are
-- URLs nobody has ever fetched. "How many websites do we have" stopped being one number that day.
--
-- The method is not lost, it is just awkward to reach: the resolver writes '{}' into
-- `company_canonical_field_sources.metadata` and each source records its own evidence in the
-- OBSERVATION's metadata. So the answer needs a two-hop join every time somebody asks, which is
-- how a query gets copied slightly wrong into six places.
--
-- WHAT `verified` MEANS, AND WHY IT IS NOT `confidence`
--
-- `verified` is TRUE when some page was actually read and something on it tied the page to this
-- company. It is FALSE only for the register export, which asserts a URL and offers no evidence
-- at all. Confidence cannot answer this: the export's rows carry a flat 0.50, which sits ABOVE
-- several genuinely verified discovery observations, so ordering by confidence mixes the two kinds
-- back together. Filter on `verified` when the question is "what can we defend", on `confidence`
-- when the question is "how sure was the finder".
--
-- WHAT THIS VIEW DELIBERATELY DOES NOT DO
--
-- No ORDER BY. Postgres permits one in a view definition and then discards it whenever the caller
-- adds their own, so it buys nothing and misleads the next reader into thinking the order is a
-- guarantee.
--
-- No `WHERE website_url IS NOT NULL`. A company with no website is the most interesting row in a
-- coverage question -- "50.6% of active companies have one" is unanswerable from a view that has
-- already dropped the other half. Callers who want only the found ones add the predicate.
--
-- No join to `public.companies`. `company_name`, `registration_number` and `register_status` are
-- all columns on `v_companies_enriched` already; re-joining the base table to fetch them costs a
-- join and invites the two copies to drift.

CREATE OR REPLACE VIEW public.v_website_provenance AS
SELECT
    v.id,
    v.company_name,
    v.registration_number,
    v.register_status,                                  -- aktiv / erloschen / in Liquidation
    v.website_url,
    v.website_source                        AS found_by_source,
    v.website_confidence,
    -- Every source records `verified_by` except the register export, which has nothing to record.
    -- Two brave_search_discovery rows predate that key and stay NULL rather than being guessed at.
    COALESCE(
        obs.metadata ->> 'verified_by',
        CASE WHEN v.website_source = 'northdata_export' THEN 'register_export' END
    )                                       AS method,
    -- Only combined_discovery agrees with itself across several methods and records the list; for
    -- every other source this is NULL, which is the honest answer rather than a one-element list.
    obs.metadata ->> 'methods'              AS methods_agreeing,
    -- TRUE unless the source said otherwise. Only the register export sets `verified` to false, so
    -- the default belongs on the side of the sources that did read a page.
    COALESCE((obs.metadata ->> 'verified')::boolean, v.website_url IS NOT NULL)
                                            AS verified,
    obs.metadata ->> 'imprint_url'          AS imprint_url,
    obs.metadata ->> 'why'                  AS why,
    obs.metadata                            AS evidence,
    ccfs.selected_at                        AS resolved_at
FROM public.v_companies_enriched v
LEFT JOIN public.company_canonical_field_sources ccfs
       ON ccfs.company_id = v.id
      AND ccfs.field_name = 'website'
LEFT JOIN public.company_field_observations obs
       ON obs.id = ccfs.company_field_observation_id;

COMMENT ON VIEW public.v_website_provenance IS
    'Every company with its canonical website, the source and METHOD that produced it, and whether '
    'any page was actually read to prove it. Companies with no website are included with NULLs. '
    'Filter on verified for defensible URLs; the register export supplies 2,816 unverified ones.';
