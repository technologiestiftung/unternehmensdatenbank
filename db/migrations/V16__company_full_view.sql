-- V16: one row per company, everything we hold about it — the register export AND our findings.
--
-- WHY THIS EXISTS
--
-- The data is spread over four places by design, and that design is right: `companies` is the
-- answer, `company_field_observations` is the proof, `northdata_export` is the raw input, and
-- `company_managers` is a one-to-many. Reading it therefore needs four joins and knowledge of which
-- table owns which column, which is fine for a query and hopeless for anybody who just wants to
-- look at the data in a spreadsheet.
--
-- So this flattens all of it. `v_website_provenance` (V15) answers "where did this website come
-- from"; this answers "what do we know about this company, from anywhere".
--
-- EVERY COMPANY, INCLUDING THE DEAD ONES
--
-- Driven `FROM public.companies`, so all 14,443 rows appear: 6,298 `aktiv`, 7,673 `erloschen`,
-- 472 `in Liquidation`. That matters because the coverage question is only answerable with the
-- misses present — "51% of active companies have a website" cannot be computed from a table that
-- has already dropped the ones without. Filter on `register_status` when you want a slice.
--
-- What it therefore does NOT show: the 13 `northdata_export` rows whose `internal_id` matched no
-- company. None of them carries a website, so nothing is lost today, but a Berlin export matched on
-- a different key could leave more behind. Query `northdata_export WHERE company_id IS NULL` for
-- those; they have no company to hang on.
--
-- TWO SNAPSHOTS, ONE ROW
--
-- `northdata_export` is keyed `(source_file, internal_id)` precisely so two exports a week apart can
-- coexist — that is how "what did Northdata change" gets answered. A plain join would then return
-- one row per snapshot per company and silently double every count. The LATERAL below takes the
-- **most recently imported** snapshot per company instead, and exposes `export_source_file` so you
-- can see which one you got. To diff two snapshots, query `northdata_export` directly; this view is
-- deliberately the current-state answer.
--
-- THE PREFIXES ARE THE POINT
--
-- `export_*` is what the register aggregator asserts. Everything else is ours. They disagree often
-- enough that merging them into one column would destroy the only evidence of the disagreement:
-- `export_status` is English (`active`) where `register_status` is German (`aktiv`), and on 184
-- companies `export_website` and `website` name different hosts.
--
-- `website_verified` is FALSE only where the website came from the export, which has fetched
-- nothing. Filter on it, not on `website_confidence` — the export's flat 0.50 sits above several
-- genuinely verified findings. Measured 2026-09-07: 3,542 websites, 726 evidence-backed.

CREATE OR REPLACE VIEW public.v_company_full AS
SELECT
    -- ---------- identity, ours ----------
    c.id,
    c.company_name,
    c.normalized_name,
    c.legal_form,
    c.registration_number,
    c.register_status,                          -- aktiv / erloschen / in Liquidation
    c.established_at,
    c.business_purpose,
    c.address,
    c.postcode,
    c.city,
    c.district,
    c.locality,
    c.wz_code,
    c.data_quality_score,

    -- ---------- our findings ----------
    p.website_url,
    p.found_by_source                           AS website_source,
    p.method                                    AS website_method,
    p.verified                                  AS website_verified,
    p.website_confidence,
    p.imprint_url,
    p.why                                       AS website_why,
    p.resolved_at                               AS website_resolved_at,
    e.cluster,
    e.cluster_confidence,
    e.tech_field,
    e.tech_field_confidence,
    e.revenue,
    e.employee_count,
    m.manager_count,
    m.managers,

    -- ---------- what the register export asserts ----------
    n.source_file                               AS export_source_file,
    n.internal_id                               AS export_internal_id,
    n.name                                      AS export_name,
    n.status                                    AS export_status,       -- English, unlike ours
    n.register_id                               AS export_register_id,
    n.register_city                             AS export_register_city,
    n.eu_id                                     AS export_eu_id,
    n.vat_id                                    AS export_vat_id,
    n.street                                    AS export_street,
    n.postal_code                               AS export_postal_code,
    n.city                                      AS export_city,
    n.state                                     AS export_state,
    n.country                                   AS export_country,
    n.address_formatted                         AS export_address,
    n.lat                                       AS export_lat,
    n.lng                                       AS export_lng,
    n.website                                   AS export_website,
    n.email                                     AS export_email,
    n.phone                                     AS export_phone,
    n.fax                                       AS export_fax,
    n.nace                                      AS export_nace,
    n.nace2025                                  AS export_nace2025,
    n.wz                                        AS export_wz,
    n.wz2025                                    AS export_wz2025,
    n.subject                                   AS export_subject,
    n.north_data_url                            AS export_url,
    n.last_update                               AS export_last_update,

    -- ---------- the two useful comparisons ----------
    -- Whether the export knows a website at all, and whether it agrees with ours. The host compare
    -- drops scheme, `www.` and everything from the first `/`, `?` or `#` — cutting at `/` alone
    -- leaves a query string behind on the four export rows that carry one with no path.
    (n.website IS NOT NULL)                     AS export_has_website,
    CASE
        WHEN n.website IS NULL AND p.website_url IS NULL THEN 'both_blank'
        WHEN n.website IS NULL                           THEN 'ours_only'
        WHEN p.website_url IS NULL                       THEN 'export_only'
        WHEN lower(regexp_replace(regexp_replace(regexp_replace(n.website, '^https?://', ''), '^www\.', ''), '[/?#].*$', ''))
           = rtrim(lower(regexp_replace(regexp_replace(regexp_replace(p.website_url, '^https?://', ''), '^www\.', ''), '[/?#].*$', '')), '.')
             THEN 'same_host'
        ELSE 'different_host'
    END                                         AS website_agreement
FROM public.companies c
LEFT JOIN public.v_website_provenance p ON p.id = c.id
LEFT JOIN public.v_companies_enriched e ON e.id = c.id
-- Newest snapshot only — see "TWO SNAPSHOTS, ONE ROW" above.
LEFT JOIN LATERAL (
    SELECT nd.*
    FROM public.northdata_export nd
    WHERE nd.company_id = c.id
    ORDER BY nd.imported_at DESC, nd.source_file DESC
    LIMIT 1
) n ON true
-- Managers are one-to-many (7,167 rows over 1,014 companies), so they collapse to a count and a
-- semicolon-joined list rather than multiplying the rows of this view.
LEFT JOIN LATERAL (
    SELECT count(*)                                          AS manager_count,
           string_agg(DISTINCT cm.full_name, '; ' ORDER BY cm.full_name) AS managers
    FROM public.company_managers cm
    WHERE cm.company_id = c.id
) m ON true;

COMMENT ON VIEW public.v_company_full IS
    'One row per company (all 14,443, dead ones included) with our canonical findings, the raw '
    'Northdata export columns prefixed export_, and a website_agreement column comparing the two. '
    'Newest export snapshot only. Filter website_verified for evidence-backed URLs.';
