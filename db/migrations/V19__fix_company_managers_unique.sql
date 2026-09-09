-- V19: stop `company_managers` growing by ~1,014 rows every time the coordinator runs.
--
-- WHY. `resolver.resolve_ceo_contact` ends its INSERT with `ON CONFLICT DO NOTHING`, which reads
-- like protection and has never once fired. With no conflict target, that clause can only suppress
-- a conflict on a unique constraint that already exists, and this table has exactly one:
-- `company_managers_pkey` on `id`, whose default is `gen_random_uuid()`. A fresh UUID never
-- collides. So every run appended a full copy of every manager it could name.
--
-- The statement also has no batch and no "only what changed" filter, so it re-inserts for every
-- company with a qualifying `ceo_name_or_contact` observation, every time. That is why the growth
-- is a flat ~1,014 rows per run rather than proportional to anything that happened.
--
-- Measured before this migration: **9,195 rows for 1,047 distinct (company_id, full_name) pairs** —
-- 8,148 duplicates, and 8.8 copies of the average name. The worst have **21**, which is the number
-- of times the coordinator has run against this data.
--
-- Steps 3 and 5 of the unification each cost exactly one run, and step 7 would make runs automatic.
-- Hence fixing it now: an assertion in `tests/integration/test_ground_truth_discovery.py` has been
-- catching each increase, but a test can only report the bill, not decline it.

-- ---------------------------------------------------------------------------------------------
-- 1. Keep the FIRST row of each group and delete the rest.
--
-- Ordered by `created_at`, not by `id`. `id` is `gen_random_uuid()` — random, not monotonic — so
-- `MIN(id)` would keep an arbitrary copy and call it the original. `created_at` is the real
-- insertion order, with `id` as a deterministic tie-break for rows written inside one transaction.
--
-- Nothing is lost by choosing the earliest. Every copy is identical in every column the INSERT
-- sets: `role` is always the literal `'Geschäftsführer'`, `is_current` always true, `metadata`
-- always `'{}'`, and `email`/`phone` are never written by this path at all. The rows differ only in
-- `id`, `created_at` and `updated_at`.
-- ---------------------------------------------------------------------------------------------

DELETE FROM public.company_managers
WHERE id NOT IN (
  SELECT DISTINCT ON (company_id, full_name) id
  FROM public.company_managers
  ORDER BY company_id, full_name, created_at, id
);

-- ---------------------------------------------------------------------------------------------
-- 2. The constraint that makes the existing `ON CONFLICT` clause mean something.
--
-- `(company_id, full_name)` and not `(company_id)`: a company may legitimately have several
-- managers with different names. The current writer never produces that — its `best` CTE is
-- `DISTINCT ON (o.company_id)`, so it names one person per company — but the table should not
-- forbid what the domain allows.
--
-- A CONSTRAINT rather than a bare unique index, because `resolve_ceo_contact` now names it:
-- `ON CONFLICT ON CONSTRAINT uq_company_managers_company_name DO NOTHING`. Naming it means a future
-- migration that drops the constraint breaks the INSERT loudly instead of silently restoring the
-- duplication.
-- ---------------------------------------------------------------------------------------------

ALTER TABLE public.company_managers
  ADD CONSTRAINT uq_company_managers_company_name UNIQUE (company_id, full_name);

COMMENT ON CONSTRAINT uq_company_managers_company_name ON public.company_managers IS
    'What makes resolve_ceo_contact idempotent. Before it existed, ON CONFLICT DO NOTHING had no '
    'target to conflict on and every coordinator run appended ~1,014 duplicate rows: 9,195 rows '
    'for 1,047 distinct names, worst case 21 copies. Do not drop this without also changing '
    'pipeline_coordinator/resolver.py, which names it explicitly.';
