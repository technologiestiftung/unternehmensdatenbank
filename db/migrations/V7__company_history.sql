-- What a company's shown data was, and when it changed (issue #54).
--
-- Every claim was already kept (company_field_observations), but not what was SHOWN when:
-- company_canonical_field_sources holds only the current winner, and a re-score overwrites an
-- observation's confidence in place. company_field_history is append-only: one row whenever a shown
-- value appears, is replaced, keeps its value with a new confidence or source, or disappears.
-- pipeline_coordinator writes it with record_company_history(), comparing what is shown now with
-- the latest row per field, so every rule that picks a winner (priority, age, corrections) is covered.
--
-- PERSONAL DATA: manager names stay here after they are no longer shown (a former Geschäftsführer).
-- Included as decided on 2026-10-30, flagged for the privacy review (#54).
-- v_company_claims deliberately lists every claim to every logged-in user, also manager names that
-- were never shown and corrections below the bar (one person's, or withdrawn). Decided on
-- 2026-10-30, FLAGGED FOR THE PRIVACY REVIEW (#54).

CREATE TABLE public.company_field_history (
    id bigserial PRIMARY KEY,
    company_id uuid NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    -- The coordinator's field name (website, name, technology_field, ...); 'management' per name.
    field_name text NOT NULL,
    event text NOT NULL CHECK (event IN ('shown', 'replaced', 'confidence_changed', 'removed')),
    value text,
    previous_value text,
    source_id uuid REFERENCES public.sources(id) ON DELETE SET NULL,
    -- No foreign key: the history outlives the observation it points to.
    observation_id uuid,
    confidence numeric(5,4),
    previous_confidence numeric(5,4),
    changed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX company_field_history_latest_idx
    ON public.company_field_history (company_id, field_name, changed_at DESC, id DESC);
CREATE INDEX company_field_history_management_idx
    ON public.company_field_history (company_id, value, changed_at DESC, id DESC)
    WHERE field_name = 'management';
COMMENT ON TABLE public.company_field_history IS
  'Append-only history of every shown value per company: shown, replaced, confidence_changed, removed. Written by record_company_history() at the end of each pipeline_coordinator run. Never updated or deleted.';

CREATE FUNCTION public.record_company_history() RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  written integer;
  managers integer;
BEGIN
  -- Overlapping coordinator runs would both compare with the same latest row and write it twice.
  PERFORM pg_advisory_xact_lock(hashtext('record_company_history'));
  -- A change is what the app shows: value, source or confidence. A new observation id alone is not
  -- (scripts/import_register_managers.py re-inserts the same names), so nothing is recorded for it.
  -- One value per field: the canonical observation.
  WITH current AS (
    SELECT cfs.company_id, cfs.field_name,
           COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text) AS value,
           cfs.source_id, o.id AS observation_id, o.confidence
    FROM public.company_canonical_field_sources cfs
    JOIN public.company_field_observations o ON o.id = cfs.company_field_observation_id
  ),
  last AS (
    SELECT DISTINCT ON (h.company_id, h.field_name)
           h.company_id, h.field_name, h.event, h.value, h.source_id, h.observation_id, h.confidence
    FROM public.company_field_history h
    WHERE h.field_name <> 'management'
    ORDER BY h.company_id, h.field_name, h.changed_at DESC, h.id DESC
  )
  INSERT INTO public.company_field_history
    (company_id, field_name, event, value, previous_value, source_id, observation_id, confidence,
     previous_confidence)
  SELECT COALESCE(c.company_id, l.company_id), COALESCE(c.field_name, l.field_name),
         CASE WHEN c.company_id IS NULL THEN 'removed'
              WHEN l.company_id IS NULL OR l.event = 'removed' THEN 'shown'
              WHEN c.value IS DISTINCT FROM l.value THEN 'replaced'
              ELSE 'confidence_changed' END,
         c.value,
         CASE WHEN l.event = 'removed' THEN NULL ELSE l.value END,
         COALESCE(c.source_id, l.source_id), COALESCE(c.observation_id, l.observation_id),
         c.confidence,
         CASE WHEN l.event = 'removed' THEN NULL ELSE l.confidence END
  FROM current c
  FULL JOIN last l ON l.company_id = c.company_id AND l.field_name = c.field_name
  WHERE (c.company_id IS NULL AND l.event <> 'removed')
     OR (c.company_id IS NOT NULL AND (
           l.company_id IS NULL OR l.event = 'removed'
           OR c.value IS DISTINCT FROM l.value
           OR c.source_id IS DISTINCT FROM l.source_id
           OR c.confidence IS DISTINCT FROM l.confidence));
  GET DIAGNOSTICS written = ROW_COUNT;

  -- Several values per field: every current manager name, one history line per name.
  WITH current AS (
    SELECT m.company_id, m.full_name AS value, m.source_id, o.id AS observation_id, o.confidence
    FROM public.company_managers m
    LEFT JOIN public.company_field_observations o ON o.id = m.company_field_observation_id
    WHERE m.is_current
  ),
  last AS (
    SELECT DISTINCT ON (h.company_id, h.value)
           h.company_id, h.value, h.event, h.source_id, h.observation_id, h.confidence
    FROM public.company_field_history h
    WHERE h.field_name = 'management'
    ORDER BY h.company_id, h.value, h.changed_at DESC, h.id DESC
  )
  INSERT INTO public.company_field_history
    (company_id, field_name, event, value, previous_value, source_id, observation_id, confidence,
     previous_confidence)
  SELECT COALESCE(c.company_id, l.company_id), 'management',
         CASE WHEN c.company_id IS NULL THEN 'removed'
              WHEN l.company_id IS NULL OR l.event = 'removed' THEN 'shown'
              ELSE 'confidence_changed' END,
         COALESCE(c.value, l.value),
         NULL,
         COALESCE(c.source_id, l.source_id), COALESCE(c.observation_id, l.observation_id),
         c.confidence,
         CASE WHEN l.event = 'removed' THEN NULL ELSE l.confidence END
  FROM current c
  FULL JOIN last l ON l.company_id = c.company_id AND l.value = c.value
  WHERE (c.company_id IS NULL AND l.event <> 'removed')
     OR (c.company_id IS NOT NULL AND (
           l.company_id IS NULL OR l.event = 'removed'
           OR c.source_id IS DISTINCT FROM l.source_id
           OR c.confidence IS DISTINCT FROM l.confidence));
  GET DIAGNOSTICS managers = ROW_COUNT;
  RETURN written + managers;
END
$$;

-- The baseline: what is shown now, dated when it was chosen. Earlier changes were never recorded.
INSERT INTO public.company_field_history
  (company_id, field_name, event, value, source_id, observation_id, confidence, changed_at)
SELECT cfs.company_id, cfs.field_name, 'shown',
       COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text),
       cfs.source_id, o.id, o.confidence, cfs.selected_at
FROM public.company_canonical_field_sources cfs
JOIN public.company_field_observations o ON o.id = cfs.company_field_observation_id;
INSERT INTO public.company_field_history
  (company_id, field_name, event, value, source_id, observation_id, confidence, changed_at)
SELECT m.company_id, 'management', 'shown', m.full_name, m.source_id, o.id, o.confidence, m.created_at
FROM public.company_managers m
LEFT JOIN public.company_field_observations o ON o.id = m.company_field_observation_id
WHERE m.is_current;

-- What the app reads. Field keys are the app's (management, deeptech), so it reuses its labels.
CREATE VIEW public.v_company_history AS
 SELECT h.id,
    h.company_id,
    CASE h.field_name WHEN 'deeptech_class' THEN 'deeptech' ELSE h.field_name END AS field_key,
    h.event,
    h.value,
    h.previous_value,
    h.confidence,
    h.previous_confidence,
    s.key AS source_key,
    s.name AS source_name,
    h.changed_at
   FROM public.company_field_history h
     LEFT JOIN public.sources s ON s.id = h.source_id;
COMMENT ON VIEW public.v_company_history IS
  'The shown-value history per company, for the app''s company page (#54). Newest first by changed_at.';

-- Every claim a source made, for the "all claims" toggle. Only fields the app may show: never email
-- or phone (stored, never resolved), never a _withheld field (the rules refused to publish it), no
-- internal bookkeeping (discovery_tier, osm_id, directory_url), and no person behind a correction.
-- Deliberately included (decided 2026-10-30, FLAGGED FOR THE PRIVACY REVIEW, #54): manager names
-- that were never shown, and corrections below the bar (one person's, or withdrawn).
CREATE VIEW public.v_company_claims AS
 SELECT o.id,
    o.company_id,
    CASE o.field_name
      WHEN 'ceo_name_or_contact' THEN 'management'
      WHEN 'candidate_url' THEN 'website'
      WHEN 'deeptech_class' THEN 'deeptech'
      ELSE o.field_name END AS field_key,
    COALESCE(o.value_text, (o.value_number)::text, (o.value_date)::text) AS value,
    o.confidence,
    o.is_canonical OR EXISTS (
      SELECT 1 FROM public.company_managers m
      WHERE m.company_field_observation_id = o.id AND m.is_current) AS is_shown,
    COALESCE((o.metadata ->> 'verified_by'), (o.metadata ->> 'decided_by')) AS method,
    s.key AS source_key,
    s.name AS source_name,
    COALESCE(o.source_updated_at, o.observed_at) AS read_at
   FROM public.company_field_observations o
     JOIN public.sources s ON s.id = o.source_id
  WHERE o.field_name IN (
    'name', 'legal_form', 'address', 'postcode', 'city', 'register_status', 'established_at',
    'business_purpose', 'wz_code', 'website', 'candidate_url', 'technology_field', 'purpose_field',
    'activity_keywords', 'register_announcement', 'ceo_name_or_contact', 'employee_count', 'revenue',
    'deeptech_class', 'is_ai');
COMMENT ON VIEW public.v_company_claims IS
  'Every claim any source made about a company, shown or not, for the app''s "all claims" toggle (#54). Excludes email, phone, _withheld fields and bookkeeping by a field allow-list. Deliberately includes manager names never shown and corrections below the bar (one person''s, or withdrawn), visible to every logged-in user: flagged for the privacy review (#54).';

GRANT SELECT, INSERT ON public.company_field_history TO ${writer_role};
GRANT USAGE ON SEQUENCE public.company_field_history_id_seq TO ${writer_role};
GRANT SELECT ON public.company_field_history, public.v_company_history, public.v_company_claims TO ${reader_role};
GRANT SELECT ON public.v_company_history, public.v_company_claims TO ${writer_role};
