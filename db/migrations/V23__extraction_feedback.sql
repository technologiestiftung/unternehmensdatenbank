-- V23: users say whether an extracted value is right. The first hand labels this project has.
--
-- WHY
--
-- Not one row of any extracted or classified column has been hand-checked (CLAUDE.md, deferred
-- work 5), so there is no precision figure for anything, and spaCy cannot be trained on manager
-- names without examples someone has confirmed. Users of the app see these values every day; two
-- buttons next to a value ("stimmt" / "falsch") turn that into labels.
--
-- WHAT IS STORED
--
-- One vote per user per (company, field, value). The VALUE is stored, not just the field, because
-- `management` holds several names and each is judged on its own, and because the value can
-- change after a re-run: a vote is about the value the user saw, not about whatever the field
-- holds later. A user can change their vote (Hasura upsert on `extraction_feedback_one_vote`).
--
-- `field_name` is a closed list on purpose. Adding a field to the app's buttons is a decision, and
-- this CHECK makes it a visible one.
--
-- PERSONAL DATA
--
-- For `management` the value is a person's name, stored next to an employee's Keycloak id. That is
-- a new collection of personal data: get the privacy review before this goes live on the hub.

CREATE TABLE IF NOT EXISTS public.extraction_feedback (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
  field_name TEXT NOT NULL CHECK (field_name IN ('management', 'website', 'technologiefeld')),
  value TEXT NOT NULL CHECK (length(value) BETWEEN 1 AND 500),
  verdict TEXT NOT NULL CHECK (verdict IN ('correct', 'wrong')),
  author_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT extraction_feedback_one_vote UNIQUE (company_id, field_name, value, author_id)
);

CREATE INDEX IF NOT EXISTS extraction_feedback_field_idx
  ON public.extraction_feedback (field_name, verdict);

CREATE TRIGGER extraction_feedback_touch_updated_at
BEFORE UPDATE ON public.extraction_feedback
FOR EACH ROW
EXECUTE FUNCTION public.touch_updated_at();

COMMENT ON TABLE public.extraction_feedback IS
  'Users'' votes on extracted values (correct/wrong), one per user per company, field and value. '
  'The labels for measuring and training extraction. Contains personal data for field management. See V23.';

-- Hasura connects as the reader; votes are inserted and changed through it, nothing else.
GRANT INSERT, UPDATE ON public.extraction_feedback TO ${reader_role};
