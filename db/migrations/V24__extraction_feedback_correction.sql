-- V24: a user who marks a value wrong may say what it should be.
--
-- A "wrong" vote tells us an extraction failed; a correction tells us what it should have found,
-- which is what training spaCy (and measuring recall) actually needs. Optional, so a quick ✗ still
-- costs one click. The app clears it when the vote changes to "correct".
--
-- A correction does NOT change the value shown or any canonical field. Whether user corrections
-- become an observation source of their own, at which priority, is a separate decision.
--
-- PERSONAL DATA: for `management` a correction is a person's name typed by an employee; it falls
-- under the same privacy review as V23.

ALTER TABLE public.extraction_feedback
  ADD COLUMN IF NOT EXISTS correction TEXT
  CHECK (correction IS NULL OR length(correction) BETWEEN 1 AND 500);

ALTER TABLE public.extraction_feedback
  ADD CONSTRAINT extraction_feedback_correction_only_when_wrong
  CHECK (correction IS NULL OR verdict = 'wrong');
