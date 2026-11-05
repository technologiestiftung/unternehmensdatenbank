-- Review queue (#153): a person judges ~200 results of one source and field, and the verdicts flow
-- back into the collection. A research-phase tool, built to be removed (see "Removal" at the end).
--
-- A **round** is one draw: source x field x kind (`shown`: values the app shows; `refused`: values
-- below the bar or refused; `gap`: companies without a value, where the person enters it) x mode
-- (`measure`: a random draw, the only one that gives a precision; `teach`: the cases a model is least
-- sure of, never counted in a precision). A **task** is one company's value in a round, with the
-- observation's confidence and evidence frozen at drawing time. A **verdict** is the one answer to a
-- task (one person checks a round, decided 2026-11-05): correct, wrong or unsure, a reason, and the
-- right value.
--
-- **Back into the loop:** a correct/wrong verdict is copied into `extraction_feedback`, the app's
-- votes, under the round's `feedback_field`. That is what `feedback_observations` (corrections as
-- `user_feedback`) and `technology_field_train` already read, so neither changes. A round without a
-- `feedback_field` (email, phone: stored, never resolved) is counted here only.
--
-- **Switch:** `review_settings.enabled`. Hasura shows rounds and tasks only while it is true, and the
-- app reads it to show the page, so the feature is turned off without a deploy.
--
-- **Removal:** export the verdicts (training data), then one migration drops the trigger, the view
-- and the four tables. What was copied into `extraction_feedback` stays: it is ordinary feedback.

CREATE TABLE public.review_settings (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    enabled boolean NOT NULL DEFAULT true
);
INSERT INTO public.review_settings DEFAULT VALUES;
COMMENT ON TABLE public.review_settings IS 'One row: is the review queue (#153) on. Hasura and the app read it.';

CREATE TABLE public.review_rounds (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_key text,
    field_name text NOT NULL,
    feedback_field text,
    kind text NOT NULL CHECK (kind IN ('shown', 'refused', 'gap')),
    mode text NOT NULL DEFAULT 'measure' CHECK (mode IN ('measure', 'teach')),
    version text,
    title text NOT NULL,
    size integer NOT NULL CHECK (size > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    CHECK (kind = 'gap' OR source_key IS NOT NULL)
);
COMMENT ON TABLE public.review_rounds IS 'One draw of the review queue (#153): source x field x kind x mode.';
COMMENT ON COLUMN public.review_rounds.field_name IS 'The observation field drawn from, e.g. technology_field, candidate_url.';
COMMENT ON COLUMN public.review_rounds.feedback_field IS 'The extraction_feedback field a verdict is copied to; NULL = counted here only (email, phone).';
COMMENT ON COLUMN public.review_rounds.version IS 'The code or model version the drawn values came from, for before/after.';

CREATE TABLE public.review_tasks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    round_id uuid NOT NULL REFERENCES public.review_rounds(id) ON DELETE CASCADE,
    position integer NOT NULL,
    company_id uuid NOT NULL REFERENCES public.companies(id) ON DELETE CASCADE,
    observation_id uuid REFERENCES public.company_field_observations(id) ON DELETE SET NULL,
    value text NOT NULL DEFAULT '' CHECK (length(value) <= 500),
    confidence real,
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    method text,
    suggestion text CHECK (suggestion IS NULL OR length(suggestion) <= 500),
    UNIQUE (round_id, position),
    UNIQUE (round_id, company_id, value)
);
CREATE INDEX review_tasks_company_idx ON public.review_tasks (company_id);
COMMENT ON TABLE public.review_tasks IS 'One value to judge in a review round (#153); value is empty for a gap round.';
COMMENT ON COLUMN public.review_tasks.suggestion IS 'A proposal shown next to a gap (e.g. the LLM label, #87); taking it is a correction like any other.';

CREATE TABLE public.review_verdicts (
    task_id uuid PRIMARY KEY REFERENCES public.review_tasks(id) ON DELETE CASCADE,
    verdict text NOT NULL CHECK (verdict IN ('correct', 'wrong', 'unsure')),
    reason text CHECK (reason IS NULL OR length(reason) BETWEEN 1 AND 100),
    correction text CHECK (correction IS NULL OR length(correction) BETWEEN 1 AND 500),
    author_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_vote boolean NOT NULL DEFAULT false,
    CHECK (correction IS NULL OR verdict = 'wrong'),
    CHECK (reason IS NULL OR verdict = 'wrong')
);
COMMENT ON TABLE public.review_verdicts IS 'The one answer to a review task (#153). Copied into extraction_feedback by a trigger.';
COMMENT ON COLUMN public.review_verdicts.created_vote IS 'Set by the trigger: this verdict wrote the extraction_feedback vote, so only it may take the vote back.';

CREATE TRIGGER review_verdicts_touch_updated_at BEFORE UPDATE ON public.review_verdicts
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();

-- A verdict becomes the person's vote on the same value in extraction_feedback, so corrections and
-- training read it without knowing about the queue. A gap task has no value: only a correction can be
-- copied (the table requires one for an empty value). `unsure` and a deleted verdict take back only a
-- vote the queue wrote (`created_vote`): a vote cast in the app stays. If another verdict of the same
-- person on the same value still says correct/wrong, the vote follows that one instead.
CREATE FUNCTION public.copy_review_verdict() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v public.review_verdicts;
    t public.review_tasks;
    other public.review_verdicts;
    target text;
    created boolean := false;
BEGIN
    IF TG_OP = 'UPDATE' AND pg_trigger_depth() > 1 THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN v := OLD; ELSE v := NEW; END IF;
    IF TG_OP <> 'INSERT' THEN created := OLD.created_vote; END IF;
    SELECT * INTO t FROM public.review_tasks WHERE id = v.task_id;
    SELECT feedback_field INTO target FROM public.review_rounds WHERE id = t.round_id;
    IF target IS NULL THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' OR NEW.verdict = 'unsure' OR (t.value = '' AND NEW.correction IS NULL) THEN
        SELECT o.* INTO other
        FROM public.review_verdicts o
        JOIN public.review_tasks ot ON ot.id = o.task_id
        JOIN public.review_rounds r ON r.id = ot.round_id
        WHERE o.task_id <> v.task_id AND o.author_id = v.author_id AND ot.company_id = t.company_id
          AND ot.value = t.value AND r.feedback_field = target AND o.verdict IN ('correct', 'wrong')
          AND (t.value <> '' OR o.correction IS NOT NULL)
        ORDER BY o.updated_at DESC
        LIMIT 1;
        IF FOUND AND TG_OP <> 'INSERT' THEN
            UPDATE public.extraction_feedback
            SET verdict = CASE WHEN t.value = '' THEN 'wrong' ELSE other.verdict END, correction = other.correction
            WHERE company_id = t.company_id AND field_name = target AND value = t.value AND author_id = v.author_id;
            IF created THEN
                UPDATE public.review_verdicts SET created_vote = true WHERE task_id = other.task_id;
            END IF;
        ELSIF created THEN
            DELETE FROM public.extraction_feedback
            WHERE company_id = t.company_id AND field_name = target AND value = t.value AND author_id = v.author_id;
        END IF;
        created := false;
    ELSE
        created := created OR NOT EXISTS (
            SELECT 1 FROM public.extraction_feedback
            WHERE company_id = t.company_id AND field_name = target AND value = t.value AND author_id = v.author_id);
        INSERT INTO public.extraction_feedback (company_id, field_name, value, verdict, correction, author_id)
        VALUES (t.company_id, target, t.value, CASE WHEN t.value = '' THEN 'wrong' ELSE v.verdict END,
                v.correction, v.author_id)
        ON CONFLICT (company_id, field_name, value, author_id)
        DO UPDATE SET verdict = EXCLUDED.verdict, correction = EXCLUDED.correction;
    END IF;
    IF TG_OP <> 'DELETE' AND created IS DISTINCT FROM v.created_vote THEN
        UPDATE public.review_verdicts SET created_vote = created WHERE task_id = v.task_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER review_verdicts_copy AFTER INSERT OR UPDATE OR DELETE ON public.review_verdicts
    FOR EACH ROW EXECUTE FUNCTION public.copy_review_verdict();

-- Counts only. Precision only for `measure` rounds of kind shown/refused: a `teach` draw picks the hard
-- cases on purpose, and a gap round has no value to be right or wrong.
CREATE VIEW public.v_review_round_results AS
SELECT r.id AS round_id,
       count(t.id) AS tasks,
       count(v.task_id) AS judged,
       count(*) FILTER (WHERE v.verdict = 'correct') AS correct,
       count(*) FILTER (WHERE v.verdict = 'wrong') AS wrong,
       count(*) FILTER (WHERE v.verdict = 'unsure') AS unsure,
       count(*) FILTER (WHERE v.correction IS NOT NULL) AS corrected,
       count(*) FILTER (WHERE t.suggestion IS NOT NULL AND v.verdict IN ('correct', 'wrong')) AS suggestion_judged,
       count(*) FILTER (WHERE t.suggestion IS NOT NULL AND v.correction = t.suggestion) AS suggestion_taken,
       CASE WHEN r.mode = 'measure' AND r.kind <> 'gap' THEN
           round((count(*) FILTER (WHERE v.verdict = 'correct'))::numeric
                 / NULLIF(count(*) FILTER (WHERE v.verdict IN ('correct', 'wrong')), 0), 3)
       END AS precision
FROM public.review_rounds r
LEFT JOIN public.review_tasks t ON t.round_id = r.id
LEFT JOIN public.review_verdicts v ON v.task_id = t.id
GROUP BY r.id;
COMMENT ON VIEW public.v_review_round_results IS 'Progress and precision per review round (#153). Counts only.';

-- What a rule change needs: which reason, which confidence band, which evidence flag is wrong how often.
CREATE VIEW public.v_review_breakdown AS
WITH judged AS (
    SELECT t.round_id, t.confidence, t.evidence, v.verdict, v.reason
    FROM public.review_tasks t
    JOIN public.review_verdicts v ON v.task_id = t.id
    WHERE v.verdict IN ('correct', 'wrong')
)
SELECT round_id, 'reason' AS dimension, reason AS bucket, 0 AS correct, count(*) AS wrong
FROM judged WHERE verdict = 'wrong' GROUP BY round_id, reason
UNION ALL
SELECT round_id, 'confidence', to_char(floor(confidence * 10) / 10, 'FM0.0'),
       count(*) FILTER (WHERE verdict = 'correct'), count(*) FILTER (WHERE verdict = 'wrong')
FROM judged WHERE confidence IS NOT NULL GROUP BY round_id, floor(confidence * 10)
UNION ALL
SELECT round_id, 'evidence', flag,
       count(*) FILTER (WHERE verdict = 'correct'), count(*) FILTER (WHERE verdict = 'wrong')
FROM judged, jsonb_array_elements_text(evidence) AS flag GROUP BY round_id, flag;
COMMENT ON VIEW public.v_review_breakdown IS 'Review verdicts per round by reason, confidence band (0.1 wide) and evidence flag (#153). Counts only.';

GRANT SELECT ON TABLE public.review_settings TO ${writer_role};
GRANT SELECT ON TABLE public.review_settings TO ${reader_role};
GRANT SELECT, INSERT, UPDATE ON TABLE public.review_rounds TO ${writer_role};
GRANT SELECT ON TABLE public.review_rounds TO ${reader_role};
GRANT SELECT, INSERT ON TABLE public.review_tasks TO ${writer_role};
GRANT SELECT ON TABLE public.review_tasks TO ${reader_role};
GRANT SELECT ON TABLE public.review_verdicts TO ${writer_role};
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.review_verdicts TO ${reader_role};
GRANT SELECT ON public.v_review_round_results TO ${writer_role};
GRANT SELECT ON public.v_review_round_results TO ${reader_role};
GRANT SELECT ON public.v_review_breakdown TO ${writer_role};
GRANT SELECT ON public.v_review_breakdown TO ${reader_role};
