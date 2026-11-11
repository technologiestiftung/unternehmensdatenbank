-- Start, rename and delete review rounds from the app, for a manager only (#190): the Keycloak realm role
-- and Hasura role `manager`, never `admin`, which Hasura treats as its superuser with no permission checks.
--
--   1. `review_round_requests`: a manager asks for a round in the app; the DAG `review_round_requests` draws it with
--      `review_round.draw` and writes back `drawn` and the round, or `failed` and why. The app never
--      talks to Airflow. No requester is stored.
--   2. Deleting a round takes back its votes. Without this, `ON DELETE CASCADE` removes the round and its
--      tasks before `copy_review_verdict` (V4) runs for the verdicts, so the trigger finds no task, returns,
--      and the votes stay in `extraction_feedback` (measured locally, 2026-11-11). A BEFORE DELETE trigger
--      deletes the round's verdicts first, while the round and its tasks still exist.
--   3. Grants: Hasura (reader) inserts requests, renames and deletes rounds; Hasura's role `manager` alone
--      may do so (hasura/metadata). The DAG (writer) reads and updates requests.

CREATE TABLE public.review_round_requests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    field_name text NOT NULL CHECK (field_name <> ''),
    kind text NOT NULL CHECK (kind IN ('shown', 'refused', 'gap')),
    source_key text,
    size integer NOT NULL DEFAULT 200 CHECK (size BETWEEN 1 AND 1000),
    mode text NOT NULL DEFAULT 'measure' CHECK (mode IN ('measure', 'teach')),
    title text,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'drawn', 'failed')),
    round_id uuid REFERENCES public.review_rounds(id) ON DELETE SET NULL,
    error text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    done_at timestamptz
);
CREATE INDEX review_round_requests_pending_idx ON public.review_round_requests (requested_at) WHERE status = 'pending';
COMMENT ON TABLE public.review_round_requests IS 'A round a manager asked for in the app (#190); the DAG review_round_requests draws it.';
COMMENT ON COLUMN public.review_round_requests.error IS 'Why the draw failed, e.g. "nothing to draw for ...". Never personal data.';

CREATE FUNCTION public.delete_review_round_verdicts() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    DELETE FROM public.review_verdicts
    WHERE task_id IN (SELECT id FROM public.review_tasks WHERE round_id = OLD.id);
    RETURN OLD;
END;
$$;

CREATE TRIGGER review_rounds_take_back_votes BEFORE DELETE ON public.review_rounds
    FOR EACH ROW EXECUTE FUNCTION public.delete_review_round_verdicts();

GRANT SELECT, INSERT ON TABLE public.review_round_requests TO ${reader_role};
GRANT SELECT, UPDATE ON TABLE public.review_round_requests TO ${writer_role};
GRANT UPDATE (title), DELETE ON TABLE public.review_rounds TO ${reader_role};
