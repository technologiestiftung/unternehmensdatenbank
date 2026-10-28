-- A user can take back their own vote.
--
-- In the app, clicking your active ✓ or ✗ again removes the vote and its correction. Hasura
-- connects as the reader login, which until now could only insert and update votes. Hasura's
-- delete permission (hasura/metadata/.../public_extraction_feedback.yaml) limits the delete to rows
-- whose author_id is the signed-in user, so nobody can remove somebody else's vote.
GRANT DELETE ON public.extraction_feedback TO ${reader_role};
