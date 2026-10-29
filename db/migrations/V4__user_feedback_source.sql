-- People's corrections from the app become a source (issue #45).
--
-- Priority 50, ahead of every other source (100), decided 2026-10-29. Register values carry
-- confidence 1.0 and verified websites up to 0.99; at equal priority a correction could never
-- replace a wrong name, address or website. One person's correction (0.30) stays below the
-- coordinator's bars (0.50, websites 0.35) and is not shown, unless no other source has a value for
-- the field (then 0.60); from two people who agree (0.60) it wins. The source's own value stays
-- stored and wins again when people withdraw their votes. The DAG `feedback_observations` writes it.
INSERT INTO public.sources (key, name, source_type, priority, metadata)
VALUES (
  'user_feedback',
  'Corrections from the app',
  'user_feedback',
  50,
  '{"dag_id": "feedback_observations",
    "confidence_by_net_agreement": {"1": 0.30, "1 on an empty field": 0.60, "2": 0.60, "3+": 0.75}}'
)
ON CONFLICT (key) DO UPDATE SET priority = EXCLUDED.priority;
