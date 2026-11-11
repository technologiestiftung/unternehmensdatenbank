# Review queue (#153)

A person judges about 200 results of one source and one field in the app ("Prüfen", app issue
technologiestiftung/unternehmensdaten-app#30). The verdicts go back into the data: they correct
values, they train models, and they show which rule makes which mistakes. Like a captcha: people
label, the machine learns.

**A research-phase tool.** It is built to be removed once trained models take over, and it can be
switched off without a deploy (`review_settings.enabled`).

## How it works

```
draw a round  ──►  review_tasks  ──►  a person judges in the app  ──►  review_verdicts
(review_round.draw)                                                      │ trigger
                                                                         ▼
                                    extraction_feedback (the app's votes)
                                     ├─► feedback_observations: corrections become `user_feedback`
                                     └─► technology_field_train: training labels and the held-out test
```

- **A round** is one draw: source × field × kind × mode.
  - `shown`: values the app shows. Answers: how many shown values are wrong.
  - `refused`: values below the bar or refused, e.g. `candidate_url` at 0.34. Answers: how much we
    throw away, and where the bar belongs.
  - `gap`: companies without a value; the person enters it. Answers: what we miss. For
    `technology_field` these are the companies the keywords left empty on a website that was read,
    with the LLM's label (#87) as a suggestion.
  - Mode `measure` is a random draw, the only kind that gives a precision. Mode `teach` draws the
    values whose confidence is closest to the bar (websites 0.35, everything else 0.50): the cases a
    rule or model is least sure of, where a verdict teaches the most. Technologiefeld, WZ estimate,
    DeepTech and KI are shown from any guess above 0; for them 0.50 is the edge between a red and a
    yellow guess in the app, where it is most open whether a guess is right (decided 2026-11-09).
    Only for `shown` and `refused` (a gap has no confidence), never for `email` or `phone` (never
    resolved, so no bar), never counted in a precision.
    Its verdicts still become votes, so a model judged on held-out votes also sees these harder
    cases: its score can only come out lower.
- **A task** keeps the value, its confidence, evidence flags and method as they were when drawn.
  A round draws at most one task per company: `feedback_observations` keeps one vote per person,
  company and field, so a second value of the same company would overwrite the first one's verdict.
- **A verdict** (one per task, one person per round): `correct`, `wrong` (with a reason and the right
  value) or `unsure`. A trigger copies `correct`/`wrong` into `extraction_feedback` as that person's
  vote, under the round's feedback field. `unsure` or a deleted verdict takes back only a vote the
  queue wrote (`created_vote`): a vote the person cast in the app gets back the answer the verdict
  overwrote (`prior_verdict`, `prior_correction`). If another verdict of the
  same person on the same company and value still says `correct`/`wrong` (a second round drew it),
  the vote follows that verdict instead of disappearing. While a queue verdict stands, the queue
  owns that vote: an edit made in the app meanwhile is not restored when the verdicts are taken
  back; the vote returns to the answer from before the queue.
- **Email and phone** are shown in the queue (decided for the research phase, 2026-11-05) but have no
  feedback field: their verdicts are counted, never turned into a correction (they are stored, never
  resolved). Because nothing is resolved, the app shows every stored value: a `shown` round draws
  from all of them (`NEVER_RESOLVED` in `draw.py`), and `draw.py` rejects a `refused` round for them.

## Draw a round

**In the app** (#190), for every logged-in person (no separate role yet, decided 2026-11-11): the form "Neue Runde" on
`/pruefen` writes a request into `review_round_requests`. The DAG `review_round_requests` draws every
pending request within 5 minutes with the same `review_round.draw`, and writes back the round or why it
failed ("nothing to draw for ..."), which the page shows. The app never talks to Airflow. Anyone also
renames a round (its title only) and deletes one.

**Deleting a round takes back its votes**, as if each verdict were set to "unsure": a vote the round
created is removed, an app vote it overwrote gets its earlier answer back. A trigger (V11) deletes the
verdicts before the round; without it the cascade removed the round first and every vote stayed
(measured locally, 2026-11-11).

Why the role is never called `admin`: [release](release.md), "Keycloak roles"; a test
(`tests/unit/test_review_round_requests.py`) fails on one.

**In Airflow** (local and play): trigger the DAG `review_round` with "Trigger DAG w/ config" and fill in
the form: field, kind, source (for `shown`/`refused`), size (at most 1,000), mode, and optionally the
version and a title. One run draws one round; it is never retried, since a retry would draw a second
one. The run's log names the round.

**From a shell**, the same with arguments:

```bash
PYTHONPATH=airflow/dags PG_DSN="host=localhost port=5433 dbname=unternehmensdatenbank user=writer password=writer" \
  python -m review_round.draw --field technology_field --kind gap --size 150
python -m review_round.draw --source branch_classifier --field technology_field --kind shown
python -m review_round.draw --source website_discovery --field candidate_url --kind refused
python -m review_round.draw --source website_discovery --field candidate_url --kind refused --mode teach
```

On play, in the pipelines image: `docker run --rm --entrypoint python --network unternehmensdatenbank
--env-file <PG_DSN> ghcr.io/technologiestiftung/unternehmensdatenbank-pipelines:main -m review_round.draw ...`

**For the Technologiefeld model:** `technology_field_train` keeps a fixed fifth of the judged companies
aside as its test and needs 30 of them. So a `gap` round of about 150 companies is what lets the first
model be put into use.

## Results

- `v_review_round_results`: progress and precision per round (precision only for `measure` rounds of
  kind `shown`/`refused`), and how often the suggestion was taken.
- `v_review_breakdown`: per round, wrong answers by reason, and correct/wrong by confidence band
  (0.1 wide) and by evidence flag. That is what tells which rule to change.

Counts only, never who judged what.

## Removal

1. Export the verdicts once (training data).
2. One migration drops the triggers and their functions, the two views and the five tables (with `review_round_requests`, V11).
3. Delete the Hasura metadata (`review_*`, `v_review_round_results`, `v_review_breakdown`), restart Hasura.
4. Delete `airflow/dags/review_round/`, `airflow/dags/review_round_requests/` and the app's `/pruefen` routes.

What was copied into `extraction_feedback` stays: it is ordinary feedback.
