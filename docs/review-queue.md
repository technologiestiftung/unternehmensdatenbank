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
  - Mode `measure` is a random draw, the only kind that gives a precision. `teach` (the cases a model
    is least sure of) is not built yet.
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
  from all of them (`NEVER_RESOLVED` in `draw.py`), and a `refused` round is refused.

## Draw a round

```bash
PYTHONPATH=airflow/dags PG_DSN="host=localhost port=5433 dbname=unternehmensdatenbank user=writer password=writer" \
  python -m review_round.draw --field technology_field --kind gap --size 150
python -m review_round.draw --source branch_classifier --field technology_field --kind shown
python -m review_round.draw --source website_discovery --field candidate_url --kind refused
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
2. One migration drops the trigger and its function, the two views and the four tables.
3. Delete the Hasura metadata (`review_*`, `v_review_round_results`, `v_review_breakdown`), restart Hasura.
4. Delete `airflow/dags/review_round/` and the app's `/pruefen` routes.

What was copied into `extraction_feedback` stays: it is ordinary feedback.
