#!/bin/sh
# Builds the migration image (db/Dockerfile) and runs it twice against an EMPTY Postgres 15 that
# was set up the way the hub sets one up: the init script creates the three logins, here under
# role names that are not ours, so a migration that hardcodes `writer` or `reader` fails.
#   1st run: V1 to the newest must all apply.   2nd run: must apply nothing.
# Then checks the grants: the writer may INSERT, the reader may not UPDATE. And the real taxonomy
# (#84): the free checks use fixtures, so what the tables must hold is checked here.
# Needs Docker only. Leaves nothing behind.
set -eu

net=udb-migration-test
pg=udb-migration-test-pg
cleanup() { docker rm -f "$pg" >/dev/null 2>&1 || true; docker network rm "$net" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker build -q -t udb-migrations:test db >/dev/null
docker build -q -t udb-postgres:test postgres >/dev/null
docker network create "$net" >/dev/null

docker run -d --name "$pg" --network "$net" \
  -e POSTGRES_PASSWORD=pw \
  -e AIRFLOW_DB_USER=airflow -e AIRFLOW_DB_PASSWORD=pw -e AIRFLOW_DB_NAME=airflow \
  -e APP_DB_USER=hub_owner -e APP_DB_PASSWORD=pw \
  -e APP_DB_WRITER_USER=hub_writer -e APP_DB_WRITER_PASSWORD=pw \
  -e APP_DB_READER_USER=hub_reader -e APP_DB_READER_PASSWORD=pw \
  -e APP_DB_NAME=udb \
  -e KEYCLOAK_DB_USER=keycloak -e KEYCLOAK_DB_PASSWORD=pw -e KEYCLOAK_DB_NAME=keycloak \
  -e HASURA_DB_USER=hasura -e HASURA_DB_PASSWORD=pw -e HASURA_DB_NAME=hasura \
  -v "$PWD/postgres/init:/docker-entrypoint-initdb.d:ro" \
  udb-postgres:test >/dev/null

# pg_isready turns true during the init phase too; the init script's last database is the signal.
ready=
for _ in $(seq 60); do
  docker exec "$pg" psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'hasura'" 2>/dev/null | grep -q 1 \
    && docker exec "$pg" pg_isready -U postgres -h localhost >/dev/null 2>&1 && ready=1 && break
  sleep 2
done
[ -n "$ready" ] || { echo "Postgres not ready after 120 s"; docker logs "$pg"; exit 1; }

migrate() {
  docker run --rm --network "$net" \
    -e FLYWAY_URL="jdbc:postgresql://$pg:5432/udb" \
    -e FLYWAY_USER=hub_owner -e FLYWAY_PASSWORD=pw \
    -e FLYWAY_PLACEHOLDERS_WRITER_ROLE=hub_writer \
    -e FLYWAY_PLACEHOLDERS_READER_ROLE=hub_reader \
    udb-migrations:test
}

expect() {
  out=$(migrate 2>&1) || { echo "$out"; exit 1; }
  echo "$out" | grep -E "$1" || { echo "$out"; echo "expected: $1"; exit 1; }
}

expect "Successfully applied"
expect "No migration necessary"

grants=$(docker exec "$pg" psql -U postgres -d udb -tAc \
  "SELECT has_table_privilege('hub_writer', 'public.companies', 'INSERT')::text || ',' ||
          has_table_privilege('hub_reader', 'public.companies', 'UPDATE')::text")
[ "$grants" = "true,false" ] || { echo "unexpected grants (writer INSERT, reader UPDATE): $grants"; exit 1; }
# The taxonomy tables: counts as generated from the YAML they replaced, the removed keywords still
# removed, the four keywords check_branch_classifier.py's compound cases use still active, and
# nobody but a migration may change them.
taxonomy=$(docker exec "$pg" psql -U postgres -d udb -tAc "
  SELECT concat_ws(',',
    (SELECT count(*) FROM public.technology_fields),
    (SELECT count(*) FILTER (WHERE name_en IS NOT NULL) FROM public.technology_fields),
    (SELECT count(berlin_partner) FROM public.technology_fields),
    (SELECT count(*) FROM public.technology_field_keywords WHERE removed_at IS NULL),
    (SELECT count(*) FROM public.technology_field_keywords WHERE removed_at IS NOT NULL),
    (SELECT count(*) FROM public.wz_divisions),
    (SELECT count(DISTINCT section) FROM public.wz_divisions),
    (SELECT count(*) FROM public.technology_field_keywords
      WHERE lower(keyword) = 'sicherheit' AND removed_at IS NULL),
    (SELECT count(*) FROM public.technology_field_keywords
      WHERE lower(keyword) = 'sensorik' AND removed_at IS NULL),
    (SELECT count(DISTINCT lower(keyword)) FROM public.technology_field_keywords
      WHERE lower(keyword) IN ('batterierecycling', 'schienenfahrzeuge', 'industrieanlagen', 'holzbau')
        AND removed_at IS NULL),
    (SELECT count(*) FROM public.technology_field_keywords
      WHERE field = 'Defence Tech' AND removed_at IS NULL) >= 10,
    has_table_privilege('hub_writer', 'public.technology_field_keywords', 'INSERT'),
    has_table_privilege('hub_writer', 'public.technology_field_keywords', 'SELECT'),
    has_table_privilege('hub_reader', 'public.wz_divisions', 'SELECT'))")
want="77,77,67,544,27,87,22,0,7,4,t,f,t,t"
[ "$taxonomy" = "$want" ] || { echo "taxonomy tables: got $taxonomy, want $want"; exit 1; }
# The review queue (#153): a verdict the reader (Hasura) writes becomes the same person's vote in
# extraction_feedback; "unsure" takes it back; a gap task copies only a correction; a round without a
# feedback field (email, phone) copies nothing; taking back a second round's verdict keeps the first
# round's vote; a vote cast in the app gets its own answer back when the verdict that overwrote it
# is deleted, also after two rounds overwrote it and were deleted in draw order; the reader cannot
# draw tasks.
review=$(docker exec -i "$pg" psql -U postgres -d udb -v ON_ERROR_STOP=1 -qtA <<'SQL'
BEGIN;
INSERT INTO public.companies (id) VALUES ('00000000-0000-0000-0000-000000000001');
INSERT INTO public.review_rounds (id, source_key, field_name, feedback_field, kind, title, size) VALUES
  ('00000000-0000-0000-0000-0000000000a1', 'branch_classifier', 'technology_field', 'technology_field', 'shown', 't', 1),
  ('00000000-0000-0000-0000-0000000000a2', NULL, 'technology_field', 'technology_field', 'gap', 'g', 1),
  ('00000000-0000-0000-0000-0000000000a3', 'impressum_contacts', 'email', NULL, 'shown', 'e', 1),
  ('00000000-0000-0000-0000-0000000000a4', 'branch_classifier', 'technology_field', 'technology_field', 'shown', 't2', 1);
INSERT INTO public.review_tasks (id, round_id, position, company_id, value) VALUES
  ('00000000-0000-0000-0000-0000000000b1', '00000000-0000-0000-0000-0000000000a1', 1, '00000000-0000-0000-0000-000000000001', 'Photonics'),
  ('00000000-0000-0000-0000-0000000000b2', '00000000-0000-0000-0000-0000000000a2', 1, '00000000-0000-0000-0000-000000000001', ''),
  ('00000000-0000-0000-0000-0000000000b3', '00000000-0000-0000-0000-0000000000a3', 1, '00000000-0000-0000-0000-000000000001', 'x@example.de'),
  ('00000000-0000-0000-0000-0000000000b4', '00000000-0000-0000-0000-0000000000a1', 2, '00000000-0000-0000-0000-000000000001', 'Robotics'),
  ('00000000-0000-0000-0000-0000000000b5', '00000000-0000-0000-0000-0000000000a4', 1, '00000000-0000-0000-0000-000000000001', 'Photonics'),
  ('00000000-0000-0000-0000-0000000000b6', '00000000-0000-0000-0000-0000000000a4', 2, '00000000-0000-0000-0000-000000000001', 'Robotics');
SET ROLE hub_reader;
INSERT INTO public.review_verdicts (task_id, verdict, author_id) VALUES
  ('00000000-0000-0000-0000-0000000000b1', 'correct', 'u1'),
  ('00000000-0000-0000-0000-0000000000b3', 'wrong', 'u1');
INSERT INTO public.review_verdicts (task_id, verdict, correction, author_id) VALUES
  ('00000000-0000-0000-0000-0000000000b2', 'wrong', 'Software', 'u1');
SELECT string_agg(value || ':' || verdict || ':' || coalesce(correction, '-'), ' ' ORDER BY value) FROM public.extraction_feedback;
INSERT INTO public.review_verdicts (task_id, verdict, author_id) VALUES
  ('00000000-0000-0000-0000-0000000000b5', 'correct', 'u1');
INSERT INTO public.review_verdicts (task_id, verdict, author_id) VALUES
  ('00000000-0000-0000-0000-0000000000b5', 'unsure', 'u1')
  ON CONFLICT (task_id) DO UPDATE SET verdict = EXCLUDED.verdict;
SELECT count(*) FROM public.extraction_feedback WHERE value = 'Photonics';
UPDATE public.review_verdicts SET verdict = 'unsure' WHERE task_id = '00000000-0000-0000-0000-0000000000b1';
SELECT count(*) FROM public.extraction_feedback;
INSERT INTO public.extraction_feedback (company_id, field_name, value, verdict, author_id) VALUES
  ('00000000-0000-0000-0000-000000000001', 'technology_field', 'Robotics', 'correct', 'u1');
INSERT INTO public.review_verdicts (task_id, verdict, correction, author_id) VALUES
  ('00000000-0000-0000-0000-0000000000b4', 'wrong', 'Software', 'u1');
SELECT verdict || ':' || coalesce(correction, '-') FROM public.extraction_feedback WHERE value = 'Robotics';
DELETE FROM public.review_verdicts WHERE task_id = '00000000-0000-0000-0000-0000000000b4';
SELECT verdict || ':' || coalesce(correction, '-') FROM public.extraction_feedback WHERE value = 'Robotics';
INSERT INTO public.review_verdicts (task_id, verdict, correction, author_id) VALUES
  ('00000000-0000-0000-0000-0000000000b4', 'wrong', 'Software', 'u1');
INSERT INTO public.review_verdicts (task_id, verdict, correction, author_id) VALUES
  ('00000000-0000-0000-0000-0000000000b6', 'wrong', 'Biotech', 'u1');
DELETE FROM public.review_verdicts WHERE task_id = '00000000-0000-0000-0000-0000000000b4';
SELECT verdict || ':' || coalesce(correction, '-') FROM public.extraction_feedback WHERE value = 'Robotics';
DELETE FROM public.review_verdicts WHERE task_id = '00000000-0000-0000-0000-0000000000b6';
SELECT verdict || ':' || coalesce(correction, '-') FROM public.extraction_feedback WHERE value = 'Robotics';
SELECT has_table_privilege('hub_reader', 'public.review_tasks', 'INSERT');
ROLLBACK;
SQL
)
want=$(printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s' ':wrong:Software Photonics:correct:-' '1' '1' 'wrong:Software' 'correct:-' \
  'wrong:Biotech' 'correct:-' 'f')
[ "$review" = "$want" ] || { echo "review queue: got [$review], want [$want]"; exit 1; }
# A drawn round holds at most one task per company: company c1 has two refused website candidates,
# c2 one, so the writer's draw (review_round.draw, run for real) gives 2 tasks for 2 companies.
docker exec -i "$pg" psql -U postgres -d udb -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO public.companies (id) VALUES
  ('00000000-0000-0000-0000-0000000000c1'), ('00000000-0000-0000-0000-0000000000c2');
INSERT INTO public.sources (id, key, name, source_type) VALUES
  ('00000000-0000-0000-0000-0000000000d1', 'website_discovery', 'w', 'web');
INSERT INTO public.company_source_records (id, company_id, source_id, external_id) VALUES
  ('00000000-0000-0000-0000-0000000000e1', '00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000d1', 'c1'),
  ('00000000-0000-0000-0000-0000000000e2', '00000000-0000-0000-0000-0000000000c2', '00000000-0000-0000-0000-0000000000d1', 'c2');
INSERT INTO public.company_field_observations (company_id, company_source_record_id, source_id, field_name, value_text) VALUES
  ('00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000e1', '00000000-0000-0000-0000-0000000000d1', 'candidate_url', 'https://a.example'),
  ('00000000-0000-0000-0000-0000000000c1', '00000000-0000-0000-0000-0000000000e1', '00000000-0000-0000-0000-0000000000d1', 'candidate_url', 'https://b.example'),
  ('00000000-0000-0000-0000-0000000000c2', '00000000-0000-0000-0000-0000000000e2', '00000000-0000-0000-0000-0000000000d1', 'candidate_url', 'https://c.example');
SQL
drawn=$(docker run --rm --network "$net" -v "$PWD/airflow/dags:/dags:ro" -e PYTHONPATH=/dags python:3.12-slim sh -c "
pip install -q --disable-pip-version-check --root-user-action=ignore psycopg2-binary >/dev/null && python - <<'PY'
import psycopg2
from review_round.draw import draw
with psycopg2.connect(host='$pg', dbname='udb', user='hub_writer', password='pw') as conn, conn.cursor() as cur:
    round_id, n = draw(cur, source='website_discovery', field='candidate_url', kind='refused', size=10)
    cur.execute('SELECT count(DISTINCT company_id), max(feedback_field) FROM public.review_tasks t '
                'JOIN public.review_rounds r ON r.id = t.round_id WHERE r.id = %s', (round_id,))
    print(n, *cur.fetchone())
PY")
[ "$drawn" = "2 2 website" ] || { echo "review draw: got [$drawn], want [2 2 website]"; exit 1; }
echo "migration image OK"
