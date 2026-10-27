# The migration image. The Data Hub runs it as an initContainer before the app starts, so the
# schema is current before anything reads it. Flyway skips migrations it has already applied and
# takes a lock, so running it on every start, from several pods at once, is safe.
#
# Configured by environment only (see docs/release.md):
#   FLYWAY_URL                        jdbc:postgresql://<host>:5432/<database>
#   FLYWAY_USER / FLYWAY_PASSWORD     the database OWNER login
#   FLYWAY_PLACEHOLDERS_WRITER_ROLE   the hub's name for the writer login
#   FLYWAY_PLACEHOLDERS_READER_ROLE   the hub's name for the reader login
FROM flyway/flyway:12

COPY migrations /flyway/sql

# The database may still be starting when the pod does.
ENV FLYWAY_CONNECT_RETRIES=10

CMD ["migrate"]
