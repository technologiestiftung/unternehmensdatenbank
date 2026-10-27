-- V21: grants for the Data Hub's three database roles.
--
-- WHY
--
-- On the Berlin Data Hub every project database comes with three logins: an owner that runs the
-- migrations, a writer the pipelines use, and a reader for apps. Until now everything here ran as
-- the owner (`app`), so a DAG could have dropped a table and nothing would have noticed. The
-- local stack now creates the same three roles (postgres/init/01-create-databases.sh) and Airflow
-- connects as the writer, so a missing grant fails here first instead of on the hub.
--
-- The role NAMES are Flyway placeholders, because the hub names them and we do not know its
-- scheme yet. Locally they default to `writer` and `reader` (docker-compose.yml, flyway service).
--
-- WHAT EACH ROLE GETS
--
-- writer: read and write every table, use every sequence, and create TEMP tables -- the
--         coordinator's resolver stages rows in one (pipeline_coordinator/resolver.py). No DDL:
--         schema belongs to Flyway.
-- reader: SELECT only. The one exception, inserting comments, is granted in V22 next to the table.
--
-- ALTER DEFAULT PRIVILEGES covers every table a later migration creates, because migrations run
-- as the owner and default privileges attach to the role that creates the object.

GRANT USAGE ON SCHEMA public TO ${writer_role}, ${reader_role};

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${writer_role};
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${writer_role};
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO ${writer_role}, ${reader_role};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${reader_role};

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${writer_role};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO ${writer_role};
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO ${reader_role};

DO $$
BEGIN
  EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO ${writer_role}', current_database());
END
$$;
