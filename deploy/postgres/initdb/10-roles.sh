#!/bin/sh
# Creates least-privilege roles on first database initialisation:
#
#   airq_migrator  owns the schema and runs DDL (`airq migrate`)
#   airq_app       the service: DML on the tables it needs, nothing else
#
# Default privileges let the app read every table the migrator creates (including
# goose's version table, for the "migrations pending" health check). Write access is
# granted per table by the migrations themselves (see 000002_grant_app_privileges.sql),
# so the app can never tamper with migration state. The app cannot CREATE, ALTER or
# DROP anything: a SQL injection or a compromised service cannot change the schema.
set -eu

: "${AIRQ_MIGRATOR_PASSWORD:?must be set}"
: "${AIRQ_APP_PASSWORD:?must be set}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v migrator_pw="$AIRQ_MIGRATOR_PASSWORD" -v app_pw="$AIRQ_APP_PASSWORD" -v db="$POSTGRES_DB" <<'SQL'
CREATE ROLE airq_migrator LOGIN PASSWORD :'migrator_pw';
CREATE ROLE airq_app LOGIN PASSWORD :'app_pw' CONNECTION LIMIT 50;

REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO airq_migrator, airq_app;

-- Postgres 15+ no longer lets PUBLIC create in the public schema; be explicit anyway.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO airq_migrator;
GRANT USAGE ON SCHEMA public TO airq_app;

ALTER DEFAULT PRIVILEGES FOR ROLE airq_migrator IN SCHEMA public
  GRANT SELECT ON TABLES TO airq_app;
ALTER DEFAULT PRIVILEGES FOR ROLE airq_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO airq_app;

-- Server-side guard rails for the service role (the pool also sets these per session).
ALTER ROLE airq_app SET statement_timeout = '5s';
ALTER ROLE airq_app SET idle_in_transaction_session_timeout = '20s';
SQL
