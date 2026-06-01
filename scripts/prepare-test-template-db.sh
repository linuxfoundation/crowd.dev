#!/usr/bin/env bash
# Recreates test_template and applies Flyway migrations.
# Requires a running Postgres reachable at DB_HOST:DB_PORT (see .env.test for local).
#
# Optional: SEQUIN_BOOTSTRAP=1 — CI Postgres has no init SQL; local compose runs it on first boot.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/utils
source "${ROOT_DIR}/scripts/utils"

: "${DB_HOST:?DB_HOST is required}"
: "${DB_PORT:?DB_PORT is required}"
: "${DB_USER:?DB_USER is required}"
: "${DB_PASSWORD:?DB_PASSWORD is required}"

DATABASE_DIR="${ROOT_DIR}/backend/src/database"

pg_psql() {
  docker run --rm --network host \
    -e "PGPASSWORD=${DB_PASSWORD}" \
    postgres:14-alpine \
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 "$@"
}

if [[ "${SEQUIN_BOOTSTRAP:-0}" == "1" ]]; then
  say "Applying Sequin bootstrap SQL..."
  docker run --rm --network host \
    -v "${ROOT_DIR}/scripts/scaffold/sequin/postgres-docker-entrypoint-initdb.d/create-sequin-database.sql:/bootstrap.sql:ro" \
    -e "PGPASSWORD=${DB_PASSWORD}" \
    postgres:14-alpine \
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 -f /bootstrap.sql
fi

say "Recreating test_template..."
pg_psql \
  -c "DROP DATABASE IF EXISTS test_template;" \
  -c "CREATE DATABASE test_template;"

say "Building flyway image..."
docker build -t crowd_flyway -f "${DATABASE_DIR}/Dockerfile.flyway" "${DATABASE_DIR}"

say "Migrating test_template..."
docker run --rm --network host \
  -e "PGHOST=${DB_HOST}" \
  -e "PGPORT=${DB_PORT}" \
  -e "PGUSER=${DB_USER}" \
  -e "PGPASSWORD=${DB_PASSWORD}" \
  -e PGDATABASE=test_template \
  crowd_flyway

say "Test template database ready."
