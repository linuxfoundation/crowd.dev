#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/utils
source "${REPO_ROOT}/scripts/utils"

: "${DB_HOST:?DB_HOST is required}"
: "${DB_PORT:?DB_PORT is required}"
: "${DB_USER:?DB_USER is required}"
: "${DB_PASSWORD:?DB_PASSWORD is required}"

say "Applying Sequin bootstrap SQL..."
docker run --rm --network host \
  -v "${REPO_ROOT}/scripts/scaffold/sequin/postgres-docker-entrypoint-initdb.d/create-sequin-database.sql:/bootstrap.sql:ro" \
  -e "PGPASSWORD=${DB_PASSWORD}" \
  postgres:14-alpine \
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 -f /bootstrap.sql

say "Recreating test_template..."
docker run --rm --network host \
  -e "PGPASSWORD=${DB_PASSWORD}" \
  postgres:14-alpine \
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS test_template;" \
  -c "CREATE DATABASE test_template;"

say "Building flyway image..."
docker build -t crowd_flyway -f "${REPO_ROOT}/backend/src/database/Dockerfile.flyway" "${REPO_ROOT}/backend/src/database"

say "Migrating test_template..."
docker run --rm --network host \
  -e "PGHOST=${DB_HOST}" \
  -e "PGPORT=${DB_PORT}" \
  -e "PGUSER=${DB_USER}" \
  -e "PGPASSWORD=${DB_PASSWORD}" \
  -e PGDATABASE=test_template \
  crowd_flyway

say "Test template database ready."
