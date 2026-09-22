#!/usr/bin/env bash
# Restore packages-db from a .dump file created by dump-packages-db.sh.
# Usage: ./scripts/restore-packages-db.sh <dump-file>
# WARNING: drops and recreates the packages-db database.
set -euo pipefail

DUMP_FILE="${1:-}"
if [[ -z "$DUMP_FILE" ]]; then
  echo "Usage: $0 <dump-file>" >&2
  exit 1
fi
if [[ ! -f "$DUMP_FILE" ]]; then
  echo "ERROR: file not found: $DUMP_FILE" >&2
  exit 1
fi

# Find the packages postgres container by its exposed port (5434).
CONTAINER=$(docker ps --format '{{.Names}}\t{{.Ports}}' | awk -F'\t' '$2 ~ /5434/ {print $1}' | head -1)
if [[ -z "$CONTAINER" ]]; then
  echo "ERROR: packages-db container not found. Is scaffold running?" >&2
  exit 1
fi

echo "Container : $CONTAINER"
echo "Database  : packages-db"
echo "Input     : $DUMP_FILE"
echo ""
read -rp "This will DROP and recreate packages-db. Continue? [y/N] " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
  echo "Aborted."
  exit 0
fi

echo "Dropping and recreating database..."
docker exec "$CONTAINER" \
  psql --username postgres --dbname postgres \
  -c "DROP DATABASE IF EXISTS \"packages-db\";" \
  -c "CREATE DATABASE \"packages-db\";"

echo "Restoring dump (this may take a while)..."
docker exec --interactive "$CONTAINER" \
  pg_restore \
    --username postgres \
    --dbname packages-db \
    --no-password \
    --verbose \
< "$DUMP_FILE"

echo ""
echo "Restore complete."
