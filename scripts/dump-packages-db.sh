#!/usr/bin/env bash
# Dump packages-db to a .dump file (pg_dump custom format, compressed).
# Usage: ./scripts/dump-packages-db.sh [output-file]
# Default output: packages-db-<date>.dump in the current directory.
set -euo pipefail

DUMP_FILE="${1:-packages-db-$(date +%Y%m%d-%H%M%S).dump}"

# Find the packages postgres container by its exposed port (5434).
CONTAINER=$(docker ps --format '{{.Names}}\t{{.Ports}}' | awk -F'\t' '$2 ~ /5434/ {print $1}' | head -1)
if [[ -z "$CONTAINER" ]]; then
  echo "ERROR: packages-db container not found. Is scaffold running?" >&2
  exit 1
fi

echo "Container : $CONTAINER"
echo "Database  : packages-db"
echo "Output    : $DUMP_FILE"
echo ""

docker exec "$CONTAINER" \
  pg_dump \
    --username postgres \
    --dbname packages-db \
    --format custom \
    --compress 9 \
    --no-password \
> "$DUMP_FILE"

SIZE=$(du -sh "$DUMP_FILE" | cut -f1)
echo "Done. $DUMP_FILE ($SIZE)"
