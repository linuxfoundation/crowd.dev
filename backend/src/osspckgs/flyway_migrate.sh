#!/usr/bin/env bash

set -e
echo "Migrating jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}"

flyway \
    -locations="filesystem:/tmp/migrations" \
    -url="jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}" \
    -user="$PGUSER" \
    -password="$PGPASSWORD" \
    -connectRetries=60 \
    -outOfOrder=true \
    -mixed=true \
    -placeholderReplacement=false \
    -schemas=public \
    -X \
    migrate
