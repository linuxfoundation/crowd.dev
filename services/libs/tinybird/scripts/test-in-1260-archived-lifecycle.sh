#!/usr/bin/env bash
# Queries the deployed, materialized project_insights_copy_ds directly —
# the same table the insights frontend reads — asserting every project
# with status='archived' also has lifecycleLabel='archived'. Only reflects
# a fix once project_insights_copy.pipe has actually been deployed.
#
# Requires: `tb` CLI authenticated against the lfx_insights workspace (`tb auth info`).
set -euo pipefail
export TB_VERSION_WARNING=0

cd "$(dirname "$0")/.."

QUERY="SELECT count() AS mismatches FROM project_insights_copy_ds WHERE type = 'project' AND status = 'archived' AND lifecycleLabel != 'archived'"

RESULT=$(tb sql "$QUERY" --format csv | grep -v '^$' | tail -n 1)

echo "Archived-in-PCC projects whose lifecycleLabel is not 'archived': $RESULT"

if [ "$RESULT" -eq 0 ]; then
    echo "✅ PASS: every PCC-archived project has lifecycleLabel='archived'"
    exit 0
else
    echo "🚨 FAIL: $RESULT PCC-archived project(s) have a stale non-archived lifecycleLabel"
    exit 1
fi
