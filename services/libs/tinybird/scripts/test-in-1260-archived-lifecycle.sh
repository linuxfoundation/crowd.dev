#!/usr/bin/env bash
# Pulls the current lifecycleLabel expression straight out of the pipe file
# (not a hardcoded copy) and re-runs it against live production data via `tb sql`,
# asserting every project with status='archived' also has lifecycleLabel='archived'.
#
# Requires: `tb` CLI authenticated against the lfx_insights workspace (`tb auth info`).
set -euo pipefail
export TB_VERSION_WARNING=0

cd "$(dirname "$0")/.."

PIPE_FILE="pipes/project_insights_copy.pipe"

# Extract the current lifecycleLabel expression from NODE project_insights_copy_project_results
# (not the unrelated NODE project_insights_copy_repo_results, which also has an "AS lifecycleLabel,").
LIFECYCLE_EXPR=$(awk '/^NODE project_insights_copy_project_results$/{f=1} f && /AS lifecycleLabel,/{print; exit} /^NODE /&&!/project_insights_copy_project_results/{if(f)exit}' "$PIPE_FILE" \
    | sed -E 's/^[[:space:]]*(.*) AS lifecycleLabel,[[:space:]]*$/\1/')

if [ -z "$LIFECYCLE_EXPR" ]; then
    echo "🚨 ERROR: could not find the lifecycleLabel expression in $PIPE_FILE"
    exit 1
fi

echo "Testing lifecycleLabel expression: $LIFECYCLE_EXPR"

QUERY=$(cat <<SQL
WITH base AS (
    SELECT id, status
    FROM insights_projects_populated_ds
    GROUP BY id, status
),
hv2 AS (
    SELECT
        rep.insightsProjectId AS insightsProjectId,
        if(
            empty(groupArray(h.lifecycleLabelV2)),
            NULL,
            toNullable(
                arrayElement(
                    arraySort(
                        x -> indexOf(
                            ['active', 'stable', 'declining', 'inert', 'abandoned', 'archived'], x
                        ),
                        groupArray(h.lifecycleLabelV2)
                    ),
                    1
                )
            )
        ) AS lifecycleLabelV2
    FROM repositories rep FINAL
    INNER JOIN health_score_v2_repo_copy_ds h ON h.repoUrl = rep.url
    WHERE rep.insightsProjectId != '' AND rep.enabled = true AND rep.excluded = false
    GROUP BY rep.insightsProjectId
)
SELECT
    countIf(base.status = 'archived' AND ${LIFECYCLE_EXPR} != 'archived') AS mismatches
FROM base
LEFT JOIN hv2 ON base.id = hv2.insightsProjectId
SQL
)

RESULT=$(tb sql "$QUERY" --format csv | grep -v '^$' | tail -n 1)

echo "Archived-in-PCC projects whose lifecycleLabel is not 'archived': $RESULT"

if [ "$RESULT" -eq 0 ]; then
    echo "✅ PASS: every PCC-archived project has lifecycleLabel='archived'"
    exit 0
else
    echo "🚨 FAIL: $RESULT PCC-archived project(s) have a stale non-archived lifecycleLabel"
    exit 1
fi
