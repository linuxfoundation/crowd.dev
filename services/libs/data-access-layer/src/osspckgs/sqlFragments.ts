// Severity stored as uppercase in advisories table.
// Ranks: CRITICAL=4, HIGH=3, MEDIUM=2, LOW=1
export const SEVERITY_RANK_EXPR = `MAX(CASE a.severity
    WHEN 'CRITICAL' THEN 4
    WHEN 'HIGH'     THEN 3
    WHEN 'MEDIUM'   THEN 2
    WHEN 'LOW'      THEN 1
    ELSE 0 END)::int`

export const STEWARD_MENTIONED_JOIN = `
  LEFT JOIN stewards st_mentioned
    ON sa.activity_type = 'steward_added'
    AND st_mentioned.user_id = (sa.metadata->>'userId')`

export const STEWARD_DISPLAY_NAME_METADATA = `CASE
        WHEN sa.activity_type = 'steward_added' AND st_mentioned.display_name IS NOT NULL
        THEN COALESCE(sa.metadata, '{}'::jsonb) || jsonb_build_object('stewardDisplayName', st_mentioned.display_name)
        ELSE sa.metadata
      END`
