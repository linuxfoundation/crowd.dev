import { SCORECARD_DATASET } from '../../deps-dev/config'

export const SCORECARD_REPOS_SQL = `
SELECT
  CASE
    WHEN repo.name LIKE 'github.com/%' THEN LOWER(CONCAT('https://', repo.name))
    ELSE CONCAT('https://', repo.name)
  END AS repo_url,
  score,
  date AS scanned_at
FROM \`${SCORECARD_DATASET}.scorecard-v2_latest\`
WHERE repo.name IS NOT NULL
`

export const SCORECARD_CHECKS_SQL = `
SELECT
  CASE
    WHEN r.repo.name LIKE 'github.com/%' THEN LOWER(CONCAT('https://', r.repo.name))
    ELSE CONCAT('https://', r.repo.name)
  END AS repo_url,
  c.name   AS check_name,
  c.score  AS check_score,
  c.reason AS check_reason
FROM \`${SCORECARD_DATASET}.scorecard-v2_latest\` r,
UNNEST(r.checks) AS c
WHERE r.repo.name IS NOT NULL
`
