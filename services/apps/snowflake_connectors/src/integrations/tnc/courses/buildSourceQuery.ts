import { IS_PROD_ENV } from '@crowd/common'

// Main: analytics.bronze_census_ti.course_actions (course action data)
// Joins:
// - analytics.bronze_census_ti.users (user resolution via internal_ti_user_id)
// - analytics.bronze_census_ti.courses (course metadata)
// - analytics.silver_fact.enrollments (segment + org resolution via email + course_id)
// - analytics.silver_dim._crowd_dev_segments_union (segment resolution)
// - analytics.bronze_fivetran_salesforce.accounts + analytics.bronze_fivetran_salesforce_b2b.accounts (org data)

const CDP_MATCHED_SEGMENTS = `
  cdp_matched_segments AS (
    SELECT DISTINCT
      s.SOURCE_ID AS sourceId,
      s.slug
    FROM ANALYTICS.SILVER_DIM._CROWD_DEV_SEGMENTS_UNION s
    WHERE s.PARENT_SLUG IS NOT NULL
      AND s.GRANDPARENTS_SLUG IS NOT NULL
      AND s.SOURCE_ID IS NOT NULL
  )`

const ORG_ACCOUNTS = `
  org_accounts AS (
    SELECT account_id, account_name, website, domain_aliases, LOGO_URL, INDUSTRY, N_EMPLOYEES
    FROM analytics.bronze_fivetran_salesforce.accounts
    WHERE website IS NOT NULL
    UNION ALL
    SELECT account_id, account_name, website, domain_aliases, NULL AS LOGO_URL, NULL AS INDUSTRY, NULL AS N_EMPLOYEES
    FROM analytics.bronze_fivetran_salesforce_b2b.accounts
    WHERE website IS NOT NULL
  )`

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  let select = `
  SELECT
    ca.*,
    co.*,
    tu.user_email,
    tu.lfid,
    tu.learner_name,
    tu.user_country,
    tu.job_title,
    e.project_slug AS PROJECT_SLUG,
    e.project_id AS PROJECT_ID,
    e.account_id,
    org.account_name AS ORGANIZATION_NAME,
    org.website AS ORG_WEBSITE,
    org.domain_aliases AS ORG_DOMAIN_ALIASES,
    org.logo_url AS LOGO_URL,
    org.industry AS ORGANIZATION_INDUSTRY,
    CAST(org.n_employees AS VARCHAR) AS ORGANIZATION_SIZE
  FROM analytics.bronze_census_ti.course_actions ca
  INNER JOIN analytics.bronze_census_ti.users tu
    ON ca.internal_ti_user_id = tu.internal_ti_user_id
  INNER JOIN analytics.bronze_census_ti.courses co
    ON ca.course_id = co.course_id
  INNER JOIN analytics.silver_fact.enrollments e
    ON e.course_id = ca.course_id
    AND LOWER(e.user_email) = LOWER(tu.user_email)
  INNER JOIN cdp_matched_segments cms
    ON cms.slug = e.project_slug
    AND cms.sourceId = e.project_id
  LEFT JOIN org_accounts org
    ON e.account_id = org.account_id
  WHERE ca.type = 'status_change'
    AND ca.source = 'course_started'
    AND co.is_test_or_archived = false
    AND tu.user_email IS NOT NULL`

  // Limit to a single project in non-prod to avoid exporting all projects data
  if (!IS_PROD_ENV) {
    select += ` AND e.project_slug = 'cncf'`
  }

  const dedup = `
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ca.course_action_id ORDER BY org.website DESC) = 1`

  if (!sinceTimestamp) {
    return `
  WITH ${ORG_ACCOUNTS},
  ${CDP_MATCHED_SEGMENTS}
  ${select}
  ${dedup}`.trim()
  }

  return `
  WITH ${ORG_ACCOUNTS},
  ${CDP_MATCHED_SEGMENTS},
  new_cdp_segments AS (
    SELECT DISTINCT
      s.SOURCE_ID AS sourceId,
      s.slug
    FROM ANALYTICS.SILVER_DIM._CROWD_DEV_SEGMENTS_UNION s
    WHERE s.CREATED_TS >= '${sinceTimestamp}'
      AND s.PARENT_SLUG IS NOT NULL
      AND s.GRANDPARENTS_SLUG IS NOT NULL
      AND s.SOURCE_ID IS NOT NULL
  )

  -- New course actions since last export
  ${select}
    AND ca.timestamp >= '${sinceTimestamp}'
  ${dedup}

  UNION

  -- All course actions in newly created segments
  ${select}
    AND EXISTS (
      SELECT 1 FROM new_cdp_segments ncs
      WHERE ncs.slug = cms.slug AND ncs.sourceId = cms.sourceId
    )
  ${dedup}`.trim()
}
