import { IS_PROD_ENV } from '@crowd/common'

// Main: analytics.silver_fact.enrollments (enrollment data)
// Joins:
// - analytics.silver_dim._crowd_dev_segments_union (segment resolution)
// - analytics.bronze_fivetran_salesforce.bronze_salesforce_merged_user (LFID)
// - analytics.silver_dim.users (LFID fallback)
// - analytics.bronze_fivetran_salesforce.accounts + analytics.bronze_fivetran_salesforce_b2b.accounts (org data)
// - analytics.bronze_census_ti.course_actions + analytics.bronze_census_ti.users (course status)

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

const LFID_COALESCE = `COALESCE(mu.user_name, u.lf_username)`

const COURSE_STATUS = `
  course_status AS (
    SELECT
      tu.user_email,
      ca.course_id,
      MAX(CASE WHEN ca.source = 'course_started' THEN ca.timestamp END) AS COURSE_STARTED_DATE,
      MAX(CASE WHEN ca.source = 'course_completed' THEN ca.timestamp END) AS COURSE_COMPLETED_DATE,
      MAX_BY(ca.source, ca.timestamp) AS COURSE_STATUS
    FROM analytics.bronze_census_ti.course_actions ca
    INNER JOIN analytics.bronze_census_ti.users tu
      ON ca.internal_ti_user_id = tu.internal_ti_user_id
    WHERE ca.type = 'status_change'
      AND ca.source IN ('course_started', 'course_completed')
    GROUP BY tu.user_email, ca.course_id
  )`

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  let select = `
  SELECT
    e.*,
    org.account_name AS ORGANIZATION_NAME,
    org.website AS ORG_WEBSITE,
    org.domain_aliases AS ORG_DOMAIN_ALIASES,
    org.logo_url AS LOGO_URL,
    org.industry AS ORGANIZATION_INDUSTRY,
    CAST(org.n_employees AS VARCHAR) AS ORGANIZATION_SIZE,
    ${LFID_COALESCE} AS LFID,
    cs.COURSE_STARTED_DATE,
    cs.COURSE_COMPLETED_DATE,
    cs.COURSE_STATUS
  FROM analytics.silver_fact.enrollments e
  INNER JOIN cdp_matched_segments cms
    ON cms.slug = e.project_slug
    AND cms.sourceId = e.project_id
  LEFT JOIN analytics.bronze_fivetran_salesforce.bronze_salesforce_merged_user mu
    ON e.user_id = mu.user_id
    AND mu.user_name IS NOT NULL
  LEFT JOIN analytics.silver_dim.users u
    ON LOWER(e.user_email) = LOWER(u.email)
    AND u.lf_username IS NOT NULL
  LEFT JOIN org_accounts org
    ON e.account_id = org.account_id
  LEFT JOIN course_status cs
    ON LOWER(e.user_email) = LOWER(cs.user_email)
    AND e.course_id = cs.course_id
  WHERE e.user_email IS NOT NULL`

  // Limit to a single project in non-prod to avoid exporting all projects data
  if (!IS_PROD_ENV) {
    select += ` AND e.project_slug = 'cncf'`
  }

  const dedup = `
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.enrollment_id ORDER BY org.website DESC) = 1`

  if (!sinceTimestamp) {
    return `
  WITH ${ORG_ACCOUNTS},
  ${CDP_MATCHED_SEGMENTS},
  ${COURSE_STATUS}
  ${select}
  ${dedup}`.trim()
  }

  return `
  WITH ${ORG_ACCOUNTS},
  ${CDP_MATCHED_SEGMENTS},
  ${COURSE_STATUS},
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

  -- New enrollments since last export
  ${select}
    AND e.enrollment_ts >= '${sinceTimestamp}'
  ${dedup}

  UNION

  -- All enrollments in newly created segments
  ${select}
    AND EXISTS (
      SELECT 1 FROM new_cdp_segments ncs
      WHERE ncs.slug = cms.slug AND ncs.sourceId = cms.sourceId
    )
  ${dedup}`.trim()
}
