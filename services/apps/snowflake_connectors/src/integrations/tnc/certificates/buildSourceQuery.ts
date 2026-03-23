import { IS_PROD_ENV } from '@crowd/common'

// Main: analytics.silver_fact.certificates (certificate data)
// Joins:
// - analytics.silver_dim._crowd_dev_segments_union (segment resolution)
// - analytics.bronze_fivetran_salesforce.bronze_salesforce_merged_user (LFID)
// - analytics.silver_dim.users (LFID fallback)
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

const LFID_COALESCE = `COALESCE(mu.user_name, u.lf_username)`

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  let select = `
  SELECT
    c.*,
    cms.slug AS PROJECT_SLUG,
    org.account_name AS ORGANIZATION_NAME,
    org.website AS ORG_WEBSITE,
    org.domain_aliases AS ORG_DOMAIN_ALIASES,
    org.logo_url AS LOGO_URL,
    org.industry AS ORGANIZATION_INDUSTRY,
    CAST(org.n_employees AS VARCHAR) AS ORGANIZATION_SIZE,
    ${LFID_COALESCE} AS LFID
  FROM analytics.silver_fact.certificates c
  INNER JOIN cdp_matched_segments cms
    ON cms.sourceId = c.project_id
  LEFT JOIN analytics.bronze_fivetran_salesforce.bronze_salesforce_merged_user mu
    ON c.user_id = mu.user_id
    AND mu.user_name IS NOT NULL
  LEFT JOIN analytics.silver_dim.users u
    ON LOWER(c.user_email) = LOWER(u.email)
    AND u.lf_username IS NOT NULL
  LEFT JOIN org_accounts org
    ON c.account_id = org.account_id
  WHERE c.user_email IS NOT NULL`

  // Limit to a single project in non-prod to avoid exporting all projects data
  if (!IS_PROD_ENV) {
    select += ` AND cms.slug = 'cncf'`
  }

  const dedup = `
  QUALIFY ROW_NUMBER() OVER (PARTITION BY c.certificate_id ORDER BY org.website DESC) = 1`

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

  -- New certificates since last export
  ${select}
    AND c.issued_ts >= '${sinceTimestamp}'
  ${dedup}

  UNION

  -- All certificates in newly created segments
  ${select}
    AND EXISTS (
      SELECT 1 FROM new_cdp_segments ncs
      WHERE ncs.slug = cms.slug AND ncs.sourceId = cms.sourceId
    )
  ${dedup}`.trim()
}
