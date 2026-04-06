import { IS_PROD_ENV } from '@crowd/common'

// Main: FIVETRAN_INGEST.SFDC_CONNECTOR_PROD_PLATFORM.COMMUNITY__C
// Joins:
// - ANALYTICS.SILVER_DIM.COMMITTEE (committee metadata + project slug)
// - ANALYTICS.BRONZE_KAFKA_CROWD_DEV.SEGMENTS (segment resolution)
// - ANALYTICS.SILVER_DIM.USERS (member identity: email, lf_username, name)
// - ANALYTICS.BRONZE_FIVETRAN_SALESFORCE_B2B.ACCOUNTS (org data)

const CDP_MATCHED_SEGMENTS = `
  cdp_matched_segments AS (
    SELECT DISTINCT
      s.SOURCE_ID AS sourceId,
      s.slug
    FROM ANALYTICS.BRONZE_KAFKA_CROWD_DEV.SEGMENTS s
    WHERE s.PARENT_SLUG IS NOT NULL
      AND s.GRANDPARENTS_SLUG IS NOT NULL
      AND s.SOURCE_ID IS NOT NULL
  )`

const ORG_ACCOUNTS = `
  org_accounts AS (
    SELECT account_id, account_name, website, domain_aliases
    FROM ANALYTICS.BRONZE_FIVETRAN_SALESFORCE_B2B.ACCOUNTS
    WHERE website IS NOT NULL
  )`

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  let select = `
  SELECT
    c.SFID,
    c._FIVETRAN_DELETED AS FIVETRAN_DELETED,
    c.CONTACTEMAIL__C,
    c.CREATEDBYID,
    c.COLLABORATION_NAME__C,
    c.ACCOUNT__C,
    c.ROLE__C,
    c.CREATEDDATE::TIMESTAMP_NTZ AS CREATEDDATE,
    c.LASTMODIFIEDDATE::TIMESTAMP_NTZ AS LASTMODIFIEDDATE,
    c._FIVETRAN_SYNCED::TIMESTAMP_NTZ AS FIVETRAN_SYNCED,
    cm.COMMITTEE_ID,
    cm.COMMITTEE_NAME,
    cm.PROJECT_ID,
    cm.PROJECT_NAME,
    cm.PROJECT_SLUG,
    su.EMAIL AS SU_EMAIL,
    su.LF_USERNAME,
    su.PRIMARY_SOURCE_USER_ID,
    su.FIRST_NAME AS SU_FIRST_NAME,
    su.LAST_NAME AS SU_LAST_NAME,
    su.FULL_NAME AS SU_FULL_NAME,
    org.account_name AS ACCOUNT_NAME,
    org.website AS ORG_WEBSITE,
    org.domain_aliases AS ORG_DOMAIN_ALIASES
  FROM FIVETRAN_INGEST.SFDC_CONNECTOR_PROD_PLATFORM.COMMUNITY__C c
  JOIN ANALYTICS.SILVER_DIM.COMMITTEE cm
    ON c.COLLABORATION_NAME__C = cm.COMMITTEE_ID
  INNER JOIN cdp_matched_segments cms
    ON cms.slug = cm.PROJECT_SLUG
    AND cms.sourceId = cm.PROJECT_ID
  LEFT JOIN ANALYTICS.SILVER_DIM.USERS su
    ON LOWER(c.CONTACTEMAIL__C) = LOWER(su.EMAIL)
  LEFT JOIN org_accounts org
    ON c.ACCOUNT__C = org.account_id
  WHERE c.LASTMODIFIEDDATE IS NOT NULL`

  // Limit to a single project in non-prod to avoid exporting all project data
  if (!IS_PROD_ENV) {
    select += ` AND cm.PROJECT_SLUG = 'cncf'`
  }

  const dedup = `
  QUALIFY ROW_NUMBER() OVER (PARTITION BY c.SFID ORDER BY org.website DESC) = 1`

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
    FROM ANALYTICS.BRONZE_KAFKA_CROWD_DEV.SEGMENTS s
    WHERE s.CREATED_TS >= '${sinceTimestamp}'
      AND s.PARENT_SLUG IS NOT NULL
      AND s.GRANDPARENTS_SLUG IS NOT NULL
      AND s.SOURCE_ID IS NOT NULL
  )

  -- Updated committee memberships since last export
  ${select}
    AND (
      c.LASTMODIFIEDDATE > '${sinceTimestamp}'
      OR (c._FIVETRAN_DELETED = TRUE AND c._FIVETRAN_SYNCED > '${sinceTimestamp}')
    )
  ${dedup}

  UNION

  -- All committee memberships in newly created segments
  ${select}
    AND EXISTS (
      SELECT 1 FROM new_cdp_segments ncs
      WHERE ncs.slug = cms.slug AND ncs.sourceId = cms.sourceId
    )
  ${dedup}`.trim()
}
