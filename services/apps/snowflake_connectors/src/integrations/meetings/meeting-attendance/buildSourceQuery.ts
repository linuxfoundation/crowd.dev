import { IS_PROD_ENV } from '@crowd/common'

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
    SELECT account_id, account_name, website, domain_aliases, LOGO_URL, INDUSTRY, N_EMPLOYEES
    FROM analytics.bronze_fivetran_salesforce.accounts
    WHERE website IS NOT NULL
    UNION ALL
    SELECT account_id, account_name, website, domain_aliases, NULL AS LOGO_URL, NULL AS INDUSTRY, NULL AS N_EMPLOYEES
    FROM analytics.bronze_fivetran_salesforce_b2b.accounts
    WHERE website IS NOT NULL
  )`

const LF_SSO_LOOKUP = `
  lf_sso_lookup AS (
    SELECT INVITEE_LF_USER_ID AS LF_USER_ID, MAX(INVITEE_LF_SSO) AS LF_SSO
    FROM ANALYTICS.SILVER_FACT.MEETING_ATTENDANCE
    WHERE INVITEE_LF_SSO IS NOT NULL
      AND INVITEE_LF_USER_ID IS NOT NULL
    GROUP BY INVITEE_LF_USER_ID
  )`

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  let select = `
  SELECT
    MD5(COALESCE(CAST(t.PRIMARY_KEY AS VARCHAR), '') || '|' || COALESCE(CAST(t.COMMITTEE_ID AS VARCHAR), '')) AS GENERATED_SOURCE_ID,
    CAST(t.MEETING_ID AS VARCHAR) AS MEETING_ID,
    t.MEETING_NAME,
    t.PROJECT_ID,
    t.PROJECT_NAME,
    t.PROJECT_SLUG,
    t.ACCOUNT_ID,
    t.ACCOUNT_NAME,
    t.MEETING_DATE,
    t.MEETING_TIME,
    t.INVITEE_FULL_NAME,
    COALESCE(t.INVITEE_LF_SSO, sso.LF_SSO) AS INVITEE_LF_SSO,
    t.INVITEE_LF_USER_ID,
    t.INVITEE_EMAIL,
    t.INVITEE_ATTENDED,
    t.WAS_INVITED,
    t.RAW_COMMITTEE_TYPE,
    t.UPDATED_TS,
    org.website AS ORG_WEBSITE,
    org.domain_aliases AS ORG_DOMAIN_ALIASES,
    org.logo_url AS LOGO_URL,
    org.industry AS ORGANIZATION_INDUSTRY,
    CAST(org.n_employees AS VARCHAR) AS ORGANIZATION_SIZE
  FROM ANALYTICS.SILVER_FACT.MEETING_ATTENDANCE t
  INNER JOIN cdp_matched_segments cms
    ON cms.slug = t.PROJECT_SLUG
    AND cms.sourceId = t.PROJECT_ID
  LEFT JOIN lf_sso_lookup sso
    ON t.INVITEE_LF_USER_ID = sso.LF_USER_ID
  LEFT JOIN org_accounts org
    ON t.ACCOUNT_ID = org.account_id
  WHERE (t.WAS_INVITED = TRUE OR t.INVITEE_ATTENDED = TRUE)
    AND NULLIF(TRIM(t.INVITEE_EMAIL), '') IS NOT NULL`

  if (!IS_PROD_ENV) {
    select += ` AND t.PROJECT_SLUG = 'cncf'`
  }

  const dedup = `
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.PRIMARY_KEY ORDER BY org.website DESC) = 1`

  if (!sinceTimestamp) {
    return `
  WITH ${ORG_ACCOUNTS},
  ${CDP_MATCHED_SEGMENTS},
  ${LF_SSO_LOOKUP}
  ${select}
  ${dedup}`.trim()
  }

  return `
  WITH ${ORG_ACCOUNTS},
  ${CDP_MATCHED_SEGMENTS},
  ${LF_SSO_LOOKUP},
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

  -- Updated records in existing segments
  ${select}
    AND t.UPDATED_TS > '${sinceTimestamp}'
  ${dedup}

  UNION

  -- All records in newly created segments
  ${select}
    AND EXISTS (
      SELECT 1 FROM new_cdp_segments ncs
      WHERE ncs.slug = cms.slug AND ncs.sourceId = cms.sourceId
    )
  ${dedup}`.trim()
}
