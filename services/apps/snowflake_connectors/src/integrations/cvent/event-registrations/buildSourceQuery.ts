import { IS_PROD_ENV } from '@crowd/common'

// Exclude TIMESTAMP_TZ columns and re-add as TIMESTAMP_NTZ so Parquet export gets timezone-normalized values.
const TIMESTAMP_TZ_COLUMNS = [
  'EVENT_START_DATE',
  'EVENT_END_DATE',
  'EVENT_CREATED_TS',
  'EVENT_UPDATED_AT',
  'REGISTRATION_TS',
  'REGISTRATION_CREATED_TS',
  'EVENT_REGISTRATION_UPDATED_AT',
  'PURCHASE_TS',
  'UPDATED_TS',
]

const LFID_COALESCE = `COALESCE(
      CASE WHEN er.registration_source = 'bevy' THEN er.user_name END,
      mu.user_name,
      u.lf_username
    )`

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

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  const excludeClause = TIMESTAMP_TZ_COLUMNS.join(', ')
  const castClauses = TIMESTAMP_TZ_COLUMNS.map((col) => `er.${col}::TIMESTAMP_NTZ AS ${col}`).join(
    ', ',
  )

  let select = `
  SELECT
    er.* EXCLUDE (${excludeClause}),
    ${castClauses},
    org.website AS ORG_WEBSITE,
    org.domain_aliases AS ORG_DOMAIN_ALIASES,
    ${LFID_COALESCE} AS LFID
  FROM analytics.silver_fact.event_registrations er
  INNER JOIN cdp_matched_segments cms
    ON cms.slug = er.project_slug
    AND cms.sourceId = er.project_id
  LEFT JOIN analytics.bronze_fivetran_salesforce.bronze_salesforce_merged_user mu
    ON er.user_id = mu.user_id
    AND mu.user_name IS NOT NULL
  LEFT JOIN analytics.silver_dim.users u
    ON LOWER(er.email) = LOWER(u.email)
    AND u.lf_username IS NOT NULL
  LEFT JOIN org_accounts org
    ON er.account_id = org.account_id
  WHERE ${LFID_COALESCE} IS NOT NULL`

  // Limit to a single project in non-prod to avoid exporting all projects data
  if (!IS_PROD_ENV) {
    select += ` AND er.project_slug = 'pytorch'`
  }

  const dedup = `
  QUALIFY ROW_NUMBER() OVER (PARTITION BY er.registration_id ORDER BY org.website DESC) = 1`

  const orgAccounts = `
  org_accounts AS (
    SELECT account_id, website, domain_aliases
    FROM analytics.bronze_fivetran_salesforce.accounts
    WHERE website IS NOT NULL
    UNION ALL
    SELECT account_id, website, domain_aliases
    FROM analytics.bronze_fivetran_salesforce_b2b.accounts
    WHERE website IS NOT NULL
  )`

  if (!sinceTimestamp) {
    return `
  WITH ${orgAccounts},
  ${CDP_MATCHED_SEGMENTS}
  ${select}
  ${dedup}`.trim()
  }

  return `
  WITH ${orgAccounts},
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

  -- Updated records in existing segments
  ${select}
    AND er.updated_ts > '${sinceTimestamp}'
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
