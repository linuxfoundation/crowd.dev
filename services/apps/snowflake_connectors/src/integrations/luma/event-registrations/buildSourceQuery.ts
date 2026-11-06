import { IS_PROD_ENV } from '@crowd/common'

import { personalEmailCondition } from '../../../core/personalEmail'

export const LUMA_EVENT_HOST = 'luma.com'
export const FIRST_INGEST_EVENT_ID = 'gatewaze_166d7ea1-9759-4cf5-943c-76ee2bd53449'

const NON_PROD_PROJECT_SLUG = 'agentic-ai-foundation'

const SELECTED_COLUMNS = [
  'er.registration_id',
  'er.registration_status',
  'er.registration_type',
  'er.user_attended',
  'er.user_checked_in_ts',
  'er.event_id',
  'er.event_name',
  'er.event_start_date',
  'er.event_url',
  'er.event_city',
  'er.event_country',
  'er.event_registration_type',
  'er.project_id',
  'er.project_slug',
  'er.email',
  'er.full_name',
  'er.first_name',
  'er.last_name',
  'er.job_title',
  'er.user_country',
  'er.user_photo_url',
  'er.account_name',
  'er.logo_url',
  'er.organization_size',
  'er.organization_industry',
]

const EVENT_HOST = `REGEXP_SUBSTR(LOWER(er.event_url), '^https?://([^/]+)', 1, 1, 'e', 1)`

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
    SELECT account_id, website, domain_aliases
    FROM analytics.bronze_fivetran_salesforce.accounts
    WHERE website IS NOT NULL
    UNION ALL
    SELECT account_id, website, domain_aliases
    FROM analytics.bronze_fivetran_salesforce_b2b.accounts
    WHERE website IS NOT NULL
  )`

const DEDUP = `
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY er.registration_id
    ORDER BY org.website DESC NULLS LAST, LFID ASC NULLS LAST
  ) = 1`

const buildSelect = (): string => {
  let select = `
  SELECT
    ${SELECTED_COLUMNS.join(',\n    ')},
    CONVERT_TIMEZONE('UTC', er.registration_ts)::TIMESTAMP_NTZ AS registration_ts,
    org.website AS ORG_WEBSITE,
    org.domain_aliases AS ORG_DOMAIN_ALIASES,
    mu.user_name AS LFID
  FROM analytics.silver_fact.event_registrations er
  INNER JOIN cdp_matched_segments cms
    ON cms.slug = er.project_slug
    AND cms.sourceId = er.project_id
  LEFT JOIN analytics.bronze_fivetran_salesforce.bronze_salesforce_merged_user mu
    ON er.user_id = mu.user_id
    AND mu.user_name IS NOT NULL
  LEFT JOIN org_accounts org
    ON er.account_id = org.account_id
  WHERE er.registration_source = 'gatewaze'
    AND ${EVENT_HOST} = '${LUMA_EVENT_HOST}'
    AND er.event_id = '${FIRST_INGEST_EVENT_ID}'
    AND (
      er.registration_status = 'Accepted'
      OR (er.user_attended AND er.user_checked_in_ts IS NOT NULL)
    )
    AND ${personalEmailCondition('er.email')}`

  if (!IS_PROD_ENV) {
    select += `
    AND er.project_slug = '${NON_PROD_PROJECT_SLUG}'`
  }

  return select
}

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  const select = buildSelect()

  if (!sinceTimestamp) {
    return `
  WITH ${ORG_ACCOUNTS},
  ${CDP_MATCHED_SEGMENTS}
  ${select}
  ${DEDUP}`.trim()
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
  ${select}
    AND er.updated_ts > '${sinceTimestamp}'
  ${DEDUP}

  UNION

  ${select}
    AND EXISTS (
      SELECT 1 FROM new_cdp_segments ncs
      WHERE ncs.slug = cms.slug AND ncs.sourceId = cms.sourceId
    )
  ${DEDUP}`.trim()
}
