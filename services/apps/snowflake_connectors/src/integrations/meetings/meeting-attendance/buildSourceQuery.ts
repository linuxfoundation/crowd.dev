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

export const buildSourceQuery = (sinceTimestamp?: string): string => {
  let select = `
  SELECT
    t.PRIMARY_KEY,
    t.MEETING_ID,
    t.MEETING_NAME,
    t.PROJECT_ID,
    t.PROJECT_NAME,
    t.PROJECT_SLUG,
    t.ACCOUNT_ID,
    t.ACCOUNT_NAME,
    t.MEETING_DATE,
    t.MEETING_TIME,
    t.INVITEE_FULL_NAME,
    t.INVITEE_LF_SSO,
    t.INVITEE_LF_USER_ID,
    t.INVITEE_EMAIL,
    t.INVITEE_ATTENDED,
    t.WAS_INVITED,
    t.RAW_COMMITTEE_TYPE
  FROM ANALYTICS.SILVER_FACT.MEETING_ATTENDANCE t
  INNER JOIN cdp_matched_segments cms
    ON cms.slug = t.PROJECT_SLUG
    AND cms.sourceId = t.PROJECT_ID
  WHERE (t.WAS_INVITED = TRUE OR t.INVITEE_ATTENDED = TRUE)`

  if (!IS_PROD_ENV) {
    select += ` AND t.PROJECT_SLUG = 'cncf'`
  }

  const dedup = `
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.PRIMARY_KEY ORDER BY t.MEETING_DATE DESC) = 1`

  if (!sinceTimestamp) {
    return `
  WITH ${CDP_MATCHED_SEGMENTS}
  ${select}
  ${dedup}`.trim()
  }

  return `
  WITH ${CDP_MATCHED_SEGMENTS},
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
    AND t.MEETING_DATE > '${sinceTimestamp}'::DATE
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
