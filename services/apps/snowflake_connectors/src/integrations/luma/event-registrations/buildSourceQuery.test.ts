import { describe, expect, it } from 'vitest'

import { personalEmailCondition } from '../../../core/personalEmail'
import { FIRST_INGEST_EVENT_ID, LUMA_EVENT_HOST, buildSourceQuery } from './buildSourceQuery'

describe('buildSourceQuery', () => {
  it.each([undefined, '2026-11-04T00:00:00.000Z'])(
    'exports only accepted or checked-in luma registrations of the first event (since %j)',
    (sinceTimestamp) => {
      const query = buildSourceQuery(sinceTimestamp)
      const branches = sinceTimestamp ? 2 : 1

      expect(query.split("er.registration_source = 'gatewaze'")).toHaveLength(branches + 1)
      expect(query.split(`= '${LUMA_EVENT_HOST}'`)).toHaveLength(branches + 1)
      expect(query.split(`er.event_id = '${FIRST_INGEST_EVENT_ID}'`)).toHaveLength(branches + 1)
      expect(query).toContain(
        "er.registration_status = 'Accepted'\n      OR (er.user_attended AND er.user_checked_in_ts IS NOT NULL)",
      )
      expect(query).toContain(personalEmailCondition('er.email'))
      expect(query).toContain(
        "CONVERT_TIMEZONE('UTC', er.registration_ts)::TIMESTAMP_NTZ AS registration_ts",
      )
      expect(query).toMatch(/COALESCE\(mu\.user_name, u\.lf_username\) AS LFID/)
      expect(query).not.toMatch(/er\.\*/)
      expect(query.includes(`er.updated_ts > '${sinceTimestamp}'`)).toBe(Boolean(sinceTimestamp))
    },
  )
})
