import { describe, expect, it } from 'vitest'

import { personalEmailCondition } from '../../../core/personalEmail'
import { buildSourceQuery } from './buildSourceQuery'

describe('buildSourceQuery', () => {
  it.each([undefined, '2026-11-04 00:00:00'])(
    'exports registrations without a resolvable LFID and keeps the attendee LFID apart from the registrant LFID (since %j)',
    (sinceTimestamp) => {
      const query = buildSourceQuery(sinceTimestamp)

      const bevyLfid = `CASE WHEN er.registration_source = 'bevy' THEN er.user_name END`

      expect(query).not.toMatch(/\bLFID\s+IS NOT NULL/)
      expect(query).toContain(`COALESCE(${bevyLfid}, u.lf_username) AS LFID,`)
      expect(query).toContain(`COALESCE(${bevyLfid}, mu.user_name) AS REGISTRANT_LFID`)
      expect(query).toContain(`WHERE ${personalEmailCondition('er.email')}`)
    },
  )
})
