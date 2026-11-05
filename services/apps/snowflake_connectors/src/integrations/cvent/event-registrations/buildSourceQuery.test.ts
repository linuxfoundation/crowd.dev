import { describe, expect, it } from 'vitest'

import { personalEmailCondition } from '../../../core/personalEmail'
import { buildSourceQuery } from './buildSourceQuery'

describe('buildSourceQuery', () => {
  it.each([undefined, '2026-11-04 00:00:00'])(
    'exports registrations without a resolvable LFID and still resolves it when one exists (since %j)',
    (sinceTimestamp) => {
      const query = buildSourceQuery(sinceTimestamp)

      expect(query).not.toMatch(/u\.lf_username\s+\)\s+IS NOT NULL/)
      expect(query).toMatch(/u\.lf_username\s+\)\s+AS LFID/)
      expect(query).toContain(`WHERE ${personalEmailCondition('er.email')}`)
    },
  )
})
