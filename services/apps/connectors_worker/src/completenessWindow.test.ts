import { describe, expect, it } from 'vitest'

import { previousValidationWeek } from './completenessWindow'

const week = (from: string, to: string) => ({ kind: 'range', from, to })

describe('previousValidationWeek', () => {
  it('returns the week ending at the Saturday midnight before the Saturday 06:00 run', () => {
    expect(previousValidationWeek(new Date('2026-10-31T06:00:00Z'))).toEqual(
      week('2026-10-24T00:00:00.000Z', '2026-10-31T00:00:00.000Z'),
    )
  })

  it('treats Saturday midnight itself as the end of the week', () => {
    expect(previousValidationWeek(new Date('2026-10-31T00:00:00Z'))).toEqual(
      week('2026-10-24T00:00:00.000Z', '2026-10-31T00:00:00.000Z'),
    )
  })

  it('returns the previous week just before Saturday midnight', () => {
    expect(previousValidationWeek(new Date('2026-10-30T23:59:59Z'))).toEqual(
      week('2026-10-17T00:00:00.000Z', '2026-10-24T00:00:00.000Z'),
    )
  })
})
