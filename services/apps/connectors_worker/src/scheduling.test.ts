import { describe, expect, it } from 'vitest'

import { MAX_REQUESTED_DELAY_MS, MIN_REQUESTED_DELAY_MS, requestedRunAt } from './scheduling'

const NOW = new Date('2026-11-25T10:00:00.000Z')
const after = (ms: number) => new Date(NOW.getTime() + ms)

describe('requestedRunAt', () => {
  it('keeps a requested time inside the bounds', () => {
    const requested = after(24 * 60 * 60_000)

    expect(requestedRunAt(requested, NOW)).toEqual(requested)
  })

  it('moves a past time to one minute from now', () => {
    expect(requestedRunAt(new Date('2026-11-20T10:00:00.000Z'), NOW)).toEqual(
      after(MIN_REQUESTED_DELAY_MS),
    )
    expect(MIN_REQUESTED_DELAY_MS).toBe(60_000)
  })

  it('caps a time two years ahead at 31 days from now', () => {
    expect(requestedRunAt(new Date('2028-11-25T10:00:00.000Z'), NOW)).toEqual(
      after(MAX_REQUESTED_DELAY_MS),
    )
    expect(MAX_REQUESTED_DELAY_MS).toBe(31 * 24 * 60 * 60_000)
  })

  it('throws on an invalid date', () => {
    expect(() => requestedRunAt(new Date('not a date'), NOW)).toThrow(
      'requested next run is not a valid date',
    )
  })
})
