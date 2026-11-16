import { describe, expect, it } from 'vitest'

import { coverDates } from '../downloadGaps'

describe('coverDates', () => {
  it('returns no ranges for an empty list', () => {
    expect(coverDates([])).toEqual([])
  })

  it('wraps a single date into a one-day range', () => {
    expect(coverDates(['2020-01-01'])).toEqual([{ start: '2020-01-01', end: '2020-01-01' }])
  })

  it('merges consecutive dates into one range', () => {
    expect(coverDates(['2020-01-01', '2020-01-02', '2020-01-03'])).toEqual([
      { start: '2020-01-01', end: '2020-01-03' },
    ])
  })

  it('spans gaps that fit inside one range, since refetching a stored day costs no request', () => {
    expect(coverDates(['2020-01-01', '2020-01-02', '2020-03-05', '2020-06-30'])).toEqual([
      { start: '2020-01-01', end: '2020-06-30' },
    ])
  })

  it('starts a new range at the first date past maxDays', () => {
    expect(coverDates(['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-09'], 2)).toEqual([
      { start: '2020-01-01', end: '2020-01-02' },
      { start: '2020-01-03', end: '2020-01-03' },
      { start: '2020-01-09', end: '2020-01-09' },
    ])
  })

  it('ends a range at its last needed date instead of the full maxDays', () => {
    expect(coverDates(['2020-01-01', '2020-01-04'], 10)).toEqual([
      { start: '2020-01-01', end: '2020-01-04' },
    ])
  })

  it('keeps every range within 540 days by default', () => {
    const start = Date.UTC(2024, 0, 1)
    const dates = Array.from({ length: 1000 }, (_, i) =>
      new Date(start + i * 86_400_000).toISOString().slice(0, 10),
    )

    const ranges = coverDates(dates)

    expect(ranges).toEqual([
      { start: '2024-01-01', end: '2025-06-23' },
      { start: '2025-06-24', end: '2026-09-26' },
    ])
  })

  it('treats month and year boundaries as consecutive', () => {
    expect(coverDates(['2019-12-31', '2020-01-01', '2020-01-31', '2020-02-01'], 33)).toEqual([
      { start: '2019-12-31', end: '2020-02-01' },
    ])
  })
})
