import { describe, expect, it } from 'vitest'

import { computeMissingLast30dWindows } from '../last30dGaps'

describe('computeMissingLast30dWindows', () => {
  it('clamps to NPM_EARLIEST when firstReleaseAt is null and clamps the start date', () => {
    // lower = NPM_EARLIEST (2015-01-10); the 2015-01-01 end_date is < earliest
    // so it is skipped, and the 2015-02-01 window start clamps to the earliest.
    expect(computeMissingLast30dWindows(null, '2015-03-01', [])).toEqual([
      { start: '2015-01-10', end: '2015-02-01', isLatest: false },
      { start: '2015-01-30', end: '2015-03-01', isLatest: true },
    ])
  })

  it('returns no windows when firstReleaseAt is after the upper bound', () => {
    expect(computeMissingLast30dWindows('2030-05-15', '2020-01-01', [])).toEqual([])
  })

  it('skips months whose end_date is already present', () => {
    expect(computeMissingLast30dWindows('2020-01-15', '2020-03-01', ['2020-02-01'])).toEqual([
      { start: '2019-12-02', end: '2020-01-01', isLatest: false },
      { start: '2020-01-31', end: '2020-03-01', isLatest: true },
    ])
  })

  it('marks only the final (upper-bound) month as isLatest', () => {
    const out = computeMissingLast30dWindows('2019-11-15', '2020-01-01', [])
    expect(out).toEqual([
      { start: '2019-10-02', end: '2019-11-01', isLatest: false },
      { start: '2019-11-01', end: '2019-12-01', isLatest: false },
      { start: '2019-12-02', end: '2020-01-01', isLatest: true },
    ])
    expect(out.filter((w) => w.isLatest)).toHaveLength(1)
  })

  it('handles a single month (firstMonth === upper bound)', () => {
    expect(computeMissingLast30dWindows('2020-06-10', '2020-06-01', [])).toEqual([
      { start: '2020-05-02', end: '2020-06-01', isLatest: true },
    ])
  })
})
