import { describe, expect, it } from 'vitest'

import { computeChunks } from '../downloadGaps'

describe('computeChunks', () => {
  it('returns no windows for an empty list', () => {
    expect(computeChunks([])).toEqual([])
  })

  it('wraps a single date into a one-day window', () => {
    expect(computeChunks(['2020-01-01'])).toEqual([{ start: '2020-01-01', end: '2020-01-01' }])
  })

  it('merges consecutive dates into one window', () => {
    expect(computeChunks(['2020-01-01', '2020-01-02', '2020-01-03'])).toEqual([
      { start: '2020-01-01', end: '2020-01-03' },
    ])
  })

  it('splits on a non-consecutive gap', () => {
    expect(computeChunks(['2020-01-01', '2020-01-02', '2020-01-05', '2020-01-06'])).toEqual([
      { start: '2020-01-01', end: '2020-01-02' },
      { start: '2020-01-05', end: '2020-01-06' },
    ])
  })

  it('caps a contiguous run at maxDays', () => {
    expect(computeChunks(['2020-01-01', '2020-01-02', '2020-01-03'], 2)).toEqual([
      { start: '2020-01-01', end: '2020-01-02' },
      { start: '2020-01-03', end: '2020-01-03' },
    ])
  })

  it('treats a month boundary as consecutive', () => {
    expect(computeChunks(['2020-01-31', '2020-02-01'])).toEqual([
      { start: '2020-01-31', end: '2020-02-01' },
    ])
  })

  it('treats a year boundary as consecutive', () => {
    expect(computeChunks(['2019-12-31', '2020-01-01'])).toEqual([
      { start: '2019-12-31', end: '2020-01-01' },
    ])
  })
})
