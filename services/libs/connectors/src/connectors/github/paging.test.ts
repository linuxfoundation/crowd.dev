import { describe, expect, it } from 'vitest'

import { readWatermark } from './paging'

describe('readWatermark', () => {
  it('reads processed as a string map and reads anything else as an empty map', () => {
    const base = { phase: 'incremental', since: '2026-10-21T00:00:00.000Z' }

    expect(
      readWatermark({ ...base, processed: { '1': '2026-10-20T10:00:00.000Z', '2': 5, '3': null } })
        .processed,
    ).toEqual({ '1': '2026-10-20T10:00:00.000Z' })
    expect(readWatermark(base).processed).toEqual({})
    expect(readWatermark({ ...base, processed: ['2026-10-20T10:00:00.000Z'] }).processed).toEqual(
      {},
    )
    expect(readWatermark({ ...base, processed: 'x' }).processed).toEqual({})
    expect(readWatermark(null).processed).toEqual({})
  })
})
