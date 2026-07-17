import { beforeEach, describe, expect, it, vi } from 'vitest'

import { insertLast30dDownloadIfAbsent } from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { monthlyWindowFor, persistPackagist30dWindow } from '../downloads'

vi.mock('@crowd/data-access-layer/src/packages', () => ({
  insertLast30dDownloadIfAbsent: vi.fn().mockResolvedValue([]),
}))

const mockWindow = vi.mocked(insertLast30dDownloadIfAbsent)

const qx = {} as QueryExecutor
const PURL = 'pkg:composer/monolog/monolog'

beforeEach(() => {
  vi.clearAllMocks()
})

// Observed rolling window anchored on the 1st of the run month,
// start_date = end_date − 30 days.
describe('monthlyWindowFor', () => {
  it('anchors the window on the 1st of the run month', () => {
    expect(monthlyWindowFor('2026-07-15')).toEqual({
      startDate: '2026-06-01',
      endDate: '2026-07-01',
    })
    expect(monthlyWindowFor('2026-07-01')).toEqual({
      startDate: '2026-06-01',
      endDate: '2026-07-01',
    })
  })

  it('computes start_date by calendar arithmetic, not "previous month 1st"', () => {
    expect(monthlyWindowFor('2026-03-05')).toEqual({
      startDate: '2026-01-30',
      endDate: '2026-03-01',
    })
  })
})

// The monthly downloads-30d lane: one window row per purl per month, mirrored to
// packages.downloads_last_30d (npm parity). A window already recorded for the month
// is never overwritten — the value is the observation closest to the boundary.
// insertLast30dDownloadIfAbsent does the presence check and the write atomically, so
// there is no separate "does it exist" call to assert on here — a race is exercised
// directly against Postgres in downloadsLast30d's own DAL-level coverage.
describe('persistPackagist30dWindow', () => {
  it('writes the window with the mirror and returns the changed fields when the month has no row yet', async () => {
    mockWindow.mockResolvedValue(['downloads_last_30d.count', 'packages.downloads_last_30d'])

    const changedFields = await persistPackagist30dWindow(qx, PURL, 300, '2026-07-01')

    expect(mockWindow).toHaveBeenCalledWith(qx, PURL, '2026-06-01', '2026-07-01', 300, true)
    expect(changedFields).toEqual(['downloads_last_30d.count', 'packages.downloads_last_30d'])
  })

  it('returns no changed fields when a window for the month already exists', async () => {
    mockWindow.mockResolvedValue([])

    const changedFields = await persistPackagist30dWindow(qx, PURL, 300, '2026-07-15')

    expect(mockWindow).toHaveBeenCalledWith(qx, PURL, '2026-06-01', '2026-07-01', 300, true)
    expect(changedFields).toEqual([])
  })

  it('writes nothing and returns no changed fields when the registry reported no monthly count', async () => {
    const changedFields = await persistPackagist30dWindow(qx, PURL, null, '2026-07-01')

    expect(mockWindow).not.toHaveBeenCalled()
    expect(changedFields).toEqual([])
  })
})
