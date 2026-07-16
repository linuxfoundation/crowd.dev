import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getExistingLast30dEndDates,
  upsertLast30dDownload,
} from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { monthlyWindowFor, persistPackagist30dWindow } from '../downloads'

vi.mock('@crowd/data-access-layer/src/packages', () => ({
  getExistingLast30dEndDates: vi.fn().mockResolvedValue([]),
  upsertLast30dDownload: vi.fn().mockResolvedValue([]),
}))

const mockExistingWindows = vi.mocked(getExistingLast30dEndDates)
const mockWindow = vi.mocked(upsertLast30dDownload)

const qx = {} as QueryExecutor
const PURL = 'pkg:composer/monolog/monolog'

beforeEach(() => {
  vi.clearAllMocks()
  mockExistingWindows.mockResolvedValue([])
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
describe('persistPackagist30dWindow', () => {
  it('writes the window with the mirror when the month has no row yet', async () => {
    await persistPackagist30dWindow(qx, PURL, 300, '2026-07-01')

    expect(mockExistingWindows).toHaveBeenCalledWith(qx, PURL, '2026-07-01', '2026-07-01')
    expect(mockWindow).toHaveBeenCalledWith(qx, PURL, '2026-06-01', '2026-07-01', 300, true)
  })

  it('does not overwrite an existing window for the month', async () => {
    mockExistingWindows.mockResolvedValue(['2026-07-01'])

    await persistPackagist30dWindow(qx, PURL, 300, '2026-07-15')

    expect(mockWindow).not.toHaveBeenCalled()
  })

  it('writes nothing when the registry reported no monthly count', async () => {
    await persistPackagist30dWindow(qx, PURL, null, '2026-07-01')

    expect(mockExistingWindows).not.toHaveBeenCalled()
    expect(mockWindow).not.toHaveBeenCalled()
  })
})
