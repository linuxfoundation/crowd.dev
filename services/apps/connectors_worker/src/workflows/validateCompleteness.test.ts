import { beforeEach, describe, expect, it, vi } from 'vitest'

import { validateCompleteness } from './validateCompleteness'

const mocks = vi.hoisted(() => ({
  listValidationUnits: vi.fn(),
  validateUnit: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({
    listValidationUnits: mocks.listValidationUnits,
    validateUnit: mocks.validateUnit,
  }),
  log: mocks.log,
}))

const input = {
  target: { kind: 'platform' as const, platform: 'github' },
  window: { kind: 'all-time' as const },
  counterKeys: null,
}

describe('validateCompleteness', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('pages until an empty page and counts a rejected unit as a failure while others still run', async () => {
    const units = Array.from({ length: 12 }, (_, i) => ({ id: `u${String(i).padStart(2, '0')}` }))
    mocks.listValidationUnits.mockImplementation(async ({ afterId, limit }) => {
      const start = afterId ? units.findIndex((unit) => unit.id === afterId) + 1 : 0
      return units.slice(start, start + limit)
    })
    mocks.validateUnit.mockImplementation(async ({ unit }) => {
      if (unit.id === 'u03') throw new Error('boom')
      return {
        unitId: unit.id,
        written: 2,
        failures: unit.id === 'u05' ? [{ unitId: 'u05', counterKey: 'a', errorMessage: 'x' }] : [],
        noCounters: unit.id === 'u11',
      }
    })

    const summary = await validateCompleteness(input)

    expect(mocks.listValidationUnits.mock.calls.map(([call]) => call.afterId)).toEqual([
      undefined,
      'u09',
      'u11',
    ])
    expect(summary).toEqual({
      unitCount: 12,
      resultCount: 22,
      failureCount: 2,
      noCounterUnitCount: 1,
    })
    expect(mocks.log.info).toHaveBeenCalledWith('completeness_validation_summary', summary)
    expect(mocks.log.error).toHaveBeenCalledWith(
      'completeness validation counter failed',
      expect.objectContaining({ unitId: 'u03', errorMessage: 'boom' }),
    )
  })
})
