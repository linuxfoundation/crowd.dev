import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'

import { dispatcher } from './dispatcher'

const mocks = vi.hoisted(() => ({
  touchHeartbeat: vi.fn(),
  claimDue: vi.fn(),
  admitByBudget: vi.fn(),
  startRun: vi.fn(),
  deferUnit: vi.fn(),
  logDispatchSummary: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({
    touchHeartbeat: mocks.touchHeartbeat,
    claimDue: mocks.claimDue,
    admitByBudget: mocks.admitByBudget,
    startRun: mocks.startRun,
    deferUnit: mocks.deferUnit,
    logDispatchSummary: mocks.logDispatchSummary,
  }),
  log: mocks.log,
}))

function unit(id: string, leaseLapsed = false): IClaimedUnit {
  return {
    id,
    integrationId: 'integration-1',
    platform: 'github',
    syncName: 'issues',
    channelId: `channel-${id}`,
    channelName: `https://github.com/org/${id}`,
    leaseLapsed,
  }
}

describe('dispatcher', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('claims 300 units, starts them 25 at a time and counts every outcome without stopping on a failed start', async () => {
    const admitted = Array.from({ length: 27 }, (_, index) => unit(`u${index + 1}`))
    const deferred = [unit('d1'), unit('d2', true)]
    mocks.claimDue.mockResolvedValue([...admitted, ...deferred])
    mocks.admitByBudget.mockResolvedValue({ admitted, deferred })
    const releaseFirstBatch: Array<() => void> = []
    mocks.startRun.mockImplementation(({ id }: IClaimedUnit) => {
      if (id === 'u26') {
        return Promise.resolve('alreadyRunning')
      }
      if (id === 'u27') {
        return Promise.reject(new Error('temporal unavailable'))
      }
      return new Promise<string>((resolve) => releaseFirstBatch.push(() => resolve('started')))
    })

    const tick = dispatcher()
    await vi.waitFor(() => expect(mocks.startRun).toHaveBeenCalledTimes(25))
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(mocks.startRun).toHaveBeenCalledTimes(25)

    releaseFirstBatch.forEach((release) => release())
    await tick

    expect(mocks.claimDue).toHaveBeenCalledWith(300)
    expect(mocks.startRun.mock.calls.map(([started]) => started.id)).toEqual(
      admitted.map(({ id }) => id),
    )
    expect(mocks.log.error).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ unitId: 'u27' }),
    )
    expect(mocks.deferUnit.mock.calls).toEqual([
      ['d1', false],
      ['d2', true],
    ])
    expect(mocks.logDispatchSummary).toHaveBeenCalledWith({
      claimed: 29,
      admitted: 27,
      deferred: 2,
      started: 25,
      alreadyRunning: 1,
      failed: 1,
      durationMs: expect.any(Number),
    })
  })
})
