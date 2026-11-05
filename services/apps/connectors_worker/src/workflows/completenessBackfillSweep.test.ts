import { beforeEach, describe, expect, it, vi } from 'vitest'

import { completenessBackfillSweep } from './completenessBackfillSweep'

const mocks = vi.hoisted(() => ({
  dueBackfillChecks: vi.fn(),
  completeBackfillCheck: vi.fn(),
  executeChild: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({
    dueBackfillChecks: mocks.dueBackfillChecks,
    completeBackfillCheck: mocks.completeBackfillCheck,
  }),
  executeChild: mocks.executeChild,
  log: mocks.log,
}))

vi.mock('./validateCompleteness', () => ({ validateCompleteness: 'validateCompleteness' }))

describe('completenessBackfillSweep', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('starts one child per due check counting up to its onboarding, completes every check including a failed one and logs the summary', async () => {
    const checks = Array.from({ length: 12 }, (_, i) => ({
      integrationId: 'i',
      channelId: `c${String(i).padStart(2, '0')}`,
      onboardedAt: '2026-10-29T09:05:00Z',
    }))
    mocks.dueBackfillChecks.mockResolvedValue(checks)
    mocks.executeChild.mockImplementation(async (_workflow, options) => {
      if (options.args[0].target.channelId === 'c03') throw new Error('boom')
      return { resultCount: 2, failureCount: 0 }
    })

    await completenessBackfillSweep()

    expect(mocks.executeChild).toHaveBeenCalledTimes(12)
    expect(mocks.executeChild).toHaveBeenCalledWith('validateCompleteness', {
      workflowId: 'validate-completeness-backfill-i-c00',
      workflowExecutionTimeout: '2 hours',
      args: [
        {
          target: { kind: 'channelId', integrationId: 'i', channelId: 'c00' },
          window: { kind: 'all-time', to: '2026-10-29T09:05:00Z' },
          counterKeys: null,
        },
      ],
    })
    expect(mocks.log.error).toHaveBeenCalledTimes(1)
    expect(mocks.completeBackfillCheck.mock.calls.map(([check]) => check)).toEqual(checks)
    expect(mocks.log.info).toHaveBeenCalledWith('completeness_backfill_sweep_summary', {
      dueCount: 12,
      failedCount: 1,
    })
  })
})
