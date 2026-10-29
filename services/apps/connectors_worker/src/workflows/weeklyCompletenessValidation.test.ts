import { beforeEach, describe, expect, it, vi } from 'vitest'

import { weeklyCompletenessValidation } from './weeklyCompletenessValidation'

const mocks = vi.hoisted(() => ({
  listValidationChannels: vi.fn(),
  executeChild: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({ listValidationChannels: mocks.listValidationChannels }),
  executeChild: mocks.executeChild,
  log: mocks.log,
}))

vi.mock('./validateCompleteness', () => ({ validateCompleteness: 'validateCompleteness' }))

describe('weeklyCompletenessValidation', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-10-31T06:00:00Z'))
  })

  it('pages channels until empty, starts one child per channel and counts a rejected child as a failed repo', async () => {
    const channels = Array.from({ length: 12 }, (_, i) => ({
      integrationId: 'i',
      channelId: `c${String(i).padStart(2, '0')}`,
      channelName: `repo-${i}`,
    }))
    mocks.listValidationChannels.mockImplementation(async ({ after, limit }) => {
      const start = after ? channels.findIndex((c) => c.channelId === after.channelId) + 1 : 0
      return channels.slice(start, start + limit)
    })
    mocks.executeChild.mockImplementation(async (_workflow, options) => {
      if (options.args[0].target.channelName === 'repo-3') throw new Error('boom')
      return { unitCount: 1, resultCount: 2, failureCount: 1, noCounterUnitCount: 0 }
    })

    await weeklyCompletenessValidation()

    expect(mocks.listValidationChannels.mock.calls.map(([input]) => input)).toEqual([
      { after: undefined, limit: 10 },
      { after: { integrationId: 'i', channelId: 'c09' }, limit: 10 },
      { after: { integrationId: 'i', channelId: 'c11' }, limit: 10 },
    ])
    expect(mocks.executeChild).toHaveBeenCalledTimes(12)
    expect(mocks.executeChild).toHaveBeenCalledWith('validateCompleteness', {
      workflowId: 'validate-completeness-i-c00-2026-10-24',
      workflowExecutionTimeout: '2 hours',
      args: [
        {
          target: { kind: 'channel', channelName: 'repo-0', integrationId: 'i' },
          window: {
            kind: 'range',
            from: '2026-10-24T00:00:00.000Z',
            to: '2026-10-31T00:00:00.000Z',
          },
          counterKeys: null,
        },
      ],
    })
    expect(mocks.log.error).toHaveBeenCalledTimes(1)
    expect(mocks.log.info).toHaveBeenCalledWith('weekly_completeness_validation_summary', {
      repoCount: 12,
      failedRepoCount: 1,
      resultCount: 22,
      failureCount: 11,
    })
  })
})
