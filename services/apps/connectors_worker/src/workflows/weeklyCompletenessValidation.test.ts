import { beforeEach, describe, expect, it, vi } from 'vitest'

import { weeklyCompletenessValidation } from './weeklyCompletenessValidation'

const mocks = vi.hoisted(() => ({
  listValidationChannels: vi.fn(),
  executeChild: vi.fn(),
  continueAsNew: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({ listValidationChannels: mocks.listValidationChannels }),
  executeChild: mocks.executeChild,
  continueAsNew: mocks.continueAsNew,
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
      if (options.args[0].target.channelId === 'c03') throw new Error('boom')
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
          target: { kind: 'channelId', integrationId: 'i', channelId: 'c00' },
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

  it('continues as new after 50 pages with the cursor and totals, and resumes from a given state', async () => {
    const channels = Array.from({ length: 600 }, (_, i) => ({
      integrationId: 'i',
      channelId: `c${String(i).padStart(3, '0')}`,
      channelName: `repo-${i}`,
    }))
    mocks.listValidationChannels.mockImplementation(async ({ after, limit }) => {
      const start = after ? channels.findIndex((c) => c.channelId === after.channelId) + 1 : 0
      return channels.slice(start, start + limit)
    })
    mocks.executeChild.mockResolvedValue({ resultCount: 2, failureCount: 1 })
    mocks.continueAsNew.mockRejectedValue(new Error('continued'))

    await expect(weeklyCompletenessValidation()).rejects.toThrow('continued')

    const window = {
      kind: 'range',
      from: '2026-10-24T00:00:00.000Z',
      to: '2026-10-31T00:00:00.000Z',
    } as const
    const state = {
      window,
      after: { integrationId: 'i', channelId: 'c499' },
      repoCount: 500,
      failedRepoCount: 0,
      resultCount: 1000,
      failureCount: 500,
    }
    expect(mocks.continueAsNew).toHaveBeenCalledWith(state)
    expect(mocks.log.info).not.toHaveBeenCalled()

    vi.setSystemTime(new Date('2026-11-07T06:00:00Z'))
    mocks.continueAsNew.mockReset()
    mocks.listValidationChannels.mockClear()

    await weeklyCompletenessValidation(state)

    expect(mocks.listValidationChannels.mock.calls[0][0]).toEqual({
      after: state.after,
      limit: 10,
    })
    expect(mocks.executeChild.mock.lastCall?.[1].args[0].window).toEqual(window)
    expect(mocks.log.info).toHaveBeenCalledWith('weekly_completeness_validation_summary', {
      repoCount: 600,
      failedRepoCount: 0,
      resultCount: 1200,
      failureCount: 600,
    })
  })
})
