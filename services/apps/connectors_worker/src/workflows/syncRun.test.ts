import { ActivityFailure, ApplicationFailure, TimeoutFailure } from '@temporalio/workflow'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { SYNC_RUN_FAILURE_RECORDED } from '../syncRunFailure'
import { syncRun } from './syncRun'

const mocks = vi.hoisted(() => ({
  executeSync: vi.fn(),
  recordSyncRunTimeout: vi.fn(),
}))

vi.mock('@temporalio/workflow', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@temporalio/workflow')>()),
  proxyActivities: () => ({
    executeSync: mocks.executeSync,
    recordSyncRunTimeout: mocks.recordSyncRunTimeout,
  }),
}))

function activityFailure(cause: Error): ActivityFailure {
  return new ActivityFailure(
    'activity failed',
    'executeSync',
    '1',
    'MAXIMUM_ATTEMPTS_REACHED',
    undefined,
    cause,
  )
}

describe('syncRun', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('completes without failing when the run already recorded its failure', async () => {
    mocks.executeSync.mockRejectedValue(
      activityFailure(
        ApplicationFailure.create({
          message: 'github graphql response has no data',
          type: SYNC_RUN_FAILURE_RECORDED,
        }),
      ),
    )

    await expect(syncRun('u')).resolves.toBeUndefined()
    expect(mocks.recordSyncRunTimeout).not.toHaveBeenCalled()
  })

  it('fails when the sync activity fails without recording the failure', async () => {
    const failure = activityFailure(
      ApplicationFailure.create({ message: 'connection terminated unexpectedly' }),
    )
    mocks.executeSync.mockRejectedValue(failure)

    await expect(syncRun('u')).rejects.toBe(failure)
    expect(mocks.recordSyncRunTimeout).not.toHaveBeenCalled()
  })

  it('records a heartbeat timeout and completes', async () => {
    mocks.executeSync.mockRejectedValue(
      activityFailure(new TimeoutFailure('heartbeat timeout', undefined, 'HEARTBEAT')),
    )

    await expect(syncRun('u')).resolves.toBeUndefined()
    expect(mocks.recordSyncRunTimeout).toHaveBeenCalledWith('u', 'HEARTBEAT', expect.any(String))
  })

  it('fails when recording the timeout fails', async () => {
    mocks.executeSync.mockRejectedValue(
      activityFailure(new TimeoutFailure('start to close timeout', undefined, 'START_TO_CLOSE')),
    )
    mocks.recordSyncRunTimeout.mockRejectedValue(new Error('db down'))

    await expect(syncRun('u')).rejects.toThrow('db down')
  })

  it('rethrows anything that is not an activity failure', async () => {
    mocks.executeSync.mockRejectedValue(new Error('workflow bug'))

    await expect(syncRun('u')).rejects.toThrow('workflow bug')
  })
})
