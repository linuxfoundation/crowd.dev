import { ActivityFailure, TimeoutFailure } from '@temporalio/workflow'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { mirrorSweep } from './mirrorSweep'

const mocks = vi.hoisted(() => ({
  countShadowRepos: vi.fn(),
  mirrorBatch: vi.fn(),
  logMirrorSweepSummary: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@temporalio/workflow')>()),
  proxyActivities: () => ({
    countShadowRepos: mocks.countShadowRepos,
    mirrorBatch: mocks.mirrorBatch,
    logMirrorSweepSummary: mocks.logMirrorSweepSummary,
  }),
  log: mocks.log,
}))

const timedOut = (lastHeartbeatDetails: unknown) =>
  new ActivityFailure(
    'activity failed',
    'mirrorBatch',
    '1',
    'MAXIMUM_ATTEMPTS_REACHED',
    undefined,
    new TimeoutFailure('heartbeat timeout', lastHeartbeatDetails, 'HEARTBEAT'),
  )

describe('mirrorSweep', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('mirrors up to the room left in the shadow pool and logs the summary, failed or not', async () => {
    mocks.countShadowRepos.mockResolvedValue(4600)
    mocks.mirrorBatch.mockResolvedValue({
      candidates: 900,
      attempted: 300,
      mirrored: 290,
      skipped: 10,
      unitsInserted: 2320,
      failed: true,
    })

    const summary = await mirrorSweep({ batchSize: 1000, shadowCap: 5000 })

    expect(mocks.mirrorBatch).toHaveBeenCalledWith(400)
    expect(summary).toMatchObject({
      batchSize: 1000,
      shadowCap: 5000,
      inShadow: 4600,
      candidates: 900,
      attempted: 300,
      mirrored: 290,
      skipped: 10,
      unitsInserted: 2320,
      failed: true,
    })
    expect(mocks.logMirrorSweepSummary).toHaveBeenCalledWith(summary)

    vi.clearAllMocks()
    mocks.countShadowRepos.mockResolvedValue(0)
    mocks.mirrorBatch.mockRejectedValue(
      timedOut({ candidates: 900, attempted: 600, mirrored: 590, unitsInserted: 4720 }),
    )

    const timedOutRun = await mirrorSweep({ batchSize: 1000, shadowCap: 5000 })

    expect(mocks.mirrorBatch).toHaveBeenCalledWith(1000)
    expect(timedOutRun).toMatchObject({
      candidates: 900,
      attempted: 600,
      mirrored: 590,
      unitsInserted: 4720,
      failed: true,
    })
    expect(mocks.logMirrorSweepSummary).toHaveBeenCalledWith(timedOutRun)

    mocks.mirrorBatch.mockRejectedValue(new Error('worker lost'))
    await expect(mirrorSweep({ batchSize: 1000, shadowCap: 5000 })).resolves.toMatchObject({
      attempted: 0,
      failed: true,
    })
  })

  it('does not mirror anything when the shadow pool is full, but still logs the summary', async () => {
    mocks.countShadowRepos.mockResolvedValue(5000)

    const summary = await mirrorSweep({ batchSize: 500, shadowCap: 5000 })

    expect(mocks.mirrorBatch).not.toHaveBeenCalled()
    expect(summary).toMatchObject({ inShadow: 5000, candidates: 0, attempted: 0, failed: false })
    expect(mocks.logMirrorSweepSummary).toHaveBeenCalledWith(summary)

    vi.clearAllMocks()
    await expect(mirrorSweep({ batchSize: 0, shadowCap: 5000 })).rejects.toThrow('batchSize')
    await expect(mirrorSweep({ shadowCap: 5000 } as never)).rejects.toThrow('batchSize')
    await expect(mirrorSweep({ batchSize: 600, shadowCap: 500 })).rejects.toThrow('shadowCap')
    expect(mocks.countShadowRepos).not.toHaveBeenCalled()
    expect(mocks.logMirrorSweepSummary).not.toHaveBeenCalled()
  })
})
