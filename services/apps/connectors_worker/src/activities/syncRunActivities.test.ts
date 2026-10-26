import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  ProviderAccessDeniedError,
  ProviderAuthError,
  ProviderContractError,
} from '@crowd/connectors'
import type { ConnectorError, SyncContext, SyncOutcome } from '@crowd/connectors'

import { executeSync, recordSyncRunTimeout } from './syncRunActivities'

const CADENCE_RUN_AT = new Date('2026-10-16T12:05:00.000Z')
const SHORT_DEFER_RUN_AT = new Date('2026-10-16T12:01:00.000Z')

const mocks = vi.hoisted(() => ({
  run: vi.fn(),
  getUnitById: vi.fn(),
  reassertUnitClaim: vi.fn(),
  recordRunSuccess: vi.fn(),
  recordRunFailure: vi.fn(),
  recordRunTimeout: vi.fn(),
  rescheduleUnit: vi.fn(),
  emittedCount: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}))

const activityCancellation = vi.hoisted(() => ({ controller: new AbortController() }))

vi.mock('@temporalio/activity', () => ({
  Context: {
    current: () => ({
      info: { workflowExecution: { workflowId: 'wf', runId: 'run' } },
      heartbeat: vi.fn(),
      cancellationSignal: activityCancellation.controller.signal,
    }),
  },
}))

vi.mock('../main', () => ({
  svc: {
    postgres: { writer: {} },
    redis: {},
    dataSinkWorkerEmitter: {},
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
  },
}))

vi.mock('@crowd/logging', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/logging')>()),
  getChildLogger: () => mocks.log,
}))

vi.mock('@crowd/connectors', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/connectors')>()),
  getManifest: () => ({}),
  getSync: () => ({ cadenceMinutes: 5, schema: {}, run: mocks.run }),
  findSync: () => ({ cadenceMinutes: 5 }),
  createTokenPool: () => ({ acquire: vi.fn(), park: vi.fn(), invalidate: vi.fn() }),
  createHttpClient: () => ({ requestCount: () => 0 }),
  createEmit: () => ({ emit: vi.fn(), emittedCount: mocks.emittedCount }),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  getUnitById: mocks.getUnitById,
  reassertUnitClaim: mocks.reassertUnitClaim,
  recordRunSuccess: mocks.recordRunSuccess,
  rescheduleUnit: mocks.rescheduleUnit,
  recordRunPartial: vi.fn(),
  recordRunFailure: mocks.recordRunFailure,
  recordRunTimeout: mocks.recordRunTimeout,
  parkUnit: vi.fn(),
  recordShadowRecords: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  fetchIntegrationById: async () => ({ segmentId: 'segment-1' }),
}))

vi.mock(
  '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo',
  () => ({
    default: class {
      publishExternalResult = vi.fn()
    },
  }),
)

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({
  dbStoreQx: vi.fn(() => ({})),
}))

vi.mock('../scheduling', () => ({
  cadenceRunAt: () => CADENCE_RUN_AT,
  shortDeferRunAt: () => SHORT_DEFER_RUN_AT,
  failureRunAt: () => SHORT_DEFER_RUN_AT,
}))

const UNIT = {
  id: 'unit-1',
  integrationId: 'integration-1',
  platform: 'octolens',
  syncName: 'mentionPoll',
  channelId: 'channel',
  channelName: 'channel',
  watermark: { cursor: 'before' },
  emitEnabled: true,
  consecutiveFailures: 0,
}

function runReturning(outcome: SyncOutcome) {
  mocks.run.mockImplementation(async (ctx: SyncContext<unknown>) => {
    await ctx.commitWatermark({ cursor: 'after' })
    return outcome
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  activityCancellation.controller = new AbortController()
  mocks.emittedCount.mockReturnValue(3)
  mocks.getUnitById.mockResolvedValue(UNIT)
  mocks.reassertUnitClaim.mockResolvedValue(true)
})

describe('executeSync worker shutdown', () => {
  it('stops at the next budget check once the worker asks for cancellation and keeps its progress', async () => {
    const budgetChecks: boolean[] = []
    mocks.run.mockImplementation(async (ctx: SyncContext<unknown>) => {
      budgetChecks.push(ctx.hasRunBudget())
      await ctx.commitWatermark({ cursor: 'after' })
      activityCancellation.controller.abort(new Error('WORKER_SHUTDOWN'))
      budgetChecks.push(ctx.hasRunBudget())
      return { complete: budgetChecks[1] }
    })

    await executeSync(UNIT.id)

    expect(budgetChecks).toEqual([true, false])
    expect(mocks.recordRunSuccess).toHaveBeenCalledWith(
      {},
      UNIT.id,
      { watermark: { cursor: 'after' }, emittedCount: 3, complete: false },
      SHORT_DEFER_RUN_AT,
    )
    expect(mocks.log.info).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'sync_run_summary', cancellationReason: 'WORKER_SHUTDOWN' }),
      'sync run summary',
    )
  })

  it('only defers a run cancelled before it made any progress, leaving the unit state untouched', async () => {
    mocks.emittedCount.mockReturnValue(0)
    mocks.run.mockImplementation(async () => {
      activityCancellation.controller.abort(new Error('WORKER_SHUTDOWN'))
      return { complete: false }
    })

    await executeSync(UNIT.id)

    expect(mocks.rescheduleUnit).toHaveBeenCalledWith({}, UNIT.id, SHORT_DEFER_RUN_AT)
    expect(mocks.recordRunSuccess).not.toHaveBeenCalled()
  })
})

describe('executeSync outcome scheduling', () => {
  it.each([
    [{ complete: true }, true, CADENCE_RUN_AT],
    [{ complete: false }, false, SHORT_DEFER_RUN_AT],
    [{ complete: false, resumeAtCadence: true }, false, CADENCE_RUN_AT],
  ])('records %j as complete=%s, next run at %s', async (outcome, complete, nextRunAt) => {
    runReturning(outcome)

    await executeSync(UNIT.id)

    expect(mocks.recordRunSuccess).toHaveBeenCalledWith(
      {},
      UNIT.id,
      { watermark: { cursor: 'after' }, emittedCount: 3, complete },
      nextRunAt,
    )
  })
})

describe('executeSync claim', () => {
  it('re-locks the unit before reading it, so the whole run is covered by lockedAt', async () => {
    runReturning({ complete: true })

    await executeSync(UNIT.id)

    expect(mocks.reassertUnitClaim).toHaveBeenCalledWith({}, UNIT.id)
    expect(mocks.reassertUnitClaim.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.getUnitById.mock.invocationCallOrder[0],
    )
  })

  it('does not run at all when the unit is no longer active', async () => {
    mocks.reassertUnitClaim.mockResolvedValueOnce(false)

    await executeSync(UNIT.id)

    expect(mocks.getUnitById).not.toHaveBeenCalled()
  })
})

describe('executeSync failure', () => {
  it.each<[number | null, ConnectorError]>([
    [1, new ProviderAccessDeniedError('github access denied: ip allow list')],
    [5, new ProviderAuthError('github unauthorized')],
    [null, new ProviderContractError('github forbidden: Resource not accessible by integration')],
  ])('dead-letters after %s failures for %s', async (deadLetterAfter, error) => {
    mocks.run.mockRejectedValue(error)

    await expect(executeSync(UNIT.id)).rejects.toBe(error)

    expect(mocks.recordRunFailure).toHaveBeenCalledWith(
      {},
      UNIT.id,
      error.errorClass,
      error.message,
      deadLetterAfter,
      SHORT_DEFER_RUN_AT,
      null,
    )
  })
})

describe('recordSyncRunTimeout', () => {
  const ATTEMPT_STARTED_AT = '2026-10-26T03:03:11.000Z'

  it('logs a failed run summary with the timeout class when the timeout is recorded', async () => {
    mocks.recordRunTimeout.mockResolvedValue(1)

    await recordSyncRunTimeout(UNIT.id, 'HEARTBEAT', ATTEMPT_STARTED_AT)

    expect(mocks.recordRunTimeout).toHaveBeenCalledWith(
      {},
      UNIT.id,
      'sync run timed out (HEARTBEAT)',
      new Date(ATTEMPT_STARTED_AT),
    )
    expect(mocks.log.error).toHaveBeenCalledWith(
      expect.objectContaining({
        event: 'sync_run_summary',
        outcome: 'failed',
        errorClass: 'worker.timeout',
        errorMessage: 'sync run timed out (HEARTBEAT)',
        consecutiveFailures: 1,
      }),
      'sync run summary',
    )
  })

  it('logs that the attempt already finished when nothing was recorded', async () => {
    mocks.recordRunTimeout.mockResolvedValue(0)

    await recordSyncRunTimeout(UNIT.id, 'HEARTBEAT', ATTEMPT_STARTED_AT)

    expect(mocks.log.error).not.toHaveBeenCalled()
    expect(mocks.log.info).toHaveBeenCalledWith(
      expect.objectContaining({ event: 'sync_run_timeout_ignored', timeoutType: 'HEARTBEAT' }),
      expect.any(String),
    )
  })

  it('does nothing when the unit no longer exists', async () => {
    mocks.getUnitById.mockResolvedValue(null)

    await recordSyncRunTimeout(UNIT.id, 'HEARTBEAT', ATTEMPT_STARTED_AT)

    expect(mocks.recordRunTimeout).not.toHaveBeenCalled()
  })
})
