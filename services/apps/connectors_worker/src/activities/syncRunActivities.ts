import { ApplicationFailure, Context } from '@temporalio/activity'

import {
  ConnectorError,
  createEmit,
  createHttpClient,
  createTokenPool,
  findSync,
  getCredential,
  getManifest,
  getSync,
} from '@crowd/connectors'
import type { ConnectorHttp, Emitter, ErrorClass, SyncContext } from '@crowd/connectors'
import {
  getUnitById,
  parkUnit,
  reassertUnitClaim,
  recordRunFailure,
  recordRunPartial,
  recordRunSuccess,
  recordRunTimeout,
  recordShadowRecords,
  rescheduleUnit,
} from '@crowd/data-access-layer/src/connectors'
import type { ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import { fetchIntegrationById } from '@crowd/data-access-layer/src/integrations'
import IntegrationStreamRepository from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getChildLogger } from '@crowd/logging'

import { svc } from '../main'
import { RUN_BUDGET_MS } from '../runLimits'
import { resolveRunSegmentId } from '../runSegment'
import { cadenceRunAt, failureRunAt, shortDeferRunAt } from '../scheduling'
import { SYNC_RUN_FAILURE_RECORDED } from '../syncRunFailure'

const DEAD_LETTER_AFTER: Partial<Record<ErrorClass, number>> = {
  'provider.auth': 5,
  'provider.access_denied': 1,
}
const HEARTBEAT_INTERVAL_MS = 10_000
const RATE_LIMIT_FALLBACK_MS = 60_000
const UNAVAILABLE_PARK_MS = 300_000

const PARKED_ERROR_CLASSES = ['provider.rate_limit', 'provider.unavailable'] as const

function syncRunLogFields(activityContext: Context, unit: ISyncUnit): Record<string, unknown> {
  return {
    workflowId: activityContext.info.workflowExecution.workflowId,
    runId: activityContext.info.workflowExecution.runId,
    unitId: unit.id,
    integrationId: unit.integrationId,
    platform: unit.platform,
    syncName: unit.syncName,
    channelId: unit.channelId,
    channelName: unit.channelName,
  }
}

export async function executeSync(unitId: string): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)

  if (!(await reassertUnitClaim(qx, unitId))) {
    svc.log.info({ unitId }, 'sync unit is no longer active, skipping run and releasing its claim')
    return
  }
  const unit = await getUnitById(qx, unitId)
  if (!unit) {
    throw new Error(`sync unit ${unitId} not found`)
  }

  const activityContext = Context.current()
  const log = getChildLogger('syncRun', svc.log, syncRunLogFields(activityContext, unit))

  const heartbeat = setInterval(() => {
    try {
      activityContext.heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'heartbeat failed')
    }
  }, HEARTBEAT_INTERVAL_MS)

  let emitter: Emitter | null = null
  let http: ConnectorHttp | null = null
  let committedWatermark = unit.watermark
  const startedAt = Date.now()
  const runDeadline = startedAt + RUN_BUDGET_MS

  const cancellationReason = () =>
    (activityContext.cancellationSignal.reason as Error | undefined)?.message ?? null

  const runSummary = (fields: Record<string, unknown>) => ({
    event: 'sync_run_summary',
    durationMs: Date.now() - startedAt,
    emittedCount: emitter?.emittedCount() ?? 0,
    requestCount: http?.requestCount() ?? 0,
    complete: null as boolean | null,
    emitEnabled: unit.emitEnabled,
    cancellationReason: cancellationReason(),
    ...fields,
  })

  log.info(
    {
      event: 'sync_run_started',
      consecutiveFailures: unit.consecutiveFailures,
      emitEnabled: unit.emitEnabled,
    },
    'sync run started',
  )

  try {
    const manifest = getManifest(unit.platform)
    const integration = await fetchIntegrationById(qx, unit.integrationId)
    const segmentId = resolveRunSegmentId(unit.integrationId, integration, manifest)
    if (!svc.dataSinkWorkerEmitter) {
      throw new Error('data sink worker emitter not initialized')
    }

    let resolvedCredential =
      manifest.preparePool || manifest.mintToken
        ? await getCredential(qx, unit.integrationId)
        : null
    const pool = createTokenPool(svc.redis, unit.platform, {
      probeBudget: manifest.probeBudget,
      mintToken:
        resolvedCredential && manifest.mintToken
          ? manifest.mintToken(resolvedCredential)
          : undefined,
      log,
    })
    let preferredEntryId: string | undefined
    if (resolvedCredential && manifest.preparePool) {
      preferredEntryId = (await manifest.preparePool(resolvedCredential, pool)).preferredEntryId
    }
    http = createHttpClient({
      acquireToken: () => pool.acquire(preferredEntryId),
      parkToken: pool.park,
      invalidateToken: pool.invalidate,
      interpretResponse: manifest.interpretResponse,
      log,
    })

    const sync = getSync(unit.platform, unit.syncName)

    const streamRepo = new IntegrationStreamRepository(svc.postgres.writer, log)
    emitter = createEmit({
      publishResult: streamRepo.publishExternalResult.bind(streamRepo),
      sinkEmitter: svc.dataSinkWorkerEmitter,
      recordShadow: (records) => recordShadowRecords(qx, unit.id, records),
      unit,
      segmentId,
      emitsOwnSegment: manifest.emitsOwnSegment,
      schema: sync.schema,
      log,
    })

    const ctx: SyncContext<unknown> = {
      channel: { channelId: unit.channelId, channelName: unit.channelName },
      integrationId: unit.integrationId,
      watermark: unit.watermark,
      emit: emitter.emit,
      commitWatermark: async (watermark) => {
        committedWatermark = watermark
      },
      hasRunBudget: () => Date.now() < runDeadline && !activityContext.cancellationSignal.aborted,
      http,
      log,
      getCredential: async () => {
        if (!resolvedCredential) {
          resolvedCredential = await getCredential(qx, unit.integrationId)
        }
        return resolvedCredential
      },
      deps: manifest.createDeps?.({ qx, integrationId: unit.integrationId }),
    }

    const outcome = await sync.run(ctx)
    const cancelledBeforeAnyProgress =
      activityContext.cancellationSignal.aborted &&
      committedWatermark === unit.watermark &&
      emitter.emittedCount() === 0
    if (cancelledBeforeAnyProgress) {
      const nextRunAt = shortDeferRunAt()
      await rescheduleUnit(qx, unitId, nextRunAt)
      log.info(runSummary({ outcome: 'deferred', complete: false, nextRunAt }), 'sync run summary')
      return
    }
    const nextRunAt =
      outcome.complete || outcome.resumeAtCadence
        ? cadenceRunAt(sync.cadenceMinutes)
        : shortDeferRunAt()

    await recordRunSuccess(
      qx,
      unitId,
      {
        watermark: committedWatermark ?? {},
        emittedCount: emitter.emittedCount(),
        complete: outcome.complete,
      },
      nextRunAt,
    )
    log.info(
      runSummary({ outcome: 'success', complete: outcome.complete, nextRunAt }),
      'sync run summary',
    )
  } catch (err) {
    if (
      err instanceof ConnectorError &&
      (PARKED_ERROR_CLASSES as readonly string[]).includes(err.errorClass)
    ) {
      const fallbackMs =
        err.errorClass === 'provider.rate_limit' ? RATE_LIMIT_FALLBACK_MS : UNAVAILABLE_PARK_MS
      const resumeAt = err.options?.resumeAt ?? new Date(Date.now() + fallbackMs)
      const progressCommitted = Boolean(emitter && committedWatermark)
      if (emitter && committedWatermark) {
        await recordRunPartial(
          qx,
          unitId,
          { watermark: committedWatermark, emittedCount: emitter.emittedCount() },
          resumeAt,
          err.errorClass,
          err.message,
        )
      } else {
        await parkUnit(qx, unitId, resumeAt, err.errorClass, err.message)
      }
      log.info(
        runSummary({
          outcome: 'parked',
          errorClass: err.errorClass,
          errorMessage: err.message,
          nextRunAt: resumeAt,
          progressCommitted,
          request: err.options?.request,
          response: err.options?.response,
        }),
        'sync run summary',
      )
      return
    }
    const errorClass: ErrorClass = err instanceof ConnectorError ? err.errorClass : 'unknown'
    const deadLetterAfter = DEAD_LETTER_AFTER[errorClass] ?? null
    const consecutiveFailures = unit.consecutiveFailures + 1
    const nextRunAt = failureRunAt(
      consecutiveFailures,
      findSync(unit.platform, unit.syncName)?.cadenceMinutes ?? null,
    )
    const errorMessage = err instanceof Error ? err.message : String(err)
    log.error(
      runSummary({
        outcome: 'failed',
        errorClass,
        errorMessage,
        consecutiveFailures,
        nextRunAt,
        request: err instanceof ConnectorError ? err.options?.request : undefined,
        response: err instanceof ConnectorError ? err.options?.response : undefined,
        err,
      }),
      'sync run summary',
    )
    await recordRunFailure(
      qx,
      unitId,
      errorClass,
      errorMessage,
      deadLetterAfter,
      nextRunAt,
      committedWatermark === unit.watermark ? null : committedWatermark,
    )
    throw ApplicationFailure.create({
      type: SYNC_RUN_FAILURE_RECORDED,
      message: errorMessage,
      cause: err instanceof Error ? err : undefined,
    })
  } finally {
    clearInterval(heartbeat)
  }
}

export async function recordSyncRunTimeout(
  unitId: string,
  timeoutType: string,
  attemptStartedAt: string,
): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  const unit = await getUnitById(qx, unitId)
  if (!unit) {
    return
  }
  const log = getChildLogger('syncRun', svc.log, syncRunLogFields(Context.current(), unit))
  const errorMessage = `sync run timed out (${timeoutType})`
  const recorded = await recordRunTimeout(qx, unitId, errorMessage, new Date(attemptStartedAt))
  const consecutiveFailures =
    recorded === 1 ? unit.consecutiveFailures + 1 : await ownRecordedTimeoutFailures(qx, unitId)
  if (consecutiveFailures === null) {
    log.info(
      { event: 'sync_run_timeout_ignored', timeoutType },
      'sync run timeout ignored, attempt already recorded its outcome',
    )
    return
  }
  log.error(
    {
      event: 'sync_run_summary',
      outcome: 'failed',
      errorClass: 'worker.timeout',
      errorMessage,
      consecutiveFailures,
      emitEnabled: unit.emitEnabled,
    },
    'sync run summary',
  )
}

async function ownRecordedTimeoutFailures(
  qx: QueryExecutor,
  unitId: string,
): Promise<number | null> {
  const current = await getUnitById(qx, unitId)
  return current?.lastErrorClass === 'worker.timeout' ? current.consecutiveFailures : null
}
