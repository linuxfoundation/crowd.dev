import { Context } from '@temporalio/activity'

import {
  ConnectorError,
  createEmit,
  createHttpClient,
  createTokenPool,
  getCredential,
  getSync,
} from '@crowd/connectors'
import type { SyncContext } from '@crowd/connectors'
import {
  getUnitById,
  recordRunFailure,
  recordRunSuccess,
  rescheduleUnit,
} from '@crowd/data-access-layer/src/connectors'
import { fetchIntegrationById } from '@crowd/data-access-layer/src/integrations'
import IntegrationStreamRepository from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getChildLogger } from '@crowd/logging'

import { svc } from '../main'

const DEAD_LETTER_AFTER = 5
const HEARTBEAT_INTERVAL_MS = 10_000
const RATE_LIMIT_FALLBACK_MS = 60_000

export async function executeSync(unitId: string): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)

  const unit = await getUnitById(qx, unitId)
  if (!unit) {
    throw new Error(`sync unit ${unitId} not found`)
  }

  const activityContext = Context.current()
  const log = getChildLogger('syncRun', svc.log, {
    runId: activityContext.info.workflowExecution.runId,
    unitId: unit.id,
    platform: unit.platform,
    syncName: unit.syncName,
    channelName: unit.channelName,
  })

  const heartbeat = setInterval(() => {
    try {
      activityContext.heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'heartbeat failed')
    }
  }, HEARTBEAT_INTERVAL_MS)

  try {
    const integration = await fetchIntegrationById(qx, unit.integrationId)
    if (!integration?.segmentId) {
      throw new Error(`integration ${unit.integrationId} not found or has no segmentId`)
    }
    if (!svc.dataSinkWorkerEmitter) {
      throw new Error('data sink worker emitter not initialized')
    }

    // POC only: dummy has no credentials; token minting from the credential is M4
    if (unit.platform !== 'dummy') {
      await getCredential(qx, unit.integrationId)
    }

    const pool = createTokenPool(svc.redis, unit.platform, unit.integrationId)
    const http = createHttpClient({
      acquireToken: pool.acquire,
      parkToken: pool.park,
      quarantineToken: pool.quarantine,
      log,
    })

    const sync = getSync(unit.platform, unit.syncName)

    const streamRepo = new IntegrationStreamRepository(svc.postgres.writer, log)
    const emitter = createEmit({
      publishResult: streamRepo.publishExternalResult.bind(streamRepo),
      sinkEmitter: svc.dataSinkWorkerEmitter,
      unit,
      segmentId: integration.segmentId,
      schema: sync.schema,
      log,
    })

    let committedWatermark = unit.watermark

    const ctx: SyncContext = {
      channel: { channelId: unit.channelId, channelName: unit.channelName },
      watermark: unit.watermark,
      emit: emitter.emit,
      commitWatermark: async (watermark) => {
        committedWatermark = watermark
      },
      http,
      log,
    }

    await sync.run(ctx)

    await recordRunSuccess(qx, unitId, {
      watermark: committedWatermark ?? {},
      emittedCount: emitter.emittedCount(),
    })
    log.info({ emittedCount: emitter.emittedCount() }, 'sync run succeeded')
  } catch (err) {
    if (err instanceof ConnectorError && err.errorClass === 'provider.rate_limit') {
      const resumeAt = err.options?.resumeAt ?? new Date(Date.now() + RATE_LIMIT_FALLBACK_MS)
      await rescheduleUnit(qx, unitId, resumeAt)
      log.info({ resumeAt }, 'sync run rate-limit parked')
      return
    }
    const errorClass = err instanceof ConnectorError ? err.errorClass : 'unknown'
    log.error(err, 'sync run failed')
    await recordRunFailure(qx, unitId, errorClass, DEAD_LETTER_AFTER)
    throw err
  } finally {
    clearInterval(heartbeat)
  }
}
