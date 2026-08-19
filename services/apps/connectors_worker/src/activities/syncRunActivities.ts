import { Context } from '@temporalio/activity'

import { getSync } from '@crowd/connectors'
import type { SyncContext } from '@crowd/connectors'
import {
  getUnitById,
  recordRunFailure,
  recordRunSuccess,
} from '@crowd/data-access-layer/src/connectors'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getChildLogger } from '@crowd/logging'

import { svc } from '../main'

const DEAD_LETTER_AFTER = 5
const HEARTBEAT_INTERVAL_MS = 10_000

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

  let emittedCount = 0
  let committedWatermark = unit.watermark

  // POC only: emit collects counts in memory; the Kafka sink emitter arrives in M2
  const ctx: SyncContext = {
    channel: { channelId: unit.channelId, channelName: unit.channelName },
    watermark: unit.watermark,
    emit: async (records) => {
      emittedCount += records.length
    },
    commitWatermark: async (watermark) => {
      committedWatermark = watermark
    },
    log,
  }

  const heartbeat = setInterval(() => {
    try {
      activityContext.heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'heartbeat failed')
    }
  }, HEARTBEAT_INTERVAL_MS)

  try {
    const sync = getSync(unit.platform, unit.syncName)
    await sync.run(ctx)

    await recordRunSuccess(qx, unitId, {
      watermark: committedWatermark ?? {},
      emittedCount,
    })
    log.info({ emittedCount }, 'sync run succeeded')
  } catch (err) {
    log.error(err, 'sync run failed')
    // POC only: everything unclassified is framework.internal; the 7-class
    // error taxonomy arrives with the M2 HTTP client
    await recordRunFailure(qx, unitId, 'framework.internal', DEAD_LETTER_AFTER)
    throw err
  } finally {
    clearInterval(heartbeat)
  }
}
