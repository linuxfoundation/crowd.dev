import { Context } from '@temporalio/activity'

import { getManifest, selectCounters } from '@crowd/connectors'
import type {
  CountWindow,
  IValidationChannel,
  IValidationUnit,
  ValidationTarget,
} from '@crowd/data-access-layer/src/connectors'
import {
  deleteBackfillCheck,
  listDueBackfillChecks,
  listValidationChannelsPage,
  listValidationUnitsPage,
  markReadyBackfillChecks,
  upsertValidationResult,
} from '@crowd/data-access-layer/src/connectors'
import { fetchIntegrationById } from '@crowd/data-access-layer/src/integrations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { findSegmentsForRepos } from '@crowd/data-access-layer/src/repositories'
import { TinybirdClient } from '@crowd/database'
import { getChildLogger } from '@crowd/logging'

import { svc } from '../main'
import { createPlatformHttp } from '../platformHttp'
import { resolveRunSegmentId } from '../runSegment'
import { countTinybirdActivities } from '../tinybirdActivityCount'

const HEARTBEAT_INTERVAL_MS = 30_000
const BACKFILL_VALIDATION_DELAY_SECONDS = 3 * 60 * 60
const BACKFILL_CHECKS_PER_SWEEP = 10

export interface IValidationInput {
  target: ValidationTarget
  window: CountWindow
  counterKeys: string[] | null
}

export interface IUnitValidationFailure {
  unitId: string
  counterKey: string
  errorMessage: string
}

export interface IUnitValidationResult {
  unitId: string
  written: number
  failures: IUnitValidationFailure[]
  noCounters: boolean
}

export async function listValidationUnits(input: {
  target: ValidationTarget
  afterId?: string
  limit: number
}): Promise<IValidationUnit[]> {
  return listValidationUnitsPage(dbStoreQx(svc.postgres.writer), input.target, {
    afterId: input.afterId,
    limit: input.limit,
  })
}

export async function listValidationChannels(input: {
  after?: { integrationId: string; channelId: string }
  limit: number
}): Promise<IValidationChannel[]> {
  return listValidationChannelsPage(dbStoreQx(svc.postgres.writer), {
    after: input.after,
    limit: input.limit,
  })
}

export async function dueBackfillChecks(): Promise<
  { integrationId: string; channelId: string; onboardedAt: string }[]
> {
  const qx = dbStoreQx(svc.postgres.writer)
  await markReadyBackfillChecks(qx)
  return listDueBackfillChecks(qx, BACKFILL_VALIDATION_DELAY_SECONDS, BACKFILL_CHECKS_PER_SWEEP)
}

export async function completeBackfillCheck(input: {
  integrationId: string
  channelId: string
}): Promise<void> {
  await deleteBackfillCheck(dbStoreQx(svc.postgres.writer), input.integrationId, input.channelId)
}

export async function validateUnit(input: {
  unit: IValidationUnit
  window: CountWindow
  counterKeys: string[] | null
}): Promise<IUnitValidationResult> {
  const { unit, window } = input
  const log = getChildLogger('completenessValidation', svc.log, {
    unitId: unit.id,
    platform: unit.platform,
    channelName: unit.channelName,
    syncName: unit.syncName,
  })
  const manifest = getManifest(unit.platform)
  const counters = selectCounters(
    manifest,
    unit.syncName,
    window.kind,
    input.counterKeys ?? unit.validationCounterKeys,
  )

  if (counters.length === 0) {
    log.info({ event: 'completeness_validation_no_counters' }, 'no counters for unit')
    return { unitId: unit.id, written: 0, failures: [], noCounters: true }
  }

  const activityContext = Context.current()
  const heartbeat = setInterval(() => {
    try {
      activityContext.heartbeat()
    } catch (err) {
      log.warn({ errMsg: (err as Error).message }, 'heartbeat failed')
    }
  }, HEARTBEAT_INTERVAL_MS)

  try {
    const qx = dbStoreQx(svc.postgres.writer)
    const integration = await fetchIntegrationById(qx, unit.integrationId)
    const [repoSegment] = await findSegmentsForRepos(qx, svc.redis, log, [
      { integrationId: unit.integrationId, url: unit.channelName },
    ])
    const segmentId =
      repoSegment?.segmentId ?? resolveRunSegmentId(unit.integrationId, integration, manifest)
    const http = await createPlatformHttp(qx, unit.platform, unit.integrationId, log)
    const tinybird = new TinybirdClient()
    const deps = manifest.createDeps?.({ qx, integrationId: unit.integrationId })

    let written = 0
    const failures: IUnitValidationFailure[] = []

    for (const counter of counters) {
      try {
        const sourceCount = await counter.count(
          {
            channel: { channelId: unit.channelId, channelName: unit.channelName },
            integrationId: unit.integrationId,
            http,
            log,
            progress: null,
            saveProgress: () => undefined,
            deps,
          },
          window,
        )
        const tinybirdCount = await countTinybirdActivities(tinybird, {
          segmentId,
          platform: unit.platform,
          channel: unit.channelName,
          activityTypes: counter.activityTypes,
          window,
        })
        await upsertValidationResult(qx, {
          unitId: unit.id,
          counterKey: counter.key,
          window,
          sourceCount,
          tinybirdCount,
        })
        written++
      } catch (err) {
        log.error({ err, counterKey: counter.key }, 'completeness counter failed')
        failures.push({
          unitId: unit.id,
          counterKey: counter.key,
          errorMessage: err instanceof Error ? err.message : String(err),
        })
      }
    }

    return { unitId: unit.id, written, failures, noCounters: false }
  } finally {
    clearInterval(heartbeat)
  }
}
