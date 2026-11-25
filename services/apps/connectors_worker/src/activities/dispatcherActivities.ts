import { createTokenPool, findManifest, mapWithConcurrency } from '@crowd/connectors'
import {
  claimDueUnits,
  countRunningUnits,
  renewRunningUnitLease,
  rescheduleUnit,
} from '@crowd/data-access-layer/src/connectors'
import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { RedisCache } from '@crowd/redis'
import {
  WorkflowIdConflictPolicy,
  WorkflowIdReusePolicy,
  WorkflowNotFoundError,
} from '@crowd/temporal'

import { svc } from '../main'
import { applyRunCaps, releaseOrphan } from '../runCap'
import { RUNNING_LOCK_MAX_AGE_MINUTES } from '../runLimits'
import { shortDeferRunAt } from '../scheduling'
import type { IAdmissionResult, IDispatchCounts, StartRunResult } from '../types'

const TASK_QUEUE = 'connectors'
const HEARTBEAT_TTL_SECONDS = 300
const DEFAULT_RUN_ESTIMATE = 50
const BUDGET_PROBE_CONCURRENCY = 10
const CHANNEL_LABEL_MAX_LENGTH = 80

export async function claimDue(limit: number): Promise<IClaimedUnit[]> {
  return claimDueUnits(dbStoreQx(svc.postgres.writer), limit)
}

export async function admitByBudget(units: IClaimedUnit[]): Promise<IAdmissionResult> {
  const headrooms = await mapWithConcurrency(units, BUDGET_PROBE_CONCURRENCY, (unit) => {
    const manifest = findManifest(unit.platform)
    const pool = createTokenPool(svc.redis, unit.platform, {
      probeBudget: manifest?.probeBudget,
    })
    return pool.hasHeadroom(manifest?.runCostEstimate ?? DEFAULT_RUN_ESTIMATE)
  })
  const byHeadroom = {
    admitted: units.filter((_, index) => headrooms[index]),
    deferred: units.filter((_, index) => !headrooms[index]),
  }
  if (byHeadroom.deferred.length > 0) {
    svc.log.info(
      { unitIds: byHeadroom.deferred.map((unit) => unit.id) },
      'units deferred: no token headroom',
    )
  }

  const caps = runCapsOf(units)
  const cappedPlatforms = Object.keys(caps)
  if (cappedPlatforms.length === 0) {
    return byHeadroom
  }

  const liveUnitIds = await findLiveLapsedUnitIds(
    units.filter((unit) => unit.leaseLapsed && caps[unit.platform] !== undefined),
  )
  const running = await countRunningUnits(
    dbStoreQx(svc.postgres.writer),
    cappedPlatforms,
    units.map((unit) => unit.id),
    RUNNING_LOCK_MAX_AGE_MINUTES,
  )
  const byRunCap = applyRunCaps(byHeadroom.admitted, units, caps, running, liveUnitIds)
  if (byRunCap.deferred.length > 0) {
    svc.log.info(
      { unitIds: byRunCap.deferred.map((unit) => unit.id) },
      'units deferred: platform run cap',
    )
  }
  return {
    admitted: byRunCap.admitted,
    deferred: [
      ...byHeadroom.deferred.map((unit) => releaseOrphan(unit, caps, liveUnitIds)),
      ...byRunCap.deferred,
    ],
  }
}

async function findLiveLapsedUnitIds(lapsedUnits: IClaimedUnit[]): Promise<Set<string>> {
  const live = await mapWithConcurrency(lapsedUnits, BUDGET_PROBE_CONCURRENCY, isRunLive)
  return new Set(lapsedUnits.filter((_, index) => live[index]).map((unit) => unit.id))
}

async function isRunLive(unit: IClaimedUnit): Promise<boolean> {
  try {
    const { status } = await svc.temporal.workflow.getHandle(syncRunWorkflowId(unit)).describe()
    return status.name === 'RUNNING'
  } catch (err) {
    if (err instanceof WorkflowNotFoundError) {
      return false
    }
    throw err
  }
}

function runCapsOf(units: IClaimedUnit[]): Record<string, number> {
  const caps: Record<string, number> = {}
  for (const platform of new Set(units.map((unit) => unit.platform))) {
    const cap = findManifest(platform)?.maxConcurrentRuns
    if (cap !== undefined) {
      caps[platform] = cap
    }
  }
  return caps
}

// A lapsed lease may belong to a run still in flight, so that claim is kept instead of released.
export async function deferUnit(unitId: string, leaseLapsed = false): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  if (leaseLapsed) {
    await renewRunningUnitLease(qx, unitId)
  } else {
    await rescheduleUnit(qx, unitId, shortDeferRunAt())
  }
}

function channelLabel(channelName: string): string {
  const label = channelName
    .replace(/^[a-z][a-z0-9+.-]*:\/\/[^/]+\//i, '')
    .replace(/^\/+|\/+$/g, '')
    .replace(/\s+/g, '-')
    .slice(0, CHANNEL_LABEL_MAX_LENGTH)

  return label || 'unknown-channel'
}

export function syncRunWorkflowId(unit: IClaimedUnit): string {
  return `sync-run/${unit.platform}/${unit.syncName}/${channelLabel(unit.channelName)}/${unit.id}`
}

export async function startRun(unit: IClaimedUnit): Promise<StartRunResult> {
  try {
    await svc.temporal.workflow.start('syncRun', {
      taskQueue: TASK_QUEUE,
      workflowId: syncRunWorkflowId(unit),
      workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
      workflowIdConflictPolicy: WorkflowIdConflictPolicy.FAIL,
      args: [unit.id],
      memo: {
        unitId: unit.id,
        integrationId: unit.integrationId,
        platform: unit.platform,
        syncName: unit.syncName,
        channelId: unit.channelId,
        channelName: unit.channelName,
      },
    })
    return 'started'
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      await backOffRunningUnit(unit)
      return 'alreadyRunning'
    }
    throw err
  }
}

async function backOffRunningUnit(unit: IClaimedUnit): Promise<void> {
  const renewed = await renewRunningUnitLease(dbStoreQx(svc.postgres.writer), unit.id)
  svc.log.info(
    { unitId: unit.id, renewed: renewed > 0 },
    'unit already running, lease renewed until it lapses',
  )
}

export async function logDispatchSummary(counts: IDispatchCounts): Promise<void> {
  svc.log.info({ event: 'dispatcher_tick', ...counts }, 'dispatch summary')
}

export async function touchHeartbeat(): Promise<void> {
  const cache = new RedisCache('connectors', svc.redis, svc.log)
  await cache.set('dispatcherHeartbeat', new Date().toISOString(), HEARTBEAT_TTL_SECONDS)
}
