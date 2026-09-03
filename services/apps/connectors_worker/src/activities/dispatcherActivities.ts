import { createTokenPool, findManifest, mapWithConcurrency } from '@crowd/connectors'
import { claimDueUnits, rescheduleUnit } from '@crowd/data-access-layer/src/connectors'
import type { IClaimedUnit } from '@crowd/data-access-layer/src/connectors'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { RedisCache } from '@crowd/redis'
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from '@crowd/temporal'

import { svc } from '../main'
import { runningProbeRunAt, shortDeferRunAt } from '../scheduling'
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
    return pool.hasHeadroom(DEFAULT_RUN_ESTIMATE)
  })
  const result = {
    admitted: units.filter((_, index) => headrooms[index]),
    deferred: units.filter((_, index) => !headrooms[index]),
  }
  if (result.deferred.length > 0) {
    svc.log.info(
      { unitIds: result.deferred.map((unit) => unit.id) },
      'units deferred: no token headroom',
    )
  }
  return result
}

export async function deferUnit(unitId: string): Promise<void> {
  await rescheduleUnit(dbStoreQx(svc.postgres.writer), unitId, shortDeferRunAt())
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
  const nextRunAt = await runningSince(unit)
    .then(runningProbeRunAt)
    .catch(() => shortDeferRunAt())

  await rescheduleUnit(dbStoreQx(svc.postgres.writer), unit.id, nextRunAt)
  svc.log.info({ unitId: unit.id, nextRunAt }, 'unit already running, rescheduled probe')
}

async function runningSince(unit: IClaimedUnit): Promise<Date> {
  const description = await svc.temporal.workflow.getHandle(syncRunWorkflowId(unit)).describe()

  return description.startTime
}

export async function logDispatchSummary(counts: IDispatchCounts): Promise<void> {
  svc.log.info({ event: 'dispatcher_tick', ...counts }, 'dispatch summary')
}

export async function touchHeartbeat(): Promise<void> {
  const cache = new RedisCache('connectors', svc.redis, svc.log)
  await cache.set('dispatcherHeartbeat', new Date().toISOString(), HEARTBEAT_TTL_SECONDS)
}
