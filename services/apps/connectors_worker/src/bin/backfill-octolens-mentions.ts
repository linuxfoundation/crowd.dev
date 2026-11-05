import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  listActiveOctolensKeywordMappings,
  listOctolensMentionPollUnits,
  queueOctolensKeywordBackfills,
  rewindOctolensMentionPollStartDate,
} from '@crowd/data-access-layer/src/octolens'
import type { IOctolensMentionPollWriteResult } from '@crowd/data-access-layer/src/octolens'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import {
  OCTOLENS_BACKFILL_USAGE,
  parseOctolensBackfillArgs,
  planOctolensBackfill,
} from '../octolensBackfill'
import type { OctolensBackfillArgs, OctolensBackfillPlan } from '../octolensBackfill'

const log = getServiceLogger()

const RETRY_HINT = 'retry in a few minutes, once the current mention poll run has finished'

function parseArgsOrExit(): OctolensBackfillArgs {
  try {
    return parseOctolensBackfillArgs(process.argv.slice(2), new Date())
  } catch (err) {
    log.error((err as Error).message)
    log.error(OCTOLENS_BACKFILL_USAGE)
    process.exit(1)
  }
}

async function loadPlan(qx: QueryExecutor, args: OctolensBackfillArgs) {
  const [units, mappings] = await Promise.all([
    listOctolensMentionPollUnits(qx),
    listActiveOctolensKeywordMappings(qx),
  ])
  const activeMappingCounts = new Map<number, number>()
  for (const { keywordId } of mappings) {
    activeMappingCounts.set(keywordId, (activeMappingCounts.get(keywordId) ?? 0) + 1)
  }
  return planOctolensBackfill(args, { units, activeMappingCounts })
}

function describePlan(plan: OctolensBackfillPlan): Record<string, unknown> {
  const base = { unitId: plan.unit.id, integrationId: plan.unit.integrationId }
  if (plan.mode === 'keywords') {
    return {
      ...base,
      keywordIdsToQueue: plan.keywordIdsToQueue,
      alreadyQueued: plan.alreadyQueued,
      currentBackfill: plan.unit.watermark?.backfill ?? [],
    }
  }
  return {
    ...base,
    previousStartDate: plan.previousStartDate,
    startDate: plan.startDate,
    droppedCursor: plan.unit.watermark?.cursor ?? null,
    droppedPendingMaxTimestamp: plan.unit.watermark?.pendingMaxTimestamp ?? null,
  }
}

function applyPlan(
  qx: QueryExecutor,
  plan: OctolensBackfillPlan,
): Promise<IOctolensMentionPollWriteResult> {
  if (plan.mode === 'keywords') {
    return queueOctolensKeywordBackfills(qx, plan.unit.id, plan.keywordIdsToQueue)
  }
  return rewindOctolensMentionPollStartDate(
    qx,
    plan.unit.id,
    plan.startDate,
    plan.previousStartDate,
  )
}

setImmediate(async () => {
  const args = parseArgsOrExit()
  try {
    const qx = pgpQx(await getDbConnection(WRITE_DB_CONFIG()))
    const plan = await loadPlan(qx, args)
    const summary = describePlan(plan)

    if (plan.mode === 'keywords' && plan.keywordIdsToQueue.length === 0) {
      log.info(summary, 'nothing to do: every keyword is already queued for backfill')
      process.exit(0)
    }

    const busy = plan.unit.lockedAt !== null

    if (args.dryRun) {
      log.info({ ...summary, busy }, 'dry run: would update the octolens mention poll watermark')
      if (busy) {
        log.warn(`a mention poll run is in progress, a real run would refuse now: ${RETRY_HINT}`)
      }
      process.exit(0)
    }

    if (busy) {
      log.error(summary, `refused: a mention poll run is in progress; ${RETRY_HINT}`)
      process.exit(2)
    }

    const result = await applyPlan(qx, plan)
    if (result.outcome === 'locked') {
      log.error(summary, `refused: a mention poll run claimed the unit just now; ${RETRY_HINT}`)
      process.exit(2)
    }
    if (result.outcome === 'changed') {
      log.error(
        { ...summary, watermark: result.watermark },
        'refused: the start date changed since the plan was checked (another rewind or the first poll run); re-run to check against the new start date',
      )
      process.exit(2)
    }
    if (result.outcome !== 'applied') {
      log.error(
        { ...summary, outcome: result.outcome },
        'refused: the sync unit is no longer active or was removed',
      )
      process.exit(1)
    }

    log.info(
      { ...summary, watermark: result.watermark },
      'octolens mention poll watermark updated; the next poll runs pick it up',
    )
    process.exit(0)
  } catch (err) {
    log.error(err, 'octolens mention backfill failed')
    process.exit(1)
  }
})
