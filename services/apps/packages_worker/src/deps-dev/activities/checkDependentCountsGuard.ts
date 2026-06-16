import { getLastCompletedJobRowCount as dalGetLastCompletedJobRowCount } from '@crowd/data-access-layer'
import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { getPackagesDb } from '../../db'

export interface CheckDependentCountsGuardInput {
  currentRowCount: number
  snapshotDate: string
}

export interface CheckDependentCountsGuardOutput {
  ok: boolean
  prevRowCount: number | null
  dropPct: number | null
}

const DROP_THRESHOLD = 0.05

export async function checkDependentCountsGuard(
  input: CheckDependentCountsGuardInput,
): Promise<CheckDependentCountsGuardOutput> {
  const qx = await getPackagesDb()
  const prevRowCount = await dalGetLastCompletedJobRowCount(qx, 'dependent_counts')

  if (prevRowCount === null || prevRowCount <= 0) {
    return { ok: true, prevRowCount, dropPct: null }
  }

  const dropPct = (prevRowCount - input.currentRowCount) / prevRowCount

  if (dropPct >= DROP_THRESHOLD) {
    sendSlackNotification(
      SlackChannel.CDP_CRITICAL_ALERTS,
      SlackPersona.CRITICAL_ALERTER,
      ':warning: dependent_counts row count anomaly detected',
      [
        {
          title: 'Snapshot',
          text: input.snapshotDate,
        },
        {
          title: 'Row count',
          text: `${input.currentRowCount.toLocaleString()} (prev max: ${prevRowCount.toLocaleString()}, drop: ${(dropPct * 100).toFixed(1)}%)`,
        },
        {
          title: 'Action',
          text: 'Ingest aborted — existing `dependent_count` values preserved. Check deps.dev `Dependents` table for this snapshot date.',
        },
      ],
    )
    return { ok: false, prevRowCount, dropPct }
  }

  return { ok: true, prevRowCount, dropPct }
}
