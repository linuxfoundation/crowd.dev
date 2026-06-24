import {
  OsspckgsJobKind,
  getLastCompletedJobRowCount as dalGetLastCompletedJobRowCount,
} from '@crowd/data-access-layer'
import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { getPackagesDb } from '../../db'

export interface CheckDependentCountsGuardInput {
  currentRowCount: number
  snapshotDate: string
  // Which dependent-counts job kind to baseline against. The three ways each guard against their
  // own history: 'dependent_counts' (edges) | 'dependent_counts_go' | 'dependent_counts_nuget'.
  jobKind?: OsspckgsJobKind
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
  const jobKind = input.jobKind ?? 'dependent_counts'
  const prevRowCount = await dalGetLastCompletedJobRowCount(qx, jobKind)

  if (prevRowCount === null || prevRowCount <= 0) {
    return { ok: true, prevRowCount, dropPct: null }
  }

  const dropPct = (prevRowCount - input.currentRowCount) / prevRowCount

  if (dropPct >= DROP_THRESHOLD) {
    sendSlackNotification(
      SlackChannel.CDP_CRITICAL_ALERTS,
      SlackPersona.CRITICAL_ALERTER,
      `:warning: ${jobKind} row count anomaly detected`,
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
