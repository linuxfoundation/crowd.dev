import { getLastCompletedJobRowCount as dalGetLastCompletedJobRowCount } from '@crowd/data-access-layer'
import type { OsspckgsJobKind } from '@crowd/data-access-layer'
import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { getPackagesDb } from '../../db'

export interface CheckDependentCountsGuardInput {
  currentRowCount: number
  snapshotDate: string
  // Which dependent-counts job kind to baseline against. The four ways each guard against their
  // own history: 'dependent_counts' (edges) | 'dependent_counts_go' | 'dependent_counts_nuget'
  // | 'dependent_counts_rubygems'.
  jobKind?: OsspckgsJobKind
}

// Source table to point operators at in the Slack alert, keyed by job kind — the edge variant reads
// deps.dev's `Dependents` reverse index, while GO/NUGET/RUBYGEMS invert their own manifest table
// (they're absent from `Dependents`, see ADR-0004). A hardcoded "check Dependents" message would send
// on-call to the wrong table for those three.
const GUARD_SOURCE_TABLE: Partial<Record<OsspckgsJobKind, string>> = {
  dependent_counts: 'Dependents',
  dependent_counts_go: 'GoRequirementsLatest',
  dependent_counts_nuget: 'NuGetRequirementsLatest',
  dependent_counts_rubygems: 'RubyGemsRequirementsLatest',
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
          text: `Ingest aborted — existing \`dependent_count\` values preserved. Check deps.dev \`${GUARD_SOURCE_TABLE[jobKind] ?? 'Dependents'}\` table for this snapshot date.`,
        },
      ],
    )
    return { ok: false, prevRowCount, dropPct }
  }

  return { ok: true, prevRowCount, dropPct }
}
