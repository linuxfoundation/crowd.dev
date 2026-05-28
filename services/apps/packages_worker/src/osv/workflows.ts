import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

// Per ADR-0001 §CVSS scoring strategy the v3.1 inline scorer + qualitative
// fallback are mature; v4 is deferred to a follow-up. Per ADR-0001
// §`advisory_affected_ranges` uniqueness scope the dedup key matches the
// widened unique index so multi-distro ranges are preserved. Per ADR-0001
// §`has_critical_vulnerability` semantics the derive step runs after every
// ingest pass so packages added between schedule firings are at most one
// cycle stale and self-heal on the next run.
const { osvSyncEcosystem, osvDeriveCriticalFlag } = proxyActivities<typeof activities>({
  // npm sync alone is ~1 hour today (N+1 upsert path, see deferred review
  // comment on upsertAdvisory.ts). Maven is ~5 minutes. We give each
  // per-ecosystem activity a generous 2-hour ceiling.
  startToCloseTimeout: '2 hours',
  // Activity heartbeats every 1000 records (see activities.ts). 5 minutes is
  // plenty of headroom for a network hiccup mid-batch.
  heartbeatTimeout: '5 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
    // Activities translate FetchError(NOT_FOUND|PARSE) to ApplicationFailure
    // with these type names — Temporal short-circuits the retry.
    nonRetryableErrorTypes: ['NOT_FOUND', 'PARSE'],
  },
})

export interface OsvSyncWorkflowInput {
  // OSV ecosystem labels (case-sensitive: 'npm', 'Maven', 'PyPI', …). The same
  // list is used both for the per-ecosystem download loop and as the
  // post-parse allowlist filter inside parseOsvRecord.
  ecosystems: string[]
}

// osvSync replaces the standalone-bin runOsvSync loop. The schedule (see
// schedule.ts) fires this workflow daily; cadence is owned by Temporal, not
// by an internal sleep. One ecosystem failing is recoverable: the workflow
// records the per-ecosystem result and continues with the next, then runs
// derivation on whatever did ingest.
export async function osvSync(input: OsvSyncWorkflowInput): Promise<void> {
  log.info('osvSync starting', { ecosystems: input.ecosystems })

  const allowedEcosystems = input.ecosystems

  for (const ecosystem of input.ecosystems) {
    try {
      const stats = await osvSyncEcosystem({ ecosystem, allowedEcosystems })
      log.info('osvSyncEcosystem succeeded', { ...stats })
    } catch (err) {
      // Per-ecosystem failure does not abort the pass — log and continue so
      // the other ecosystems and the derive step still run on partial data.
      log.error('osvSyncEcosystem failed; continuing with next ecosystem', {
        ecosystem,
        // Workflow log serializes the error via the Temporal SDK.
        err: err as Error,
      })
    }
  }

  const derive = await osvDeriveCriticalFlag()
  log.info('osvDeriveCriticalFlag succeeded', { ...derive })
  log.info('osvSync complete')
}
