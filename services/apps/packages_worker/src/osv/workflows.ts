import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

// Per ADR-0001 §CVSS scoring strategy the v3.1 inline scorer + qualitative
// fallback are mature; v4 is deferred to a follow-up. Per ADR-0001
// §`advisory_affected_ranges` uniqueness scope the dedup key matches the
// widened unique index so multi-distro ranges are preserved. Per ADR-0001
// §`has_critical_vulnerability` semantics the derive step runs after every
// ingest pass so packages added between schedule firings are at most one
// cycle stale and self-heal on the next run.
//
// Sync and derive use separate proxyActivities configs because their
// heartbeat shape differs: sync emits one heartbeat per ~1000 records (see
// activities.ts) so a 5-minute heartbeatTimeout is the right liveness signal;
// derive is a single tight loop over packages with no per-page heartbeat,
// so it relies on startToCloseTimeout only — sharing the sync heartbeat
// config would silently cancel the derive activity at Tier 2 scale.
const { osvSyncEcosystem } = proxyActivities<typeof activities>({
  // npm sync alone is ~1 hour today (N+1 upsert path, see deferred review
  // comment on upsertAdvisory.ts). Maven is ~5 minutes. We give each
  // per-ecosystem activity a generous 2-hour ceiling.
  startToCloseTimeout: '2 hours',
  // Activity heartbeats every 1000 records (see activities.ts), but the FIRST
  // heartbeat only fires after the full ecosystem zip has been downloaded
  // (no heartbeat during downloadZip). DOWNLOAD_TIMEOUT_MS in
  // fetchEcosystemZip.ts is 10 minutes, so the heartbeatTimeout must exceed
  // that — otherwise Temporal kills the activity as unresponsive on a slow
  // CDN even though the download is progressing. 15 minutes leaves 5 minutes
  // of headroom past the download cap before the next heartbeat is expected.
  heartbeatTimeout: '15 minutes',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
    // Activities translate FetchError(NOT_FOUND|PARSE) to ApplicationFailure
    // with these type names — Temporal short-circuits the retry.
    nonRetryableErrorTypes: ['NOT_FOUND', 'PARSE'],
  },
})

const { osvDeriveCriticalFlag } = proxyActivities<typeof activities>({
  // Paged scan over packages (~600-700k at Tier 2 scale). The whole derive
  // pass runs in ~5 minutes on the current dataset; we give it 1 hour of
  // headroom for the table to grow. No heartbeatTimeout — the activity does
  // not heartbeat, so adding one would cancel the activity silently before
  // startToCloseTimeout fires.
  startToCloseTimeout: '1 hour',
  retry: {
    initialInterval: '30 seconds',
    backoffCoefficient: 2,
    maximumAttempts: 3,
  },
})

export interface OsvSyncWorkflowInput {
  // OSV ecosystem labels in OSV's canonical case ('npm', 'Maven', 'PyPI', …).
  // Same list is used by the per-ecosystem download loop, where OSV's bucket
  // is case-sensitive (Maven/all.zip), and as the allowlist source for
  // parseOsvRecord. The activity lowercases the allowlist internally before
  // matching parsed records, since storage normalizes to lowercase per
  // ADR-0001 §OSV "Ecosystem normalization".
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
