import { proxyActivities } from '@temporalio/workflow'

import type * as activities from './activities'

const { processMavenCriticalBatch } = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
})

const { processMavenNonCriticalBatch } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
})

/**
 * Temporal workflow: runs a full pass of Maven package syncing.
 *
 * Phase 1 — non-critical: copies universe stats into packages (DB-only, no HTTP).
 * Phase 2 — critical: full POM enrichment (HTTP calls to Maven Central).
 *
 * Each phase loops until its batch returns empty, then the workflow exits.
 * The Temporal schedule re-triggers this workflow on the configured interval.
 */
export async function pomFetcherWorkflow(): Promise<void> {
  // Phase 1: non-critical — DB-only, fast
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const result = await processMavenNonCriticalBatch()
    if (result.processed + result.skipped + result.errors === 0) break
  }

  // Phase 2: critical — HTTP-bound, slower
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const result = await processMavenCriticalBatch()
    if (result.processed + result.skipped + result.errors === 0) break
  }
}
