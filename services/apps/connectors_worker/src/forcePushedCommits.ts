import type { ConnectorHttp } from '@crowd/connectors'
import { mapWithConcurrency } from '@crowd/connectors'
import {
  IRequestLimits,
  fetchPullRequestsForCommit,
} from '@crowd/connectors/src/connectors/github/commits'
import type { Logger } from '@crowd/logging'

import { IShadowDiffMismatch } from './shadowDiff'

const PULL_REQUEST_COMMITS_SYNC_NAME = 'pull-request-commits'
const CONFIRM_CONCURRENCY = 5
const CONFIRM_BUDGET_MS = 60_000
const CONFIRM_REQUEST_LIMITS: IRequestLimits = { timeoutMs: 10_000, maxAttempts: 1 }

export function hasForcePushCandidates(
  syncName: string,
  mismatches: IShadowDiffMismatch[],
): boolean {
  return (
    syncName === PULL_REQUEST_COMMITS_SYNC_NAME &&
    mismatches.some((m) => m.kind === 'missing_in_shadow')
  )
}

export interface ForcePushedCommitFilterResult {
  mismatches: IShadowDiffMismatch[]
  skippedCount: number
  failedCount: number
}

type CommitCheckOutcome = 'orphaned' | 'kept' | 'check_failed'

async function checkCommitOrphaned(
  http: ConnectorHttp,
  owner: string,
  repo: string,
  sha: string,
  log: Logger,
): Promise<CommitCheckOutcome> {
  try {
    const pulls = await fetchPullRequestsForCommit(
      http,
      owner,
      repo,
      sha,
      log,
      CONFIRM_REQUEST_LIMITS,
    )
    if (!Array.isArray(pulls)) {
      return 'check_failed'
    }
    return pulls.length === 0 ? 'orphaned' : 'kept'
  } catch {
    return 'check_failed'
  }
}

export async function dropConfirmedForcePushedCommits(
  syncName: string,
  mismatches: IShadowDiffMismatch[],
  owner: string,
  repo: string,
  http: ConnectorHttp,
  log: Logger,
): Promise<ForcePushedCommitFilterResult> {
  if (!hasForcePushCandidates(syncName, mismatches)) {
    return { mismatches, skippedCount: 0, failedCount: 0 }
  }

  const candidates = mismatches.filter((m) => m.kind === 'missing_in_shadow')
  const deadline = Date.now() + CONFIRM_BUDGET_MS
  const outcomes = await mapWithConcurrency(
    candidates,
    CONFIRM_CONCURRENCY,
    async (mismatch): Promise<CommitCheckOutcome> =>
      Date.now() >= deadline
        ? 'check_failed'
        : checkCommitOrphaned(http, owner, repo, mismatch.sourceId, log),
  )

  const orphanedSourceIds = new Set(
    candidates.filter((_, index) => outcomes[index] === 'orphaned').map((m) => m.sourceId),
  )
  const failedCount = outcomes.filter((outcome) => outcome === 'check_failed').length

  return {
    mismatches: mismatches.filter(
      (m) => !(m.kind === 'missing_in_shadow' && orphanedSourceIds.has(m.sourceId)),
    ),
    skippedCount: orphanedSourceIds.size,
    failedCount,
  }
}
