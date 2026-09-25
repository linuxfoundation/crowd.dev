import type { ConnectorHttp } from '@crowd/connectors'
import { mapWithConcurrency } from '@crowd/connectors'
import { fetchPullRequestsForCommit } from '@crowd/connectors/src/connectors/github/commits'
import type { Logger } from '@crowd/logging'

import { IShadowDiffMismatch } from './shadowDiff'

const PULL_REQUEST_COMMITS_SYNC_NAME = 'pull-request-commits'
const CONFIRM_CONCURRENCY = 5

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
    const pulls = await fetchPullRequestsForCommit(http, owner, repo, sha, log)
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
  if (syncName !== PULL_REQUEST_COMMITS_SYNC_NAME) {
    return { mismatches, skippedCount: 0, failedCount: 0 }
  }

  const candidates = mismatches.filter((m) => m.kind === 'missing_in_shadow')
  if (candidates.length === 0) {
    return { mismatches, skippedCount: 0, failedCount: 0 }
  }

  const outcomes = await mapWithConcurrency(candidates, CONFIRM_CONCURRENCY, (mismatch) =>
    checkCommitOrphaned(http, owner, repo, mismatch.sourceId, log),
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
