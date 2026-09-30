import type { ConnectorHttp } from '@crowd/connectors'
import { mapWithConcurrency } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { IShadowDiffMismatch } from './shadowDiff'

const PULL_REQUEST_COMMITS_SYNC_NAME = 'pull-request-commits'
const COMMITS_QUERY_BATCH_SIZE = 50
const CONFIRM_CONCURRENCY = 3
const CONFIRM_BUDGET_MS = 60_000
const CONFIRM_REQUEST_TIMEOUT_MS = 10_000
const CONFIRM_REQUEST_MAX_ATTEMPTS = 1

interface GraphqlEnvelope<T> {
  data?: T
}

interface CommitAssociationNode {
  associatedPullRequests?: { totalCount: number }
}

interface CommitAssociationsResult {
  repository: Record<string, CommitAssociationNode | null> | null
}

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

interface BatchConfirmation {
  orphanedShas: Set<string>
  unconfirmedShas: Set<string>
}

function toBatches<T>(items: T[], size: number): T[][] {
  const batches: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    batches.push(items.slice(i, i + size))
  }
  return batches
}

function buildAssociationsQuery(size: number): string {
  const variables = Array.from({ length: size }, (_, i) => `$oid${i}: GitObjectID!`).join(', ')
  const aliases = Array.from(
    { length: size },
    (_, i) =>
      `c${i}: object(oid: $oid${i}) { ... on Commit { associatedPullRequests(first: 1) { totalCount } } }`,
  ).join(' ')
  return `query($owner: String!, $repo: String!, ${variables}) { repository(owner: $owner, name: $repo) { ${aliases} } }`
}

async function confirmOrphanedCommits(
  http: ConnectorHttp,
  owner: string,
  repo: string,
  shas: string[],
  log: Logger,
): Promise<BatchConfirmation | null> {
  try {
    const oidVariables = Object.fromEntries(shas.map((sha, i) => [`oid${i}`, sha]))
    const body = await http.request<GraphqlEnvelope<CommitAssociationsResult>>(
      {
        method: 'post',
        url: 'https://api.github.com/graphql',
        data: {
          query: buildAssociationsQuery(shas.length),
          variables: { owner, repo, ...oidVariables },
        },
        timeout: CONFIRM_REQUEST_TIMEOUT_MS,
      },
      log,
      CONFIRM_REQUEST_MAX_ATTEMPTS,
    )
    const repository = body.data?.repository
    if (!repository) {
      return null
    }
    const orphanedShas = new Set<string>()
    const unconfirmedShas = new Set<string>()
    shas.forEach((sha, i) => {
      const totalCount = repository[`c${i}`]?.associatedPullRequests?.totalCount
      if (totalCount === 0) {
        orphanedShas.add(sha)
      } else if (totalCount === undefined) {
        unconfirmedShas.add(sha)
      }
    })
    return { orphanedShas, unconfirmedShas }
  } catch {
    return null
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

  const candidateShas = [
    ...new Set(mismatches.filter((m) => m.kind === 'missing_in_shadow').map((m) => m.sourceId)),
  ]
  const deadline = Date.now() + CONFIRM_BUDGET_MS
  const orphanedShas = new Set<string>()
  const unconfirmedShas = new Set<string>()

  await mapWithConcurrency(
    toBatches(candidateShas, COMMITS_QUERY_BATCH_SIZE),
    CONFIRM_CONCURRENCY,
    async (batch) => {
      const confirmation =
        Date.now() >= deadline ? null : await confirmOrphanedCommits(http, owner, repo, batch, log)
      if (confirmation === null) {
        for (const sha of batch) unconfirmedShas.add(sha)
        return
      }
      for (const sha of confirmation.orphanedShas) orphanedShas.add(sha)
      for (const sha of confirmation.unconfirmedShas) unconfirmedShas.add(sha)
    },
  )

  return {
    mismatches: mismatches.filter(
      (m) =>
        !(
          m.kind === 'missing_in_shadow' &&
          (orphanedShas.has(m.sourceId) || unconfirmedShas.has(m.sourceId))
        ),
    ),
    skippedCount: orphanedShas.size,
    failedCount: unconfirmedShas.size,
  }
}
