import type { ConnectorHttp } from '@crowd/connectors'
import { mapWithConcurrency } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { IShadowDiffMismatch } from './shadowDiff'

const PULL_REQUEST_COMMITS_SYNC_NAME = 'pull-request-commits'
const COMMITS_QUERY_BATCH_SIZE = 50
const ASSOCIATED_PULL_REQUESTS_PAGE_SIZE = 5
const CONFIRM_CONCURRENCY = 3
const CONFIRM_BUDGET_MS = 60_000
const CONFIRM_REQUEST_TIMEOUT_MS = 10_000
const CONFIRM_REQUEST_MAX_ATTEMPTS = 1

interface GraphqlEnvelope<T> {
  data?: T
}

interface AssociatedPullRequestNode {
  id: string
}

interface CommitAssociationNode {
  associatedPullRequests?: {
    totalCount: number
    nodes?: (AssociatedPullRequestNode | null)[] | null
  }
}

interface CommitAssociationsResult {
  repository: Record<string, CommitAssociationNode | null> | null
}

interface CommitCandidate {
  sha: string
  parentId?: string
}

type CommitAssociationOutcome = 'kept' | 'orphaned' | 'stale_parent' | 'unconfirmed'

export function isForcePushCandidate(syncName: string, mismatch: IShadowDiffMismatch): boolean {
  return syncName === PULL_REQUEST_COMMITS_SYNC_NAME && mismatch.kind === 'missing_in_shadow'
}

export function hasForcePushCandidates(
  syncName: string,
  mismatches: IShadowDiffMismatch[],
): boolean {
  return mismatches.some((m) => isForcePushCandidate(syncName, m))
}

export interface ForcePushedCommitFilterResult {
  mismatches: IShadowDiffMismatch[]
  skippedCount: number
  staleParentCount: number
  failedCount: number
}

interface BatchConfirmation {
  orphanedShas: Set<string>
  staleParentShas: Set<string>
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
      `c${i}: object(oid: $oid${i}) { ... on Commit { associatedPullRequests(first: ${ASSOCIATED_PULL_REQUESTS_PAGE_SIZE}) { totalCount nodes { id } } } }`,
  ).join(' ')
  return `query($owner: String!, $repo: String!, ${variables}) { repository(owner: $owner, name: $repo) { ${aliases} } }`
}

function classifyAssociation(
  node: CommitAssociationNode | null | undefined,
  parentId: string | undefined,
): CommitAssociationOutcome {
  const associations = node?.associatedPullRequests
  if (!associations) {
    return 'unconfirmed'
  }
  if (associations.totalCount === 0) {
    return 'orphaned'
  }
  if (parentId === undefined) {
    return 'kept'
  }
  const associatedIds = (associations.nodes ?? [])
    .filter((n): n is AssociatedPullRequestNode => n !== null)
    .map((n) => n.id)
  if (associatedIds.includes(parentId)) {
    return 'kept'
  }
  return associatedIds.length < associations.totalCount ? 'unconfirmed' : 'stale_parent'
}

async function confirmCommitAssociations(
  http: ConnectorHttp,
  owner: string,
  repo: string,
  candidates: CommitCandidate[],
  log: Logger,
): Promise<BatchConfirmation | null> {
  try {
    const oidVariables = Object.fromEntries(candidates.map((c, i) => [`oid${i}`, c.sha]))
    const body = await http.request<GraphqlEnvelope<CommitAssociationsResult>>(
      {
        method: 'post',
        url: 'https://api.github.com/graphql',
        data: {
          query: buildAssociationsQuery(candidates.length),
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
    const confirmation: BatchConfirmation = {
      orphanedShas: new Set<string>(),
      staleParentShas: new Set<string>(),
      unconfirmedShas: new Set<string>(),
    }
    candidates.forEach((candidate, i) => {
      const outcome = classifyAssociation(repository[`c${i}`], candidate.parentId)
      if (outcome === 'orphaned') {
        confirmation.orphanedShas.add(candidate.sha)
      } else if (outcome === 'stale_parent') {
        confirmation.staleParentShas.add(candidate.sha)
      } else if (outcome === 'unconfirmed') {
        confirmation.unconfirmedShas.add(candidate.sha)
      }
    })
    return confirmation
  } catch {
    return null
  }
}

function toCommitCandidates(mismatches: IShadowDiffMismatch[]): CommitCandidate[] {
  const candidatesBySha = new Map<string, CommitCandidate>()
  for (const m of mismatches) {
    if (m.kind === 'missing_in_shadow' && !candidatesBySha.has(m.sourceId)) {
      candidatesBySha.set(m.sourceId, { sha: m.sourceId, parentId: m.sourceParentId })
    }
  }
  return [...candidatesBySha.values()]
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
    return { mismatches, skippedCount: 0, staleParentCount: 0, failedCount: 0 }
  }

  const deadline = Date.now() + CONFIRM_BUDGET_MS
  const orphanedShas = new Set<string>()
  const staleParentShas = new Set<string>()
  const unconfirmedShas = new Set<string>()

  await mapWithConcurrency(
    toBatches(toCommitCandidates(mismatches), COMMITS_QUERY_BATCH_SIZE),
    CONFIRM_CONCURRENCY,
    async (batch) => {
      const confirmation =
        Date.now() >= deadline
          ? null
          : await confirmCommitAssociations(http, owner, repo, batch, log)
      if (confirmation === null) {
        for (const { sha } of batch) unconfirmedShas.add(sha)
        return
      }
      for (const sha of confirmation.orphanedShas) orphanedShas.add(sha)
      for (const sha of confirmation.staleParentShas) staleParentShas.add(sha)
      for (const sha of confirmation.unconfirmedShas) unconfirmedShas.add(sha)
    },
  )

  return {
    mismatches: mismatches.filter(
      (m) =>
        !(
          m.kind === 'missing_in_shadow' &&
          (orphanedShas.has(m.sourceId) ||
            staleParentShas.has(m.sourceId) ||
            unconfirmedShas.has(m.sourceId))
        ),
    ),
    skippedCount: orphanedShas.size,
    staleParentCount: staleParentShas.size,
    failedCount: unconfirmedShas.size,
  }
}
