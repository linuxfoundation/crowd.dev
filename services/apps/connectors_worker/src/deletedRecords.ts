import type { ConnectorHttp } from '@crowd/connectors'
import { mapWithConcurrency } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { IShadowDiffMismatch } from './shadowDiff'

const COMMIT_SYNC_NAME = 'pull-request-commits'
const SYNTHETIC_SOURCE_ID_PREFIX = 'gen-'
const NODES_QUERY_BATCH_SIZE = 100
const CONFIRM_CONCURRENCY = 3
const CONFIRM_BUDGET_MS = 60_000
const CONFIRM_REQUEST_TIMEOUT_MS = 10_000
const CONFIRM_REQUEST_MAX_ATTEMPTS = 1

const NODES_QUERY = `query($ids: [ID!]!) { nodes(ids: $ids) { id } }`

interface GraphqlEnvelope<T> {
  data?: T
  errors?: { type?: string; message?: string; path?: (string | number)[] }[]
}

interface NodesQueryResult {
  nodes: ({ id: string } | null)[]
}

function isNodeIdCandidate(mismatch: IShadowDiffMismatch): boolean {
  return !mismatch.sourceId.startsWith(SYNTHETIC_SOURCE_ID_PREFIX)
}

export function hasDeletedRecordCandidates(
  syncName: string,
  mismatches: IShadowDiffMismatch[],
): boolean {
  return (
    syncName !== COMMIT_SYNC_NAME &&
    mismatches.some((m) => m.kind === 'missing_in_shadow' && isNodeIdCandidate(m))
  )
}

export interface DeletedRecordFilterResult {
  mismatches: IShadowDiffMismatch[]
  confirmedDeletedCount: number
  keptUnconfirmedCount: number
}

function toBatches<T>(items: T[], size: number): T[][] {
  const batches: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    batches.push(items.slice(i, i + size))
  }
  return batches
}

interface BatchConfirmation {
  deletedIds: Set<string>
  unconfirmedIds: Set<string>
}

async function confirmDeletedNodeIds(
  http: ConnectorHttp,
  ids: string[],
  log: Logger,
): Promise<BatchConfirmation | null> {
  try {
    const body = await http.request<GraphqlEnvelope<NodesQueryResult>>(
      {
        method: 'post',
        url: 'https://api.github.com/graphql',
        data: { query: NODES_QUERY, variables: { ids } },
        timeout: CONFIRM_REQUEST_TIMEOUT_MS,
      },
      log,
      CONFIRM_REQUEST_MAX_ATTEMPTS,
    )
    if (!body.data || body.data.nodes.length !== ids.length) {
      return null
    }
    const notFoundIndexes = new Set(
      (body.errors ?? [])
        .filter((e) => e.type === 'NOT_FOUND' && e.path?.[0] === 'nodes')
        .map((e) => e.path?.[1])
        .filter((i): i is number => typeof i === 'number'),
    )
    const deletedIds = new Set<string>()
    const unconfirmedIds = new Set<string>()
    body.data.nodes.forEach((node, i) => {
      if (node !== null) {
        return
      }
      if (notFoundIndexes.has(i)) {
        deletedIds.add(ids[i])
      } else {
        unconfirmedIds.add(ids[i])
      }
    })
    return { deletedIds, unconfirmedIds }
  } catch {
    return null
  }
}

export async function dropConfirmedDeletedRecords(
  syncName: string,
  mismatches: IShadowDiffMismatch[],
  http: ConnectorHttp,
  log: Logger,
): Promise<DeletedRecordFilterResult> {
  if (!hasDeletedRecordCandidates(syncName, mismatches)) {
    return { mismatches, confirmedDeletedCount: 0, keptUnconfirmedCount: 0 }
  }

  const candidates = mismatches.filter(
    (m) => m.kind === 'missing_in_shadow' && isNodeIdCandidate(m),
  )
  const candidateIds = [...new Set(candidates.map((m) => m.sourceId))]
  const batches = toBatches(candidateIds, NODES_QUERY_BATCH_SIZE)
  const deadline = Date.now() + CONFIRM_BUDGET_MS

  const confirmedDeletedIds = new Set<string>()
  const uncheckedIds = new Set<string>()

  await mapWithConcurrency(batches, CONFIRM_CONCURRENCY, async (batch) => {
    if (Date.now() >= deadline) {
      for (const id of batch) uncheckedIds.add(id)
      return
    }
    const confirmation = await confirmDeletedNodeIds(http, batch, log)
    if (confirmation === null) {
      for (const id of batch) uncheckedIds.add(id)
      return
    }
    for (const id of confirmation.deletedIds) confirmedDeletedIds.add(id)
    for (const id of confirmation.unconfirmedIds) uncheckedIds.add(id)
  })

  return {
    mismatches: mismatches.filter(
      (m) =>
        !(
          m.kind === 'missing_in_shadow' &&
          isNodeIdCandidate(m) &&
          confirmedDeletedIds.has(m.sourceId)
        ),
    ),
    confirmedDeletedCount: confirmedDeletedIds.size,
    keptUnconfirmedCount: uncheckedIds.size,
  }
}
