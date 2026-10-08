import type { ConnectorHttp } from '@crowd/connectors'
import { mapWithConcurrency } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { IShadowDiffMismatch } from './shadowDiff'

const COMMIT_SYNC_NAME = 'pull-request-commits'
const PARENT_BODY_SYNTHETIC_SOURCE_ID = /^gen-(?:AE|CE|ME|RRE)_([A-Z]+_[A-Za-z0-9-]+)(?:_|$)/
const NODES_QUERY_BATCH_SIZE = 100
const CONFIRM_CONCURRENCY = 3
const CONFIRM_BUDGET_MS = 60_000
const CONFIRM_REQUEST_TIMEOUT_MS = 10_000
const CONFIRM_REQUEST_MAX_ATTEMPTS = 1

const NODES_QUERY = `query($ids: [ID!]!) { nodes(ids: $ids) { id ... on Comment { lastEditedAt } } }`

interface GraphqlEnvelope<T> {
  data?: T
}

interface NodesQueryResult {
  nodes: ({ id: string; lastEditedAt?: string | null } | null)[]
}

function editedNodeId(mismatch: IShadowDiffMismatch): string | null {
  const synthetic = PARENT_BODY_SYNTHETIC_SOURCE_ID.exec(mismatch.sourceId)
  if (synthetic) {
    return synthetic[1]
  }
  return mismatch.sourceId.startsWith('gen-') ? null : mismatch.sourceId
}

export function isEditedRecordCandidate(syncName: string, mismatch: IShadowDiffMismatch): boolean {
  return (
    syncName !== COMMIT_SYNC_NAME &&
    mismatch.kind === 'field_mismatch' &&
    (mismatch.fields?.length ?? 0) > 0 &&
    mismatch.fields.every((f) => f.field === 'body') &&
    editedNodeId(mismatch) !== null
  )
}

export function hasEditedRecordCandidates(
  syncName: string,
  mismatches: IShadowDiffMismatch[],
): boolean {
  return mismatches.some((m) => isEditedRecordCandidate(syncName, m))
}

export interface EditedRecordFilterResult {
  mismatches: IShadowDiffMismatch[]
  confirmedEditedCount: number
  unconfirmedCount: number
}

function toBatches<T>(items: T[], size: number): T[][] {
  const batches: T[][] = []
  for (let i = 0; i < items.length; i += size) {
    batches.push(items.slice(i, i + size))
  }
  return batches
}

async function fetchEditedNodeIds(
  http: ConnectorHttp,
  ids: string[],
  log: Logger,
): Promise<Set<string> | null> {
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
    return new Set(
      body.data.nodes
        .filter((node): node is { id: string; lastEditedAt: string } => Boolean(node?.lastEditedAt))
        .map((node) => node.id),
    )
  } catch {
    return null
  }
}

export async function dropConfirmedEditedRecords(
  syncName: string,
  mismatches: IShadowDiffMismatch[],
  http: ConnectorHttp,
  log: Logger,
): Promise<EditedRecordFilterResult> {
  const candidates = mismatches.filter((m) => isEditedRecordCandidate(syncName, m))
  if (candidates.length === 0) {
    return { mismatches, confirmedEditedCount: 0, unconfirmedCount: 0 }
  }

  const nodeIds = [...new Set(candidates.map((m) => editedNodeId(m) as string))]
  const batches = toBatches(nodeIds, NODES_QUERY_BATCH_SIZE)
  const deadline = Date.now() + CONFIRM_BUDGET_MS

  const editedNodeIds = new Set<string>()
  const uncheckedNodeIds = new Set<string>()

  await mapWithConcurrency(batches, CONFIRM_CONCURRENCY, async (batch) => {
    if (Date.now() >= deadline) {
      for (const id of batch) uncheckedNodeIds.add(id)
      return
    }
    const edited = await fetchEditedNodeIds(http, batch, log)
    if (edited === null) {
      for (const id of batch) uncheckedNodeIds.add(id)
      return
    }
    for (const id of edited) editedNodeIds.add(id)
  })

  const isConfirmedEdited = (m: IShadowDiffMismatch) =>
    isEditedRecordCandidate(syncName, m) && editedNodeIds.has(editedNodeId(m) as string)

  return {
    mismatches: mismatches.filter((m) => !isConfirmedEdited(m)),
    confirmedEditedCount: candidates.filter(isConfirmedEdited).length,
    unconfirmedCount: candidates.filter((m) => uncheckedNodeIds.has(editedNodeId(m) as string))
      .length,
  }
}
