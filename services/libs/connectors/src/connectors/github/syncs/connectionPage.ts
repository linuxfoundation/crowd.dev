import type { Logger } from '@crowd/logging'

import type { SyncContext } from '../../../types'
import { githubGraphql } from '../gql'

const CONNECTION_MAX_ATTEMPTS = 2

export interface ConnectionPage<TNode> {
  totalCount: number
  pageInfo: {
    endCursor: string | null
    hasNextPage: boolean
  }
  edges: ({ node: TNode | null } | null)[]
}

export interface ConnectionSpec<TPage, TNode> {
  query: string
  select: (page: TPage) => ConnectionPage<TNode> | null | undefined
  missingNode: string
  missingConnection: string
  stillMissingConnection: string
}

export async function fetchConnectionPage<TPage extends { nodes: unknown[] }, TNode>(
  ctx: SyncContext,
  spec: ConnectionSpec<TPage, TNode>,
  variables: Record<string, unknown>,
  log: Logger,
): Promise<ConnectionPage<TNode> | null> {
  for (let attempt = 1; attempt <= CONNECTION_MAX_ATTEMPTS; attempt++) {
    const page = await githubGraphql<TPage>(ctx.http, spec.query, variables, log)
    if (!page.nodes[0]) {
      log.warn(spec.missingNode)
      return null
    }
    const connection = spec.select(page)
    if (connection?.edges) {
      return connection
    }
    log.warn(
      attempt < CONNECTION_MAX_ATTEMPTS ? spec.missingConnection : spec.stillMissingConnection,
    )
  }
  return null
}
