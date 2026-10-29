import type { CountWindow } from '@crowd/data-access-layer/src/connectors'
import { GithubActivityType } from '@crowd/integrations'

import type { CountContext, SourceCounter } from '../../../types'
import { githubGraphql } from '../gql'
import { parseRepoChannel } from '../paging'

type PrTimelineKey =
  | GithubActivityType.PULL_REQUEST_CLOSED
  | GithubActivityType.PULL_REQUEST_REVIEWED
  | GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED
  | GithubActivityType.PULL_REQUEST_ASSIGNED

type Bound = 'all' | 'from' | 'to'

type PullRequestNode = ({ updatedAt?: string } & Record<string, unknown>) | null

interface PullRequestsPage {
  repository: {
    pullRequests: {
      pageInfo: { endCursor: string | null; hasNextPage: boolean }
      nodes: PullRequestNode[]
    }
  }
}

const PAGE_SIZE = 50

const ITEM_TYPES: Record<PrTimelineKey, string> = {
  [GithubActivityType.PULL_REQUEST_CLOSED]: 'CLOSED_EVENT',
  [GithubActivityType.PULL_REQUEST_REVIEWED]: 'PULL_REQUEST_REVIEW',
  [GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED]: 'REVIEW_REQUESTED_EVENT',
  [GithubActivityType.PULL_REQUEST_ASSIGNED]: 'ASSIGNED_EVENT',
}

const KEYS = Object.keys(ITEM_TYPES) as PrTimelineKey[]

function alias(key: PrTimelineKey, bound: Bound): string {
  return `${ITEM_TYPES[key].toLowerCase()}_${bound}`
}

function selection(key: PrTimelineKey, bound: Bound): string {
  const since = bound === 'all' ? '' : `, since: $${bound}`
  return `${alias(key, bound)}: timelineItems(first: 1, itemTypes: [${ITEM_TYPES[key]}]${since}) { filteredCount }`
}

function buildQuery(isRange: boolean): string {
  const bounds: Bound[] = isRange ? ['from', 'to'] : ['all']
  const selections = KEYS.flatMap((key) => bounds.map((bound) => selection(key, bound)))
  const variables = isRange ? ', $from: DateTime!, $to: DateTime!' : ''
  const order = isRange ? ', orderBy: {field: UPDATED_AT, direction: DESC}' : ''
  return `
    query ($owner: String!, $repo: String!, $cursor: String${variables}) {
      repository(owner: $owner, name: $repo) {
        pullRequests(first: ${PAGE_SIZE}, after: $cursor${order}) {
          pageInfo { endCursor hasNextPage }
          nodes {
            ${isRange ? 'updatedAt' : '__typename'}
            ${selections.join('\n            ')}
          }
        }
      }
    }
  `
}

function filteredCount(node: NonNullable<PullRequestNode>, key: PrTimelineKey, bound: Bound) {
  return (node[alias(key, bound)] as { filteredCount: number }).filteredCount
}

async function countPrTimeline(
  ctx: CountContext,
  window: CountWindow,
): Promise<Record<PrTimelineKey, number>> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const query = buildQuery(window.kind === 'range')
  const windowVariables = window.kind === 'range' ? { from: window.from, to: window.to } : {}
  const totals = Object.fromEntries(KEYS.map((key) => [key, 0])) as Record<PrTimelineKey, number>
  let cursor: string | null = null
  for (;;) {
    const data: PullRequestsPage = await githubGraphql<PullRequestsPage>(
      ctx.http,
      query,
      { owner, repo, cursor, ...windowVariables },
      ctx.log,
    )
    const { nodes, pageInfo } = data.repository.pullRequests
    for (const node of nodes) {
      if (node === null) {
        continue
      }
      if (window.kind === 'range') {
        if (Date.parse(node.updatedAt as string) < Date.parse(window.from)) {
          return totals
        }
        for (const key of KEYS) {
          totals[key] += filteredCount(node, key, 'from') - filteredCount(node, key, 'to')
        }
      } else {
        for (const key of KEYS) {
          totals[key] += filteredCount(node, key, 'all')
        }
      }
    }
    if (!pageInfo.hasNextPage) {
      return totals
    }
    cursor = pageInfo.endCursor
  }
}

function prTimelineCounter(key: PrTimelineKey): SourceCounter {
  return {
    key,
    syncName: 'pull-requests',
    activityTypes: [key],
    cost: { range: 'cheap', 'all-time': 'expensive' },
    count: async (ctx, window) => (await countPrTimeline(ctx, window))[key],
  }
}

export const githubPrTimelineCounters: SourceCounter[] = KEYS.map(prTimelineCounter)
