import type { CountWindow } from '@crowd/data-access-layer/src/connectors'
import { GithubActivityType } from '@crowd/integrations'

import type { CountContext, SourceCounter } from '../../../types'
import { type CountRange, boundedRange } from '../../../validation'
import { githubGraphql } from '../gql'
import { parseRepoChannel } from '../paging'

interface TotalCountResponse {
  repository: Record<string, { totalCount: number }>
}

type SearchType = 'ISSUE' | 'DISCUSSION'

type SearchCountField = 'issueCount' | 'discussionCount'

type SearchResponse = { search: Record<SearchCountField, number> }

interface ForkCreatedAtPage {
  repository: {
    forks: {
      pageInfo: { endCursor: string | null; hasNextPage: boolean }
      nodes: { createdAt: string }[]
    }
  }
}

const CHEAP = { range: 'cheap', 'all-time': 'cheap' } as const

const FORK_PAGE_SIZE = 100

const FORK_CREATED_AT_QUERY = `
  query ($owner: String!, $repo: String!, $first: Int!, $cursor: String) {
    repository(owner: $owner, name: $repo) {
      forks(first: $first, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          createdAt
        }
      }
    }
  }
`

function toSearchSeconds(epochMs: number): string {
  return new Date(epochMs).toISOString().replace(/\.\d{3}Z$/, 'Z')
}

function searchRange(window: CountRange): string {
  return `${toSearchSeconds(Date.parse(window.from))}..${toSearchSeconds(Date.parse(window.to) - 1000)}`
}

function countByWindow(
  window: CountWindow,
  countRange: (range: CountRange) => Promise<number>,
  countTotal: () => Promise<number>,
): Promise<number> {
  const range = boundedRange(window)
  return range === null ? countTotal() : countRange(range)
}

function repoSlug(ctx: CountContext): string {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  return `${owner}/${repo}`
}

async function searchTotal(
  ctx: CountContext,
  type: SearchType,
  qualifiers: string,
): Promise<number> {
  const field: SearchCountField = type === 'ISSUE' ? 'issueCount' : 'discussionCount'
  const data = await githubGraphql<SearchResponse>(
    ctx.http,
    `query ($q: String!) { search(type: ${type}, query: $q) { ${field} } }`,
    { q: `repo:${repoSlug(ctx)} ${qualifiers}` },
    ctx.log,
  )
  return data.search[field]
}

async function repositoryTotal(
  ctx: CountContext,
  field: string,
  selection: string,
): Promise<number> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const data = await githubGraphql<TotalCountResponse>(
    ctx.http,
    `query ($owner: String!, $repo: String!) {
      repository(owner: $owner, name: $repo) { ${field}: ${selection} { totalCount } }
    }`,
    { owner, repo },
    ctx.log,
  )
  return data.repository[field].totalCount
}

async function countForksInRange(ctx: CountContext, window: CountRange): Promise<number> {
  const { owner, repo } = parseRepoChannel(ctx.channel.channelName)
  const from = Date.parse(window.from)
  const to = Date.parse(window.to)
  let count = 0
  let cursor: string | null = null
  for (;;) {
    const data: ForkCreatedAtPage = await githubGraphql<ForkCreatedAtPage>(
      ctx.http,
      FORK_CREATED_AT_QUERY,
      { owner, repo, first: FORK_PAGE_SIZE, cursor },
      ctx.log,
    )
    const { nodes, pageInfo } = data.repository.forks
    for (const node of nodes) {
      const createdAt = Date.parse(node.createdAt)
      if (createdAt < from) {
        return count
      }
      if (createdAt < to) {
        count++
      }
    }
    if (!pageInfo.hasNextPage) {
      return count
    }
    cursor = pageInfo.endCursor
  }
}

export const githubCheapCounters: SourceCounter[] = [
  {
    key: GithubActivityType.PULL_REQUEST_OPENED,
    syncName: 'pull-requests',
    activityTypes: [GithubActivityType.PULL_REQUEST_OPENED],
    cost: CHEAP,
    count: (ctx, window) =>
      countByWindow(
        window,
        (range) => searchTotal(ctx, 'ISSUE', `is:pr created:${searchRange(range)}`),
        () => repositoryTotal(ctx, 'pullRequests', 'pullRequests'),
      ),
  },
  {
    key: GithubActivityType.PULL_REQUEST_MERGED,
    syncName: 'pull-requests',
    activityTypes: [GithubActivityType.PULL_REQUEST_MERGED],
    cost: CHEAP,
    count: (ctx, window) =>
      countByWindow(
        window,
        (range) => searchTotal(ctx, 'ISSUE', `is:pr is:merged merged:${searchRange(range)}`),
        () => repositoryTotal(ctx, 'merged', 'pullRequests(states: MERGED)'),
      ),
  },
  {
    key: GithubActivityType.ISSUE_OPENED,
    syncName: 'issues',
    activityTypes: [GithubActivityType.ISSUE_OPENED],
    cost: CHEAP,
    count: (ctx, window) =>
      countByWindow(
        window,
        (range) => searchTotal(ctx, 'ISSUE', `is:issue created:${searchRange(range)}`),
        () => repositoryTotal(ctx, 'issues', 'issues'),
      ),
  },
  {
    key: GithubActivityType.ISSUE_CLOSED,
    syncName: 'issues',
    activityTypes: [GithubActivityType.ISSUE_CLOSED],
    cost: CHEAP,
    count: (ctx, window) =>
      countByWindow(
        window,
        (range) => searchTotal(ctx, 'ISSUE', `is:issue is:closed closed:${searchRange(range)}`),
        () => repositoryTotal(ctx, 'closed', 'issues(states: CLOSED)'),
      ),
  },
  {
    key: GithubActivityType.DISCUSSION_STARTED,
    syncName: 'discussions',
    activityTypes: [GithubActivityType.DISCUSSION_STARTED],
    cost: CHEAP,
    count: (ctx, window) =>
      countByWindow(
        window,
        (range) => searchTotal(ctx, 'DISCUSSION', `created:${searchRange(range)}`),
        () => repositoryTotal(ctx, 'discussions', 'discussions'),
      ),
  },
  {
    key: GithubActivityType.FORK,
    syncName: 'forks',
    activityTypes: [GithubActivityType.FORK],
    cost: CHEAP,
    count: (ctx, window) =>
      countByWindow(
        window,
        (range) => countForksInRange(ctx, range),
        () => repositoryTotal(ctx, 'forks', 'forks'),
      ),
  },
]
