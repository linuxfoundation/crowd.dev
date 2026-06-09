import { getServiceChildLogger } from '@crowd/logging'

import { IssueNode, PrNode, computeIssueMedians, computePrMedians } from './computeMedians'
import { FetchError, RepoActivitySnapshot } from './types'

const log = getServiceChildLogger('fetch-activity-snapshot')

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'
const SNAPSHOT_WINDOW_MONTHS = 12
const PAGE_SIZE = 100
const RESPONSES_PER_NODE = 50

interface GraphqlError {
  type?: string
  message?: string
}

interface GraphqlResponse<T> {
  data?: T
  errors?: GraphqlError[]
}

async function graphqlRequest<T>(
  query: string,
  variables: Record<string, string>,
  token: string,
  timeoutMs: number,
): Promise<T> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

  let response: Response
  try {
    response = await fetch(GITHUB_GRAPHQL_URL, {
      method: 'POST',
      headers: { Authorization: `bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, variables }),
      signal: controller.signal,
    })
  } catch (err) {
    throw new FetchError('TRANSIENT', `Network error: ${(err as Error).message}`)
  } finally {
    clearTimeout(timeoutId)
  }

  const resetSec = parseInt(response.headers.get('x-ratelimit-reset') ?? '0', 10)
  const resetMs = resetSec ? resetSec * 1000 + 5_000 : Date.now() + 65_000

  if (response.status === 401) throw new FetchError('AUTH', '401 Unauthorized')
  if (response.status === 403) {
    const body = await response.text()
    if (body.toLowerCase().includes('rate limit'))
      throw new FetchError('RATE_LIMIT', 'Rate limited', resetMs)
    throw new FetchError('AUTH', '403 Forbidden')
  }
  if (response.status === 404) throw new FetchError('NOT_FOUND', '404 Not Found')
  if (response.status >= 500) throw new FetchError('TRANSIENT', `${response.status} Server Error`)

  const json = (await response.json()) as GraphqlResponse<T>
  if (json.errors?.length) {
    const error = json.errors[0]
    if (error.type === 'RATE_LIMITED' || error.message?.toLowerCase().includes('rate limit'))
      throw new FetchError('RATE_LIMIT', 'RATE_LIMITED', resetMs)
    if (error.type === 'NOT_FOUND') throw new FetchError('NOT_FOUND', 'NOT_FOUND')
    if (error.message?.toLowerCase().includes('ip allow list'))
      throw new FetchError('AUTH', 'IP allowlist blocked')
    throw new FetchError('TRANSIENT', `GraphQL error: ${error.message ?? error.type}`)
  }

  return json.data as T
}

function toDateString(date: Date): string {
  return date.toISOString().slice(0, 10)
}

function buildDateWindows(): {
  since12m: Date
  since12mIso: string
  since6mIso: string
  since12mDate: string
  since6mDate: string
} {
  const now = new Date()

  const since12m = new Date(now)
  since12m.setMonth(now.getMonth() - SNAPSHOT_WINDOW_MONTHS)

  const since6m = new Date(now)
  since6m.setMonth(now.getMonth() - SNAPSHOT_WINDOW_MONTHS / 2)

  return {
    since12m,
    since12mIso: since12m.toISOString(),
    since6mIso: since6m.toISOString(),
    since12mDate: toDateString(since12m),
    since6mDate: toDateString(since6m),
  }
}

interface RateLimit {
  cost: number
}

interface IssueCount {
  issueCount: number
}

interface CommitHistory {
  totalCount: number
}

interface PageInfo {
  hasNextPage: boolean
  endCursor?: string
}

interface SummaryQueryResult {
  rateLimit: RateLimit
  repository: {
    defaultBranchRef?: {
      target?: {
        commits12m?: CommitHistory
        commits6m?: CommitHistory
        commitsPrior6m?: CommitHistory
      }
    }
  }
  prsOpened: IssueCount
  prsMerged: IssueCount
  prsClosedUnmerged: IssueCount
  issuesOpened: IssueCount
  issuesClosed: IssueCount
  issuesOpened6m: IssueCount
  issuesOpenNow: IssueCount
}

interface PrPageQueryResult {
  rateLimit: RateLimit
  repository: {
    pullRequests: {
      pageInfo: PageInfo
      nodes: PrNode[]
    }
  }
}

interface IssuePageQueryResult {
  rateLimit: RateLimit
  repository: {
    issues: {
      pageInfo: PageInfo
      nodes: IssueNode[]
    }
  }
}

const SUMMARY_QUERY = `
  query(
    $owner: String!, $name: String!,
    $since12m: GitTimestamp!, $since6m: GitTimestamp!,
    $searchPrsOpened: String!, $searchPrsMerged: String!,
    $searchPrsClosedUnmerged: String!,
    $searchIssuesOpened: String!, $searchIssuesClosed: String!,
    $searchIssuesOpened6m: String!, $searchIssuesOpenNow: String!
  ) {
    rateLimit { cost }
    repository(owner: $owner, name: $name) {
      defaultBranchRef {
        target {
          ... on Commit {
            commits12m:     history(since: $since12m)                   { totalCount }
            commits6m:      history(since: $since6m)                    { totalCount }
            commitsPrior6m: history(since: $since12m, until: $since6m) { totalCount }
          }
        }
      }
    }
    prsOpened:         search(query: $searchPrsOpened,         type: ISSUE) { issueCount }
    prsMerged:         search(query: $searchPrsMerged,         type: ISSUE) { issueCount }
    prsClosedUnmerged: search(query: $searchPrsClosedUnmerged, type: ISSUE) { issueCount }
    issuesOpened:      search(query: $searchIssuesOpened,      type: ISSUE) { issueCount }
    issuesClosed:      search(query: $searchIssuesClosed,      type: ISSUE) { issueCount }
    issuesOpened6m:    search(query: $searchIssuesOpened6m,    type: ISSUE) { issueCount }
    issuesOpenNow:     search(query: $searchIssuesOpenNow,     type: ISSUE) { issueCount }
  }
`

const PR_PAGE_QUERY = `
  query($owner: String!, $name: String!, $cursor: String) {
    rateLimit { cost }
    repository(owner: $owner, name: $name) {
      pullRequests(first: ${PAGE_SIZE}, orderBy: { field: CREATED_AT, direction: DESC }, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          createdAt
          mergedAt
          author { login }
          comments(first: ${RESPONSES_PER_NODE}) { nodes { createdAt author { login } } }
          reviews(first: ${RESPONSES_PER_NODE})  { nodes { createdAt author { login } } }
        }
      }
    }
  }
`

async function pagePrs(
  owner: string,
  name: string,
  token: string,
  timeoutMs: number,
  windowStart: Date,
): Promise<{ nodes: PrNode[]; rateLimitCost: number; httpRequestCount: number }> {
  const nodes: PrNode[] = []
  let rateLimitCost = 0
  let httpRequestCount = 0
  let cursor: string | undefined

  for (;;) {
    const variables: Record<string, string> = { owner, name }
    if (cursor) variables.cursor = cursor

    const data = await graphqlRequest<PrPageQueryResult>(PR_PAGE_QUERY, variables, token, timeoutMs)
    rateLimitCost += data.rateLimit.cost
    httpRequestCount++

    const connection = data.repository.pullRequests
    let reachedWindowBoundary = false

    for (const node of connection.nodes) {
      if (new Date(node.createdAt) < windowStart) {
        reachedWindowBoundary = true
        break
      }
      nodes.push(node)
    }

    if (reachedWindowBoundary || !connection.pageInfo.hasNextPage) break
    cursor = connection.pageInfo.endCursor
  }

  return { nodes, rateLimitCost, httpRequestCount }
}

const ISSUE_PAGE_QUERY = `
  query($owner: String!, $name: String!, $cursor: String) {
    rateLimit { cost }
    repository(owner: $owner, name: $name) {
      issues(first: ${PAGE_SIZE}, orderBy: { field: CREATED_AT, direction: DESC }, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          createdAt
          closedAt
          author { login }
          comments(first: ${RESPONSES_PER_NODE}) { nodes { createdAt author { login } } }
        }
      }
    }
  }
`

async function pageIssues(
  owner: string,
  name: string,
  token: string,
  timeoutMs: number,
  windowStart: Date,
): Promise<{ nodes: IssueNode[]; rateLimitCost: number; httpRequestCount: number }> {
  const nodes: IssueNode[] = []
  let rateLimitCost = 0
  let httpRequestCount = 0
  let cursor: string | undefined

  for (;;) {
    const variables: Record<string, string> = { owner, name }
    if (cursor) variables.cursor = cursor

    const data = await graphqlRequest<IssuePageQueryResult>(
      ISSUE_PAGE_QUERY,
      variables,
      token,
      timeoutMs,
    )
    rateLimitCost += data.rateLimit.cost
    httpRequestCount++

    const connection = data.repository.issues
    let reachedWindowBoundary = false

    for (const node of connection.nodes) {
      if (new Date(node.createdAt) < windowStart) {
        reachedWindowBoundary = true
        break
      }
      nodes.push(node)
    }

    if (reachedWindowBoundary || !connection.pageInfo.hasNextPage) break
    cursor = connection.pageInfo.endCursor
  }

  return { nodes, rateLimitCost, httpRequestCount }
}

export async function fetchActivitySnapshot(
  repoId: string,
  owner: string,
  name: string,
  token: string,
  timeoutMs: number,
): Promise<RepoActivitySnapshot> {
  const { since12m, since12mIso, since6mIso, since12mDate, since6mDate } = buildDateWindows()
  const repoFilter = `repo:${owner}/${name}`

  const summaryData = await graphqlRequest<SummaryQueryResult>(
    SUMMARY_QUERY,
    {
      owner,
      name,
      since12m: since12mIso,
      since6m: since6mIso,
      searchPrsOpened: `${repoFilter} is:pr created:>=${since12mDate}`,
      searchPrsMerged: `${repoFilter} is:pr is:merged created:>=${since12mDate}`,
      searchPrsClosedUnmerged: `${repoFilter} is:pr is:unmerged is:closed created:>=${since12mDate}`,
      searchIssuesOpened: `${repoFilter} is:issue created:>=${since12mDate}`,
      searchIssuesClosed: `${repoFilter} is:issue is:closed created:>=${since12mDate}`,
      searchIssuesOpened6m: `${repoFilter} is:issue created:>=${since6mDate}`,
      searchIssuesOpenNow: `${repoFilter} is:issue is:open`,
    },
    token,
    timeoutMs,
  )

  let totalRateLimitCost = summaryData.rateLimit.cost
  let totalHttpRequests = 1 // summary query

  const commitTarget = summaryData.repository.defaultBranchRef?.target
  const commitsLast12m: number | null = commitTarget?.commits12m?.totalCount ?? null
  const commitsLast6m: number | null = commitTarget?.commits6m?.totalCount ?? null
  const commitsPrior6m: number | null = commitTarget?.commitsPrior6m?.totalCount ?? null

  log.debug({ owner, name, commitsLast12m, commitsLast6m, commitsPrior6m }, 'Commit totals fetched')

  const [prResult, issueResult] = await Promise.all([
    pagePrs(owner, name, token, timeoutMs, since12m),
    pageIssues(owner, name, token, timeoutMs, since12m),
  ])

  totalRateLimitCost += prResult.rateLimitCost + issueResult.rateLimitCost
  totalHttpRequests += prResult.httpRequestCount + issueResult.httpRequestCount

  log.debug(
    {
      owner,
      name,
      prCount: prResult.nodes.length,
      issueCount: issueResult.nodes.length,
      totalRateLimitCost,
    },
    'Snapshot paging complete',
  )

  const prMedians = computePrMedians(prResult.nodes)
  const issueMedians = computeIssueMedians(issueResult.nodes)

  return {
    repoId,
    snapshotAt: new Date().toISOString(),
    windowMonths: SNAPSHOT_WINDOW_MONTHS,
    commitsLast12m,
    commitsLast6m,
    commitsPrior6m,
    prsOpenedLast12m: summaryData.prsOpened.issueCount,
    prsMergedLast12m: summaryData.prsMerged.issueCount,
    prsClosedUnmerged12m: summaryData.prsClosedUnmerged.issueCount,
    prMedianTimeToMergeHours: prMedians.medianTimeToMergeHours,
    prMedianTimeToFirstResponseHours: prMedians.medianTimeToFirstResponseHours,
    issuesOpenedLast12m: summaryData.issuesOpened.issueCount,
    issuesClosedLast12m: summaryData.issuesClosed.issueCount,
    issuesOpenedLast6m: summaryData.issuesOpened6m.issueCount,
    issuesOpenedPrior6m:
      summaryData.issuesOpened.issueCount - summaryData.issuesOpened6m.issueCount,
    issuesOpenNow: summaryData.issuesOpenNow.issueCount,
    issueMedianTimeToCloseHours: issueMedians.medianTimeToCloseHours,
    issueMedianTimeToFirstResponseHours: issueMedians.medianTimeToFirstResponseHours,
    httpRequestCount: totalHttpRequests,
    rateLimitCost: totalRateLimitCost,
  }
}
