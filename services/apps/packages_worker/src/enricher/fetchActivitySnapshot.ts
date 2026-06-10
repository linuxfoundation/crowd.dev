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

function buildDateWindows(): {
  since12m: Date
  since6m: Date
  since12mIso: string
  since6mIso: string
} {
  const now = new Date()

  const since12m = new Date(now)
  since12m.setMonth(now.getMonth() - SNAPSHOT_WINDOW_MONTHS)

  const since6m = new Date(now)
  since6m.setMonth(now.getMonth() - SNAPSHOT_WINDOW_MONTHS / 2)

  return {
    since12m,
    since6m,
    since12mIso: since12m.toISOString(),
    since6mIso: since6m.toISOString(),
  }
}

interface RateLimit {
  cost: number
  remaining: number
  resetAt: string
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
    openIssues: { totalCount: number }
  }
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
  query($owner: String!, $name: String!, $since12m: GitTimestamp!, $since6m: GitTimestamp!) {
    rateLimit { cost remaining resetAt }
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
      openIssues: issues(states: OPEN) { totalCount }
    }
  }
`

const PR_PAGE_QUERY = `
  query($owner: String!, $name: String!, $cursor: String) {
    rateLimit { cost remaining resetAt }
    repository(owner: $owner, name: $name) {
      pullRequests(first: ${PAGE_SIZE}, orderBy: { field: CREATED_AT, direction: DESC }, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          createdAt
          mergedAt
          closedAt
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
): Promise<{
  nodes: PrNode[]
  rateLimitCost: number
  httpRequestCount: number
  rateLimit: RateLimit | null
}> {
  const nodes: PrNode[] = []
  let rateLimitCost = 0
  let httpRequestCount = 0
  let rateLimit: RateLimit | null = null
  let cursor: string | undefined

  for (;;) {
    const variables: Record<string, string> = { owner, name }
    if (cursor) variables.cursor = cursor

    const data = await graphqlRequest<PrPageQueryResult>(PR_PAGE_QUERY, variables, token, timeoutMs)
    rateLimitCost += data.rateLimit.cost
    httpRequestCount++
    rateLimit = data.rateLimit

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

  return { nodes, rateLimitCost, httpRequestCount, rateLimit }
}

const ISSUE_PAGE_QUERY = `
  query($owner: String!, $name: String!, $cursor: String) {
    rateLimit { cost remaining resetAt }
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
): Promise<{
  nodes: IssueNode[]
  rateLimitCost: number
  httpRequestCount: number
  rateLimit: RateLimit | null
}> {
  const nodes: IssueNode[] = []
  let rateLimitCost = 0
  let httpRequestCount = 0
  let rateLimit: RateLimit | null = null
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
    rateLimit = data.rateLimit

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

  return { nodes, rateLimitCost, httpRequestCount, rateLimit }
}

export async function fetchActivitySnapshot(
  repoId: string,
  owner: string,
  name: string,
  token: string,
  timeoutMs: number,
): Promise<RepoActivitySnapshot> {
  const { since12m, since6m, since12mIso, since6mIso } = buildDateWindows()

  const summaryData = await graphqlRequest<SummaryQueryResult>(
    SUMMARY_QUERY,
    { owner, name, since12m: since12mIso, since6m: since6mIso },
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
  const issuesOpenedLast6m = issueResult.nodes.filter(
    (n) => new Date(n.createdAt) >= since6m,
  ).length

  // Lowest remaining across all requests — remaining only decreases within a reset window
  const lastRateLimit = [summaryData.rateLimit, prResult.rateLimit, issueResult.rateLimit]
    .filter((rl): rl is RateLimit => rl !== null)
    .reduce((min, rl) => (rl.remaining < min.remaining ? rl : min))

  return {
    repoId,
    snapshotAt: new Date().toISOString(),
    windowMonths: SNAPSHOT_WINDOW_MONTHS,
    commitsLast12m,
    commitsLast6m,
    commitsPrior6m,
    prsOpenedLast12m: prResult.nodes.length,
    prsMergedLast12m: prResult.nodes.filter((n) => n.mergedAt).length,
    prsClosedUnmerged12m: prResult.nodes.filter((n) => n.closedAt && !n.mergedAt).length,
    prMedianTimeToMergeHours: prMedians.medianTimeToMergeHours,
    prMedianTimeToFirstResponseHours: prMedians.medianTimeToFirstResponseHours,
    issuesOpenedLast12m: issueResult.nodes.length,
    issuesClosedLast12m: issueResult.nodes.filter((n) => n.closedAt).length,
    issuesOpenedLast6m,
    issuesOpenedPrior6m: issueResult.nodes.length - issuesOpenedLast6m,
    issuesOpenNow: summaryData.repository.openIssues.totalCount,
    issueMedianTimeToCloseHours: issueMedians.medianTimeToCloseHours,
    issueMedianTimeToFirstResponseHours: issueMedians.medianTimeToFirstResponseHours,
    httpRequestCount: totalHttpRequests,
    rateLimitCost: totalRateLimitCost,
    rateLimitRemaining: lastRateLimit.remaining,
    rateLimitResetAt: lastRateLimit.resetAt,
  }
}
