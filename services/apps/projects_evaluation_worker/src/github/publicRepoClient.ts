import { GithubPublicClientError, IPublicRepoMetrics, IPublicRepoReadme } from './types'

const GITHUB_API_URL = 'https://api.github.com'
const DEFAULT_README_MAX_CHARS = 8_000

const METRICS_QUERY = `
  query($owner: String!, $name: String!) {
    repository(owner: $owner, name: $name) {
      description
      primaryLanguage { name }
      stargazerCount
      forkCount
      pushedAt
      createdAt
      isArchived
      isFork
      openIssues: issues(states: OPEN) { totalCount }
      closedIssues: issues(states: CLOSED) { totalCount }
      openPullRequests: pullRequests(states: OPEN) { totalCount }
      closedPullRequests: pullRequests(states: [CLOSED, MERGED]) { totalCount }
    }
  }
`

interface MetricsGraphqlResponse {
  data?: {
    repository: {
      description: string | null
      primaryLanguage: { name: string } | null
      stargazerCount: number
      forkCount: number
      pushedAt: string | null
      createdAt: string
      isArchived: boolean
      isFork: boolean
      openIssues: { totalCount: number }
      closedIssues: { totalCount: number }
      openPullRequests: { totalCount: number }
      closedPullRequests: { totalCount: number }
    } | null
  }
  errors?: Array<{ type?: string; message?: string }>
}

export function getGithubToken(): string {
  const token = process.env.CROWD_PROJECT_EVALUATION_GITHUB_TOKEN

  if (!token) {
    throw new GithubPublicClientError(
      'CONFIG',
      'Missing CROWD_PROJECT_EVALUATION_GITHUB_TOKEN configuration',
    )
  }

  return token
}

export function parseGithubUrl(url: string): { owner: string; name: string } {
  const match = url.match(/https?:\/\/github\.com\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/)
  if (!match) {
    throw new GithubPublicClientError('NOT_FOUND', `Cannot parse GitHub URL: ${url}`)
  }
  return { owner: match[1], name: match[2] }
}

function rateLimitResetMs(headers: Headers): number {
  const retryAfterSec = parseInt(headers.get('retry-after') ?? '0', 10)
  const resetSec = parseInt(headers.get('x-ratelimit-reset') ?? '0', 10)
  if (retryAfterSec) return Date.now() + retryAfterSec * 1000
  if (resetSec) return resetSec * 1000 + 5_000
  return Date.now() + 65_000
}

// A 401 here means our own token is missing/expired/revoked, never a repo signal — it must
// propagate as an error so callers stop instead of guessing, unlike the silent-failure bug
// this client's Python predecessor had (see CM-1469 / Joana's onboarding findings).
function assertAuthenticated(response: Response, url: string): void {
  if (response.status === 401) {
    throw new GithubPublicClientError('AUTH', `401 Unauthorized calling GitHub for ${url}`)
  }
}

export async function fetchPublicRepoMetrics(
  repoUrl: string,
  token: string,
): Promise<IPublicRepoMetrics> {
  const { owner, name } = parseGithubUrl(repoUrl)

  const response = await fetch(`${GITHUB_API_URL}/graphql`, {
    method: 'POST',
    headers: {
      Authorization: `bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: METRICS_QUERY, variables: { owner, name } }),
  })

  assertAuthenticated(response, repoUrl)

  if (response.status === 403) {
    const body = await response.text()
    if (body.toLowerCase().includes('rate limit')) {
      throw new GithubPublicClientError(
        'RATE_LIMIT',
        `Rate limited fetching metrics for ${repoUrl}`,
        rateLimitResetMs(response.headers),
      )
    }
    throw new GithubPublicClientError('AUTH', `403 Forbidden fetching metrics for ${repoUrl}`)
  }

  if (response.status === 404) {
    throw new GithubPublicClientError('NOT_FOUND', `Repository not found: ${repoUrl}`)
  }

  if (!response.ok) {
    throw new GithubPublicClientError(
      'TRANSIENT',
      `GitHub returned HTTP ${response.status} fetching metrics for ${repoUrl}`,
    )
  }

  const json = (await response.json()) as MetricsGraphqlResponse

  if (json.errors?.length) {
    const err = json.errors[0]
    if (err.type === 'RATE_LIMITED' || err.message?.toLowerCase().includes('rate limit')) {
      throw new GithubPublicClientError(
        'RATE_LIMIT',
        `RATE_LIMITED fetching metrics for ${repoUrl}`,
        rateLimitResetMs(response.headers),
      )
    }
    if (err.type === 'NOT_FOUND') {
      throw new GithubPublicClientError('NOT_FOUND', `Repository not found: ${repoUrl}`)
    }
    throw new GithubPublicClientError(
      'TRANSIENT',
      `GraphQL error fetching metrics for ${repoUrl}: ${err.message ?? err.type}`,
    )
  }

  const repo = json.data?.repository
  if (!repo) {
    throw new GithubPublicClientError('NOT_FOUND', `Repository not found: ${repoUrl}`)
  }

  return {
    description: repo.description,
    primaryLanguage: repo.primaryLanguage?.name ?? null,
    stars: repo.stargazerCount,
    forks: repo.forkCount,
    openIssues: repo.openIssues.totalCount,
    closedIssues: repo.closedIssues.totalCount,
    openPullRequests: repo.openPullRequests.totalCount,
    closedPullRequests: repo.closedPullRequests.totalCount,
    pushedAt: repo.pushedAt,
    createdAt: repo.createdAt,
    isArchived: repo.isArchived,
    isFork: repo.isFork,
  }
}

export async function fetchPublicRepoReadme(
  repoUrl: string,
  token: string,
  maxChars: number = DEFAULT_README_MAX_CHARS,
): Promise<IPublicRepoReadme | null> {
  const { owner, name } = parseGithubUrl(repoUrl)

  const response = await fetch(`${GITHUB_API_URL}/repos/${owner}/${name}/readme`, {
    headers: {
      Authorization: `bearer ${token}`,
      Accept: 'application/vnd.github.raw+json',
    },
  })

  assertAuthenticated(response, repoUrl)

  if (response.status === 404) {
    return null
  }

  if (response.status === 403) {
    const body = await response.text()
    if (body.toLowerCase().includes('rate limit')) {
      throw new GithubPublicClientError(
        'RATE_LIMIT',
        `Rate limited fetching README for ${repoUrl}`,
        rateLimitResetMs(response.headers),
      )
    }
    throw new GithubPublicClientError('AUTH', `403 Forbidden fetching README for ${repoUrl}`)
  }

  if (!response.ok) {
    throw new GithubPublicClientError(
      'TRANSIENT',
      `GitHub returned HTTP ${response.status} fetching README for ${repoUrl}`,
    )
  }

  const content = await response.text()

  return {
    content: content.length > maxChars ? content.slice(0, maxChars) : content,
    truncated: content.length > maxChars,
  }
}
