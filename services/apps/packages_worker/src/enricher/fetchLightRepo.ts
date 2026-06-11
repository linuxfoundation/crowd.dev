import { getServiceChildLogger } from '@crowd/logging'

import { FetchError, LightRepoResult } from './types'

const log = getServiceChildLogger('fetch-light-repo')

const GITHUB_API_URL = 'https://api.github.com'

const REPO_QUERY = `
  query($owner: String!, $name: String!) {
    rateLimit { limit cost remaining resetAt }
    repository(owner: $owner, name: $name) {
      description
      primaryLanguage { name }
      repositoryTopics(first: 25) { nodes { topic { name } } }
      stargazerCount
      forkCount
      watchers { totalCount }
      issues(states: OPEN) { totalCount }
      pushedAt
      isArchived
      isDisabled
      isFork
      createdAt
      isSecurityPolicyEnabled
    }
  }
`

export function parseGithubUrl(url: string): { owner: string; name: string } {
  const match = url.match(/https?:\/\/github\.com\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/)
  if (!match) throw new FetchError('MALFORMED', `Cannot parse GitHub URL: ${url}`)
  return { owner: match[1], name: match[2] }
}

// community/profile API doesn't reliably return files.security — use Contents API instead.
async function fetchSecurityFileEnabled(
  url: string,
  owner: string,
  name: string,
  token: string,
  timeoutMs: number,
): Promise<boolean | null> {
  const headers = { Authorization: `bearer ${token}`, Accept: 'application/vnd.github+json' }
  const check = async (path: string): Promise<boolean> => {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const response = await fetch(`${GITHUB_API_URL}/repos/${owner}/${name}/contents/${path}`, {
        headers,
        signal: controller.signal,
      })
      if (response.status === 200) return true
      if (response.status === 404) return false
      if (response.status === 403) {
        const body = await response.text()
        if (body.toLowerCase().includes('rate limit')) {
          // REST secondary limits send retry-after; primary limits send x-ratelimit-reset
          const retryAfterSec = parseInt(response.headers.get('retry-after') ?? '0', 10)
          const resetSec = parseInt(response.headers.get('x-ratelimit-reset') ?? '0', 10)
          const resetMs = retryAfterSec
            ? Date.now() + retryAfterSec * 1000
            : resetSec
              ? resetSec * 1000 + 5_000
              : Date.now() + 65_000
          throw new FetchError('RATE_LIMIT', `Contents API rate limited on ${path}`, resetMs)
        }
      }
      throw new Error(`Unexpected status ${response.status} for ${path}`)
    } finally {
      clearTimeout(timeoutId)
    }
  }

  try {
    const [root, dotGithub] = await Promise.all([
      check('SECURITY.md'),
      check('.github/SECURITY.md'),
    ])
    return root || dotGithub
  } catch (err) {
    // Rate limits propagate so the caller can park the installation and requeue the repo
    if (err instanceof FetchError && err.kind === 'RATE_LIMIT') throw err
    log.warn(
      {
        url,
        errName: (err as Error).name,
        errMsg: (err as Error).message,
        errStack: (err as Error).stack,
      },
      'Security file check failed — securityFileEnabled will be null',
    )
    return null
  }
}

interface RepoGraphqlResponse {
  data?: {
    rateLimit: { limit: number; cost: number; remaining: number; resetAt: string }
    repository: {
      description: string | null
      primaryLanguage: { name: string } | null
      repositoryTopics: { nodes: Array<{ topic: { name: string } }> }
      stargazerCount: number
      forkCount: number
      watchers: { totalCount: number }
      issues: { totalCount: number }
      pushedAt: string | null
      isArchived: boolean
      isDisabled: boolean
      isFork: boolean
      createdAt: string
      isSecurityPolicyEnabled: boolean
    } | null
  }
  errors?: Array<{ type?: string; message?: string }>
}

export async function fetchLightRepo(
  url: string,
  token: string,
  timeoutMs: number,
): Promise<LightRepoResult> {
  const { owner, name } = parseGithubUrl(url)

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

  let response: Response
  try {
    response = await fetch(`${GITHUB_API_URL}/graphql`, {
      method: 'POST',
      headers: {
        Authorization: `bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: REPO_QUERY, variables: { owner, name } }),
      signal: controller.signal,
    })
  } catch (err) {
    throw new FetchError('TRANSIENT', `Network error for ${url}: ${(err as Error).message}`)
  } finally {
    clearTimeout(timeoutId)
  }

  const resetSec = parseInt(response.headers.get('x-ratelimit-reset') ?? '0', 10)
  const resetMs = resetSec ? resetSec * 1000 + 5_000 : Date.now() + 65_000

  // 401 is requester/platform-side (bad token, GitHub auth incident) — never a repo signal
  if (response.status === 401) throw new FetchError('TRANSIENT', `401 Unauthorized for ${url}`)

  if (response.status === 403) {
    const body = await response.text()
    if (body.toLowerCase().includes('rate limit'))
      throw new FetchError('RATE_LIMIT', `Rate limited on ${url}`, resetMs)
    throw new FetchError('AUTH', `403 Forbidden for ${url}`)
  }

  if (response.status === 404) throw new FetchError('NOT_FOUND', `404 for ${url}`)
  if (response.status >= 500) throw new FetchError('TRANSIENT', `${response.status} for ${url}`)

  const [json, securityFileEnabled] = await Promise.all([
    response.json() as Promise<RepoGraphqlResponse>,
    fetchSecurityFileEnabled(url, owner, name, token, timeoutMs),
  ])

  if (json.errors?.length) {
    const err = json.errors[0]
    if (err.type === 'RATE_LIMITED' || err.message?.toLowerCase().includes('rate limit'))
      throw new FetchError('RATE_LIMIT', `RATE_LIMITED for ${url}`, resetMs)
    if (err.type === 'NOT_FOUND') throw new FetchError('NOT_FOUND', `NOT_FOUND for ${url}`)
    if (err.message?.toLowerCase().includes('ip allow list'))
      throw new FetchError('AUTH', `IP allowlist blocks access to ${url}`)
    throw new FetchError('TRANSIENT', `GraphQL error for ${url}: ${err.message ?? err.type}`)
  }

  const repo = json.data?.repository
  if (!repo) throw new FetchError('NOT_FOUND', `No repository data for ${url}`)

  return {
    url,
    host: 'github',
    owner,
    name,
    description: repo.description ?? null,
    primaryLanguage: repo.primaryLanguage?.name ?? null,
    topics: (repo.repositoryTopics?.nodes ?? []).map(
      (n: { topic: { name: string } }) => n.topic.name,
    ),
    stars: repo.stargazerCount ?? null,
    forks: repo.forkCount ?? null,
    watchers: repo.watchers?.totalCount ?? null,
    openIssues: repo.issues?.totalCount ?? null,
    lastCommitAt: repo.pushedAt ?? null,
    archived: repo.isArchived ?? null,
    disabled: repo.isDisabled ?? null,
    isFork: repo.isFork ?? null,
    createdAt: repo.createdAt ?? null,
    securityPolicyEnabled: repo.isSecurityPolicyEnabled ?? null,
    securityFileEnabled,
    rateLimit: json.data?.rateLimit ?? null,
  }
}
