import { FetchError, LightRepoResult } from './types'

const GRAPHQL_URL = 'https://api.github.com/graphql'

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
    }
  }
`

export function parseGithubUrl(url: string): { owner: string; name: string } {
  const match = url.match(/https?:\/\/github\.com\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/)
  if (!match) throw new FetchError('MALFORMED', `Cannot parse GitHub URL: ${url}`)
  return { owner: match[1], name: match[2] }
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
    response = await fetch(GRAPHQL_URL, {
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

  if (response.status === 401) throw new FetchError('AUTH', `401 Unauthorized for ${url}`)

  if (response.status === 403) {
    const body = await response.text()
    if (body.toLowerCase().includes('rate limit'))
      throw new FetchError('RATE_LIMIT', `Rate limited on ${url}`, resetMs)
    throw new FetchError('AUTH', `403 Forbidden for ${url}`)
  }

  if (response.status === 404) throw new FetchError('NOT_FOUND', `404 for ${url}`)
  if (response.status >= 500) throw new FetchError('TRANSIENT', `${response.status} for ${url}`)

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const json = (await response.json()) as any

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
    rateLimit: json.data?.rateLimit ?? null,
  }
}
