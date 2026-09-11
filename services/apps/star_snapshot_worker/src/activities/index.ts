import { ApplicationFailure } from '@temporalio/client'

import { getGithubInstallationToken } from '@crowd/common_services'
import {
  findReposForStarSnapshot as findReposForStarSnapshotQx,
  upsertStarSnapshot,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { IRepoForStarSnapshot } from '@crowd/types'

import { svc } from '../main'

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'
const FETCH_TIMEOUT_MS = 30_000

const NON_RETRYABLE_GRAPHQL_ERROR_TYPES = new Set(['NOT_FOUND', 'FORBIDDEN', 'INSUFFICIENT_SCOPES'])

interface BatchGraphqlResponse {
  data?: Record<string, { stargazerCount: number } | null>
  errors?: Array<{ type?: string; message?: string; path?: Array<string | number> }>
}

export interface RepoStarFetchResult {
  repositoryId: string
  repoUrl: string
  starCount?: number
  error?: string
}

function buildBatchQuery(repos: Array<{ owner: string; name: string }>): {
  query: string
  variables: Record<string, string>
} {
  const variables: Record<string, string> = {}
  const varDefs: string[] = []
  const fields: string[] = []

  repos.forEach((repo, i) => {
    variables[`owner${i}`] = repo.owner
    variables[`name${i}`] = repo.name
    varDefs.push(`$owner${i}: String!, $name${i}: String!`)
    fields.push(`r${i}: repository(owner: $owner${i}, name: $name${i}) { stargazerCount }`)
  })

  return {
    query: `query(${varDefs.join(', ')}) { ${fields.join(' ')} }`,
    variables,
  }
}

export function parseGithubRepoUrl(url: string): { owner: string; name: string } {
  let parsed: URL
  try {
    parsed = new URL(url.replace('git@github.com:', 'https://github.com/'))
  } catch {
    throw ApplicationFailure.nonRetryable(`Cannot parse GitHub URL: ${url}`, 'INVALID_URL')
  }

  const pathParts = parsed.pathname
    .replace(/^\//, '')
    .replace(/\/$/, '')
    .replace(/\.git$/, '')
    .split('/')

  if (
    parsed.hostname !== 'github.com' ||
    pathParts.length !== 2 ||
    !pathParts[0] ||
    !pathParts[1]
  ) {
    throw ApplicationFailure.nonRetryable(`Cannot parse GitHub URL: ${url}`, 'INVALID_URL')
  }

  return { owner: pathParts[0], name: pathParts[1] }
}

export async function fetchAndSaveStarSnapshotBatch(
  repos: IRepoForStarSnapshot[],
  capturedAt: string,
): Promise<RepoStarFetchResult[]> {
  const parsed = repos.map((repo) => ({ repo, ...parseGithubRepoUrl(repo.repoUrl) }))
  const { query, variables } = buildBatchQuery(parsed)

  const token = await getGithubInstallationToken()

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  let json: BatchGraphqlResponse
  try {
    const response = await fetch(GITHUB_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        Authorization: `bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query, variables }),
      signal: controller.signal,
    })

    if (response.status === 401) {
      throw new Error('GitHub auth failure (401) fetching stargazer counts')
    }

    if (response.status === 403) {
      const body = await response.text()
      if (body.toLowerCase().includes('rate limit')) {
        throw new Error('GitHub rate limit hit fetching stargazer counts')
      }
      throw ApplicationFailure.nonRetryable(
        'GitHub auth failure (403) fetching stargazer counts',
        'AUTH_ERROR',
      )
    }

    if (!response.ok) {
      throw new Error(`GitHub API error ${response.status} fetching stargazer counts`)
    }

    json = (await response.json()) as BatchGraphqlResponse
  } finally {
    clearTimeout(timeoutId)
  }

  const topLevelError = json.errors?.find((error) => !error.path?.length)
  if (topLevelError) {
    const message = `GraphQL error fetching stargazer counts: ${topLevelError.message ?? 'unknown error'}`
    if (topLevelError.message?.toLowerCase().includes('rate limit')) {
      throw new Error(message)
    }
    throw new Error(message)
  }

  const errorsByAlias = new Map<string, { type?: string; message?: string }>()
  for (const error of json.errors ?? []) {
    const alias = error.path?.[0]
    if (typeof alias === 'string') {
      errorsByAlias.set(alias, error)
    }
  }

  const results: RepoStarFetchResult[] = parsed.map((entry, i) => {
    const alias = `r${i}`
    const starCount = json.data?.[alias]?.stargazerCount

    if (starCount !== undefined && starCount !== null) {
      return { repositoryId: entry.repo.repositoryId, repoUrl: entry.repo.repoUrl, starCount }
    }

    const error = errorsByAlias.get(alias)
    const errorType = error?.type
    const message = error?.message ?? 'No repository data returned'
    if (errorType && !NON_RETRYABLE_GRAPHQL_ERROR_TYPES.has(errorType)) {
      throw new Error(
        `GraphQL error fetching stargazer count for ${entry.repo.repoUrl}: ${message}`,
      )
    }

    return { repositoryId: entry.repo.repositoryId, repoUrl: entry.repo.repoUrl, error: message }
  })

  const qx = pgpQx(svc.postgres.writer.connection())
  for (const result of results) {
    if (result.starCount !== undefined) {
      await upsertStarSnapshot(qx, result.repositoryId, result.starCount, capturedAt)
    }
  }

  return results
}

export async function findReposForStarSnapshot(limit?: number): Promise<IRepoForStarSnapshot[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return findReposForStarSnapshotQx(qx, limit)
}
