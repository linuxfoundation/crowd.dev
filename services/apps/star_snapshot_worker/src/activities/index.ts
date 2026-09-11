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
const MAX_ALIAS_ATTEMPTS = 3
const ALIAS_RETRY_DELAY_MS = 2_000

const NON_RETRYABLE_GRAPHQL_ERROR_TYPES = new Set(['NOT_FOUND', 'FORBIDDEN', 'INSUFFICIENT_SCOPES'])

interface GraphqlAliasError {
  type?: string
  message?: string
}

interface BatchGraphqlResponse {
  data?: Record<string, { stargazerCount: number } | null>
  errors?: Array<GraphqlAliasError & { path?: Array<string | number> }>
}

function isRetryableAliasError(error?: GraphqlAliasError): boolean {
  if (error?.message?.toLowerCase().includes('rate limit')) {
    return true
  }
  return !error?.type || !NON_RETRYABLE_GRAPHQL_ERROR_TYPES.has(error.type)
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

async function queryStargazerCounts(
  entries: Array<{ repo: IRepoForStarSnapshot; owner: string; name: string }>,
): Promise<BatchGraphqlResponse> {
  const { query, variables } = buildBatchQuery(entries)
  const token = await getGithubInstallationToken()

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

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

    return (await response.json()) as BatchGraphqlResponse
  } finally {
    clearTimeout(timeoutId)
  }
}

// Retries only the aliases that failed transiently, up to MAX_ALIAS_ATTEMPTS, so an
// activity-level rejection (which would discard every already-persisted result) is only
// ever raised on the first attempt, before anything in this batch has succeeded.
async function fetchStargazerCounts(
  entries: Array<{ repo: IRepoForStarSnapshot; owner: string; name: string }>,
): Promise<RepoStarFetchResult[]> {
  const results: RepoStarFetchResult[] = []
  let pending = entries

  for (let attempt = 1; attempt <= MAX_ALIAS_ATTEMPTS && pending.length > 0; attempt++) {
    let json: BatchGraphqlResponse
    try {
      json = await queryStargazerCounts(pending)
    } catch (error) {
      if (attempt === 1) {
        throw error
      }
      for (const entry of pending) {
        results.push({
          repositoryId: entry.repo.repositoryId,
          repoUrl: entry.repo.repoUrl,
          error: (error as Error).message,
        })
      }
      pending = []
      break
    }

    const topLevelError = json.errors?.find((error) => !error.path?.length)
    if (topLevelError) {
      const message = `GraphQL error fetching stargazer counts: ${topLevelError.message ?? 'unknown error'}`
      if (attempt === 1) {
        throw new Error(message)
      }
      for (const entry of pending) {
        results.push({
          repositoryId: entry.repo.repositoryId,
          repoUrl: entry.repo.repoUrl,
          error: message,
        })
      }
      pending = []
      break
    }

    const errorsByAlias = new Map<string, GraphqlAliasError>()
    for (const error of json.errors ?? []) {
      const alias = error.path?.[0]
      if (typeof alias === 'string') {
        errorsByAlias.set(alias, error)
      }
    }

    const stillPending: typeof pending = []

    pending.forEach((entry, i) => {
      const alias = `r${i}`
      const starCount = json.data?.[alias]?.stargazerCount

      if (starCount !== undefined && starCount !== null) {
        results.push({
          repositoryId: entry.repo.repositoryId,
          repoUrl: entry.repo.repoUrl,
          starCount,
        })
        return
      }

      const error = errorsByAlias.get(alias)
      const message = error?.message ?? 'No repository data returned'

      if (attempt < MAX_ALIAS_ATTEMPTS && isRetryableAliasError(error)) {
        stillPending.push(entry)
        return
      }

      results.push({
        repositoryId: entry.repo.repositoryId,
        repoUrl: entry.repo.repoUrl,
        error: message,
      })
    })

    pending = stillPending
    if (pending.length > 0) {
      await new Promise((resolve) => setTimeout(resolve, ALIAS_RETRY_DELAY_MS * attempt))
    }
  }

  return results
}

export async function fetchAndSaveStarSnapshotBatch(
  repos: IRepoForStarSnapshot[],
  capturedAt: string,
): Promise<RepoStarFetchResult[]> {
  const parsed: Array<{ repo: IRepoForStarSnapshot; owner: string; name: string }> = []
  const unparseableResults: RepoStarFetchResult[] = []

  for (const repo of repos) {
    try {
      const { owner, name } = parseGithubRepoUrl(repo.repoUrl)
      parsed.push({ repo, owner, name })
    } catch (error) {
      unparseableResults.push({
        repositoryId: repo.repositoryId,
        repoUrl: repo.repoUrl,
        error: (error as Error).message,
      })
    }
  }

  if (parsed.length === 0) {
    return unparseableResults
  }

  const results = await fetchStargazerCounts(parsed)

  const qx = pgpQx(svc.postgres.writer.connection())
  for (const result of results) {
    if (result.starCount !== undefined) {
      await upsertStarSnapshot(qx, result.repositoryId, result.starCount, capturedAt)
    }
  }

  return [...results, ...unparseableResults]
}

export async function findReposForStarSnapshot(
  limit?: number,
  afterUrl?: string,
): Promise<IRepoForStarSnapshot[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return findReposForStarSnapshotQx(qx, limit, afterUrl)
}
