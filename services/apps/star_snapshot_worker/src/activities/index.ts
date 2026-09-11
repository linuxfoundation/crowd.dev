import { ApplicationFailure } from '@temporalio/client'

import {
  findReposForStarSnapshot as findReposForStarSnapshotQx,
  upsertStarSnapshot,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { NangoIntegration, getNangoConnectionData, initNangoCloudClient } from '@crowd/nango'
import { IRepoForStarSnapshot } from '@crowd/types'

import { svc } from '../main'

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'
const FETCH_TIMEOUT_MS = 30_000

const NON_RETRYABLE_GRAPHQL_ERROR_TYPES = new Set(['NOT_FOUND', 'FORBIDDEN', 'INSUFFICIENT_SCOPES'])

const STARGAZER_COUNT_QUERY = `
  query($owner: String!, $name: String!) {
    repository(owner: $owner, name: $name) {
      stargazerCount
    }
  }
`

interface StargazerCountGraphqlResponse {
  data?: {
    repository: { stargazerCount: number } | null
  }
  errors?: Array<{ type?: string; message?: string }>
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

export async function fetchAndSaveStarSnapshot(
  repoUrl: string,
  repositoryId: string,
  token: string,
  capturedAt: string,
): Promise<void> {
  const { owner, name } = parseGithubRepoUrl(repoUrl)

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

  let json: StargazerCountGraphqlResponse
  try {
    const response = await fetch(GITHUB_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        Authorization: `bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: STARGAZER_COUNT_QUERY, variables: { owner, name } }),
      signal: controller.signal,
    })

    if (response.status === 401) {
      throw new Error(`GitHub auth failure (401) fetching stargazer count for ${repoUrl}`)
    }

    if (response.status === 403) {
      const body = await response.text()
      if (body.toLowerCase().includes('rate limit')) {
        throw new Error(`GitHub rate limit hit fetching stargazer count for ${repoUrl}`)
      }
      throw ApplicationFailure.nonRetryable(
        `GitHub auth failure (403) fetching stargazer count for ${repoUrl}`,
        'AUTH_ERROR',
      )
    }

    if (!response.ok) {
      throw new Error(`GitHub API error ${response.status} fetching stargazer count for ${repoUrl}`)
    }

    json = (await response.json()) as StargazerCountGraphqlResponse
  } finally {
    clearTimeout(timeoutId)
  }

  if (json.errors?.length) {
    const [error] = json.errors
    const message = `GraphQL error fetching stargazer count for ${repoUrl}: ${error.message ?? 'unknown error'}`
    if (error.message?.toLowerCase().includes('rate limit')) {
      throw new Error(message)
    }
    if (error.type && NON_RETRYABLE_GRAPHQL_ERROR_TYPES.has(error.type)) {
      throw ApplicationFailure.nonRetryable(message, error.type)
    }
    throw new Error(message)
  }

  const starCount = json.data?.repository?.stargazerCount
  if (starCount === undefined || starCount === null) {
    throw ApplicationFailure.nonRetryable(`No repository data returned for ${repoUrl}`, 'NOT_FOUND')
  }

  const qx = pgpQx(svc.postgres.writer.connection())
  await upsertStarSnapshot(qx, repositoryId, starCount, capturedAt)
}

export async function findReposForStarSnapshot(limit?: number): Promise<IRepoForStarSnapshot[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return findReposForStarSnapshotQx(qx, limit)
}

export async function getGithubTokenForConnection(connectionId: string): Promise<string> {
  await initNangoCloudClient()

  const connection = await getNangoConnectionData(NangoIntegration.GITHUB, connectionId)

  if (connection.credentials.type !== 'APP') {
    throw ApplicationFailure.nonRetryable(
      `Unexpected Nango credential type '${connection.credentials.type}' for connection ${connectionId}`,
      'UNEXPECTED_CREDENTIAL_TYPE',
    )
  }

  return connection.credentials.access_token
}
