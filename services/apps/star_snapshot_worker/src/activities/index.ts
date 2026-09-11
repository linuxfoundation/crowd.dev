import { ApplicationFailure } from '@temporalio/client'

import { upsertStarSnapshot } from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { svc } from '../main'

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'

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
  const match = url.match(/https?:\/\/github\.com\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/)
  if (!match) {
    throw new Error(`Cannot parse GitHub URL: ${url}`)
  }
  return { owner: match[1], name: match[2] }
}

export async function fetchAndSaveStarSnapshot(
  repoUrl: string,
  repositoryId: string,
  token: string,
): Promise<void> {
  const { owner, name } = parseGithubRepoUrl(repoUrl)

  const response = await fetch(GITHUB_GRAPHQL_URL, {
    method: 'POST',
    headers: {
      Authorization: `bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: STARGAZER_COUNT_QUERY, variables: { owner, name } }),
  })

  if (response.status === 401) {
    throw ApplicationFailure.nonRetryable(
      `GitHub auth failure (401) fetching stargazer count for ${repoUrl}`,
      'AUTH_ERROR',
    )
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

  const json = (await response.json()) as StargazerCountGraphqlResponse

  if (json.errors?.length) {
    throw new Error(
      `GraphQL error fetching stargazer count for ${repoUrl}: ${json.errors[0].message ?? 'unknown error'}`,
    )
  }

  const starCount = json.data?.repository?.stargazerCount
  if (starCount === undefined || starCount === null) {
    throw new Error(`No repository data returned for ${repoUrl}`)
  }

  const qx = pgpQx(svc.postgres.writer.connection())
  await upsertStarSnapshot(qx, repositoryId, starCount, new Date().toISOString())
}
