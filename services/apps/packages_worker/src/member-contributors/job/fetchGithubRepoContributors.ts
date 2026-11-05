import { ApplicationFailure } from '@temporalio/activity'

import { getGithubAppConfig } from '../../config'
import { githubApiGet } from '../../security-contacts/githubToken'
import { RepoContributorsFetch, fetchRepoContributors } from '../github/fetchRepoContributors'
import { toGithubContributorLogins } from '../github/mapContributors'
import { HEARTBEAT_INTERVAL_MS, heartbeatSafely } from '../github/syncGithubContributorsBatch'

const FETCH_TIMEOUT_MS = 15_000

export interface FetchGithubRepoContributorsInput {
  repoUrl: string
}

export interface GithubRepoContributorsFetch {
  status: 'fetched' | 'empty'
  logins: string[]
}

async function fetchWithHeartbeat(repoUrl: string): Promise<RepoContributorsFetch> {
  const heartbeatTimer = setInterval(() => heartbeatSafely(), HEARTBEAT_INTERVAL_MS)
  try {
    return await fetchRepoContributors(repoUrl, (path) =>
      githubApiGet(path, FETCH_TIMEOUT_MS, { extraOkStatuses: [204] }),
    )
  } finally {
    clearInterval(heartbeatTimer)
  }
}

export async function fetchGithubRepoContributors(
  input: FetchGithubRepoContributorsInput,
): Promise<GithubRepoContributorsFetch> {
  getGithubAppConfig()
  const fetched = await fetchWithHeartbeat(input.repoUrl)
  if (fetched.status === 'missing') {
    throw ApplicationFailure.nonRetryable('Repository not found on GitHub')
  }
  if (fetched.status === 'error') {
    throw new Error(`GitHub contributors fetch failed for ${input.repoUrl}`)
  }
  const logins = toGithubContributorLogins(fetched.contributors)
  return { status: logins.length === 0 ? 'empty' : 'fetched', logins }
}
