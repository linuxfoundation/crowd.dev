import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../http/client'

export interface IRequestLimits {
  timeoutMs: number
  maxAttempts: number
}

export async function fetchPullRequestsForCommit(
  http: ConnectorHttp,
  owner: string,
  repo: string,
  sha: string,
  log: Logger,
  limits?: IRequestLimits,
): Promise<unknown[]> {
  return http.request<unknown[]>(
    {
      method: 'get',
      url: `https://api.github.com/repos/${owner}/${repo}/commits/${sha}/pulls`,
      headers: { Accept: 'application/vnd.github+json' },
      ...(limits ? { timeout: limits.timeoutMs } : {}),
    },
    log,
    limits?.maxAttempts,
  )
}
