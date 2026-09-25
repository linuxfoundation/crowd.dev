import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../http/client'

export async function fetchPullRequestsForCommit(
  http: ConnectorHttp,
  owner: string,
  repo: string,
  sha: string,
  log: Logger,
): Promise<unknown[]> {
  return http.request<unknown[]>(
    {
      method: 'get',
      url: `https://api.github.com/repos/${owner}/${repo}/commits/${sha}/pulls`,
      headers: { Accept: 'application/vnd.github+json' },
    },
    log,
  )
}
