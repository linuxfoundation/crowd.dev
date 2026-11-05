import { NANGO_INTEGRATION_CONFIG, NangoIntegration } from './types'

export interface IGithubSyncMapping {
  connectorSync: string
  nangoSync: string
  model: string
}

const GITHUB = NANGO_INTEGRATION_CONFIG[NangoIntegration.GITHUB]

export const GITHUB_SYNC_MAPPINGS: readonly IGithubSyncMapping[] = [
  {
    connectorSync: 'discussions',
    nangoSync: GITHUB.syncs.DISCUSSIONS,
    model: GITHUB.models.DISCUSSION,
  },
  { connectorSync: 'forks', nangoSync: GITHUB.syncs.FORKS, model: GITHUB.models.FORK },
  { connectorSync: 'issues', nangoSync: GITHUB.syncs.ISSUES, model: GITHUB.models.ISSUE },
  {
    connectorSync: 'issue-comments',
    nangoSync: GITHUB.syncs.ISSUE_COMMENTS,
    model: GITHUB.models.ISSUE_COMMENT,
  },
  {
    connectorSync: 'pull-requests',
    nangoSync: GITHUB.syncs.PULL_REQUESTS,
    model: GITHUB.models.PULL_REQUEST,
  },
  {
    connectorSync: 'pull-request-comments',
    nangoSync: GITHUB.syncs.PULL_REQUEST_COMMENTS,
    model: GITHUB.models.PULL_REQUEST_COMMENT,
  },
  {
    connectorSync: 'pull-request-review-comments',
    nangoSync: GITHUB.syncs.PULL_REQUEST_REVIEWS,
    model: GITHUB.models.PULL_REQUEST_REVIEW_THREAD_COMMENT,
  },
  {
    connectorSync: 'pull-request-commits',
    nangoSync: GITHUB.syncs.PULL_REQUEST_COMMITS,
    model: GITHUB.models.PULL_REQUEST_COMMIT,
  },
]

export function getGithubSyncForNangoModel(model: string): IGithubSyncMapping | null {
  return GITHUB_SYNC_MAPPINGS.find((mapping) => mapping.model === model) ?? null
}

export function getGithubSyncForConnectorSync(connectorSync: string): IGithubSyncMapping | null {
  return GITHUB_SYNC_MAPPINGS.find((mapping) => mapping.connectorSync === connectorSync) ?? null
}

export function githubChannelName(owner: string, repo: string): string {
  return `https://github.com/${owner}/${repo}`
}
