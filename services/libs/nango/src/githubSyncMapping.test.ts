import { describe, expect, it } from 'vitest'

import {
  GITHUB_SYNC_MAPPINGS,
  getGithubSyncForConnectorSync,
  getGithubSyncForNangoModel,
} from './githubSyncMapping'
import { NANGO_INTEGRATION_CONFIG, NangoIntegration } from './types'

const GITHUB = NANGO_INTEGRATION_CONFIG[NangoIntegration.GITHUB]

describe('github sync mappings', () => {
  it('cover every nango sync and every model except stars exactly once', () => {
    expect(GITHUB_SYNC_MAPPINGS.map((m) => m.nangoSync).sort()).toEqual(
      Object.values(GITHUB.syncs).sort(),
    )
    expect(GITHUB_SYNC_MAPPINGS.map((m) => m.model).sort()).toEqual(
      Object.values(GITHUB.models)
        .filter((model) => model !== GITHUB.models.STAR)
        .sort(),
    )
    expect(new Set(GITHUB_SYNC_MAPPINGS.map((m) => m.connectorSync)).size).toBe(
      GITHUB_SYNC_MAPPINGS.length,
    )
  })

  it('resolve in both directions, with the review-comments name differing per side', () => {
    expect(getGithubSyncForConnectorSync('pull-request-review-comments')).toMatchObject({
      nangoSync: 'pull-request-reviews',
      model: 'GithubPullRequestReviewThreadComment',
    })
    expect(getGithubSyncForNangoModel('GithubIssue')?.connectorSync).toBe('issues')
    expect(getGithubSyncForNangoModel(GITHUB.models.STAR)).toBeNull()
    expect(getGithubSyncForConnectorSync('stars')).toBeNull()
  })
})
