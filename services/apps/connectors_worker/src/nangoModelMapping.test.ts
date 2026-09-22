import { describe, expect, it } from 'vitest'

import { getNangoModelForSync } from './nangoModelMapping'

describe('getNangoModelForSync', () => {
  it.each([
    ['discussions', 'GithubDiscussion'],
    ['forks', 'GithubFork'],
    ['issues', 'GithubIssue'],
    ['issue-comments', 'GithubIssueComment'],
    ['pull-requests', 'GithubPullRequest'],
    ['pull-request-comments', 'GithubPullRequestComment'],
    ['pull-request-review-comments', 'GithubPullRequestReviewThreadComment'],
    ['pull-request-commits', 'GithubPullRequestCommit'],
  ])('maps syncName %s to nango model %s', (syncName, expectedModel) => {
    expect(getNangoModelForSync(syncName)).toBe(expectedModel)
  })

  it('returns null for a syncName with no nango model counterpart', () => {
    expect(getNangoModelForSync('unknown-sync')).toBeNull()
  })
})
