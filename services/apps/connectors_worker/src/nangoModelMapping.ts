const SYNC_NAME_TO_NANGO_MODEL: Record<string, string> = {
  discussions: 'GithubDiscussion',
  forks: 'GithubFork',
  issues: 'GithubIssue',
  'issue-comments': 'GithubIssueComment',
  'pull-requests': 'GithubPullRequest',
  'pull-request-comments': 'GithubPullRequestComment',
  'pull-request-review-comments': 'GithubPullRequestReviewThreadComment',
  'pull-request-commits': 'GithubPullRequestCommit',
}

export function getNangoModelForSync(syncName: string): string | null {
  return SYNC_NAME_TO_NANGO_MODEL[syncName] ?? null
}
