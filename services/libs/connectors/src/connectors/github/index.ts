import type { Manifest } from '../../types'

import { createGithubTokenMinter, prepareGithubPool } from './appToken'
import { probeGithubBudget } from './budget'
import { discoverRepos } from './discover'
import { interpretGithubResponse } from './interpret'
import { discussionsSync } from './syncs/discussions'
import { forksSync } from './syncs/forks'
import { issueCommentsSync } from './syncs/issueComments'
import { issuesSync } from './syncs/issues'
import { pullRequestCommentsSync } from './syncs/pullRequestComments'
import { pullRequestCommitsSync } from './syncs/pullRequestCommits'
import { pullRequestReviewCommentsSync } from './syncs/pullRequestReviewComments'
import { pullRequestsSync } from './syncs/pullRequests'
import { starsSync } from './syncs/stars'

export const githubConnector: Manifest = {
  platform: 'github',
  syncs: [
    discussionsSync,
    forksSync,
    issuesSync,
    issueCommentsSync,
    pullRequestsSync,
    pullRequestCommentsSync,
    pullRequestReviewCommentsSync,
    pullRequestCommitsSync,
    starsSync,
  ],
  discover: discoverRepos,
  preparePool: prepareGithubPool,
  mintToken: createGithubTokenMinter,
  probeBudget: probeGithubBudget,
  interpretResponse: interpretGithubResponse,
}
