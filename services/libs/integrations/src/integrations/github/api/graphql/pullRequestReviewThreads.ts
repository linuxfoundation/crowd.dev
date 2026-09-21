import { Repo } from '../../types'
import BaseQuery from './baseQuery'

/* eslint class-methods-use-this: 0 */
class PullRequestReviewThreadsQuery extends BaseQuery {
  repo: Repo

  constructor(repo: Repo, pullRequestNumber: string, githubToken: string, perPage = 100) {
    const pullRequestReviewThreadsQuery = `{
        repository(name: "${repo.name}", owner: "${repo.owner}") {
          pullRequest(number: ${pullRequestNumber}) {
            id
            reviewDecision
            reviewThreads(first: ${perPage}, \${beforeCursor}) {
              pageInfo ${BaseQuery.PAGE_SELECT}
              nodes {
                id
              }
            }
          }
        }
      }`

    super(githubToken, pullRequestReviewThreadsQuery, 'pullRequestReviewThreads', perPage)

    this.repo = repo
  }

  getEventData(result) {
    return {
      hasPreviousPage: result.repository?.pullRequest?.reviewThreads?.pageInfo?.hasPreviousPage,
      startCursor: result.repository?.pullRequest?.reviewThreads?.pageInfo?.startCursor,
      data: result.repository?.pullRequest?.reviewThreads?.nodes,
    }
  }
}

export default PullRequestReviewThreadsQuery
