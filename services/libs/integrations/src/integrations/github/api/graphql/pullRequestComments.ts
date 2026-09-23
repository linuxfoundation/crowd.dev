/* eslint-disable  @typescript-eslint/no-explicit-any */
import { Repo } from '../../types'
import BaseQuery from './baseQuery'

/* eslint class-methods-use-this: 0 */
class PullRequestCommentsQuery extends BaseQuery {
  repo: Repo

  constructor(repo: Repo, pullRequestNumber: string, githubToken: string, perPage = 100) {
    const pullRequestCommentsQuery = `{
      repository(name: "${repo.name}", owner: "${repo.owner}") {
        pullRequest(number: ${pullRequestNumber}) {
          comments(first: ${perPage}, \${beforeCursor}) {
            pageInfo ${BaseQuery.PAGE_SELECT}
            nodes {
              author {
                ... on User ${BaseQuery.USER_SELECT}
              }
              authorBot: author {
                ... on Bot ${BaseQuery.BOT_SELECT}
              }
              bodyText
              url
              id
              createdAt
              pullRequest {
                  url
                  id
                  title
              }
              repository {
                  url
              }
            }
          }
        }
      }
    }`

    super(githubToken, pullRequestCommentsQuery, 'pullRequestComments', perPage)

    this.repo = repo
  }

  getEventData(result) {
    return {
      hasPreviousPage: result.repository?.pullRequest?.comments?.pageInfo?.hasPreviousPage,
      startCursor: result.repository?.pullRequest?.comments?.pageInfo?.startCursor,
      data: result.repository?.pullRequest?.comments?.nodes,
    }
  }
}

export default PullRequestCommentsQuery
