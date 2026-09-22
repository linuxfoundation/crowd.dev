import { Repo } from '../../types'
import BaseQuery from './baseQuery'

/* eslint class-methods-use-this: 0 */
class PullRequestReviewThreadCommentsQuery extends BaseQuery {
  repo: Repo

  constructor(repo: Repo, reviewThreadId: string, githubToken: string, perPage = 50) {
    const pullRequestReviewThreadCommentsQuery = `{
      node(id: "${reviewThreadId}") {
        ... on PullRequestReviewThread {
          comments(first: ${perPage}, \${beforeCursor}) {
            pageInfo ${BaseQuery.PAGE_SELECT}
            nodes {
              author {
                ... on User ${BaseQuery.USER_SELECT}
              }
               authorBot: author {
                  ... on Bot ${BaseQuery.BOT_SELECT}
              }
              pullRequestReview {
                submittedAt
                author {
                  ... on User ${BaseQuery.USER_SELECT}
                }
                 authorBot: author {
                  ... on Bot ${BaseQuery.BOT_SELECT}
                }
              }
              bodyText
              url
              id
              createdAt
              pullRequest {
                url
                id
                title
                additions
                deletions
                changedFiles
                authorAssociation
                state
                repository{
                  url
                }
              }
            }
          }
        }
      }
    }`

    super(
      githubToken,
      pullRequestReviewThreadCommentsQuery,
      'pullRequestReviewThreadComments',
      perPage,
    )

    this.repo = repo
  }

  getEventData(result) {
    return {
      hasPreviousPage: result.node?.comments?.pageInfo?.hasPreviousPage,
      startCursor: result.node?.comments?.pageInfo?.startCursor,
      data: result.node?.comments?.nodes,
    }
  }
}

export default PullRequestReviewThreadCommentsQuery
