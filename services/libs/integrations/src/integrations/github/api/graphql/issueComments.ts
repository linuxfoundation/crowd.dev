import { Repo } from '../../types'
import BaseQuery from './baseQuery'

/* eslint class-methods-use-this: 0 */
class IssueCommentsQuery extends BaseQuery {
  repo: Repo

  constructor(repo: Repo, issueNumber: string, githubToken: string, perPage = 100) {
    const issueCommentsQuery = `{
      repository(name: "${repo.name}", owner: "${repo.owner}") {
        issue(number: ${issueNumber}) {
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
              issue {
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

    super(githubToken, issueCommentsQuery, 'issueComments', perPage)

    this.repo = repo
  }

  getEventData(result) {
    return {
      hasPreviousPage: result.repository?.issue?.comments?.pageInfo?.hasPreviousPage,
      startCursor: result.repository?.issue?.comments?.pageInfo?.startCursor,
      data: result.repository?.issue?.comments?.nodes,
    }
  }
}

export default IssueCommentsQuery
