import { Repo } from '../../types'
import BaseQuery from './baseQuery'

/* eslint class-methods-use-this: 0 */
class DiscussionCommentsQuery extends BaseQuery {
  repo: Repo

  constructor(
    repo: Repo,
    discussionNumber: string,
    githubToken: string,
    perPage = 100,
    maxRepliesPerComment = 100,
  ) {
    const discussionCommentsQuery = `{
        repository(name: "${repo.name}", owner: "${repo.owner}") {
          discussion(number: ${discussionNumber}) {
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
                isAnswer
                replies(first: ${maxRepliesPerComment}) {
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
                  }
                }
                discussion {
                  url
                  id
                  title
                }
              }
              
            }
          }
        }
      }`

    super(githubToken, discussionCommentsQuery, 'discussionComments', perPage)

    this.repo = repo
  }

  getEventData(result) {
    return {
      hasPreviousPage: result.repository?.discussion?.comments?.pageInfo?.hasPreviousPage,
      startCursor: result.repository?.discussion?.comments?.pageInfo?.startCursor,
      data: result.repository?.discussion?.comments?.nodes,
    }
  }
}

export default DiscussionCommentsQuery
