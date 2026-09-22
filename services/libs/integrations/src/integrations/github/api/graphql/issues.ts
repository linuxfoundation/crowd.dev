import { Repo } from '../../types'
import BaseQuery from './baseQuery'

/* eslint class-methods-use-this: 0 */
class IssuesQuery extends BaseQuery {
  repo: Repo

  constructor(repo: Repo, githubToken: string, perPage = 100) {
    const issuesQuery = `{
        repository(owner: "${repo.owner}", name: "${repo.name}") {
            issues(last: ${perPage}, \${beforeCursor}) {
                pageInfo ${BaseQuery.PAGE_SELECT}
                nodes {
                    author {
                        ... on User ${BaseQuery.USER_SELECT}
                    }
                    authorBot: author {
                        ... on Bot ${BaseQuery.BOT_SELECT}
                    }
                    bodyText
                    state
                    id
                    title
                    url
                    createdAt
                    number
                    timelineItems(first: 100, itemTypes: [CLOSED_EVENT]) {
                      nodes {
                        ... on ClosedEvent {
                          __typename
                          id
                          actor {
                            ... on User ${BaseQuery.USER_SELECT}
                          }
                          actorBot: actor {
                            ... on Bot ${BaseQuery.BOT_SELECT}
                          }
                          createdAt
                        }
                      }
                    }
                }
            }
        }
    }`

    super(githubToken, issuesQuery, 'issues', perPage)

    this.repo = repo
  }

  getEventData(result) {
    return { ...super.getEventData(result), data: result.repository?.issues?.nodes }
  }
}

export default IssuesQuery
