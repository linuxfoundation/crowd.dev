import { Repo } from '../../types'
import BaseQuery from './baseQuery'

export interface PullRequestCommit {
  repository: {
    pullRequest: {
      id: string
      number: number
      baseRefName: string
      headRefName: string
      commits: {
        pageInfo: {
          hasPreviousPage: boolean
          startCursor: string
        }
        nodes: {
          commit: {
            authoredDate: string
            committedDate: string
            additions: number
            changedFilesIfAvailable: number
            deletions: number
            oid: string
            message: string
            url: string
            parents: {
              totalCount: number
            }
            authors: {
              nodes: {
                user: {
                  login: string
                  name: string
                  avatarUrl: string
                  id: string
                  isHireable: boolean
                  twitterUsername: string | null
                  url: string
                  websiteUrl: string | null
                  email: string
                  bio: string
                  company: string
                  location: string | null
                  followers: {
                    totalCount: number
                  }
                }
              }[]
            }
          }
        }[]
      }
    }
  }
}

/* eslint class-methods-use-this: 0 */
class PullRequestCommitsQuery extends BaseQuery {
  repo: Repo

  constructor(
    repo: Repo,
    pullRequestNumber: string,
    githubToken: string,
    perPage = 100,
    maxAuthors = 1,
  ) {
    const pullRequestCommitsQuery = `{
      repository(name: "${repo.name}", owner: "${repo.owner}") {
        pullRequest(number: ${pullRequestNumber}) {
          id
          number
          baseRefName
          headRefName
          commits(first: ${perPage}, \${beforeCursor}) {
            pageInfo ${BaseQuery.PAGE_SELECT}
            nodes {
              commit {
                authoredDate
                committedDate
                additions
                changedFilesIfAvailable
                deletions
                oid
                message
                url
                parents(first: 2) {
                	totalCount
                }
                authors(first: ${maxAuthors}) {
                  nodes {
                    user ${BaseQuery.USER_SELECT}
                  }
                }
              }
            }
          }
        }
      }
    }`

    super(githubToken, pullRequestCommitsQuery, 'pullRequestCommits', perPage)

    this.repo = repo
  }

  // Override the getEventData method to process commit details
  getEventData(result) {
    const commitData = result as PullRequestCommit

    return {
      hasPreviousPage: result.repository?.pullRequest?.commits?.pageInfo?.hasPreviousPage,
      startCursor: result.repository?.pullRequest?.commits?.pageInfo?.startCursor,
      data: [commitData], // returning an array to match the parseActivities function
    }
  }
}

export default PullRequestCommitsQuery
