import type { GithubUserNode } from '../mappers/member'

import { USER_FIELDS } from './fields'

export interface StargazerEdge {
  starredAt: string
  node: GithubUserNode | null
}

export interface StargazersPage {
  repository: {
    stargazers: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      edges: (StargazerEdge | null)[]
    }
  }
}

export const STARGAZERS_QUERY = `
  query ($owner: String!, $repo: String!, $first: Int!, $cursor: String) {
    repository(owner: $owner, name: $repo) {
      stargazers(first: $first, after: $cursor, orderBy: {field: STARRED_AT, direction: ASC}) {
        pageInfo {
          endCursor
          hasNextPage
        }
        edges {
          starredAt
          node {
            login
            ${USER_FIELDS}
          }
        }
      }
    }
  }
`
