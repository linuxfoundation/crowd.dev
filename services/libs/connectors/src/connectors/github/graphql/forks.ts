import type { GithubUserNode } from '../mappers/member'

import { ORGANIZATION_FIELDS, USER_FIELDS } from './fields'

export interface ForkNode {
  id: string
  name: string
  createdAt: string
  updatedAt: string
  url: string
  isFork: boolean
  isInOrganization: boolean
  parent: {
    nameWithOwner: string
    isFork: boolean
  } | null
  owner: GithubUserNode
}

export interface ForksPage {
  repository: {
    forks: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      nodes: (ForkNode | null)[]
    }
  }
}

export const FORKS_QUERY = `
  query ($owner: String!, $repo: String!, $first: Int!, $cursor: String) {
    repository(owner: $owner, name: $repo) {
      forks(first: $first, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          id
          name
          createdAt
          updatedAt
          url
          isFork
          isInOrganization
          parent {
            nameWithOwner
            isFork
          }
          owner {
            login
            ... on User {
              ${USER_FIELDS}
            }
            ... on Organization {
              ${ORGANIZATION_FIELDS}
            }
          }
        }
      }
    }
  }
`
