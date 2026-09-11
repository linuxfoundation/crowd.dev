import type { GithubUserNode } from '../mappers/member'

import { BOT_FIELDS, ORGANIZATION_FIELDS, USER_FIELDS } from './fields'

export interface PrCommentNode {
  id: string
  body: string
  createdAt: string
  url: string
  author: GithubUserNode | null
}

export interface PrCommentsConnection {
  pageInfo: {
    endCursor: string | null
    hasNextPage: boolean
  }
  edges: ({ node: PrCommentNode | null } | null)[]
}

export interface PrCommentsBatchPage {
  nodes: ({
    id: string
    number: number
    comments: PrCommentsConnection
  } | null)[]
}

export interface ReviewThreadNode {
  id: string
  isResolved: boolean
}

export interface ReviewThreadsBatchPage {
  nodes: ({
    id: string
    number: number
    reviewThreads: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      edges: ({ node: ReviewThreadNode | null } | null)[]
    }
  } | null)[]
}

export interface ThreadCommentNode {
  id: string
  body: string
  createdAt: string
  url: string
  author: GithubUserNode | null
}

export interface ThreadCommentsBatchPage {
  nodes: ({
    id: string
    isResolved: boolean
    comments: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      edges: ({ node: ThreadCommentNode | null } | null)[]
    }
  } | null)[]
}

export interface PrCommitNode {
  commit: {
    additions: number
    deletions: number
    parents: { totalCount: number }
    id: string
    oid: string
    message: string
    authoredDate: string
    url: string
    author: {
      user: GithubUserNode | null
      email: string | null
      name: string | null
    } | null
  }
}

export interface PrCommitsPage {
  repository: {
    pullRequest: {
      commits: {
        pageInfo: {
          endCursor: string | null
          hasNextPage: boolean
        }
        nodes: (PrCommitNode | null)[]
      }
    } | null
  }
}

export const COMMENTS_FOR_PRS_QUERY = `
  query ($ids: [ID!]!, $first: Int!, $after: String) {
    nodes(ids: $ids) {
      ... on PullRequest {
        id
        number
        comments(first: $first, after: $after) {
          pageInfo {
            endCursor
            hasNextPage
          }
          edges {
            node {
              id
              body
              createdAt
              url
              author {
                __typename
                login
                ... on User {
                  ${USER_FIELDS}
                }
                ... on Bot {
                  ${BOT_FIELDS}
                }
                ... on Organization {
                  ${ORGANIZATION_FIELDS}
                }
              }
            }
          }
        }
      }
    }
  }
`

export const REVIEW_THREADS_FOR_PRS_QUERY = `
  query ($ids: [ID!]!, $first: Int!, $after: String) {
    nodes(ids: $ids) {
      ... on PullRequest {
        id
        number
        reviewThreads(first: $first, after: $after) {
          pageInfo {
            endCursor
            hasNextPage
          }
          edges {
            node {
              id
              isResolved
            }
          }
        }
      }
    }
  }
`

export const COMMENTS_FOR_THREADS_QUERY = `
  query ($ids: [ID!]!, $first: Int!, $after: String) {
    nodes(ids: $ids) {
      ... on PullRequestReviewThread {
        id
        isResolved
        comments(first: $first, after: $after) {
          pageInfo {
            endCursor
            hasNextPage
          }
          edges {
            node {
              id
              body
              createdAt
              url
              author {
                __typename
                login
                ... on User {
                  ${USER_FIELDS}
                }
                ... on Bot {
                  ${BOT_FIELDS}
                }
                ... on Organization {
                  ${ORGANIZATION_FIELDS}
                }
              }
            }
          }
        }
      }
    }
  }
`

export const PR_COMMITS_QUERY = `
  query ($owner: String!, $repo: String!, $prNumber: Int!, $first: Int!, $cursor: String) {
    repository(name: $repo, owner: $owner) {
      pullRequest(number: $prNumber) {
        commits(first: $first, after: $cursor) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            commit {
              additions
              deletions
              parents {
                totalCount
              }
              id
              oid
              message
              authoredDate
              url
              author {
                user {
                  ... on User {
                    ${USER_FIELDS}
                  }
                }
                email
                name
              }
            }
          }
        }
      }
    }
  }
`
