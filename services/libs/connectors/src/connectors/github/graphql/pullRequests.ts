import type { GithubUserNode } from '../mappers/member'

import { BOT_FIELDS, ORGANIZATION_FIELDS, USER_FIELDS } from './fields'

export interface PullRequestNode {
  id: string
  number: number
  createdAt: string
  updatedAt: string
  url: string
  title: string
  body: string
  state: string
  authorAssociation: string
  labels: { nodes: { name: string }[] | null } | null
  additions: number
  deletions: number
  changedFiles: number
  author: GithubUserNode | null
}

export interface PullRequestsPage {
  repository: {
    pullRequests: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      nodes: (PullRequestNode | null)[]
    }
  }
}

export interface PrTimelineItem {
  __typename: string
  id?: string
  createdAt?: string
  submittedAt?: string
  state?: string
  body?: string
  actor?: GithubUserNode | null
  author?: GithubUserNode | null
  assignee?: GithubUserNode | null
  requestedReviewer?: GithubUserNode | null
}

export interface PrTimelineBatchPage {
  nodes: ({
    id: string
    timelineItems: {
      nodes: (PrTimelineItem | null)[]
    }
  } | null)[]
}

const ACTOR_FIELDS = `
  __typename
  login
  ... on User {
    ${USER_FIELDS}
  }
  ... on Bot {
    ${BOT_FIELDS}
  }
`

export const PULL_REQUESTS_QUERY = `
  query ($owner: String!, $repo: String!, $first: Int!, $cursor: String, $direction: OrderDirection!) {
    repository(owner: $owner, name: $repo) {
      pullRequests(first: $first, after: $cursor, orderBy: {field: UPDATED_AT, direction: $direction}) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          id
          number
          createdAt
          updatedAt
          url
          title
          body
          state
          authorAssociation
          labels(first: $first) {
            nodes {
              name
            }
          }
          additions
          deletions
          changedFiles
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
`

export interface PrTimelinePage {
  nodes: ({
    id: string
    timelineItems: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      nodes: (PrTimelineItem | null)[]
    }
  } | null)[]
}

export const PR_TIMELINE_QUERY = `
  query ($ids: [ID!]!, $first: Int!, $after: String) {
    nodes(ids: $ids) {
      ... on PullRequest {
        id
        timelineItems(
          first: $first
          after: $after
          itemTypes: [PULL_REQUEST_REVIEW, MERGED_EVENT, ASSIGNED_EVENT, REVIEW_REQUESTED_EVENT, CLOSED_EVENT]
        ) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            __typename
            ... on ReviewRequestedEvent {
              id
              createdAt
              actor {
                ${ACTOR_FIELDS}
              }
              requestedReviewer {
                __typename
                ... on User {
                  login
                  ${USER_FIELDS}
                }
                ... on Bot {
                  login
                  ${BOT_FIELDS}
                }
                ... on Team {
                  name
                  id
                }
              }
            }
            ... on PullRequestReview {
              id
              state
              submittedAt
              body
              author {
                ${ACTOR_FIELDS}
              }
            }
            ... on AssignedEvent {
              id
              createdAt
              assignee {
                __typename
                ... on User {
                  login
                  ${USER_FIELDS}
                }
                ... on Bot {
                  login
                  ${BOT_FIELDS}
                }
                ... on Organization {
                  login
                  ${ORGANIZATION_FIELDS}
                }
              }
              actor {
                ${ACTOR_FIELDS}
              }
            }
            ... on MergedEvent {
              id
              createdAt
              actor {
                ${ACTOR_FIELDS}
              }
            }
            ... on ClosedEvent {
              id
              createdAt
              actor {
                ${ACTOR_FIELDS}
              }
            }
          }
        }
      }
    }
  }
`
