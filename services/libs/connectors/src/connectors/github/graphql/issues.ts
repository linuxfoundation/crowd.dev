import type { GithubUserNode } from '../mappers/member'

import { BOT_FIELDS, ORGANIZATION_FIELDS, USER_FIELDS } from './fields'

export interface IssueTimelineNode {
  __typename: string
  actor?: GithubUserNode | null
  createdAt?: string
}

export interface IssueNode {
  id: string
  number: number
  title: string
  url: string
  state: string
  createdAt: string
  updatedAt: string
  bodyText: string
  author: GithubUserNode | null
  timelineItems: {
    nodes: (IssueTimelineNode | null)[]
  }
}

export interface IssuesPage {
  repository: {
    issues: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      nodes: (IssueNode | null)[]
    }
  }
}

export interface IssueCommentNode {
  id: string
  bodyText: string
  url: string
  createdAt: string
  author: GithubUserNode | null
}

export interface IssueCommentsConnection {
  pageInfo: {
    endCursor: string | null
    hasNextPage: boolean
  }
  nodes: (IssueCommentNode | null)[]
}

export interface IssueCommentsBatchPage {
  nodes: ({
    id: string
    number: number
    comments: IssueCommentsConnection
  } | null)[]
}

export interface IssueCommentsPaginatedPage {
  repository: {
    issue: {
      comments: IssueCommentsConnection
    } | null
  }
}

const COMMENT_FIELDS = `
  id
  bodyText
  url
  createdAt
  author {
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
`

export const ISSUE_COMMENTS_QUERY = `
  query ($ids: [ID!]!, $first: Int!) {
    nodes(ids: $ids) {
      ... on Issue {
        id
        number
        comments(first: $first) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            ${COMMENT_FIELDS}
          }
        }
      }
    }
  }
`

export const ISSUE_COMMENTS_PAGINATED_QUERY = `
  query ($owner: String!, $repo: String!, $issueNumber: Int!, $first: Int!, $cursor: String) {
    repository(owner: $owner, name: $repo) {
      issue(number: $issueNumber) {
        comments(first: $first, after: $cursor) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            ${COMMENT_FIELDS}
          }
        }
      }
    }
  }
`

export const ISSUES_QUERY = `
  query ($owner: String!, $repo: String!, $first: Int!, $cursor: String, $since: DateTime) {
    repository(owner: $owner, name: $repo) {
      issues(
        first: $first
        after: $cursor
        orderBy: {field: UPDATED_AT, direction: ASC}
        states: [OPEN, CLOSED]
        filterBy: {since: $since}
      ) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          id
          number
          title
          url
          state
          createdAt
          updatedAt
          bodyText
          author {
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
          timelineItems(first: 10, itemTypes: [CLOSED_EVENT]) {
            nodes {
              __typename
              ... on ClosedEvent {
                createdAt
                actor {
                  login
                  ... on User {
                    ${USER_FIELDS}
                  }
                  ... on Bot {
                    ${BOT_FIELDS}
                  }
                }
              }
            }
          }
        }
      }
    }
  }
`
