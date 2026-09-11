import type { GithubUserNode } from '../mappers/member'

import { BOT_FIELDS, ORGANIZATION_FIELDS, USER_FIELDS } from './fields'

export interface DiscussionCategory {
  id: string
  isAnswerable: boolean
  name: string
  slug: string
  emoji: string
  description: string | null
}

export interface DiscussionNode {
  id: string
  number: number
  title: string
  bodyText: string
  url: string
  createdAt: string
  updatedAt: string
  isAnswered: boolean | null
  author: GithubUserNode | null
  category: DiscussionCategory
  comments: { totalCount: number }
}

export interface DiscussionsPage {
  repository: {
    discussions: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      nodes: (DiscussionNode | null)[]
    }
  }
}

export interface DiscussionReplyNode {
  id: string
  bodyText: string
  url: string
  createdAt: string
}

export interface DiscussionCommentNode extends DiscussionReplyNode {
  replyTo: { id: string } | null
  replies: RepliesConnection
}

export interface RepliesConnection {
  pageInfo: {
    endCursor: string | null
    hasNextPage: boolean
  }
  nodes: (DiscussionReplyNode | null)[]
}

export interface DiscussionCommentsPage {
  node: {
    comments: {
      pageInfo: {
        endCursor: string | null
        hasNextPage: boolean
      }
      nodes: (DiscussionCommentNode | null)[]
    }
  } | null
}

export interface CommentRepliesPage {
  node: {
    replies: RepliesConnection
  } | null
}

const AUTHOR_FIELDS = `
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

const REPLY_FIELDS = `
  id
  bodyText
  url
  createdAt
`

export const DISCUSSIONS_QUERY = `
  query ($owner: String!, $repo: String!, $first: Int!, $cursor: String) {
    repository(owner: $owner, name: $repo) {
      discussions(
        first: $first
        after: $cursor
        orderBy: {field: UPDATED_AT, direction: DESC}
      ) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          id
          number
          title
          bodyText
          url
          createdAt
          updatedAt
          isAnswered
          ${AUTHOR_FIELDS}
          category {
            id
            isAnswerable
            name
            slug
            emoji
            description
          }
          comments {
            totalCount
          }
        }
      }
    }
  }
`

export const DISCUSSION_COMMENTS_QUERY = `
  query ($id: ID!, $first: Int!, $cursor: String, $repliesFirst: Int!) {
    node(id: $id) {
      ... on Discussion {
        comments(first: $first, after: $cursor) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            ${REPLY_FIELDS}
            replyTo {
              id
            }
            replies(first: $repliesFirst) {
              pageInfo {
                endCursor
                hasNextPage
              }
              nodes {
                ${REPLY_FIELDS}
              }
            }
          }
        }
      }
    }
  }
`

export const COMMENT_REPLIES_QUERY = `
  query ($id: ID!, $first: Int!, $cursor: String) {
    node(id: $id) {
      ... on DiscussionComment {
        replies(first: $first, after: $cursor) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            ${REPLY_FIELDS}
          }
        }
      }
    }
  }
`
