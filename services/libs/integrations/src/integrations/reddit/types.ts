export const REDDIT_MAX_RETROSPECT_IN_HOURS = 5

export enum RedditActivityType {
  POST = 'post',
  COMMENT = 'comment',
}

export { IRedditIntegrationSettings } from '@crowd/types'

export interface IRedditPublishData {
  type: RedditActivityType
  data: RedditPost | RedditComment
  subreddit: string
  sourceParentId?: string
  postUrl?: string
  postTitle?: string
  postId?: string
}

export interface IRedditSubredditStreamData {
  channel: string
  after?: string
}

export interface IRedditCommentStreamData {
  channel: string
  postTitle: string
  postUrl: string
  postId: string
  after?: string
}

export interface IRedditMoreCommentsStreamData {
  channel: string
  postId: string
  children: string[]
}

export enum RedditStreamType {
  SUBREDDIT = 'subreddit',
  COMMENTS = 'comments',
  MORE_COMMENTS = 'more_comments',
}

interface RedditBase {
  id: string
  name: string
  title: string
  subreddit: string
  url: string
  downs: number
  ups: number
  upvote_ratio: number
  score: number
  thumbnail: string
  permalink: string
  created: number
  author: string
  author_fullname: string
}

export interface RedditGetPostsInput {
  subreddit: string
  nangoId: string
  after?: string
}

export interface RedditGetCommentsInput {
  subreddit: string
  nangoId: string
  postId: string
}

export interface RedditMoreCommentsInput {
  nangoId: string
  postId: string
  children: string[]
}

export interface RedditComment extends RedditBase {
  body_html: string
  replies?: {
    kind: string
    data: {
      children: {
        kind: string
        data: RedditComment | RedditMoreChildren
      }[]
    }
  }
}

export interface RedditMoreChildren {
  children: string[]
}

export interface RedditPost extends RedditBase {
  selftext_html: string
}

export interface RedditPostsResponse {
  data: {
    children: {
      data: RedditPost
    }[]
  }
}

export type RedditCommentsResponse = RedditInternalComment[]

interface RedditInternalComment {
  kind: string
  data: {
    children: {
      kind: string
      data: RedditComment
    }[]
  }
}

export interface RedditMoreCommentsResponse {
  json: {
    data: {
      things: {
        data: RedditComment
        kind: string
      }[]
    }
  }
}
