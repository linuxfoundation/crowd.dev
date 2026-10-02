import {
  ILinkedInMember,
  ILinkedInOrganization,
  ILinkedInPostComment,
  ILinkedInPostReaction,
} from './api/types'

export enum LinkedinActivityType {
  COMMENT = 'comment',
  REACTION = 'reaction',
  MENTION = 'mention',
}

export enum LinkedinStreamType {
  ORGANIZATION = 'organization',
  POST_COMMENTS = 'post_comments',
  POST_REACTIONS = 'post_reactions',
  COMMENT_COMMENTS = 'comment_comments',
}

export interface ILinkedInAuthor {
  type: string
  data: ILinkedInMember | ILinkedInOrganization
}

export interface ILinkedInRootOrganizationStream {
  organization: string
  organizationUrn: string
  start?: number
}

export interface ILinkedInChildPostCommentsStream {
  postUrnId: string
  postBody: string
  start?: number
}

export interface ILinkedInChildPostReactionsStream {
  postUrnId: string
  postBody: string
  start?: number
}

export interface ILinkedInChildCommentCommentsStream {
  postUrnId: string
  postBody: string
  commentUrnId: string
  start?: number
}

export interface ILinkedInCachedMember extends ILinkedInMember {
  userId: string
}

export interface ILinkedInCachedOrganization extends ILinkedInOrganization {
  userId: string
}

export interface ILinkedInData {
  type: 'reaction' | 'comment' | 'child_comment'
}

export interface ILinkedInReactionData extends ILinkedInData {
  type: 'reaction'
  postUrnId: string
  postBody: string
  reaction: ILinkedInPostReaction
  author: ILinkedInAuthor
}

export interface ILinkedInCommentData extends ILinkedInData {
  type: 'comment'
  comment: ILinkedInPostComment
  postUrnId: string
  postBody: string
  author: ILinkedInAuthor
}

export interface ILinkedInChildCommentData extends ILinkedInData {
  type: 'child_comment'
  parentCommentUrnId: string
  comment: ILinkedInPostComment
  postUrnId: string
  postBody: string
  author: ILinkedInAuthor
}
