import { randomUUID } from 'crypto'
import he from 'he'
import sanitizeHtml from 'sanitize-html'

import {
  IActivityData,
  IMemberData,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import { IProcessDataContext, ProcessDataHandler } from '../../types'

import { REDDIT_GRID } from './grid'
import { IRedditPublishData, RedditActivityType, RedditComment, RedditPost } from './types'

interface IParsePostInput {
  channel: string
  post: RedditPost
  ctx: IProcessDataContext
}

/**
 * Parse a post from the reddit API into a crowd.dev activity
 * @param tenantId the tenant ID
 * @param channel the channel (subreddit) we are parsing
 * @param post the post from the Reddit API
 * @returns a post parsed as a crowd.dev activity
 */
async function parsePost({ channel, post, ctx }: IParsePostInput) {
  const body = post.selftext_html
    ? sanitizeHtml(he.decode(post.selftext_html))
    : `<a href="${post.url}" target="__blank">${post.url}</a>`

  const member = parseMember(post)
  const activity: IActivityData = {
    member,
    sourceId: post.id.toString(),
    type: RedditActivityType.POST,
    timestamp: new Date(post.created * 1000).toISOString(),
    body,
    title: post.title,
    url: `https://www.reddit.com${post.permalink}`,
    channel,
    score: REDDIT_GRID[RedditActivityType.POST].score,
    attributes: {
      url: post.url,
      name: post.name,
      downs: post.downs,
      ups: post.ups,
      upvoteRatio: post.upvote_ratio,
      thubmnail: post.thumbnail,
    },
  }

  await ctx.publishActivity(activity)
}

interface IParseCommentInput {
  channel: string
  comment: RedditComment
  sourceParentId: string
  postUrl: string
  postTitle: string
  postId: string
  ctx: IProcessDataContext
}

/**
 * Parse a comment from the reddit API into a crowd.dev activity
 * @param tenantId the tenant ID
 * @param channel the channel (subreddit) we are parsing
 * @param comment the comment from the Reddit API
 * @param sourceParentId the ID in Reddit of the parent comment or post
 * @returns a comment parsed as a crowd.dev activity
 */
async function parseComment({
  channel,
  comment,
  sourceParentId,
  postUrl,
  postTitle,
  postId,
  ctx,
}: IParseCommentInput) {
  const member = parseMember(comment)

  const activity: IActivityData = {
    member,
    sourceId: comment.id.toString(),
    type: RedditActivityType.COMMENT,
    timestamp: new Date(comment.created * 1000).toISOString(),
    sourceParentId,
    body: sanitizeHtml(he.decode(comment.body_html)),
    title: comment.title,
    url: `https://www.reddit.com${comment.permalink}`,
    channel,
    score: REDDIT_GRID[RedditActivityType.COMMENT].score,
    attributes: {
      url: comment.url,
      name: comment.name,
      postUrl: postUrl,
      postTitle: postTitle,
      postId: postId,
      downs: comment.downs,
      ups: comment.ups,
      upvoteRatio: comment.upvote_ratio,
      thubmnail: comment.thumbnail,
    },
  }

  await ctx.publishActivity(activity)
}

/**
 * Parse the relevant fields of a post or a comment into a community member
 * @param activity either a post or a comment
 * @returns a crowd.dev community member
 */
function parseMember(activity: RedditPost | RedditComment): IMemberData {
  if (activity.author === '[deleted]') {
    const uniqueId = `deleted:${randomUUID()}`

    return {
      identities: [
        {
          platform: PlatformType.REDDIT,
          value: uniqueId,
          type: MemberIdentityType.USERNAME,
          sourceId: uniqueId,
          verified: true,
          verifiedBy: PlatformType.REDDIT,
        },
      ],
      displayName: 'Deleted User',
    }
  }
  return {
    identities: [
      {
        platform: PlatformType.REDDIT,
        value: activity.author,
        type: MemberIdentityType.USERNAME,
        sourceId: activity.author_fullname,
        verified: true,
        verifiedBy: PlatformType.REDDIT,
      },
    ],
    displayName: activity.author,
    attributes: {
      [MemberAttributeName.SOURCE_ID]: {
        [PlatformType.REDDIT]: activity.author_fullname,
      },
      [MemberAttributeName.URL]: {
        [PlatformType.REDDIT]: `https://www.reddit.com/user/${activity.author}`,
      },
    },
  }
}

const handler: ProcessDataHandler = async (ctx) => {
  const data = ctx.data as IRedditPublishData
  // post and comment are mutually exclusive
  if (data.type === RedditActivityType.POST) {
    await parsePost({
      channel: data.subreddit,
      post: data.data as RedditPost,
      ctx,
    })
  } else if (data.type === RedditActivityType.COMMENT) {
    await parseComment({
      channel: data.subreddit,
      comment: data.data as RedditComment,
      sourceParentId: data.sourceParentId,
      postUrl: data.postUrl,
      postTitle: data.postTitle,
      postId: data.postId,
      ctx,
    })
  } else {
    await ctx.abortRunWithError(`Unknown data type`)
    throw new Error(`Unknown data type`)
  }
}

export default handler
