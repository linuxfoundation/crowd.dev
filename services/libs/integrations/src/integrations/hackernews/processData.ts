import sanitizeHtml from 'sanitize-html'

import {
  IActivityData,
  IMemberData,
  MemberAttributeName,
  MemberIdentityType,
  PlatformType,
} from '@crowd/types'

import { IProcessDataContext, ProcessDataHandler } from '../../types'

import { HACKERNEWS_GRID } from './grid'
import { HackerNewsActivityType, HackerNewsPublishData } from './types'

async function parsePost(ctx: IProcessDataContext) {
  const data = ctx.data as HackerNewsPublishData
  const { post, channel, parentId, parentTitle } = data
  const type = post.parent ? HackerNewsActivityType.COMMENT : HackerNewsActivityType.POST
  const url = `https://news.ycombinator.com/item?id=${post.parent ? post.parent : post.id}`
  const body =
    post.text !== undefined && post.text !== ''
      ? sanitizeHtml(post.text)
      : `<a href="${post.url}" target="_blank">${post.url}</a>`

  const member: IMemberData = {
    identities: [
      {
        platform: PlatformType.HACKERNEWS,
        value: post.user.id,
        type: MemberIdentityType.USERNAME,
        sourceId: post.user.id,
        verified: true,
        verifiedBy: PlatformType.HACKERNEWS,
      },
    ],
    displayName: post.user.id,
    attributes: {
      [MemberAttributeName.SOURCE_ID]: {
        [PlatformType.HACKERNEWS]: post.user.id,
      },
      [MemberAttributeName.KARMA]: {
        [PlatformType.HACKERNEWS]: post.user.karma,
      },
      [MemberAttributeName.BIO]: {
        [PlatformType.HACKERNEWS]: post.user.about,
      },
    },
  }
  const activity: IActivityData = {
    sourceId: post.id.toString(),
    ...(post.parent && { sourceParentId: post.parent.toString() }),
    type,
    timestamp: new Date(post.time * 1000).toISOString(),
    body,
    title: post.title,
    url,
    channel,
    score: HACKERNEWS_GRID[type].score,
    member,
    attributes: {
      commentsCount: post.descendants,
      destinationUrl: post.url,
      score: post.score,
      ...(post.parent && {
        parentUrl: `https://news.ycombinator.com/item?id=${parentId}`,
        parentTitle,
      }),
      type: post.type,
    },
  }
  await ctx.publishActivity(activity)
}

const handler: ProcessDataHandler = async (ctx) => {
  await parsePost(ctx)
}

export default handler
